import { App } from "@microsoft/teams.apps";
import { ChatPrompt } from "@microsoft/teams.ai";
import { LocalStorage } from "@microsoft/teams.common";
import { OpenAIChatModel } from "@microsoft/teams.openai";
import { MessageActivity, TokenCredentials, ClientCredentials } from '@microsoft/teams.api';
import { ManagedIdentityCredential } from '@azure/identity';
import * as fs from 'fs';
import * as path from 'path';
import config from "../config";
import graphApiHelper from "../graphApiHelper";
import summarizationHelper from "../summarizationHelper";

// Create storage for conversation history
const storage = new LocalStorage();

// Track last bot response for each conversation - used for contextual follow-ups like "send it to my email"
interface LastBotResponse {
  content: string;
  contentType: 'calendar' | 'summary' | 'minutes' | 'transcript' | 'meeting_overview' | 'insights' | 'general';
  subject?: string;
  timestamp: number;
}
const lastBotResponseMap = new Map<string, LastBotResponse>();

// Track active call IDs -> { conversationId, serviceUrl, organizerId, joinWebUrl } for webhook handling
interface ActiveCall {
  conversationId: string;
  serviceUrl: string;
  organizerId?: string;
  joinWebUrl?: string;
  establishedAt?: number;   // timestamp when call was established
  terminatedAt?: number;    // timestamp when call was terminated
  leavingInProgress?: boolean; // prevent duplicate hang-up
}
const activeCallMap = new Map<string, ActiveCall>();

// Live transcript storage: conversationId -> array of transcript entries
interface TranscriptEntry {
  speaker: string;
  text: string;
  timestamp: string;
  isFinal: boolean;
}
const liveTranscriptMap = new Map<string, TranscriptEntry[]>();

// Map callId -> conversationId for transcript event routing
const callToConversationMap = new Map<string, string>();

// Directory for persisted transcript files - use Azure's writable temp directory if available
const TRANSCRIPTS_DIR = process.env.TEMP || process.env.HOME 
  ? path.join(process.env.TEMP || process.env.HOME || '', 'mela_transcripts')
  : path.join(__dirname, '..', '..', 'transcripts');
const MEETING_CONTEXT_FILE = path.join(TRANSCRIPTS_DIR, 'meeting_context.json');

console.log(`[STARTUP] Transcripts directory set to: ${TRANSCRIPTS_DIR}`);

interface MeetingContextEntry {
  organizerId: string;
  joinWebUrl: string;
  subject?: string;
  updatedAt: number;
  callStartedAt?: number;
  callEndedAt?: number;
}

const meetingContextMap = new Map<string, MeetingContextEntry>();

function isBotMentioned(activity: any): boolean {
  const entities = activity?.entities;
  if (!Array.isArray(entities)) {
    return false;
  }

  const botId = activity?.recipient?.id;
  const clientId = process.env.CLIENT_ID || '';
  
  for (const entity of entities) {
    if (entity?.type !== 'mention') {
      continue;
    }

    const mentionedId = entity?.mentioned?.id || '';
    // Compare with multiple formats: exact match, stripped prefix, or CLIENT_ID
    if (
      (botId && mentionedId && mentionedId === botId) ||
      (botId && mentionedId.includes(botId)) ||
      (botId && botId.includes(mentionedId)) ||
      (clientId && mentionedId.includes(clientId)) ||
      (clientId && mentionedId === `28:${clientId}`)
    ) {
      return true;
    }
  }

  return false;
}

function removeAtMentions(text: string): string {
  return text
    .replace(/<at>[^<]*<\/at>/gi, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Parse a VTT (Web Video Text Tracks) transcript into TranscriptEntry objects.
 * Teams VTT format:
 *   WEBVTT
 *
 *   1
 *   00:00:01.000 --> 00:00:05.000
 *   <v Speaker Name>Some spoken text</v>
 *
 *   2
 *   00:00:05.500 --> 00:00:10.000
 *   <v Another Speaker>More text</v>
 */
function parseVttToEntries(vttContent: string): TranscriptEntry[] {
  const entries: TranscriptEntry[] = [];
  
  // Log a snippet of raw VTT for debugging
  console.log(`[VTT_PARSER] Raw VTT length: ${vttContent.length} chars`);
  console.log(`[VTT_PARSER] First 500 chars:\n${vttContent.slice(0, 500)}`);

  // Split by double-newline to get cue blocks (handles \r\n and \n)
  const normalized = vttContent.replace(/\r\n/g, '\n');
  const blocks = normalized.split(/\n\n+/);
  
  for (const block of blocks) {
    const lines = block.trim().split('\n');
    if (lines.length === 0) continue;

    // Skip WEBVTT header block and NOTE blocks
    if (lines[0].startsWith('WEBVTT') || lines[0].startsWith('NOTE')) continue;

    let timestamp = '';
    let speaker = 'Unknown';
    let textParts: string[] = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      // Cue sequence number � skip
      if (/^\d+$/.test(trimmed)) continue;

      // Timestamp line: 00:00:00.000 --> 00:00:03.000
      const tsMatch = trimmed.match(/^(\d{2}:\d{2}:\d{2}\.\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2}\.\d{3})/);
      if (tsMatch) {
        timestamp = tsMatch[1];
        continue;
      }

      // Text line � extract speaker tag if present
      // Handles: <v Speaker Name>text</v>, <v Speaker Name>text, and plain text
      const speakerMatch = trimmed.match(/^<v\s+([^>]+)>(.*)/);
      if (speakerMatch) {
        speaker = speakerMatch[1].trim();
        // Remove closing </v> tag if present
        const text = speakerMatch[2].replace(/<\/v>/g, '').trim();
        if (text) textParts.push(text);
      } else {
        // Plain continuation text � strip any stray </v> tags
        const cleaned = trimmed.replace(/<\/v>/g, '').trim();
        if (cleaned) textParts.push(cleaned);
      }
    }

    if (textParts.length > 0 && timestamp) {
      entries.push({
        speaker,
        text: textParts.join(' '),
        timestamp,
        isFinal: true,
      });
    }
  }

  console.log(`[VTT_PARSER] Parsed ${entries.length} entries from VTT content`);
  // Log unique speakers found
  const speakers = [...new Set(entries.map(e => e.speaker))];
  console.log(`[VTT_PARSER] Speakers found: ${speakers.join(', ')}`);
  return entries;
}

/**
 * Convert a VTT timestamp (HH:MM:SS.mmm) to a compact display format (H:MM:SS).
 * e.g. "00:01:22.530" ? "0:01:22", "01:22:32.000" ? "1:22:32"
 */
function formatVttTimestamp(vttTime: string): string {
  if (!vttTime) return '';
  const match = vttTime.match(/(\d{2}):(\d{2}):(\d{2})/);
  if (!match) return vttTime;
  const h = parseInt(match[1], 10);
  const m = match[2];
  const s = match[3];
  return `${h}:${m}:${s}`;
}

/**
 * Build a well-formatted Teams markdown transcript message.
 * Includes meeting title, date, participants, and speaker-by-speaker entries.
 */
/** Build a well-formatted HTML transcript using LLM with formatting instructions. */
async function buildTranscriptHtml(
  displayEntries: TranscriptEntry[],
  meetingTitle: string,
  members: string[],
  totalEntries: number,
  showingPartial: boolean
): Promise<string> {
  try {
    const instructionsPath = path.join(__dirname, 'transcriptFormatInstructions.txt');
    const instructions = fs.readFileSync(instructionsPath, 'utf-8');

    const now = new Date();
    const dateStr = now.toLocaleDateString('en-US', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
    });
    const timeStr = now.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    const lastTime = displayEntries.length > 0
      ? formatVttTimestamp(displayEntries[displayEntries.length - 1].timestamp)
      : '';

    const transcriptSpeakers = [...new Set(displayEntries.map(e => e.speaker))].filter(s => s !== 'Unknown');
    const participantNames = members.length > 0 ? members : transcriptSpeakers;

    const transcriptData = displayEntries.map(e => ({
      speaker: e.speaker,
      timestamp: formatVttTimestamp(e.timestamp),
      text: e.text
    }));

    const prompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content:
            `Meeting Title: ${meetingTitle}\n` +
            `Date: ${dateStr}\n` +
            `Time: ${timeStr}\n` +
            `Duration: ${lastTime || 'N/A'}\n` +
            `Participants: ${participantNames.join(', ')}\n` +
            `Total Entries: ${totalEntries}\n` +
            `Showing: ${showingPartial ? `Last 80 of ${totalEntries} entries` : 'All entries'}\n\n` +
            `Transcript Entries:\n${JSON.stringify(transcriptData, null, 2)}\n\n` +
            `Transform this transcript into a well-formatted, professional document organized by topic. Extract key insights, highlight important points, and create well-written narrative sentences. DO NOT output raw line-by-line text. Follow the instructions provided carefully for formatting.`
        },
      ],
      instructions: instructions,
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    const response = await prompt.send('');
    return response.content || 'Could not generate transcript. Please try again.';
  } catch (error) {
    console.error(`[TRANSCRIPT_FORMAT_ERROR]`, error);
    return 'Error generating transcript formatting. Please try again.';
  }
}

function buildTranscriptMarkdown(
  displayEntries: TranscriptEntry[],
  meetingTitle: string,
  members: string[],
  totalEntries: number,
  showingPartial: boolean
): string {
  const now = new Date();
  const dateStr = now.toLocaleDateString('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
  });
  const timeStr = now.toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit'
  });

  // Compute duration from last entry
  const lastTime = displayEntries.length > 0
    ? formatVttTimestamp(displayEntries[displayEntries.length - 1].timestamp)
    : '';

  // Unique speakers from transcript itself (fallback if members list is empty)
  const transcriptSpeakers = [...new Set(displayEntries.map(e => e.speaker))].filter(s => s !== 'Unknown');
  const participantNames = members.length > 0 ? members : transcriptSpeakers;

  // --- Header ---
  let md = `## ?? Meeting Transcript\n\n`;
  md += `### **${meetingTitle}**\n\n`;
  md += `**Meeting details**\n`;
  md += `- **Date:** ${dateStr}\n`;
  md += `- **Time:** ${timeStr}\n`;
  if (lastTime) {
    md += `- **Duration:** ~${lastTime}\n`;
  }
  md += `- **Entries captured:** ${totalEntries}\n\n`;

  if (participantNames.length > 0) {
    md += `**Participants**\n`;
    for (const participant of participantNames) {
      md += `- ${participant}\n`;
    }
    md += `\n`;
  }

  if (showingPartial) {
    md += `_Showing last 80 of ${totalEntries} entries for readability._\n\n`;
  }

  md += `---\n\n`;
  md += `### **Transcript**\n\n`;

  // --- Body: speaker blocks ---
  let lastSpeaker = '';
  for (const entry of displayEntries) {
    const time = formatVttTimestamp(entry.timestamp);
    if (entry.speaker !== lastSpeaker) {
      if (lastSpeaker) {
        md += `\n`;
      }
      md += `**${entry.speaker}**  \\n`;
      md += `_At ${time}_\n\n`;
      lastSpeaker = entry.speaker;
    }
    md += `- ${entry.text}\n`;
  }

  return md;
}

/** Save the current transcript for a conversation to a .txt file (Teams-like format). */
function saveTranscriptToFile(conversationId: string) {
  try {
    const entries = liveTranscriptMap.get(conversationId);
    const finalEntries = entries?.filter(e => e.isFinal) || [];
    if (finalEntries.length === 0) return;

    // Ensure transcripts directory exists
    if (!fs.existsSync(TRANSCRIPTS_DIR)) {
      fs.mkdirSync(TRANSCRIPTS_DIR, { recursive: true });
    }

    // Build a clean filename from conversation ID + date
    const safeId = conversationId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60);
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10);
    const filePath = path.join(TRANSCRIPTS_DIR, `transcript_${dateStr}_${safeId}.txt`);

    // Calculate approximate meeting duration from last timestamp
    const lastTimestamp = finalEntries[finalEntries.length - 1]?.timestamp || '';
    const formattedDate = now.toLocaleDateString('en-US', {
      year: 'numeric', month: 'long', day: 'numeric'
    });
    const formattedTime = now.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    // -- Header --
    let content = '';
    content += `MEETING TRANSCRIPT\n`;
    content += `==================\n\n`;
    content += `Title: Meeting\n`;
    content += `Date: ${formattedDate}\n`;
    content += `Time: ${formattedTime}\n`;
    if (lastTimestamp) content += `Duration: ~${formatVttTimestamp(lastTimestamp)}\n`;
    content += `\n`;
    content += `TRANSCRIPT\n`;
    content += `----------\n\n`;

    // -- Body: Teams-style speaker blocks --
    // Each new speaker (or same speaker at a different timestamp) gets a header line
    let lastSpeaker = '';
    let lastTime = '';
    for (const entry of finalEntries) {
      const time = formatVttTimestamp(entry.timestamp);
      const speakerChanged = entry.speaker !== lastSpeaker;
      const timeChanged = time !== lastTime;

      if (speakerChanged || timeChanged) {
        if (content.length > 0 && !content.endsWith('\n\n')) {
          content += '\n';
        }
        content += `${entry.speaker} (${time})\n`;
        content += `  - `;
        lastSpeaker = entry.speaker;
        lastTime = time;
      } else {
        content += `  - `;
      }
      content += `${entry.text}\n`;
    }

    fs.writeFileSync(filePath, content, 'utf-8');
    console.log(`[TRANSCRIPT_FILE] Saved ${finalEntries.length} entries to ${filePath}`);
  } catch (err) {
    console.error(`[TRANSCRIPT_FILE_ERROR] Failed to save transcript:`, err);
  }
}

function getSafeConversationId(conversationId: string): string {
  return conversationId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60);
}

function findLatestTranscriptFilePath(conversationId: string): string | null {
  try {
    if (!fs.existsSync(TRANSCRIPTS_DIR)) {
      return null;
    }

    const safeId = getSafeConversationId(conversationId);
    const matches = fs
      .readdirSync(TRANSCRIPTS_DIR)
      .filter((name) => name.startsWith('transcript_') && name.endsWith(`_${safeId}.txt`))
      .map((name) => path.join(TRANSCRIPTS_DIR, name));

    if (matches.length === 0) {
      return null;
    }

    matches.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
    return matches[0];
  } catch (error) {
    console.warn(`[TRANSCRIPT_CACHE] Failed to find cached transcript file:`, error);
    return null;
  }
}

function loadCachedTranscriptText(conversationId: string): string | null {
  try {
    const latestPath = findLatestTranscriptFilePath(conversationId);
    if (!latestPath) return null;

    const content = fs.readFileSync(latestPath, 'utf-8').trim();
    if (!content) return null;

    console.log(`[TRANSCRIPT_CACHE] Loaded transcript from file cache: ${latestPath}`);
    return content;
  } catch (error) {
    console.warn(`[TRANSCRIPT_CACHE] Failed to load transcript file cache:`, error);
    return null;
  }
}

function readMeetingContextStore(): Record<string, MeetingContextEntry> {
  try {
    if (!fs.existsSync(MEETING_CONTEXT_FILE)) {
      return {};
    }
    const raw = fs.readFileSync(MEETING_CONTEXT_FILE, 'utf-8').trim();
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, MeetingContextEntry>;
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (error) {
    console.warn(`[MEETING_CONTEXT] Failed to read context store:`, error);
    return {};
  }
}

function writeMeetingContextStore(store: Record<string, MeetingContextEntry>) {
  try {
    console.log(`[CACHE_DEBUG] writeMeetingContextStore starting, dir: ${TRANSCRIPTS_DIR}`);
    if (!fs.existsSync(TRANSCRIPTS_DIR)) {
      console.log(`[CACHE_DEBUG] Creating directory: ${TRANSCRIPTS_DIR}`);
      fs.mkdirSync(TRANSCRIPTS_DIR, { recursive: true });
    }
    console.log(`[CACHE_DEBUG] Writing to: ${MEETING_CONTEXT_FILE}`);
    fs.writeFileSync(MEETING_CONTEXT_FILE, JSON.stringify(store, null, 2), 'utf-8');
    console.log(`[CACHE_DEBUG] Write successful`);
  } catch (error) {
    console.warn(`[MEETING_CONTEXT] Failed to write context store (non-fatal):`, error);
    // Continue without file persistence - in-memory cache still works
  }
}

function cacheMeetingContext(
  conversationId: string,
  organizerId: string,
  joinWebUrl: string,
  subject?: string,
  callWindow?: { startedAt?: number; endedAt?: number }
) {
  console.log(`[CACHE_DEBUG] cacheMeetingContext called for ${conversationId}`);
  try {
    const existing = getCachedMeetingContext(conversationId);
    console.log(`[CACHE_DEBUG] Got existing: ${existing ? 'yes' : 'no'}`);
    const entry: MeetingContextEntry = {
      organizerId,
      joinWebUrl,
      subject: subject || existing?.subject,
      updatedAt: Date.now(),
      callStartedAt: callWindow?.startedAt || existing?.callStartedAt,
      callEndedAt: callWindow?.endedAt || existing?.callEndedAt,
    };
    console.log(`[CACHE_DEBUG] Entry created`);

    meetingContextMap.set(conversationId, entry);
    console.log(`[CACHE_DEBUG] Memory map updated`);
    const store = readMeetingContextStore();
    console.log(`[CACHE_DEBUG] Store read, keys: ${Object.keys(store).length}`);
    store[conversationId] = entry;
    writeMeetingContextStore(store);
    console.log(`[MEETING_CONTEXT] Cached meeting context for conversation ${conversationId}`);
  } catch (err) {
    console.error(`[CACHE_DEBUG] Error in cacheMeetingContext:`, err);
  }
}

function getCachedMeetingContext(conversationId: string): MeetingContextEntry | null {
  const memoryEntry = meetingContextMap.get(conversationId);
  if (memoryEntry) {
    return memoryEntry;
  }

  const store = readMeetingContextStore();
  const diskEntry = store[conversationId];
  if (diskEntry?.organizerId && diskEntry?.joinWebUrl) {
    meetingContextMap.set(conversationId, diskEntry);
    return diskEntry;
  }
  return null;
}

async function resolveMeetingInfoForConversation(conversationId: string) {
  console.log(`[RESOLVE_MEETING] Starting for ${conversationId}`);
  const graphInfo = await graphApiHelper.getOnlineMeetingFromChat(conversationId);
  console.log(`[RESOLVE_MEETING] getOnlineMeetingFromChat returned: organizer=${graphInfo?.organizer?.id}, joinWebUrl=${graphInfo?.joinWebUrl ? 'yes' : 'no'}`);
  if (graphInfo?.organizer?.id && graphInfo?.joinWebUrl) {
    try {
      cacheMeetingContext(conversationId, graphInfo.organizer.id, graphInfo.joinWebUrl, graphInfo.subject);
      console.log(`[RESOLVE_MEETING] Cached and returning graphInfo`);
    } catch (cacheErr) {
      console.error(`[RESOLVE_MEETING] Error caching meeting context:`, cacheErr);
    }
    return graphInfo;
  }

  const cached = getCachedMeetingContext(conversationId);
  if (cached) {
    console.log(`[MEETING_CONTEXT] Using cached meeting context for conversation ${conversationId}`);
    return {
      organizer: { id: cached.organizerId },
      joinWebUrl: cached.joinWebUrl,
      subject: cached.subject,
    };
  }

  return graphInfo;
}

function getTranscriptWindowForConversation(conversationId: string): { min?: number; max?: number } {
  const cached = getCachedMeetingContext(conversationId);
  if (!cached?.callStartedAt) {
    return {};
  }

  // Use the conversation's own call window when available to avoid pulling transcripts
  // from other occurrences in recurring meetings.
  return {
    min: cached.callStartedAt,
    max: cached.callEndedAt,
  };
}

function transcriptEntriesToPlainText(entries: TranscriptEntry[]): string {
  const finalEntries = entries.filter((e) => e.isFinal);
  if (finalEntries.length === 0) return '';

  return finalEntries
    .map((entry) => `${entry.speaker} (${formatVttTimestamp(entry.timestamp)}): ${entry.text}`)
    .join('\n');
}

async function generateKeyMeetingInsights(transcriptText: string, meetingTitle: string): Promise<string> {
  const insightsPrompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `Extract key meeting insights from this transcript.\n\n` +
          `Meeting title: ${meetingTitle}\n\n` +
          `Please provide:\n` +
          `1) Top 5 insights\n` +
          `2) Key decisions\n` +
          `3) Risks/blockers\n` +
          `4) Action items with owners (if available)\n\n` +
          `Transcript:\n${transcriptText}`,
      },
    ],
    instructions:
      'You are an expert meeting analyst. Provide concise, well-structured bullet points. Do not invent facts not present in the transcript.',
    model: new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    }),
  });

  const response = await insightsPrompt.send('');
  return response.content || 'I could not generate insights from the transcript.';
}

// ---------------------------------------------------------------
// HTML & AI-Driven Summary Generators
// ---------------------------------------------------------------

async function generateMeetingSummary(
  entries: TranscriptEntry[],
  meetingTitle: string,
  speaker: string
): Promise<string> {
  if (entries.length === 0) return 'No transcript data available for summary.';

  // Build transcript text
  const transcriptText = entries
    .map(e => `${e.speaker}: ${e.text}`)
    .join('\n');

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `You are a professional meeting analyst. Analyze this meeting transcript and provide a comprehensive, well-structured summary.\n\n` +
          `Meeting Title: ${meetingTitle}\n` +
          `Requested By: ${speaker}\n\n` +
          `Transcript:\n${transcriptText}\n\n` +
          `Provide your response with these sections:\n` +
          `1. **Overview** - A 2-3 sentence summary of what was discussed\n` +
          `2. **Key Topics** - Main points discussed (bullet format)\n` +
          `3. **Participants & Contributions** - Who said what was important\n` +
          `4. **Action Items** - Any next steps or follow-ups needed\n` +
          `5. **Sentiment/Tone** - Overall meeting tone\n\n` +
          `Keep the tone professional but natural. Make sense of abbreviated or fragmented speech.`
      },
    ],
    instructions:
      'You are a professional meeting analyst. Provide clear, well-structured summaries that make sense of meeting dialogue.',
    model: new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    }),
  });

  try {
    const response = await prompt.send('');
    return response.content || 'Could not generate summary.';
  } catch (error) {
    console.error(`[SUMMARY_ERROR]`, error);
    return 'Could not generate summary due to an error.';
  }
}

async function generateFormattedSummaryHtml(
  entries: TranscriptEntry[],
  meetingTitle: string,
  speaker: string,
  members: string[]
): Promise<string> {
  try {
    const instructionsPath = path.join(__dirname, 'summaryFormatInstructions.txt');
    const instructions = fs.readFileSync(instructionsPath, 'utf-8');

    const now = new Date();
    const dateStr = now.toLocaleDateString('en-US', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
    });
    const timeStr = now.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    const transcriptText = entries
      .map(e => `${e.speaker} (${formatVttTimestamp(e.timestamp)}): ${e.text}`)
      .join('\n');

    const prompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content:
            `Meeting Title: ${meetingTitle}\n` +
            `Date: ${dateStr}\n` +
            `Time: ${timeStr}\n` +
            `Participants: ${members.join(', ')}\n` +
            `Requested by: ${speaker}\n\n` +
            `Transcript:\n${transcriptText}\n\n` +
            `Analyze this meeting and generate an intelligent Markdown summary with the 5 required sections. Extract key insights, highlight important points, and focus on what matters. Follow the instructions for format and content. DO NOT use HTML.`
        },
      ],
      instructions: instructions,
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    const response = await prompt.send('');
    return response.content || 'Could not generate meeting summary. Please try again.';
  } catch (error) {
    console.error(`[SUMMARY_FORMAT_ERROR]`, error);
    return 'Error generating meeting summary. Please try again.';
  }
}

async function generateMinutesHtml(
  entries: TranscriptEntry[],
  meetingTitle: string,
  members: string[]
): Promise<string> {
  if (entries.length === 0) return 'No transcript data available for minutes.';

  try {
    const instructionsPath = path.join(__dirname, 'minutesFormatInstructions.txt');
    const instructions = fs.readFileSync(instructionsPath, 'utf-8');

    const now = new Date();
    const dateStr = now.toLocaleDateString('en-US', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
    });
    const timeStr = now.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    const transcriptText = entries
      .map(e => `${e.speaker} (${formatVttTimestamp(e.timestamp)}): ${e.text}`)
      .join('\n');

    const prompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content:
            `Meeting Title: ${meetingTitle}\n` +
            `Date: ${dateStr}\n` +
            `Time: ${timeStr}\n` +
            `Attendees: ${members.join(', ')}\n\n` +
            `Transcript:\n${transcriptText}\n\n` +
            `Generate formal meeting minutes with all required sections in clean Markdown format. DO NOT use HTML.`
        },
      ],
      instructions: instructions,
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    const response = await prompt.send('');
    return response.content || 'Could not generate meeting minutes. Please try again.';
  } catch (error) {
    console.error(`[MINUTES_FORMAT_ERROR]`, error);
    return 'Error generating meeting minutes. Please try again.';
  }
}

type IntentLabel =
  | 'join_meeting'
  | 'summarize'
  | 'minutes'
  | 'transcribe'
  | 'meeting_overview'
  | 'insights'
  | 'meeting_question'
  | 'send_email'
  | 'check_calendar'
  | 'general_chat';

async function classifyIntent(message: string, isMeetingConversation: boolean): Promise<IntentLabel> {
  const text = (message || '').trim();
  if (!text) return 'general_chat';

  // Get current date context for smarter calendar detection
  const now = new Date();
  const dateContext = `Current date: ${now.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })}`;

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `You are a smart intent classifier for a Teams meeting assistant bot. Analyze the user's message and determine their intent.\n\n` +
          `User message: "${text}"\n` +
          `Is this a meeting conversation: ${isMeetingConversation ? 'Yes' : 'No'}\n` +
          `${dateContext}\n\n` +
          `**Available intents:**\n` +
          `1. **join_meeting** - User wants the bot to join an ongoing call/meeting (e.g., "join the call", "come to the meeting", "join us")\n` +
          `2. **summarize** - User wants a summary/recap of content (e.g., "summarize this", "give me a recap", "what was discussed", "summarize and email me")\n` +
          `3. **minutes** - User wants formal meeting notes/minutes (e.g., "create minutes", "meeting notes", "action items", "send minutes to email")\n` +
          `4. **transcribe** - User wants a transcript (e.g., "transcribe the meeting", "get the transcript", "email the transcript")\n` +
          `5. **meeting_overview** - User asks about a specific meeting's details (e.g., "tell me about the meeting", "what happened in my last meeting")\n` +
          `6. **insights** - User wants key insights/highlights (e.g., "key takeaways", "main points", "highlights")\n` +
          `7. **meeting_question** - User asks a specific question about meeting content (e.g., "what did John say about X", "when did we discuss Y")\n` +
          `8. **send_email** - User wants to compose/send a generic email NOT related to meetings (e.g., "send an email to Bob about the project")\n` +
          `9. **check_calendar** - User asks about their schedule, meetings, availability, or calendar:\n` +
          `   - "what meetings do I have today/tomorrow/this week"\n` +
          `   - "am I free at 3pm"\n` +
          `   - "check my calendar"\n` +
          `   - "do I have any meetings"\n` +
          `   - "what's on my schedule"\n` +
          `   - "meetings today"\n` +
          `   - "am I available"\n` +
          `   - "when is my next meeting"\n` +
          `10. **general_chat** - Casual conversation, greetings, or anything that doesn't fit above\n\n` +
          `**Important rules:**\n` +
          `- For compound requests like "summarize and send to email", choose the PRIMARY content action (summarize/minutes/transcribe)\n` +
          `- Questions about calendar/schedule/availability should ALWAYS be check_calendar\n` +
          `- "meetings today", "my meetings", "do I have meetings" = check_calendar\n` +
          `- Greetings like "hi", "hello", "good morning" = general_chat\n\n` +
          `Respond with JSON only: {"intent": "<one_intent_label>", "confidence": "high|medium|low", "reasoning": "<brief explanation>"}`,
      },
    ],
    instructions: 'You are a precise intent classifier. Think carefully about what the user REALLY wants. Output valid JSON with intent, confidence, and reasoning fields.',
    model: new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    }),
  });

  try {
    const response = await prompt.send('');
    const raw = (response.content || '').trim();
    // Handle potential markdown code blocks
    const jsonStr = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    const parsed = JSON.parse(jsonStr);
    const intent = parsed?.intent as IntentLabel;
    const confidence = parsed?.confidence || 'medium';
    const reasoning = parsed?.reasoning || '';
    
    console.log(`[INTENT] LLM classified: "${intent}" (${confidence}) - ${reasoning}`);
    
    const allowed: IntentLabel[] = [
      'join_meeting',
      'summarize',
      'minutes',
      'transcribe',
      'meeting_overview',
      'insights',
      'meeting_question',
      'send_email',
      'check_calendar',
      'general_chat',
    ];
    if (allowed.includes(intent)) {
      return intent;
    }
    console.warn(`[INTENT] LLM returned unknown intent: ${intent}, defaulting to general_chat`);
  } catch (error) {
    console.warn(`[INTENT] LLM classification failed, defaulting to general_chat:`, error);
  }

  return 'general_chat';
}

function buildConversationContext(sharedMessages: any[], maxItems = 120): string {
  const items = (sharedMessages || []).slice(-maxItems);
  if (!items.length) return 'No conversation messages available.';

  const lines = items.map((msg: any) => {
    const user = (msg?.user || 'Unknown').toString().trim();
    const content = (msg?.content || '').toString().replace(/<[^>]*>/g, '').trim();
    const timestamp = msg?.timestamp ? new Date(msg.timestamp).toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' }) : '';
    return content ? `${user}${timestamp ? ` (${timestamp})` : ''}: ${content}` : '';
  }).filter(Boolean);

  return lines.length ? lines.join('\n') : 'No conversation messages available.';
}

async function getTranscriptTextForConversation(conversationId: string): Promise<string> {
  // ALWAYS try Graph first for the most up-to-date transcript
  console.log(`[TRANSCRIPT_FETCH] Trying Graph API first for conversation ${conversationId}`);
  const meetingInfo = await resolveMeetingInfoForConversation(conversationId);
  if (meetingInfo?.organizer?.id && meetingInfo?.joinWebUrl) {
    const transcriptWindow = getTranscriptWindowForConversation(conversationId);
    if (transcriptWindow.min || transcriptWindow.max) {
      console.log(`[TRANSCRIPT_FETCH] Using conversation call window filter for ${conversationId}`);
    }
    const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
      meetingInfo.organizer.id,
      meetingInfo.joinWebUrl,
      transcriptWindow.min,
      transcriptWindow.max
    );
    if (vttContent) {
      const parsed = parseVttToEntries(vttContent);
      if (parsed.length > 0) {
        console.log(`[TRANSCRIPT_FETCH] Graph API returned ${parsed.length} entries - using fresh data`);
        liveTranscriptMap.set(conversationId, parsed);
        saveTranscriptToFile(conversationId);
        return transcriptEntriesToPlainText(parsed);
      }
    }
  }
  console.log(`[TRANSCRIPT_FETCH] Graph API returned no transcript - falling back to cache`);

  // Fallback to in-memory cache
  const cachedEntries = liveTranscriptMap.get(conversationId);
  const cachedFinalEntries = cachedEntries?.filter((e) => e.isFinal) || [];
  if (cachedFinalEntries.length > 0) {
    console.log(`[TRANSCRIPT_FETCH] Using in-memory cache (${cachedFinalEntries.length} entries)`);
    return transcriptEntriesToPlainText(cachedFinalEntries);
  }

  // Final fallback to file cache
  const cachedFile = loadCachedTranscriptText(conversationId);
  if (cachedFile) {
    console.log(`[TRANSCRIPT_FETCH] Using file cache`);
    return cachedFile;
  }

  console.log(`[TRANSCRIPT_FETCH] No transcript data available from any source`);
  return '';
}

async function answerMeetingQuestionWithContext(
  question: string,
  meetingTitle: string,
  conversationContext: string,
  transcriptContext: string
): Promise<string> {
  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `User question: ${question}\n\n` +
          `Meeting title: ${meetingTitle}\n\n` +
          `Conversation Context:\n${conversationContext}\n\n` +
          `Transcript Context:\n${transcriptContext || 'No transcript available.'}\n\n` +
          `Respond with these exact sections:\n` +
          `## Conversation Context\n` +
          `## Transcript Context\n` +
          `## Answer\n` +
          `## Detailed Summary\n\n` +
          `Use detailed summary in bullets and call out uncertainties clearly if data is missing.`
      },
    ],
    instructions:
      'You are a meeting analyst assistant. Always ground the answer in provided context and separate sections exactly as requested.',
    model: new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    }),
  });

  const response = await prompt.send('');
  return response.content || 'I could not generate a meeting-context answer.';
}

// Auto-retry join: store pending join info so we can retry when meeting becomes active
interface PendingJoin {
  conversationId: string;
  serviceUrl: string;
  callbackUri: string;
  tenantId: string;
  retryCount: number;
  maxRetries: number;
  timerId?: ReturnType<typeof setTimeout>;
}
const pendingJoinMap = new Map<string, PendingJoin>();

const RETRY_INTERVAL_MS = 15_000;  // 15 seconds between retries
const MAX_RETRIES = 20;            // 20 retries � 15s = 5 minutes

interface TranscriptionStartupState {
  attemptCount: number;
  maxAttempts: number;
  inProgress: boolean;
  started: boolean;
  timerId?: ReturnType<typeof setTimeout>;
  failureNotified: boolean;
}

const transcriptionStartupMap = new Map<string, TranscriptionStartupState>();
const TRANSCRIPTION_RETRY_DELAYS_MS = [0, 5_000, 10_000, 20_000, 30_000, 45_000];

interface LiveTranscriptPollingState {
  callId: string;
  organizerId: string;
  joinWebUrl: string;
  conversationId: string;
  serviceUrl: string;
  callStartTime: number;
  pollingTimerId?: ReturnType<typeof setTimeout>;
  lastFetchedLineCount: number;
  consecutiveEmptyPolls: number;
  userNotifiedAboutDelay: boolean;
}

const liveTranscriptPollingMap = new Map<string, LiveTranscriptPollingState>();
const LIVE_TRANSCRIPT_POLL_INTERVAL_MS = 10_000; // 10 seconds

function clearPendingTranscriptionStart(callId: string) {
  const state = transcriptionStartupMap.get(callId);
  if (state?.timerId) {
    clearTimeout(state.timerId);
  }
  transcriptionStartupMap.delete(callId);
}

async function attemptAutoStartTranscription(
  callId: string,
  source: string,
  callEntryOverride?: ActiveCall
) {
  const callEntry = callEntryOverride || activeCallMap.get(callId);
  if (!callEntry) {
    clearPendingTranscriptionStart(callId);
    return;
  }

  let state = transcriptionStartupMap.get(callId);
  if (!state) {
    state = {
      attemptCount: 0,
      maxAttempts: TRANSCRIPTION_RETRY_DELAYS_MS.length,
      inProgress: false,
      started: false,
      failureNotified: false,
    };
    transcriptionStartupMap.set(callId, state);
  }

  if (state.started || state.inProgress || state.timerId) {
    return;
  }

  if (state.attemptCount >= state.maxAttempts) {
    if (!state.failureNotified) {
      state.failureNotified = true;
      await graphApiHelper.sendProactiveMessage(
        callEntry.serviceUrl,
        callEntry.conversationId,
        `?? **I couldn't auto-start Teams transcription.**\n\nPlease ensure:\n� Meeting policy allows transcription\n� The bot app has **Calls.AccessMedia.All** with admin consent\n\nYou can still start transcription manually in Teams if needed.`
      );
    }
    return;
  }

  state.inProgress = true;
  state.attemptCount++;
  const attemptNumber = state.attemptCount;
  console.log(`[TRANSCRIPTION_AUTO] Attempt ${attemptNumber}/${state.maxAttempts} for callId=${callId} (source=${source})`);

  try {
    const started = await graphApiHelper.startTranscription(callId);
    if (started) {
      state.started = true;
      state.inProgress = false;
      clearPendingTranscriptionStart(callId);
      // Removed notification - no need to spam user about transcription status
      return;
    }
  } catch (error) {
    console.warn(`[TRANSCRIPTION_AUTO] startTranscription threw for callId=${callId}:`, error);
  }

  state.inProgress = false;
  if (state.attemptCount >= state.maxAttempts) {
    if (!state.failureNotified) {
      state.failureNotified = true;
      await graphApiHelper.sendProactiveMessage(
        callEntry.serviceUrl,
        callEntry.conversationId,
        `?? **I couldn't auto-start Teams transcription.**\n\nPlease ensure:\n� Meeting policy allows transcription\n� The bot app has **Calls.AccessMedia.All** with admin consent\n\nYou can still start transcription manually in Teams if needed.`
      );
    }
    return;
  }

  const nextDelay = TRANSCRIPTION_RETRY_DELAYS_MS[Math.min(state.attemptCount, TRANSCRIPTION_RETRY_DELAYS_MS.length - 1)];
  console.log(`[TRANSCRIPTION_AUTO] Scheduling retry in ${nextDelay / 1000}s for callId=${callId}`);
  state.timerId = setTimeout(() => {
    const latestState = transcriptionStartupMap.get(callId);
    if (latestState) {
      latestState.timerId = undefined;
    }
    void attemptAutoStartTranscription(callId, 'retry-timer');
  }, nextDelay);
}

function stopLiveTranscriptPolling(callId: string) {
  const polling = liveTranscriptPollingMap.get(callId);
  if (polling?.pollingTimerId) {
    clearTimeout(polling.pollingTimerId);
  }
  liveTranscriptPollingMap.delete(callId);
}

async function pollLiveTranscript(state: LiveTranscriptPollingState) {
  try {
    const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
      state.organizerId,
      state.joinWebUrl,
      state.callStartTime
    );
    if (!vttContent) {
      state.consecutiveEmptyPolls++;
      console.log(`[LIVE_TRANSCRIPT_POLL] No transcript data available yet for callId=${state.callId} (empty polls: ${state.consecutiveEmptyPolls})`);
      
      // After 3 attempts (30 seconds), just note the delay silently
      if (state.consecutiveEmptyPolls === 3 && !state.userNotifiedAboutDelay) {
        state.userNotifiedAboutDelay = true;
        // Removed notification - no need to spam user about delays
      }
    } else {
      // Parse the full VTT content
      const allEntries = parseVttToEntries(vttContent);
      const convEntries = liveTranscriptMap.get(state.conversationId) || [];
      
      // Check if we got new entries since last poll
      if (allEntries.length > state.lastFetchedLineCount) {
        const newEntries = allEntries.slice(state.lastFetchedLineCount);
        console.log(`[LIVE_TRANSCRIPT_POLL] Got ${newEntries.length} new entries (total now: ${allEntries.length})`);
        
        // First time getting data after empty polls? Notify user
        if (state.lastFetchedLineCount === 0 && state.consecutiveEmptyPolls > 0) {
          await graphApiHelper.sendProactiveMessage(
            state.serviceUrl,
            state.conversationId,
            `? **Live transcript is now flowing!** Captured ${allEntries.length} entries so far.`
          );
        }
        
        convEntries.push(...newEntries);
        liveTranscriptMap.set(state.conversationId, convEntries);
        state.lastFetchedLineCount = allEntries.length;
        state.consecutiveEmptyPolls = 0; // Reset counter
        
        // Save updated transcript to file
        saveTranscriptToFile(state.conversationId);
      } else {
        console.log(`[LIVE_TRANSCRIPT_POLL] No new entries since last poll (current: ${allEntries.length})`);
      }
    }
  } catch (error) {
    console.warn(`[LIVE_TRANSCRIPT_POLL_ERROR] Error polling transcript for callId=${state.callId}:`, error);
  }

  // Schedule next poll
  if (liveTranscriptPollingMap.has(state.callId)) {
    state.pollingTimerId = setTimeout(() => {
      void pollLiveTranscript(state);
    }, LIVE_TRANSCRIPT_POLL_INTERVAL_MS);
  }
}

function startLiveTranscriptPolling(
  callId: string,
  organizerId: string,
  joinWebUrl: string,
  conversationId: string,
  serviceUrl: string,
  callStartTime: number
) {
  stopLiveTranscriptPolling(callId); // Clear any existing polling
  
  const state: LiveTranscriptPollingState = {
    callId,
    organizerId,
    joinWebUrl,
    conversationId,
    serviceUrl,
    callStartTime,
    lastFetchedLineCount: 0,
    consecutiveEmptyPolls: 0,
    userNotifiedAboutDelay: false,
  };

  liveTranscriptPollingMap.set(callId, state);
  console.log(`[LIVE_TRANSCRIPT_POLL] Starting live transcript polling for callId=${callId}, interval=${LIVE_TRANSCRIPT_POLL_INTERVAL_MS / 1000}s`);
  
  // Start polling immediately, then every 10s
  void pollLiveTranscript(state);
}

/** Schedule a retry to join the meeting. Called after 2203 (meeting not active yet). */
async function scheduleJoinRetry(conversationId: string) {
  const pending = pendingJoinMap.get(conversationId);
  if (!pending) return;

  pending.retryCount++;
  if (pending.retryCount > pending.maxRetries) {
    console.log(`[JOIN_RETRY] Max retries (${pending.maxRetries}) reached for ${conversationId} � giving up`);
    pendingJoinMap.delete(conversationId);
    await graphApiHelper.sendProactiveMessage(
      pending.serviceUrl,
      conversationId,
      `?? **I gave up waiting to join the meeting.**\n\nI retried for 5 minutes but the call never became active. Please start the meeting and ask me to join again.`
    );
    return;
  }

  console.log(`[JOIN_RETRY] Retry ${pending.retryCount}/${pending.maxRetries} for ${conversationId} in ${RETRY_INTERVAL_MS / 1000}s...`);
  pending.timerId = setTimeout(async () => {
    try {
      const meetingInfo = await resolveMeetingInfoForConversation(conversationId);
      if (!meetingInfo?.organizer?.id) {
        console.warn(`[JOIN_RETRY] No meeting info on retry � scheduling next`);
        await scheduleJoinRetry(conversationId);
        return;
      }

      const callResult = await graphApiHelper.joinMeetingCall(
        meetingInfo,
        pending.callbackUri,
        pending.tenantId,
        conversationId
      );

      if (callResult) {
        console.log(`[JOIN_RETRY] Join call placed, callId=${callResult.id} � waiting for webhook`);
        activeCallMap.set(callResult.id, {
          conversationId,
          serviceUrl: pending.serviceUrl,
          organizerId: meetingInfo.organizer?.id,
          joinWebUrl: meetingInfo.joinWebUrl,
        });
        // Don't delete from pendingJoinMap yet � webhook will do it on 'established' or final failure
      } else {
        console.warn(`[JOIN_RETRY] joinMeetingCall returned null � scheduling next`);
        await scheduleJoinRetry(conversationId);
      }
    } catch (err) {
      console.error(`[JOIN_RETRY_ERROR]`, err);
      await scheduleJoinRetry(conversationId);
    }
  }, RETRY_INTERVAL_MS);
}

/** Cancel any pending retry for a conversation. */
function cancelPendingJoin(conversationId: string) {
  const pending = pendingJoinMap.get(conversationId);
  if (pending?.timerId) clearTimeout(pending.timerId);
  pendingJoinMap.delete(conversationId);
}

// Load instructions from file on initialization
function loadInstructions(): string {
  const instructionsFilePath = path.join(__dirname, "instructions.txt");
  return fs.readFileSync(instructionsFilePath, 'utf-8').trim();
}

// Load instructions once at startup
const instructions = loadInstructions();

function normalizeDisplayName(name?: string): string {
  const normalized = (name || '').trim();
  if (!normalized) return '';

  const lowered = normalized.toLowerCase();
  if (lowered === 'team member' || lowered === 'user' || lowered === 'unknown') {
    return '';
  }

  return normalized;
}

function extractFirstName(fullName: string): string {
  const normalized = fullName.trim();
  if (!normalized) return '';

  // Extract first word as first name
  const parts = normalized.split(/\s+/);
  return parts[0] || normalized;
}

function getCurrentDateTimeContext(): string {
  const now = new Date();
  const dayOfWeek = now.toLocaleDateString('en-US', { weekday: 'long' });
  const dateStr = now.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  const timeStr = now.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
  const hour = now.getHours();

  let timeOfDay = 'day';
  if (hour < 12) timeOfDay = 'morning';
  else if (hour < 17) timeOfDay = 'afternoon';
  else timeOfDay = 'evening';

  return `Current date: ${dayOfWeek}, ${dateStr}. Current time: ${timeStr} (${timeOfDay}).`;
}

// Helper to detect if user wants to email results and extract email address
function detectEmailRequest(message: string): { wantsEmail: boolean; emailAddress: string | null } {
  const lower = (message || '').toLowerCase();
  const wantsEmail = 
    (lower.includes('send') && lower.includes('email')) ||
    (lower.includes('email') && (lower.includes('to') || lower.includes('me') || lower.includes('my'))) ||
    lower.includes('send it to') ||
    lower.includes('send to my') ||
    lower.includes('mail it') ||
    lower.includes('send this to');
  
  // Extract email address from message
  const emailMatch = message.match(/[\w.-]+@[\w.-]+\.\w+/i);
  const emailAddress = emailMatch ? emailMatch[0] : null;
  
  return { wantsEmail, emailAddress };
}

// Helper function to get user's display name for personalization
async function getUserDisplayName(userId: string, fallbackName?: string): Promise<string> {
  const safeFallbackName = normalizeDisplayName(fallbackName);
  try {
    const userInfo = await graphApiHelper.getUserInfo(userId);
    const graphName = normalizeDisplayName(userInfo?.displayName);
    const fullName = graphName || safeFallbackName || 'there';
    // Extract first name only for personalization
    return fullName === 'there' ? fullName : extractFirstName(fullName);
  } catch (error) {
    console.warn('Using default name for user:', error);
    const fullName = safeFallbackName || 'there';
    return fullName === 'there' ? fullName : extractFirstName(fullName);
  }
}

const createTokenFactory = () => {
  if (config.MicrosoftAppType === "UserAssignedMsi") {
    // Managed identity flow (legacy)
    return async (scope: string | string[], tenantId?: string): Promise<string> => {
      const managedIdentityCredential = new ManagedIdentityCredential({
          clientId: process.env.CLIENT_ID
        });
      const scopes = Array.isArray(scope) ? scope : [scope];
      const tokenResponse = await managedIdentityCredential.getToken(scopes, {
        tenantId: tenantId
      });
      return tokenResponse.token;
    };
  }
  // Client credentials flow (SingleTenant)
  return async (scope: string | string[], tenantId?: string): Promise<string> => {
    const clientId = process.env.CLIENT_ID;
    const clientSecret = process.env.CLIENT_SECRET;
    const tid = tenantId || process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;
    if (!clientId || !clientSecret || !tid) return '';
    const form = new URLSearchParams();
    form.append('client_id', clientId);
    form.append('client_secret', clientSecret);
    form.append('scope', Array.isArray(scope) ? scope.join(' ') : scope);
    form.append('grant_type', 'client_credentials');
    const { default: axios } = await import('axios');
    const response = await axios.post(
      `https://login.microsoftonline.com/${tid}/oauth2/v2.0/token`,
      form.toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );
    return response.data.access_token;
  };
};

// Configure authentication
const credentialOptions: Record<string, any> = {
  clientId: process.env.CLIENT_ID || '',
  tenantId: process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID,
};

if (config.MicrosoftAppType === "UserAssignedMsi") {
  credentialOptions.token = createTokenFactory();
} else if (process.env.CLIENT_SECRET) {
  credentialOptions.clientSecret = process.env.CLIENT_SECRET;
}

// Create the app with storage
const app = new App({
  ...credentialOptions,
  storage
});

// Set the token factory for GraphApiHelper (works for both MSI and SingleTenant)
{
  const graphTokenFactory = async (): Promise<string> => {
    const scopeFactory = createTokenFactory();
    return scopeFactory('https://graph.microsoft.com/.default');
  };
  graphApiHelper.setTokenFactory(graphTokenFactory);
}

// Helper to send typing indicator
async function sendTypingIndicator(sendFn: any): Promise<void> {
  try {
    await sendFn({ type: 'typing' });
  } catch (error) {
    // Typing indicator failures shouldn't break the flow
    console.warn('[TYPING] Failed to send typing indicator:', error);
  }
}

// Handle incoming messages
app.on('message', async ({ send, stream, activity }) => {
  //Get conversation history
  const conversationKey = `${activity.conversation.id}/${activity.from.id}`;
  const sharedConversationKey = `conversation/${activity.conversation.id}`;
  const llmConversationKey = `llm/${activity.conversation.id}`;

  let messages = storage.get(conversationKey) || [];
  let sharedMessages = storage.get(sharedConversationKey) || [];
  let llmMessages = storage.get(llmConversationKey) || [];

  console.log(`[MESSAGE] Received from ${activity.from.id}: ${activity.text}`);
  console.log(`[CONTEXT] Conversation: ${activity.conversation.id}, Is Group: ${activity.conversation.isGroup}`);
  console.log(`[STORAGE] Loaded ${messages.length} user messages, ${sharedMessages.length} shared messages, ${llmMessages.length} llm messages`);

  try {
    // Get user's display name for personalization
    const userName = await getUserDisplayName(activity.from.id, activity.from?.name);
    const actorName = activity.from?.name || userName;
    console.log(`[USER] Display name resolved: ${userName}`);
    
    // Store the current message in history
    if (activity.text) {
      const messageEntry = {
        timestamp: new Date().toISOString(),
        user: actorName,
        content: activity.text,
      };
      messages.push(messageEntry);
      sharedMessages.push(messageEntry);

      llmMessages.push({
        role: 'user',
        content: activity.text,
      });

      if (llmMessages.length > 30) {
        llmMessages = llmMessages.slice(-30);
      }

      console.log(`[STORAGE] Added message to history (total: ${messages.length})`);
    }
    
    const dateTimeContext = getCurrentDateTimeContext();
    const personalizedInstructions =
      `${instructions}\n\n` +
      `${dateTimeContext}\n\n` +
      `You are speaking with ${userName}.\n` +
      `Address the user by their first name naturally when appropriate (for example: \"Hello, ${userName}. Happy Friday!\" or \"Hi ${userName},\").\n` +
      `Keep responses well-formatted with short sections, concise bullet points when useful, and clear next steps.`;

    const isGroupConversation = activity.conversation.isGroup;
    const botMentioned = isBotMentioned(activity);
    const cleanText = removeAtMentions(activity.text || '');
    const userMessage = cleanText.toLowerCase();
    const conversationIdLower = (activity.conversation.id || '').toLowerCase();
    const isMeetingConversation =
      activity.conversation.isGroup &&
      (conversationIdLower.includes('meeting_') ||
       conversationIdLower.includes('meeting') ||
       conversationIdLower.includes('spaces'));
    const detectedIntent = await classifyIntent(cleanText, isMeetingConversation);
    console.log(`[INTENT] Detected intent: ${detectedIntent}`);
    const meetingAutoJoinKey = `meeting-autojoin/${activity.conversation.id}`;
    const hasAutoJoinedMeeting = storage.get(meetingAutoJoinKey) === true;

    if (
      isMeetingConversation &&
      !hasAutoJoinedMeeting &&
      (userMessage.includes('meeting started') || userMessage.includes('meeting has started'))
    ) {
      console.log(`[MEETING_AUTOJOIN] Detected meeting start message. Sending automatic greeting.`);

      const greetingActivity = new MessageActivity(
        `Hello, **Mela AI Meeting Assistant** auto-joined this meeting chat.\n\n` +
        `I can help you with:\n` +
        `- **Summarize** - Get a quick recap of the chat\n` +
        `- **Minutes** - Generate meeting minutes\n` +
        `- **Transcribe** - Get transcript when recording is available`
      ).addAiGenerated().addFeedback();

      await send(greetingActivity);
      storage.set(meetingAutoJoinKey, true);
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle summarization commands
    if (detectedIntent === 'summarize' || userMessage.includes('summarize') || userMessage.includes('summary')) {
      console.log(`[ACTION] Processing summarization request`);
      await sendTypingIndicator(send);
      
      // Check if user also wants to email the result
      const emailRequest = detectEmailRequest(cleanText || activity.text || '');
      let generatedSummary = '';
      
      try {
        console.log(`[DEBUG] Processing summarization for user`);
        
        // Use LLM to understand WHAT the user wants to summarize
        const targetExtractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Analyze this summarization request to determine what source the user wants summarized.

User request: "${cleanText || activity.text}"

Determine:
1. Is the user asking about the CURRENT conversation/meeting, or a DIFFERENT/SPECIFIC chat/group/meeting?
2. If a specific chat/group, extract the name/identifier

Respond with JSON only:
{
  "target": "current" | "specific",
  "chat_name": "name of the specific chat/group if mentioned, or null",
  "content_type": "meeting" | "chat" | "transcript" | "any"
}`
            }
          ],
          instructions: 'You are analyzing summarization requests. If the user mentions a specific group name, chat name, or meeting title (like "test agent v2 group", "the marketing chat", "yesterday\'s standup"), return target="specific" with the chat_name. If they just say "summarize" or "summarize this", return target="current". Output valid JSON only.',
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const targetResponse = await targetExtractPrompt.send('');
        const jsonStr = (targetResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        let targetInfo = { target: 'current', chat_name: null, content_type: 'any' };
        try {
          targetInfo = JSON.parse(jsonStr);
        } catch {
          console.warn(`[SUMMARIZE] Could not parse target extraction, defaulting to current conversation`);
        }
        
        console.log(`[SUMMARIZE] Target analysis: ${JSON.stringify(targetInfo)}`);
        
        // Determine which chat to summarize
        let targetChatId = activity.conversation.id;
        let targetChatName = 'this conversation';
        
        if (targetInfo.target === 'specific' && targetInfo.chat_name) {
          // User wants to summarize a specific chat - search for it
          console.log(`[SUMMARIZE] Searching for chat: "${targetInfo.chat_name}"`);
          const userId = activity.from.aadObjectId || activity.from.id;
          const matchingChats = await graphApiHelper.getUserChats(userId, targetInfo.chat_name, 20);
          
          if (matchingChats.length > 0) {
            // Use the most recently updated matching chat
            targetChatId = matchingChats[0].id;
            targetChatName = matchingChats[0].topic || targetInfo.chat_name;
            console.log(`[SUMMARIZE] Found matching chat: "${targetChatName}" (${targetChatId})`);
          } else {
            // No matching chat found
            await send(new MessageActivity(
              `I couldn't find a chat or group named "${targetInfo.chat_name}". Please check the name and try again, or ask me to summarize from the current conversation.`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }

        // First, try to get meeting transcript for summary (only for current conversation)
        const liveEntries = targetInfo.target === 'current' ? liveTranscriptMap.get(activity.conversation.id) : undefined;
        const transcriptEntries = liveEntries?.filter(e => e.isFinal) || [];

        if (transcriptEntries.length > 0) {
          console.log(`[SUMMARIZE] Found ${transcriptEntries.length} transcript entries, generating AI summary...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
          const meetingTitle = chatInfo?.subject || 'Meeting';
          const memberList = chatMembers.length > 0 ? chatMembers : [];

          generatedSummary = await generateFormattedSummaryHtml(
            transcriptEntries, 
            meetingTitle, 
            userName,
            memberList
          );
          console.log(`[SUMMARIZE] Summary generated successfully from transcript`);

          const responseActivity = new MessageActivity(generatedSummary).addAiGenerated().addFeedback();
          await send(responseActivity);
          
          // Track for email follow-up
          lastBotResponseMap.set(activity.conversation.id, {
            content: generatedSummary,
            contentType: 'summary',
            subject: `Summary: ${meetingTitle}`,
            timestamp: Date.now()
          });
          
          console.log(`[SUCCESS] Transcript-based summary sent to user`);
        } else {
          // Fetch chat messages from the target chat
          console.log(`[GRAPH] Fetching chat messages for: ${targetChatId}`);
          let chatMessages = await graphApiHelper.getChatMessages(targetChatId, 50);
          console.log(`[GRAPH] Retrieved ${chatMessages.length} messages`);

          // Fallback: if Graph API returns no messages and it's current conversation, use stored history
          if (chatMessages.length === 0 && targetInfo.target === 'current') {
            console.log(`[FALLBACK] Graph API returned no messages, using stored conversation history`);
            const storedSharedMessages = storage.get(sharedConversationKey) || [];

            const filteredMessages = storedSharedMessages.filter((msg: any) => {
              const content = (msg?.content || '').toLowerCase();
              return !content.includes('summarize') &&
                     !content.includes('summary') &&
                     !content.includes('minutes') &&
                     !content.includes('meeting notes') &&
                     !content.includes('transcribe') &&
                     !content.includes('transcript') &&
                     !content.includes('join the meeting');
            });

            if (filteredMessages.length > 0) {
              // Convert stored messages to ChatMessage format
              chatMessages = filteredMessages.map((msg: any, index: number) => {
                const content = typeof msg === 'string' 
                  ? msg 
                  : (msg.content || (typeof msg === 'object' ? JSON.stringify(msg) : String(msg)));
                
                const displayName = (msg.user && typeof msg.user === 'string') 
                  ? msg.user 
                  : userName;

                return {
                  id: `msg_${index}`,
                  from: {
                    user: {
                      id: activity.from.id,
                      displayName: displayName,
                    },
                  },
                  body: {
                    content: content,
                  },
                  createdDateTime: msg.timestamp || new Date().toISOString(),
                };
              });
              console.log(`[FALLBACK] Using ${chatMessages.length} shared stored messages from conversation history`);
            } else {
              console.log(`[INFO] No stored messages available for fallback`);
            }
          }

          if (chatMessages.length === 0) {
            const noContentMsg = targetInfo.target === 'specific'
              ? `I couldn't retrieve messages from "${targetChatName}". This might be a permissions issue or the chat has no recent messages.`
              : `I don't have enough content yet to summarize.\n\n` +
                `If this is a meeting, ask me to join or request a transcript first. ` +
                `If this is a chat, mention me in a few messages or grant Microsoft Graph permissions (Chat.Read.All / ChatMessage.Read.All).`;
            
            const responseActivity = new MessageActivity(noContentMsg).addAiGenerated().addFeedback();
            await send(responseActivity);
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }

          console.log(`[SUMMARIZE] Generating chat summary for "${targetChatName}"...`);
          generatedSummary = await summarizationHelper.summarizeChatMessages(chatMessages, 'detailed');
          console.log(`[SUMMARIZE] Summary generated successfully`);

          const summaryHeader = targetInfo.target === 'specific' 
            ? `**Summary of "${targetChatName}":**\n\n`
            : `**Chat Summary for ${userName}:**\n\n`;

          const responseActivity = new MessageActivity(
            `${summaryHeader}${generatedSummary}`
          ).addAiGenerated().addFeedback();
          await send(responseActivity);
          
          // Track for email follow-up
          lastBotResponseMap.set(activity.conversation.id, {
            content: `${summaryHeader}${generatedSummary}`,
            contentType: 'summary',
            subject: targetInfo.target === 'specific' ? `Summary: ${targetChatName}` : 'Chat Summary',
            timestamp: Date.now()
          });
          
          console.log(`[SUCCESS] Chat summary sent to user`);
        }
        
        // If user requested email, send it now
        if (emailRequest.wantsEmail && generatedSummary) {
          console.log(`[EMAIL] User requested summary via email`);
          let recipientEmail = emailRequest.emailAddress;
          
          // If no email in message, get user's email
          if (!recipientEmail) {
            const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
            recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
          }
          
          if (recipientEmail) {
            const emailResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmail,
              'Meeting Summary from Mela AI Meeting Assistant',
              generatedSummary
            );
            
            if (emailResult.success) {
              await send(new MessageActivity(`?? I've also sent the summary to **${recipientEmail}**.`).addAiGenerated());
            } else {
              await send(new MessageActivity(`I couldn't send the email: ${emailResult.error}`).addAiGenerated());
            }
          } else {
            await send(new MessageActivity(`I couldn't determine your email address. Please specify an email address.`).addAiGenerated());
          }
        }
      } catch (error) {
        console.error(`[ERROR_SUMMARIZE] Failed to summarize:`, error);
        const errorResponse = new MessageActivity(
          'I encountered an error while generating a summary. Please try again.'
        ).addAiGenerated();
        await send(errorResponse);
      }
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle meeting overview search (searches transcripts, not chat)
    if (detectedIntent === 'meeting_overview' || userMessage.toLowerCase().includes('tell me about') || userMessage.toLowerCase().includes('meeting overview')) {
      console.log(`[ACTION] Processing meeting overview request`);
      await sendTypingIndicator(send);
      try {
        console.log(`[DEBUG] Searching for meeting transcript`);

        // First, try to get meeting transcript from local storage
        let transcriptEntries = liveTranscriptMap.get(activity.conversation.id);
        const finalEntries = transcriptEntries?.filter(e => e.isFinal) || [];

        // If no local transcript, fetch from Graph API
        if (finalEntries.length === 0) {
          console.log(`[GRAPH] No local transcript, fetching from Graph API...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          if (chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
            const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
              chatInfo.organizer.id,
              chatInfo.joinWebUrl
            );
            if (vttContent) {
              transcriptEntries = parseVttToEntries(vttContent);
              console.log(`[GRAPH] Fetched ${transcriptEntries.length} transcript entries from Graph`);
            }
          }
        } else {
          console.log(`[LOCAL] Using ${finalEntries.length} local transcript entries`);
          transcriptEntries = finalEntries;
        }

        if (!transcriptEntries || transcriptEntries.length === 0) {
          const responseActivity = new MessageActivity(
            `I don't have any meeting transcript data yet.\n\n` +
            `You can:\n` +
            `� Ask me to join an upcoming meeting\n` +
            `� Share a meeting you already attended\n` +
            `� Request a transcript if a meeting was recorded`
          ).addAiGenerated().addFeedback();
          await send(responseActivity);
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        console.log(`[MEETING_OVERVIEW] Generating overview from ${transcriptEntries.length} entries...`);
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
        const meetingTitle = chatInfo?.subject || 'Meeting';
        const memberList = chatMembers.length > 0 ? chatMembers : [];

        const overviewHtml = await generateFormattedSummaryHtml(
          transcriptEntries,
          meetingTitle,
          userName,
          memberList
        );
        console.log(`[MEETING_OVERVIEW] Overview generated successfully`);

        const responseActivity = new MessageActivity(overviewHtml).addAiGenerated().addFeedback();
        await send(responseActivity);
        console.log(`[SUCCESS] Meeting overview sent to user`);
      } catch (error) {
        console.error(`[ERROR_MEETING_OVERVIEW] Failed to get meeting overview:`, error);
        const errorResponse = new MessageActivity(
          'I encountered an error while retrieving the meeting overview. Please try again.'
        ).addAiGenerated();
        await send(errorResponse);
      }
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle transcription requests
    if (detectedIntent === 'transcribe' || userMessage.includes('transcribe') || userMessage.includes('transcript')) {
      console.log(`[ACTION] Processing transcription request`);
      await sendTypingIndicator(send);
      try {
        console.log(`[TRANSCRIBE_DEBUG] Step 1: Sending initial acknowledgment`);
        await send(new MessageActivity(`? **Working on it... checking transcript sources for this channel.**`).addAiGenerated());
        console.log(`[TRANSCRIBE_DEBUG] Step 2: Resolving meeting info`);

        // Fetch meeting metadata (title + members) for header
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 3: Got chatInfo, organizer=${chatInfo?.organizer?.id}, joinWebUrl=${chatInfo?.joinWebUrl ? 'yes' : 'no'}`);
        
        const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 4: Got ${chatMembers.length} chat members`);
        
        const meetingTitle = chatInfo?.subject || 'Meeting';
        const speakerList = chatMembers.length > 0 ? chatMembers : [];

        // ALWAYS try Graph API first for the most up-to-date post-meeting transcript
        const isInCall = Array.from(callToConversationMap.values()).includes(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 5: isInCall=${isInCall}`);
        let graphTranscriptParsed: TranscriptEntry[] | null = null;

        if (!isInCall && chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
          console.log(`[TRANSCRIPT] Bot not in call - fetching from Graph API first`);
          const fetchingActivity = new MessageActivity(
            `? **Checking for meeting transcript...**`
          ).addAiGenerated();
          await send(fetchingActivity);
          console.log(`[TRANSCRIBE_DEBUG] Step 6: Sent checking message, now fetching transcript`);

          const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
          console.log(`[TRANSCRIBE_DEBUG] Step 7: Transcript window min=${transcriptWindow.min}, max=${transcriptWindow.max}`);
          const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
            chatInfo.organizer.id,
            chatInfo.joinWebUrl,
            transcriptWindow.min,
            transcriptWindow.max
          );
          console.log(`[TRANSCRIBE_DEBUG] Step 8: fetchMeetingTranscriptText returned ${vttContent ? vttContent.length + ' chars' : 'null'}`);
          if (vttContent) {
            graphTranscriptParsed = parseVttToEntries(vttContent);
            if (graphTranscriptParsed.length > 0) {
              console.log(`[TRANSCRIPT] Graph API returned ${graphTranscriptParsed.length} entries`);
              liveTranscriptMap.set(activity.conversation.id, graphTranscriptParsed);
              saveTranscriptToFile(activity.conversation.id);
            }
          }
        }

        // Use Graph transcript if available, otherwise fall back to in-memory
        let finalEntries: TranscriptEntry[] = [];
        let dataSource = '';

        if (graphTranscriptParsed && graphTranscriptParsed.length > 0) {
          finalEntries = graphTranscriptParsed.filter(e => e.isFinal);
          dataSource = 'Graph API (post-meeting)';
        } else {
          // Fallback to in-memory live transcript
          const liveEntries = liveTranscriptMap.get(activity.conversation.id);
          finalEntries = liveEntries?.filter(e => e.isFinal) || [];
          dataSource = 'in-memory (live capture)';
        }

        if (finalEntries.length > 0) {
          console.log(`[TRANSCRIPT] Returning ${finalEntries.length} entries from ${dataSource}`);

          // Determine which entries to show
          const displayEntries = finalEntries.length > 80 ? finalEntries.slice(-80) : finalEntries;
          const showingPartial = finalEntries.length > 80;

          const transcript = await buildTranscriptHtml(
            displayEntries, meetingTitle, speakerList,
            finalEntries.length, showingPartial
          );

          const responseActivity = new MessageActivity(transcript).addAiGenerated().addFeedback();
          await send(responseActivity);
          console.log(`[SUCCESS] Transcript sent to user (source: ${dataSource})`);
        } else {
            const fetchingActivity = new MessageActivity(
              `? **Checking for meeting transcript...**`
            ).addAiGenerated();
            await send(fetchingActivity);

            const meetingOnlineInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
            if (meetingOnlineInfo?.organizer?.id && meetingOnlineInfo?.joinWebUrl) {
              const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
              const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
                meetingOnlineInfo.organizer.id,
                meetingOnlineInfo.joinWebUrl,
                transcriptWindow.min,
                transcriptWindow.max
              );
              if (vttContent) {
                const parsed = parseVttToEntries(vttContent);
                if (parsed.length > 0) {
                  liveTranscriptMap.set(activity.conversation.id, parsed);
                  saveTranscriptToFile(activity.conversation.id);

                  // Use meeting metadata
                  const graphTitle = meetingOnlineInfo.subject || meetingTitle;
                  const transcript = await buildTranscriptHtml(
                    parsed.length > 80 ? parsed.slice(-80) : parsed,
                    graphTitle, speakerList,
                    parsed.length, parsed.length > 80
                  );
                  const responseActivity = new MessageActivity(transcript).addAiGenerated().addFeedback();
                  await send(responseActivity);
                  console.log(`[SUCCESS] Graph transcript fetched and sent (${parsed.length} entries)`);
                } else {
                  const responseActivity = new MessageActivity(
                    `?? **Transcript data was found but couldn't be parsed.**\n\nTeams may still be processing it � try again in a minute.`
                  ).addAiGenerated();
                  await send(responseActivity);
                }
              } else {
                const responseActivity = new MessageActivity(
                  `?? **No transcript available yet.**\n\n` +
                  `Make sure:\n` +
                  `� Recording/transcription was enabled during the meeting\n` +
                  `� The meeting has ended (transcripts are available post-meeting)\n\n` +
                  `If the meeting just ended, try again in a minute � Teams may still be processing it.`
                ).addAiGenerated();
                await send(responseActivity);
              }
            } else {
              const responseActivity = new MessageActivity(
                `?? **No transcript available.**\n\n` +
                `I could not find a transcript for this channel conversation yet.\n\n` +
                `Please ensure transcription was enabled for the meeting and try again shortly after the meeting ends.`
              ).addAiGenerated();
              await send(responseActivity);
            }
        }
      } catch (error) {
        console.error(`[ERROR_TRANSCRIBE] Failed to handle transcription:`, error);
        const errorResponse = new MessageActivity(
          'I encountered an error while getting the transcript. Please try again.'
        ).addAiGenerated();
        await send(errorResponse);
      }
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle key meeting insights (cache-first transcript retrieval)
    if (
      detectedIntent === 'insights' ||
      userMessage.includes('key meeting insight') ||
      userMessage.includes('key meeting insights') ||
      userMessage.includes('meeting insight')
    ) {
      console.log(`[ACTION] Processing key meeting insights request`);
      await sendTypingIndicator(send);
      try {
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        const meetingTitle = chatInfo?.subject || 'Meeting';

        const fetchingActivity = new MessageActivity(
          `? **Fetching transcript for this channel conversation...**`
        ).addAiGenerated();
        await send(fetchingActivity);

        // Always use Graph-first retrieval for analysis requests.
        const transcriptText = await getTranscriptTextForConversation(activity.conversation.id);
        if (transcriptText) {
          console.log(`[INSIGHTS] Loaded transcript using Graph-first retrieval`);
        }

        if (!transcriptText) {
          const noTranscriptActivity = new MessageActivity(
            `?? **No transcript available yet.**\n\n` +
            `I checked Teams transcript data for this channel conversation and did not find one yet.\n` +
            `Please ensure transcription was enabled and try again after the meeting ends.`
          ).addAiGenerated();
          await send(noTranscriptActivity);
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        const insights = await generateKeyMeetingInsights(transcriptText, meetingTitle);
        const insightsActivity = new MessageActivity(
          `## ?? Key Meeting Insights\n\n${insights}`
        ).addAiGenerated().addFeedback();
        await send(insightsActivity);
        console.log(`[SUCCESS] Key meeting insights sent`);
      } catch (error) {
        console.error(`[ERROR_INSIGHTS] Failed to generate key meeting insights:`, error);
        const errorActivity = new MessageActivity(
          `I encountered an error while generating key meeting insights. Please try again.`
        ).addAiGenerated();
        await send(errorActivity);
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle meeting minutes requests
    if (detectedIntent === 'minutes' || userMessage.includes('minutes') || userMessage.includes('meeting notes')) {
      console.log(`[ACTION] Processing meeting minutes request`);
      await sendTypingIndicator(send);
      
      // Check if user also wants to email the result
      const emailRequest = detectEmailRequest(cleanText || activity.text || '');
      let generatedMinutes = '';
      
      try {
        console.log(`[DEBUG] Processing meeting minutes for user`);

        // First, try to get meeting transcript for minutes
        const liveEntries = liveTranscriptMap.get(activity.conversation.id);
        const transcriptEntries = liveEntries?.filter(e => e.isFinal) || [];

        if (transcriptEntries.length > 0) {
          console.log(`[MINUTES] Found ${transcriptEntries.length} transcript entries, generating formal minutes...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
          const meetingTitle = chatInfo?.subject || 'Meeting';
          const memberList = chatMembers.length > 0 ? chatMembers : [userName];

          generatedMinutes = await generateMinutesHtml(
            transcriptEntries,
            meetingTitle,
            memberList
          );
          console.log(`[MINUTES] Minutes generated successfully from transcript`);

          const responseActivity = new MessageActivity(generatedMinutes).addAiGenerated().addFeedback();
          await send(responseActivity);
          console.log(`[SUCCESS] Transcript-based minutes sent to user`);
        } else {
          // Fallback to chat message minutes generation
          console.log(`[GRAPH] No transcript found, fetching chat messages for minutes (limit: 100)`);
          let chatMessages = await graphApiHelper.getChatMessages(activity.conversation.id, 100);
          console.log(`[GRAPH] Retrieved ${chatMessages.length} messages`);

          // Fallback: if Graph API returns no messages, use stored conversation history
          if (chatMessages.length === 0) {
            console.log(`[FALLBACK] Graph API returned no messages, using stored conversation history`);
            const storedUserMessages = storage.get(sharedConversationKey) || [];
            
            if (storedUserMessages.length > 0) {
              chatMessages = storedUserMessages.map((msg: any, index: number) => {
                const content = typeof msg === 'string' 
                  ? msg 
                  : (msg.content || (typeof msg === 'object' ? JSON.stringify(msg) : String(msg)));
                
                const displayName = (msg.user && typeof msg.user === 'string') 
                  ? msg.user 
                  : userName;

                return {
                  id: `msg_${index}`,
                  from: {
                    user: {
                      id: activity.from.id,
                      displayName: displayName,
                    },
                  },
                  body: {
                    content: content,
                  },
                  createdDateTime: msg.timestamp || new Date().toISOString(),
                };
              });
              console.log(`[FALLBACK] Using ${chatMessages.length} stored messages for minutes`);
            }
          }

          if (chatMessages.length === 0) {
            const responseActivity = new MessageActivity(
              `I don't have enough content yet to generate minutes.\n\n` +
              `If this is a meeting, ask me to join or request a transcript first.`
            ).addAiGenerated().addFeedback();
            await send(responseActivity);
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }

          console.log(`[GRAPH] Fetching chat info and participants`);
          const chatInfo = await graphApiHelper.getChatInfo(activity.conversation.id);
          const participants = (chatInfo?.members?.map((m: any) => m.displayName) || []).length > 0
            ? chatInfo?.members?.map((m: any) => m.displayName)
            : [userName]; // Fallback to current user if no participants found
          console.log(`[GRAPH] Found ${participants.length} participants: ${participants.join(', ')}`);

          console.log(`[MINUTES] Generating meeting minutes...`);
          generatedMinutes = await summarizationHelper.generateMeetingMinutes(
            chatMessages,
            participants,
            userMessage
          );
          console.log(`[MINUTES] Meeting minutes generated successfully`);

          const responseActivity = new MessageActivity(
            `**Meeting Minutes for ${userName}:**\n\n${generatedMinutes}`
          ).addAiGenerated().addFeedback();
          await send(responseActivity);
          console.log(`[SUCCESS] Meeting minutes sent to user`);
        }
        
        // If user requested email, send it now
        if (emailRequest.wantsEmail && generatedMinutes) {
          console.log(`[EMAIL] User requested minutes via email`);
          let recipientEmail = emailRequest.emailAddress;
          
          // If no email in message, get user's email
          if (!recipientEmail) {
            const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
            recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
          }
          
          if (recipientEmail) {
            const emailResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmail,
              'Meeting Minutes from Mela AI Meeting Assistant',
              generatedMinutes
            );
            
            if (emailResult.success) {
              await send(new MessageActivity(`?? I've also sent the minutes to **${recipientEmail}**.`).addAiGenerated());
            } else {
              await send(new MessageActivity(`I couldn't send the email: ${emailResult.error}`).addAiGenerated());
            }
          } else {
            await send(new MessageActivity(`I couldn't determine your email address. Please specify an email address.`).addAiGenerated());
          }
        }
      } catch (error) {
        console.error(`[ERROR_MINUTES] Failed to generate minutes:`, error);
        const errorResponse = new MessageActivity(
          'I encountered an error while generating meeting minutes. Please try again.'
        ).addAiGenerated();
        await send(errorResponse);
      }
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    if (detectedIntent === 'meeting_question' && isMeetingConversation) {
      console.log(`[ACTION] Processing meeting question with merged context`);
      await sendTypingIndicator(send);
      try {
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        const meetingTitle = chatInfo?.subject || 'Meeting';

        const shared = storage.get(sharedConversationKey) || [];
        const conversationContext = buildConversationContext(shared, 120);
        const transcriptContext = await getTranscriptTextForConversation(activity.conversation.id);

        const answer = await answerMeetingQuestionWithContext(
          cleanText || activity.text || '',
          meetingTitle,
          conversationContext,
          transcriptContext
        );

        await send(new MessageActivity(answer).addAiGenerated().addFeedback());
        console.log(`[SUCCESS] Meeting question answered with conversation + transcript context`);
      } catch (error) {
        console.error(`[ERROR_MEETING_QA]`, error);
        await send(new MessageActivity('I encountered an error while answering based on meeting context. Please try again.').addAiGenerated());
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    const isJoinCallIntent =
      detectedIntent === 'join_meeting' ||
      (userMessage.includes('join') && userMessage.includes('call')) ||
      (userMessage.includes('join') && userMessage.includes('meeting'));

    if (isJoinCallIntent) {
      console.log(`[ACTION] Processing join-call flow`);

      // Attempt to join the meeting as an actual participant via Graph Calls API
      const botEndpoint = process.env.BOT_ENDPOINT || '';
      const callbackUri = botEndpoint ? `${botEndpoint}/api/calls` : '';
      const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID || '';

      let joinedCall = false;
      if (callbackUri && tenantId) {
        // Cancel any previous pending retry for this conversation
        cancelPendingJoin(activity.conversation.id);

        console.log(`[CALLS_API] Getting meeting info for join attempt, callback: ${callbackUri}`);
        const meetingOnlineInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        if (meetingOnlineInfo?.organizer?.id) {
          const callResult = await graphApiHelper.joinMeetingCall(meetingOnlineInfo, callbackUri, tenantId, activity.conversation.id);
          if (callResult) {
            joinedCall = true;
            activeCallMap.set(callResult.id, {
              conversationId: activity.conversation.id,
              serviceUrl: activity.serviceUrl || '',
              organizerId: meetingOnlineInfo.organizer?.id,
              joinWebUrl: meetingOnlineInfo.joinWebUrl,
            });
            // Register pending join info so auto-retry works if the call terminates with 2203
            pendingJoinMap.set(activity.conversation.id, {
              conversationId: activity.conversation.id,
              serviceUrl: activity.serviceUrl || '',
              callbackUri,
              tenantId,
              retryCount: 0,
              maxRetries: MAX_RETRIES,
            });
            const joinedActivity = new MessageActivity(
              `? **Joining the meeting!**\n\nI'll keep trying to connect � if the call hasn't started yet I'll automatically join once someone starts it (up to 5 minutes).\n\nOnce connected, you can ask me to:\n� **Summarize** - recap of the chat so far\n� **Minutes** - formal meeting minutes\n� **Transcribe** - get transcript if recording is available`
            ).addAiGenerated().addFeedback();
            await send(joinedActivity);
          } else {
            console.warn(`[JOIN_FLOW] Could not join meeting audio`);
          }
        } else {
          console.warn(`[JOIN_FLOW] No organizer info found, skipping call join`);
        }
      } else {
        console.warn(`[JOIN_FLOW] Missing BOT_ENDPOINT or TENANT_ID - skipping call join`);
      }

      if (!joinedCall) {
        const readyActivity = new MessageActivity(
          `?? I'm here in the meeting chat!\n\nYou can ask me to:\n� **Summarize** - recap of the chat so far\n� **Minutes** - formal meeting minutes\n� **Transcribe** - get transcript if recording is available`
        ).addAiGenerated().addFeedback();
        await send(readyActivity);
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    if (userMessage.includes('read all chats') || userMessage.includes('read all chat') || userMessage.includes('all chats')) {
      console.log(`[ACTION] Processing read-all-chats flow`);
      let chatMessages = await graphApiHelper.getChatMessages(activity.conversation.id, 50);

      if (chatMessages.length === 0) {
        const storedSharedMessages = storage.get(sharedConversationKey) || [];
        chatMessages = storedSharedMessages.map((msg: any, index: number) => ({
          id: `read_all_msg_${index}`,
          from: {
            user: {
              id: activity.from.id,
              displayName: msg.user || userName,
            },
          },
          body: {
            content: msg.content || '',
          },
          createdDateTime: msg.timestamp || new Date().toISOString(),
        }));
      }

      if (chatMessages.length === 0) {
        await send(new MessageActivity(`No chat messages found yet in this meeting chat.`).addAiGenerated().addFeedback());
      } else {
        const formatted = chatMessages
          .reverse()
          .slice(-30)
          .map((m) => `� ${(m.from.user?.displayName || 'Unknown')}: ${m.body.content}`)
          .join('\n');
        await send(new MessageActivity(`**Recent Meeting Chats (${Math.min(chatMessages.length, 30)} shown):**\n\n${formatted}`).addAiGenerated().addFeedback());
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle send email intent - LLM extracts parameters and generates response
    if (detectedIntent === 'send_email') {
      console.log(`[ACTION] Processing send_email request`);
      await sendTypingIndicator(send);
      try {
        const userText = (cleanText || activity.text || '').toLowerCase();
        
        // Check if user wants to send summary/minutes/transcript (explicit keywords)
        const wantsSummary = userText.includes('summary') || userText.includes('summarize');
        const wantsMinutes = userText.includes('minutes') || userText.includes('meeting notes');
        const wantsTranscript = userText.includes('transcript');
        
        // Check for contextual references like "send it", "email that", "send this to me"
        const isContextualReference = /\b(send\s+(it|this|that)|email\s+(it|this|that|me)|send\s+to\s+(my\s+)?email)\b/i.test(userText);
        const lastResponse = lastBotResponseMap.get(activity.conversation.id);
        const hasRecentContext = lastResponse && (Date.now() - lastResponse.timestamp) < 10 * 60 * 1000; // within 10 minutes
        
        console.log(`[EMAIL] Context check: isContextualReference=${isContextualReference}, hasRecentContext=${hasRecentContext}, lastContentType=${lastResponse?.contentType}`);
        
        // Extract email address from request
        const emailMatch = (cleanText || activity.text || '').match(/[\w.-]+@[\w.-]+\.\w+/i);
        let recipientEmail = emailMatch ? emailMatch[0] : null;
        
        // If no email in message, get user's email
        if (!recipientEmail) {
          const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
          recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
        }
        
        if (!recipientEmail) {
          await send(new MessageActivity(`I couldn't determine the recipient email address. Please specify who to send the email to.`).addAiGenerated().addFeedback());
        } else if (isContextualReference && hasRecentContext && lastResponse) {
          // User is referring to something we just showed them - use conversation context!
          console.log(`[EMAIL] Using contextual reference - sending last ${lastResponse.contentType} response`);
          
          const contentTypeLabels: Record<string, string> = {
            'calendar': 'calendar schedule',
            'summary': 'meeting summary',
            'minutes': 'meeting minutes',
            'transcript': 'transcript',
            'meeting_overview': 'meeting overview',
            'insights': 'meeting insights',
            'general': 'information'
          };
          
          const emailSubject = lastResponse.subject || `${contentTypeLabels[lastResponse.contentType] || 'Information'} from Mela AI Meeting Assistant`;
          
          const sendResult = await graphApiHelper.sendEmail(
            activity.from.aadObjectId || activity.from.id,
            recipientEmail,
            emailSubject,
            lastResponse.content
          );
          
          if (sendResult.success) {
            await send(new MessageActivity(`?? Done! I've sent your ${contentTypeLabels[lastResponse.contentType] || 'information'} to **${recipientEmail}**.`).addAiGenerated());
          } else {
            await send(new MessageActivity(`I couldn't send the email: ${sendResult.error}`).addAiGenerated());
          }
        } else if (wantsSummary || wantsMinutes || wantsTranscript) {
          // Generate the requested content first, then email it
          console.log(`[EMAIL] Generating content to send: summary=${wantsSummary}, minutes=${wantsMinutes}, transcript=${wantsTranscript}`);
          
          const liveEntries = liveTranscriptMap.get(activity.conversation.id);
          const transcriptEntries = liveEntries?.filter(e => e.isFinal) || [];
          let contentToSend = '';
          let emailSubject = 'Meeting Content from Mela AI Meeting Assistant';
          
          if (transcriptEntries.length > 0) {
            const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
            const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
            const meetingTitle = chatInfo?.subject || 'Meeting';
            const memberList = chatMembers.length > 0 ? chatMembers : [userName];
            
            if (wantsSummary) {
              contentToSend = await generateFormattedSummaryHtml(transcriptEntries, meetingTitle, userName, memberList);
              emailSubject = `Meeting Summary: ${meetingTitle}`;
            } else if (wantsMinutes) {
              contentToSend = await generateMinutesHtml(transcriptEntries, meetingTitle, memberList);
              emailSubject = `Meeting Minutes: ${meetingTitle}`;
            } else if (wantsTranscript) {
              contentToSend = transcriptEntries.map(e => `[${e.speaker}]: ${e.text}`).join('\n');
              emailSubject = `Meeting Transcript: ${meetingTitle}`;
            }
          } else {
            // Fallback to chat messages
            let chatMessages = await graphApiHelper.getChatMessages(activity.conversation.id, 50);
            if (chatMessages.length === 0) {
              const storedShared = storage.get(sharedConversationKey) || [];
              chatMessages = storedShared.map((msg: any, i: number) => ({
                id: `msg_${i}`,
                from: { user: { id: activity.from.id, displayName: msg.user || userName } },
                body: { content: msg.content || '' },
                createdDateTime: msg.timestamp || new Date().toISOString()
              }));
            }
            
            if (chatMessages.length > 0) {
              if (wantsSummary) {
                contentToSend = await summarizationHelper.summarizeChatMessages(chatMessages, 'detailed');
                emailSubject = 'Chat Summary from Mela AI Meeting Assistant';
              } else if (wantsMinutes) {
                const chatInfo = await graphApiHelper.getChatInfo(activity.conversation.id);
                const participants = chatInfo?.members?.map((m: any) => m.displayName) || [userName];
                contentToSend = await summarizationHelper.generateMeetingMinutes(chatMessages, participants, userText);
                emailSubject = 'Meeting Minutes from Mela AI Meeting Assistant';
              }
            }
          }
          
          if (contentToSend) {
            // Also show in chat
            await send(new MessageActivity(contentToSend).addAiGenerated().addFeedback());
            
            // Send email
            const sendResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmail,
              emailSubject,
              contentToSend
            );
            
            if (sendResult.success) {
              await send(new MessageActivity(`?? I've sent the ${wantsSummary ? 'summary' : wantsMinutes ? 'minutes' : 'transcript'} to **${recipientEmail}**.`).addAiGenerated());
            } else {
              await send(new MessageActivity(`I couldn't send the email: ${sendResult.error}`).addAiGenerated());
            }
          } else {
            await send(new MessageActivity(`I don't have enough content to generate a ${wantsSummary ? 'summary' : wantsMinutes ? 'minutes' : 'transcript'}. If this is a meeting, ask me to join first.`).addAiGenerated().addFeedback());
          }
        } else {
          // Generic email - use LLM with conversation context to understand what to send
          // Build recent conversation context
          const recentLlmMessages = (llmMessages || []).slice(-10);
          const conversationContext = recentLlmMessages.map((msg: any) => 
            `${msg.role === 'user' ? 'User' : 'Assistant'}: ${(msg.content || '').substring(0, 500)}`
          ).join('\n');
          
          const extractPrompt = new ChatPrompt({
            messages: [
              {
                role: 'user',
                content: `Analyze this email request in the context of the recent conversation.

User's email request: "${cleanText || activity.text}"
User's name: ${userName}

Recent conversation context:
${conversationContext}

${hasRecentContext && lastResponse ? `Last bot response type: ${lastResponse.contentType}\nLast response preview: ${lastResponse.content.substring(0, 300)}...` : 'No recent bot response.'}

Based on the conversation, determine what the user wants to email:
1. If they're referring to something discussed (like "send that", "email this info"), use the relevant content from the conversation
2. If they want to compose a new email, extract the subject and body from their request
3. If unclear, ask for clarification in the body

Respond with JSON: {"subject": "clear email subject", "body": "complete email body content", "is_contextual": true/false, "needs_clarification": true/false}`
              }
            ],
            instructions: 'You are a smart email assistant. Understand context! If the user refers to previous content ("it", "that", "this", "the schedule", "the info"), include that content in the email body. Generate complete, ready-to-send email content. Output valid JSON only.',
            model: new OpenAIChatModel({
              model: config.azureOpenAIDeploymentName,
              apiKey: config.azureOpenAIKey,
              endpoint: config.azureOpenAIEndpoint,
              apiVersion: '2024-10-21'
            })
          });

          const extractResponse = await extractPrompt.send('');
          const jsonStr = (extractResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
          const extracted = JSON.parse(jsonStr);
          
          if (extracted.needs_clarification) {
            await send(new MessageActivity(extracted.body || "I'm not sure what you'd like me to email. Could you please clarify what content you want me to send?").addAiGenerated().addFeedback());
          } else {
            // Send the email
            const sendResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmail,
              extracted.subject || 'Message from Mela AI Meeting Assistant',
              extracted.body || cleanText || activity.text || ''
            );

            // Let LLM generate the response based on result
            const responsePrompt = new ChatPrompt({
              messages: [
                {
                  role: 'user',
                  content: `Generate a brief, friendly response about sending an email.\n\nResult: ${sendResult.success ? 'SUCCESS' : 'FAILED'}\nRecipient: ${recipientEmail}\nSubject: ${extracted.subject || 'Message from Mela AI Meeting Assistant'}\n${sendResult.error ? `Error: ${sendResult.error}` : ''}\n\nRespond naturally as if you just completed (or failed) this action.`
                }
              ],
              instructions: 'You are a helpful assistant. Generate a natural, brief response about the email send result. Be friendly but concise.',
              model: new OpenAIChatModel({
                model: config.azureOpenAIDeploymentName,
                apiKey: config.azureOpenAIKey,
                endpoint: config.azureOpenAIEndpoint,
                apiVersion: '2024-10-21'
              })
            });

            const responseResult = await responsePrompt.send('');
            await send(new MessageActivity(responseResult.content || 'Email action completed.').addAiGenerated().addFeedback());
          }
        }
        console.log(`[SUCCESS] Email intent processed`);
      } catch (error) {
        console.error(`[ERROR_SEND_EMAIL]`, error);
        await send(new MessageActivity(`I had trouble processing your email request. Please try again with a clear recipient and message.`).addAiGenerated().addFeedback());
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle check calendar intent - LLM extracts parameters and generates response
    if (detectedIntent === 'check_calendar') {
      console.log(`[ACTION] Processing check_calendar request`);
      await sendTypingIndicator(send);
      try {
        // Build comprehensive date context for LLM
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const tomorrow = new Date(today.getTime() + 24 * 60 * 60 * 1000);
        const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        const nextWeekStart = new Date(today.getTime() + 7 * 24 * 60 * 60 * 1000);
        const thisWeekEnd = new Date(today.getTime() + (7 - today.getDay()) * 24 * 60 * 60 * 1000);
        
        const dateContext = `Current timestamp: ${now.toISOString()}
Today's date: ${today.toISOString().split('T')[0]} (${today.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })})
Tomorrow: ${tomorrow.toISOString().split('T')[0]}
Yesterday: ${yesterday.toISOString().split('T')[0]}
This week ends: ${thisWeekEnd.toISOString().split('T')[0]}
Next week starts: ${nextWeekStart.toISOString().split('T')[0]}`;

        // Use LLM to understand what calendar info the user wants
        const extractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Analyze this calendar request and extract date/time parameters.

User request: "${cleanText || activity.text}"

${dateContext}

IMPORTANT: Interpret relative dates correctly:
- "today" / "today's meetings" = ${today.toISOString().split('T')[0]}
- "yesterday" / "meeting we had yesterday" = ${yesterday.toISOString().split('T')[0]}
- "tomorrow" = ${tomorrow.toISOString().split('T')[0]}
- "this morning" / "this afternoon" = ${today.toISOString().split('T')[0]}
- "this week" = ${today.toISOString().split('T')[0]} to ${thisWeekEnd.toISOString().split('T')[0]}
- "next week" = ${nextWeekStart.toISOString().split('T')[0]} onwards

Respond with JSON only: {"query_type": "view_events|check_availability|find_free_time|past_events", "start_date": "ISO date string", "end_date": "ISO date string", "description": "brief description of what user wants", "is_past": true|false}`
            }
          ],
          instructions: 'You are a JSON extraction assistant. Determine what calendar info the user needs. Convert relative dates (today, yesterday, tomorrow, this week, next week, this morning, etc) to actual ISO date strings based on the provided date context. Set is_past=true if user is asking about past events. Output valid JSON only.',
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const extractResponse = await extractPrompt.send('');
        const extracted = JSON.parse((extractResponse.content || '').trim());
        console.log(`[CALENDAR] Extracted: ${JSON.stringify(extracted)}`);
        
        const userId = activity.from.aadObjectId || activity.from.id;
        const calendarResult = await graphApiHelper.getCalendarEvents(
          userId,
          extracted.start_date || undefined,
          extracted.end_date || undefined
        );

        // Let LLM generate a natural response based on calendar data
        const eventsJson = calendarResult.success ? JSON.stringify(calendarResult.events?.slice(0, 15) || [], null, 2) : 'No events retrieved';
        
        const responsePrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `You are helping ${userName || 'the user'} understand their calendar.

Their question: "${cleanText || activity.text}"
Date range requested: ${extracted.start_date || 'today'} to ${extracted.end_date || 'today'}
API result: ${calendarResult.success ? 'SUCCESS' : 'FAILED'}
${calendarResult.error ? `Error: ${calendarResult.error}` : ''}

Raw calendar data:
${eventsJson}

Create a beautifully formatted, easy-to-read calendar summary. Make it visually clean and scannable. Each meeting MUST be on its own line. Group active meetings separately from canceled ones. Convert all times to a friendly readable format. End with a brief encouraging note about their day.`
            }
          ],
          instructions: `You are a professional calendar assistant. Your responses must be VISUALLY EXCELLENT and easy to scan quickly.

CRITICAL FORMATTING RULES:
- EACH meeting MUST be on its own separate line - NEVER put multiple meetings on the same line
- Use line breaks generously to create visual separation
- Active meetings should be listed first, then canceled meetings in a separate section
- Make meeting titles stand out using **bold**
- Times should be human-friendly (e.g., "1:00 PM - 1:30 PM")
- Canceled meetings should be clearly marked and visually distinct
- Keep the response concise but well-organized
- End with a short, friendly summary sentence

The output should look clean and professional when displayed in a Teams chat.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const responseResult = await responsePrompt.send('');
        const calendarResponseContent = responseResult.content || 'Calendar check completed.';
        await send(new MessageActivity(calendarResponseContent).addAiGenerated().addFeedback());
        
        // Track this response for contextual follow-ups like "send it to my email"
        lastBotResponseMap.set(activity.conversation.id, {
          content: calendarResponseContent,
          contentType: 'calendar',
          subject: `Calendar for ${extracted.start_date || 'today'}`,
          timestamp: Date.now()
        });
        
        console.log(`[SUCCESS] Calendar intent processed`);
      } catch (error) {
        console.error(`[ERROR_CHECK_CALENDAR]`, error);
        await send(new MessageActivity(`I had trouble checking your calendar. Please try again.`).addAiGenerated().addFeedback());
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    if (isGroupConversation && !botMentioned) {
      console.log(`[MESSAGE_IGNORED] Group message without bot mention: ${activity.id || 'unknown-activity'}`);
      console.log(`[DEBUG_MENTION] Recipient ID: ${activity.recipient?.id}, CLIENT_ID: ${process.env.CLIENT_ID}`);
      console.log(`[DEBUG_MENTION] Entities: ${JSON.stringify(activity.entities?.filter((e: any) => e.type === 'mention').map((e: any) => ({ ...e, mentioned: e.mentioned })))}`);
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle existing chat functionality
    console.log(`[ACTION] Processing standard chat request`);
    await sendTypingIndicator(send);
    
    // Build enhanced instructions with transcript context for Q&A
    let enhancedInstructions = personalizedInstructions;
    const liveEntries = liveTranscriptMap.get(activity.conversation.id);
    if (liveEntries && liveEntries.length > 0) {
      const finalEntries = liveEntries.filter(e => e.isFinal);
      if (finalEntries.length > 0) {
        // Take last 100 entries to avoid token limits, most recent conversation context
        const recentEntries = finalEntries.slice(-100);
        const transcriptContext = recentEntries
          .map(e => `[${e.speaker}]: ${e.text}`)
          .join('\n');
        enhancedInstructions += `\n\n## Live Meeting Transcript Context\nThe following is the transcript from the current meeting. Use this to answer questions about what participants said:\n\n${transcriptContext}`;
        console.log(`[CHAT] Added ${recentEntries.length} transcript entries to context for Q&A`);
      }
    }
    
    const prompt = new ChatPrompt({
      messages: llmMessages,
      instructions: enhancedInstructions,
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: "2024-10-21"
      })
    })

    console.log(`[CHAT] Sending prompt to model: ${config.azureOpenAIDeploymentName}`);

    if (activity.conversation.isGroup) {
      // If the conversation is a group chat, we need to send the final response
      // back to the group chat
      console.log(`[CHAT] Group chat mode - awaiting full response`);
      const response = await prompt.send(cleanText || activity.text || '');
      console.log(`[CHAT] Received response from model`);
      llmMessages.push({ role: 'assistant', content: response.content || '' });
      if (llmMessages.length > 30) {
        llmMessages = llmMessages.slice(-30);
      }
      const responseActivity = new MessageActivity(response.content).addAiGenerated().addFeedback();
      await send(responseActivity);
      console.log(`[SUCCESS] Chat response sent to group`);
    } else {
        console.log(`[CHAT] Personal/direct mode - streaming response`);
        await prompt.send(cleanText || activity.text || '', {
          onChunk: (chunk) => {
            console.log(`[STREAM] Chunk received`);
            stream.emit(chunk);
          },
        });
      // We wrap the final response with an AI Generated indicator
      stream.emit(new MessageActivity().addAiGenerated().addFeedback());
    }
    storage.set(conversationKey, messages);
    storage.set(sharedConversationKey, sharedMessages);
    storage.set(llmConversationKey, llmMessages);
  } catch (error) {
    console.error(error);
    await send("The agent encountered an error or bug.");
    await send("To continue to run this agent, please fix the agent source code.");
  }
});

// Handle conversation updates (bot added/removed, members joined/left)
app.on('conversationUpdate', async ({ send, activity }) => {
  console.log(`[CONVERSATION_UPDATE] Event triggered for: ${activity.conversation.id}`);
  console.log(`[CONVERSATION_UPDATE] Is Group: ${activity.conversation.isGroup}, Channel ID: ${activity.channelId}`);

  // Check if the bot was added to the conversation
  if (activity.membersAdded && activity.membersAdded.length > 0) {
    const recipientId = activity.recipient?.id || '';
    const clientId = process.env.CLIENT_ID || '';
    console.log(`[CONVERSATION_UPDATE] Recipient ID: ${recipientId}`);
    console.log(`[CONVERSATION_UPDATE] CLIENT_ID: ${clientId}`);
    console.log(`[CONVERSATION_UPDATE] Members Added: ${JSON.stringify(activity.membersAdded.map((m: any) => m.id))}`);
    
    const botWasAdded = activity.membersAdded.some((member: any) => {
      const memberId = member.id || '';
      return (
        memberId === recipientId ||
        memberId.includes(recipientId) ||
        recipientId.includes(memberId) ||
        (clientId && memberId.includes(clientId)) ||
        (clientId && memberId === `28:${clientId}`)
      );
    });
    
    if (botWasAdded) {
      console.log(`[BOT_ADDED] Bot was added to conversation: ${activity.conversation.id}`);
      
      try {
        // Detect if this is a meeting chat
        const isMeetingChat = activity.conversation.isGroup && 
                              (activity.conversation.id.includes('meeting') || 
                               activity.conversation.id.includes('call') ||
                               activity.channelId === 'msteams');
        
        console.log(`[MEETING_DETECTION] Detected as meeting: ${isMeetingChat}`);

        const greeting = isMeetingChat
          ? `Hello, **Mela AI Meeting Assistant** here! I've joined the meeting chat. 
            
I can help you with:
- **Summarize** - Get a quick recap of the chat
- **Minutes** - Generate formal meeting minutes
- **Transcribe** - Get meeting transcript (if recording available)

Just ask me to summarize, get minutes, or transcribe!`
          : `Hello, **Mela AI Meeting Assistant** is ready to help you!

I can quickly help with:
- **Chat** - Ask questions and get instant support
- **Summarize** - Turn long conversations into clear takeaways
- **Minutes** - Generate structured meeting minutes
- **Transcribe** - Retrieve meeting transcripts (when available)

How can I assist?`;

        const greetingActivity = new MessageActivity(greeting).addAiGenerated();
        await send(greetingActivity);
        
        console.log(`[SUCCESS] Sent greeting to ${isMeetingChat ? 'meeting' : 'conversation'}`);

        // Auto-join the meeting call if this is a meeting chat
        if (isMeetingChat) {
          console.log(`[AUTO_JOIN] Attempting to auto-join meeting call...`);
          const botEndpoint = process.env.BOT_ENDPOINT || '';
          const callbackUri = botEndpoint ? `${botEndpoint}/api/calls` : '';
          const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID || '';

          if (callbackUri && tenantId) {
            try {
              const meetingOnlineInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
              if (meetingOnlineInfo?.organizer?.id) {
                cancelPendingJoin(activity.conversation.id);
                const callResult = await graphApiHelper.joinMeetingCall(meetingOnlineInfo, callbackUri, tenantId, activity.conversation.id);
                if (callResult) {
                  activeCallMap.set(callResult.id, {
                    conversationId: activity.conversation.id,
                    serviceUrl: activity.serviceUrl || '',
                    organizerId: meetingOnlineInfo.organizer?.id,
                    joinWebUrl: meetingOnlineInfo.joinWebUrl,
                  });
                  pendingJoinMap.set(activity.conversation.id, {
                    conversationId: activity.conversation.id,
                    serviceUrl: activity.serviceUrl || '',
                    callbackUri,
                    tenantId,
                    retryCount: 0,
                    maxRetries: MAX_RETRIES,
                  });
                  console.log(`[AUTO_JOIN] Successfully initiated join for call ${callResult.id}`);
                  const autoJoinMsg = new MessageActivity(
                    `??? **Auto-joining the meeting call...**\n\nI'll connect automatically and start transcribing!`
                  ).addAiGenerated();
                  await send(autoJoinMsg);
                } else {
                  console.warn(`[AUTO_JOIN] Could not join meeting audio`);
                }
              } else {
                console.warn(`[AUTO_JOIN] No meeting organizer info found`);
              }
            } catch (error) {
              console.error(`[AUTO_JOIN_ERROR]`, error);
            }
          } else {
            console.warn(`[AUTO_JOIN] Missing BOT_ENDPOINT or TENANT_ID - skipping auto-join`);
          }
        }
      } catch (error) {
        console.error(`[ERROR_GREETING] Failed to send greeting:`, error);
      }
    }
  }

  // Handle members leaving (optional logging)
  if (activity.membersRemoved && activity.membersRemoved.length > 0) {
    const recipientId = activity.recipient?.id || '';
    const clientId = process.env.CLIENT_ID || '';
    const botWasRemoved = activity.membersRemoved.some((member: any) => {
      const memberId = member.id || '';
      return memberId === recipientId || memberId.includes(recipientId) || recipientId.includes(memberId) || (clientId && memberId.includes(clientId));
    });
    if (botWasRemoved) {
      console.log(`[BOT_REMOVED] Bot was removed from conversation: ${activity.conversation.id}`);
    }
  }
});

app.on('message.submit.feedback', async ({ activity }) => {
  //add custom feedback process logic here
  console.log("Your feedback is " + JSON.stringify(activity.value));
})

// Handle Graph Communications API call-state callback notifications
// Teams/Graph will POST events here when call state changes (ringing, established, terminated, etc.)
app.http.post('/api/calls', async (req: any, res: any) => {
  try {
    const body = req.body;
    const notifications = body?.value || [];

    // Acknowledge immediately � Graph requires 200 within 5 seconds
    res.status(200).json({});

    // Log the full raw body keys for debugging transcript events
    for (const notification of notifications) {
      const rdKeys = notification.resourceData ? Object.keys(notification.resourceData) : [];
      const topKeys = Object.keys(notification);
      console.log(`[CALLS_WEBHOOK_DEBUG] notification keys=[${topKeys.join(',')}], resourceData keys=[${rdKeys.join(',')}], resource="${notification.resource || ''}"`);
      const changeType = notification.changeType || 'unknown';
      const resource = notification.resource || '';

      // Debug: log the raw notification structure to understand the format
      console.log(`[CALLS_WEBHOOK_RAW] resource="${resource}", resourceData.id="${notification.resourceData?.id}", resourceData['@odata.id']="${notification.resourceData?.['@odata.id']}"`);

      // Extract callId from multiple possible locations:
      // 1. resourceData['@odata.id'] - often contains the full URL like /communications/calls/{callId}
      // 2. resourceData.id - sometimes the call ID directly
      // 3. resource path - like /communications/calls/{callId} or just calls/{callId}
      const odataId = notification.resourceData?.['@odata.id'] || '';
      const allPaths = [odataId, resource];

      let callId = 'unknown';
      let isParticipantEvent = resource.includes('/participants') || odataId.includes('/participants');

      // Try to extract callId from any path that contains calls/{uuid}
      for (const p of allPaths) {
        const match = p.match(/calls\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i);
        if (match) {
          callId = match[1];
          break;
        }
      }

      // Fallback: if resourceData.id looks like a UUID and we didn't find one yet
      if (callId === 'unknown' && notification.resourceData?.id) {
        const rdId = notification.resourceData.id;
        if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(rdId)) {
          callId = rdId;
        }
      }

      const callState = notification.resourceData?.state || 'unknown';
      const resultInfo = notification.resourceData?.resultInfo;

      console.log(`[CALLS_WEBHOOK] Call event: type=${changeType}, callId=${callId}, state=${callState}${isParticipantEvent ? ' [participant-update]' : ''}`);
      if (resultInfo) {
        console.log(`[CALLS_WEBHOOK] Result info: code=${resultInfo.code}, subcode=${resultInfo.subcode}, message=${resultInfo.message}`);
      }

      // Handle participant updates � check if all humans left
      if (isParticipantEvent && callId !== 'unknown') {
        // Check participants asynchronously to see if only the bot remains
        try {
          const participants = await graphApiHelper.getCallParticipants(callId);
          const humanParticipants = participants.filter((p: any) => {
            // A human participant has a user identity (not application)
            const info = p.info?.identity;
            return info?.user && !info?.application;
          });
          console.log(`[PARTICIPANTS] Total: ${participants.length}, Humans: ${humanParticipants.length}, CallId: ${callId}`);

          // Fallback trigger: if humans are in the call and transcription didn't start yet,
          // try auto-start again (first established attempt can be too early).
          if (humanParticipants.length > 0) {
            const callEntry = activeCallMap.get(callId);
            if (callEntry?.establishedAt) {
              void attemptAutoStartTranscription(callId, 'participants', callEntry);
            }
          }

          if (participants.length > 0 && humanParticipants.length === 0) {
            // Grace period: don't auto-leave within first 30s (participants may still be joining)
            const callEntry = activeCallMap.get(callId);
            const elapsed = callEntry?.establishedAt ? Date.now() - callEntry.establishedAt : 0;
            if (elapsed < 30_000) {
              console.log(`[AUTO_LEAVE] Skipping � call only ${Math.round(elapsed / 1000)}s old (grace period: 30s)`);
            } else if (callEntry?.leavingInProgress) {
              console.log(`[AUTO_LEAVE] Already leaving � skipping duplicate`);
            } else {
              console.log(`[AUTO_LEAVE] No human participants remaining � hanging up`);
              if (callEntry) {
                callEntry.leavingInProgress = true;
                // Removed notification - user will get notified when transcript is ready
              }
              await graphApiHelper.hangUp(callId);
            }
          }
        } catch (err) {
          // Don't crash the webhook if participant check fails
          console.warn(`[PARTICIPANTS] Could not check participants:`, err);
        }
        continue; // Don't process participant events as call state changes
      }

      if (callState === 'establishing' && callId !== 'unknown') {
        // For outgoing bot-initiated joins, establishing is informational only - Teams auto-establishes it.
        // Do NOT call /answer here - that API is only for incoming calls to the bot.
        console.log(`[CALLS_WEBHOOK] Call establishing - waiting for Teams to confirm connection...`);
      } else if (callState === 'established') {
        const callEntry = activeCallMap.get(callId);
        // Guard: only process the FIRST established event per call (Teams sends duplicates)
        if (callEntry?.establishedAt) {
          console.log(`[CALLS_WEBHOOK] Duplicate established event for callId=${callId} � ignoring`);
        } else if (callEntry) {
          console.log(`[CALLS_WEBHOOK] Call ESTABLISHED - bot is live in meeting. conversationId=${callEntry.conversationId}`);
          // Record establishment time and cancel any pending retries
          callEntry.establishedAt = Date.now();
          if (callEntry.organizerId && callEntry.joinWebUrl) {
            cacheMeetingContext(
              callEntry.conversationId,
              callEntry.organizerId,
              callEntry.joinWebUrl,
              undefined,
              { startedAt: callEntry.establishedAt }
            );
          }
          cancelPendingJoin(callEntry.conversationId);
          // Track callId -> conversationId for transcript routing
          callToConversationMap.set(callId, callEntry.conversationId);
          // Initialize transcript storage for this conversation
          if (!liveTranscriptMap.has(callEntry.conversationId)) {
            liveTranscriptMap.set(callEntry.conversationId, []);
          }
          // Auto-start transcription with retries (Teams can reject very early requests)
          void attemptAutoStartTranscription(callId, 'established', callEntry);
          
          // Start live polling for transcript updates (every 10 seconds while call is active)
          if (callEntry.organizerId && callEntry.joinWebUrl) {
            startLiveTranscriptPolling(
              callId,
              callEntry.organizerId,
              callEntry.joinWebUrl,
              callEntry.conversationId,
              callEntry.serviceUrl,
              callEntry.establishedAt || Date.now()
            );
          }
          
          await graphApiHelper.sendProactiveMessage(
            callEntry.serviceUrl,
            callEntry.conversationId,
            `??? **I'm now live in the meeting!**\n\nI�m auto-enabling Teams transcription now.\n\nAsk me to:\n� **Transcribe** � see the live transcript so far\n� **Summarize** � recap of the chat\n� **Minutes** � formal meeting minutes`
          );
        }
      } else if (callState === 'terminated') {
        const callEntry = activeCallMap.get(callId);
        const conversationId = callToConversationMap.get(callId);
        clearPendingTranscriptionStart(callId);
        stopLiveTranscriptPolling(callId);
        
        // Capture call timing before we delete the entry
        const callStartedAt = callEntry?.establishedAt || Date.now();
        const callEndedAt = Date.now();
        
        activeCallMap.delete(callId);
        callToConversationMap.delete(callId);
        console.log(`[CALLS_WEBHOOK] Call TERMINATED - bot left the meeting (duration: ${(callEndedAt - callStartedAt) / 1000}s)`);

        // Fetch the meeting transcript from Graph (Teams stores it server-side)
        // Use retry logic � Teams may need time to finalize transcripts
        if (callEntry?.organizerId && callEntry?.joinWebUrl && conversationId) {
          cacheMeetingContext(
            conversationId,
            callEntry.organizerId,
            callEntry.joinWebUrl,
            undefined,
            { startedAt: callStartedAt, endedAt: callEndedAt }
          );
          const orgId = callEntry.organizerId!;
          const webUrl = callEntry.joinWebUrl!;
          const svcUrl = callEntry.serviceUrl;
          const convId = conversationId;
          console.log(`[POST_MEETING_TRANSCRIPT] Will fetch transcript from Graph for organizer=${orgId}, callWindow: ${new Date(callStartedAt).toISOString()} to ${new Date(callEndedAt).toISOString()}`);

          const attemptFetch = async (attempt: number, maxAttempts: number) => {
            try {
              console.log(`[POST_MEETING_TRANSCRIPT] Attempt ${attempt}/${maxAttempts}...`);
              const vttContent = await graphApiHelper.fetchMeetingTranscriptText(orgId, webUrl, callStartedAt, callEndedAt);
              if (vttContent) {
                console.log(`[POST_MEETING_TRANSCRIPT] Got ${vttContent.length} chars of VTT content`);
                const parsed = parseVttToEntries(vttContent);
                if (parsed.length > 0) {
                  liveTranscriptMap.set(convId, parsed);
                  saveTranscriptToFile(convId);
                  console.log(`[POST_MEETING_TRANSCRIPT] Saved ${parsed.length} transcript entries`);
                  await graphApiHelper.sendProactiveMessage(
                    svcUrl, convId,
                    `?? **Meeting transcript is ready!** (${parsed.length} entries)\n\nSay **transcribe** to see it.`
                  );
                  return; // success
                } else {
                  console.log(`[POST_MEETING_TRANSCRIPT] VTT content downloaded but no entries parsed`);
                }
              } else {
                console.log(`[POST_MEETING_TRANSCRIPT] No transcript available from Graph yet`);
              }
              // Retry if we haven't exhausted attempts
              if (attempt < maxAttempts) {
                const delayMs = attempt * 15_000; // progressive: 15s, 30s, 45s, 60s
                console.log(`[POST_MEETING_TRANSCRIPT] Retrying in ${delayMs / 1000}s...`);
                setTimeout(() => attemptFetch(attempt + 1, maxAttempts), delayMs);
              } else {
                console.log(`[POST_MEETING_TRANSCRIPT] All ${maxAttempts} attempts exhausted � transcript not available`);
                await graphApiHelper.sendProactiveMessage(
                  svcUrl, convId,
                  `?? **Could not retrieve meeting transcript.**\n\nTeams may not have generated one. Ensure transcription/recording was active during the meeting.`
                );
              }
            } catch (err) {
              console.error(`[POST_MEETING_TRANSCRIPT_ERROR] Attempt ${attempt}:`, err);
              if (attempt < maxAttempts) {
                setTimeout(() => attemptFetch(attempt + 1, maxAttempts), 15_000);
              }
            }
          };

          // First attempt after 30s, then progressive retries up to 4 attempts
          setTimeout(() => attemptFetch(1, 4), 30_000);
        }

        // Keep any existing transcript data around
        if (conversationId) {
          const transcriptCount = liveTranscriptMap.get(conversationId)?.filter(e => e.isFinal).length || 0;
          if (transcriptCount > 0) {
            console.log(`[CALLS_WEBHOOK] Transcript preserved: ${transcriptCount} entries for ${conversationId}`);
            saveTranscriptToFile(conversationId);
          }
        }

        // If meeting wasn't active (2203), auto-retry instead of giving up
        if (resultInfo?.code === 400 && resultInfo?.subcode === 2203 && callEntry) {
          const pending = pendingJoinMap.get(callEntry.conversationId);
          if (pending) {
            console.log(`[CALLS_WEBHOOK] Meeting not active (400/2203) - scheduling auto-retry`);
            await scheduleJoinRetry(callEntry.conversationId);
          } else {
            console.log(`[CALLS_WEBHOOK] Meeting not active (400/2203) - no pending retry registered`);
            await graphApiHelper.sendProactiveMessage(
              callEntry.serviceUrl,
              callEntry.conversationId,
              `?? **I couldn't join the meeting call.**\n\nThe live call is not currently active. Please start the meeting and ask me to join again.`
            );
          }
        }
      }

      // --- Transcription handling ---
      // Per Graph API docs, the field is `resourceData.transcription` (type callTranscription)
      // with properties: state (notStarted|active|inactive), lastModifiedDateTime
      const transcription = notification.resourceData?.transcription;
      if (transcription) {
        const tState = transcription.state;
        console.log(`[TRANSCRIPTION_STATE] state=${tState}, callId=${callId}, lastModified=${transcription.lastModifiedDateTime || 'N/A'}`);
        // Dump the full transcription object to discover any extra fields (e.g. content)
        console.log(`[TRANSCRIPTION_STATE_FULL]`, JSON.stringify(transcription));
      }

      // Also check the legacy/alternate field names just in case
      const transcriptionUpdate = notification.resourceData?.transcriptionUpdate;
      const transcriptionData = notification.resourceData?.transcriptionData;
      if (transcriptionUpdate) {
        console.log(`[TRANSCRIPTION_UPDATE_FIELD]`, JSON.stringify(transcriptionUpdate));
      }
      if (transcriptionData) {
        console.log(`[TRANSCRIPTION_DATA_FIELD]`, JSON.stringify(transcriptionData));
      }

      // If ANY resourceData key contains the word "transcri", log it for discovery
      if (notification.resourceData) {
        for (const key of Object.keys(notification.resourceData)) {
          if (key.toLowerCase().includes('transcri')) {
            console.log(`[TRANSCRIPTION_DISCOVERY] key="${key}" value=`, JSON.stringify(notification.resourceData[key]));
          }
        }
      }

      // Handle live transcript data (if it ever arrives in any of these fields)
      const actualTranscriptData = transcriptionData || transcription?.content || transcription?.data;
      if (actualTranscriptData) {
        const conversationId = callToConversationMap.get(callId);
        if (conversationId) {
          const results = Array.isArray(actualTranscriptData) ? actualTranscriptData : (actualTranscriptData.results || []);
          const entries = liveTranscriptMap.get(conversationId) || [];
          for (const result of results) {
            const speaker = result.participant?.user?.displayName ||
                            result.participant?.application?.displayName ||
                            result.displayName || result.speaker ||
                            'Unknown';
            const text = result.text || result.content || '';
            const isFinal = result.resultType === 'final' || result.isFinal !== false;
            if (text.trim()) {
              if (isFinal) {
                const lastIdx = entries.length - 1;
                if (lastIdx >= 0 && !entries[lastIdx].isFinal && entries[lastIdx].speaker === speaker) {
                  entries[lastIdx] = { speaker, text, timestamp: new Date().toISOString(), isFinal: true };
                } else {
                  entries.push({ speaker, text, timestamp: new Date().toISOString(), isFinal: true });
                }
              } else {
                entries.push({ speaker, text, timestamp: new Date().toISOString(), isFinal: false });
              }
              console.log(`[TRANSCRIPT] [${isFinal ? 'FINAL' : 'interim'}] ${speaker}: ${text}`);
              if (isFinal) {
                saveTranscriptToFile(conversationId);
              }
            }
          }
          liveTranscriptMap.set(conversationId, entries);
        } else {
          console.warn(`[TRANSCRIPT] Got transcript data but no conversationId mapped for callId=${callId}`);
        }
      }
    }
  } catch (error) {
    console.error(`[CALLS_WEBHOOK_ERROR] Failed to process call event:`, error);
  }
});

export { activeCallMap };
export default app;