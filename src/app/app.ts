import { App } from "@microsoft/teams.apps";
import { ChatPrompt } from "@microsoft/teams.ai";
import { LocalStorage } from "@microsoft/teams.common";
import { OpenAIChatModel } from "@microsoft/teams.openai";
import { MessageActivity, TokenCredentials, ClientCredentials } from '@microsoft/teams.api';
import { ManagedIdentityCredential } from '@azure/identity';
import * as fs from 'fs';
import * as fsPromises from 'fs/promises';
import * as path from 'path';
const PDFDocument = require('pdfkit');
import config from "../config";
import graphApiHelper, { type MailMessageSummary } from "../graphApiHelper";
import summarizationHelper from "../summarizationHelper";
import {
  analyzeEmailRequest,
  draftReplyFromInboxThread,
  formatEmailResult,
  formatRecipientDisplay,
  llmSearchInbox,
  parseInboxRequest,
  smartParseInboxRequest,
  summarizeInboxMessages,
} from "./emailCapabilities";
import {
  IntentAgent,
  createIntentAgent,
  getConversationState,
  recordBotResponse as recordAgentBotResponse,
  type AgentDecision,
  type AgentContext,
  type IntentLabel as AgentIntentLabel,
} from "./intentAgent";

const ENABLE_VERBOSE_CONSOLE = (process.env.ENABLE_VERBOSE_CONSOLE || 'false').toLowerCase() === 'true';
const baseConsoleLog = console.log.bind(console);

const ESSENTIAL_LOG_PREFIXES = [
  '[MESSAGE]',
  '[MODEL_RESPONSE]',
  '[TRANSCRIPT]',
  '[TEAMS_SEND_OK]',
  '[TEAMS_SEND_FAIL]',
  '[CALLS_WEBHOOK]',
  '[CALLS_API]',
  '[CALLS_AUTH]',
  '[TRANSCRIPTION]',
  '[LIVE_TRANSCRIPT', // matches [LIVE_TRANSCRIPT], [LIVE_TRANSCRIPT_POLL], etc.
  '[LIVE_SESSION]',
  '[GRAPH_API]',
  '[GRAPH_AUTH]',
  '[TRANSCRIPT_DIAG]',
  '[TRANSCRIPT_SUB]',
  '[VTT_PARSER]',
  '[STREAM_PREVIEW]',
  '[STREAM_STATS]',
  '[PARTICIPANTS]',
  '[AUTO_LEAVE]',
  '[JOIN_FLOW]',
  '[JOIN_RETRY]',
  '[STATUS_NOTICE]',
  '[CACHE_UPDATE]',
];

function shouldKeepConsoleLog(args: any[]): boolean {
  const first = args?.[0];
  if (typeof first !== 'string') {
    return true;
  }
  if (!first.startsWith('[')) {
    return true;
  }
  return ESSENTIAL_LOG_PREFIXES.some((prefix) => first.startsWith(prefix));
}

function getTruncatedLogPreview(text: string, maxChars = 260): string {
  const normalized = (text || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return '(empty)';
  if (normalized.length <= maxChars) return normalized;
  return `${normalized.slice(0, maxChars)}... [truncated]`;
}

function toInlineScriptJson(value: unknown): string {
  // Prevent inline <script> parsing issues from user/content data.
  return JSON.stringify(value ?? {})
    .replace(/</g, '\\u003c')
    .replace(/\u2028/g, '\\u2028')
    .replace(/\u2029/g, '\\u2029');
}

function extractModelResponseText(response: any): string {
  if (!response) return '';
  if (typeof response === 'string') return response;
  if (typeof response.content === 'string' && response.content.trim()) return response.content;
  if (typeof response.text === 'string' && response.text.trim()) return response.text;
  if (typeof response.message === 'string' && response.message.trim()) return response.message;
  if (typeof response.output === 'string' && response.output.trim()) return response.output;
  if (Array.isArray(response.output)) {
    const joined = response.output.map((x: any) => (typeof x === 'string' ? x : (x?.text || ''))).join(' ').trim();
    if (joined) return joined;
  }
  return '';
}

// Keep runtime logs concise by default; set ENABLE_VERBOSE_CONSOLE=true for deep debugging.
if (!ENABLE_VERBOSE_CONSOLE) {
  console.log = (...args: any[]) => {
    if (!shouldKeepConsoleLog(args)) {
      return;
    }
    baseConsoleLog(...args);
  };
}

// Create storage for conversation history
const storage = new LocalStorage();

// Track last bot responses for each conversation - used for contextual follow-ups like "send it to my email"
// Keep up to 5 recent responses for better context resolution
interface LastBotResponse {
  content: string;
  contentType: 'calendar' | 'summary' | 'minutes' | 'transcript' | 'meeting_overview' | 'insights' | 'general' | 'inbox_email';
  subject?: string;
  timestamp: number;
  recipientType?: 'self' | 'other' | 'multiple' | 'all_participants' | null;
  recipientNames?: string[];
  recipientEmails?: string[];
  sourceRequest?: string;
}
const lastBotResponseMap = new Map<string, LastBotResponse>();
const botResponseHistoryMap = new Map<string, LastBotResponse[]>(); // Keep last 5 responses per conversation
const MAX_RESPONSE_HISTORY = 5;

interface InboxContact {
  displayName: string;
  email: string;
}

interface InboxContext {
  updatedAt: number;
  mailboxUserId?: string;
  lastMatchedMessageId?: string;
  lastMatchedSenderName?: string;
  lastMatchedSenderEmail?: string;
  lastMessages: MailMessageSummary[];
  contacts: InboxContact[];
}

const inboxContextMap = new Map<string, InboxContext>();

/** Track the last clarification question the bot asked in each conversation */
interface PendingClarification {
  question: string;
  aboutPerson?: string;
  aboutTopic?: string;
  timestamp: number;
}
const pendingClarificationMap = new Map<string, PendingClarification>();

function normalizeRecipientName(value: string): string {
  return (value || '')
    .toLowerCase()
    .replace(/'s$/i, '')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function isPronounRecipient(value: string): boolean {
  return ['him', 'her', 'them', 'that person', 'that guy', 'that lady'].includes(normalizeRecipientName(value));
}

function cacheInboxContext(conversationId: string, messages: MailMessageSummary[]) {
  const contactsMap = new Map<string, InboxContact>();
  for (const message of messages) {
    const email = (message.fromAddress || '').trim();
    if (!email) continue;
    const key = email.toLowerCase();
    contactsMap.set(key, {
      displayName: (message.fromName || email).trim(),
      email,
    });
  }

  inboxContextMap.set(conversationId, {
    updatedAt: Date.now(),
    lastMessages: messages,
    contacts: [...contactsMap.values()],
  });
}

function rememberMatchedInboxThread(
  conversationId: string,
  mailboxUserId: string,
  message?: MailMessageSummary
) {
  const current = inboxContextMap.get(conversationId) || {
    updatedAt: Date.now(),
    lastMessages: [],
    contacts: [],
  };

  inboxContextMap.set(conversationId, {
    ...current,
    updatedAt: Date.now(),
    mailboxUserId,
    lastMatchedMessageId: message?.id || current.lastMatchedMessageId,
    lastMatchedSenderName: message?.fromName || current.lastMatchedSenderName,
    lastMatchedSenderEmail: message?.fromAddress || current.lastMatchedSenderEmail,
  });
}

function extractSuggestedReplyBody(content: string): string {
  const raw = (content || '').trim();
  if (!raw) return '';

  const withoutHeader = raw
    .replace(/^##\s*Suggested Reply\s*/i, '')
    .replace(/^\*\*Subject:\*\*.*$/im, '')
    .trim();

  const rationaleMatch = withoutHeader.match(/\n##\s*Rationale\b/i);
  const replySection = rationaleMatch
    ? withoutHeader.slice(0, rationaleMatch.index).trim()
    : withoutHeader;

  return replySection.trim();
}

function resolveRecipientFromInboxContext(conversationId: string, rawName: string): InboxContact | null {
  const context = inboxContextMap.get(conversationId);
  if (!context) return null;
  if ((Date.now() - context.updatedAt) > 30 * 60 * 1000) return null;

  const normalizedQuery = normalizeRecipientName(rawName);
  if (!normalizedQuery) return null;

  if (isPronounRecipient(normalizedQuery)) {
    const latest = context.lastMessages.find((message) => !!message.fromAddress);
    if (!latest?.fromAddress) return null;
    return {
      displayName: latest.fromName || latest.fromAddress,
      email: latest.fromAddress,
    };
  }

  for (const contact of context.contacts) {
    const contactName = normalizeRecipientName(contact.displayName);
    const contactEmail = normalizeRecipientName(contact.email.split('@')[0] || '');
    if (
      contactName.includes(normalizedQuery) ||
      normalizedQuery.includes(contactName) ||
      contactEmail.includes(normalizedQuery) ||
      normalizedQuery.includes(contactEmail)
    ) {
      return contact;
    }
  }

  return null;
}

// Helper to add response to history
function recordBotResponse(conversationId: string, response: LastBotResponse) {
  lastBotResponseMap.set(conversationId, response);
  const history = botResponseHistoryMap.get(conversationId) || [];
  history.push(response);
  // Keep only last N responses
  while (history.length > MAX_RESPONSE_HISTORY) {
    history.shift();
  }
  botResponseHistoryMap.set(conversationId, history);
}

// Get recent response by type (useful for "send the summary" when multiple responses exist)
function getRecentResponseByType(conversationId: string, contentType: string, maxAgeMs = 30 * 60 * 1000): LastBotResponse | null {
  const history = botResponseHistoryMap.get(conversationId) || [];
  const now = Date.now();
  // Search from newest to oldest
  for (let i = history.length - 1; i >= 0; i--) {
    const resp = history[i];
    if ((now - resp.timestamp) < maxAgeMs && resp.contentType === contentType) {
      return resp;
    }
  }
  return null;
}

// Track active call IDs -> { conversationId, serviceUrl, organizerId, joinWebUrl } for webhook handling
interface ActiveCall {
  conversationId: string;
  serviceUrl: string;
  organizerId?: string;
  joinWebUrl?: string;
  onlineMeetingId?: string;
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

// Directory for persisted transcript and tracking files.
// On Azure App Service with WEBSITE_RUN_FROM_PACKAGE=1, wwwroot is read-only.
// Persist to HOME/data instead; keep process.cwd() for local development.
const IS_RUNNING_ON_AZURE =
  process.env.RUNNING_ON_AZURE === '1' ||
  !!process.env.WEBSITE_SITE_NAME ||
  !!process.env.WEBSITE_INSTANCE_ID;
const AZURE_HOME_DIR = process.env.HOME || process.env.HOME_EXPANDED || '';
const PERSISTENCE_ROOT = (IS_RUNNING_ON_AZURE && AZURE_HOME_DIR)
  ? path.join(AZURE_HOME_DIR, 'data', 'missa-translator')
  : process.cwd();

const TRANSCRIPTS_DIR = path.resolve(PERSISTENCE_ROOT, 'transcripts');
const ADMIN_DATA_DIR = path.resolve(PERSISTENCE_ROOT, 'admin_data');
const MEETING_CONTEXT_FILE = path.join(ADMIN_DATA_DIR, 'meeting_context.json');

console.log(`[STARTUP] Running on Azure: ${IS_RUNNING_ON_AZURE ? 'yes' : 'no'}`);
console.log(`[STARTUP] Persistence root set to: ${PERSISTENCE_ROOT}`);
console.log(`[STARTUP] Transcripts directory set to: ${TRANSCRIPTS_DIR}`);
console.log(`[STARTUP] Admin data directory set to: ${ADMIN_DATA_DIR}`);

interface MeetingContextEntry {
  organizerId: string;
  joinWebUrl: string;
  onlineMeetingId?: string;
  subject?: string;
  updatedAt: number;
  callStartedAt?: number;
  callEndedAt?: number;
  callId?: string;
}

const meetingContextMap = new Map<string, MeetingContextEntry>();

// Meeting history tracking - stores top 5 most recent meetings per conversation
const MEETING_HISTORY_FILE = path.join(ADMIN_DATA_DIR, 'meeting_history.json');
const MAX_MEETING_HISTORY = 5;

interface MeetingHistoryEntry {
  meetingId: string; // Unique identifier for this meeting instance
  subject: string;
  startTime: number;
  endTime?: number;
  transcriptFilePath?: string;
  participantCount: number;
  entryCount: number;
  organizerId?: string;
  joinWebUrl?: string;
  callId?: string;
}

// Map from conversationId to array of meeting history entries (most recent first)
const meetingHistoryMap = new Map<string, MeetingHistoryEntry[]>();

interface UserStatsEntry {
  userId: string;
  displayName: string;
  tenantId?: string;
  totalMessages: number;
  meetingJoinRequests: number;
  monthlyMeetingsJoined: Record<string, number>;
  monthlyMeetingLimitOverride: number | null;
  tokenPolicy: 'unlimited' | 'limited';
  tokenLimit: number | null;
  estimatedInputTokens: number;
  estimatedOutputTokens: number;
  estimatedTotalTokens: number;
  estimatedCostUsd: number;
  firstSeenAt: number;
  lastSeenAt: number;
  blocked: boolean;
  blockReason?: string;
  blockedAt?: number;
}

interface MeetingUsageEntry {
  meetingId: string;
  meetingName: string;
  firstSeenAt: number;
  lastSeenAt: number;
  joinRequests: number;
  estimatedInputTokens: number;
  estimatedOutputTokens: number;
  estimatedTotalTokens: number;
  estimatedCostUsd: number;
  users: string[];
}

interface DailyUsageEntry {
  day: string;
  messages: number;
  meetingsJoined: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  costUsd: number;
}

interface BotAdminStats {
  startedAt: number;
  lastUpdatedAt: number;
  totalMessages: number;
  totalMeetingsJoined: number;
  maxUsers: number;
  freeTierMonthlyMeetingLimit: number;
  enforceGlobalLimits: boolean;
  modelInputCostPer1kUsd: number;
  modelOutputCostPer1kUsd: number;
  totalEstimatedInputTokens: number;
  totalEstimatedOutputTokens: number;
  totalEstimatedTokens: number;
  totalEstimatedCostUsd: number;
  users: Record<string, UserStatsEntry>;
  meetings: Record<string, MeetingUsageEntry>;
  dailyUsage: Record<string, DailyUsageEntry>;
  perUserDailyUsage: Record<string, Record<string, DailyUsageEntry>>;
  activeMeetingConversationIds: string[];
  meetingConversationHistory: string[];
}

const BOT_ADMIN_STATS_FILE = path.join(ADMIN_DATA_DIR, 'bot_admin_stats.json');
const BOT_ADMIN_STATS_BACKUP_FILE = path.join(ADMIN_DATA_DIR, 'bot_admin_stats.backup.json');
const DEFAULT_MAX_USERS = Number(process.env.BOT_MAX_USERS || process.env.MAX_USERS || 200);
const DEFAULT_FREE_TIER_MONTHLY_MEETINGS = Number(process.env.FREE_TIER_MAX_MEETINGS_PER_MONTH || 5);
const DEFAULT_INPUT_COST_PER_1K_USD = Number(process.env.MODEL_INPUT_COST_PER_1K_USD || 0.00015);
const DEFAULT_OUTPUT_COST_PER_1K_USD = Number(process.env.MODEL_OUTPUT_COST_PER_1K_USD || 0.0006);
const AUTO_BLOCK_ON_POLICY_BREACH = (process.env.AUTO_BLOCK_ON_POLICY_BREACH || 'true').toLowerCase() === 'true';
let botAdminStats: BotAdminStats | null = null;

interface ModelUsageTracking {
  userId: string;
  displayName: string;
  tenantId?: string;
  meetingId: string;
  meetingName?: string;
  inputText: string;
  outputText: string;
}

function defaultBotAdminStats(): BotAdminStats {
  return {
    startedAt: Date.now(),
    lastUpdatedAt: Date.now(),
    totalMessages: 0,
    totalMeetingsJoined: 0,
    maxUsers: Number.isFinite(DEFAULT_MAX_USERS) ? DEFAULT_MAX_USERS : 200,
    freeTierMonthlyMeetingLimit: Number.isFinite(DEFAULT_FREE_TIER_MONTHLY_MEETINGS) ? DEFAULT_FREE_TIER_MONTHLY_MEETINGS : 5,
    enforceGlobalLimits: false,
    modelInputCostPer1kUsd: Number.isFinite(DEFAULT_INPUT_COST_PER_1K_USD) ? DEFAULT_INPUT_COST_PER_1K_USD : 0.00015,
    modelOutputCostPer1kUsd: Number.isFinite(DEFAULT_OUTPUT_COST_PER_1K_USD) ? DEFAULT_OUTPUT_COST_PER_1K_USD : 0.0006,
    totalEstimatedInputTokens: 0,
    totalEstimatedOutputTokens: 0,
    totalEstimatedTokens: 0,
    totalEstimatedCostUsd: 0,
    users: {},
    meetings: {},
    dailyUsage: {},
    perUserDailyUsage: {},
    activeMeetingConversationIds: [],
    meetingConversationHistory: [],
  };
}

let adminDataMigrationAttempted = false;

function ensureAdminDataDir() {
  if (!fs.existsSync(ADMIN_DATA_DIR)) {
    fs.mkdirSync(ADMIN_DATA_DIR, { recursive: true });
  }
  if (adminDataMigrationAttempted) {
    return;
  }
  adminDataMigrationAttempted = true;

  // Migrate legacy admin JSON files previously stored in transcripts/.
  const legacyToCurrent: Array<{ from: string; to: string }> = [
    { from: path.join(TRANSCRIPTS_DIR, 'bot_admin_stats.json'), to: BOT_ADMIN_STATS_FILE },
    { from: path.join(TRANSCRIPTS_DIR, 'bot_admin_stats.backup.json'), to: BOT_ADMIN_STATS_BACKUP_FILE },
    { from: path.join(TRANSCRIPTS_DIR, 'meeting_context.json'), to: MEETING_CONTEXT_FILE },
    { from: path.join(TRANSCRIPTS_DIR, 'admin_error_logs.json'), to: path.join(ADMIN_DATA_DIR, 'admin_error_logs.json') },
  ];

  for (const pair of legacyToCurrent) {
    try {
      if (!fs.existsSync(pair.from) || fs.existsSync(pair.to)) {
        continue;
      }
      fs.copyFileSync(pair.from, pair.to);
      fs.rmSync(pair.from, { force: true });
      console.log(`[ADMIN_DATA] Migrated ${path.basename(pair.from)} -> ${ADMIN_DATA_DIR}`);
    } catch (error) {
      console.warn(`[ADMIN_DATA] Failed to migrate ${pair.from}:`, error);
    }
  }
}

function saveBotAdminStats() {
  if (!botAdminStats) return;
  ensureAdminDataDir();
  botAdminStats.lastUpdatedAt = Date.now();
  const payload = JSON.stringify(botAdminStats, null, 2);
  const tempFile = `${BOT_ADMIN_STATS_FILE}.tmp`;

  // Write to a temp file first to avoid partially-written JSON files.
  fs.writeFileSync(tempFile, payload, 'utf-8');
  if (fs.existsSync(BOT_ADMIN_STATS_FILE)) {
    fs.rmSync(BOT_ADMIN_STATS_FILE, { force: true });
  }
  fs.renameSync(tempFile, BOT_ADMIN_STATS_FILE);

  // Keep a recovery snapshot for parse/IO failures on startup.
  fs.writeFileSync(BOT_ADMIN_STATS_BACKUP_FILE, payload, 'utf-8');
}

function loadBotAdminStats(): BotAdminStats {
  if (botAdminStats) return botAdminStats;
  try {
    let transcriptSource: 'A' | 'B' | 'C' | 'none' = 'none';
    ensureAdminDataDir();
    if (fs.existsSync(BOT_ADMIN_STATS_FILE)) {
      const raw = fs.readFileSync(BOT_ADMIN_STATS_FILE, 'utf-8');
      const parsed = JSON.parse(raw) as BotAdminStats;
      botAdminStats = {
        ...defaultBotAdminStats(),
        ...parsed,
        users: parsed.users || {},
        meetings: parsed.meetings || {},
        dailyUsage: parsed.dailyUsage || {},
        perUserDailyUsage: parsed.perUserDailyUsage || {},
        activeMeetingConversationIds: parsed.activeMeetingConversationIds || [],
        meetingConversationHistory: parsed.meetingConversationHistory || [],
      };

      // Backfill newly added user-policy fields for older persisted files.
      for (const user of Object.values(botAdminStats.users || {})) {
        if (typeof user.monthlyMeetingLimitOverride === 'undefined') {
          user.monthlyMeetingLimitOverride = null;
        }
        if (user.tokenPolicy !== 'limited' && user.tokenPolicy !== 'unlimited') {
          user.tokenPolicy = 'unlimited';
        }
        if (typeof user.tokenLimit === 'undefined') {
          user.tokenLimit = null;
        }
        if (typeof user.tenantId !== 'string') {
          user.tenantId = '';
        }
      }
      if (typeof botAdminStats.enforceGlobalLimits !== 'boolean') {
        botAdminStats.enforceGlobalLimits = false;
      }
      return botAdminStats;
    }
  } catch (error) {
    console.warn('[ADMIN_STATS] Failed to load primary stats file, attempting backup recovery:', error);
    try {
      if (fs.existsSync(BOT_ADMIN_STATS_BACKUP_FILE)) {
        const backupRaw = fs.readFileSync(BOT_ADMIN_STATS_BACKUP_FILE, 'utf-8');
        const backupParsed = JSON.parse(backupRaw) as BotAdminStats;
        botAdminStats = {
          ...defaultBotAdminStats(),
          ...backupParsed,
          users: backupParsed.users || {},
          meetings: backupParsed.meetings || {},
          dailyUsage: backupParsed.dailyUsage || {},
          perUserDailyUsage: backupParsed.perUserDailyUsage || {},
          activeMeetingConversationIds: backupParsed.activeMeetingConversationIds || [],
          meetingConversationHistory: backupParsed.meetingConversationHistory || [],
        };
        saveBotAdminStats();
        console.warn('[ADMIN_STATS] Recovered stats from backup snapshot');
        return botAdminStats;
      }
    } catch (backupError) {
      console.warn('[ADMIN_STATS] Backup recovery failed, using defaults:', backupError);
    }
  }
  botAdminStats = defaultBotAdminStats();
  saveBotAdminStats();
  return botAdminStats;
}

function upsertUserStats(userId: string, displayName: string, tenantId?: string): UserStatsEntry {
  const stats = loadBotAdminStats();
  const existing = stats.users[userId];
  if (existing) {
    existing.displayName = displayName || existing.displayName;
    if (tenantId && tenantId.trim()) {
      existing.tenantId = tenantId.trim();
    }
    existing.lastSeenAt = Date.now();
    return existing;
  }
  const entry: UserStatsEntry = {
    userId,
    displayName: displayName || userId,
    tenantId: (tenantId || '').trim(),
    totalMessages: 0,
    meetingJoinRequests: 0,
    monthlyMeetingsJoined: {},
    monthlyMeetingLimitOverride: null,
    tokenPolicy: 'unlimited',
    tokenLimit: null,
    estimatedInputTokens: 0,
    estimatedOutputTokens: 0,
    estimatedTotalTokens: 0,
    estimatedCostUsd: 0,
    firstSeenAt: Date.now(),
    lastSeenAt: Date.now(),
    blocked: false,
    blockReason: '',
    blockedAt: undefined,
  };
  stats.users[userId] = entry;
  saveBotAdminStats();
  return entry;
}

function getNonBlockedUserCount(): number {
  const stats = loadBotAdminStats();
  return Object.values(stats.users).filter((u) => !u.blocked).length;
}

function normalizeTenantId(tenantId?: string): string {
  const normalized = (tenantId || '').trim();
  return normalized || 'unknown-tenant';
}

function canUserAccess(userId: string): { allowed: boolean; reason?: string } {
  const stats = loadBotAdminStats();
  const user = stats.users[userId];
  if (user?.blocked) {
    return { allowed: false, reason: 'blocked' };
  }
  if (!stats.enforceGlobalLimits) {
    return { allowed: true };
  }
  if (!user) {
    const activeUsers = getNonBlockedUserCount();
    if (activeUsers >= stats.maxUsers) {
      return { allowed: false, reason: 'limit_reached' };
    }
  }
  return { allowed: true };
}

function recordUserMessage(userId: string, displayName: string, tenantId?: string) {
  const stats = loadBotAdminStats();
  const user = upsertUserStats(userId, displayName, tenantId);
  user.totalMessages += 1;
  user.lastSeenAt = Date.now();
  stats.totalMessages += 1;
  recordDailyMetrics(userId, { messages: 1 });
  saveBotAdminStats();
}

function recordMeetingJoinRequest(userId: string, displayName: string, tenantId?: string) {
  const user = upsertUserStats(userId, displayName, tenantId);
  user.meetingJoinRequests += 1;
  user.lastSeenAt = Date.now();
  saveBotAdminStats();
}

function getMonthKey(date = new Date()): string {
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
}

function getDayKey(date = new Date()): string {
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`;
}

function ensureDailyUsageEntry(target: Record<string, DailyUsageEntry>, dayKey: string): DailyUsageEntry {
  if (!target[dayKey]) {
    target[dayKey] = {
      day: dayKey,
      messages: 0,
      meetingsJoined: 0,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      costUsd: 0,
    };
  }
  return target[dayKey];
}

function recordDailyMetrics(
  userId: string,
  delta: Partial<Pick<DailyUsageEntry, 'messages' | 'meetingsJoined' | 'inputTokens' | 'outputTokens' | 'totalTokens' | 'costUsd'>>
) {
  const stats = loadBotAdminStats();
  const dayKey = getDayKey();

  const globalEntry = ensureDailyUsageEntry(stats.dailyUsage, dayKey);
  globalEntry.messages += delta.messages || 0;
  globalEntry.meetingsJoined += delta.meetingsJoined || 0;
  globalEntry.inputTokens += delta.inputTokens || 0;
  globalEntry.outputTokens += delta.outputTokens || 0;
  globalEntry.totalTokens += delta.totalTokens || 0;
  globalEntry.costUsd += delta.costUsd || 0;

  if (!stats.perUserDailyUsage[userId]) {
    stats.perUserDailyUsage[userId] = {};
  }
  const userEntry = ensureDailyUsageEntry(stats.perUserDailyUsage[userId], dayKey);
  userEntry.messages += delta.messages || 0;
  userEntry.meetingsJoined += delta.meetingsJoined || 0;
  userEntry.inputTokens += delta.inputTokens || 0;
  userEntry.outputTokens += delta.outputTokens || 0;
  userEntry.totalTokens += delta.totalTokens || 0;
  userEntry.costUsd += delta.costUsd || 0;
}

function estimateTokensFromText(text: string): number {
  const normalized = (text || '').trim();
  if (!normalized) return 0;
  return Math.max(1, Math.ceil(normalized.length / 4));
}

function isMeetingConversationId(conversationId: string): boolean {
  const normalized = (conversationId || '').toLowerCase();
  return normalized.includes('meeting_') || normalized.includes('meeting') || normalized.includes('spaces');
}

function getOrCreateMeetingUsage(meetingId: string, meetingName?: string): MeetingUsageEntry {
  const stats = loadBotAdminStats();
  if (!stats.meetings[meetingId]) {
    stats.meetings[meetingId] = {
      meetingId,
      meetingName: meetingName || 'Meeting',
      firstSeenAt: Date.now(),
      lastSeenAt: Date.now(),
      joinRequests: 0,
      estimatedInputTokens: 0,
      estimatedOutputTokens: 0,
      estimatedTotalTokens: 0,
      estimatedCostUsd: 0,
      users: [],
    };
  }
  if (meetingName && meetingName.trim()) {
    stats.meetings[meetingId].meetingName = meetingName;
  }
  return stats.meetings[meetingId];
}

function recordMeetingJoinForQuota(userId: string, displayName: string, meetingId: string, meetingName?: string, tenantId?: string) {
  const stats = loadBotAdminStats();
  const user = upsertUserStats(userId, displayName, tenantId);
  const monthKey = getMonthKey();
  user.monthlyMeetingsJoined[monthKey] = (user.monthlyMeetingsJoined[monthKey] || 0) + 1;
  user.lastSeenAt = Date.now();

  const meetingUsage = getOrCreateMeetingUsage(meetingId, meetingName);
  meetingUsage.joinRequests += 1;
  meetingUsage.lastSeenAt = Date.now();
  if (!meetingUsage.users.includes(userId)) {
    meetingUsage.users.push(userId);
  }

  recordDailyMetrics(userId, { meetingsJoined: 1 });

  saveBotAdminStats();
}

function getUserMonthlyMeetingLimit(userId: string): number {
  const stats = loadBotAdminStats();
  const user = stats.users[userId];
  if (user?.monthlyMeetingLimitOverride && user.monthlyMeetingLimitOverride > 0) {
    return user.monthlyMeetingLimitOverride;
  }
  return stats.freeTierMonthlyMeetingLimit;
}

function autoBlockUser(userId: string, reason: string) {
  if (!AUTO_BLOCK_ON_POLICY_BREACH) return;
  const stats = loadBotAdminStats();
  const user = stats.users[userId];
  if (!user || user.blocked) return;
  user.blocked = true;
  user.blockReason = reason;
  user.blockedAt = Date.now();
  user.lastSeenAt = Date.now();
  saveBotAdminStats();
}

function canUserJoinMeetingThisMonth(userId: string): { allowed: boolean; used: number; limit: number; remaining: number } {
  const stats = loadBotAdminStats();
  const user = stats.users[userId];
  const limit = getUserMonthlyMeetingLimit(userId);
  const used = user?.monthlyMeetingsJoined?.[getMonthKey()] || 0;

  if (!stats.enforceGlobalLimits) {
    return {
      allowed: true,
      used,
      limit,
      remaining: Number.MAX_SAFE_INTEGER,
    };
  }

  const remaining = Math.max(limit - used, 0);
  if (used >= limit && user?.monthlyMeetingLimitOverride && user.monthlyMeetingLimitOverride > 0) {
    autoBlockUser(userId, 'monthly_meeting_limit_exceeded');
  }
  return {
    allowed: used < limit,
    used,
    limit,
    remaining,
  };
}

function canUserUseTokens(userId: string): { allowed: boolean; used: number; limit?: number } {
  const stats = loadBotAdminStats();
  const user = stats.users[userId];
  if (!user) {
    return { allowed: true, used: 0 };
  }
  const used = user.estimatedTotalTokens || 0;
  if (user.tokenPolicy === 'limited' && user.tokenLimit && user.tokenLimit > 0) {
    if (used >= user.tokenLimit) {
      autoBlockUser(userId, 'token_limit_exceeded');
    }
    return {
      allowed: used < user.tokenLimit,
      used,
      limit: user.tokenLimit,
    };
  }
  return { allowed: true, used };
}

function recordEstimatedModelUsage(entry: ModelUsageTracking) {
  const stats = loadBotAdminStats();
  const user = upsertUserStats(entry.userId, entry.displayName, entry.tenantId);
  const meetingId = entry.meetingId || '';
  const shouldAggregateMeeting = !!meetingId && isMeetingConversationId(meetingId);

  const inputTokens = estimateTokensFromText(entry.inputText);
  const outputTokens = estimateTokensFromText(entry.outputText);
  const totalTokens = inputTokens + outputTokens;
  const costUsd =
    (inputTokens / 1000) * stats.modelInputCostPer1kUsd +
    (outputTokens / 1000) * stats.modelOutputCostPer1kUsd;

  user.estimatedInputTokens += inputTokens;
  user.estimatedOutputTokens += outputTokens;
  user.estimatedTotalTokens += totalTokens;
  user.estimatedCostUsd += costUsd;
  user.lastSeenAt = Date.now();

  if (shouldAggregateMeeting) {
    const cachedMeetingName = getCachedMeetingContext(meetingId)?.subject;
    const meetingUsage = getOrCreateMeetingUsage(meetingId, entry.meetingName || cachedMeetingName);
    meetingUsage.estimatedInputTokens += inputTokens;
    meetingUsage.estimatedOutputTokens += outputTokens;
    meetingUsage.estimatedTotalTokens += totalTokens;
    meetingUsage.estimatedCostUsd += costUsd;
    meetingUsage.lastSeenAt = Date.now();
    if (!meetingUsage.users.includes(entry.userId)) {
      meetingUsage.users.push(entry.userId);
    }
  }

  stats.totalEstimatedInputTokens += inputTokens;
  stats.totalEstimatedOutputTokens += outputTokens;
  stats.totalEstimatedTokens += totalTokens;
  stats.totalEstimatedCostUsd += costUsd;

  recordDailyMetrics(entry.userId, {
    inputTokens,
    outputTokens,
    totalTokens,
    costUsd,
  });

  saveBotAdminStats();
}

async function sendPromptWithTracking(
  prompt: ChatPrompt,
  input: string,
  tracking?: {
    userId: string;
    displayName: string;
    tenantId?: string;
    meetingId: string;
    estimatedInputText: string;
  }
) {
  const response = await prompt.send(input);
  const responseText = extractModelResponseText(response);
  if (tracking) {
    recordEstimatedModelUsage({
      userId: tracking.userId,
      displayName: tracking.displayName,
      tenantId: tracking.tenantId,
      meetingId: tracking.meetingId,
      inputText: tracking.estimatedInputText,
      outputText: responseText,
    });
  }
  return response;
}

function recordMeetingEstablished(conversationId: string, meetingName?: string) {
  const stats = loadBotAdminStats();
  if (!stats.meetingConversationHistory.includes(conversationId)) {
    stats.meetingConversationHistory.push(conversationId);
    stats.totalMeetingsJoined += 1;
  }
  if (isMeetingConversationId(conversationId)) {
    const cachedMeetingName = getCachedMeetingContext(conversationId)?.subject;
    getOrCreateMeetingUsage(conversationId, meetingName || cachedMeetingName);
  }
  if (!stats.activeMeetingConversationIds.includes(conversationId)) {
    stats.activeMeetingConversationIds.push(conversationId);
  }
  saveBotAdminStats();
}

function recordMeetingTerminated(conversationId: string) {
  const stats = loadBotAdminStats();
  stats.activeMeetingConversationIds = stats.activeMeetingConversationIds.filter((id) => id !== conversationId);
  saveBotAdminStats();
}

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

function getActivityTenantId(activity: any): string | undefined {
  return (
    activity?.conversation?.tenantId ||
    activity?.channelData?.tenant?.id ||
    activity?.channelData?.tenantId ||
    activity?.value?.tenantId ||
    undefined
  );
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
function parseVTT(vtt: string): string {
  if (!vtt) return '';
  return vtt
    .replace(/\r\n/g, '\n')
    .split('\n')
    .filter((line) => {
      const trimmed = line.trim();
      return !!trimmed &&
        !trimmed.startsWith('WEBVTT') &&
        !trimmed.startsWith('NOTE') &&
        !trimmed.includes('-->') &&
        !/^\d+$/.test(trimmed);
    })
    .map((line) => line.replace(/<v\s+[^>]+>/g, '').replace(/<\/v>/g, '').trim())
    .filter(Boolean)
    .join('\n');
}

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
  showingPartial: boolean,
  tracking?: { userId: string; displayName: string; meetingId: string }
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

    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${meetingTitle}\n${participantNames.join(', ')}\n${JSON.stringify(transcriptData)}`,
    } : undefined);
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
  let md = `## 📝 Meeting Transcript\n\n`;
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

/**
 * Search all cached transcripts (files + meeting history) for a matching meeting by joinWebUrl.
 * Returns the transcript text if found in cache, null otherwise.
 */
function findCachedTranscriptByJoinUrl(joinWebUrl: string, afterTime?: number): string | null {
  try {
    const tolerance = 5 * 60 * 1000; // 5 minutes tolerance for time filtering
    let bestMatch: { content: string; startTime: number } | null = null;

    // 1. Check meeting history across all conversations for a matching joinWebUrl
    const historyStore = readMeetingHistoryStore();
    for (const [, entries] of Object.entries(historyStore)) {
      for (const entry of entries) {
        if (entry.joinWebUrl && areJoinUrlsEquivalent(entry.joinWebUrl, joinWebUrl)) {
          // If afterTime is specified, skip transcripts from older call sessions
          if (afterTime && entry.startTime < afterTime - tolerance) {
            continue;
          }
          if (entry.transcriptFilePath && fs.existsSync(entry.transcriptFilePath)) {
            const content = fs.readFileSync(entry.transcriptFilePath, 'utf-8').trim();
            if (content && content.length > 50) {
              if (!bestMatch || entry.startTime > bestMatch.startTime) {
                bestMatch = { content, startTime: entry.startTime };
              }
            }
          }
        }
      }
    }
    // Also check in-memory history
    for (const [, entries] of meetingHistoryMap) {
      for (const entry of entries) {
        if (entry.joinWebUrl && areJoinUrlsEquivalent(entry.joinWebUrl, joinWebUrl)) {
          if (afterTime && entry.startTime < afterTime - tolerance) {
            continue;
          }
          if (entry.transcriptFilePath && fs.existsSync(entry.transcriptFilePath)) {
            const content = fs.readFileSync(entry.transcriptFilePath, 'utf-8').trim();
            if (content && content.length > 50) {
              if (!bestMatch || entry.startTime > bestMatch.startTime) {
                bestMatch = { content, startTime: entry.startTime };
              }
            }
          }
        }
      }
    }

    if (bestMatch) {
      console.log(`[CACHE_FIRST] Found latest cached transcript (startTime=${new Date(bestMatch.startTime).toISOString()})`);
      return bestMatch.content;
    }

    // 2. Check transcriptMetaCache for matching meeting
    for (const [, meta] of transcriptMetaCache) {
      if (meta.filePath && fs.existsSync(meta.filePath)) {
        // If afterTime is specified, skip old transcripts
        if (afterTime && meta.fetchedAt < afterTime - tolerance) {
          continue;
        }
        const content = fs.readFileSync(meta.filePath, 'utf-8').trim();
        if (content && content.length > 50) {
          // Check if the file header contains the joinWebUrl's meeting thread
          const threadId = extractMeetingThreadId(joinWebUrl);
          if (threadId && content.includes(threadId)) {
            console.log(`[CACHE_FIRST] Found cached transcript via meta cache: ${meta.filePath}`);
            return content;
          }
        }
      }
    }
  } catch (err) {
    console.warn(`[CACHE_FIRST] Error searching cached transcripts:`, err);
  }
  return null;
}

/**
 * Parse cached transcript text (our formatted file format) back into TranscriptEntry[].
 * Format: "Speaker Name (H:MM:SS)\n  - text\n"
 */
function parseCachedTranscriptToEntries(content: string): TranscriptEntry[] {
  const entries: TranscriptEntry[] = [];
  // Match lines like: "Malvine Owuor (0:01:02)"
  const speakerLineRegex = /^(.+?)\s+\((\d+:\d{2}:\d{2})\)\s*$/;
  const textLineRegex = /^\s+-\s+(.+)$/;

  const lines = content.split('\n');
  let currentSpeaker = '';
  let currentTimestamp = '';

  for (const line of lines) {
    const speakerMatch = line.match(speakerLineRegex);
    if (speakerMatch) {
      currentSpeaker = speakerMatch[1].trim();
      currentTimestamp = speakerMatch[2];
      continue;
    }
    const textMatch = line.match(textLineRegex);
    if (textMatch && currentSpeaker) {
      entries.push({
        speaker: currentSpeaker,
        text: textMatch[1].trim(),
        timestamp: currentTimestamp,
        isFinal: true,
      });
    }
  }
  return entries;
}

/**
 * Cache-first transcript fetch. Checks local file cache before hitting Graph API.
 * Always returns parsed TranscriptEntry[] for consistent handling by callers.
 * Also returns raw VTT content when fetched from Graph (for callers that need it).
 */
async function fetchTranscriptCacheFirst(
  organizerId: string,
  joinWebUrl: string,
  startTime?: number,
  endTime?: number
): Promise<{ entries: TranscriptEntry[]; vttContent: string | null; fromCache: boolean }> {
  // 1. Check local cache first (instant) — pass startTime to filter old call sessions
  const cached = findCachedTranscriptByJoinUrl(joinWebUrl, startTime);
  if (cached) {
    const entries = parseCachedTranscriptToEntries(cached);
    if (entries.length > 0) {
      console.log(`[CACHE_FIRST] Returning ${entries.length} cached entries — skipping Graph API`);
      return { entries, vttContent: null, fromCache: true };
    }
    // Cache hit but couldn't parse — fall through to Graph
    console.log(`[CACHE_FIRST] Cache hit but parsed 0 entries, falling through to Graph API`);
  }

  // 2. Fall back to Graph API
  console.log(`[CACHE_FIRST] No cache hit, fetching from Graph API...`);
  const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
    organizerId,
    joinWebUrl,
    startTime,
    endTime
  );
  const entries = vttContent ? parseVttToEntries(vttContent) : [];
  return { entries, vttContent, fromCache: false };
}

/**
 * Poll for transcript availability — checks cache first, then Graph API.
 * Retries multiple times with increasing delays until transcript is ready or timeout.
 * @param organizerId - Meeting organizer's user ID
 * @param joinWebUrl - Meeting join URL
 * @param startTime - Optional meeting start time for filtering
 * @param endTime - Optional meeting end time for filtering
 * @param maxAttempts - Maximum number of retry attempts (default: 6)
 * @param initialDelayMs - Initial delay between retries (default: 5000ms)
 * @returns Object with success status, vttContent, and attempt count
 */
async function pollForTranscriptReady(
  organizerId: string,
  joinWebUrl: string,
  startTime?: number,
  endTime?: number,
  maxAttempts: number = 6,
  initialDelayMs: number = 5000
): Promise<{ success: boolean; vttContent: string | null; attempts: number; error?: string; fromCache?: boolean }> {
  // CACHE-FIRST: Check local files before any Graph API calls
  const cached = findCachedTranscriptByJoinUrl(joinWebUrl, startTime);
  if (cached) {
    console.log(`[TRANSCRIPT_POLL] Cache hit! Returning cached transcript (${cached.length} chars) — no Graph API needed`);
    return { success: true, vttContent: cached, attempts: 0, fromCache: true };
  }

  let attempts = 0;
  let delayMs = initialDelayMs;

  while (attempts < maxAttempts) {
    attempts++;
    console.log(`[TRANSCRIPT_POLL] Attempt ${attempts}/${maxAttempts} to fetch transcript...`);

    try {
      const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
        organizerId,
        joinWebUrl,
        startTime,
        endTime
      );

      if (vttContent) {
        const parsed = parseVttToEntries(vttContent);
        if (parsed.length > 0) {
          console.log(`[TRANSCRIPT_POLL] Transcript ready! ${parsed.length} entries found on attempt ${attempts}`);
          return { success: true, vttContent, attempts };
        }
      }

      // Transcript not ready yet, wait and retry
      if (attempts < maxAttempts) {
        console.log(`[TRANSCRIPT_POLL] Transcript not ready, waiting ${delayMs / 1000}s before retry...`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
        // Increase delay for next attempt (exponential backoff, capped at 30s)
        delayMs = Math.min(delayMs * 1.5, 30000);
      }
    } catch (error: any) {
      console.warn(`[TRANSCRIPT_POLL] Attempt ${attempts} failed:`, error?.message);
      if (attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, delayMs));
        delayMs = Math.min(delayMs * 1.5, 30000);
      }
    }
  }

  return { 
    success: false, 
    vttContent: null, 
    attempts,
    error: `Transcript not available after ${attempts} attempts. It may still be processing.`
  };
}

/** Save the current transcript for a conversation to a .txt file (Teams-like format). Non-blocking. */
function saveTranscriptToFile(conversationId: string) {
  // Fire-and-forget async save
  void saveTranscriptToFileAsync(conversationId);
}

// Pin transcript file paths per meeting so ongoing saves always update the same file
// Key: meetingKey (callId or onlineMeetingId or conversationId), Value: file path
const pinnedTranscriptPaths = new Map<string, string>();

/** Async implementation of transcript saving - non-blocking I/O */
async function saveTranscriptToFileAsync(conversationId: string): Promise<void> {
  try {
    const entries = liveTranscriptMap.get(conversationId);
    const finalEntries = entries?.filter(e => e.isFinal) || [];
    if (finalEntries.length === 0) return;

    // Ensure transcripts directory exists
    await fsPromises.mkdir(TRANSCRIPTS_DIR, { recursive: true });

    // Get meeting context for metadata
    const meetingContext = getCachedMeetingContext(conversationId);
    const liveSession = getLiveTranscriptSession(conversationId);
    const isLive = liveSession !== null;
    const meetingSubject = liveSession?.meetingSubject || meetingContext?.subject || 'Meeting';

    // Generate unique meeting ID based on call start time or current time
    const meetingStartTime = meetingContext?.callStartedAt || (liveSession ? Date.now() : Date.now());
    const meetingId = generateMeetingId(conversationId, meetingStartTime);

    // Build a unique filename tied to this specific meeting instance
    // Use callId (unique per call) or onlineMeetingId as the primary identifier
    const callId = liveSession?.callId || meetingContext?.callId || '';
    const onlineMeetingId = meetingContext?.onlineMeetingId || '';
    // Primary key: callId if available, else onlineMeetingId, else conversation-based fallback
    const meetingKey = callId
      ? callId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50)
      : onlineMeetingId
        ? onlineMeetingId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50)
        : conversationId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50);

    // PINNING: Reuse the same file path for the entire lifetime of this meeting.
    // Only create a new path if we've never saved for this meetingKey before.
    let filePath = pinnedTranscriptPaths.get(meetingKey);
    if (!filePath) {
      const safeSubject = meetingSubject.replace(/[^a-zA-Z0-9 _-]/g, '').trim().replace(/\s+/g, '_').slice(0, 40) || 'meeting';
      const startDate = new Date(meetingStartTime);
      const dateStr = startDate.toISOString().slice(0, 10);
      const hourMin = `${String(startDate.getHours()).padStart(2, '0')}_${String(startDate.getMinutes()).padStart(2, '0')}`;
      const prefix = isLive ? 'live' : 'transcript';
      // Format: {prefix}_{date}_{startTime}_{subject}_{meetingKey}.txt
      filePath = path.join(TRANSCRIPTS_DIR, `${prefix}_${dateStr}_${hourMin}_${safeSubject}_${meetingKey}.txt`);
      pinnedTranscriptPaths.set(meetingKey, filePath);
      console.log(`[TRANSCRIPT_FILE] Pinned file path for meeting ${meetingKey}: ${filePath}`);
    }

    // Calculate approximate meeting duration from last timestamp
    const lastTimestamp = finalEntries[finalEntries.length - 1]?.timestamp || '';
    const meetingDate = new Date(meetingStartTime);
    const formattedDate = meetingDate.toLocaleDateString('en-US', {
      year: 'numeric', month: 'long', day: 'numeric'
    });
    const formattedTime = meetingDate.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    // -- Header with meeting context --
    let content = '';
    content += `MEETING TRANSCRIPT\n`;
    content += `==================\n\n`;
    content += `Title: ${meetingSubject}\n`;
    content += `Date: ${formattedDate}\n`;
    content += `Time: ${formattedTime}\n`;
    if (lastTimestamp) content += `Duration: ~${formatVttTimestamp(lastTimestamp)}\n`;
    content += `Entries: ${finalEntries.length}\n`;
    content += `Status: ${isLive ? 'LIVE (in progress)' : 'Historical'}\n`;
    if (liveSession?.callId) content += `Call ID: ${liveSession.callId}\n`;
    if (meetingContext?.onlineMeetingId) content += `Meeting ID: ${meetingContext.onlineMeetingId}\n`;
    content += `Conversation: ${conversationId}\n`;
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

    await fsPromises.writeFile(filePath, content, 'utf-8');
    console.log(`[TRANSCRIPT_FILE] Saved ${finalEntries.length} entries to ${filePath}`);
    
    // Update cache with the file path
    if (liveSession?.callId) {
      const liveCacheKey = `live:${conversationId}:${liveSession.callId}`;
      const cached = transcriptMetaCache.get(liveCacheKey);
      if (cached) {
        cached.filePath = filePath;
        cached.fetchedAt = Date.now();
        cached.entryCount = finalEntries.length;
        cached.charCount = content.length;
      }
    }
    
    // Add to meeting history (top 5 meetings per conversation)
    const uniqueSpeakers = new Set(finalEntries.map(e => e.speaker));
    addMeetingToHistory(conversationId, {
      meetingId,
      subject: meetingSubject,
      startTime: meetingStartTime,
      endTime: meetingContext?.callEndedAt,
      transcriptFilePath: filePath,
      participantCount: uniqueSpeakers.size,
      entryCount: finalEntries.length,
      organizerId: meetingContext?.organizerId,
      joinWebUrl: meetingContext?.joinWebUrl,
      callId: liveSession?.callId || meetingContext?.callId,
    });
  } catch (err) {
    console.error(`[TRANSCRIPT_FILE_ERROR] Failed to save transcript:`, err);
  }
}

function getSafeConversationId(conversationId: string): string {
  return conversationId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 60);
}

function findLatestTranscriptFilePath(
  conversationId: string,
  meetingIdentifiers?: { callId?: string; onlineMeetingId?: string }
): string | null {
  try {
    if (!fs.existsSync(TRANSCRIPTS_DIR)) {
      return null;
    }

    const allFiles = fs.readdirSync(TRANSCRIPTS_DIR);
    let matches: string[] = [];

    // 1. BEST: Match by callId or onlineMeetingId (exact meeting)
    if (meetingIdentifiers?.callId) {
      const safeCallId = meetingIdentifiers.callId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50);
      matches = allFiles
        .filter((name) => name.includes(safeCallId) && name.endsWith('.txt'))
        .map((name) => path.join(TRANSCRIPTS_DIR, name));
      if (matches.length > 0) {
        console.log(`[TRANSCRIPT_CACHE] Found ${matches.length} file(s) by callId`);
      }
    }
    if (matches.length === 0 && meetingIdentifiers?.onlineMeetingId) {
      const safeMeetingId = meetingIdentifiers.onlineMeetingId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50);
      matches = allFiles
        .filter((name) => name.includes(safeMeetingId) && name.endsWith('.txt'))
        .map((name) => path.join(TRANSCRIPTS_DIR, name));
      if (matches.length > 0) {
        console.log(`[TRANSCRIPT_CACHE] Found ${matches.length} file(s) by onlineMeetingId`);
      }
    }

    // 2. FALLBACK: Match by conversation ID (legacy files or no meeting ID available)
    if (matches.length === 0) {
      const safeId = getSafeConversationId(conversationId);
      matches = allFiles
        .filter((name) =>
          (name.startsWith('transcript_') || name.startsWith('live_') || name.startsWith('cached_')) &&
          name.includes(safeId) && name.endsWith('.txt')
        )
        .map((name) => path.join(TRANSCRIPTS_DIR, name));
    }

    if (matches.length === 0) {
      return null;
    }

    // Return most recently modified
    matches.sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);
    return matches[0];
  } catch (error) {
    console.warn(`[TRANSCRIPT_CACHE] Failed to find cached transcript file:`, error);
    return null;
  }
}

function loadCachedTranscriptText(
  conversationId: string,
  meetingIdentifiers?: { callId?: string; onlineMeetingId?: string }
): string | null {
  try {
    const latestPath = findLatestTranscriptFilePath(conversationId, meetingIdentifiers);
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

/**
 * Get all available transcripts for a conversation from meeting history.
 * Returns array of { index, subject, startTime, content } sorted by most recent first.
 */
function getAllTranscriptsForConversation(conversationId: string): Array<{
  index: number;
  subject: string;
  startTime: number;
  meetingId: string;
  content: string;
}> {
  const history = getMeetingHistory(conversationId);
  const results: Array<{
    index: number;
    subject: string;
    startTime: number;
    meetingId: string;
    content: string;
  }> = [];
  
  for (let i = 0; i < history.length; i++) {
    const entry = history[i];
    const content = loadTranscriptFromHistoryEntry(entry);
    if (content) {
      results.push({
        index: i + 1, // 1-indexed for user display
        subject: entry.subject,
        startTime: entry.startTime,
        meetingId: entry.meetingId,
        content,
      });
    }
  }
  
  console.log(`[MEETING_HISTORY] Found ${results.length} transcripts for conversation ${conversationId}`);
  return results;
}

// ============================================================================
// BACKGROUND TRANSCRIPT WORKER - Fetches and caches last 5 meeting transcripts
// ============================================================================

interface CachedTranscriptMeta {
  transcriptId: string;
  meetingId: string;
  organizerId: string;
  createdDateTime: string;
  filePath: string;
  fetchedAt: number;
  charCount: number;
  meetingSubject?: string;
  conversationId?: string;
  callId?: string;
  entryCount: number;
  isLive: boolean; // True if this is from an active call
}

// In-memory cache of fetched transcripts metadata
const transcriptMetaCache = new Map<string, CachedTranscriptMeta>();

// Track known organizers for background fetching
const knownOrganizers = new Set<string>();

// --- MEETING CACHE: Store recent meetings per user for fast lookup ---
interface CachedMeeting {
  id: string;
  subject: string;
  joinWebUrl: string;
  organizerId: string;
  start: string;
  end: string;
  hasTranscript?: boolean;
}
interface UserMeetingCache {
  meetings: CachedMeeting[];
  fetchedAt: number;
}
const userMeetingCache = new Map<string, UserMeetingCache>();
const MEETING_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const MEETING_CACHE_MAX_PER_USER = 10;

/**
 * Get cached meetings for a user, or fetch from calendar if stale/missing.
 */
async function getCachedUserMeetings(userId: string, forceFresh = false): Promise<CachedMeeting[]> {
  const cached = userMeetingCache.get(userId);
  const now = Date.now();
  
  if (!forceFresh && cached && (now - cached.fetchedAt) < MEETING_CACHE_TTL_MS) {
    console.log(`[MEETING_CACHE] Returning ${cached.meetings.length} cached meetings for user ${userId}`);
    return cached.meetings;
  }
  
  // Fetch from calendar - look back 7 days
  console.log(`[MEETING_CACHE] Fetching fresh meetings for user ${userId}`);
  const weekAgo = new Date(now - 7 * 24 * 60 * 60 * 1000);
  const today = new Date();
  
  const result = await graphApiHelper.getCalendarEvents(
    userId,
    weekAgo.toISOString().split('T')[0],
    today.toISOString().split('T')[0]
  );
  
  if (!result.success || !result.events) {
    console.log(`[MEETING_CACHE] Failed to fetch calendar: ${result.error}`);
    return cached?.meetings || [];
  }
  
  // Filter to Teams meetings and map to our format
  const teamsMeetings: CachedMeeting[] = result.events
    .filter((evt: any) => evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl)
    .slice(0, MEETING_CACHE_MAX_PER_USER)
    .map((evt: any) => ({
      id: evt.id,
      subject: evt.subject || 'Untitled Meeting',
      joinWebUrl: evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl,
      organizerId: userId, // Will be resolved when needed
      start: evt.start?.dateTime,
      end: evt.end?.dateTime,
    }));
  
  userMeetingCache.set(userId, { meetings: teamsMeetings, fetchedAt: now });
  console.log(`[MEETING_CACHE] Cached ${teamsMeetings.length} meetings for user ${userId}`);
  
  return teamsMeetings;
}

/**
 * Find a meeting from cache by date and/or subject.
 */
async function findMeetingFromCache(
  userId: string,
  date?: string,
  subject?: string
): Promise<CachedMeeting | null> {
  const meetings = await getCachedUserMeetings(userId);
  
  let candidates = meetings;
  
  // Filter by date if provided
  if (date) {
    candidates = candidates.filter(m => m.start?.startsWith(date));
  }
  
  // Filter by subject if provided
  if (subject && candidates.length > 1) {
    const subjectLower = subject.toLowerCase();
    const matched = candidates.filter(m => 
      m.subject.toLowerCase().includes(subjectLower)
    );
    if (matched.length > 0) {
      candidates = matched;
    }
  }
  
  // Return most recent match
  return candidates.length > 0 ? candidates[0] : null;
}

// Track active live transcripts (conversationId -> { callId, startTime })
interface LiveTranscriptSession {
  callId: string;
  startTime: number;
  conversationId: string;
  meetingSubject?: string;
  lastUpdateTime: number;
}
const liveTranscriptSessions = new Map<string, LiveTranscriptSession>();

// Background worker state
let transcriptWorkerTimerId: ReturnType<typeof setTimeout> | undefined;
let transcriptWorkerRunning = false;
const TRANSCRIPT_WORKER_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
const TRANSCRIPT_WORKER_INITIAL_DELAY_MS = 30_000; // 30 seconds after startup
const MAX_TRANSCRIPTS_TO_CACHE = 5;
const TRANSCRIPT_CACHE_FRESHNESS_MS = 30 * 60 * 1000; // 30 minutes - skip re-fetch if recent

/**
 * Check if a conversation has an active live transcript session.
 */
function hasActiveLiveTranscript(conversationId: string): boolean {
  const session = liveTranscriptSessions.get(conversationId);
  if (!session) return false;
  // Consider session active if updated within the last 5 minutes
  return (Date.now() - session.lastUpdateTime) < 5 * 60 * 1000;
}

/**
 * Get the active live transcript session for a conversation.
 */
function getLiveTranscriptSession(conversationId: string): LiveTranscriptSession | null {
  const session = liveTranscriptSessions.get(conversationId);
  if (!session) return null;
  if ((Date.now() - session.lastUpdateTime) >= 5 * 60 * 1000) {
    // Session is stale
    return null;
  }
  return session;
}

/**
 * Register or update a live transcript session.
 */
function registerLiveTranscriptSession(
  conversationId: string,
  callId: string,
  meetingSubject?: string
) {
  const existing = liveTranscriptSessions.get(conversationId);
  const now = Date.now();
  
  if (existing && existing.callId === callId) {
    existing.lastUpdateTime = now;
    if (meetingSubject) existing.meetingSubject = meetingSubject;
  } else {
    liveTranscriptSessions.set(conversationId, {
      callId,
      startTime: now,
      conversationId,
      meetingSubject,
      lastUpdateTime: now,
    });
    console.log(`[LIVE_SESSION] Registered live transcript session for conversation=${conversationId}, callId=${callId}`);
  }
}

/**
 * End a live transcript session (call ended).
 */
function endLiveTranscriptSession(conversationId: string, callId?: string) {
  const existing = liveTranscriptSessions.get(conversationId);
  if (existing && (!callId || existing.callId === callId)) {
    liveTranscriptSessions.delete(conversationId);
    // Clear pinned file path so next meeting on same thread gets a fresh file
    const endedCallId = callId || existing.callId;
    if (endedCallId) {
      const safeCallId = endedCallId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 50);
      pinnedTranscriptPaths.delete(safeCallId);
    }
    console.log(`[LIVE_SESSION] Ended live transcript session for conversation=${conversationId}`);
  }
}

/**
 * Register an organizer ID for background transcript fetching.
 * Called when we discover meeting organizers from calls/chats.
 */
function registerOrganizerForBackgroundFetch(organizerId: string) {
  if (organizerId && !knownOrganizers.has(organizerId)) {
    knownOrganizers.add(organizerId);
    console.log(`[TRANSCRIPT_WORKER] Registered organizer for background fetch: ${organizerId}`);
  }
}

/**
 * Save a transcript to file asynchronously (non-blocking).
 * Returns the file path on success, null on failure.
 */
async function saveTranscriptToFileFromVtt(
  vttContent: string,
  meetingId: string,
  transcriptId: string,
  createdDateTime: string
): Promise<string | null> {
  try {
    await fsPromises.mkdir(TRANSCRIPTS_DIR, { recursive: true });

    // Parse the VTT to get entries
    const entries = parseVttToEntries(vttContent);
    if (entries.length === 0) {
      console.log(`[TRANSCRIPT_WORKER] No entries parsed from VTT for transcript ${transcriptId}`);
      return null;
    }

    // Build filename from meeting ID and date
    const safeMeetingId = meetingId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 40);
    const dateStr = createdDateTime ? createdDateTime.slice(0, 10) : new Date().toISOString().slice(0, 10);
    const filePath = path.join(TRANSCRIPTS_DIR, `cached_${dateStr}_${safeMeetingId}.txt`);

    // Format the transcript
    const createdDate = new Date(createdDateTime || Date.now());
    const formattedDate = createdDate.toLocaleDateString('en-US', {
      year: 'numeric', month: 'long', day: 'numeric'
    });
    const formattedTime = createdDate.toLocaleTimeString('en-US', {
      hour: 'numeric', minute: '2-digit'
    });

    let content = '';
    content += `CACHED MEETING TRANSCRIPT\n`;
    content += `=========================\n\n`;
    content += `Meeting ID: ${meetingId}\n`;
    content += `Transcript ID: ${transcriptId}\n`;
    content += `Date: ${formattedDate}\n`;
    content += `Time: ${formattedTime}\n`;
    content += `Entries: ${entries.length}\n`;
    content += `\n`;
    content += `TRANSCRIPT\n`;
    content += `----------\n\n`;

    let lastSpeaker = '';
    let lastTime = '';
    for (const entry of entries) {
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

    await fsPromises.writeFile(filePath, content, 'utf-8');
    console.log(`[TRANSCRIPT_WORKER] Cached transcript to ${filePath} (${entries.length} entries, ${content.length} chars)`);
    return filePath;
  } catch (err) {
    console.error(`[TRANSCRIPT_WORKER] Failed to save cached transcript:`, err);
    return null;
  }
}

/**
 * Background worker: Fetch and cache the last N transcripts for known organizers.
 * NOTE: This worker uses /users/{userId}/onlineMeetings/getAllTranscripts which requires
 * Application Access Policy. Since we only have RSC permissions, this worker is disabled.
 * Live transcripts still work via /chats/{chatId}/transcripts endpoint.
 * 
 * To enable: Configure Application Access Policy in Teams Admin PowerShell:
 *   New-CsApplicationAccessPolicy -Identity "MissaPolicy" -AppIds "678e7c4e-9b4b-402b-a6c3-f6892cb50674"
 *   Grant-CsApplicationAccessPolicy -PolicyName "MissaPolicy" -Global
 */
async function runTranscriptBackgroundWorker(): Promise<void> {
  console.log(`[TRANSCRIPT_WORKER] Background worker disabled - requires Application Access Policy`);
  console.log(`[TRANSCRIPT_WORKER] Live transcripts work via RSC (/chats/{chatId}/transcripts)`);
  // Worker is disabled - live transcript polling via /chats endpoint still works
}

/**
 * Schedule the next background worker run.
 */
function scheduleTranscriptWorker(delayMs: number = TRANSCRIPT_WORKER_INTERVAL_MS) {
  if (transcriptWorkerTimerId) {
    clearTimeout(transcriptWorkerTimerId);
  }

  transcriptWorkerTimerId = setTimeout(() => {
    void runTranscriptBackgroundWorker().finally(() => {
      // Re-schedule after completion
      scheduleTranscriptWorker();
    });
  }, delayMs);

  console.log(`[TRANSCRIPT_WORKER] Next run scheduled in ${Math.round(delayMs / 1000)}s`);
}

/**
 * Start the background transcript worker.
 * Called once at app startup.
 */
function startTranscriptBackgroundWorker() {
  console.log(`[TRANSCRIPT_WORKER] Initializing background worker (first run in ${TRANSCRIPT_WORKER_INITIAL_DELAY_MS / 1000}s)`);
  
  // Load all organizers from the meeting context store on startup
  try {
    const store = readMeetingContextStore();
    const organizerIds = new Set<string>();
    for (const entry of Object.values(store)) {
      if (entry.organizerId) {
        organizerIds.add(entry.organizerId);
      }
    }
    for (const organizerId of organizerIds) {
      knownOrganizers.add(organizerId);
    }
    console.log(`[TRANSCRIPT_WORKER] Loaded ${organizerIds.size} organizers from meeting context store`);
  } catch (err) {
    console.warn(`[TRANSCRIPT_WORKER] Failed to load organizers from meeting context:`, err);
  }
  
  scheduleTranscriptWorker(TRANSCRIPT_WORKER_INITIAL_DELAY_MS);
}

/**
 * Get cached transcript metadata (for quick lookups).
 */
function getCachedTranscriptMeta(meetingId: string): CachedTranscriptMeta[] {
  const results: CachedTranscriptMeta[] = [];
  for (const [key, meta] of transcriptMetaCache) {
    if (key.startsWith(meetingId + ':') || meta.meetingId === meetingId) {
      results.push(meta);
    }
  }
  return results;
}

/**
 * Get all cached transcript metadata sorted by date (newest first).
 */
function getAllCachedTranscriptMeta(): CachedTranscriptMeta[] {
  return Array.from(transcriptMetaCache.values())
    .sort((a, b) => new Date(b.createdDateTime).getTime() - new Date(a.createdDateTime).getTime());
}

/**
 * Load a cached transcript file content by meeting ID.
 */
async function loadCachedTranscriptByMeetingId(meetingId: string): Promise<string | null> {
  const metas = getCachedTranscriptMeta(meetingId);
  if (metas.length === 0) return null;

  // Use the most recent
  const latest = metas.sort((a, b) => new Date(b.createdDateTime).getTime() - new Date(a.createdDateTime).getTime())[0];

  try {
    const content = await fsPromises.readFile(latest.filePath, 'utf-8');
    console.log(`[TRANSCRIPT_CACHE] Loaded cached transcript for meeting ${meetingId} from ${latest.filePath}`);
    return content;
  } catch (err) {
    console.warn(`[TRANSCRIPT_CACHE] Failed to load cached file ${latest.filePath}:`, err);
    return null;
  }
}

// ============================================================================
// END BACKGROUND TRANSCRIPT WORKER
// ============================================================================

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
    console.log(`[CACHE_DEBUG] writeMeetingContextStore starting, dir: ${ADMIN_DATA_DIR}`);
    ensureAdminDataDir();
    console.log(`[CACHE_DEBUG] Writing to: ${MEETING_CONTEXT_FILE}`);
    fs.writeFileSync(MEETING_CONTEXT_FILE, JSON.stringify(store, null, 2), 'utf-8');
    console.log(`[CACHE_DEBUG] Write successful`);
  } catch (error) {
    console.warn(`[MEETING_CONTEXT] Failed to write context store (non-fatal):`, error);
    // Continue without file persistence - in-memory cache still works
  }
}

// ============================================================================
// MEETING HISTORY - Track top 5 most recent meetings per conversation
// ============================================================================

function readMeetingHistoryStore(): Record<string, MeetingHistoryEntry[]> {
  try {
    if (!fs.existsSync(MEETING_HISTORY_FILE)) {
      return {};
    }
    const raw = fs.readFileSync(MEETING_HISTORY_FILE, 'utf-8').trim();
    if (!raw) return {};
    const parsed = JSON.parse(raw) as Record<string, MeetingHistoryEntry[]>;
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (error) {
    console.warn(`[MEETING_HISTORY] Failed to read history store:`, error);
    return {};
  }
}

function writeMeetingHistoryStore(store: Record<string, MeetingHistoryEntry[]>) {
  try {
    ensureAdminDataDir();
    fs.writeFileSync(MEETING_HISTORY_FILE, JSON.stringify(store, null, 2), 'utf-8');
  } catch (error) {
    console.warn(`[MEETING_HISTORY] Failed to write history store (non-fatal):`, error);
  }
}

function loadMeetingHistoryIntoMemory() {
  const store = readMeetingHistoryStore();
  for (const [conversationId, entries] of Object.entries(store)) {
    meetingHistoryMap.set(conversationId, entries);
  }
  console.log(`[MEETING_HISTORY] Loaded history for ${Object.keys(store).length} conversations`);
}

function getMeetingHistory(conversationId: string): MeetingHistoryEntry[] {
  // Check memory first
  const memoryEntries = meetingHistoryMap.get(conversationId);
  if (memoryEntries) {
    return memoryEntries;
  }
  
  // Fallback to disk
  const store = readMeetingHistoryStore();
  const diskEntries = store[conversationId] || [];
  if (diskEntries.length > 0) {
    meetingHistoryMap.set(conversationId, diskEntries);
  }
  return diskEntries;
}

function addMeetingToHistory(
  conversationId: string,
  entry: MeetingHistoryEntry
): void {
  // Get existing history
  let history = meetingHistoryMap.get(conversationId) || [];
  
  // Check if this meeting already exists (by meetingId)
  const existingIndex = history.findIndex(h => h.meetingId === entry.meetingId);
  if (existingIndex >= 0) {
    // Update existing entry
    history[existingIndex] = { ...history[existingIndex], ...entry };
  } else {
    // Add new entry at the beginning (most recent first)
    history.unshift(entry);
    
    // Keep only top MAX_MEETING_HISTORY
    if (history.length > MAX_MEETING_HISTORY) {
      history = history.slice(0, MAX_MEETING_HISTORY);
    }
  }
  
  // Update memory
  meetingHistoryMap.set(conversationId, history);
  
  // Persist to disk
  const store = readMeetingHistoryStore();
  store[conversationId] = history;
  writeMeetingHistoryStore(store);
  
  console.log(`[MEETING_HISTORY] Updated history for ${conversationId}: ${history.length} meetings stored`);
}

function generateMeetingId(conversationId: string, startTime?: number): string {
  // Generate a unique meeting ID based on conversation and time
  const timestamp = startTime || Date.now();
  const safeConvId = conversationId.replace(/[^a-zA-Z0-9]/g, '').slice(-10);
  return `${safeConvId}_${timestamp}`;
}

function getLatestMeetingFromHistory(conversationId: string): MeetingHistoryEntry | null {
  const history = getMeetingHistory(conversationId);
  return history.length > 0 ? history[0] : null;
}

function loadTranscriptFromHistoryEntry(entry: MeetingHistoryEntry): string | null {
  if (!entry.transcriptFilePath) return null;
  try {
    if (!fs.existsSync(entry.transcriptFilePath)) {
      console.warn(`[MEETING_HISTORY] Transcript file not found: ${entry.transcriptFilePath}`);
      return null;
    }
    return fs.readFileSync(entry.transcriptFilePath, 'utf-8');
  } catch (error) {
    console.warn(`[MEETING_HISTORY] Failed to load transcript from ${entry.transcriptFilePath}:`, error);
    return null;
  }
}

function cacheMeetingContext(
  conversationId: string,
  organizerId: string,
  joinWebUrl: string,
  subject?: string,
  callWindow?: { startedAt?: number; endedAt?: number },
  callId?: string,
  onlineMeetingId?: string
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
      callId: callId || existing?.callId,
      onlineMeetingId: onlineMeetingId || existing?.onlineMeetingId,
    };
    console.log(`[CACHE_DEBUG] Entry created`);

    // Register the organizer for background transcript fetching
    if (organizerId) {
      registerOrganizerForBackgroundFetch(organizerId);
    }

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
    // Register the organizer for background transcript fetching
    registerOrganizerForBackgroundFetch(diskEntry.organizerId);
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
      cacheMeetingContext(
        conversationId,
        graphInfo.organizer.id,
        graphInfo.joinWebUrl,
        graphInfo.subject,
        undefined,
        undefined,
        (graphInfo as any).onlineMeetingId
      );
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
      onlineMeetingId: cached.onlineMeetingId,
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
  // from other occurrences in recurring meetings. Subtract 30 minutes from the start to
  // handle cases where transcription began before the bot joined the call.
  return {
    min: cached.callStartedAt - (30 * 60 * 1000),
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

/**
 * Parse a transcript text back into TranscriptEntry objects.
 * Handles multiple formats:
 *   - "Speaker (timestamp): text"
 *   - "[Speaker]: text"
 *   - "Speaker: text"
 */
function parseTranscriptTextToEntries(text: string): TranscriptEntry[] {
  if (!text || text.trim() === '') return [];

  const entries: TranscriptEntry[] = [];
  const lines = text.split('\n');
  const defaultTimestamp = '00:00:00.000';

  // Header keyword prefixes written by saveTranscriptToFileAsync — skip these lines
  const headerPrefixes = [
    'MEETING TRANSCRIPT', '==================', 'TRANSCRIPT', '----------',
    'Title:', 'Date:', 'Time:', 'Duration:', 'Entries:', 'Status:',
    'Call ID:', 'Meeting ID:', 'Conversation:',
  ];

  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    if (!trimmed || headerPrefixes.some(p => trimmed.startsWith(p))) {
      i++;
      continue;
    }

    // Pattern 1: "Speaker (00:00:00.000): text" — transcriptEntriesToPlainText format
    let match = trimmed.match(/^(.+?)\s*\((\d{2}:\d{2}:\d{2}\.\d{3})\):\s*(.+)$/);
    if (match) {
      entries.push({ speaker: match[1].trim(), text: match[3].trim(), timestamp: match[2], isFinal: true });
      i++;
      continue;
    }

    // Pattern 2: "Speaker (H:MM:SS): text" — compact with colon after closing paren
    match = trimmed.match(/^(.+?)\s*\((\d{1,2}:\d{2}:\d{2})\):\s*(.+)$/);
    if (match) {
      const parts = match[2].split(':');
      const paddedTs = `${parts[0].padStart(2, '0')}:${parts[1]}:${parts[2]}.000`;
      entries.push({ speaker: match[1].trim(), text: match[3].trim(), timestamp: paddedTs, isFinal: true });
      i++;
      continue;
    }

    // Pattern 3 (multi-line): "Speaker (H:MM:SS)" alone on a line, followed by "  - text" bullet lines
    // This is the format written by saveTranscriptToFileAsync
    match = trimmed.match(/^(.+?)\s*\((\d{1,2}:\d{2}:\d{2})\)$/);
    if (match) {
      const speaker = match[1].trim();
      const parts = match[2].split(':');
      const paddedTs = `${parts[0].padStart(2, '0')}:${parts[1]}:${parts[2]}.000`;
      const textParts: string[] = [];
      i++;
      while (i < lines.length) {
        const bulletMatch = lines[i].match(/^[ \t]*-[ \t]+(.+)$/);
        if (bulletMatch) {
          textParts.push(bulletMatch[1].trim());
          i++;
        } else {
          break;
        }
      }
      if (textParts.length > 0) {
        entries.push({ speaker, text: textParts.join(' '), timestamp: paddedTs, isFinal: true });
      }
      continue;
    }

    // Pattern 4: "[Speaker]: text"
    match = trimmed.match(/^\[(.+?)\]:\s*(.+)$/);
    if (match) {
      entries.push({ speaker: match[1].trim(), text: match[2].trim(), timestamp: defaultTimestamp, isFinal: true });
      i++;
      continue;
    }

    // Pattern 5: "Speaker: text" (generic fallback — limit speaker name to avoid false positives)
    match = trimmed.match(/^(.+?):\s*(.+)$/);
    if (match && match[1].length < 50) {
      entries.push({ speaker: match[1].trim(), text: match[2].trim(), timestamp: defaultTimestamp, isFinal: true });
    }

    i++;
  }

  return entries;
}

/**
 * Extract specific parts of transcript based on speaker, topic, or time range.
 * Used when user requests like "what John said", "first 10 minutes", "budget discussion".
 */
async function extractSpecificTranscriptContent(
  entries: TranscriptEntry[],
  specificRequest: string,
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<{ filtered: TranscriptEntry[]; description: string }> {
  const finalEntries = entries.filter(e => e.isFinal);
  if (finalEntries.length === 0) {
    return { filtered: [], description: 'No transcript entries available' };
  }

  // Try speaker-based filtering first (fast path)
  const speakerMatch = specificRequest.match(/what\s+(\w+)\s+said|(\w+)'s\s+(contributions?|comments?|points?)|only\s+(\w+)/i);
  if (speakerMatch) {
    const speakerName = (speakerMatch[1] || speakerMatch[2] || speakerMatch[4] || '').toLowerCase();
    if (speakerName) {
      const filtered = finalEntries.filter(e => 
        e.speaker.toLowerCase().includes(speakerName) ||
        e.speaker.split(' ')[0].toLowerCase() === speakerName
      );
      if (filtered.length > 0) {
        return { 
          filtered, 
          description: `${filtered.length} entries from ${filtered[0].speaker}` 
        };
      }
    }
  }

  // Try time-based filtering
  const timeMatch = specificRequest.match(/(first|last)\s+(\d+)\s+(minutes?|entries)/i);
  if (timeMatch) {
    const position = timeMatch[1].toLowerCase();
    const count = parseInt(timeMatch[2], 10);
    const unit = timeMatch[3].toLowerCase();
    
    if (unit.startsWith('entr')) {
      // Entry count based
      if (position === 'first') {
        return { 
          filtered: finalEntries.slice(0, count), 
          description: `First ${count} transcript entries` 
        };
      } else {
        return { 
          filtered: finalEntries.slice(-count), 
          description: `Last ${count} transcript entries` 
        };
      }
    } else {
      // Time-based (approximate - use entry timestamps)
      const minutesMs = count * 60 * 1000;
      if (finalEntries.length > 0) {
        // Parse first timestamp to get start time
        const parseTime = (ts: string) => {
          const match = ts.match(/(\d{2}):(\d{2}):(\d{2})/);
          if (!match) return 0;
          return parseInt(match[1]) * 3600 + parseInt(match[2]) * 60 + parseInt(match[3]);
        };
        
        const startSec = parseTime(finalEntries[0].timestamp);
        const endSec = parseTime(finalEntries[finalEntries.length - 1].timestamp);
        
        if (position === 'first') {
          const cutoffSec = startSec + (count * 60);
          const filtered = finalEntries.filter(e => parseTime(e.timestamp) <= cutoffSec);
          return { 
            filtered, 
            description: `First ${count} minutes (${filtered.length} entries)` 
          };
        } else {
          const cutoffSec = endSec - (count * 60);
          const filtered = finalEntries.filter(e => parseTime(e.timestamp) >= cutoffSec);
          return { 
            filtered, 
            description: `Last ${count} minutes (${filtered.length} entries)` 
          };
        }
      }
    }
  }

  // Use LLM for topic-based or complex extraction
  try {
    const transcriptPreview = finalEntries.slice(0, 100).map(e => 
      `[${e.speaker}]: ${e.text}`
    ).join('\n');
    
    const extractPrompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content:
            `User wants specific content: "${specificRequest}"\n\n` +
            `Analyze this transcript and identify which entries are relevant:\n${transcriptPreview}\n\n` +
            `Return JSON with:\n` +
            `1. "relevantSpeakers": array of speaker names whose content matches the request\n` +
            `2. "keywords": array of keywords to search for in transcript text\n` +
            `3. "description": brief description of what was found\n\n` +
            `Example: {"relevantSpeakers": ["John Smith"], "keywords": ["budget", "cost", "spending"], "description": "Budget discussion by John"}`
        }
      ],
      instructions: 'You help identify relevant transcript sections. Return valid JSON only.',
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    const response = await sendPromptWithTracking(extractPrompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${specificRequest}\n${transcriptPreview.substring(0, 500)}`,
    } : undefined);
    
    const jsonStr = (response.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    const parsed = JSON.parse(jsonStr);
    
    // Filter by speakers and/or keywords
    const relevantSpeakers = (parsed.relevantSpeakers || []).map((s: string) => s.toLowerCase());
    const keywords = (parsed.keywords || []).map((k: string) => k.toLowerCase());
    
    const filtered = finalEntries.filter(e => {
      const speakerMatch = relevantSpeakers.length === 0 || 
        relevantSpeakers.some((s: string) => e.speaker.toLowerCase().includes(s));
      const keywordMatch = keywords.length === 0 ||
        keywords.some((k: string) => e.text.toLowerCase().includes(k));
      return speakerMatch && keywordMatch;
    });
    
    return {
      filtered: filtered.length > 0 ? filtered : finalEntries.slice(0, 50),
      description: parsed.description || `Filtered content for "${specificRequest}"`
    };
  } catch (error) {
    console.warn(`[TRANSCRIPT_EXTRACT] LLM extraction failed, returning full transcript:`, error);
    return { 
      filtered: finalEntries, 
      description: 'Full transcript (specific extraction failed)' 
    };
  }
}

async function generateKeyMeetingInsights(
  transcriptText: string,
  meetingTitle: string,
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<string> {
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

  const response = await sendPromptWithTracking(insightsPrompt, '', tracking ? {
    ...tracking,
    estimatedInputText: `${meetingTitle}\n${transcriptText}`,
  } : undefined);
  return response.content || 'I could not generate insights from the transcript.';
}

// ---------------------------------------------------------------
// HTML & AI-Driven Summary Generators
// ---------------------------------------------------------------

async function generateMeetingSummary(
  entries: TranscriptEntry[],
  meetingTitle: string,
  speaker: string,
  tracking?: { userId: string; displayName: string; meetingId: string }
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
    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${meetingTitle}\n${speaker}\n${transcriptText}`,
    } : undefined);
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
  members: string[],
  meetingDate?: Date | string, // Optional: actual meeting date for past meetings
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<string> {
  try {
    const instructionsPath = path.join(__dirname, 'summaryFormatInstructions.txt');
    const instructions = fs.readFileSync(instructionsPath, 'utf-8');

    // Use provided meeting date or fall back to current time
    const actualDate = meetingDate ? new Date(meetingDate) : new Date();
    const dateStr = actualDate.toLocaleDateString('en-US', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
    });
    const timeStr = actualDate.toLocaleTimeString('en-US', {
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

    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${meetingTitle}\n${speaker}\n${members.join(', ')}\n${transcriptText}`,
    } : undefined);
    return response.content || 'Could not generate meeting summary. Please try again.';
  } catch (error) {
    console.error(`[SUMMARY_FORMAT_ERROR]`, error);
    return 'Error generating meeting summary. Please try again.';
  }
}

async function generateMinutesHtml(
  entries: TranscriptEntry[],
  meetingTitle: string,
  members: string[],
  meetingDate?: Date | string, // Optional: actual meeting date for past meetings
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<string> {
  if (entries.length === 0) return 'No transcript data available for minutes.';

  try {
    const instructionsPath = path.join(__dirname, 'minutesFormatInstructions.txt');
    const instructions = fs.readFileSync(instructionsPath, 'utf-8');

    // Use provided meeting date or fall back to current time
    const actualDate = meetingDate ? new Date(meetingDate) : new Date();
    const dateStr = actualDate.toLocaleDateString('en-US', {
      weekday: 'long', year: 'numeric', month: 'long', day: 'numeric'
    });
    const timeStr = actualDate.toLocaleTimeString('en-US', {
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

    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${meetingTitle}\n${members.join(', ')}\n${transcriptText}`,
    } : undefined);
    return response.content || 'Could not generate meeting minutes. Please try again.';
  } catch (error) {
    console.error(`[MINUTES_FORMAT_ERROR]`, error);
    return 'Error generating meeting minutes. Please try again.';
  }
}

/**
 * Follow-up detection patterns - words/phrases that indicate the user is referring to something just discussed.
 */
const FOLLOWUP_INDICATORS = [
  // Pronouns referring back
  /\b(it|that|this|the|those|these)\s+(call|meeting|email|event|appointment|schedule|message)\b/i,
  /\bthe\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+(call|meeting|one)\b/i,
  /\b(it|that|this)\b.*\b(start|end|begin|finish|last|duration|time|when|where|who)\b/i,
  // Implicit references
  /\bwhat\s+(time|is)\s+(it|that)\b/i,
  /\bwhen\s+(does|is|will)\s+(it|that)\b/i,
  /\bwho\s+(is|are|was|were)\s+(in|on|at)\s+(it|that|the)\b/i,
  /\b(end|start|begin|finish)\s+time\b/i,
  /\b(how long|duration|length)\b/i,
  // Corrections and clarifications
  /\bi\s+meant\b/i,
  /\bnot\s+(today|yesterday|tomorrow)\b.*\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i,
  /\bthe\s+(one|call|meeting)\s+(on|at|for)\b/i,
  // Follow-up questions
  /\bwhat\s+about\b/i,
  /\band\s+(what|when|where|who|how)\b/i,
  /\balso\b/i,
];

interface FollowupEnrichment {
  isFollowup: boolean;
  enrichedQuery: string;
  originalQuery: string;
  contextUsed: string | null;
}

/**
 * Fast follow-up detection and query enrichment.
 * Detects if the user's message is a follow-up to a recent conversation
 * and enriches the query with context so handlers can properly resolve references.
 */
async function detectAndEnrichFollowup(
  userMessage: string,
  recentTurns: Array<{ role: string; content: string }>,
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<FollowupEnrichment> {
  const trimmedMsg = (userMessage || '').trim();
  if (!trimmedMsg || recentTurns.length === 0) {
    return { isFollowup: false, enrichedQuery: trimmedMsg, originalQuery: trimmedMsg, contextUsed: null };
  }

  // Fast check: does the message match any follow-up patterns?
  const matchesPattern = FOLLOWUP_INDICATORS.some(pattern => pattern.test(trimmedMsg));
  
  // Also check for short queries that likely need context (less than 8 words)
  const wordCount = trimmedMsg.split(/\s+/).length;
  const isShortQuery = wordCount <= 8;
  
  // Check for implicit references (no explicit subject)
  const hasNoExplicitSubject = !/\b(my|the\s+\w+\s+(meeting|call|email))\b/i.test(trimmedMsg) && 
                               /\b(when|what|who|where|how|end|start|time)\b/i.test(trimmedMsg);
  
  if (!matchesPattern && !isShortQuery && !hasNoExplicitSubject) {
    return { isFollowup: false, enrichedQuery: trimmedMsg, originalQuery: trimmedMsg, contextUsed: null };
  }

  // Get the last bot response for context
  const lastBotResponse = recentTurns.filter(t => t.role === 'assistant').pop();
  const lastUserMessage = recentTurns.filter(t => t.role === 'user').slice(-2, -1)[0]; // Second to last user message
  
  if (!lastBotResponse) {
    return { isFollowup: false, enrichedQuery: trimmedMsg, originalQuery: trimmedMsg, contextUsed: null };
  }

  // Use LLM to quickly determine if this is a follow-up and extract context
  const contextSnippet = lastBotResponse.content.slice(0, 800);
  const prevUserSnippet = lastUserMessage?.content?.slice(0, 200) || '';

  try {
    const prompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content: `Quickly analyze if this is a follow-up question and enrich it with context.

PREVIOUS USER MESSAGE: "${prevUserSnippet}"
BOT'S LAST RESPONSE: "${contextSnippet}"
CURRENT USER MESSAGE: "${trimmedMsg}"

Is the current message a follow-up referring to something in the previous exchange?

If YES, rewrite the current message to be self-contained by including the relevant context.
Examples:
- "what is the end time for the call" + context about "Monday Armely call at 4pm" → "what is the end time for the Monday Armely Weekly Status Call"
- "who else is invited" + context about "Project Sync meeting" → "who else is invited to the Project Sync meeting"
- "send it to my email" + context about a summary → "send the meeting summary to my email"

If NO, return the original message unchanged.

Respond with JSON only: {"is_followup": true|false, "enriched_query": "the enriched or original query", "context_summary": "brief description of context used, or null"}`
        }
      ],
      instructions: 'You are a fast context resolver. Determine if the message refers to previous context and enrich it if needed. Be concise. Output valid JSON only.',
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${prevUserSnippet}\n${contextSnippet}\n${trimmedMsg}`,
    } : undefined);
    
    const rawResponse = (response.content || '').trim();
    const jsonStr = rawResponse.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
    const result = JSON.parse(jsonStr);
    
    console.log(`[FOLLOWUP] Detected: ${result.is_followup}, Enriched: "${result.enriched_query?.slice(0, 100)}"`);
    
    return {
      isFollowup: result.is_followup === true,
      enrichedQuery: result.enriched_query || trimmedMsg,
      originalQuery: trimmedMsg,
      contextUsed: result.context_summary || null,
    };
  } catch (error) {
    console.error(`[FOLLOWUP] Error in enrichment:`, error);
    return { isFollowup: false, enrichedQuery: trimmedMsg, originalQuery: trimmedMsg, contextUsed: null };
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
  | 'check_inbox'
  | 'reply_email'
  | 'send_email'
  | 'profile_details'
  | 'check_calendar'
  | 'general_chat';

async function classifyIntent(
  message: string,
  isMeetingConversation: boolean,
  tracking?: { userId: string; displayName: string; meetingId: string },
  recentContext?: string
): Promise<IntentLabel> {
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
          (recentContext ? `**Recent conversation (for follow-up resolution):**\n${recentContext}\n\n` : '') +
          `User message: "${text}"\n` +
          `Is this a meeting conversation: ${isMeetingConversation ? 'Yes' : 'No'}\n` +
          `${dateContext}\n\n` +
          `**Available intents:**\n` +
          `1. **join_meeting** - User wants the bot to join an ongoing call/meeting (e.g., "join the call", "come to the meeting", "join us")\n` +
          `2. **summarize** - User wants a NEW summary/recap generated (e.g., "summarize this meeting", "give me a recap", "what was discussed")\n` +
          `3. **minutes** - User wants NEW formal meeting notes/minutes generated (e.g., "create minutes", "meeting notes", "action items")\n` +
          `4. **transcribe** - User wants a NEW transcript generated (e.g., "transcribe the meeting", "get the transcript")\n` +
          `5. **meeting_overview** - User asks about a specific meeting's details (e.g., "tell me about the meeting", "what happened in my last meeting")\n` +
          `6. **insights** - User wants key insights/highlights (e.g., "key takeaways", "main points", "highlights")\n` +
          `7. **meeting_question** - User asks a specific question about meeting content (e.g., "what did John say about X", "when did we discuss Y")\n` +
          `8. **check_inbox** - User wants the bot to review mailbox content (e.g., "check my inbox", "show urgent emails", "what did Marin send me")\n` +
          `9. **reply_email** - User wants a reply drafted based on an email or email thread (e.g., "draft a reply to Marin", "respond after analyzing that email conversation")\n` +
          `10. **send_email** - User wants to send/email something. Includes ANY of these:\n` +
          `   - Sending previous content: "send to my inbox", "send it to my email", "email this to me", "email that"\n` +
          `   - Sending last output: "send the last summary/transcript/minutes to email"\n` +
          `   - Forward requests: "forward this", "send this"\n` +
          `   - Any mention of inbox/email as destination without asking for NEW content first\n` +
          `11. **profile_details** - User asks for their own profile details like "my email", "what is my email address", "my full details", "my profile"\n` +
          `12. **check_calendar** - User asks about their schedule, meetings, availability, or calendar\n` +
          `13. **general_chat** - Casual conversation, greetings, or anything that doesn't fit above\n\n` +
          `**Important rules:**\n` +
          `- "check my inbox", "urgent email", "what did Marin send me" = check_inbox\n` +
          `- "reply to Marin", "draft a response", "respond after analyzing that email" = reply_email\n` +
          `- Follow-up pronouns like "respond to him/her", "reply to them" = reply_email if prior context shows an email/inbox result\n` +
          `- Corrections like "I meant X not Y", "not X, I meant Y" = re-use prior context intent (reply_email if replying, check_inbox if browsing inbox)\n` +
          `- **EMAIL CONTENT FOLLOW-UPS**: If user asks about content/details of something someone "sent" or an email shown earlier = check_inbox\n` +
          `  - "what was the email about", "what did [name] send", "what was in that email" = check_inbox\n` +
          `  - "what was the meeting X sent" (asking about EMAIL content, not a live meeting) = check_inbox\n` +
          `  - "tell me more about that email", "what's in the summary leonard sent" = check_inbox\n` +
          `  - Key indicator: asking about what someone SENT (past tense) = likely email, not live meeting\n` +
          `- If user asks to EMAIL/SEND something without asking to CREATE new content first = send_email\n` +
          `- "send to my inbox", "email me", "send it", "forward this" = send_email\n` +
          `- "send the last summary to email" = send_email (not summarize!)\n` +
          `- "summarize AND email" = summarize (creating new content is primary)\n` +
          `- "my email", "what is my email", "my details", "my profile" = profile_details\n` +
          `- Questions about calendar/schedule/availability = check_calendar\n` +
          `- Greetings = general_chat\n\n` +
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
    const response = await sendPromptWithTracking(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: message,
    } : undefined);
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
      'check_inbox',
      'reply_email',
      'send_email',
      'profile_details',
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
  return (await getTranscriptWithContext(conversationId)).text;
}

/**
 * Transcript source type - helps differentiate live vs historical
 */
type TranscriptSource = 'live' | 'graph_fresh' | 'memory_cache' | 'file_cache' | 'background_cache' | 'none';

/**
 * Get transcript text with full context about its source
 */
async function getTranscriptWithContext(conversationId: string): Promise<{
  text: string;
  source: TranscriptSource;
  isLive: boolean;
  entryCount: number;
  meetingSubject?: string;
  callId?: string;
}> {
  const result = {
    text: '',
    source: 'none' as TranscriptSource,
    isLive: false,
    entryCount: 0,
    meetingSubject: undefined as string | undefined,
    callId: undefined as string | undefined,
  };

  // 1. FIRST: Check if there's an active LIVE transcript session for this conversation
  const liveSession = getLiveTranscriptSession(conversationId);
  if (liveSession) {
    console.log(`[TRANSCRIPT_FETCH] Active live session detected for ${conversationId} (callId=${liveSession.callId})`);
    const liveEntries = liveTranscriptMap.get(conversationId);
    const liveFinalEntries = liveEntries?.filter((e) => e.isFinal) || [];
    
    if (liveFinalEntries.length > 0) {
      console.log(`[TRANSCRIPT_FETCH] Using LIVE transcript (${liveFinalEntries.length} entries)`);
      result.text = transcriptEntriesToPlainText(liveFinalEntries);
      result.source = 'live';
      result.isLive = true;
      result.entryCount = liveFinalEntries.length;
      result.meetingSubject = liveSession.meetingSubject;
      result.callId = liveSession.callId;
      return result;
    }
    // Live session exists but no entries yet - continue to check other sources
    console.log(`[TRANSCRIPT_FETCH] Live session active but no entries captured yet`);
  }

  // 2. SECOND: Check in-memory cache (may have recent entries from polling)
  const cachedEntries = liveTranscriptMap.get(conversationId);
  const cachedFinalEntries = cachedEntries?.filter((e) => e.isFinal) || [];
  if (cachedFinalEntries.length > 0) {
    console.log(`[TRANSCRIPT_FETCH] Using in-memory cache (${cachedFinalEntries.length} entries)`);
    result.text = transcriptEntriesToPlainText(cachedFinalEntries);
    result.source = 'memory_cache';
    result.isLive = false;
    result.entryCount = cachedFinalEntries.length;
    return result;
  }

  // 3. THIRD: Check file cache (saved transcripts from this meeting)
  // Use meeting identifiers for precise lookup instead of just conversation ID
  const meetingContext = getCachedMeetingContext(conversationId);
  const meetingIds = {
    callId: liveSession?.callId || meetingContext?.callId,
    onlineMeetingId: meetingContext?.onlineMeetingId,
  };
  const cachedFile = loadCachedTranscriptText(conversationId, meetingIds);
  if (cachedFile) {
    console.log(`[TRANSCRIPT_FETCH] Using file cache for conversation (callId=${meetingIds.callId || 'none'}, meetingId=${meetingIds.onlineMeetingId || 'none'})`);
    result.text = cachedFile;
    result.source = 'file_cache';
    result.isLive = false;
    // Estimate entry count from content
    result.entryCount = (cachedFile.match(/\n\s+-\s/g) || []).length || 1;
    result.meetingSubject = meetingContext?.subject;
    result.callId = meetingIds.callId;
    return result;
  }

  // 4. FOURTH: Check background worker cache (pre-fetched transcripts)
  if (meetingContext?.onlineMeetingId) {
    const cachedMetas = getCachedTranscriptMeta(meetingContext.onlineMeetingId);
    if (cachedMetas.length > 0) {
      // Use the most recent cached transcript
      const latest = cachedMetas.sort((a, b) => 
        new Date(b.createdDateTime).getTime() - new Date(a.createdDateTime).getTime()
      )[0];
      
      const content = await loadCachedTranscriptByMeetingId(meetingContext.onlineMeetingId);
      if (content) {
        console.log(`[TRANSCRIPT_FETCH] Using background-cached transcript (meeting=${meetingContext.onlineMeetingId})`);
        result.text = content;
        result.source = 'background_cache';
        result.isLive = false;
        result.entryCount = latest.entryCount;
        result.meetingSubject = latest.meetingSubject;
        return result;
      }
    }
  }

  // 5. FIFTH: Fetch fresh from Graph API (only if cache misses)
  console.log(`[TRANSCRIPT_FETCH] Cache miss - fetching from Graph API for ${conversationId}`);
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
        console.log(`[TRANSCRIPT_FETCH] Graph API returned ${parsed.length} entries - caching result`);
        liveTranscriptMap.set(conversationId, parsed);
        saveTranscriptToFile(conversationId);
        result.text = transcriptEntriesToPlainText(parsed);
        result.source = 'graph_fresh';
        result.isLive = false;
        result.entryCount = parsed.length;
        return result;
      }
    }
  }

  console.log(`[TRANSCRIPT_FETCH] No transcript data available from any source`);
  return result;
}

/**
 * Get a summary of available transcripts for the user
 */
function getTranscriptAvailabilitySummary(conversationId: string): string {
  const parts: string[] = [];
  
  // Check live session
  const liveSession = getLiveTranscriptSession(conversationId);
  if (liveSession) {
    const liveEntries = liveTranscriptMap.get(conversationId)?.filter(e => e.isFinal) || [];
    parts.push(`📍 **Live call in progress** (${liveEntries.length} lines captured${liveSession.meetingSubject ? `, "${liveSession.meetingSubject}"` : ''})`);
  }
  
  // Check memory cache
  const memoryEntries = liveTranscriptMap.get(conversationId)?.filter(e => e.isFinal) || [];
  if (memoryEntries.length > 0 && !liveSession) {
    parts.push(`💾 **Recent transcript** in memory (${memoryEntries.length} entries)`);
  }
  
  // Check background cache
  const allCached = getAllCachedTranscriptMeta();
  if (allCached.length > 0) {
    const recent = allCached.slice(0, 3);
    parts.push(`📂 **${allCached.length} cached transcript(s)** from recent meetings:`);
    for (const meta of recent) {
      const date = new Date(meta.createdDateTime).toLocaleDateString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });
      parts.push(`   - ${meta.meetingSubject || 'Untitled meeting'} (${date}, ${meta.entryCount} entries)`);
    }
  }
  
  if (parts.length === 0) {
    return 'No transcripts available. Join a call or ask me to transcribe a past meeting.';
  }
  
  return parts.join('\n');
}

async function answerMeetingQuestionWithContext(
  question: string,
  meetingTitle: string,
  conversationContext: string,
  transcriptContext: string,
  tracking?: { userId: string; displayName: string; meetingId: string }
): Promise<string> {
  // Check for no-data scenarios and provide helpful guidance
  const hasTranscript = transcriptContext && transcriptContext.trim().length > 50;
  const hasConversation = conversationContext && conversationContext !== 'No conversation messages available.';
  
  if (!hasTranscript && !hasConversation) {
    return `I don't have any meeting content to answer your question yet.\n\n` +
      `**To get started:**\n` +
      `- Ask me to **join the call** during an active meeting to capture live transcript\n` +
      `- Or say **transcribe** after a recorded meeting to fetch the transcript from Teams\n` +
      `- Once I have transcript data, I can answer questions like "${question}"`;
  }

  // Build context-aware prompt based on what's available
  let contextSection = '';
  if (hasConversation) {
    contextSection += `\n\n**Chat Messages:**\n${conversationContext}`;
  }
  if (hasTranscript) {
    contextSection += `\n\n**Meeting Transcript:**\n${transcriptContext}`;
  }

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `User question about "${meetingTitle}": ${question}\n\n` +
          `Available context:${contextSection}\n\n` +
          `**Instructions:**\n` +
          `1. Answer the question directly based ONLY on the provided context\n` +
          `2. If asking about a specific person (e.g., "what did John say"), search for their name variations (John, John Smith, J. Smith)\n` +
          `3. If the information isn't in the context, clearly state that\n` +
          `4. Synthesize and paraphrase — do NOT copy-paste raw transcript lines or dump verbatim quotes\n` +
          `5. Only reference timestamps if the user specifically asks about timing\n` +
          `6. Tell the story naturally, as a knowledgeable colleague would explain it\n\n` +
          `How to format your response:\n` +
          `- Write a natural, conversational answer — no rigid section headers like "Answer", "Supporting Details", "Confidence"\n` +
          `- Summarize what was said in your own words, weaving in key phrases or short quotes only when they add flavor (not entire paragraphs)\n` +
          `- If the speaker told a story, joke, or anecdote, retell it concisely and naturally — capture the essence, not every line\n` +
          `- Keep it focused and readable — a few well-written paragraphs, not a wall of bullet points\n` +
          `- If you're unsure the context fully answers the question, mention that briefly at the end`
      },
    ],
    instructions:
      'You are a friendly meeting assistant. Answer questions using ONLY the provided context. ' +
      'If a speaker name is mentioned, try fuzzy matching (first name, last name, partial match). ' +
      'Never make up information not in the context. ' +
      'Be smart about what matters — read the raw transcript, understand it, and give a clear, natural answer. ' +
      'Do NOT dump raw transcript lines with timestamps. Paraphrase and synthesize instead.',
    model: new OpenAIChatModel({
      model: config.azureOpenAIDeploymentName,
      apiKey: config.azureOpenAIKey,
      endpoint: config.azureOpenAIEndpoint,
      apiVersion: '2024-10-21',
    }),
  });

  const response = await sendPromptWithTracking(prompt, '', tracking ? {
    ...tracking,
    estimatedInputText: `${question}\n${meetingTitle}\n${conversationContext}\n${transcriptContext}`,
  } : undefined);
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
const MAX_RETRIES = 20;            // 20 retries ~ 15s = 5 minutes

/**
 * Run a one-time diagnostic when a call is established.
 * Tries each transcript API once and logs the exact result/error so we can see
 * which permissions are missing or what error Graph returns.
 */
async function runTranscriptDiagnostic(
  conversationId: string,
  callId: string,
  organizerId?: string,
  onlineMeetingId?: string
) {
  console.log(`[TRANSCRIPT_DIAG] ===== Running transcript API diagnostic for callId=${callId} =====`);

  // Test 1: /chats/{chatId}/transcripts with graph token
  try {
    const graphToken = await (graphApiHelper as any).getTokenUsingClientCredentials();
    if (!graphToken) {
      console.log(`[TRANSCRIPT_DIAG] Test 1 SKIP: No graph client credentials token available`);
    } else {
      const encoded = encodeURIComponent(conversationId);
      const url = `https://graph.microsoft.com/v1.0/chats/${encoded}/transcripts`;
      console.log(`[TRANSCRIPT_DIAG] Test 1: GET ${url}`);
      const axios = require('axios');
      const resp = await axios.get(url, {
        headers: { Authorization: `Bearer ${graphToken}` },
        timeout: 10000,
      });
      const count = resp.data?.value?.length ?? 0;
      console.log(`[TRANSCRIPT_DIAG] Test 1 OK: /chats transcripts returned ${count} item(s) (status=${resp.status})`);
    }
  } catch (err: any) {
    const st = err?.response?.status;
    const msg = err?.response?.data?.error?.message || err?.message;
    const code = err?.response?.data?.error?.code || '';
    console.log(`[TRANSCRIPT_DIAG] Test 1 FAIL: /chats transcripts -> status=${st}, code=${code}, message=${msg}`);
  }

  // Test 2: /users/{org}/onlineMeetings/{mid}/transcripts with graph token
  if (organizerId && onlineMeetingId) {
    try {
      const graphToken = await (graphApiHelper as any).getTokenUsingClientCredentials();
      if (graphToken) {
        const url = `https://graph.microsoft.com/v1.0/users/${organizerId}/onlineMeetings/${onlineMeetingId}/transcripts`;
        console.log(`[TRANSCRIPT_DIAG] Test 2: GET ${url}`);
        const axios = require('axios');
        const resp = await axios.get(url, {
          headers: { Authorization: `Bearer ${graphToken}` },
          timeout: 10000,
        });
        const count = resp.data?.value?.length ?? 0;
        console.log(`[TRANSCRIPT_DIAG] Test 2 OK: /onlineMeetings transcripts returned ${count} item(s) (status=${resp.status})`);
      }
    } catch (err: any) {
      const st = err?.response?.status;
      const msg = err?.response?.data?.error?.message || err?.message;
      const code = err?.response?.data?.error?.code || '';
      console.log(`[TRANSCRIPT_DIAG] Test 2 FAIL: /onlineMeetings transcripts -> status=${st}, code=${code}, message=${msg}`);
    }
  }

  console.log(`[TRANSCRIPT_DIAG] ===== Diagnostic complete =====`);
}

/**
 * Attempt to start transcription on the call via Graph API.
 * Transcript retrieval is done through onlineMeetings transcript endpoints.
 * Retry a few times with delay since the call may not be fully ready immediately.
 */
async function attemptBotStartTranscription(callId: string, conversationId: string, serviceUrl: string) {
  const MAX_START_ATTEMPTS = 3;
  const START_RETRY_DELAY_MS = 5000;

  for (let attempt = 1; attempt <= MAX_START_ATTEMPTS; attempt++) {
    console.log(`[TRANSCRIPTION] Attempting to start transcription on callId=${callId} (attempt ${attempt}/${MAX_START_ATTEMPTS})`);
    const success = await graphApiHelper.startTranscription(callId);
    if (success) {
      console.log(`[TRANSCRIPTION] Bot-initiated transcription started successfully on callId=${callId}`);
      // Reset polling state on successful transcription start
      const polling = liveTranscriptPollingMap.get(callId);
      if (polling) {
        polling.consecutiveEmptyPolls = 0;
      }
      return;
    }

    if (attempt < MAX_START_ATTEMPTS) {
      console.log(`[TRANSCRIPTION] Will retry in ${START_RETRY_DELAY_MS / 1000}s...`);
      await new Promise(resolve => setTimeout(resolve, START_RETRY_DELAY_MS));
    }
  }
  console.log(`[TRANSCRIPTION] Could not start transcription after ${MAX_START_ATTEMPTS} attempts. User can still enable manually in Teams.`);
}

/**
 * Create a Graph subscription to receive notifications when transcripts are created on a chat.
 * POST /subscriptions with resource=/chats/{chatId}/transcripts
 */
async function createTranscriptSubscription(chatId: string, callId: string) {
  try {
    const graphToken = await (graphApiHelper as any).getTokenUsingClientCredentials();
    const botToken = await (graphApiHelper as any).getTokenUsingBotCredentials();
    const token = graphToken || botToken;
    if (!token) {
      console.log(`[TRANSCRIPT_SUB] No token available for subscription creation`);
      return;
    }

    const botEndpoint = process.env.BOT_ENDPOINT;
    if (!botEndpoint) {
      console.log(`[TRANSCRIPT_SUB] No BOT_ENDPOINT configured - cannot create subscription`);
      return;
    }

    const encoded = encodeURIComponent(chatId);
    const expirationDateTime = new Date(Date.now() + 60 * 60 * 1000).toISOString(); // 1 hour

    const payload = {
      changeType: 'created',
      notificationUrl: `${botEndpoint}/api/transcriptNotifications`,
      resource: `/chats/${encoded}/transcripts`,
      expirationDateTime,
      clientState: callId, // Use callId as client state for verification
    };

    console.log(`[TRANSCRIPT_SUB] Creating subscription for /chats/${chatId}/transcripts`);
    const axios = require('axios');
    const resp = await axios.post(
      'https://graph.microsoft.com/v1.0/subscriptions',
      payload,
      {
        headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
        timeout: 15000,
      }
    );
    console.log(`[TRANSCRIPT_SUB] Subscription created: id=${resp.data?.id}, expires=${resp.data?.expirationDateTime}`);
  } catch (err: any) {
    const st = err?.response?.status;
    const msg = err?.response?.data?.error?.message || err?.message;
    console.log(`[TRANSCRIPT_SUB] Subscription creation failed (status=${st}): ${msg} — polling will continue as fallback`);
  }
}



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
const LIVE_TRANSCRIPT_POLL_INTERVAL_MS = 10_000; // 10 seconds — poll via fetchMeetingTranscriptText

type MeetingStatusNoticeKey =
  | 'live_setup'
  | 'transcript_detected'
  | 'transcription_enabled_waiting';

const meetingStatusNoticeMap = new Map<string, Set<MeetingStatusNoticeKey>>();

async function sendMeetingStatusNoticeOnce(
  serviceUrl: string,
  conversationId: string,
  noticeKey: MeetingStatusNoticeKey,
  message: string
) {
  let sent = meetingStatusNoticeMap.get(conversationId);
  if (!sent) {
    sent = new Set<MeetingStatusNoticeKey>();
    meetingStatusNoticeMap.set(conversationId, sent);
  }
  if (sent.has(noticeKey)) {
    console.log(`[STATUS_NOTICE] Skipping duplicate notice (${noticeKey}) for conversation=${conversationId}`);
    return;
  }
  sent.add(noticeKey);
  await graphApiHelper.sendProactiveMessage(serviceUrl, conversationId, message);
}

function clearMeetingStatusNotices(conversationId: string) {
  meetingStatusNoticeMap.delete(conversationId);
}

function clearMeetingStatusNotice(conversationId: string, key: MeetingStatusNoticeKey) {
  const sent = meetingStatusNoticeMap.get(conversationId);
  if (sent) {
    sent.delete(key);
  }
}



function stopLiveTranscriptPolling(callId: string) {
  const polling = liveTranscriptPollingMap.get(callId);
  if (polling?.pollingTimerId) {
    clearTimeout(polling.pollingTimerId);
  }
  liveTranscriptPollingMap.delete(callId);
}

function extractMeetingCallIdFromActivityPayload(activity: any): string | null {
  const idRegex = /([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/i;
  const callIdRegex = /"callId"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"/i;
  const uriXmlRegex = /<Id\s+type="callId"\s+value="([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"\s*\/?>/i;

  const text = typeof activity?.text === 'string' ? activity.text : '';
  const candidates: string[] = [text];

  // Teams can send system metadata as non-text payloads.
  const payloadParts = [activity?.value, activity?.channelData, activity?.entities, activity?.attachments];
  for (const part of payloadParts) {
    if (part == null) continue;
    try {
      candidates.push(typeof part === 'string' ? part : JSON.stringify(part));
    } catch {
      // Ignore serialization failures for individual payload fragments.
    }
  }

  for (const raw of candidates) {
    if (!raw) continue;
    const xmlMatch = raw.match(uriXmlRegex);
    if (xmlMatch?.[1]) return xmlMatch[1].toLowerCase();

    const jsonMatch = raw.match(callIdRegex);
    if (jsonMatch?.[1]) return jsonMatch[1].toLowerCase();

    // Last-resort fallback when payload is malformed but still contains a callId token near "callId".
    if (/callId/i.test(raw)) {
      const anyId = raw.match(idRegex);
      if (anyId?.[1]) return anyId[1].toLowerCase();
    }
  }

  return null;
}

async function pollLiveTranscript(state: LiveTranscriptPollingState) {
  try {
    // Live transcript retrieval via fetchMeetingTranscriptText:
    // - Uses callStartTime filter to get transcripts from THIS session only
    // - Always downloads VTT (content updates even when transcript ID stays the same)
    // - Tracks progress via lastFetchedLineCount, not transcript ID
    const vttContent = await graphApiHelper.fetchMeetingTranscriptText(
      state.organizerId,
      state.joinWebUrl,
      state.callStartTime
    );

    if (!vttContent) {
      state.consecutiveEmptyPolls++;
      console.log(`[LIVE_TRANSCRIPT_POLL] No transcript data available yet for callId=${state.callId} (empty polls: ${state.consecutiveEmptyPolls})`);
      
      // After 3 attempts (30 seconds), note delay silently.
      if (state.consecutiveEmptyPolls === 3 && !state.userNotifiedAboutDelay) {
        state.userNotifiedAboutDelay = true;
      }
    } else {
      // Parse the full VTT content
      const allEntries = parseVttToEntries(vttContent);
      const convEntries = liveTranscriptMap.get(state.conversationId) || [];
      
      // Check if we got new entries since last poll
      if (allEntries.length > state.lastFetchedLineCount) {
        const newEntries = allEntries.slice(state.lastFetchedLineCount);
        console.log(`[LIVE_TRANSCRIPT_POLL] Got ${newEntries.length} new entries (total now: ${allEntries.length})`);
        
        // First time getting data after empty polls - silently continue (no spam)
        
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

/* REPLACED: old pollLiveTranscript was removed — simplified to single fetchMeetingTranscriptText call */

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
  
  // Start polling immediately, then every 5s
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
      `⏱️ **I gave up waiting to join the meeting.**\n\nI retried for 5 minutes but the call never became active. Please start the meeting and ask me to join again.`
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
          organizerId: meetingInfo.organizer?.id || getCachedMeetingContext(conversationId)?.organizerId,
          joinWebUrl: meetingInfo.joinWebUrl || getCachedMeetingContext(conversationId)?.joinWebUrl,
          onlineMeetingId: (meetingInfo as any)?.onlineMeetingId,
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

async function resolveCurrentUserProfile(activity: any, actorName?: string): Promise<{ displayName: string; email: string; aadObjectId: string; tenantId: string }> {
  const aadObjectId = activity?.from?.aadObjectId || activity?.from?.id || '';
  const displayName = normalizeDisplayName(actorName) || normalizeDisplayName(activity?.from?.name) || 'Unknown user';
  const tenantId = getActivityTenantId(activity) || '';

  let email = '';
  try {
    if (activity?.conversation?.id) {
      const members = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
      const normalizedActor = (actorName || '').toLowerCase().trim();
      const memberMatch = members.find((m) => m.userId === aadObjectId || (normalizedActor && m.displayName.toLowerCase() === normalizedActor));
      email = memberMatch?.email || '';
    }
  } catch (error) {
    console.warn('[PROFILE] Chat member email resolution failed:', error);
  }

  if (!email && aadObjectId) {
    try {
      const userInfo = await graphApiHelper.getUserInfo(aadObjectId);
      email = userInfo?.mail || userInfo?.userPrincipalName || '';
    } catch (error) {
      console.warn('[PROFILE] Graph user lookup failed:', error);
    }
  }

  return { displayName, email, aadObjectId, tenantId };
}

// Helper to detect if user wants to email results and extract email address
function detectEmailRequest(message: string): { wantsEmail: boolean; emailAddress: string | null; sendToAllAttendees: boolean } {
  const lower = (message || '').toLowerCase();
  const wantsEmail = 
    (lower.includes('send') && lower.includes('email')) ||
    (lower.includes('email') && (lower.includes('to') || lower.includes('me') || lower.includes('my'))) ||
    lower.includes('send it to') ||
    lower.includes('send to my') ||
    lower.includes('mail it') ||
    lower.includes('send this to');
  
  // Detect if user wants to send to all attendees/participants
  const sendToAllAttendees = 
    lower.includes('all attendees') ||
    lower.includes('all participants') ||
    lower.includes('everyone in') ||
    lower.includes('everyone on') ||
    lower.includes('send to everyone') ||
    lower.includes('email everyone') ||
    lower.includes('to all') ||
    lower.includes('all members');
  
  // Extract email address from message
  const emailMatch = message.match(/[\w.-]+@[\w.-]+\.\w+/i);
  const emailAddress = emailMatch ? emailMatch[0] : null;
  
  return { wantsEmail: wantsEmail || sendToAllAttendees, emailAddress, sendToAllAttendees };
}

async function resolveCalendarAttendeesForRequest(
  userId: string,
  requestText: string,
  conversationId?: string,
  meetingJoinUrl?: string // NEW: pass joinWebUrl to match exact meeting
): Promise<{ emails: string[]; names: string[]; meetingSubject?: string }> {
  try {
    // DEBUG - March 16 2026 - FORCE RESTART
    process.stdout.write(`\n[CALENDAR_ATTENDEES] ========== STARTING V2 ==========\n`);
    process.stdout.write(`[CALENDAR_ATTENDEES] userId=${userId}\n`);
    process.stdout.write(`[CALENDAR_ATTENDEES] meetingJoinUrl=${meetingJoinUrl ? 'YES' : 'NONE'}\n`);
    process.stdout.write(`[CALENDAR_ATTENDEES] conversationId=${conversationId || 'NONE'}\n`);
    
    const now = new Date();
    // Look back 7 days and forward 14 days - more focused range
    const start = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const end = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000);
    console.log(`[CALENDAR_ATTENDEES] Date range: ${start.toISOString()} to ${end.toISOString()}`);
    
    // If we have a joinWebUrl, try to get it from conversation context
    let targetJoinUrl = meetingJoinUrl || '';
    let conversationSubject = '';
    if (conversationId) {
      const context = getCachedMeetingContext(conversationId);
      if (context?.joinWebUrl && !targetJoinUrl) targetJoinUrl = context.joinWebUrl;
      conversationSubject = (context?.subject || '').toLowerCase();
      if (!targetJoinUrl || !conversationSubject) {
        const graphInfo = await resolveMeetingInfoForConversation(conversationId);
        if (graphInfo?.joinWebUrl && !targetJoinUrl) targetJoinUrl = graphInfo.joinWebUrl;
        if (graphInfo?.subject && !conversationSubject) conversationSubject = (graphInfo.subject || '').toLowerCase();
      }
      if (targetJoinUrl) {
        console.log(`[CALENDAR_ATTENDEES] Have meeting joinWebUrl to match: ${targetJoinUrl.slice(0, 80)}...`);
      }
      if (conversationSubject) {
        console.log(`[CALENDAR_ATTENDEES] Conversation subject: "${conversationSubject}"`);
      }
    }
    
    const calendarResult = await graphApiHelper.getCalendarEvents(userId, start.toISOString(), end.toISOString());
    if (!calendarResult.success) {
      console.log(`[CALENDAR_ATTENDEES] API failed: ${calendarResult.error}`);
      return { emails: [], names: [] };
    }
    if (!calendarResult.events?.length) {
      console.log(`[CALENDAR_ATTENDEES] No calendar events found in date range`);
      return { emails: [], names: [] };
    }
    console.log(`[CALENDAR_ATTENDEES] Found ${calendarResult.events.length} total calendar events`);
    
    // Debug: Show ALL events and why they might be filtered out
    const allEventSubjects = calendarResult.events.map((e: any) => e.subject).join(' | ');
    console.log(`[CALENDAR_ATTENDEES] ALL ${calendarResult.events.length} events: ${allEventSubjects.slice(0, 500)}`);
    
    // Check if "Test with Someone" is in raw results
    const testMeeting = calendarResult.events.find((e: any) => 
      (e.subject || '').toLowerCase().includes('test with someone')
    );
    if (testMeeting) {
      console.log(`[CALENDAR_ATTENDEES] FOUND "Test with Someone" in raw results!`);
      console.log(`[CALENDAR_ATTENDEES]   - onlineMeeting: ${JSON.stringify(testMeeting.onlineMeeting || 'NONE')}`);
      console.log(`[CALENDAR_ATTENDEES]   - onlineMeetingUrl: ${testMeeting.onlineMeetingUrl || 'NONE'}`);
      console.log(`[CALENDAR_ATTENDEES]   - attendees: ${JSON.stringify(testMeeting.attendees || 'NONE')}`);
    } else {
      console.log(`[CALENDAR_ATTENDEES] ⚠️ "Test with Someone" NOT in raw ${calendarResult.events.length} results!`);
    }

    const teamsMeetings = calendarResult.events.filter((evt: any) =>
      (evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl) && Array.isArray(evt.attendees) && evt.attendees.length > 0
    );
    if (teamsMeetings.length === 0) {
      console.log(`[CALENDAR_ATTENDEES] No Teams meetings with attendees found. Events without online meeting links or attendees were filtered out.`);
      return { emails: [], names: [] };
    }
    console.log(`[CALENDAR_ATTENDEES] Found ${teamsMeetings.length} Teams meetings with attendees`);
    
    // List ALL meeting subjects to help debug
    const allSubjects = teamsMeetings.map((e: any) => e.subject).join(' | ');
    console.log(`[CALENDAR_ATTENDEES] ALL meeting subjects: ${allSubjects}`);

    // PRIORITY 1: If we have a joinWebUrl, find EXACT match first
    if (targetJoinUrl) {
      const targetThreadId = extractMeetingThreadId(targetJoinUrl);
      process.stdout.write(`[CALENDAR_ATTENDEES] Looking for joinWebUrl match. Target threadId: "${targetThreadId}"\n`);
      
      // Debug: show first few calendar meeting URLs and their thread IDs
      teamsMeetings.slice(0, 5).forEach((evt: any, i: number) => {
        const evtUrl = evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl || '';
        const evtThreadId = extractMeetingThreadId(evtUrl);
        process.stdout.write(`[CALENDAR_ATTENDEES] Cal[${i}] "${evt.subject}": threadId="${evtThreadId || 'NONE'}"\n`);
      });
      
      const exactMatch = teamsMeetings.find((evt: any) => {
        const evtUrl = evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl || '';
        const evtThreadId = extractMeetingThreadId(evtUrl);
        // Match by thread ID (most reliable)
        if (targetThreadId && evtThreadId && targetThreadId === evtThreadId) {
          return true;
        }
        // Fallback: full URL comparison
        const normalizedTarget = normalizeMeetingJoinUrl(targetJoinUrl);
        const evtNorm = normalizeMeetingJoinUrl(evtUrl);
        return evtNorm === normalizedTarget;
      });
      
      if (exactMatch) {
        process.stdout.write(`[CALENDAR_ATTENDEES] ✓ Found EXACT joinWebUrl match: "${exactMatch.subject}"\n`);
        const uniqueRecipients = new Map<string, string>();
        for (const attendee of exactMatch.attendees || []) {
          const email = (attendee?.emailAddress?.address || '').trim().toLowerCase();
          const name = (attendee?.emailAddress?.name || email).trim();
          if (!email || !email.includes('@')) continue;
          if (!uniqueRecipients.has(email)) {
            uniqueRecipients.set(email, name);
          }
        }
        process.stdout.write(`[CALENDAR_ATTENDEES] Found ${uniqueRecipients.size} attendees for "${exactMatch.subject}"\n`);
        return {
          emails: Array.from(uniqueRecipients.keys()),
          names: Array.from(uniqueRecipients.values()),
          meetingSubject: exactMatch.subject || undefined,
        };
      } else {
        process.stdout.write(`[CALENDAR_ATTENDEES] ✗ No joinWebUrl match, trying subject match...\n`);
      }
    }
    
    // PRIORITY 2: Try EXACT subject match using conversation subject (group name)
    // This handles cases where joinWebUrl doesn't match but subject does
    if (conversationSubject) {
      process.stdout.write(`[CALENDAR_ATTENDEES] Looking for exact subject match: "${conversationSubject}"\n`);
      // List first 5 meeting subjects for comparison
      teamsMeetings.slice(0, 5).forEach((evt: any, i: number) => {
        process.stdout.write(`[CALENDAR_ATTENDEES] Cal[${i}] subject: "${evt.subject}"\n`);
      });
      const subjectMatch = teamsMeetings.find((evt: any) => {
        const evtSubject = (evt.subject || '').toLowerCase().trim();
        return evtSubject === conversationSubject;
      });
      
      if (subjectMatch) {
        console.log(`[CALENDAR_ATTENDEES] ✓ Found EXACT subject match: "${subjectMatch.subject}"`);
        const uniqueRecipients = new Map<string, string>();
        for (const attendee of subjectMatch.attendees || []) {
          const email = (attendee?.emailAddress?.address || '').trim().toLowerCase();
          const name = (attendee?.emailAddress?.name || email).trim();
          if (!email || !email.includes('@')) continue;
          if (!uniqueRecipients.has(email)) {
            uniqueRecipients.set(email, name);
          }
        }
        console.log(`[CALENDAR_ATTENDEES] Found ${uniqueRecipients.size} attendees for "${subjectMatch.subject}"`);
        return {
          emails: Array.from(uniqueRecipients.keys()),
          names: Array.from(uniqueRecipients.values()),
          meetingSubject: subjectMatch.subject || undefined,
        };
      } else {
        console.log(`[CALENDAR_ATTENDEES] ✗ No exact subject match found for "${conversationSubject}"`);
        // List available meeting subjects for debugging
        const availableSubjects = teamsMeetings.slice(0, 10).map((evt: any) => evt.subject).join(', ');
        console.log(`[CALENDAR_ATTENDEES] Available meetings: ${availableSubjects}`);
      }
    }
    
    // If we had a conversation context (joinWebUrl or subject), don't fall back to fuzzy matching
    if (targetJoinUrl || conversationSubject) {
      console.log(`[CALENDAR_ATTENDEES] No exact match found. NOT falling back to fuzzy matching to avoid wrong meeting.`);
      return { emails: [], names: [] };
    }
    
    // PRIORITY 3: ONLY use fuzzy matching if we DON'T have any conversation context
    // This is for cases like "list attendees for Monday standup" where user specifies a meeting

    // Fall back to keyword/subject matching
    const requestLower = (requestText || '').toLowerCase();
    const weekdayNames = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
    const requestedWeekday = weekdayNames.find((day) => requestLower.includes(day));
    const wantsTomorrow = /\btomorrow\b/i.test(requestText);
    const wantsToday = /\btoday\b/i.test(requestText);
    const wantsLast = /\b(last|previous|recent)\b/i.test(requestText);

    const stopWords = new Set([
      'list', 'show', 'get', 'all', 'attendees', 'participants', 'emails', 'email',
      'name', 'names', 'and', 'their', 'for', 'the', 'meeting', 'group', 'please',
      'last', 'previous', 'recent', 'current', 'this'
    ]);
    const requestTokens: string[] = (requestLower.match(/[a-z0-9]+/g) || [])
      .filter((token: string) => token.length > 2 && !stopWords.has(token));

    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());

    const scoreMeeting = (evt: any): number => {
      const subject = (evt.subject || '').toLowerCase();
      const startTime = new Date(evt.start?.dateTime || 0);
      const endTime = new Date(evt.end?.dateTime || evt.start?.dateTime || 0);
      let score = 0;

      // Conversation context matching (highest priority)
      if (conversationSubject) {
        if (subject === conversationSubject) score += 120;
        else if (subject.includes(conversationSubject) || conversationSubject.includes(subject)) score += 90;
      }

      // Request keyword matching
      if (requestTokens.length > 0) {
        const tokenMatches = requestTokens.filter((token) => subject.includes(token)).length;
        score += tokenMatches * 20;
        if (tokenMatches >= Math.min(2, requestTokens.length)) score += 25;
      }

      // Day-specific matching
      if (requestedWeekday && startTime.toLocaleDateString('en-US', { weekday: 'long' }).toLowerCase() === requestedWeekday) {
        score += 40;
      }
      if (wantsTomorrow) {
        const tomorrow = new Date(todayStart);
        tomorrow.setDate(tomorrow.getDate() + 1);
        if (startTime.toDateString() === tomorrow.toDateString()) score += 40;
      }
      if (wantsToday && startTime.toDateString() === todayStart.toDateString()) {
        score += 35;
      }

      // Active meeting gets high priority
      if (startTime <= now && endTime >= now) score += 50;
      
      // "Last/recent meeting" - boost recently ended meetings
      if (wantsLast && endTime < now && now.getTime() - endTime.getTime() < 24 * 60 * 60 * 1000) {
        score += 45;
      }
      
      // Today's meetings get a baseline boost
      if (startTime.toDateString() === todayStart.toDateString()) {
        score += 15;
      }
      
      // Upcoming meeting (within 6 hours) gets a small boost
      if (startTime > now && startTime.getTime() - now.getTime() < 6 * 60 * 60 * 1000) score += 10;
      
      // Penalize old meetings (more than 3 days ago)
      if (endTime < now && now.getTime() - endTime.getTime() > 3 * 24 * 60 * 60 * 1000) score -= 10;

      return score;
    };

    const rankedMeetings = teamsMeetings
      .map((evt: any) => ({ evt, score: scoreMeeting(evt) }))
      .sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        // Tie-breaker: prefer closest to now (active > just ended > upcoming)
        const aStart = new Date(a.evt.start?.dateTime || 0).getTime();
        const bStart = new Date(b.evt.start?.dateTime || 0).getTime();
        return Math.abs(aStart - now.getTime()) - Math.abs(bStart - now.getTime());
      });

    console.log(`[CALENDAR_ATTENDEES] Meeting rankings: ${rankedMeetings.slice(0, 5).map(m => `"${m.evt.subject}" (score=${m.score})`).join(', ')}`);

    // Determine if user explicitly specified search criteria (keywords, day names, dates)
    const hasExplicitUserAnchor = requestTokens.length > 0 || !!requestedWeekday || wantsTomorrow || wantsToday || wantsLast;
    const bestMatch = rankedMeetings[0];
    
    // Only reject if USER explicitly specified criteria that didn't match well
    // Conversation context alone shouldn't block selection - just use best available meeting
    if (!bestMatch || (hasExplicitUserAnchor && bestMatch.score < 30)) {
      // If no good match and no explicit criteria, fall back to the most recently ended meeting
      if (!hasExplicitUserAnchor && rankedMeetings.length > 0) {
        // Find the most recently ended meeting by end time
        const recentlyEnded = rankedMeetings
          .filter(m => new Date(m.evt.end?.dateTime || 0).getTime() <= now.getTime())
          .sort((a, b) => new Date(b.evt.end?.dateTime || 0).getTime() - new Date(a.evt.end?.dateTime || 0).getTime());
        if (recentlyEnded.length > 0) {
          console.log(`[CALENDAR_ATTENDEES] No explicit criteria — defaulting to most recently ended meeting: "${recentlyEnded[0].evt.subject}"`);
          // Use this meeting instead of failing
          const fallback = recentlyEnded[0];
          rankedMeetings[0] = fallback;
        } else {
          console.log(`[CALENDAR_ATTENDEES] No suitable match found. hasExplicitUserAnchor=${hasExplicitUserAnchor}, bestScore=${bestMatch?.score ?? 'none'}`);
          return { emails: [], names: [] };
        }
      } else {
        console.log(`[CALENDAR_ATTENDEES] No suitable match found. hasExplicitUserAnchor=${hasExplicitUserAnchor}, bestScore=${bestMatch?.score ?? 'none'}`);
        return { emails: [], names: [] };
      }
    }

    const targetMeeting = bestMatch.evt;
    console.log(`[CALENDAR_ATTENDEES] Selected meeting: "${targetMeeting.subject}" (score=${bestMatch.score})`);
    console.log(`[CALENDAR_ATTENDEES] Meeting has ${(targetMeeting.attendees || []).length} attendees in calendar`);

    const uniqueRecipients = new Map<string, string>();
    for (const attendee of targetMeeting.attendees || []) {
      const email = (attendee?.emailAddress?.address || '').trim().toLowerCase();
      const name = (attendee?.emailAddress?.name || email).trim();
      if (!email || !email.includes('@')) continue;
      if (!uniqueRecipients.has(email)) {
        uniqueRecipients.set(email, name);
      }
    }

    console.log(`[CALENDAR_ATTENDEES] Found ${uniqueRecipients.size} attendees for "${targetMeeting.subject}"`);

    return {
      emails: Array.from(uniqueRecipients.keys()),
      names: Array.from(uniqueRecipients.values()),
      meetingSubject: targetMeeting.subject || undefined,
    };
  } catch (error) {
    console.warn('[CALENDAR_ATTENDEES] Calendar attendee resolution failed:', error);
    return { emails: [], names: [] };
  }
}

function normalizeMeetingJoinUrl(joinUrl?: string): string {
  const raw = (joinUrl || '').trim();
  if (!raw) return '';
  try {
    const parsed = new URL(raw);
    const host = parsed.hostname.toLowerCase();
    const path = decodeURIComponent(parsed.pathname || '').toLowerCase().replace(/\/+$/, '');
    return `${host}${path}`;
  } catch {
    return raw.toLowerCase();
  }
}

/**
 * Extract the meeting thread ID from a Teams joinWebUrl.
 * Returns the thread ID portion like "19:meeting_abc123@thread.v2" or empty string.
 */
function extractMeetingThreadId(joinUrl?: string): string {
  if (!joinUrl) return '';
  try {
    // URL-decode the path
    const decoded = decodeURIComponent(joinUrl);
    // Match the meeting thread ID pattern: 19:meeting_[base64]@thread.v2
    const match = decoded.match(/19:meeting_[A-Za-z0-9_-]+@thread\.v2/i);
    return match ? match[0].toLowerCase() : '';
  } catch {
    return '';
  }
}

function areJoinUrlsEquivalent(left?: string, right?: string): boolean {
  const a = normalizeMeetingJoinUrl(left);
  const b = normalizeMeetingJoinUrl(right);
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
}

function isGenericMeetingTitle(title?: string): boolean {
  if (!title) return true;
  const normalized = title.trim().toLowerCase();
  return ['meeting', 'untitled meeting', 'microsoft teams meeting', 'teams meeting', 'meet now'].includes(normalized);
}

async function resolveDisplayMeetingTitle(
  conversationId: string,
  userId: string,
  preferredTitle?: string,
  transcriptTitle?: string
): Promise<string> {
  // Try each candidate in priority order
  const candidates = [
    preferredTitle,
    getCachedMeetingContext(conversationId)?.subject,
    transcriptTitle,
  ];

  const bestCandidate = candidates.find(c => c && !isGenericMeetingTitle(c));
  if (bestCandidate) return bestCandidate;

  // All candidates are generic — try calendar lookup
  try {
    const calendarResult = await resolveCalendarAttendeesForConversationOnly(userId, conversationId);
    if (calendarResult.meetingSubject && !isGenericMeetingTitle(calendarResult.meetingSubject)) {
      return calendarResult.meetingSubject;
    }
  } catch {
    // Ignore calendar lookup failures
  }

  // Fall back to first non-empty candidate or generic default
  return candidates.find(c => !!c) || 'Meeting';
}

async function resolveCalendarAttendeesForConversationOnly(
  userId: string,
  conversationId: string
): Promise<{ emails: string[]; names: string[]; meetingSubject?: string }> {
  try {
    console.log(`[CALENDAR_ATTENDEES_STRICT] Starting for conversation ${conversationId}`);
    const meetingInfo = await resolveMeetingInfoForConversation(conversationId);
    const targetJoinUrl = meetingInfo?.joinWebUrl || getCachedMeetingContext(conversationId)?.joinWebUrl;
    const meetingSubjectFromChat = meetingInfo?.subject || getCachedMeetingContext(conversationId)?.subject;
    
    if (!targetJoinUrl && !meetingSubjectFromChat) {
      console.log(`[CALENDAR_ATTENDEES_STRICT] No joinWebUrl or subject for conversation ${conversationId}`);
      return { emails: [], names: [] };
    }
    
    const targetThreadId = extractMeetingThreadId(targetJoinUrl);
    console.log(`[CALENDAR_ATTENDEES_STRICT] Looking for threadId="${targetThreadId}" or subject="${meetingSubjectFromChat}"`);

    // Use same date range as list_attendees: 7 days back, 14 days forward
    const now = new Date();
    const start = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const end = new Date(now.getTime() + 14 * 24 * 60 * 60 * 1000);
    const calendarResult = await graphApiHelper.getCalendarEvents(userId, start.toISOString(), end.toISOString());
    if (!calendarResult.success || !calendarResult.events?.length) {
      console.log(`[CALENDAR_ATTENDEES_STRICT] No calendar events found`);
      return { emails: [], names: [] };
    }

    const teamsMeetings = calendarResult.events.filter((evt: any) =>
      (evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl) && Array.isArray(evt.attendees) && evt.attendees.length > 0
    );
    console.log(`[CALENDAR_ATTENDEES_STRICT] Found ${teamsMeetings.length} Teams meetings with attendees`);

    // PRIORITY 1: Match by thread ID (most reliable)
    let targetMeeting = teamsMeetings.find((evt: any) => {
      const evtUrl = evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl || '';
      const evtThreadId = extractMeetingThreadId(evtUrl);
      return targetThreadId && evtThreadId && targetThreadId === evtThreadId;
    });

    // PRIORITY 2: Match by exact subject if thread ID didn't match
    if (!targetMeeting && meetingSubjectFromChat) {
      const subjectLower = meetingSubjectFromChat.toLowerCase().trim();
      targetMeeting = teamsMeetings.find((evt: any) => 
        (evt.subject || '').toLowerCase().trim() === subjectLower
      );
      if (targetMeeting) {
        console.log(`[CALENDAR_ATTENDEES_STRICT] Matched by subject: "${targetMeeting.subject}"`);
      }
    }

    if (!targetMeeting) {
      console.log(`[CALENDAR_ATTENDEES_STRICT] No exact meeting match found for conversation ${conversationId}`);
      return { emails: [], names: [] };
    }

    const uniqueRecipients = new Map<string, string>();
    for (const attendee of targetMeeting.attendees || []) {
      const email = (attendee?.emailAddress?.address || '').trim().toLowerCase();
      const name = (attendee?.emailAddress?.name || email).trim();
      if (!email || !email.includes('@')) continue;
      if (!uniqueRecipients.has(email)) {
        uniqueRecipients.set(email, name);
      }
    }

    console.log(`[CALENDAR_ATTENDEES_STRICT] Found ${uniqueRecipients.size} attendees for meeting "${targetMeeting.subject || 'unknown'}"`);
    return {
      emails: Array.from(uniqueRecipients.keys()),
      names: Array.from(uniqueRecipients.values()),
      meetingSubject: targetMeeting.subject || undefined,
    };
  } catch (error) {
    console.warn('[CALENDAR_ATTENDEES_STRICT] Failed to resolve attendees for current conversation:', error);
    return { emails: [], names: [] };
  }
}

async function autoEmailSummaryToParticipants(
  chatId: string,
  senderUserId: string,
  summarySubject: string,
  summaryBody: string
): Promise<{ sentCount: number; failedCount: number; skippedCount: number }> {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  const uniqueRecipients = new Map<string, string>();

  // 1. Get chat members
  const members = await graphApiHelper.getChatMembersDetailed(chatId);
  for (const m of members) {
    const nameLower = (m.displayName || '').toLowerCase();
    if (nameLower.includes('bot') || nameLower === 'assistant') {
      continue;
    }
    const email = (m.email || '').trim().toLowerCase();
    if (emailRegex.test(email) && !uniqueRecipients.has(email)) {
      uniqueRecipients.set(email, m.displayName || email);
    }
  }

  // 2. Get calendar attendees for THIS conversation's exact meeting only.
  // Never use broad/fuzzy meeting selection for auto-email.
  try {
    const calendarResult = await resolveCalendarAttendeesForConversationOnly(senderUserId, chatId);
    if (calendarResult.emails?.length) {
      console.log(`[AUTO_EMAIL] Found ${calendarResult.emails.length} calendar attendees for current meeting (subject: ${calendarResult.meetingSubject || 'unknown'})`);
      for (let i = 0; i < calendarResult.emails.length; i++) {
        const email = (calendarResult.emails[i] || '').trim().toLowerCase();
        const name = calendarResult.names[i] || email;
        // Skip bot-like names
        const nameLower = name.toLowerCase();
        if (nameLower.includes('bot') || nameLower === 'assistant') continue;
        if (emailRegex.test(email) && !uniqueRecipients.has(email)) {
          uniqueRecipients.set(email, name);
          console.log(`[AUTO_EMAIL] Added calendar attendee: ${name} <${email}>`);
        }
      }
    }
  } catch (calErr) {
    console.warn('[AUTO_EMAIL] Could not fetch calendar attendees:', calErr);
  }

  if (!uniqueRecipients.size) {
    return { sentCount: 0, failedCount: 0, skippedCount: 0 };
  }

  // Safety brake: prevent accidental broad fan-out.
  const maxAutoEmailRecipients = Math.max(1, Number(process.env.MAX_AUTO_EMAIL_RECIPIENTS || 25));
  if (uniqueRecipients.size > maxAutoEmailRecipients) {
    console.warn(
      `[AUTO_EMAIL] BLOCKED: recipient count ${uniqueRecipients.size} exceeds safety cap ${maxAutoEmailRecipients}. ` +
      `No emails sent for conversation ${chatId}.`
    );
    return { sentCount: 0, failedCount: 0, skippedCount: uniqueRecipients.size };
  }

  console.log(`[AUTO_EMAIL] Sending to ${uniqueRecipients.size} unique recipients (current chat + current meeting calendar only)`);

  let sentCount = 0;
  let failedCount = 0;

  for (const [email] of uniqueRecipients) {
    const sendResult = await graphApiHelper.sendEmail(
      senderUserId,
      email,
      summarySubject,
      summaryBody
    );
    if (sendResult.success) {
      sentCount += 1;
    } else {
      failedCount += 1;
    }
  }

  return {
    sentCount,
    failedCount,
    skippedCount: members.length - uniqueRecipients.size,
  };
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
};

// Only pass tenantId for SingleTenant bots — MultiTenant bots must omit it
// so the Bot Framework connector accepts tokens from any tenant
const isMultiTenant = (process.env.BOT_TYPE || '').toLowerCase() === 'multitenant';
if (!isMultiTenant) {
  credentialOptions.tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;
}

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

// Load meeting history into memory at startup
loadMeetingHistoryIntoMemory();

// Start the background transcript worker (non-blocking)
startTranscriptBackgroundWorker();

// Set the token factory for GraphApiHelper (works for both MSI and SingleTenant)
{
  const graphTokenFactory = async (): Promise<string> => {
    const scopeFactory = createTokenFactory();
    return scopeFactory('https://graph.microsoft.com/.default');
  };
  graphApiHelper.setTokenFactory(graphTokenFactory);
}

// Create the smart Intent Agent for routing decisions
const intentAgent = createIntentAgent(sendPromptWithTracking);
console.log('[INIT] Intent Agent created - smart routing enabled');

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
app.on('message', async ({ send: sendActivity, stream, activity }) => {
  const send = async (outgoing: any) => {
    try {
      await sendActivity(outgoing);
      const text = typeof outgoing === 'string'
        ? outgoing
        : (outgoing?.text || outgoing?.summary || '[non-text activity]');
      console.log(`[TEAMS_SEND_OK] conversation=${activity?.conversation?.id || 'unknown'} preview="${getTruncatedLogPreview(String(text || ''))}"`);
    } catch (error: any) {
      const status = error?.response?.status || error?.response?.statusCode;
      console.error(`[TEAMS_SEND_FAIL] conversation=${activity?.conversation?.id || 'unknown'} status=${status || 'unknown'}`);
      // Don't crash the handler on 403 (stale conversation) or 429 (throttled)
      if (status === 403 || status === 429) {
        console.warn(`[TEAMS_SEND_FAIL] Non-fatal ${status} — conversation may be stale or throttled`);
        return;
      }
      throw error;
    }
  };
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

  // Ignore system metadata payloads; user intents come from normal text messages.
  const msgText = activity.text || '';
  const looksSystemPayload =
    !msgText.trim() ||
    msgText.includes('<URIObject') ||
    msgText.includes('"callId"') ||
    msgText.trim().startsWith('{') ||
    !!extractMeetingCallIdFromActivityPayload(activity);
  if (looksSystemPayload) {
    return;
  }

  try {
    // Get user's display name for personalization
    const userName = await getUserDisplayName(activity.from.id, activity.from?.name);
    const actorName = activity.from?.name || userName;
    const requesterId = activity.from.aadObjectId || activity.from.id;
    const activityTenantId = getActivityTenantId(activity);
    const meetingId = activity.conversation.id || 'unknown_meeting';
    console.log(`[USER] Display name resolved: ${userName}`);

    const accessCheck = canUserAccess(requesterId);
    if (!accessCheck.allowed) {
      const deniedMessage = accessCheck.reason === 'blocked'
        ? `Your account is currently blocked from using this bot. Please contact an administrator.`
        : `This bot has reached its user capacity. Please contact an administrator to increase the user limit.`;
      await send(new MessageActivity(deniedMessage).addAiGenerated());
      return;
    }

    const tokenAllowance = canUserUseTokens(requesterId);
    if (!tokenAllowance.allowed) {
      await send(
        new MessageActivity(
          `You've reached your token usage limit (${tokenAllowance.used}/${tokenAllowance.limit}). ` +
          `Please contact an administrator to increase your token allowance.`
        ).addAiGenerated()
      );
      return;
    }
    
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

      recordUserMessage(requesterId, actorName, activityTenantId);

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

    // In group/meeting chats, only respond when the bot is @mentioned
    if (isGroupConversation && !botMentioned) {
      console.log(`[MESSAGE] Ignoring group chat message — bot not @mentioned`);
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Build recent context for the intent classifier so it handles follow-ups and corrections naturally
    const recentLlmTurns = llmMessages.slice(-6).map((m: any) =>
      `${m.role === 'user' ? 'User' : 'Bot'}: ${(m.content || '').slice(0, 300)}`
    ).join('\n');
    const _inboxCtxSnap = inboxContextMap.get(activity.conversation.id);
    const _lastRespSnap = lastBotResponseMap.get(activity.conversation.id);
    const stateLines: string[] = [];
    if (_inboxCtxSnap?.lastMatchedSenderName) stateLines.push(`Bot last showed inbox email from: ${_inboxCtxSnap.lastMatchedSenderName} <${_inboxCtxSnap.lastMatchedSenderEmail || ''}>`);
    if (_lastRespSnap?.subject) stateLines.push(`Bot last response subject: ${_lastRespSnap.subject}`);
    const classifyContext = [recentLlmTurns, stateLines.join('\n')].filter(Boolean).join('\n\n');

    // === SMART INTENT AGENT ROUTING ===
    // Build agent context for smart decision making
    const agentContext: AgentContext = {
      userId: requesterId,
      userName: userName,
      conversationId: activity.conversation.id,
      isMeetingConversation,
      meetingInfo: meetingId ? {
        subject: getCachedMeetingContext(activity.conversation.id)?.subject,
        hasActiveCall: Array.from(activeCallMap.values()).some(c => c.conversationId === activity.conversation.id),
        hasTranscript: (liveTranscriptMap.get(activity.conversation.id)?.length || 0) > 0,
      } : undefined,
      inboxContext: _inboxCtxSnap ? {
        lastSender: _inboxCtxSnap.lastMatchedSenderName,
        lastSubject: _inboxCtxSnap.lastMessages?.[0]?.subject || undefined,
        recentSenders: _inboxCtxSnap.contacts?.map(c => c.displayName) || [],
        justShowedInbox: _inboxCtxSnap.updatedAt > Date.now() - 5 * 60 * 1000,
      } : undefined,
      lastBotResponse: _lastRespSnap ? {
        contentType: _lastRespSnap.contentType,
        content: _lastRespSnap.content?.slice(0, 500),
        subject: _lastRespSnap.subject,
        timestamp: _lastRespSnap.timestamp,
      } : undefined,
      pendingClarification: (() => {
        const pending = pendingClarificationMap.get(activity.conversation.id);
        if (pending && Date.now() - pending.timestamp < 5 * 60 * 1000) {
          return {
            question: pending.question,
            aboutPerson: pending.aboutPerson,
            aboutTopic: pending.aboutTopic,
          };
        }
        return undefined;
      })(),
    };

    // Always use full LLM intent reasoning for action routing.
    let effectiveQuery = cleanText;
    const agentDecision: AgentDecision = await intentAgent.analyze(cleanText, agentContext, {
      userId: requesterId,
      displayName: actorName,
      meetingId,
    });
    effectiveQuery = agentDecision.refinedQuery || cleanText;
    
    console.log(`[INTENT_AGENT] Decision: ${agentDecision.intent} (${agentDecision.confidence}) - ${agentDecision.reasoning.slice(0, 100)}`);

    // Clear pending clarification now that we received a new message
    pendingClarificationMap.delete(activity.conversation.id);

    // Handle clarification requests
    if (agentDecision.needsClarification && agentDecision.clarificationQuestion) {
      console.log(`[INTENT_AGENT] Requesting clarification: ${agentDecision.clarificationQuestion}`);
      await send(new MessageActivity(agentDecision.clarificationQuestion).addAiGenerated().addFeedback());
      recordAgentBotResponse(activity.conversation.id, agentDecision.clarificationQuestion);

      // Record clarification context for follow-up resolution
      pendingClarificationMap.set(activity.conversation.id, {
        question: agentDecision.clarificationQuestion,
        aboutPerson: agentDecision.parameters.personReference,
        aboutTopic: agentDecision.parameters.contentType || agentDecision.intent,
        timestamp: Date.now(),
      });
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle list_attendees intent (new intent from agent)
    if (agentDecision.intent === 'list_attendees') {
      console.log(`\n\n======== LIST_ATTENDEES HANDLER START ========`);
      console.log(`[ATTENDEES] Processing attendee list request via Intent Agent`);
      await sendTypingIndicator(send);

      const uniqueAttendees = new Map<string, string>(); // email -> name
      let meetingSubject: string | undefined;
      let meetingJoinUrl: string | undefined;
      const sources: string[] = [];

      // STEP 1: If in a group/meeting chat, get chat members AND meeting info
      let chatInfo: any = null;
      if (isMeetingConversation || activity.conversation.conversationType === 'groupChat') {
        console.log(`[ATTENDEES] STEP1: In meeting/group chat - fetching chat members and meeting info`);
        const chatMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
        chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        console.log(`[ATTENDEES] STEP1: chatInfo received: subject="${chatInfo?.subject || 'NONE'}", joinWebUrl=${chatInfo?.joinWebUrl ? 'YES' : 'NO'}`);
        
        for (const m of chatMembers) {
          const nameLower = (m.displayName || '').toLowerCase();
          if (nameLower.includes('bot') || nameLower === 'assistant') continue;
          const email = (m.email || '').trim().toLowerCase();
          if (email && email.includes('@') && !uniqueAttendees.has(email)) {
            uniqueAttendees.set(email, m.displayName || email);
          }
        }
        if (chatInfo?.subject) {
          meetingSubject = chatInfo.subject;
          console.log(`[ATTENDEES] STEP1: Using subject from chatInfo: "${meetingSubject}"`);
        }
        if (chatInfo?.joinWebUrl) {
          meetingJoinUrl = chatInfo.joinWebUrl;
          console.log(`[ATTENDEES] STEP1: Have meeting joinWebUrl for exact calendar match`);
        }
        if (uniqueAttendees.size > 0) {
          sources.push('chat');
          console.log(`[ATTENDEES] Found ${uniqueAttendees.size} members from chat`);
        }
      }

      // STEP 2: Get calendar attendees using joinWebUrl for EXACT meeting match
      // This ensures we get the right meeting's attendees, not just any meeting
      console.log(`[ATTENDEES] ========== STEP 2: CALENDAR LOOKUP ==========`);
      console.log(`[ATTENDEES] meetingJoinUrl to match: ${meetingJoinUrl ? meetingJoinUrl.slice(0, 80) + '...' : 'NONE'}`);
      console.log(`[ATTENDEES] meetingSubject so far: "${meetingSubject || 'NONE'}"`);
      console.log(`[ATTENDEES] Calling resolveCalendarAttendeesForRequest...`);
      const calendarResult = await resolveCalendarAttendeesForRequest(
        activity.from.aadObjectId || activity.from.id,
        effectiveQuery || activity.text || '',
        activity.conversation.id,
        meetingJoinUrl // Pass joinWebUrl for exact matching
      );
      console.log(`[ATTENDEES] Calendar result: ${calendarResult.emails.length} emails, subject="${calendarResult.meetingSubject || 'NONE'}"`);
      
      if (calendarResult.emails.length > 0) {
        let addedFromCalendar = 0;
        for (let i = 0; i < calendarResult.emails.length; i++) {
          const email = calendarResult.emails[i].toLowerCase();
          const name = calendarResult.names[i] || email;
          if (!uniqueAttendees.has(email)) {
            uniqueAttendees.set(email, name);
            addedFromCalendar++;
          }
        }
        if (addedFromCalendar > 0) {
          sources.push('calendar');
          console.log(`[ATTENDEES] Added ${addedFromCalendar} attendees from calendar`);
        }
        if (!meetingSubject && calendarResult.meetingSubject) {
          meetingSubject = calendarResult.meetingSubject;
        }
      }

      const attendeeResult = {
        emails: Array.from(uniqueAttendees.keys()),
        names: Array.from(uniqueAttendees.values()),
        meetingSubject
      };
      const source = sources.join('+') || 'unknown';

      if (!attendeeResult.emails.length) {
        await send(new MessageActivity(
          `I couldn't find attendee email addresses for that meeting. ` +
          `Try specifying the meeting day or title, for example: "list attendees for Monday standup".`
        ).addAiGenerated().addFeedback());
      } else {
        const lines = attendeeResult.emails.map((email, index) => {
          const name = attendeeResult.names[index] || email;
          return `${name}: ${email}`;
        });
        const header = attendeeResult.meetingSubject
          ? `**Attendees for ${attendeeResult.meetingSubject}:**`
          : `**Meeting attendees:**`;

        await send(new MessageActivity(`${header}\n${lines.join('\n')}`).addAiGenerated().addFeedback());
        recordAgentBotResponse(activity.conversation.id, `Listed ${attendeeResult.emails.length} attendees (source: ${source})`);
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Map agent intent to existing handler
    const detectedIntent = agentDecision.intent as typeof detectedIntent;
    console.log(`[INTENT] Using agent decision: ${detectedIntent}`);
    
    const meetingAutoJoinKey = `meeting-autojoin/${activity.conversation.id}`;
    const hasAutoJoinedMeeting = storage.get(meetingAutoJoinKey) === true;

    if (
      isMeetingConversation &&
      !hasAutoJoinedMeeting &&
      (userMessage.includes('meeting started') || userMessage.includes('meeting has started'))
    ) {
      console.log(`[MEETING_AUTOJOIN] Detected meeting start message. Sending automatic greeting.`);

      const greetingActivity = new MessageActivity(
        `Hello, **${config.botDisplayName}** auto-joined this meeting chat.\n\n` +
        `**I specialize in meeting transcription:**\n` +
        `• **Join Call** — Ask me to join and I'll capture the conversation\n` +
        `• **Transcribe** — I'll fetch the transcript after the meeting ends\n` +
        `• **Summarize** — Get an AI summary from the transcript\n` +
        `• **Minutes** — Generate formal meeting documentation`
      ).addAiGenerated().addFeedback();

      await send(greetingActivity);
      storage.set(meetingAutoJoinKey, true);
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle summarization commands
    if (detectedIntent === 'summarize') {
      console.log(`[ACTION] Processing summarization request`);
      await sendTypingIndicator(send);
      
      // ── Reformat path: user wants to reformat last summary/minutes/transcript ────
      const _lastSummaryResp = lastBotResponseMap.get(activity.conversation.id);
      const isSummaryReformatRequest = agentDecision.parameters?.isReformatRequest &&
        _lastSummaryResp &&
        ['summary', 'minutes', 'transcript', 'insights', 'meeting_overview'].includes(_lastSummaryResp.contentType) &&
        (Date.now() - _lastSummaryResp.timestamp) < 15 * 60 * 1000; // Within 15 minutes
      
      if (isSummaryReformatRequest && _lastSummaryResp?.content) {
        console.log(`[SUMMARIZE] Reformat request for ${_lastSummaryResp.contentType}, formatStyle=${agentDecision.parameters?.formatStyle}`);
        const formatStyle = agentDecision.parameters?.formatStyle || 'shorter';
        const formatInstruction = formatStyle === 'shorter' 
          ? 'Rewrite this in a natural conversational style and make it much shorter. Keep only the key takeaways, decisions, and action items.'
          : formatStyle === 'longer'
          ? 'Expand this naturally with more context and detail while keeping the same facts.'
          : formatStyle === 'bullets'
          ? 'Reformat this as clear, conversational bullet points grouped by topic.'
          : 'Reformat this naturally as requested';
        
        const reformatPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `${formatInstruction}

Original content:
${_lastSummaryResp.content}

User request: "${effectiveQuery || activity.text}"`
            }
          ],
          instructions: `You are reformatting a meeting ${_lastSummaryResp.contentType}. 
CRITICAL: Work ONLY with the original content provided - do NOT add information not present.
Apply the formatting instruction precisely.
Use a natural, human conversational tone. Avoid rigid or repetitive template wording.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });
        
        const reformatted = await sendPromptWithTracking(reformatPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: _lastSummaryResp.content,
        });
        
        const reformattedContent = reformatted.content || _lastSummaryResp.content;
        await send(new MessageActivity(reformattedContent).addAiGenerated().addFeedback());
        
        // Update with reformatted version for chained follow-ups
        recordBotResponse(activity.conversation.id, {
          content: reformattedContent,
          contentType: _lastSummaryResp.contentType,
          subject: _lastSummaryResp.subject,
          timestamp: Date.now(),
        });
        
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }
      
      // ── Follow-up question path: answer questions about cached summary/transcript ────
      // This handles "what did X say", "tell me more about Y", "focus on action items" etc
      const isFollowUpOnSummary = agentDecision.parameters?.isReformatRequest &&
        _lastSummaryResp &&
        ['summary', 'minutes', 'transcript', 'insights', 'meeting_overview'].includes(_lastSummaryResp.contentType) &&
        (Date.now() - _lastSummaryResp.timestamp) < 15 * 60 * 1000 &&
        /\b(what did|who said|tell me|what about|more about|explain|focus|mentioned|action items?|decisions?|key points?|highlights?)\b/i.test(effectiveQuery || activity.text || '');
      
      if (isFollowUpOnSummary && _lastSummaryResp?.content) {
        console.log(`[SUMMARIZE] Follow-up question on cached ${_lastSummaryResp.contentType}`);
        
        const answerPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Based on this meeting ${_lastSummaryResp.contentType}, answer the user's question.

Meeting content:
${_lastSummaryResp.content}

User question: "${effectiveQuery || activity.text}"`
            }
          ],
          instructions: `You are answering a follow-up question about a meeting ${_lastSummaryResp.contentType}.
CRITICAL: Answer ONLY based on the content provided - do NOT make up information.
If the question cannot be answered from the content, say so clearly.
Keep the response focused and concise.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });
        
        const answer = await sendPromptWithTracking(answerPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: _lastSummaryResp.content,
        });
        
        await send(new MessageActivity(answer.content || "I couldn't find that information in the meeting content.").addAiGenerated().addFeedback());
        
        // Keep the same cached response for further follow-ups
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }
      
      // Check if user also wants to email the result
      // effectiveQuery already contains follow-up context if applicable
      const emailRequest = detectEmailRequest(effectiveQuery || activity.text || '');
      let generatedSummary = '';
      
      try {
        console.log(`[DEBUG] Processing summarization for user`);
        
        // Use LLM to understand WHAT the user wants to summarize
        // Build date context for past meeting detection
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        const lastWeekStart = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
        
        const dateContext = `Today: ${today.toISOString().split('T')[0]} (${today.toLocaleDateString('en-US', { weekday: 'long' })})
Yesterday: ${yesterday.toISOString().split('T')[0]} (${yesterday.toLocaleDateString('en-US', { weekday: 'long' })})
Last week: ${lastWeekStart.toISOString().split('T')[0]} to ${today.toISOString().split('T')[0]}`;
        
        // effectiveQuery already contains follow-up context if applicable
        const targetExtractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Determine what the user wants summarized.

User request: "${effectiveQuery || activity.text}"
User is currently in a meeting chat: ${isMeetingConversation ? 'YES' : 'NO'}

${dateContext}

RESPOND WITH JSON:
{
  "target": "current" | "past_meeting" | "last_meeting" | "specific_chat",
  "chat_name": "name if specific chat mentioned, else null",
  "meeting_date": "ISO date if explicit date mentioned, else null",
  "meeting_subject": "title if mentioned, else null",
  "content_type": "any"
}`
            }
          ],
          instructions: `DECISION LOGIC (follow in order):

1. If user is in a meeting chat (YES above):
   - Default to target="current" unless they explicitly name a DIFFERENT meeting or date
   - Generic phrases like "summarize", "summary", "recap" refer to current session
   
2. If user is NOT in a meeting chat:
   - "summarize", "the meeting", "last meeting", "last call", "previous meeting", "what was discussed", "what happened" → target="last_meeting" with meeting_date=null
   - ONLY set target="past_meeting" when user gives an EXPLICIT PAST date like "yesterday", "March 10", "last Friday"

3. CRITICAL: "last meeting" / "last call" / "most recent meeting" = target="last_meeting" with meeting_date=null. Do NOT set meeting_date to today or the current time.
   
4. meeting_date must be a DATE ONLY string like "2026-03-14" — never include time components.

5. Set target="specific_chat" only when user names a specific group/channel by title

Output valid JSON only.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const targetResponse = await sendPromptWithTracking(targetExtractPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: cleanText || activity.text || '',
        });
        const jsonStr = (targetResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        let targetInfo: { target: string; chat_name: string | null; meeting_date: string | null; meeting_subject: string | null; content_type: string } = { 
          target: 'current', chat_name: null, meeting_date: null, meeting_subject: null, content_type: 'any' 
        };
        try {
          targetInfo = JSON.parse(jsonStr);
        } catch {
          console.warn(`[SUMMARIZE] Could not parse target extraction, defaulting to current conversation`);
        }
        
        console.log(`[SUMMARIZE] Target analysis: ${JSON.stringify(targetInfo)}`);
        
        // Code-level defense: if LLM returns past_meeting with today's date or no date,
        // treat it as last_meeting (search last 7 days instead of one specific day)
        if (targetInfo.target === 'past_meeting') {
          const today = new Date().toISOString().split('T')[0];
          const dateOnly = targetInfo.meeting_date?.split('T')[0];
          if (!dateOnly || dateOnly === today) {
            console.log(`[SUMMARIZE] Correcting past_meeting (date=${targetInfo.meeting_date}) → last_meeting`);
            targetInfo.target = 'last_meeting';
            targetInfo.meeting_date = null;
          }
        }
        
        // Handle past meeting by date - find and fetch transcript
        if (targetInfo.target === 'past_meeting' && targetInfo.meeting_date) {
          console.log(`[SUMMARIZE] Looking up past meeting from ${targetInfo.meeting_date}`);
          const userId = activity.from.aadObjectId || activity.from.id;
          const pastMeeting = await graphApiHelper.findPastMeeting(userId, targetInfo.meeting_date, targetInfo.meeting_subject || undefined);
          
          if (pastMeeting.success && pastMeeting.meeting) {
            console.log(`[SUMMARIZE] Found past meeting: "${pastMeeting.meeting.subject}"`);
            
            // Notify user we're fetching the transcript (it may take time)
            await send(new MessageActivity(
              `Found meeting "**${pastMeeting.meeting.subject}**". Checking for transcript availability...`
            ).addAiGenerated());
            
            // Poll for transcript with retries (transcripts may not be immediately available after meeting ends)
            const pollResult = await pollForTranscriptReady(
              pastMeeting.meeting.organizerId,
              pastMeeting.meeting.joinWebUrl,
              pastMeeting.meeting.start ? new Date(pastMeeting.meeting.start).getTime() : undefined,
              pastMeeting.meeting.end ? new Date(pastMeeting.meeting.end).getTime() : undefined,
              6,  // maxAttempts
              5000 // initialDelayMs (5 seconds)
            );
            
            if (pollResult.success && pollResult.vttContent) {
              const parsed = parseVttToEntries(pollResult.vttContent);
              if (parsed.length > 0) {
                console.log(`[SUMMARIZE] Got ${parsed.length} transcript entries from past meeting`);
                generatedSummary = await generateFormattedSummaryHtml(
                  parsed,
                  pastMeeting.meeting.subject,
                  userName,
                  [],
                  pastMeeting.meeting.start, // Use actual meeting date
                  { userId: requesterId, displayName: actorName, meetingId }
                );
                
                await send(new MessageActivity(generatedSummary).addAiGenerated().addFeedback());
                
                // Track for email follow-up
                recordBotResponse(activity.conversation.id, {
                  content: generatedSummary,
                  contentType: 'summary',
                    subject: `Meeting Summary: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                  timestamp: Date.now()
                });
                
                // Handle email if requested
                if (emailRequest.wantsEmail) {
                  if (emailRequest.sendToAllAttendees) {
                    // Send to all meeting attendees
                    const emailResult = await autoEmailSummaryToParticipants(
                      activity.conversation.id,
                      activity.from.aadObjectId || activity.from.id,
                      `Meeting Summary: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                      generatedSummary
                    );
                    if (emailResult.sentCount > 0) {
                      await send(new MessageActivity(`Done! I've emailed this summary to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
                    } else {
                      await send(new MessageActivity(`I couldn't email the summary to attendees. No valid email addresses found.`).addAiGenerated());
                    }
                  } else if (emailRequest.emailAddress) {
                    const sendResult = await graphApiHelper.sendEmail(
                      activity.from.aadObjectId || activity.from.id,
                      emailRequest.emailAddress,
                      `Meeting Summary: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                      generatedSummary
                    );
                    if (sendResult.success) {
                      await send(new MessageActivity(`Done! I've emailed this summary to **${emailRequest.emailAddress}**.`).addAiGenerated());
                    }
                  }
                }
                
                storage.set(conversationKey, messages);
                storage.set(sharedConversationKey, sharedMessages);
                storage.set(llmConversationKey, llmMessages);
                return;
              }
            }
            
            // No transcript available for this past meeting after polling
            await send(new MessageActivity(
              `I found the meeting "**${pastMeeting.meeting.subject}**" but no transcript is available yet. ` +
              `${pollResult.error || 'Transcription may not have been enabled, or the transcript is still processing.'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          } else {
            await send(new MessageActivity(
              `I couldn't find a Teams meeting on ${targetInfo.meeting_date}. ` +
              `${pastMeeting.error || 'Please check the date and try again.'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }
        
        // --- LAST MEETING LOOKUP (most recent from calendar) ---
        if (targetInfo.target === 'last_meeting' || (!isMeetingConversation && targetInfo.target === 'current')) {
          console.log(`[SUMMARIZE] Looking up recent meetings from calendar`);
          const userId = activity.from.aadObjectId || activity.from.id;
          
          // Find multiple recent meetings so we can try each until one has a transcript
          const pastMeetings = await graphApiHelper.findPastMeetings(userId, undefined, targetInfo.meeting_subject || undefined, 5);
          
          if (pastMeetings.success && pastMeetings.meetings.length > 0) {
            let foundTranscript = false;
            
            for (const meeting of pastMeetings.meetings) {
              console.log(`[SUMMARIZE] Trying meeting: "${meeting.subject}"`);
              
              const pollResult = await pollForTranscriptReady(
                meeting.organizerId,
                meeting.joinWebUrl,
                meeting.start ? new Date(meeting.start).getTime() : undefined,
                meeting.end ? new Date(meeting.end).getTime() : undefined,
                3, // fewer retries per meeting since we're trying multiple
                3000
              );
              
              if (pollResult.success && pollResult.vttContent) {
                const parsed = parseVttToEntries(pollResult.vttContent);
                if (parsed.length > 0) {
                  console.log(`[SUMMARIZE] Got ${parsed.length} transcript entries from "${meeting.subject}"`);
                  
                  await send(new MessageActivity(
                    `Found transcript for "**${meeting.subject}**". Generating summary...`
                  ).addAiGenerated());
                  
                  generatedSummary = await generateFormattedSummaryHtml(
                    parsed,
                    meeting.subject,
                    userName,
                    [],
                    meeting.start,
                    { userId: requesterId, displayName: actorName, meetingId }
                  );
                  
                  await send(new MessageActivity(generatedSummary).addAiGenerated().addFeedback());
                  
                  recordBotResponse(activity.conversation.id, {
                    content: generatedSummary,
                    contentType: 'summary',
                    subject: `Meeting Summary: ${meeting.subject} — ${config.botDisplayName}`,
                    timestamp: Date.now()
                  });
                  
                  // Handle email if requested
                  if (emailRequest.wantsEmail) {
                    if (emailRequest.sendToAllAttendees) {
                      const emailResult = await autoEmailSummaryToParticipants(
                        activity.conversation.id,
                        activity.from.aadObjectId || activity.from.id,
                        `Meeting Summary: ${meeting.subject} — ${config.botDisplayName}`,
                        generatedSummary
                      );
                      if (emailResult.sentCount > 0) {
                        await send(new MessageActivity(`Done! I've emailed this summary to **${emailResult.sentCount} attendee(s)**.`).addAiGenerated());
                      }
                    } else if (emailRequest.emailAddress) {
                      const sendResult = await graphApiHelper.sendEmail(
                        activity.from.aadObjectId || activity.from.id,
                        emailRequest.emailAddress,
                        `Meeting Summary: ${meeting.subject} — ${config.botDisplayName}`,
                        generatedSummary
                      );
                      if (sendResult.success) {
                        await send(new MessageActivity(`Done! I've emailed this summary to **${emailRequest.emailAddress}**.`).addAiGenerated());
                      }
                    }
                  }
                  
                  foundTranscript = true;
                  break;
                }
              }
              console.log(`[SUMMARIZE] No transcript for "${meeting.subject}", trying next...`);
            }
            
            if (!foundTranscript) {
              console.log(`[SUMMARIZE] No transcripts found in ${pastMeetings.meetings.length} recent meetings`);
              await send(new MessageActivity(
                `No transcripts are available for your recent meetings. Transcription needs to be enabled during the call for me to generate a summary.`
              ).addAiGenerated().addFeedback());
            }
            
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          } else if (!isMeetingConversation) {
            // Only show error if not in meeting chat (if in meeting chat, let it fall through to current conversation logic)
            await send(new MessageActivity(
              `I couldn't find any recent Teams meetings in your calendar. ` +
              `${pastMeetings.error || 'Try specifying a date like "summarize yesterday\'s meeting".'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
          // If in meeting chat and no recent meeting found, fall through to current conversation logic
        }
        
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
        let liveEntries = targetInfo.target === 'current' ? liveTranscriptMap.get(activity.conversation.id) : undefined;
        let transcriptEntries = liveEntries?.filter(e => e.isFinal) || [];

        // If no local entries but bot is in an active call, force-fetch transcript from onlineMeetings.
        if (transcriptEntries.length === 0 && targetInfo.target === 'current') {
          const activeCall = Array.from(activeCallMap.entries()).find(
            ([_, call]) => call.conversationId === activity.conversation.id && !call.terminatedAt
          );
          if (activeCall) {
            const [activeCallId] = activeCall;
            console.log(`[SUMMARIZE] Bot in active call ${activeCallId} but no cached entries. Force-fetching from onlineMeetings transcript API...`);
            const meetingInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
            if (meetingInfo?.organizer?.id && meetingInfo?.joinWebUrl) {
              const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
              const fetchResult = await fetchTranscriptCacheFirst(
                meetingInfo.organizer.id,
                meetingInfo.joinWebUrl,
                transcriptWindow.min,
                transcriptWindow.max
              );
              if (fetchResult.entries.length > 0) {
                console.log(`[SUMMARIZE] Force-fetch got ${fetchResult.entries.length} entries (fromCache=${fetchResult.fromCache})`);
                liveTranscriptMap.set(activity.conversation.id, fetchResult.entries);
                saveTranscriptToFile(activity.conversation.id);
                transcriptEntries = fetchResult.entries.filter(e => e.isFinal);
              }
            }
            if (transcriptEntries.length === 0) {
              console.log(`[SUMMARIZE] Force-fetch returned no transcript data for active call ${activeCallId}`);
            }
          }
        }

        if (transcriptEntries.length > 0) {
          console.log(`[SUMMARIZE] Found ${transcriptEntries.length} transcript entries, generating AI summary...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
          const meetingTitle = await resolveDisplayMeetingTitle(
            activity.conversation.id,
            requesterId,
            chatInfo?.subject
          );
          const memberList = chatMembers.length > 0 ? chatMembers : [];

          generatedSummary = await generateFormattedSummaryHtml(
            transcriptEntries,
            meetingTitle,
            userName,
            memberList,
            chatInfo?.startDateTime, // Use actual meeting date
            { userId: requesterId, displayName: actorName, meetingId }
          );
          console.log(`[SUMMARIZE] Summary generated successfully from transcript`);

          const responseActivity = new MessageActivity(generatedSummary).addAiGenerated().addFeedback();
          await send(responseActivity);
          
          // Track for email follow-up
          recordBotResponse(activity.conversation.id, {
            content: generatedSummary,
            contentType: 'summary',
            subject: `Summary: ${meetingTitle}`,
            timestamp: Date.now()
          });

          // Handle email if user requested it
          if (emailRequest.wantsEmail) {
            console.log(`[SUMMARIZE] User requested email - sending summary`);
            if (emailRequest.sendToAllAttendees) {
              // Send to all meeting attendees
              const emailResult = await autoEmailSummaryToParticipants(
                activity.conversation.id,
                activity.from.aadObjectId || activity.from.id,
                `Meeting Summary: ${meetingTitle}`,
                generatedSummary
              );
              if (emailResult.sentCount > 0) {
                await send(new MessageActivity(`Done! I've emailed the summary to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
              } else {
                await send(new MessageActivity(`I couldn't email the summary to attendees. No valid email addresses found.`).addAiGenerated());
              }
            } else {
              // Send to specific recipient or requesting user
              let recipientEmail = emailRequest.emailAddress;
              if (!recipientEmail) {
                const detailedMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
                const selfMember = detailedMembers.find((m) => m.userId === (activity.from.aadObjectId || activity.from.id) || m.displayName.toLowerCase() === (actorName || '').toLowerCase());
                recipientEmail = selfMember?.email || '';
                if (!recipientEmail) {
                  const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
                  recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
                }
              }
              if (recipientEmail) {
                const emailResult = await graphApiHelper.sendEmail(
                  activity.from.aadObjectId || activity.from.id,
                  recipientEmail,
                  `Meeting Summary: ${meetingTitle}`,
                  generatedSummary,
                  { replyToEmail: recipientEmail, replyToName: actorName }
                );
                if (emailResult.success) {
                  await send(new MessageActivity(`Done! I've emailed the summary to **${recipientEmail}**.`).addAiGenerated());
                } else {
                  await send(new MessageActivity(`I generated the summary but couldn't send the email: ${emailResult.error || 'unknown error'}`).addAiGenerated());
                }
              } else {
                await send(new MessageActivity(`I generated the summary but couldn't find your email address. Please try "send to [email]".`).addAiGenerated());
              }
            }
          }

          console.log(`[SUCCESS] Transcript-based summary sent to user`);
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }
        
        // If target was explicitly 'last_meeting', don't fall through to current conversation Graph API
        // (we already tried calendar lookup above - avoid duplicate summaries)
        if (targetInfo.target === 'last_meeting') {
          console.log(`[SUMMARIZE] last_meeting lookup didn't find transcript, not falling through to current conversation`);
          await send(new MessageActivity(
            `I couldn't find a transcript for your recent meetings. Make sure transcription was enabled during the call, or try specifying a date like "summarize yesterday's meeting".`
          ).addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }
        
        // No local transcript - try to fetch from Graph API
        console.log(`[SUMMARIZE] No local transcript entries, trying Graph API`);
          
        // Try to fetch transcript from Graph API for past meetings
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        if (chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
          console.log(`[SUMMARIZE] Checking cache, then Graph API...`);
          const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
          const sumFetchResult = await fetchTranscriptCacheFirst(
            chatInfo.organizer.id,
            chatInfo.joinWebUrl,
            transcriptWindow.min,
            transcriptWindow.max
          );
          
          if (sumFetchResult.entries.length > 0) {
            const parsed = sumFetchResult.entries;
              console.log(`[SUMMARIZE] Got ${parsed.length} entries (fromCache=${sumFetchResult.fromCache})`);
              liveTranscriptMap.set(activity.conversation.id, parsed);
              saveTranscriptToFile(activity.conversation.id);
              
              const meetingTitle = await resolveDisplayMeetingTitle(
                activity.conversation.id,
                requesterId,
                chatInfo.subject
              );
              const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
              
              generatedSummary = await generateFormattedSummaryHtml(
                parsed,
                meetingTitle,
                userName,
                chatMembers,
                chatInfo.startDateTime, // Use actual meeting date
                { userId: requesterId, displayName: actorName, meetingId }
              );
              
              await send(new MessageActivity(generatedSummary).addAiGenerated().addFeedback());
              
              recordBotResponse(activity.conversation.id, {
                content: generatedSummary,
                contentType: 'summary',
                subject: `Summary: ${meetingTitle}`,
                timestamp: Date.now()
              });

              // Handle email if requested
              if (emailRequest.wantsEmail) {
                console.log(`[SUMMARIZE] User requested email - sending summary`);
                if (emailRequest.sendToAllAttendees) {
                  // Send to all meeting attendees
                  const emailResult = await autoEmailSummaryToParticipants(
                    activity.conversation.id,
                    activity.from.aadObjectId || activity.from.id,
                    `Meeting Summary: ${meetingTitle}`,
                    generatedSummary
                  );
                  if (emailResult.sentCount > 0) {
                    await send(new MessageActivity(`Done! I've emailed the summary to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
                  } else {
                    await send(new MessageActivity(`Couldn't email the summary to attendees. No valid email addresses found.`).addAiGenerated());
                  }
                } else {
                  let recipientEmail = emailRequest.emailAddress;
                  if (!recipientEmail) {
                    const detailedMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
                    const selfMember = detailedMembers.find((m) => m.userId === (activity.from.aadObjectId || activity.from.id) || m.displayName.toLowerCase() === (actorName || '').toLowerCase());
                    recipientEmail = selfMember?.email || '';
                    if (!recipientEmail) {
                      const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
                      recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
                    }
                  }
                  if (recipientEmail) {
                    const emailResult = await graphApiHelper.sendEmail(
                      activity.from.aadObjectId || activity.from.id,
                      recipientEmail,
                      `Meeting Summary: ${meetingTitle}`,
                      generatedSummary,
                      { replyToEmail: recipientEmail, replyToName: actorName }
                    );
                    if (emailResult.success) {
                      await send(new MessageActivity(`Done! I've emailed the summary to **${recipientEmail}**.`).addAiGenerated());
                    } else {
                      await send(new MessageActivity(`Summary generated but email failed: ${emailResult.error || 'unknown error'}`).addAiGenerated());
                    }
                  }
                }
              }
              
              storage.set(conversationKey, messages);
              storage.set(sharedConversationKey, sharedMessages);
              storage.set(llmConversationKey, llmMessages);
              return;
            }
          }
        
        // No transcript found anywhere - generate natural response
        const noTranscriptMsg = isMeetingConversation
          ? `I don't have a transcript to summarize yet. If you're in a meeting, ask me to **join the call** and I'll start capturing the conversation. Or if the meeting already happened and was recorded, just ask me to **transcribe** it.`
          : `I'd need a meeting transcript to create a summary. This looks like a regular chat - for meeting summaries, start a Teams meeting and invite me, or ask about a past meeting like "summarize yesterday's standup"!`;
        
        await send(new MessageActivity(noTranscriptMsg).addAiGenerated().addFeedback());
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
    if (detectedIntent === 'meeting_overview') {
      console.log(`[ACTION] Processing meeting overview request`);
      await sendTypingIndicator(send);
      try {
        console.log(`[DEBUG] Searching for meeting transcript`);

        // First, try to get meeting transcript from local storage
        let transcriptEntries = liveTranscriptMap.get(activity.conversation.id);
        const finalEntries = transcriptEntries?.filter(e => e.isFinal) || [];

        // If no local transcript, fetch from Graph API
        if (finalEntries.length === 0) {
          console.log(`[GRAPH] No local transcript, checking cache then Graph API...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          if (chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
            const graphFetchResult = await fetchTranscriptCacheFirst(
              chatInfo.organizer.id,
              chatInfo.joinWebUrl
            );
            if (graphFetchResult.entries.length > 0) {
              transcriptEntries = graphFetchResult.entries;
              console.log(`[GRAPH] Got ${transcriptEntries.length} transcript entries (fromCache=${graphFetchResult.fromCache})`);
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
        const meetingTitle = await resolveDisplayMeetingTitle(
          activity.conversation.id,
          requesterId,
          chatInfo?.subject
        );
        const memberList = chatMembers.length > 0 ? chatMembers : [];

        const overviewHtml = await generateFormattedSummaryHtml(
          transcriptEntries,
          meetingTitle,
          userName,
          memberList,
          chatInfo?.startDateTime, // Use actual meeting date
          { userId: requesterId, displayName: actorName, meetingId }
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

    if (detectedIntent === 'profile_details') {
      console.log(`[ACTION] Processing profile_details request`);
      await sendTypingIndicator(send);
      const profile = await resolveCurrentUserProfile(activity, actorName);
      const lines = [
        `## Your Profile Details`,
        '',
        `Hi ${userName}, here is what I can access:`,
        '',
        `• **Name:** ${profile.displayName || 'Not available'}`,
        `• **Email:** ${profile.email || 'Not available'}`,
      ];
      if (!profile.email) {
        lines.push('');
        lines.push(`I couldn't resolve your email from this chat context.`);
        lines.push(`You can share a preferred email address and I will use it for delivery.`);
      }
      await send(new MessageActivity(lines.join('\n')).addAiGenerated().addFeedback());
      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    // Handle transcription requests
    if (detectedIntent === 'transcribe') {
      console.log(`[ACTION] Processing transcription request`);
      await sendTypingIndicator(send);
      
      // ── Reformat/Follow-up path: use cached content instead of re-fetching ────
      const _lastTranscriptResp = lastBotResponseMap.get(activity.conversation.id);
      const isTranscriptFollowUp = agentDecision.parameters?.isReformatRequest &&
        _lastTranscriptResp &&
        ['transcript', 'summary', 'minutes', 'insights', 'meeting_overview'].includes(_lastTranscriptResp.contentType) &&
        (Date.now() - _lastTranscriptResp.timestamp) < 15 * 60 * 1000;
      
      if (isTranscriptFollowUp && _lastTranscriptResp?.content) {
        console.log(`[TRANSCRIBE] Follow-up on cached ${_lastTranscriptResp.contentType}, using cached content`);
        const formatStyle = agentDecision.parameters?.formatStyle || 'shorter';
        const isQuestion = /\b(what did|who said|tell me|what about|more about|explain|focus|mentioned)\b/i.test(effectiveQuery || activity.text || '');
        const rewriteInstruction = formatStyle === 'shorter'
          ? 'Rewrite this in a natural conversational style and make it much shorter. Keep only the key points and outcomes.'
          : formatStyle === 'longer'
          ? 'Expand this naturally with more context and detail while staying accurate.'
          : formatStyle === 'bullets'
          ? 'Rewrite this as conversational bullet points grouped by topic.'
          : 'Reformat this naturally as requested.';
        
        const followUpPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: isQuestion 
                ? `Based on this transcript content, answer the user's question.\n\nContent:\n${_lastTranscriptResp.content}\n\nUser question: "${effectiveQuery || activity.text}"`
                : `${rewriteInstruction}\n\n${_lastTranscriptResp.content}\n\nUser request: "${effectiveQuery || activity.text}"`
            }
          ],
          instructions: isQuestion 
            ? 'Answer based ONLY on the provided content. Do not make up information.'
            : 'Reformat naturally and conversationally while preserving facts. Do not add information not present.',
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });
        
        const followUpResult = await sendPromptWithTracking(followUpPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: _lastTranscriptResp.content,
        });
        
        await send(new MessageActivity(followUpResult.content || _lastTranscriptResp.content).addAiGenerated().addFeedback());
        if (!isQuestion) {
          recordBotResponse(activity.conversation.id, {
            content: followUpResult.content || _lastTranscriptResp.content,
            contentType: _lastTranscriptResp.contentType,
            subject: _lastTranscriptResp.subject,
            timestamp: Date.now(),
          });
        }
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }
      
      try {
        console.log(`[TRANSCRIBE_DEBUG] Step 1-2: Processing transcript request`);

        // --- TARGET EXTRACTION: Determine if user wants current meeting or a past meeting ---
        const today = new Date();
        const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        
        // effectiveQuery already contains follow-up context if applicable
        const targetExtractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `User message: "${effectiveQuery || activity.text}"

Context:
- Today: ${today.toISOString().split('T')[0]} (${today.toLocaleDateString('en-US', { weekday: 'long' })})
- Yesterday: ${yesterday.toISOString().split('T')[0]} (${yesterday.toLocaleDateString('en-US', { weekday: 'long' })})
- Is user in a meeting chat conversation: ${isMeetingConversation ? 'YES' : 'NO'}

Extract what meeting the user wants transcribed:
{
  "target": "current" | "past_meeting" | "last_meeting",
  "meeting_date": "YYYY-MM-DD or null",
  "meeting_subject": "keyword or null"
}`
            }
          ],
          instructions: `DECISION LOGIC:
1. If user is in a meeting chat and says generic "transcribe", "get transcript" → target="current"
2. If user says "last meeting", "previous meeting", "the meeting", "last call" (NOT in meeting chat) → target="last_meeting" with meeting_date=null
3. ONLY set target="past_meeting" when user gives an EXPLICIT PAST date like "yesterday", "last Friday", "March 10" — never use today or the current time
4. meeting_date must be a DATE ONLY string like "2026-03-14" — never include time components
5. If user mentions a meeting name/subject → include in meeting_subject
6. If NOT in meeting chat and no date given → target="last_meeting" (find most recent)

Output valid JSON only.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const targetResponse = await sendPromptWithTracking(targetExtractPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: effectiveQuery || activity.text || '',
        });
        const jsonStr = (targetResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        let targetInfo: { target: string; meeting_date: string | null; meeting_subject: string | null } = { 
          target: isMeetingConversation ? 'current' : 'last_meeting', meeting_date: null, meeting_subject: null
        };
        try {
          targetInfo = JSON.parse(jsonStr);
        } catch {
          console.warn(`[TRANSCRIBE] Could not parse target extraction, defaulting to current`);
        }
        
        console.log(`[TRANSCRIBE] Target analysis: ${JSON.stringify(targetInfo)}`);

        // Code-level defense: past_meeting with today's date → last_meeting
        if (targetInfo.target === 'past_meeting') {
          const today = new Date().toISOString().split('T')[0];
          const dateOnly = targetInfo.meeting_date?.split('T')[0];
          if (!dateOnly || dateOnly === today) {
            console.log(`[TRANSCRIBE] Correcting past_meeting (date=${targetInfo.meeting_date}) → last_meeting`);
            targetInfo.target = 'last_meeting';
            targetInfo.meeting_date = null;
          }
        }

        // --- PAST MEETING LOOKUP ---
        if (targetInfo.target === 'past_meeting' && targetInfo.meeting_date) {
          console.log(`[TRANSCRIBE] Looking up past meeting from ${targetInfo.meeting_date}`);
          const userId = activity.from.aadObjectId || activity.from.id;
          const pastMeeting = await graphApiHelper.findPastMeeting(userId, targetInfo.meeting_date, targetInfo.meeting_subject || undefined);
          
          if (pastMeeting.success && pastMeeting.meeting) {
            console.log(`[TRANSCRIBE] Found past meeting: "${pastMeeting.meeting.subject}"`);
            
            await send(new MessageActivity(
              `Found meeting "**${pastMeeting.meeting.subject}**". Fetching transcript...`
            ).addAiGenerated());
            
            // Poll for transcript
            const pollResult = await pollForTranscriptReady(
              pastMeeting.meeting.organizerId,
              pastMeeting.meeting.joinWebUrl,
              pastMeeting.meeting.start ? new Date(pastMeeting.meeting.start).getTime() : undefined,
              pastMeeting.meeting.end ? new Date(pastMeeting.meeting.end).getTime() : undefined,
              6, 5000
            );
            
            if (pollResult.success && pollResult.vttContent) {
              const parsed = parseVttToEntries(pollResult.vttContent);
              if (parsed.length > 0) {
                console.log(`[TRANSCRIBE] Got ${parsed.length} transcript entries from past meeting`);
                const displayEntries = parsed.length > 80 ? parsed.slice(-80) : parsed;
                const transcript = await buildTranscriptHtml(
                  displayEntries,
                  pastMeeting.meeting.subject,
                  [],
                  parsed.length,
                  parsed.length > 80,
                  { userId: requesterId, displayName: actorName, meetingId }
                );
                await send(new MessageActivity(transcript).addAiGenerated().addFeedback());
                storage.set(conversationKey, messages);
                storage.set(sharedConversationKey, sharedMessages);
                storage.set(llmConversationKey, llmMessages);
                return;
              }
            }
            
            await send(new MessageActivity(
              `I found the meeting "**${pastMeeting.meeting.subject}**" but no transcript is available. ` +
              `${pollResult.error || 'Transcription may not have been enabled during the call.'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          } else {
            await send(new MessageActivity(
              `I couldn't find a Teams meeting on ${targetInfo.meeting_date}. ` +
              `${pastMeeting.error || 'Please check the date and try again.'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }

        // --- LAST MEETING LOOKUP (most recent from calendar) ---
        if (targetInfo.target === 'last_meeting' || (targetInfo.target === 'past_meeting' && !targetInfo.meeting_date)) {
          console.log(`[TRANSCRIBE] Looking up recent meetings from calendar`);
          const userId = activity.from.aadObjectId || activity.from.id;
          
          const pastMeetings = await graphApiHelper.findPastMeetings(userId, undefined, targetInfo.meeting_subject || undefined, 5);
          
          if (pastMeetings.success && pastMeetings.meetings.length > 0) {
            let foundTranscript = false;
            
            for (const meeting of pastMeetings.meetings) {
              console.log(`[TRANSCRIBE] Trying meeting: "${meeting.subject}"`);
              
              const pollResult = await pollForTranscriptReady(
                meeting.organizerId,
                meeting.joinWebUrl,
                meeting.start ? new Date(meeting.start).getTime() : undefined,
                meeting.end ? new Date(meeting.end).getTime() : undefined,
                3, 3000
              );
              
              if (pollResult.success && pollResult.vttContent) {
                const parsed = parseVttToEntries(pollResult.vttContent);
                if (parsed.length > 0) {
                  console.log(`[TRANSCRIBE] Got ${parsed.length} transcript entries from "${meeting.subject}"`);
                  
                  await send(new MessageActivity(
                    `Found transcript for "**${meeting.subject}**". Formatting...`
                  ).addAiGenerated());
                  
                  const displayEntries = parsed.length > 80 ? parsed.slice(-80) : parsed;
                  const transcript = await buildTranscriptHtml(
                    displayEntries,
                    meeting.subject,
                    [],
                    parsed.length,
                    parsed.length > 80,
                    { userId: requesterId, displayName: actorName, meetingId }
                  );
                  await send(new MessageActivity(transcript).addAiGenerated().addFeedback());
                  foundTranscript = true;
                  break;
                }
              }
              console.log(`[TRANSCRIBE] No transcript for "${meeting.subject}", trying next...`);
            }
            
            if (!foundTranscript) {
              console.log(`[TRANSCRIBE] No transcripts found in ${pastMeetings.meetings.length} recent meetings`);
              await send(new MessageActivity(
                `No transcripts are available for your recent meetings. Transcription needs to be enabled during the call.`
              ).addAiGenerated().addFeedback());
            }
            
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          } else {
            await send(new MessageActivity(
              `I couldn't find any recent Teams meetings in your calendar. ` +
              `${pastMeetings.error || 'Try specifying a date like "transcribe yesterday\'s meeting".'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }

        // --- CURRENT CONVERSATION TRANSCRIPT ---
        // Fetch meeting metadata (title + members) for header
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 3: Got chatInfo, organizer=${chatInfo?.organizer?.id}, joinWebUrl=${chatInfo?.joinWebUrl ? 'yes' : 'no'}`);
        
        const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 4: Got ${chatMembers.length} chat members`);
        
        const meetingTitle = await resolveDisplayMeetingTitle(
          activity.conversation.id,
          requesterId,
          chatInfo?.subject
        );
        const speakerList = chatMembers.length > 0 ? chatMembers : [];

        // ALWAYS try Graph API first for the most up-to-date post-meeting transcript
        const isInCall = Array.from(callToConversationMap.values()).includes(activity.conversation.id);
        console.log(`[TRANSCRIBE_DEBUG] Step 5: isInCall=${isInCall}`);
        let graphTranscriptParsed: TranscriptEntry[] | null = null;

        if (!isInCall && chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
          console.log(`[TRANSCRIPT] Bot not in call - fetching from Graph API first`);
          console.log(`[TRANSCRIBE_DEBUG] Step 6: Checking cache, then Graph API`);

          const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
          console.log(`[TRANSCRIBE_DEBUG] Step 7: Transcript window min=${transcriptWindow.min}, max=${transcriptWindow.max}`);
          
          const transcribeFetchResult = await fetchTranscriptCacheFirst(
            chatInfo.organizer.id,
            chatInfo.joinWebUrl,
            transcriptWindow.min,
            transcriptWindow.max
          );
          console.log(`[TRANSCRIBE_DEBUG] Step 8: fetchTranscriptCacheFirst returned ${transcribeFetchResult.entries.length} entries (fromCache=${transcribeFetchResult.fromCache})`);
          
          if (transcribeFetchResult.entries.length > 0) {
            graphTranscriptParsed = transcribeFetchResult.entries;
              console.log(`[TRANSCRIPT] Got ${graphTranscriptParsed.length} entries (fromCache=${transcribeFetchResult.fromCache})`);
              liveTranscriptMap.set(activity.conversation.id, graphTranscriptParsed);
              saveTranscriptToFile(activity.conversation.id);
          }
        } else if (isInCall) {
          // Bot IS in the call - force-fetch transcript from onlineMeetings API.
          const activeCall = Array.from(activeCallMap.entries()).find(
            ([_, call]) => call.conversationId === activity.conversation.id && !call.terminatedAt
          );
          if (activeCall) {
            const [activeCallId] = activeCall;
            console.log(`[TRANSCRIBE] Bot in active call ${activeCallId}. Checking cache, then Graph...`);
            if (chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
              const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
              const activeCallFetch = await fetchTranscriptCacheFirst(
                chatInfo.organizer.id,
                chatInfo.joinWebUrl,
                transcriptWindow.min,
                transcriptWindow.max
              );
              if (activeCallFetch.entries.length > 0) {
                graphTranscriptParsed = activeCallFetch.entries;
                  console.log(`[TRANSCRIBE] Got ${graphTranscriptParsed.length} entries from active call (fromCache=${activeCallFetch.fromCache})`);
                  liveTranscriptMap.set(activity.conversation.id, graphTranscriptParsed);
                  saveTranscriptToFile(activity.conversation.id);
              }
            }
            if (!graphTranscriptParsed || graphTranscriptParsed.length === 0) {
              console.log(`[TRANSCRIBE] Force-fetch returned no transcript data for active call ${activeCallId}`);
            }
          }
        }

        // Use Graph transcript if available, otherwise fall back to in-memory
        let finalEntries: TranscriptEntry[] = [];
        let dataSource = '';

        if (graphTranscriptParsed && graphTranscriptParsed.length > 0) {
          finalEntries = graphTranscriptParsed.filter(e => e.isFinal);
          dataSource = isInCall ? 'Graph API (live call force-fetch)' : 'Graph API (post-meeting)';
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
            finalEntries.length, showingPartial,
            { userId: requesterId, displayName: actorName, meetingId }
          );

          const responseActivity = new MessageActivity(transcript).addAiGenerated().addFeedback();
          await send(responseActivity);
          console.log(`[SUCCESS] Transcript sent to user (source: ${dataSource})`);
        } else {
            console.log(`[TRANSCRIPT] No entries found - trying fallback Graph fetch`);

            const meetingOnlineInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
            if (meetingOnlineInfo?.organizer?.id && meetingOnlineInfo?.joinWebUrl) {
              const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
              const fallbackFetch = await fetchTranscriptCacheFirst(
                meetingOnlineInfo.organizer.id,
                meetingOnlineInfo.joinWebUrl,
                transcriptWindow.min,
                transcriptWindow.max
              );
              if (fallbackFetch.entries.length > 0) {
                const parsed = fallbackFetch.entries;
                  liveTranscriptMap.set(activity.conversation.id, parsed);
                  saveTranscriptToFile(activity.conversation.id);

                  // Use meeting metadata
                  const graphTitle = await resolveDisplayMeetingTitle(
                    activity.conversation.id,
                    requesterId,
                    meetingOnlineInfo.subject,
                    meetingTitle
                  );
                  const transcript = await buildTranscriptHtml(
                    parsed.length > 80 ? parsed.slice(-80) : parsed,
                    graphTitle, speakerList,
                    parsed.length, parsed.length > 80,
                    { userId: requesterId, displayName: actorName, meetingId }
                  );
                  const responseActivity = new MessageActivity(transcript).addAiGenerated().addFeedback();
                  await send(responseActivity);
                  console.log(`[SUCCESS] Graph transcript fetched and sent (${parsed.length} entries)`);
              } else {
                // Check if the bot ever joined this meeting's call
                const cachedCtx = getCachedMeetingContext(activity.conversation.id);
                const botWasInCall = !!(cachedCtx?.callStartedAt);
                
                console.log(`[TRANSCRIPT] No Graph transcript available. botWasInCall=${botWasInCall}, cachedCtx=${JSON.stringify(cachedCtx || {})}`);
                
                let noTranscriptMsg: string;
                if (botWasInCall) {
                  // Bot was in the call but no transcript found - Teams might still be processing
                  noTranscriptMsg = 
                    `I joined this meeting but haven't received the transcript yet.\n\n` +
                    `**Possible reasons:**\n` +
                    `• Teams transcription wasn't enabled during the call\n` +
                    `• Teams is still processing the transcript (can take a few minutes)\n\n` +
                    `**Try again in 2-3 minutes**, or enable transcription in Teams during your next call.`;
                } else {
                  // Bot never joined this meeting
                  noTranscriptMsg = 
                    `I don't have a transcript for this meeting because I wasn't asked to join the call.\n\n` +
                    `**To get transcripts, I need to join your meeting:**\n` +
                    `• Start or schedule a Teams meeting\n` +
                    `• Say "**join the call**" or "**join**" in the meeting chat\n` +
                    `• I'll join and capture the conversation live\n\n` +
                    `_Note: I can only transcribe meetings that I've joined. Teams' built-in transcription is separate._`;
                }
                
                const responseActivity = new MessageActivity(noTranscriptMsg).addAiGenerated();
                await send(responseActivity);
              }
            } else {
              // Check if bot was ever in this meeting
              const cachedCtx = getCachedMeetingContext(activity.conversation.id);
              const botWasInCall = !!(cachedCtx?.callStartedAt);
              
              console.log(`[TRANSCRIPT] No meeting info available. botWasInCall=${botWasInCall}, isMeetingConversation=${isMeetingConversation}`);
              
              let noInfoMsg: string;
              if (botWasInCall) {
                noInfoMsg = 
                  `I was in this meeting but can't find the transcript right now.\n\n` +
                  `**Try again in a minute** — Teams may still be processing it.`;
              } else if (isMeetingConversation) {
                noInfoMsg = 
                  `I don't have a transcript for this meeting yet.\n\n` +
                  `**To capture transcripts:**\n` +
                  `• Say "**join**" or "**join the call**" when you start the meeting\n` +
                  `• I'll join and record the conversation live\n\n` +
                  `_I can only transcribe meetings I've joined._`;
              } else {
                noInfoMsg = 
                  `This doesn't appear to be a meeting chat. To get transcripts:\n\n` +
                  `• Start a Teams meeting\n` +
                  `• Ask me to join from the meeting chat\n` +
                  `• I'll capture the conversation for you`;
              }
              
              const responseActivity = new MessageActivity(noInfoMsg).addAiGenerated();
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
    if (detectedIntent === 'insights') {
      console.log(`[ACTION] Processing key meeting insights request`);
      await sendTypingIndicator(send);
      try {
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        const meetingTitle = await resolveDisplayMeetingTitle(
          activity.conversation.id,
          requesterId,
          chatInfo?.subject
        );

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
            `No transcript available yet for this meeting.\n\n` +
            `**If this is a Meet Now/instant call:**\n` +
            `• Say "**join the call**" so I can capture live transcription\n\n` +
            `**If the meeting already ended:**\n` +
            `• Transcripts take 1-2 minutes to appear after a meeting ends`
          ).addAiGenerated();
          await send(noTranscriptActivity);
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        const insights = await generateKeyMeetingInsights(transcriptText, meetingTitle, {
          userId: requesterId,
          displayName: actorName,
          meetingId,
        });
        const insightsActivity = new MessageActivity(
          `## 💡 Key Meeting Insights\n\n${insights}`
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
    if (detectedIntent === 'minutes') {
      console.log(`[ACTION] Processing meeting minutes request`);
      await sendTypingIndicator(send);
      
      // ── Reformat/Follow-up path: use cached content instead of re-fetching ────
      const _lastMinutesResp = lastBotResponseMap.get(activity.conversation.id);
      const isMinutesFollowUp = agentDecision.parameters?.isReformatRequest &&
        _lastMinutesResp &&
        ['minutes', 'summary', 'transcript', 'insights', 'meeting_overview'].includes(_lastMinutesResp.contentType) &&
        (Date.now() - _lastMinutesResp.timestamp) < 15 * 60 * 1000;
      
      if (isMinutesFollowUp && _lastMinutesResp?.content) {
        console.log(`[MINUTES] Follow-up on cached ${_lastMinutesResp.contentType}, using cached content`);
        const formatStyle = agentDecision.parameters?.formatStyle || 'shorter';
        const isQuestion = /\b(what did|who said|tell me|what about|more about|explain|focus|mentioned|action items?|decisions?)\b/i.test(effectiveQuery || activity.text || '');
        
        const followUpPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: isQuestion 
                ? `Based on this meeting content, answer the user's question.\n\nContent:\n${_lastMinutesResp.content}\n\nUser question: "${effectiveQuery || activity.text}"`
                : `${formatStyle === 'shorter' ? 'Make this MUCH shorter - key points only' : formatStyle === 'longer' ? 'Expand with more details' : formatStyle === 'bullets' ? 'Format as bullet points' : 'Reformat as requested'}:\n\n${_lastMinutesResp.content}\n\nUser request: "${effectiveQuery || activity.text}"`
            }
          ],
          instructions: isQuestion 
            ? 'Answer based ONLY on the provided content. Do not make up information.'
            : 'Reformat the content precisely as instructed. Do not add information not present.',
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });
        
        const followUpResult = await sendPromptWithTracking(followUpPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: _lastMinutesResp.content,
        });
        
        await send(new MessageActivity(followUpResult.content || _lastMinutesResp.content).addAiGenerated().addFeedback());
        if (!isQuestion) {
          recordBotResponse(activity.conversation.id, {
            content: followUpResult.content || _lastMinutesResp.content,
            contentType: _lastMinutesResp.contentType,
            subject: _lastMinutesResp.subject,
            timestamp: Date.now(),
          });
        }
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }
      
      // Check if user also wants to email the result
      const emailRequest = detectEmailRequest(effectiveQuery || activity.text || '');
      let generatedMinutes = '';
      
      try {
        console.log(`[DEBUG] Processing meeting minutes for user`);

        // Analyze whether user asked for current meeting minutes or a past meeting by date
        const now = new Date();
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const yesterday = new Date(today.getTime() - 24 * 60 * 60 * 1000);
        const lastWeekStart = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);

        const dateContext = `Today: ${today.toISOString().split('T')[0]} (${today.toLocaleDateString('en-US', { weekday: 'long' })})
Yesterday: ${yesterday.toISOString().split('T')[0]} (${yesterday.toLocaleDateString('en-US', { weekday: 'long' })})
Last week: ${lastWeekStart.toISOString().split('T')[0]} to ${today.toISOString().split('T')[0]}`;

        // effectiveQuery already contains follow-up context if applicable
        const targetExtractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Determine what meeting the user wants minutes for.

User request: "${effectiveQuery || activity.text}"
User is currently in a meeting chat: ${isMeetingConversation ? 'YES' : 'NO'}

${dateContext}

RESPOND WITH JSON:
{
  "target": "current" | "past_meeting" | "last_meeting",
  "meeting_date": "YYYY-MM-DD if explicit date mentioned, else null",
  "meeting_subject": "title if mentioned, else null"
}`
            }
          ],
          instructions: `DECISION LOGIC (follow in order):

1. If user is in a meeting chat (YES above):
   - Default to target="current" unless they explicitly name a different date
   - Generic phrases all mean the current session
   
2. If user is NOT in a meeting chat:
   - "minutes", "last meeting minutes", "meeting notes" → target="last_meeting" with meeting_date=null
   - Only set target="past_meeting" when user gives an EXPLICIT PAST date ("yesterday", "March 10", "last Tuesday")

3. CRITICAL: "last meeting" / "most recent meeting" = target="last_meeting" with meeting_date=null. Do NOT set meeting_date to today or a timestamp.

4. meeting_date must be DATE ONLY like "2026-03-14" — never include time components.

Output valid JSON only.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const targetResponse = await sendPromptWithTracking(targetExtractPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: cleanText || activity.text || '',
        });
        const targetJsonStr = (targetResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        let targetInfo: { target: string; meeting_date: string | null; meeting_subject: string | null } = {
          target: 'current', meeting_date: null, meeting_subject: null
        };
        try {
          targetInfo = JSON.parse(targetJsonStr);
        } catch {
          console.warn(`[MINUTES] Could not parse target extraction, defaulting to current conversation`);
        }

        console.log(`[MINUTES] Target analysis: ${JSON.stringify(targetInfo)}`);

        // Code-level defense: past_meeting with today's date → last_meeting
        if (targetInfo.target === 'past_meeting') {
          const today = new Date().toISOString().split('T')[0];
          const dateOnly = targetInfo.meeting_date?.split('T')[0];
          if (!dateOnly || dateOnly === today) {
            console.log(`[MINUTES] Correcting past_meeting (date=${targetInfo.meeting_date}) → last_meeting`);
            targetInfo.target = 'last_meeting';
            targetInfo.meeting_date = null;
          }
        }

        // Handle past meeting request by date
        if (targetInfo.target === 'past_meeting' && targetInfo.meeting_date) {
          console.log(`[MINUTES] Looking up past meeting from ${targetInfo.meeting_date}`);
          const userId = activity.from.aadObjectId || activity.from.id;
          const pastMeeting = await graphApiHelper.findPastMeeting(
            userId,
            targetInfo.meeting_date,
            targetInfo.meeting_subject || undefined
          );

          if (pastMeeting.success && pastMeeting.meeting) {
            console.log(`[MINUTES] Found past meeting: "${pastMeeting.meeting.subject}"`);
            
            // Notify user we're fetching the transcript
            await send(new MessageActivity(
              `Found meeting "**${pastMeeting.meeting.subject}**". Checking for transcript availability...`
            ).addAiGenerated());
            
            // Poll for transcript with retries
            const pollResult = await pollForTranscriptReady(
              pastMeeting.meeting.organizerId,
              pastMeeting.meeting.joinWebUrl,
              pastMeeting.meeting.start ? new Date(pastMeeting.meeting.start).getTime() : undefined,
              pastMeeting.meeting.end ? new Date(pastMeeting.meeting.end).getTime() : undefined,
              6,
              5000
            );

            if (pollResult.success && pollResult.vttContent) {
              const parsed = parseVttToEntries(pollResult.vttContent);
              if (parsed.length > 0) {
                generatedMinutes = await generateMinutesHtml(
                  parsed,
                  pastMeeting.meeting.subject,
                  [],
                  pastMeeting.meeting.start,
                  { userId: requesterId, displayName: actorName, meetingId }
                );

                await send(new MessageActivity(generatedMinutes).addAiGenerated().addFeedback());

                recordBotResponse(activity.conversation.id, {
                  content: generatedMinutes,
                  contentType: 'minutes',
                    subject: `Meeting Minutes: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                  timestamp: Date.now()
                });

                if (emailRequest.wantsEmail) {
                  if (emailRequest.sendToAllAttendees) {
                    const emailResult = await autoEmailSummaryToParticipants(
                      activity.conversation.id,
                      activity.from.aadObjectId || activity.from.id,
                      `Meeting Minutes: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                      generatedMinutes
                    );
                    if (emailResult.sentCount > 0) {
                      await send(new MessageActivity(`Done! I've emailed these minutes to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
                    }
                  } else if (emailRequest.emailAddress) {
                    const sendResult = await graphApiHelper.sendEmail(
                      activity.from.aadObjectId || activity.from.id,
                      emailRequest.emailAddress,
                      `Meeting Minutes: ${pastMeeting.meeting.subject} — ${config.botDisplayName}`,
                      generatedMinutes
                    );
                    if (sendResult.success) {
                      await send(new MessageActivity(`Done! I've emailed these minutes to **${emailRequest.emailAddress}**.`).addAiGenerated());
                    }
                  }
                }

                storage.set(conversationKey, messages);
                storage.set(sharedConversationKey, sharedMessages);
                storage.set(llmConversationKey, llmMessages);
                return;
              }
            }

            await send(new MessageActivity(
              `I found the meeting "**${pastMeeting.meeting.subject}**" but no transcript is available yet. ` +
              `${pollResult.error || 'Transcription may not have been enabled, or the transcript is still processing.'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }

          await send(new MessageActivity(
            `I couldn't find a Teams meeting on ${targetInfo.meeting_date}. ` +
            `${pastMeeting.error || 'Please check the date and try again.'}`
          ).addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        // --- LAST MEETING LOOKUP (most recent from calendar) ---
        if (targetInfo.target === 'last_meeting' || (!isMeetingConversation && targetInfo.target === 'current')) {
          console.log(`[MINUTES] Looking up recent meetings from calendar`);
          const userId = activity.from.aadObjectId || activity.from.id;
          const pastMeetings = await graphApiHelper.findPastMeetings(userId, undefined, targetInfo.meeting_subject || undefined, 5);
          
          if (pastMeetings.success && pastMeetings.meetings.length > 0) {
            let foundTranscript = false;
            
            for (const meeting of pastMeetings.meetings) {
              console.log(`[MINUTES] Trying meeting: "${meeting.subject}"`);
              
              const pollResult = await pollForTranscriptReady(
                meeting.organizerId,
                meeting.joinWebUrl,
                meeting.start ? new Date(meeting.start).getTime() : undefined,
                meeting.end ? new Date(meeting.end).getTime() : undefined,
                3, 3000
              );
              
              if (pollResult.success && pollResult.vttContent) {
                const parsed = parseVttToEntries(pollResult.vttContent);
                if (parsed.length > 0) {
                  console.log(`[MINUTES] Got ${parsed.length} transcript entries from "${meeting.subject}"`);
                  
                  await send(new MessageActivity(
                    `Found transcript for "**${meeting.subject}**". Generating minutes...`
                  ).addAiGenerated());
                  
                  generatedMinutes = await generateMinutesHtml(
                    parsed,
                    meeting.subject,
                    [],
                    meeting.start,
                    { userId: requesterId, displayName: actorName, meetingId }
                  );
                  
                  await send(new MessageActivity(generatedMinutes).addAiGenerated().addFeedback());
                  
                  recordBotResponse(activity.conversation.id, {
                    content: generatedMinutes,
                    contentType: 'minutes',
                    subject: `Meeting Minutes: ${meeting.subject} — ${config.botDisplayName}`,
                    timestamp: Date.now()
                  });
                  
                  if (emailRequest.wantsEmail) {
                    if (emailRequest.sendToAllAttendees) {
                      const emailResult = await autoEmailSummaryToParticipants(
                        activity.conversation.id,
                        activity.from.aadObjectId || activity.from.id,
                        `Meeting Minutes: ${meeting.subject} — ${config.botDisplayName}`,
                        generatedMinutes
                      );
                      if (emailResult.sentCount > 0) {
                        await send(new MessageActivity(`Done! I've emailed these minutes to **${emailResult.sentCount} attendee(s)**.`).addAiGenerated());
                      }
                    } else if (emailRequest.emailAddress) {
                      const sendResult = await graphApiHelper.sendEmail(
                        activity.from.aadObjectId || activity.from.id,
                        emailRequest.emailAddress,
                        `Meeting Minutes: ${meeting.subject} — ${config.botDisplayName}`,
                        generatedMinutes
                      );
                      if (sendResult.success) {
                        await send(new MessageActivity(`Done! I've emailed these minutes to **${emailRequest.emailAddress}**.`).addAiGenerated());
                      }
                    }
                  }
                  
                  foundTranscript = true;
                  break;
                }
              }
              console.log(`[MINUTES] No transcript for "${meeting.subject}", trying next...`);
            }
            
            if (!foundTranscript) {
              console.log(`[MINUTES] No transcripts found in ${pastMeetings.meetings.length} recent meetings`);
              await send(new MessageActivity(
                `No transcripts are available for your recent meetings. Transcription needs to be enabled during the call for me to generate minutes.`
              ).addAiGenerated().addFeedback());
            }
            
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          } else if (!isMeetingConversation) {
            await send(new MessageActivity(
              `I couldn't find any recent Teams meetings in your calendar. ` +
              `${pastMeetings.error || 'Try specifying a date like "minutes from yesterday\'s meeting".'}`
            ).addAiGenerated().addFeedback());
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }

        // First, try to get meeting transcript for minutes
        const liveEntries = liveTranscriptMap.get(activity.conversation.id);
        const transcriptEntries = liveEntries?.filter(e => e.isFinal) || [];

        if (transcriptEntries.length > 0) {
          console.log(`[MINUTES] Found ${transcriptEntries.length} transcript entries, generating formal minutes...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
          const meetingTitle = await resolveDisplayMeetingTitle(
            activity.conversation.id,
            requesterId,
            chatInfo?.subject
          );
          const memberList = chatMembers.length > 0 ? chatMembers : [userName];

          generatedMinutes = await generateMinutesHtml(
            transcriptEntries,
            meetingTitle,
            memberList,
            chatInfo?.startDateTime, // Use actual meeting date
            { userId: requesterId, displayName: actorName, meetingId }
          );
          console.log(`[MINUTES] Minutes generated successfully from transcript`);

          const responseActivity = new MessageActivity(generatedMinutes).addAiGenerated().addFeedback();
          await send(responseActivity);
          console.log(`[SUCCESS] Transcript-based minutes sent to user`);
        } else {
          // No local transcript - try to fetch from Graph API
          console.log(`[MINUTES] No local transcript, checking cache then Graph API...`);
          const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          
          if (chatInfo?.organizer?.id && chatInfo?.joinWebUrl) {
            const transcriptWindow = getTranscriptWindowForConversation(activity.conversation.id);
            const minutesFetch = await fetchTranscriptCacheFirst(
              chatInfo.organizer.id,
              chatInfo.joinWebUrl,
              transcriptWindow.min,
              transcriptWindow.max
            );
            
            if (minutesFetch.entries.length > 0) {
              const parsed = minutesFetch.entries;
                console.log(`[MINUTES] Got ${parsed.length} entries (fromCache=${minutesFetch.fromCache})`);
                liveTranscriptMap.set(activity.conversation.id, parsed);
                saveTranscriptToFile(activity.conversation.id);
                
                const meetingTitle = await resolveDisplayMeetingTitle(
                  activity.conversation.id,
                  requesterId,
                  chatInfo.subject
                );
                const chatMembers = await graphApiHelper.getChatMembers(activity.conversation.id);
                
                generatedMinutes = await generateMinutesHtml(
                  parsed,
                  meetingTitle,
                  chatMembers,
                  chatInfo.startDateTime, // Use actual meeting date
                  { userId: requesterId, displayName: actorName, meetingId }
                );
                
                await send(new MessageActivity(generatedMinutes).addAiGenerated().addFeedback());
                
                recordBotResponse(activity.conversation.id, {
                  content: generatedMinutes,
                  contentType: 'minutes',
                    subject: `Meeting Minutes: ${meetingTitle} — ${config.botDisplayName}`,
                  timestamp: Date.now()
                });
                
                // Handle email if requested
                if (emailRequest.wantsEmail) {
                  if (emailRequest.sendToAllAttendees) {
                    const emailResult = await autoEmailSummaryToParticipants(
                      activity.conversation.id,
                      activity.from.aadObjectId || activity.from.id,
                      `Meeting Minutes: ${meetingTitle} — ${config.botDisplayName}`,
                      generatedMinutes
                    );
                    if (emailResult.sentCount > 0) {
                      await send(new MessageActivity(`Done! I've emailed the minutes to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
                    }
                  } else {
                    let recipientEmail = emailRequest.emailAddress;
                    if (!recipientEmail) {
                      const chatMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
                      const selfMember = chatMembers.find((m) => m.userId === (activity.from.aadObjectId || activity.from.id) || m.displayName.toLowerCase() === (actorName || '').toLowerCase());
                      recipientEmail = selfMember?.email || '';
                      if (!recipientEmail) {
                        const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
                        recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
                      }
                    }
                    if (recipientEmail) {
                      const emailResult = await graphApiHelper.sendEmail(
                        activity.from.aadObjectId || activity.from.id,
                        recipientEmail,
                        `Meeting Minutes: ${meetingTitle} — ${config.botDisplayName}`,
                        generatedMinutes,
                        { replyToEmail: recipientEmail, replyToName: actorName }
                      );
                      if (emailResult.success) {
                        await send(new MessageActivity(`Done! I've emailed the minutes to **${recipientEmail}**.`).addAiGenerated());
                      }
                    }
                  }
                }
                
                storage.set(conversationKey, messages);
                storage.set(sharedConversationKey, sharedMessages);
                storage.set(llmConversationKey, llmMessages);
                return;
              }
            }
          
          // No transcript found - generate natural response
          const noTranscriptMsg = isMeetingConversation
            ? `I need a transcript to generate meeting minutes. Ask me to **join the call** if you're in a meeting, or say **transcribe** if the meeting was recorded.`
            : `I can only create minutes from meeting transcripts. Start a Teams meeting and invite me, or ask about a past meeting like "minutes from yesterday's sync"!`;
          
          await send(new MessageActivity(noTranscriptMsg).addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }
        
        // If user requested email, send it now
        if (emailRequest.wantsEmail && generatedMinutes) {
          console.log(`[EMAIL] User requested minutes via email`);
          if (emailRequest.sendToAllAttendees) {
            const emailResult = await autoEmailSummaryToParticipants(
              activity.conversation.id,
              activity.from.aadObjectId || activity.from.id,
              `Meeting Minutes from ${config.botDisplayName}`,
              generatedMinutes
            );
            if (emailResult.sentCount > 0) {
              await send(new MessageActivity(`Done! I've emailed the minutes to **${emailResult.sentCount} attendee(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.`).addAiGenerated());
            } else {
              await send(new MessageActivity(`I couldn't email the minutes to attendees. No valid email addresses found.`).addAiGenerated());
            }
          } else {
            let recipientEmail = emailRequest.emailAddress;
            
            // If no email in message, get user's email
            if (!recipientEmail) {
              const chatMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
              const selfMember = chatMembers.find((m) => m.userId === (activity.from.aadObjectId || activity.from.id) || m.displayName.toLowerCase() === (actorName || '').toLowerCase());
              recipientEmail = selfMember?.email || '';
              if (!recipientEmail) {
                const userInfo = await graphApiHelper.getUserInfo(activity.from.aadObjectId || activity.from.id);
                recipientEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
              }
            }
            
            if (recipientEmail) {
              const emailResult = await graphApiHelper.sendEmail(
                activity.from.aadObjectId || activity.from.id,
                recipientEmail,
                `Meeting Minutes from ${config.botDisplayName}`,
                generatedMinutes
              );
              
              if (emailResult.success) {
                await send(new MessageActivity(`Done! I've emailed the minutes to **${recipientEmail}**.`).addAiGenerated());
              } else {
                await send(new MessageActivity(`Hmm, I couldn't send that email - ${emailResult.error || 'something went wrong'}. Want to try again?`).addAiGenerated());
              }
            } else {
              await send(new MessageActivity(`I couldn't figure out your email address. Could you tell me where to send it?`).addAiGenerated());
            }
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

    if (detectedIntent === 'meeting_question') {
      console.log(`[ACTION] Processing meeting question`);
      await sendTypingIndicator(send);
      
      // First check if we have cached meeting content to answer from (avoids Graph calls)
      const _cachedMeetingContent = lastBotResponseMap.get(activity.conversation.id);
      const hasCachedMeetingContent = _cachedMeetingContent &&
        ['summary', 'minutes', 'transcript', 'insights', 'meeting_overview'].includes(_cachedMeetingContent.contentType) &&
        (Date.now() - _cachedMeetingContent.timestamp) < 30 * 60 * 1000 && // Within 30 minutes
        _cachedMeetingContent.content;
      
      if (hasCachedMeetingContent) {
        // Answer from cached content - no Graph API call needed
        console.log(`[MEETING_QA] Answering from cached ${_cachedMeetingContent.contentType} content`);
        const answerPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Based on this meeting content, answer the user's question.

Meeting content (${_cachedMeetingContent.contentType}):
${_cachedMeetingContent.content}

User question: "${effectiveQuery || activity.text}"`
            }
          ],
          instructions: `Answer based ONLY on the provided meeting content. If the information is not present, say so clearly. Be concise and direct.`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });
        
        const answer = await sendPromptWithTracking(answerPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: _cachedMeetingContent.content,
        });
        
        await send(new MessageActivity(answer.content || "I couldn't find that information in the meeting content.").addAiGenerated().addFeedback());
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }
      
      // If in a meeting conversation, use the existing meeting context
      if (isMeetingConversation) {
      try {
        const chatInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        const meetingTitle = await resolveDisplayMeetingTitle(
          activity.conversation.id,
          requesterId,
          chatInfo?.subject
        );

        const transcriptResult = await getTranscriptWithContext(activity.conversation.id);
        const transcriptContext = transcriptResult.text;
        const hasTranscript = transcriptContext && transcriptContext.trim().length > 50;

        // If the bot is in an active call but has no transcript captured yet,
        // don't mislead the user by answering from chat messages. Be clear.
        const isInActiveCall = hasActiveLiveTranscript(activity.conversation.id) ||
          Array.from(activeCallMap.values()).some(c => c.conversationId === activity.conversation.id);

        if (!hasTranscript && isInActiveCall) {
          console.log(`[MEETING_QA] Active call but no transcript data captured yet`);
          const answer =
            `I'm in the meeting but don't have any spoken-word transcript data yet.\n\n` +
            `**Why?** The Microsoft Graph API only makes transcript content available after transcription is stopped or the meeting ends. ` +
            `During an active call, I can detect that transcription is on, but I can't access the live transcript text in real time.\n\n` +
            `**What you can do:**\n` +
            `- **Stop transcription** briefly (click ••• → Record & Transcribe → Stop transcription), then **restart** it. ` +
            `The stopped segment's transcript will become available and I can pull it.\n` +
            `- **After the meeting ends**, ask me to **transcribe** or **summarize** — the full transcript will be available then.\n` +
            `- You can also ask me to **read chats** to see what was typed in the meeting chat.`;
          await send(new MessageActivity(answer).addAiGenerated().addFeedback());
          console.log(`[SUCCESS] Informed user about live transcript limitation`);
        } else {
          const shared = storage.get(sharedConversationKey) || [];
          const conversationContext = buildConversationContext(shared, 120);

          // effectiveQuery already contains follow-up context if applicable
          const answer = await answerMeetingQuestionWithContext(
            effectiveQuery || activity.text || '',
            meetingTitle,
            conversationContext,
            transcriptContext,
            { userId: requesterId, displayName: actorName, meetingId }
          );

          await send(new MessageActivity(answer).addAiGenerated().addFeedback());
          console.log(`[SUCCESS] Meeting question answered with conversation + transcript context`);
        }
      } catch (error) {
        console.error(`[ERROR_MEETING_QA]`, error);
        await send(new MessageActivity('I encountered an error while answering based on meeting context. Please try again.').addAiGenerated());
      }
      } else {
        // 1:1 chat - try to fetch user's most recent meeting transcript
        console.log(`[MEETING_QA] 1:1 chat - looking up user's most recent meeting`);
        const userId = activity.from.aadObjectId || activity.from.id;
        
        const pastMeetings = await graphApiHelper.findPastMeetings(userId, undefined, undefined, 3);
        
        if (pastMeetings.success && pastMeetings.meetings.length > 0) {
          let foundTranscript = false;
          
          for (const meeting of pastMeetings.meetings) {
            const pollResult = await pollForTranscriptReady(
              meeting.organizerId,
              meeting.joinWebUrl,
              meeting.start ? new Date(meeting.start).getTime() : undefined,
              meeting.end ? new Date(meeting.end).getTime() : undefined,
              2, // quick check
              2000
            );
            
            if (pollResult.success && pollResult.vttContent) {
              const parsed = parseVttToEntries(pollResult.vttContent);
              if (parsed.length > 0) {
                console.log(`[MEETING_QA] Found transcript for "${meeting.subject}", answering question`);
                const transcriptText = parsed.map(e => `${e.speaker}: ${e.text}`).join('\n');
                
                const answerPrompt = new ChatPrompt({
                  messages: [
                    {
                      role: 'user',
                      content: `Based on this meeting transcript from "${meeting.subject}", answer the user's question.

Transcript:
${transcriptText.slice(0, 15000)}

User question: "${effectiveQuery || activity.text}"

Instructions:
- Synthesize and paraphrase — do NOT copy-paste raw transcript lines or dump verbatim quotes with timestamps
- Tell the story naturally, as a knowledgeable colleague would explain it
- Only reference timestamps if the user specifically asks about timing
- Weave in short key phrases or quotes only when they add flavor, not entire paragraphs
- Keep it focused and readable — a few well-written paragraphs, not a wall of bullet points
- If unsure the transcript fully answers the question, mention that briefly at the end`
                    }
                  ],
                  instructions: `You are a friendly meeting assistant. Answer based ONLY on the provided transcript. Be smart about what matters — read the raw transcript, understand it, and give a clear, natural answer. Do NOT dump raw lines with timestamps. Paraphrase and synthesize. Mention the meeting title naturally.`,
                  model: new OpenAIChatModel({
                    model: config.azureOpenAIDeploymentName,
                    apiKey: config.azureOpenAIKey,
                    endpoint: config.azureOpenAIEndpoint,
                    apiVersion: '2024-10-21'
                  })
                });
                
                const answer = await sendPromptWithTracking(answerPrompt, '', {
                  userId: requesterId,
                  displayName: actorName,
                  meetingId,
                  estimatedInputText: transcriptText.slice(0, 15000),
                });
                
                await send(new MessageActivity(answer.content || "I couldn't find that information in the meeting transcript.").addAiGenerated().addFeedback());
                
                // Cache for follow-up questions
                recordBotResponse(activity.conversation.id, {
                  content: transcriptText.slice(0, 20000),
                  contentType: 'transcript',
                  subject: meeting.subject,
                  timestamp: Date.now(),
                });
                
                foundTranscript = true;
                break;
              }
            }
          }
          
          if (!foundTranscript) {
            await send(new MessageActivity(
              `I couldn't find any transcripts for your recent meetings. To answer questions about a meeting, I need the transcript to be available.\n\n` +
              `**Tip:** Ask me to **summarize** your last meeting first, then follow up with questions.`
            ).addAiGenerated().addFeedback());
          }
        } else {
          await send(new MessageActivity(
            `I couldn't find any recent meetings on your calendar. Ask me about a specific meeting or try summarizing your last meeting first.`
          ).addAiGenerated().addFeedback());
        }
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    if (detectedIntent === 'check_inbox' || detectedIntent === 'reply_email') {
      // ── Guard: if bot just showed a reply draft and user is confirming send,
      // redirect to send_email logic which handles in-thread replies properly.
      const _lastResp = lastBotResponseMap.get(activity.conversation.id);
      const isDraftConfirmation =
        _lastResp &&
        (Date.now() - _lastResp.timestamp) < 10 * 60 * 1000 &&
        (_lastResp.subject || '').toLowerCase().startsWith('email reply draft') &&
        /^(yes|yeah|yep|sure|ok|okay|go ahead|send|send it|send that|yes send|do it)[\s!.,]*$/i.test((cleanText || '').trim());
      if (isDraftConfirmation) {
        console.log(`[INBOX→SEND] User confirming draft send, redirecting to send_email handler`);
        // Fall through to send_email handler below by NOT entering this block
      } else {
      console.log(`[ACTION] Processing inbox capability request`);
      await sendTypingIndicator(send);

      try {
        const inboxRequest = parseInboxRequest(effectiveQuery || activity.text || '');
        console.log(`[INBOX] Request: reply=${inboxRequest.wantsReplyDraft}, max=${inboxRequest.maxResults}`);
        
        const mailboxUserId = activity.from.aadObjectId || activity.from.id;
        const cachedInboxCtx = inboxContextMap.get(activity.conversation.id);

        // ── Fast path: reply_email with a cached matched message ─────────────────
        if (
          detectedIntent === 'reply_email' &&
          cachedInboxCtx?.lastMessages?.length
        ) {
          // Use LLM to find the right message from cache
          const searchResult = await llmSearchInbox(
            effectiveQuery || activity.text || '',
            cachedInboxCtx.lastMessages,
            sendPromptWithTracking,
            { userId: requesterId, displayName: actorName, meetingId, estimatedInputText: effectiveQuery || '' }
          );
          
          const targetMsg = searchResult.matchingMessages[0] || cachedInboxCtx.lastMessages[0];
          if (targetMsg && cachedInboxCtx.mailboxUserId) {
            rememberMatchedInboxThread(activity.conversation.id, mailboxUserId, targetMsg);
            const threadMessages = targetMsg.conversationId
              ? await graphApiHelper.getMailConversationMessages(mailboxUserId, targetMsg.conversationId, 8)
              : [];
            const draftReply = await draftReplyFromInboxThread(
              cleanText || activity.text || '',
              threadMessages.length ? [...threadMessages].reverse() : [targetMsg],
              sendPromptWithTracking,
              {
                userId: requesterId,
                displayName: actorName,
                meetingId,
                estimatedInputText: `${cleanText || activity.text || ''}\n${JSON.stringify(threadMessages.length ? threadMessages : [targetMsg])}`,
              },
              { name: targetMsg.fromName || '', email: targetMsg.fromAddress || '' }
            );
            await send(new MessageActivity(draftReply).addAiGenerated().addFeedback());
            recordBotResponse(activity.conversation.id, {
              content: draftReply,
              contentType: 'general',
              subject: `Email Reply Draft for ${targetMsg.fromName || targetMsg.fromAddress}`,
              timestamp: Date.now(),
            });
            storage.set(conversationKey, messages);
            storage.set(sharedConversationKey, sharedMessages);
            storage.set(llmConversationKey, llmMessages);
            return;
          }
        }

        // ── Reformat path: user wants to reformat last inbox content ────────────
        if (agentDecision.parameters?.isReformatRequest && cachedInboxCtx?.lastMessages?.length) {
          console.log(`[INBOX] Reformat request with formatStyle=${agentDecision.parameters.formatStyle}`);
          const formatInstruction = agentDecision.parameters.formatStyle === 'shorter' 
            ? 'Make this MUCH shorter - 2 paragraphs maximum'
            : agentDecision.parameters.formatStyle === 'longer'
            ? 'Provide more details and expand on key points'
            : agentDecision.parameters.formatStyle === 'bullets'
            ? 'Format as clear bullet points'
            : 'Reformat this content';
          
          const reformatQuery = `${formatInstruction}. Original request: ${effectiveQuery}`;
          const reformattedSummary = await summarizeInboxMessages(
            reformatQuery,
            cachedInboxCtx.lastMessages,
            sendPromptWithTracking,
            {
              userId: requesterId,
              displayName: actorName,
              meetingId,
              estimatedInputText: `${reformatQuery}\n${JSON.stringify(cachedInboxCtx.lastMessages)}`,
            }
          );
          await send(new MessageActivity(reformattedSummary).addAiGenerated().addFeedback());
          recordBotResponse(activity.conversation.id, {
            content: reformattedSummary,
            contentType: 'inbox_email',
            subject: `Inbox Summary from ${config.botDisplayName}`,
            timestamp: Date.now(),
          });
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        // ── Follow-up question path: answer questions about cached email content ────
        const isFollowUpQuestion = cachedInboxCtx?.lastMessages?.length &&
          cachedInboxCtx.updatedAt > Date.now() - 10 * 60 * 1000 &&
          /\b(what did|who said|tell me|what about|more about|explain|details|focus|priority|mentioned)\b/i.test(effectiveQuery || '');
        
        if (isFollowUpQuestion) {
          console.log(`[INBOX] Follow-up question about cached email content`);
          const emailContext = cachedInboxCtx.lastMessages.map(m => ({
            from: m.fromName || m.fromAddress,
            subject: m.subject,
            date: m.receivedDateTime,
            body: m.bodyContent || m.bodyPreview || '',
          }));

          const answerPrompt = new ChatPrompt({
            messages: [
              {
                role: 'user',
                content:
                  `User question: "${effectiveQuery}"\n\n` +
                  `Email content shown earlier:\n${JSON.stringify(emailContext, null, 2)}\n\n` +
                  `Instructions:\n` +
                  `- Answer the user's question ONLY using information from the email content above\n` +
                  `- If the information is not in the email, say "I don't see that information in the email that was shown"\n` +
                  `- Be concise and direct\n` +
                  `- Use markdown formatting\n` +
                  `- NEVER make up or hallucinate information not present in the emails`,
              },
            ],
            instructions: 'Answer strictly from the provided email content. Never hallucinate.',
            model: new OpenAIChatModel({
              model: config.azureOpenAIDeploymentName,
              apiKey: config.azureOpenAIKey,
              endpoint: config.azureOpenAIEndpoint,
              apiVersion: '2024-10-21',
            }),
          });

          const answerResponse = await sendPromptWithTracking(answerPrompt, '', {
            userId: requesterId,
            displayName: actorName,
            meetingId,
            estimatedInputText: `${effectiveQuery}\n${JSON.stringify(emailContext)}`,
          });

          await send(new MessageActivity(answerResponse.content || 'I couldn\'t find that information in the email shown.').addAiGenerated().addFeedback());
          recordBotResponse(activity.conversation.id, {
            content: answerResponse.content || '',
            contentType: 'inbox_email',
            subject: `Follow-up Answer from ${config.botDisplayName}`,
            timestamp: Date.now(),
          });
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        // ── Normal path: fetch ALL inbox messages, let LLM find relevant ones ────
        const allInboxMessages = await graphApiHelper.getInboxMessages(mailboxUserId, {
          top: 20, // Fetch more messages for LLM to search through
        });
        console.log(`[INBOX] Fetched ${allInboxMessages.length} total messages`);

        if (!allInboxMessages.length) {
          await send(new MessageActivity('Your inbox appears to be empty.').addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        // Use LLM to find messages matching the user's query
        const searchResult = await llmSearchInbox(
          effectiveQuery || activity.text || '',
          allInboxMessages,
          sendPromptWithTracking,
          { userId: requesterId, displayName: actorName, meetingId, estimatedInputText: effectiveQuery || '' }
        );
        console.log(`[INBOX] LLM search found ${searchResult.matchingMessages.length} matches: ${searchResult.searchReasoning}`);

        // Cache results for follow-ups
        cacheInboxContext(activity.conversation.id, searchResult.matchingMessages.length ? searchResult.matchingMessages : allInboxMessages.slice(0, 5));
        if (searchResult.matchingMessages.length > 0) {
          rememberMatchedInboxThread(activity.conversation.id, mailboxUserId, searchResult.matchingMessages[0]);
        }

        if (!searchResult.matchingMessages.length) {
          await send(new MessageActivity(searchResult.noMatchReason || 'No matching emails found.').addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }

        if (detectedIntent === 'reply_email' || inboxRequest.wantsReplyDraft) {
          const primaryMessage = searchResult.matchingMessages[0];
          rememberMatchedInboxThread(activity.conversation.id, mailboxUserId, primaryMessage);
          const threadMessages = primaryMessage.conversationId
            ? await graphApiHelper.getMailConversationMessages(mailboxUserId, primaryMessage.conversationId, 8)
            : [];
          const draftReply = await draftReplyFromInboxThread(
            cleanText || activity.text || '',
            threadMessages.length ? [...threadMessages].reverse() : [primaryMessage],
            sendPromptWithTracking,
            {
              userId: requesterId,
              displayName: actorName,
              meetingId,
              estimatedInputText: `${cleanText || activity.text || ''}\n${JSON.stringify(threadMessages.length ? threadMessages : [primaryMessage])}`,
            },
            { name: primaryMessage.fromName || '', email: primaryMessage.fromAddress || '' }
          );
          await send(new MessageActivity(draftReply).addAiGenerated().addFeedback());
          recordBotResponse(activity.conversation.id, {
            content: draftReply,
            contentType: 'general',
            subject: `Email Reply Draft for ${primaryMessage.fromName || primaryMessage.fromAddress}`,
            timestamp: Date.now(),
          });
        } else {
          const inboxSummary = await summarizeInboxMessages(
            effectiveQuery || activity.text || '',
            searchResult.matchingMessages,
            sendPromptWithTracking,
            {
              userId: requesterId,
              displayName: actorName,
              meetingId,
              estimatedInputText: `${effectiveQuery || activity.text || ''}\n${JSON.stringify(searchResult.matchingMessages)}`,
            }
          );
          await send(new MessageActivity(inboxSummary).addAiGenerated().addFeedback());
          recordBotResponse(activity.conversation.id, {
            content: inboxSummary,
            contentType: 'inbox_email',
            subject: `Inbox Summary from ${config.botDisplayName}`,
            timestamp: Date.now(),
          });
        }
      } catch (error) {
        console.error(`[ERROR_INBOX]`, error);
        await send(new MessageActivity(
          `I couldn't review the inbox right now. Please verify mail permissions and try again.`
        ).addAiGenerated().addFeedback());
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    } // end isDraftConfirmation else
    }

    if (detectedIntent === 'join_meeting') {
      console.log(`[ACTION] Processing join-call flow`);

      const monthlyJoinCheck = canUserJoinMeetingThisMonth(requesterId);
      if (!monthlyJoinCheck.allowed) {
        await send(
          new MessageActivity(
            `Free tier limit reached: you've used ${monthlyJoinCheck.used}/${monthlyJoinCheck.limit} meeting joins this month. ` +
            `Upgrade your plan or wait for next month to join more meetings.`
          ).addAiGenerated().addFeedback()
        );
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }

      // Guard: Check if bot is already in this meeting's call
      const existingCall = Array.from(activeCallMap.entries()).find(
        ([_, call]) => call.conversationId === activity.conversation.id && !call.terminatedAt
      );
      if (existingCall) {
        console.log(`[JOIN_FLOW] Bot already in call ${existingCall[0]} for this conversation - skipping duplicate join`);
        const alreadyJoinedActivity = new MessageActivity(
          `I'm already in this meeting! Just ask me to **summarize**, generate **minutes**, or **transcribe** when you're ready.`
        ).addAiGenerated().addFeedback();
        await send(alreadyJoinedActivity);
        storage.set(conversationKey, messages);
        storage.set(sharedConversationKey, sharedMessages);
        storage.set(llmConversationKey, llmMessages);
        return;
      }

      // Attempt to join the meeting as an actual participant via Graph Calls API
      const botEndpoint = process.env.BOT_ENDPOINT || '';
      const callbackUri = botEndpoint ? `${botEndpoint}/api/calls` : '';
      const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID || '';

      let joinedCall = false;
      if (callbackUri && tenantId) {
        // Cancel any previous pending retry for this conversation
        cancelPendingJoin(activity.conversation.id);
        recordMeetingJoinRequest(requesterId, actorName, activityTenantId);

        console.log(`[CALLS_API] Getting meeting info for join attempt, callback: ${callbackUri}`);
        const meetingOnlineInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
        if (meetingOnlineInfo?.organizer?.id) {
          recordMeetingJoinForQuota(
            requesterId,
            actorName,
            meetingId,
            meetingOnlineInfo.subject || getCachedMeetingContext(meetingId)?.subject || 'Meeting',
            activityTenantId
          );
          const callResult = await graphApiHelper.joinMeetingCall(meetingOnlineInfo, callbackUri, tenantId, activity.conversation.id);
          if (callResult) {
            joinedCall = true;
            activeCallMap.set(callResult.id, {
              conversationId: activity.conversation.id,
              serviceUrl: activity.serviceUrl || '',
              organizerId: meetingOnlineInfo.organizer?.id || getCachedMeetingContext(activity.conversation.id)?.organizerId,
              joinWebUrl: meetingOnlineInfo.joinWebUrl || getCachedMeetingContext(activity.conversation.id)?.joinWebUrl,
              onlineMeetingId: (meetingOnlineInfo as any)?.onlineMeetingId,
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
              `Joining the meeting now! I'll automatically connect once someone starts the call (I'll keep trying for up to 5 minutes). Once I'm in, just ask me to **summarize**, generate **minutes**, or fetch the **transcript**.`
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
          `Hey! I'm here and ready to help. During or after your meeting, just ask me to **summarize**, generate **minutes**, or fetch a **transcript** from Teams.`
        ).addAiGenerated().addFeedback();
        await send(readyActivity);
      }

      storage.set(conversationKey, messages);
      storage.set(sharedConversationKey, sharedMessages);
      storage.set(llmConversationKey, llmMessages);
      return;
    }

    if (detectedIntent === 'read_chats') {
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
          .map((m) => `� ${(m.from?.user?.displayName || (m.from as any)?.application?.displayName || 'Unknown')}: ${m.body?.content || ''}`)
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
        // effectiveQuery already contains follow-up context if applicable
        const userText = effectiveQuery || activity.text || '';
        const lastResponse = lastBotResponseMap.get(activity.conversation.id);
        const hasRecentContext = lastResponse && (Date.now() - lastResponse.timestamp) < 10 * 60 * 1000; // within 10 minutes
        
        // Use LLM to intelligently analyze the email request
        const emailAnalysis = await analyzeEmailRequest(
          userText,
          !!hasRecentContext,
          lastResponse?.contentType,
          sendPromptWithTracking,
          {
            userId: requesterId,
            displayName: actorName,
            meetingId,
            estimatedInputText: `${userText}\n${lastResponse?.contentType || ''}`,
          }
        );
        console.log(`[EMAIL] LLM Analysis: contextual=${emailAnalysis.isContextualReference}, contentType=${emailAnalysis.contentType}, recipientType=${emailAnalysis.recipientType}, emails=${emailAnalysis.recipientEmails.join(',')}, names=${emailAnalysis.recipientNames.join(',')}, reasoning=${emailAnalysis.reasoning}`);
        
        // Determine recipient emails - support multiple recipients
        let recipientEmails: string[] = [...emailAnalysis.recipientEmails];
        let recipientNames: string[] = [...emailAnalysis.recipientNames];
        let effectiveRecipientType = emailAnalysis.recipientType;
        
        // HARDCODED OVERRIDE: If user says "all attendees"/"all participants", ALWAYS set all_participants
        const allAttendeesRequested = /\b(all|every)\s*(meeting\s*)?(attendees?|participants?|members?|invitees?)\b/i.test(userText);
        if (allAttendeesRequested && effectiveRecipientType !== 'all_participants') {
          console.log(`[EMAIL] Forcing all_participants based on "all attendees" phrase in request`);
          effectiveRecipientType = 'all_participants';
        }

        if (
          emailAnalysis.isContextualReference &&
          hasRecentContext &&
          lastResponse?.recipientType === 'all_participants' &&
          recipientEmails.length === 0 &&
          recipientNames.length === 0 &&
          (emailAnalysis.recipientType === null || emailAnalysis.recipientType === 'self')
        ) {
          effectiveRecipientType = 'all_participants';
          recipientEmails = [...(lastResponse.recipientEmails || [])];
          recipientNames = [...(lastResponse.recipientNames || [])];
          console.log(`[EMAIL] Reusing recipient target from previous draft: all_participants (${recipientEmails.length} cached)`);
        }
        
        // If LLM found recipient names but no emails, try to resolve them from chat members
        if (recipientEmails.length === 0 && emailAnalysis.recipientNames.length > 0 && (effectiveRecipientType === 'other' || effectiveRecipientType === 'multiple')) {
          console.log(`[EMAIL] Trying to resolve emails for: ${emailAnalysis.recipientNames.join(', ')}`);
          for (const name of emailAnalysis.recipientNames) {
            const memberMatch = await graphApiHelper.findMemberEmailByName(activity.conversation.id, name);
            if (memberMatch && memberMatch.email) {
              recipientEmails.push(memberMatch.email);
              // Update name to resolved display name
              const idx = recipientNames.indexOf(name);
              if (idx >= 0) recipientNames[idx] = memberMatch.displayName;
              console.log(`[EMAIL] Resolved "${name}" to ${memberMatch.displayName} <${memberMatch.email}>`);
              continue;
            }

            const inboxMatch = resolveRecipientFromInboxContext(activity.conversation.id, name);
            if (inboxMatch?.email) {
              recipientEmails.push(inboxMatch.email);
              const idx = recipientNames.indexOf(name);
              if (idx >= 0) recipientNames[idx] = inboxMatch.displayName;
              console.log(`[EMAIL] Resolved "${name}" from inbox context to ${inboxMatch.displayName} <${inboxMatch.email}>`);
            }
          }
        }
        
        // Handle "all_participants" - resolve from calendar first, then current chat members only.
        if (effectiveRecipientType === 'all_participants' && recipientEmails.length === 0) {
          console.log(`[EMAIL] Resolving all participant emails from calendar first`);
          
          // Get meeting joinWebUrl for accurate calendar matching (includes external attendees)
          let meetingJoinUrl: string | undefined;
          try {
            const meetingInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
            meetingJoinUrl = meetingInfo?.joinWebUrl || getCachedMeetingContext(activity.conversation.id)?.joinWebUrl;
            if (meetingJoinUrl) {
              console.log(`[EMAIL] Have meeting joinWebUrl for calendar lookup`);
            }
          } catch (err) {
            console.warn(`[EMAIL] Could not get meeting info:`, err);
          }
          
          const calendarRecipients = await resolveCalendarAttendeesForRequest(
            activity.from.aadObjectId || activity.from.id,
            lastResponse?.sourceRequest || userText,
            activity.conversation.id,
            meetingJoinUrl // Pass joinWebUrl for exact matching
          );
          recipientEmails = [...calendarRecipients.emails];
          recipientNames = [...calendarRecipients.names];

          if (recipientEmails.length === 0) {
            console.log(`[EMAIL] Calendar attendee lookup returned no recipients, falling back to current chat members only`);
            const chatMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
            const uniqueRecipients = new Map<string, string>();
            for (const member of chatMembers) {
              const email = (member.email || '').trim().toLowerCase();
              if (!email || !email.includes('@')) continue;
              if (!uniqueRecipients.has(email)) {
                uniqueRecipients.set(email, member.displayName || email);
              }
            }
            recipientEmails = Array.from(uniqueRecipients.keys());
            recipientNames = Array.from(uniqueRecipients.values());
          }
          console.log(`[EMAIL] Resolved ${recipientEmails.length} participant emails`);
        }
        
        // If still no emails (sending to self or couldn't resolve), try chat members first (cross-tenant),
        // then fall back to Graph /users/ (home-tenant only).
        if (recipientEmails.length === 0 && (emailAnalysis.recipientType === 'self' || !emailAnalysis.recipientType)) {
          // Chat members API works across tenants and returns email for all participants.
          const selfAadId = activity.from.aadObjectId || activity.from.id;
          const chatMembers = await graphApiHelper.getChatMembersDetailed(activity.conversation.id);
          const selfMember = chatMembers.find((m) => m.userId === selfAadId || m.displayName.toLowerCase() === (actorName || '').toLowerCase());
          if (selfMember?.email) {
            recipientEmails.push(selfMember.email);
            recipientNames.push(userName);
            console.log(`[EMAIL] Resolved self email from chat members: ${selfMember.email}`);
          } else {
            // Fall back to /users/{id} (only works for home-tenant users)
            const userInfo = await graphApiHelper.getUserInfo(selfAadId);
            const selfEmail = userInfo?.mail || userInfo?.userPrincipalName || '';
            if (selfEmail) {
              recipientEmails.push(selfEmail);
              recipientNames.push(userName);
            }
          }
        }

        // Last-resort resolution from recent inbox context for phrases like "to his email".
        if (recipientEmails.length === 0 && emailAnalysis.recipientNames.length > 0) {
          for (const name of emailAnalysis.recipientNames) {
            const inboxMatch = resolveRecipientFromInboxContext(activity.conversation.id, name);
            if (inboxMatch?.email) {
              recipientEmails.push(inboxMatch.email);
              recipientNames.push(inboxMatch.displayName);
              console.log(`[EMAIL] Last-resort inbox resolution: ${inboxMatch.displayName} <${inboxMatch.email}>`);
            }
          }
        }
        
        if (recipientEmails.length === 0 && effectiveRecipientType === 'all_participants') {
          await send(new MessageActivity(`I couldn't get attendee email addresses from calendar or current chat participants. I won't use tenant-wide user lookup for "all attendees". Please check the meeting attendees in Outlook/Teams, or tell me exactly who to email.`).addAiGenerated().addFeedback());
        } else if (recipientEmails.length === 0) {
          await send(new MessageActivity(`I couldn't determine the recipient email address. Please specify who to send the email to (e.g., "send the summary to John" or provide an email address like john@example.com).`).addAiGenerated().addFeedback());
        } else if (emailAnalysis.isContextualReference && hasRecentContext && lastResponse) {
          // User is referring to something we just showed them - use conversation context!
          console.log(`[EMAIL] Using contextual reference - sending last ${lastResponse.contentType} response to ${recipientEmails.length} recipient(s)`);

          // Check if user also wants modifications before sending (e.g. "make it shorter and resend")
          const wantsModification = /\b(short|shorter|brief|concise|longer|expand|bullet|detail|reformat|rework|rewrite|condense|simplify)\b/i.test(userText);
          let contentToSend = lastResponse.content;

          if (wantsModification) {
            console.log(`[EMAIL] User wants modification before sending — reformatting cached content`);
            const modPrompt = new ChatPrompt({
              messages: [
                {
                  role: 'user',
                  content: `${userText}\n\nOriginal content:\n${lastResponse.content}`
                }
              ],
              instructions: `Rewrite the content as the user requested. Keep the same facts — do not add or invent information. Use a natural, conversational tone. Output clean Markdown only.`,
              model: new OpenAIChatModel({
                model: config.azureOpenAIDeploymentName,
                apiKey: config.azureOpenAIKey,
                endpoint: config.azureOpenAIEndpoint,
                apiVersion: '2024-10-21',
              }),
            });
            const modResult = await sendPromptWithTracking(modPrompt, '', {
              userId: requesterId,
              displayName: actorName,
              meetingId,
              estimatedInputText: lastResponse.content,
            });
            contentToSend = modResult.content || lastResponse.content;
            // Show the modified version in chat too
            await send(new MessageActivity(contentToSend).addAiGenerated().addFeedback());
          }

          const inboxContext = inboxContextMap.get(activity.conversation.id);
          const hasThreadReplyContext =
            !!inboxContext?.lastMatchedMessageId &&
            !!inboxContext?.mailboxUserId &&
            (lastResponse.subject || '').toLowerCase().startsWith('email reply draft');

          if (hasThreadReplyContext) {
            const replyBody = extractSuggestedReplyBody(lastResponse.content);
            const threadReplyResult = await graphApiHelper.replyToMessageInThread(
              inboxContext.mailboxUserId as string,
              inboxContext.lastMatchedMessageId as string,
              replyBody
            );

            if (threadReplyResult.success) {
              const targetName = inboxContext.lastMatchedSenderName || recipientNames[0] || 'the sender';
              await send(new MessageActivity(`Done! I replied in the same email thread to **${targetName}**.`).addAiGenerated().addFeedback());
              console.log(`[EMAIL] Sent in-thread reply instead of new email`);
              storage.set(conversationKey, messages);
              storage.set(sharedConversationKey, sharedMessages);
              storage.set(llmConversationKey, llmMessages);
              return;
            }

            console.warn(`[EMAIL] In-thread reply failed, falling back to regular email send: ${threadReplyResult.error}`);
          }
          
          const contentTypeLabels: Record<string, string> = {
            'calendar': 'calendar schedule',
            'summary': 'meeting summary',
            'minutes': 'meeting minutes',
            'transcript': 'transcript',
            'meeting_overview': 'meeting overview',
            'insights': 'meeting insights',
            'general': 'information'
          };
          
          const emailSubject = lastResponse.subject || `${contentTypeLabels[lastResponse.contentType] || 'Information'} from ${config.botDisplayName}`;
          const contentTypeName = contentTypeLabels[lastResponse.contentType] || 'information';
          
          // Use independent sending for multiple recipients so one failure doesn't affect others
          const sendResult = await graphApiHelper.sendEmail(
            activity.from.aadObjectId || activity.from.id,
            recipientEmails,
            emailSubject,
            lastResponse.content,
            { replyToEmail: recipientEmails[0], replyToName: actorName, sendIndependently: recipientEmails.length > 1 }
          );
          
          // Use the helper to format the result with partial success handling
          const resultMessage = formatEmailResult(sendResult, recipientNames, contentTypeName);
          await send(new MessageActivity(resultMessage).addAiGenerated().addFeedback());
        } else if (emailAnalysis.contentType === 'summary' || emailAnalysis.contentType === 'minutes' || emailAnalysis.contentType === 'transcript') {
          // Generate the requested content first, then email it
          const wantsSummary = emailAnalysis.contentType === 'summary';
          const wantsMinutes = emailAnalysis.contentType === 'minutes';
          const wantsTranscript = emailAnalysis.contentType === 'transcript';
          console.log(`[EMAIL] Generating content to send: summary=${wantsSummary}, minutes=${wantsMinutes}, transcript=${wantsTranscript}`);
          let transcriptEntries: TranscriptEntry[] = [];
          let resolvedMeetingTitle: string | undefined;
          let resolvedMeetingStart: string | undefined;
          let resolvedMemberList: string[] = [];
          let missingContentHint = '';
          let transcriptResult: Awaited<ReturnType<typeof getTranscriptWithContext>> = {
            text: '',
            source: 'none' as TranscriptSource,
            isLive: false,
            entryCount: 0,
            meetingSubject: undefined,
            callId: undefined,
          };

          const targetExtractPrompt = new ChatPrompt({
            messages: [
              {
                role: 'user',
                content: `Determine which meeting the user wants when asking to email generated meeting content.

User request: "${userText}"
User is currently in a meeting chat: ${isMeetingConversation ? 'YES' : 'NO'}

Rules:
- "last meeting", "latest meeting", "previous meeting", "most recent meeting" => target="last_meeting"
 - Explicit past dates like "yesterday", "last Tuesday", "March 10" => target="past_meeting"
 - In a meeting chat, generic requests like "send the transcript" => target="current"
 - In a meeting chat, requests like "last meeting" usually refer to this meeting thread unless user gave an explicit past date/title.
 - Outside a meeting chat, generic requests without a date usually mean the most recent meeting => target="last_meeting"

Respond with JSON only:
{"target":"current"|"past_meeting"|"last_meeting","meeting_date":"YYYY-MM-DD or null","meeting_subject":"meeting title or null"}`
              }
            ],
            instructions: 'Resolve whether the user wants the current meeting, a dated past meeting, or the most recent meeting. Output valid JSON only.',
            model: new OpenAIChatModel({
              model: config.azureOpenAIDeploymentName,
              apiKey: config.azureOpenAIKey,
              endpoint: config.azureOpenAIEndpoint,
              apiVersion: '2024-10-21',
            })
          });

          let targetInfo: { target: 'current' | 'past_meeting' | 'last_meeting'; meeting_date: string | null; meeting_subject: string | null } = {
            target: isMeetingConversation ? 'current' : 'last_meeting',
            meeting_date: null,
            meeting_subject: null,
          };

          try {
            const targetResponse = await sendPromptWithTracking(targetExtractPrompt, '', {
              userId: requesterId,
              displayName: actorName,
              meetingId,
              estimatedInputText: userText,
            });
            const targetJson = (targetResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
            targetInfo = JSON.parse(targetJson);
          } catch (error) {
            console.warn(`[EMAIL] Could not parse meeting target, defaulting to ${targetInfo.target}`);
          }

          // In a meeting thread, treat ambiguous "last meeting" requests as current meeting context.
          if (
            isMeetingConversation &&
            targetInfo.target === 'last_meeting' &&
            !targetInfo.meeting_date &&
            !targetInfo.meeting_subject
          ) {
            targetInfo.target = 'current';
          }

          console.log(`[EMAIL] Meeting target analysis: ${JSON.stringify(targetInfo)}`);

          const wantsPastMeeting = targetInfo.target === 'past_meeting' && !!targetInfo.meeting_date;
          const wantsLastMeeting = targetInfo.target === 'last_meeting' || (!isMeetingConversation && targetInfo.target === 'current');

          if (wantsPastMeeting || wantsLastMeeting) {
            const meetingLookup = await graphApiHelper.findPastMeeting(
              activity.from.aadObjectId || activity.from.id,
              wantsPastMeeting ? targetInfo.meeting_date || undefined : undefined,
              targetInfo.meeting_subject || undefined
            );

            if (meetingLookup.success && meetingLookup.meeting) {
              resolvedMeetingTitle = meetingLookup.meeting.subject;
              resolvedMeetingStart = meetingLookup.meeting.start;
              resolvedMemberList = (meetingLookup.meeting.attendees || []).map((attendee) => attendee.name || attendee.email).filter(Boolean);

              if (effectiveRecipientType === 'all_participants' && recipientEmails.length === 0) {
                const meetingRecipients = (meetingLookup.meeting.attendees || []).filter((attendee) => attendee.email);
                recipientEmails = meetingRecipients.map((attendee) => attendee.email);
                recipientNames = meetingRecipients.map((attendee) => attendee.name || attendee.email);
                console.log(`[EMAIL] Using ${recipientEmails.length} attendee(s) from the resolved meeting for all_participants delivery`);
              }

              const emailFetch = await fetchTranscriptCacheFirst(
                meetingLookup.meeting.organizerId,
                meetingLookup.meeting.joinWebUrl,
                meetingLookup.meeting.start ? new Date(meetingLookup.meeting.start).getTime() : undefined,
                meetingLookup.meeting.end ? new Date(meetingLookup.meeting.end).getTime() : undefined
              );

              let transcriptParsed = emailFetch.entries;
              if (transcriptParsed.length === 0) {
                console.log(`[EMAIL] Initial transcript lookup missed for resolved meeting; polling briefly...`);
                const pollResult = await pollForTranscriptReady(
                  meetingLookup.meeting.organizerId,
                  meetingLookup.meeting.joinWebUrl,
                  meetingLookup.meeting.start ? new Date(meetingLookup.meeting.start).getTime() : undefined,
                  meetingLookup.meeting.end ? new Date(meetingLookup.meeting.end).getTime() : undefined,
                  3,
                  3000
                );
                if (pollResult.vttContent) {
                  transcriptParsed = parseVttToEntries(pollResult.vttContent);
                }
              }

              if (transcriptParsed.length > 0) {
                transcriptEntries = transcriptParsed;
                console.log(`[EMAIL] Loaded ${transcriptEntries.length} transcript entries for resolved meeting`);
              } else {
                missingContentHint = wantsLastMeeting
                  ? `I found your most recent meeting, but its transcript is not available yet.`
                  : `I found that meeting, but its transcript is not available yet.`;
              }
            } else {
              missingContentHint = wantsLastMeeting
                ? `I couldn't find a recent Teams meeting with transcript data yet.`
                : `I couldn't find the past meeting you asked for.`;
            }
          }

          if (transcriptEntries.length === 0 && !wantsPastMeeting && !wantsLastMeeting) {
            // Use the comprehensive transcript retrieval that checks all sources:
            // 1. Live session, 2. Memory cache, 3. File cache, 4. Background cache, 5. Graph API
            // Only current-meeting requests should wait for Teams to prepare the transcript.
            transcriptResult = await getTranscriptWithContext(activity.conversation.id);

            const MAX_RETRIES = 3;
            const RETRY_DELAY_MS = 5000;

            for (let attempt = 0; attempt < MAX_RETRIES && (!transcriptResult.text || transcriptResult.source === 'none'); attempt++) {
              if (attempt === 0) {
                await send(new MessageActivity(`Waiting for Teams to prepare the transcript... This may take a moment.`).addAiGenerated());
              }
              console.log(`[EMAIL] No current-meeting transcript found, waiting for Teams (attempt ${attempt + 1}/${MAX_RETRIES})...`);
              await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
              transcriptResult = await getTranscriptWithContext(activity.conversation.id);
            }

            if (transcriptResult.text && transcriptResult.source !== 'none') {
              console.log(`[EMAIL] Found transcript from source: ${transcriptResult.source} (${transcriptResult.entryCount} entries)`);
              const memoryEntries = liveTranscriptMap.get(activity.conversation.id);
              transcriptEntries = memoryEntries?.filter(e => e.isFinal) || [];

              if (transcriptEntries.length === 0 && transcriptResult.text) {
                transcriptEntries = parseTranscriptTextToEntries(transcriptResult.text);
                console.log(`[EMAIL] Parsed ${transcriptEntries.length} entries from cached text`);
              }
            }
          }

          let contentToSend = '';
          let emailSubject = `Meeting Content from ${config.botDisplayName}`;
          
          if (transcriptEntries.length > 0) {
            const chatInfo = resolvedMeetingTitle || resolvedMemberList.length > 0 || resolvedMeetingStart
              ? null
              : await resolveMeetingInfoForConversation(activity.conversation.id);
            const chatMembers = resolvedMemberList.length > 0
              ? []
              : await graphApiHelper.getChatMembers(activity.conversation.id);
            const meetingTitle = await resolveDisplayMeetingTitle(
              activity.conversation.id,
              requesterId,
              resolvedMeetingTitle || chatInfo?.subject,
              transcriptResult.meetingSubject
            );
            const meetingStartTime = resolvedMeetingStart || chatInfo?.startDateTime;
            const memberList = resolvedMemberList.length > 0
              ? resolvedMemberList
              : chatMembers.length > 0
                ? chatMembers
                : [userName];
            
            if (wantsSummary) {
              contentToSend = await generateFormattedSummaryHtml(transcriptEntries, meetingTitle, userName, memberList, meetingStartTime, {
                userId: requesterId,
                displayName: actorName,
                meetingId,
              });
                emailSubject = `Meeting Summary: ${meetingTitle} — ${config.botDisplayName}`;
            } else if (wantsMinutes) {
              contentToSend = await generateMinutesHtml(transcriptEntries, meetingTitle, memberList, meetingStartTime, {
                userId: requesterId,
                displayName: actorName,
                meetingId,
              });
                emailSubject = `Meeting Minutes: ${meetingTitle} — ${config.botDisplayName}`;
            } else if (wantsTranscript) {
              // Always format transcript nicely as HTML - never send raw text
              contentToSend = await buildTranscriptHtml(
                transcriptEntries,
                meetingTitle,
                memberList,
                transcriptEntries.length,
                false, // not partial
                {
                  userId: requesterId,
                  displayName: actorName,
                  meetingId,
                }
              );
              emailSubject = `Meeting Transcript: ${meetingTitle} — ${config.botDisplayName}`;
            }
          }
          
          if (contentToSend) {
            // Also show in chat
            await send(new MessageActivity(contentToSend).addAiGenerated().addFeedback());
            
            // Send email to all recipients independently so one failure doesn't affect others
            const contentTypeName = wantsSummary ? 'summary' : wantsMinutes ? 'minutes' : 'transcript';
            const sendResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmails,
              emailSubject,
              contentToSend,
              { replyToEmail: recipientEmails[0], replyToName: actorName, sendIndependently: recipientEmails.length > 1 }
            );
            
            // Use the helper to format the result with partial success handling
            const resultMessage = formatEmailResult(sendResult, recipientNames, contentTypeName);
            await send(new MessageActivity(resultMessage).addAiGenerated().addFeedback());
          } else {
            const contentTypeName = wantsSummary ? 'summary' : wantsMinutes ? 'minutes' : 'transcript';
            const requestedContentLabel = wantsSummary
              ? 'a meeting summary'
              : wantsMinutes
                ? 'meeting minutes'
                : 'a meeting transcript';
            const recipientDisplay = effectiveRecipientType === 'all_participants' && recipientEmails.length === 0
              ? 'the calendar attendees for that meeting'
              : formatRecipientDisplay(recipientEmails, recipientNames);
            await send(new MessageActivity(
              `${missingContentHint ? `${missingContentHint}\n\n` : ''}` +
              `I can send ${requestedContentLabel} as soon as I have transcript content.\n\n` +
              `If this is the same meeting chat, say "transcribe this meeting".\n` +
              `For a past meeting, say something like "transcribe yesterday's standup".\n` +
              `If the meeting is live right now, say "join the call" and I'll capture it in real time.\n\n` +
              `Then I'll prepare the ${contentTypeName} and email it to ${recipientDisplay}.`
            ).addAiGenerated().addFeedback());
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
3. For "test" or "sample" emails, generate a simple professional test message
4. If unclear, ask for clarification

CRITICAL RULES:
- NEVER use placeholders like [Your Name], [Recipient Name], [Date], etc.
- Use "${userName}" as the sender's actual name
- Generate complete, ready-to-send content with no blanks
- Sign emails with the sender's actual name: "${userName}"

Respond with JSON: {"subject": "clear email subject", "body": "complete email body content with NO placeholders", "is_contextual": true/false, "needs_clarification": true/false}`
              }
            ],
            instructions: `You are a smart email assistant. Generate complete, ready-to-send email content. NEVER use placeholder text like [Your Name]. Use "${userName}" as the sender name. Output valid JSON only.`,
            model: new OpenAIChatModel({
              model: config.azureOpenAIDeploymentName,
              apiKey: config.azureOpenAIKey,
              endpoint: config.azureOpenAIEndpoint,
              apiVersion: '2024-10-21'
            })
          });

          const extractResponse = await sendPromptWithTracking(extractPrompt, '', {
            userId: requesterId,
            displayName: actorName,
            meetingId,
            estimatedInputText: `${cleanText || activity.text || ''}\n${conversationContext}`,
          });
          const jsonStr = (extractResponse.content || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
          const extracted = JSON.parse(jsonStr);
          
          if (extracted.needs_clarification) {
            await send(new MessageActivity(extracted.body || "I'm not sure what you'd like me to email. Could you please clarify what content you want me to send?").addAiGenerated().addFeedback());
          } else {
            // Send the email to all recipients independently so one failure doesn't affect others
            const sendResult = await graphApiHelper.sendEmail(
              activity.from.aadObjectId || activity.from.id,
              recipientEmails,
              extracted.subject || `Message from ${config.botDisplayName}`,
              extracted.body || cleanText || activity.text || '',
              { replyToEmail: recipientEmails[0], replyToName: actorName, sendIndependently: recipientEmails.length > 1 }
            );

            // Use helper for consistent result formatting with partial success support
            if (sendResult.partialSuccess || !sendResult.success) {
              // Use the detailed helper for failures or partial success
              const resultMessage = formatEmailResult(sendResult, recipientNames, 'email');
              await send(new MessageActivity(resultMessage).addAiGenerated().addFeedback());
            } else {
              // Full success - show explicit confirmation with details
              const recipientListDisplay = formatRecipientDisplay(sendResult.sentTo || recipientEmails, recipientNames);
              const emailSubject = extracted.subject || `Message from ${config.botDisplayName}`;
              const senderInfo = config.emailSenderUserId ? ` (sent from ${config.emailSenderUserId})` : '';
              console.log(`[EMAIL] Successfully sent email. Subject: "${emailSubject}", To: ${recipientListDisplay}${senderInfo}`);
              await send(new MessageActivity(
                `✅ **Email Sent Successfully**\n\n` +
                `**To:** ${recipientListDisplay}\n` +
                `**Subject:** ${emailSubject}\n` +
                `**From:** ${config.emailSenderUserId || 'Your account'}${senderInfo ? '' : ''}`
              ).addAiGenerated().addFeedback());
            }
          }
        }
        console.log(`[SUCCESS] Email intent processed`);
      } catch (error: any) {
        console.error(`[ERROR_SEND_EMAIL]`, error);
        await send(new MessageActivity(`Sorry, I had trouble with that email request. Could you try again with the recipient and what you'd like to send?`).addAiGenerated().addFeedback());
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
        // effectiveQuery already contains follow-up context if applicable
        const extractPrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Analyze this calendar request and extract date/time parameters.

User request: "${effectiveQuery || activity.text}"

${dateContext}

IMPORTANT RULES:
1. You can ONLY check the CURRENT USER's calendar - not anyone else's
2. If the user asks about another person's calendar (e.g., "Armely's meetings", "what does John have"), set "other_person_requested": true
3. Extract dates ONLY for the current user's meetings

Interpret relative dates correctly:
- "today" / "today's meetings" = ${today.toISOString().split('T')[0]}
- "yesterday" / "meeting we had yesterday" = ${yesterday.toISOString().split('T')[0]}
- "tomorrow" = ${tomorrow.toISOString().split('T')[0]}
- "this morning" / "this afternoon" = ${today.toISOString().split('T')[0]}
- "this week" = ${today.toISOString().split('T')[0]} to ${thisWeekEnd.toISOString().split('T')[0]}
- "next week" = ${nextWeekStart.toISOString().split('T')[0]} to 7 days later

Respond with JSON only: {"query_type": "view_events|check_availability|find_free_time|past_events|schedule_meeting", "start_date": "ISO date string", "end_date": "ISO date string", "description": "brief description of what user wants", "is_past": true|false, "other_person_requested": true|false, "other_person_name": "name if mentioned"}`
            }
          ],
          instructions: 'You are a JSON extraction assistant. Determine what calendar info the user needs. Convert relative dates to actual ISO date strings. IMPORTANT: If the user asks about ANYONE ELSE\'s calendar, set other_person_requested=true. Output valid JSON only.',
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const extractResponse = await sendPromptWithTracking(extractPrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: effectiveQuery || activity.text || '',
        });
        const rawExtract = (extractResponse.content || '').trim();
        // Strip markdown code blocks if present (LLM sometimes wraps JSON in ```json...```)
        const extractJson = rawExtract.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
        const extracted = JSON.parse(extractJson);
        console.log(`[CALENDAR] Extracted: ${JSON.stringify(extracted)}`);

        if (extracted.query_type === 'schedule_meeting') {
          await send(new MessageActivity(
            `I can check your calendar right now, but I can't create/schedule a new meeting yet. ` +
            `If you want, I can help by checking your free slots first.`
          ).addAiGenerated().addFeedback());
          storage.set(conversationKey, messages);
          storage.set(sharedConversationKey, sharedMessages);
          storage.set(llmConversationKey, llmMessages);
          return;
        }
        
        // Check if user asked about another person's calendar
        const otherPersonRequested = extracted.other_person_requested === true;
        const otherPersonName = extracted.other_person_name || '';
        
        const userId = activity.from.aadObjectId || activity.from.id;
        const calendarResult = await graphApiHelper.getCalendarEvents(
          userId,
          extracted.start_date || undefined,
          extracted.end_date || undefined
        );

        // Pre-format event times in code so the LLM never does date/timezone math
        const userTimezone = calendarResult.timezone || 'UTC';
        const weekdays = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
        const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

        const formatTimeFromDatetime = (dtStr?: string): string => {
          if (!dtStr) return '';
          // dtStr is e.g. "2026-03-16T09:00:00.0000000" — already in user's timezone from Prefer header
          const timePart = dtStr.match(/T(\d{2}):(\d{2})/);
          if (!timePart) return '';
          let h = parseInt(timePart[1], 10);
          const m = timePart[2];
          const ampm = h >= 12 ? 'PM' : 'AM';
          if (h === 0) h = 12;
          else if (h > 12) h -= 12;
          return m === '00' ? `${h} ${ampm}` : `${h}:${m} ${ampm}`;
        };

        const formatDateFromDatetime = (dtStr?: string): string => {
          if (!dtStr) return '';
          const datePart = dtStr.match(/^(\d{4})-(\d{2})-(\d{2})/);
          if (!datePart) return '';
          const y = parseInt(datePart[1], 10);
          const mo = parseInt(datePart[2], 10) - 1;
          const d = parseInt(datePart[3], 10);
          // Use Date constructor with explicit components to get day-of-week (no timezone shift since we only need the weekday)
          const dt = new Date(y, mo, d);
          return `${weekdays[dt.getDay()]}, ${monthNames[mo]} ${d}`;
        };

        const formattedEvents = (calendarResult.events || []).slice(0, 15).map((evt: any) => {
          const startDt = evt.start?.dateTime || '';
          const endDt = evt.end?.dateTime || '';
          return {
            subject: evt.subject || 'Untitled',
            date: formatDateFromDatetime(startDt),
            startTime: formatTimeFromDatetime(startDt),
            endTime: formatTimeFromDatetime(endDt),
            isAllDay: evt.isAllDay || false,
            isCancelled: evt.isCancelled || false,
            location: evt.location?.displayName || '',
            organizer: evt.organizer?.emailAddress?.name || '',
          };
        });

        const eventsText = calendarResult.success
          ? formattedEvents.map((e) =>
              e.isAllDay
                ? `• ${e.date}: "${e.subject}" (all day)${e.isCancelled ? ' [CANCELLED]' : ''}`
                : `• ${e.date}: "${e.subject}" ${e.startTime}–${e.endTime}${e.isCancelled ? ' [CANCELLED]' : ''}${e.location ? ` | ${e.location}` : ''}`
            ).join('\n')
          : 'No events retrieved';
        
        const responsePrompt = new ChatPrompt({
          messages: [
            {
              role: 'user',
              content: `Help ${userName || 'the user'} understand their calendar.

Their question: "${effectiveQuery || activity.text}"
Query date range: ${extracted.start_date || 'today'} to ${extracted.end_date || 'today'}
API result: ${calendarResult.success ? 'SUCCESS' : 'FAILED'}
${calendarResult.error ? `Error: ${calendarResult.error}` : ''}
${otherPersonRequested ? `NOTE: User also asked about ${otherPersonName || 'another person'}'s calendar, but I can only access YOUR calendar.` : ''}

Calendar events (${userName}'s calendar, times in ${userTimezone}):
${eventsText}

Use the times EXACTLY as shown above — they are already formatted in the user's local timezone. Do NOT modify, convert, or recalculate any times.

Respond naturally to their question about their schedule.${otherPersonRequested ? ` Politely mention that you can only check their own calendar, not ${otherPersonName || 'other people'}'s.` : ''}`
            }
          ],
          instructions: `You are a friendly, conversational calendar assistant. Respond naturally like a helpful colleague would.

CRITICAL: The times shown in the event list are ALREADY in the user's local timezone and pre-formatted. Use them EXACTLY as given. Do NOT recalculate, convert, or adjust any times. Just relay them.

RESPONSE STYLE:
- Be conversational and warm, not robotic or templated
- Vary your language - don't use the same format every time
- Use **bold** for meeting titles to help them stand out
- Keep it brief and easy to scan
- Sound like a helpful human, not a form or report
- Mention cancelled events briefly so the user knows they're cleared

Examples of good responses:
- "You've got a busy afternoon! **Project Sync** at 2 PM, then **1:1 with Sarah** at 3:30 PM."
- "Looks like your Monday is clear - no meetings scheduled."
- "On March 5th you had three meetings: **Standup** (9 AM), **Design Review** (11 AM), and **Team Lunch** (12:30 PM)."`,
          model: new OpenAIChatModel({
            model: config.azureOpenAIDeploymentName,
            apiKey: config.azureOpenAIKey,
            endpoint: config.azureOpenAIEndpoint,
            apiVersion: '2024-10-21'
          })
        });

        const responseResult = await sendPromptWithTracking(responsePrompt, '', {
          userId: requesterId,
          displayName: actorName,
          meetingId,
          estimatedInputText: `${effectiveQuery || activity.text || ''}\ncalendar events`,
        });
        const calendarResponseContent = responseResult.content || 'Calendar check completed.';
        await send(new MessageActivity(calendarResponseContent).addAiGenerated().addFeedback());
        
        // Track this response for contextual follow-ups like "send it to my email"
        recordBotResponse(activity.conversation.id, {
          content: calendarResponseContent,
          contentType: 'calendar',
          subject: `Calendar for ${extracted.start_date || 'today'}`,
          timestamp: Date.now()
        });
        
        console.log(`[SUCCESS] Calendar intent processed`);
      } catch (error: any) {
        console.error(`[ERROR_CHECK_CALENDAR]`, error);
        // Generate a helpful error response via LLM
        try {
          const errorPrompt = new ChatPrompt({
            messages: [
              {
                role: 'user',
                content: `The user asked: "${effectiveQuery || activity.text}"
I tried to check their calendar but got an error: ${error?.message || 'Unknown error'}

Generate a brief, friendly apology and suggest they try again. Be conversational, not robotic.`
              }
            ],
            instructions: 'You are a helpful assistant. Generate a brief, friendly message acknowledging the issue. Keep it under 2 sentences.',
            model: new OpenAIChatModel({
              model: config.azureOpenAIDeploymentName,
              apiKey: config.azureOpenAIKey,
              endpoint: config.azureOpenAIEndpoint,
              apiVersion: '2024-10-21'
            })
          });
          const errorResponse = await sendPromptWithTracking(errorPrompt, '', {
            userId: requesterId,
            displayName: actorName,
            meetingId,
            estimatedInputText: `${effectiveQuery || activity.text || ''}\ncalendar error`,
          });
          await send(new MessageActivity(errorResponse.content || 'Sorry, I ran into an issue checking your calendar. Could you try again?').addAiGenerated().addFeedback());
        } catch {
          await send(new MessageActivity(`Sorry, I ran into an issue checking your calendar. Could you try again?`).addAiGenerated().addFeedback());
        }
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

    // Add inbox email context if recently shown - prevents hallucination on follow-up questions
    const _inboxCtxForChat = inboxContextMap.get(activity.conversation.id);
    if (_inboxCtxForChat?.lastMessages?.length && _inboxCtxForChat.updatedAt > Date.now() - 10 * 60 * 1000) {
      const inboxEmailContext = _inboxCtxForChat.lastMessages.map(m => {
        const body = m.bodyContent || m.bodyPreview || '';
        return `**From:** ${m.fromName || m.fromAddress}\n**Subject:** ${m.subject}\n**Date:** ${m.receivedDateTime}\n**Content:** ${body.slice(0, 2000)}`;
      }).join('\n\n---\n\n');
      enhancedInstructions += `\n\n## Recently Shown Inbox Email(s)\nThe user recently asked about their inbox. Here is the email content that was shown - use this to answer follow-up questions:\n\n${inboxEmailContext}\n\n⚠️ IMPORTANT: If the user asks "what did [person] say" or any follow-up about email content, answer ONLY from this context. Do NOT make up information.`;
      console.log(`[CHAT] Added ${_inboxCtxForChat.lastMessages.length} inbox email(s) to context for follow-up Q&A`);
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
      const responseText = extractModelResponseText(response);
      const draftEmailRequest = detectEmailRequest(effectiveQuery || activity.text || '');
      let draftRecipientEmails: string[] = [];
      let draftRecipientNames: string[] = [];
      let draftRecipientType: LastBotResponse['recipientType'] = null;
      if (draftEmailRequest.sendToAllAttendees) {
        // Get meeting joinWebUrl for accurate calendar matching (includes external attendees)
        let meetingJoinUrl: string | undefined;
        try {
          const meetingInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          meetingJoinUrl = meetingInfo?.joinWebUrl || getCachedMeetingContext(activity.conversation.id)?.joinWebUrl;
        } catch (err) {
          console.log(`[DRAFT_EMAIL] Could not get meeting joinWebUrl: ${err instanceof Error ? err.message : err}`);
        }
        const calendarRecipients = await resolveCalendarAttendeesForRequest(
          activity.from.aadObjectId || activity.from.id,
          effectiveQuery || activity.text || '',
          activity.conversation.id,
          meetingJoinUrl
        );
        draftRecipientEmails = calendarRecipients.emails;
        draftRecipientNames = calendarRecipients.names;
        draftRecipientType = 'all_participants';
      }
      recordEstimatedModelUsage({
        userId: requesterId,
        displayName: actorName,
        meetingId,
        inputText: cleanText || activity.text || '',
        outputText: responseText,
      });
      console.log(`[CHAT] Received response from model`);
      console.log(`[MODEL_RESPONSE] ${getTruncatedLogPreview(responseText)}`);
      llmMessages.push({ role: 'assistant', content: responseText });
      if (llmMessages.length > 30) {
        llmMessages = llmMessages.slice(-30);
      }
      recordBotResponse(activity.conversation.id, {
        content: responseText,
        contentType: 'general',
        subject: `Response from ${config.botDisplayName}`,
        timestamp: Date.now(),
        recipientType: draftRecipientType,
        recipientEmails: draftRecipientEmails,
        recipientNames: draftRecipientNames,
        sourceRequest: effectiveQuery || activity.text || '',
      });
      const responseActivity = new MessageActivity(responseText).addAiGenerated().addFeedback();
      await send(responseActivity);
      console.log(`[SUCCESS] Chat response sent to group`);
    } else {
        console.log(`[CHAT] Personal/direct mode - streaming response`);
        let streamedResponse = '';
        let streamFailed = false;
        const streamedResult = await prompt.send(cleanText || activity.text || '', {
          onChunk: (chunk) => {
            if (streamFailed) return; // stop streaming if a previous chunk failed
            console.log(`[STREAM] Chunk received`);
            streamedResponse += chunk || '';
            try {
              stream.emit(chunk);
            } catch (streamErr: any) {
              // 403 or other connector errors — collect remaining text but stop streaming
              console.warn(`[STREAM] Streaming failed (${streamErr?.response?.status || streamErr?.code || 'unknown'}), will send final response as a single message`);
              streamFailed = true;
            }
          },
        });
      if (!streamedResponse.trim()) {
        streamedResponse = extractModelResponseText(streamedResult);
      }

      // If streaming failed mid-way, send the full response as a normal message
      if (streamFailed && streamedResponse.trim()) {
        console.log(`[STREAM] Sending full response as fallback after stream failure`);
        try {
          await send(new MessageActivity(streamedResponse).addAiGenerated().addFeedback());
        } catch (fallbackErr) {
          console.error(`[STREAM] Fallback send also failed — conversation may be stale`, fallbackErr);
        }
      }
      recordEstimatedModelUsage({
        userId: requesterId,
        displayName: actorName,
        meetingId,
        inputText: cleanText || activity.text || '',
        outputText: streamedResponse,
      });
      const draftEmailRequest = detectEmailRequest(effectiveQuery || activity.text || '');
      let draftRecipientEmails: string[] = [];
      let draftRecipientNames: string[] = [];
      let draftRecipientType: LastBotResponse['recipientType'] = null;
      if (draftEmailRequest.sendToAllAttendees) {
        // Get meeting joinWebUrl for accurate calendar matching (includes external attendees)
        let meetingJoinUrl: string | undefined;
        try {
          const meetingInfo = await resolveMeetingInfoForConversation(activity.conversation.id);
          meetingJoinUrl = meetingInfo?.joinWebUrl || getCachedMeetingContext(activity.conversation.id)?.joinWebUrl;
        } catch (err) {
          console.log(`[DRAFT_EMAIL] Could not get meeting joinWebUrl: ${err instanceof Error ? err.message : err}`);
        }
        const calendarRecipients = await resolveCalendarAttendeesForRequest(
          activity.from.aadObjectId || activity.from.id,
          effectiveQuery || activity.text || '',
          activity.conversation.id,
          meetingJoinUrl
        );
        draftRecipientEmails = calendarRecipients.emails;
        draftRecipientNames = calendarRecipients.names;
        draftRecipientType = 'all_participants';
      }
      recordBotResponse(activity.conversation.id, {
        content: streamedResponse,
        contentType: 'general',
        subject: `Response from ${config.botDisplayName}`,
        timestamp: Date.now(),
        recipientType: draftRecipientType,
        recipientEmails: draftRecipientEmails,
        recipientNames: draftRecipientNames,
        sourceRequest: effectiveQuery || activity.text || '',
      });
      console.log(`[MODEL_RESPONSE] ${getTruncatedLogPreview(streamedResponse)}`);
      // We wrap the final response with an AI Generated indicator
      if (!streamFailed) {
        try {
          stream.emit(new MessageActivity().addAiGenerated().addFeedback());
        } catch (finalStreamErr) {
          console.warn(`[STREAM] Final stream emit failed, response was already delivered`);
        }
      }
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
app.on('conversationUpdate', async ({ send: sendActivity, activity }) => {
  const send = async (outgoing: any) => {
    try {
      await sendActivity(outgoing);
      const text = typeof outgoing === 'string'
        ? outgoing
        : (outgoing?.text || outgoing?.summary || '[non-text activity]');
      console.log(`[TEAMS_SEND_OK] conversation=${activity?.conversation?.id || 'unknown'} preview="${getTruncatedLogPreview(String(text || ''))}"`);
    } catch (error: any) {
      const status = error?.response?.status || error?.response?.statusCode;
      console.error(`[TEAMS_SEND_FAIL] conversation=${activity?.conversation?.id || 'unknown'} status=${status || 'unknown'}`);
      if (status === 403 || status === 429) {
        console.warn(`[TEAMS_SEND_FAIL] Non-fatal ${status} — conversation may be stale or throttled`);
        return;
      }
      throw error;
    }
  };
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

      // Teams can emit duplicate conversationUpdate events; prevent double greeting spam.
      const greetingSentKey = `greeting-sent/${activity.conversation.id}`;
      const lastGreetingAt = Number(storage.get(greetingSentKey) || 0);
      if (lastGreetingAt && Date.now() - lastGreetingAt < 10 * 60 * 1000) {
        console.log(`[BOT_ADDED] Greeting already sent recently for ${activity.conversation.id} - skipping duplicate`);
        return;
      }
      storage.set(greetingSentKey, Date.now());
      
      try {
        // Detect if this is a meeting chat
        const isMeetingChat = activity.conversation.isGroup && 
                              (activity.conversation.id.includes('meeting') || 
                               activity.conversation.id.includes('call') ||
                               activity.channelId === 'msteams');
        
        console.log(`[MEETING_DETECTION] Detected as meeting: ${isMeetingChat}`);

        const greeting = isMeetingChat
          ? `Hello, **${config.botDisplayName}** here! I specialize in meeting transcription.
            
**What I can do:**
• **Join Call** — I'll join and capture the conversation live
• **Transcribe** — Fetch the meeting transcript from Teams
• **Summarize** — Generate an AI summary from the transcript
• **Minutes** — Create formal meeting minutes

Ask me to "join the call" to start capturing, or "transcribe" after the meeting!`
          : `Hello, **${config.botDisplayName}** is ready to help with meeting transcription!

**My focus is on meeting transcripts:**
• **Join Meetings** — I'll capture conversations in real-time
• **Transcribe** — Fetch transcripts from past meetings
• **Summarize** — AI-powered summaries from transcripts
• **Minutes** — Formal documentation from transcripts
• **Email** — Send transcripts or summaries via email

Start a meeting and invite me, or ask about a past meeting!`;

        const greetingActivity = new MessageActivity(greeting).addAiGenerated();
        await send(greetingActivity);
        
        console.log(`[SUCCESS] Sent greeting to ${isMeetingChat ? 'meeting' : 'conversation'}`);

        // NOTE: We no longer auto-join calls automatically on bot add.
        // Auto-join caused confusion when users wanted to ask about PAST meetings.
        // Users should explicitly say "join the call" to start live transcription.
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

interface AdminSession {
  accessToken: string;
  expiresAt: number;
  tenantId: string;
  displayName?: string;
  email?: string;
  userId?: string;
}

interface AdminUserProfile {
  displayName: string;
  email: string;
  userId: string;
  tenantId: string;
  jobTitle?: string;
  department?: string;
}

const ADMIN_SESSION_COOKIE = 'mela_admin_session';
const ADMIN_LOGIN_PATH = '/admin/login';
const ADMIN_AUTH_CALLBACK_PATH = '/admin/auth/callback';
const ADMIN_LOGOUT_PATH = '/admin/logout';
const ADMIN_AUTH_DISABLED = (process.env.ADMIN_AUTH_DISABLED || 'true').toLowerCase() === 'true';
const ADMIN_ERROR_LOG_FILE = path.join(ADMIN_DATA_DIR, 'admin_error_logs.json');

interface AdminErrorLogEntry {
  id: string;
  ts: string;
  source: 'server' | 'client';
  route: string;
  message: string;
  stack?: string;
  userId?: string;
  tenantId?: string;
  meta?: any;
}

function normalizeError(err: any): { message: string; stack?: string } {
  if (!err) return { message: 'unknown_error' };
  if (err instanceof Error) {
    return { message: err.message || 'error', stack: err.stack || '' };
  }
  if (typeof err === 'string') {
    return { message: err };
  }
  try {
    return { message: JSON.stringify(err) };
  } catch {
    return { message: String(err) };
  }
}

function loadAdminErrorLogs(): AdminErrorLogEntry[] {
  try {
    ensureAdminDataDir();
    if (!fs.existsSync(ADMIN_ERROR_LOG_FILE)) {
      return [];
    }
    const raw = fs.readFileSync(ADMIN_ERROR_LOG_FILE, 'utf-8').trim();
    if (!raw) return [];
    const parsed = JSON.parse(raw) as AdminErrorLogEntry[];
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function saveAdminErrorLogs(entries: AdminErrorLogEntry[]) {
  ensureAdminDataDir();
  fs.writeFileSync(ADMIN_ERROR_LOG_FILE, JSON.stringify(entries, null, 2), 'utf-8');
}

function logAdminError(source: 'server' | 'client', route: string, error: any, options?: { userId?: string; tenantId?: string; meta?: any }) {
  const normalized = normalizeError(error);
  const entries = loadAdminErrorLogs();
  const next: AdminErrorLogEntry = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
    ts: new Date().toISOString(),
    source,
    route,
    message: normalized.message,
    stack: normalized.stack,
    userId: options?.userId,
    tenantId: options?.tenantId,
    meta: options?.meta,
  };
  entries.push(next);
  if (entries.length > 500) {
    entries.splice(0, entries.length - 500);
  }
  saveAdminErrorLogs(entries);
}

function getConfiguredTenantId(): string {
  const raw = (process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID || '').trim();
  if (!raw) return '';

  // Accept plain tenant IDs/domains, and defensively normalize accidental full URLs.
  try {
    if (raw.startsWith('http://') || raw.startsWith('https://')) {
      const parsed = new URL(raw);
      const match = parsed.pathname.match(/\/([^/]+)\/oauth2\//i);
      if (match?.[1]) {
        return match[1];
      }
    }
  } catch {
    // Fall through to raw cleanup below
  }

  return raw
    .replace(/^https?:\/\/login\.microsoftonline\.com\//i, '')
    .replace(/\/oauth2\/v2\.0\/(authorize|token).*$/i, '')
    .replace(/\/$/, '');
}

function getAdminBaseUrl(req?: any): string {
  const forwardedHost = req?.headers?.['x-forwarded-host'];
  const host = forwardedHost || req?.headers?.host;
  if (host) {
    const forwardedProto = req?.headers?.['x-forwarded-proto'];
    const protocol = forwardedProto || (String(host).includes('localhost') ? 'http' : 'https');
    return `${protocol}://${host}`.replace(/\/$/, '');
  }

  const configured = (process.env.BOT_ENDPOINT || '').trim();
  if (configured) {
    return configured.replace(/\/$/, '');
  }

  return 'http://localhost:3978';
}

function parseCookies(cookieHeader?: string): Record<string, string> {
  const map: Record<string, string> = {};
  if (!cookieHeader) return map;
  for (const part of cookieHeader.split(';')) {
    const [rawName, ...rest] = part.trim().split('=');
    if (!rawName) continue;
    map[rawName] = decodeURIComponent(rest.join('=') || '');
  }
  return map;
}

function serializeCookie(name: string, value: string, maxAgeSeconds: number, secure: boolean): string {
  const secureFlag = secure ? '; Secure' : '';
  return `${name}=${encodeURIComponent(value)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSeconds}${secureFlag}`;
}

function clearCookie(name: string): string {
  return `${name}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`;
}

function decodeJwtPayload(token?: string): Record<string, any> {
  if (!token) return {};
  try {
    const parts = token.split('.');
    if (parts.length < 2) return {};
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padded = payload + '='.repeat((4 - (payload.length % 4)) % 4);
    return JSON.parse(Buffer.from(padded, 'base64').toString('utf-8'));
  } catch {
    return {};
  }
}

function getAdminSession(req: any): AdminSession | null {
  try {
    const cookies = parseCookies(req?.headers?.cookie || '');
    const encoded = cookies[ADMIN_SESSION_COOKIE];
    if (!encoded) return null;
    const json = Buffer.from(encoded, 'base64url').toString('utf-8');
    const parsed = JSON.parse(json) as AdminSession;
    if (!parsed?.accessToken || !parsed?.expiresAt || !parsed?.tenantId) return null;
    return parsed;
  } catch {
    return null;
  }
}

function shouldUseSecureCookie(req: any): boolean {
  const forwardedProto = (req?.headers?.['x-forwarded-proto'] || '').toString().toLowerCase();
  if (forwardedProto === 'https') return true;

  const host = (req?.headers?.host || '').toString().toLowerCase();
  if (!host) return false;
  if (host.includes('localhost') || host.startsWith('127.0.0.1')) {
    return false;
  }
  return true;
}

function setAdminSessionCookie(req: any, res: any, session: AdminSession) {
  const raw = Buffer.from(JSON.stringify(session), 'utf-8').toString('base64url');
  const maxAge = Math.max(60, Math.floor((session.expiresAt - Date.now()) / 1000));
  res.setHeader('Set-Cookie', serializeCookie(ADMIN_SESSION_COOKIE, raw, maxAge, shouldUseSecureCookie(req)));
}

function redirectToAdminLogin(req: any, res: any) {
  const returnTo = encodeURIComponent(req?.originalUrl || '/admin/overview');
  res.redirect(`${ADMIN_LOGIN_PATH}?returnTo=${returnTo}`);
}

async function requireAdminAuth(req: any, res: any, isApi = false): Promise<AdminSession | null> {
  if (ADMIN_AUTH_DISABLED) {
    return {
      accessToken: '',
      expiresAt: Date.now() + 24 * 60 * 60 * 1000,
      tenantId: getConfiguredTenantId() || 'auth-disabled',
      displayName: 'Admin (Auth Disabled)',
      email: 'auth-disabled@local',
      userId: 'auth-disabled',
    };
  }

  const tenantId = getConfiguredTenantId();
  if (!tenantId) {
    if (isApi) {
      res.status(500).json({ error: 'tenant_not_configured' });
    } else {
      res.status(500).send('Admin auth is not configured. Missing TENANT_ID.');
    }
    return null;
  }

  const session = getAdminSession(req);
  if (!session || session.expiresAt <= Date.now() || session.tenantId !== tenantId) {
    res.setHeader('Set-Cookie', clearCookie(ADMIN_SESSION_COOKIE));
    if (isApi) {
      res.status(401).json({ error: 'unauthorized' });
    } else {
      redirectToAdminLogin(req, res);
    }
    return null;
  }
  return session;
}

async function getCurrentAdminUserProfile(session: AdminSession): Promise<AdminUserProfile> {
  try {
    const { default: axios } = await import('axios');
    const response = await axios.get('https://graph.microsoft.com/v1.0/me?$select=id,displayName,mail,userPrincipalName,jobTitle,department', {
      headers: {
        Authorization: `Bearer ${session.accessToken}`,
      },
    });
    const me = response.data || {};
    return {
      displayName: me.displayName || session.displayName || 'Tenant User',
      email: me.mail || me.userPrincipalName || session.email || 'unknown',
      userId: me.id || session.userId || 'unknown',
      tenantId: session.tenantId,
      jobTitle: me.jobTitle || '',
      department: me.department || '',
    };
  } catch {
    return {
      displayName: session.displayName || 'Tenant User',
      email: session.email || 'unknown',
      userId: session.userId || 'unknown',
      tenantId: session.tenantId,
      jobTitle: '',
      department: '',
    };
  }
}

app.http.get(ADMIN_LOGIN_PATH, async (req: any, res: any) => {
  if (ADMIN_AUTH_DISABLED) {
    res.redirect('/admin/overview');
    return;
  }

  const tenantId = getConfiguredTenantId();
  if (!tenantId || !process.env.CLIENT_ID || !process.env.CLIENT_SECRET) {
    res.status(500).send('Admin auth is not configured. Missing CLIENT_ID/CLIENT_SECRET/TENANT_ID.');
    return;
  }
  const returnTo = (req?.query?.returnTo as string) || '/admin/overview';
  const redirectUri = `${getAdminBaseUrl(req)}${ADMIN_AUTH_CALLBACK_PATH}`;
  console.log(`[ADMIN_AUTH] Login start host=${req?.headers?.host || 'unknown'} redirectUri=${redirectUri}`);
  const authorizeUrl = new URL(`https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/authorize`);
  authorizeUrl.searchParams.set('client_id', process.env.CLIENT_ID);
  authorizeUrl.searchParams.set('response_type', 'code');
  authorizeUrl.searchParams.set('redirect_uri', redirectUri);
  authorizeUrl.searchParams.set('response_mode', 'query');
  authorizeUrl.searchParams.set('scope', 'openid profile email User.Read');
  authorizeUrl.searchParams.set('state', Buffer.from(JSON.stringify({ returnTo }), 'utf-8').toString('base64url'));
  res.redirect(authorizeUrl.toString());
});

app.http.get(ADMIN_AUTH_CALLBACK_PATH, async (req: any, res: any) => {
  if (ADMIN_AUTH_DISABLED) {
    res.redirect('/admin/overview');
    return;
  }

  try {
    const tenantId = getConfiguredTenantId();
    const code = (req?.query?.code as string) || '';
    const stateRaw = (req?.query?.state as string) || '';
    const state = stateRaw ? JSON.parse(Buffer.from(stateRaw, 'base64url').toString('utf-8')) : {};
    const returnTo = typeof state?.returnTo === 'string' ? state.returnTo : '/admin/overview';

    if (!tenantId || !code || !process.env.CLIENT_ID || !process.env.CLIENT_SECRET) {
      res.status(400).send('Invalid admin login callback.');
      return;
    }

    const redirectUri = `${getAdminBaseUrl(req)}${ADMIN_AUTH_CALLBACK_PATH}`;
    console.log(`[ADMIN_AUTH] Callback host=${req?.headers?.host || 'unknown'} redirectUri=${redirectUri}`);
    const form = new URLSearchParams();
    form.append('client_id', process.env.CLIENT_ID);
    form.append('client_secret', process.env.CLIENT_SECRET);
    form.append('grant_type', 'authorization_code');
    form.append('code', code);
    form.append('redirect_uri', redirectUri);
    form.append('scope', 'openid profile email User.Read');

    const { default: axios } = await import('axios');
    const tokenResponse = await axios.post(
      `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
      form.toString(),
      { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
    );

    const tokenData = tokenResponse.data || {};
    const idTokenPayload = decodeJwtPayload(tokenData.id_token);
    if (idTokenPayload?.tid !== tenantId) {
      res.status(403).send('Access denied: user is not from allowed tenant.');
      return;
    }

    const session: AdminSession = {
      accessToken: tokenData.access_token,
      expiresAt: Date.now() + Math.max(300, Number(tokenData.expires_in || 3600)) * 1000,
      tenantId: idTokenPayload?.tid || tenantId,
      displayName: idTokenPayload?.name || '',
      email: idTokenPayload?.preferred_username || idTokenPayload?.email || '',
      userId: idTokenPayload?.oid || '',
    };

    setAdminSessionCookie(req, res, session);
    res.redirect(returnTo.startsWith('/admin') ? returnTo : '/admin/overview');
  } catch (error) {
    const axiosError: any = error as any;
    const aadError = axiosError?.response?.data?.error as string | undefined;
    const aadDescription = (axiosError?.response?.data?.error_description as string | undefined) || '';

    // If callback is hit twice (refresh/back button/replay), Entra returns invalid_grant code-redeemed.
    // Treat this as recoverable and continue if session already exists.
    if (aadError === 'invalid_grant' && /already redeemed/i.test(aadDescription)) {
      const existingSession = getAdminSession(req);
      if (existingSession && existingSession.expiresAt > Date.now()) {
        res.redirect('/admin/overview');
        return;
      }
      res.redirect(`${ADMIN_LOGIN_PATH}?error=code_redeemed`);
      return;
    }

    logAdminError('server', ADMIN_AUTH_CALLBACK_PATH, error, { meta: { phase: 'oauth_callback' } });
    console.error('[ADMIN_AUTH_CALLBACK] Failed:', error);
    res.status(500).send('Admin login failed.');
  }
});

app.http.get(ADMIN_LOGOUT_PATH, async (_req: any, res: any) => {
  if (ADMIN_AUTH_DISABLED) {
    res.redirect('/admin/overview');
    return;
  }

  res.setHeader('Set-Cookie', clearCookie(ADMIN_SESSION_COOKIE));
  res.redirect(ADMIN_LOGIN_PATH);
});

app.http.get('/api/admin/stats', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const stats = loadBotAdminStats();
    const monthKey = getMonthKey();
    const userId = req?.query?.userId as string | undefined;
    const meetingId = req?.query?.meetingId as string | undefined;

    if (userId) {
      const user = stats.users[userId];
      if (!user) {
        res.status(404).json({ error: 'user_not_found' });
        return;
      }
      res.json({
        month: monthKey,
        user,
        monthlyMeetingsUsed: user.monthlyMeetingsJoined?.[monthKey] || 0,
        monthlyMeetingsLimit: stats.freeTierMonthlyMeetingLimit,
      });
      return;
    }

    if (meetingId) {
      const meeting = stats.meetings[meetingId];
      if (!meeting) {
        res.status(404).json({ error: 'meeting_not_found' });
        return;
      }
      res.json({ month: monthKey, meeting });
      return;
    }

    res.json({
      month: monthKey,
      overview: {
        startedAt: stats.startedAt,
        lastUpdatedAt: stats.lastUpdatedAt,
        totalMessages: stats.totalMessages,
        totalMeetingsJoined: stats.totalMeetingsJoined,
        activeMeetings: stats.activeMeetingConversationIds.length,
        freeTierMonthlyMeetingLimit: stats.freeTierMonthlyMeetingLimit,
        modelInputCostPer1kUsd: stats.modelInputCostPer1kUsd,
        modelOutputCostPer1kUsd: stats.modelOutputCostPer1kUsd,
        totalEstimatedInputTokens: stats.totalEstimatedInputTokens,
        totalEstimatedOutputTokens: stats.totalEstimatedOutputTokens,
        totalEstimatedTokens: stats.totalEstimatedTokens,
        totalEstimatedCostUsd: Number(stats.totalEstimatedCostUsd.toFixed(6)),
      },
      users: Object.values(stats.users),
      meetings: Object.values(stats.meetings),
    });
  } catch (error) {
    logAdminError('server', '/api/admin/stats', error);
    console.error('[ADMIN_STATS_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_get_stats' });
  }
});

app.http.post('/api/admin/config/free-tier-limit', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const limit = Number(req?.body?.limit);
    if (!Number.isFinite(limit) || limit < 1 || limit > 1000) {
      res.status(400).json({ error: 'invalid_limit', message: 'limit must be between 1 and 1000' });
      return;
    }

    const stats = loadBotAdminStats();
    stats.freeTierMonthlyMeetingLimit = Math.floor(limit);
    saveBotAdminStats();
    res.json({ success: true, freeTierMonthlyMeetingLimit: stats.freeTierMonthlyMeetingLimit });
  } catch (error) {
    logAdminError('server', '/api/admin/config/free-tier-limit', error);
    console.error('[ADMIN_CONFIG_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_update_limit' });
  }
});

app.http.post('/api/admin/config/max-users', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const maxUsers = Number(req?.body?.maxUsers);
    if (!Number.isFinite(maxUsers) || maxUsers < 1 || maxUsers > 50000) {
      res.status(400).json({ error: 'invalid_max_users', message: 'maxUsers must be between 1 and 50000' });
      return;
    }

    const stats = loadBotAdminStats();
    stats.maxUsers = Math.floor(maxUsers);
    saveBotAdminStats();
    res.json({ success: true, maxUsers: stats.maxUsers });
  } catch (error) {
    logAdminError('server', '/api/admin/config/max-users', error);
    console.error('[ADMIN_CONFIG_MAX_USERS_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_update_max_users' });
  }
});

app.http.post('/api/admin/config/enforce-global-limits', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;

    const enabled = req?.body?.enabled === true;
    const stats = loadBotAdminStats();
    stats.enforceGlobalLimits = enabled;
    saveBotAdminStats();

    res.json({ success: true, enforceGlobalLimits: stats.enforceGlobalLimits });
  } catch (error) {
    logAdminError('server', '/api/admin/config/enforce-global-limits', error);
    console.error('[ADMIN_CONFIG_ENFORCE_GLOBAL_LIMITS_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_update_enforcement_setting' });
  }
});

app.http.post('/api/admin/users/block-status', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const userId = (req?.body?.userId || '').toString().trim();
    const blocked = req?.body?.blocked === true;
    if (!userId) {
      res.status(400).json({ error: 'invalid_user_id' });
      return;
    }

    const stats = loadBotAdminStats();
    const user = stats.users[userId];
    if (!user) {
      res.status(404).json({ error: 'user_not_found' });
      return;
    }

    user.blocked = blocked;
    if (!blocked) {
      user.blockReason = '';
      user.blockedAt = undefined;
    }
    user.lastSeenAt = Date.now();
    saveBotAdminStats();
    res.json({ success: true, userId, blocked: user.blocked });
  } catch (error) {
    logAdminError('server', '/api/admin/users/block-status', error);
    console.error('[ADMIN_USER_BLOCK_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_update_user_block_status' });
  }
});

app.http.post('/api/admin/users/policy', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;

    const userId = (req?.body?.userId || '').toString().trim();
    const tokenPolicyRaw = (req?.body?.tokenPolicy || 'unlimited').toString();
    const tokenLimitRaw = req?.body?.tokenLimit;
    const meetingLimitRaw = req?.body?.monthlyMeetingLimitOverride;

    if (!userId) {
      res.status(400).json({ error: 'invalid_user_id' });
      return;
    }

    const tokenPolicy = tokenPolicyRaw === 'limited' ? 'limited' : 'unlimited';
    const tokenLimit = tokenLimitRaw === null || tokenLimitRaw === '' || typeof tokenLimitRaw === 'undefined'
      ? null
      : Number(tokenLimitRaw);
    const monthlyMeetingLimitOverride = meetingLimitRaw === null || meetingLimitRaw === '' || typeof meetingLimitRaw === 'undefined'
      ? null
      : Number(meetingLimitRaw);

    if (tokenPolicy === 'limited' && (!Number.isFinite(tokenLimit) || (tokenLimit as number) < 1)) {
      res.status(400).json({ error: 'invalid_token_limit' });
      return;
    }
    if (monthlyMeetingLimitOverride !== null && (!Number.isFinite(monthlyMeetingLimitOverride) || monthlyMeetingLimitOverride < 1)) {
      res.status(400).json({ error: 'invalid_meeting_limit_override' });
      return;
    }

    const stats = loadBotAdminStats();
    const user = stats.users[userId];
    if (!user) {
      res.status(404).json({ error: 'user_not_found' });
      return;
    }

    user.tokenPolicy = tokenPolicy;
    user.tokenLimit = tokenPolicy === 'limited' ? Math.floor(tokenLimit as number) : null;
    user.monthlyMeetingLimitOverride = monthlyMeetingLimitOverride === null ? null : Math.floor(monthlyMeetingLimitOverride);
    user.lastSeenAt = Date.now();
    saveBotAdminStats();

    res.json({
      success: true,
      userId,
      tokenPolicy: user.tokenPolicy,
      tokenLimit: user.tokenLimit,
      monthlyMeetingLimitOverride: user.monthlyMeetingLimitOverride,
    });
  } catch (error) {
    logAdminError('server', '/api/admin/users/policy', error);
    console.error('[ADMIN_USER_POLICY_API] Failed:', error);
    res.status(500).json({ error: 'failed_to_update_user_policy' });
  }
});

function renderAdminLayout(
  activePage: 'overview' | 'users' | 'meetings' | 'settings' | 'invoices' | 'errors',
  pageTitle: string,
  contentHtml: string,
  adminUser: AdminUserProfile,
  pageScript = ''
): string {
  const isActive = (page: string) => (activePage === page ? 'menu-link active' : 'menu-link');
  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${pageTitle}</title>
  <style>
    :root {
      --bg: #041014;
      --bg-soft: #0a1e24;
      --panel: rgba(7, 34, 42, 0.78);
      --line: rgba(47, 85, 151, 0.4);
      --accent: #2F5597;
      --text: #e2e9f7;
      --muted: #aebddc;
      --shadow: 0 12px 36px rgba(0, 0, 0, 0.28);
    }
    * { box-sizing: border-box; }
    html, body { margin: 0; padding: 0; }
    body {
      font-family: "Trebuchet MS", "Lucida Sans Unicode", "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(900px 360px at -10% -10%, rgba(47, 85, 151, 0.28), transparent 60%),
        radial-gradient(800px 280px at 120% 110%, rgba(47, 85, 151, 0.18), transparent 60%),
        linear-gradient(140deg, var(--bg), #02090d 45%, var(--bg-soft));
      min-height: 100vh;
    }
    .layout { display: grid; grid-template-columns: 260px 1fr; min-height: 100vh; }
    .sidebar {
      position: sticky; top: 0; height: 100vh; border-right: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(6, 28, 35, 0.88), rgba(3, 14, 18, 0.95));
      padding: 22px 16px; backdrop-filter: blur(8px);
    }
    .brand { border: 1px solid var(--line); border-radius: 12px; padding: 12px; margin-bottom: 18px; background: rgba(47, 85, 151, 0.18); }
    .brand h1 { margin: 0; font-size: 18px; }
    .brand p { margin: 4px 0 0; color: var(--muted); font-size: 12px; }
    .menu { display: flex; flex-direction: column; gap: 8px; }
    .menu-link { text-decoration: none; color: var(--text); padding: 10px 12px; border-radius: 10px; border: 1px solid transparent; transition: 160ms ease; }
    .menu-link:hover { border-color: var(--line); background: rgba(47, 85, 151, 0.22); }
    .menu-link.active { border-color: var(--line); background: rgba(47, 85, 151, 0.28); }
    .main { padding: 26px; }
    .hero { border: 1px solid var(--line); border-radius: 14px; background: var(--panel); box-shadow: var(--shadow); padding: 18px; margin-bottom: 18px; }
    .subtitle { margin: 8px 0 0; color: var(--muted); font-size: 13px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .card { border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: linear-gradient(145deg, rgba(47, 85, 151, 0.2), rgba(8, 16, 31, 0.88)); }
    .card strong { color: #dce6fb; font-size: 12px; text-transform: uppercase; }
    .metric { font-size: 26px; margin-top: 8px; font-weight: 700; color: var(--accent); }
    .table-wrap { border: 1px solid var(--line); border-radius: 12px; overflow: auto; background: var(--panel); }
    table { border-collapse: collapse; width: 100%; min-width: 760px; }
    th, td { border-bottom: 1px solid rgba(47, 85, 151, 0.2); padding: 10px; text-align: left; }
    th { background: rgba(47, 85, 151, 0.22); color: #dce6fb; font-size: 12px; text-transform: uppercase; }
    .controls { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }
    .control { border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: var(--panel); }
    .chart-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
    .chart-grid canvas { width: 100%; height: 180px; border-radius: 8px; background: rgba(6, 15, 27, 0.38); }
    label { font-size: 12px; color: var(--muted); display: block; margin-bottom: 6px; }
    input { width: 100%; padding: 9px 10px; border-radius: 8px; border: 1px solid var(--line); background: rgba(1, 10, 12, 0.85); color: var(--text); margin-bottom: 8px; }
    button { border: 1px solid var(--line); background: linear-gradient(135deg, rgba(47, 85, 151, 0.34), rgba(47, 85, 151, 0.2)); color: var(--text); border-radius: 8px; padding: 8px 12px; cursor: pointer; }
    .status-pill { padding: 3px 8px; border-radius: 999px; border: 1px solid var(--line); background: rgba(47, 85, 151, 0.2); font-size: 12px; }
    .status-pill.blocked { border-color: rgba(255, 110, 110, 0.35); background: rgba(255, 110, 110, 0.12); color: #ffd4d4; }
    .muted { color: var(--muted); font-size: 12px; }
    .toast-host {
      position: fixed;
      top: 16px;
      right: 16px;
      z-index: 1200;
      display: flex;
      flex-direction: column;
      gap: 8px;
      pointer-events: none;
    }
    .toast {
      min-width: 240px;
      max-width: 360px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: rgba(10, 20, 35, 0.94);
      color: var(--text);
      padding: 10px 12px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.35);
      opacity: 0;
      transform: translateY(-6px);
      transition: 180ms ease;
      pointer-events: auto;
    }
    .toast.show { opacity: 1; transform: translateY(0); }
    .toast.success { border-color: rgba(76, 175, 80, 0.55); }
    .toast.error { border-color: rgba(244, 67, 54, 0.55); }
    .toast.info { border-color: var(--line); }
    @media (max-width: 980px) {
      .layout { grid-template-columns: 1fr; }
      .sidebar { position: static; height: auto; border-right: none; border-bottom: 1px solid var(--line); }
      .menu { flex-direction: row; flex-wrap: wrap; }
      .main { padding: 16px; }
      .chart-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand"><h1>Mela Control</h1><p>AI Ops Console</p></div>
      <div class="brand" style="margin-bottom:14px;">
        <p style="margin:0;color:var(--muted);font-size:11px;">Signed in as</p>
        <h1 style="font-size:15px;margin-top:6px;">${adminUser.displayName}</h1>
        <p style="margin:4px 0 0;font-size:12px;word-break:break-word;">${adminUser.email}</p>
      </div>
      <nav class="menu">
        <a class="${isActive('overview')}" href="/admin/overview">Overview</a>
        <a class="${isActive('users')}" href="/admin/users">Users</a>
        <a class="${isActive('meetings')}" href="/admin/meetings">Meetings</a>
        <a class="${isActive('invoices')}" href="/admin/invoices">Invoices</a>
        <a class="${isActive('errors')}" href="/admin/errors">Errors</a>
        <a class="${isActive('settings')}" href="/admin/settings">Settings</a>
      </nav>
      <div style="margin-top:16px;"><a class="menu-link" href="${ADMIN_LOGOUT_PATH}">Sign out</a></div>
    </aside>
    <main class="main">${contentHtml}</main>
  </div>
  <div id="toastHost" class="toast-host"></div>
  <script>
    function showToast(message, type = 'info', timeoutMs = 2600) {
      const host = document.getElementById('toastHost');
      if (!host) return;
      const el = document.createElement('div');
      el.className = 'toast ' + (type || 'info');
      el.textContent = message;
      host.appendChild(el);
      requestAnimationFrame(() => el.classList.add('show'));
      setTimeout(() => {
        el.classList.remove('show');
        setTimeout(() => el.remove(), 220);
      }, timeoutMs);
    }

    window.addEventListener('error', function (event) {
      const payload = {
        route: window.location.pathname,
        message: (event && event.message) ? String(event.message) : 'window_error',
        stack: event && event.error && event.error.stack ? String(event.error.stack) : '',
      };
      console.error('[ADMIN_CLIENT_ERROR]', payload.message, payload.stack || '(no stack)');
      fetch('/api/admin/errors/client', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).catch(function () {});
    });

    window.addEventListener('unhandledrejection', function (event) {
      const reason = event && event.reason ? event.reason : 'unhandled_rejection';
      const payload = {
        route: window.location.pathname,
        message: typeof reason === 'string' ? reason : ((reason && reason.message) ? String(reason.message) : 'unhandled_rejection'),
        stack: reason && reason.stack ? String(reason.stack) : '',
      };
      console.error('[ADMIN_CLIENT_REJECTION]', payload.message, payload.stack || '(no stack)');
      fetch('/api/admin/errors/client', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      }).catch(function () {});
    });
  </script>
  ${pageScript ? `<script>${pageScript}</script>` : ''}
</body>
</html>`;
}

app.http.get('/api/admin/errors', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const limitRaw = Number(req?.query?.limit);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(Math.floor(limitRaw), 500) : 200;
    const logs = loadAdminErrorLogs();
    res.json({
      total: logs.length,
      errors: logs.slice(-limit).reverse(),
    });
  } catch (error) {
    logAdminError('server', '/api/admin/errors', error);
    res.status(500).json({ error: 'failed_to_get_admin_errors' });
  }
});

app.http.post('/api/admin/errors/clear', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    saveAdminErrorLogs([]);
    res.json({ success: true });
  } catch (error) {
    logAdminError('server', '/api/admin/errors/clear', error);
    res.status(500).json({ error: 'failed_to_clear_admin_errors' });
  }
});

app.http.post('/api/admin/errors/client', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;

    const message = (req?.body?.message || '').toString().trim() || 'client_error';
    const stack = (req?.body?.stack || '').toString();
    const route = (req?.body?.route || '/admin').toString();

    logAdminError('client', route, { message, stack }, {
      userId: adminSession.userId,
      tenantId: adminSession.tenantId,
      meta: {
        userAgent: req?.headers?.['user-agent'] || '',
      },
    });
    res.json({ success: true });
  } catch {
    res.status(500).json({ error: 'failed_to_log_client_error' });
  }
});

app.http.get('/admin/errors', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const logs = loadAdminErrorLogs().slice(-200).reverse();

  const rows = logs
    .map((entry, index) => {
      const safeMessage = (entry.message || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const safeRoute = (entry.route || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      const safeSource = (entry.source || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      return `<tr>
        <td>${entry.ts}</td>
        <td>${safeSource}</td>
        <td>${safeRoute}</td>
        <td style="max-width:520px;white-space:normal;word-break:break-word;">${safeMessage}</td>
        <td><button onclick="copyErrorByIndex(${index})">Copy</button></td>
      </tr>`;
    })
    .join('');

  const content = `
    <section class="hero"><h2>Error Reporting</h2><p class="subtitle">Exact admin page/API errors for troubleshooting.</p></section>
    <section class="controls">
      <div class="control">
        <label>Actions</label>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <button onclick="refreshErrors()">Refresh</button>
          <button onclick="copyAllErrors()">Copy All</button>
          <button onclick="clearErrors()">Clear Logs</button>
        </div>
        <div class="muted" style="margin-top:8px;">Showing latest ${logs.length} error entries.</div>
      </div>
    </section>
    <div class="table-wrap" style="margin-top:14px;">
      <table id="adminErrorTable">
        <thead><tr><th>Timestamp</th><th>Source</th><th>Route</th><th>Message</th><th>Copy</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="5">No errors logged</td></tr>'}</tbody>
      </table>
    </div>`;

  const logsJson = JSON.stringify(logs);
  const script = `
    const adminErrors = ${logsJson};

    async function copyErrorByIndex(index) {
      try {
        const row = adminErrors[index];
        const text = JSON.stringify(row || {}, null, 2);
        await navigator.clipboard.writeText(text);
        showToast('Error copied', 'success', 1300);
      } catch {
        showToast('Copy failed', 'error', 2000);
      }
    }

    async function copyAllErrors() {
      try {
        const text = JSON.stringify(adminErrors, null, 2);
        await navigator.clipboard.writeText(text);
        showToast('All errors copied', 'success', 1300);
      } catch {
        showToast('Copy failed', 'error', 2000);
      }
    }

    function refreshErrors() {
      window.location.reload();
    }

    async function clearErrors() {
      const response = await fetch('/api/admin/errors/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      if (!response.ok) {
        showToast('Failed to clear logs', 'error', 2200);
        return;
      }
      showToast('Logs cleared', 'success', 1200);
      setTimeout(() => window.location.reload(), 350);
    }
  `;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('errors', 'Mela Control - Errors', content, adminUser, script));
});

app.http.get('/admin', async (_req: any, res: any) => {
  res.redirect('/admin/overview');
});

app.http.get('/admin/overview', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const stats = loadBotAdminStats();
  const monthKey = getMonthKey();
  const tenantOptions = Array.from(
    new Set(
      Object.values(stats.users)
        .map((u) => normalizeTenantId(u.tenantId))
        .filter((v) => !!v)
    )
  )
    .sort((a, b) => a.localeCompare(b))
    .map((tenant) => `<option value="${tenant}">${tenant}</option>`)
    .join('');
  const userOptions = Object.values(stats.users)
    .sort((a, b) => a.displayName.localeCompare(b.displayName))
    .map((u) => {
      const tenant = normalizeTenantId(u.tenantId);
      return `<option value="${u.userId}" data-tenant="${tenant}">${u.displayName}</option>`;
    })
    .join('');

  const dailyUsageJson = toInlineScriptJson(stats.dailyUsage || {});
  const perUserDailyUsageJson = toInlineScriptJson(stats.perUserDailyUsage || {});
  const usersSnapshotJson = toInlineScriptJson(stats.users || {});
  const statsSnapshotJson = toInlineScriptJson({
    totalMessages: stats.totalMessages || 0,
    totalEstimatedInputTokens: stats.totalEstimatedInputTokens || 0,
    totalEstimatedOutputTokens: stats.totalEstimatedOutputTokens || 0,
    totalEstimatedTokens: stats.totalEstimatedTokens || 0,
    totalEstimatedCostUsd: stats.totalEstimatedCostUsd || 0,
  });

  const content = `
    <section class="hero"><h2>Overview</h2><p class="subtitle">Billing month: ${monthKey}</p></section>
    <section class="cards">
      <div class="card"><strong>Total Messages</strong><div id="kpiMessages" class="metric">${stats.totalMessages}</div></div>
      <div class="card"><strong>Detected Users</strong><div id="kpiUsers" class="metric">${Object.keys(stats.users || {}).length}</div></div>
      <div class="card"><strong>Meetings Joined</strong><div id="kpiMeetings" class="metric">${stats.totalMeetingsJoined}</div></div>
      <div class="card"><strong>Active Meetings</strong><div class="metric">${stats.activeMeetingConversationIds.length}</div></div>
      <div class="card"><strong>Total Tokens</strong><div id="kpiTokens" class="metric">${stats.totalEstimatedTokens}</div></div>
      <div class="card"><strong>Total Cost (USD)</strong><div id="kpiCost" class="metric">$${stats.totalEstimatedCostUsd.toFixed(6)}</div></div>
    </section>
    <section class="hero" style="margin-top:12px;">
      <h2 style="margin-bottom:10px;">Usage Trends</h2>
      <div class="controls" style="margin-bottom:12px;">
        <div class="control">
          <label>Date Range</label>
          <div style="display:flex;gap:8px;flex-wrap:wrap;">
            <button id="range-3" onclick="setRange(3)">3d</button>
            <button id="range-7" onclick="setRange(7)">7d</button>
            <button id="range-30" onclick="setRange(30)">30d</button>
            <button id="range-90" onclick="setRange(90)">90d</button>
          </div>
        </div>
        <div class="control">
          <label>Tenant Filter</label>
          <select id="tenantFilter" onchange="onTenantFilterChanged()" style="width:100%;padding:9px 10px;border-radius:8px;border:1px solid var(--line);background:rgba(1, 10, 12, 0.85);color:var(--text);">
            <option value="all">All tenants</option>
            ${tenantOptions}
          </select>
        </div>
        <div class="control">
          <label>User Filter</label>
          <select id="userFilter" onchange="renderAllCharts()" style="width:100%;padding:9px 10px;border-radius:8px;border:1px solid var(--line);background:rgba(1, 10, 12, 0.85);color:var(--text);">
            <option value="all">All users</option>
            ${userOptions}
          </select>
        </div>
      </div>
      <div id="rangeSummary" class="muted" style="margin-bottom:10px;"></div>
      <div class="chart-grid">
        <div class="control"><label>Token Usage Trend</label><canvas id="tokensChart" height="170"></canvas></div>
        <div class="control"><label>Meetings Joined Trend</label><canvas id="meetingsChart" height="170"></canvas></div>
        <div class="control"><label>Messages Trend</label><canvas id="messagesChart" height="170"></canvas></div>
        <div class="control"><label>Cost Trend (USD)</label><canvas id="costChart" height="170"></canvas></div>
      </div>
    </section>`;

  const script = `
    const dailyUsage = ${dailyUsageJson};
    const perUserDailyUsage = ${perUserDailyUsageJson};
    const usersSnapshot = ${usersSnapshotJson};
    const statsSnapshot = ${statsSnapshotJson};
    let currentRange = 30;

    function setRange(days) {
      currentRange = days;
      document.querySelectorAll('[id^="range-"]').forEach((btn) => {
        btn.style.opacity = btn.id === 'range-' + String(days) ? '1' : '0.6';
      });
      renderAllCharts();
    }

    function getDateKeys(rangeDays) {
      const keys = [];
      const now = new Date();
      for (let i = rangeDays - 1; i >= 0; i--) {
        const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - i));
        const key = d.getUTCFullYear() + '-' + String(d.getUTCMonth() + 1).padStart(2, '0') + '-' + String(d.getUTCDate()).padStart(2, '0');
        keys.push(key);
      }
      return keys;
    }

    function getSeries(rangeDays) {
      const tenantId = document.getElementById('tenantFilter').value;
      const userId = document.getElementById('userFilter').value;
      let source = {};

      if (userId !== 'all') {
        source = perUserDailyUsage[userId] || {};
      } else if (tenantId === 'all') {
        source = dailyUsage || {};
      } else {
        source = {};
        const tenantUsers = Object.entries(usersSnapshot || {})
          .filter(([, u]) => (u && (u.tenantId || 'unknown-tenant').trim() || 'unknown-tenant') === tenantId)
          .map(([id]) => id);

        for (const uid of tenantUsers) {
          const dayMap = perUserDailyUsage[uid] || {};
          for (const [dayKey, entry] of Object.entries(dayMap)) {
            if (!source[dayKey]) {
              source[dayKey] = {
                totalTokens: 0,
                inputTokens: 0,
                outputTokens: 0,
                messages: 0,
                meetingsJoined: 0,
                costUsd: 0,
              };
            }
            source[dayKey].totalTokens += Number(entry.totalTokens || 0);
            source[dayKey].inputTokens += Number(entry.inputTokens || 0);
            source[dayKey].outputTokens += Number(entry.outputTokens || 0);
            source[dayKey].messages += Number(entry.messages || 0);
            source[dayKey].meetingsJoined += Number(entry.meetingsJoined || 0);
            source[dayKey].costUsd += Number(entry.costUsd || 0);
          }
        }
      }

      const hasSourceData = Object.keys(source || {}).length > 0;

      if (!hasSourceData) {
        const today = getDateKeys(1)[0];
        if (userId === 'all' && tenantId === 'all') {
          source = {
            [today]: {
              totalTokens: Number(statsSnapshot.totalEstimatedTokens || 0),
              inputTokens: Number(statsSnapshot.totalEstimatedInputTokens || 0),
              outputTokens: Number(statsSnapshot.totalEstimatedOutputTokens || 0),
              messages: Number(statsSnapshot.totalMessages || 0),
              meetingsJoined: 0,
              costUsd: Number(statsSnapshot.totalEstimatedCostUsd || 0),
            }
          };
        } else if (userId === 'all' && tenantId !== 'all') {
          const tenantUsers = Object.values(usersSnapshot || {}).filter((u) => ((u?.tenantId || 'unknown-tenant').trim() || 'unknown-tenant') === tenantId);
          const fallback = tenantUsers.reduce((acc, user) => {
            const monthMap = user?.monthlyMeetingsJoined || {};
            const meetingsJoinedApprox = Object.values(monthMap).reduce((a, b) => Number(a) + Number(b || 0), 0);
            acc.totalTokens += Number(user?.estimatedTotalTokens || 0);
            acc.inputTokens += Number(user?.estimatedInputTokens || 0);
            acc.outputTokens += Number(user?.estimatedOutputTokens || 0);
            acc.messages += Number(user?.totalMessages || 0);
            acc.meetingsJoined += Number(meetingsJoinedApprox || 0);
            acc.costUsd += Number(user?.estimatedCostUsd || 0);
            return acc;
          }, { totalTokens: 0, inputTokens: 0, outputTokens: 0, messages: 0, meetingsJoined: 0, costUsd: 0 });

          source = {
            [today]: fallback
          };
        } else {
          const user = usersSnapshot[userId] || {};
          const monthMap = user.monthlyMeetingsJoined || {};
          const meetingsJoinedApprox = Object.values(monthMap).reduce((a, b) => Number(a) + Number(b || 0), 0);
          source = {
            [today]: {
              totalTokens: Number(user.estimatedTotalTokens || 0),
              inputTokens: Number(user.estimatedInputTokens || 0),
              outputTokens: Number(user.estimatedOutputTokens || 0),
              messages: Number(user.totalMessages || 0),
              meetingsJoined: Number(meetingsJoinedApprox || 0),
              costUsd: Number(user.estimatedCostUsd || 0),
            }
          };
        }
      }

      const keys = getDateKeys(rangeDays);
      const labels = keys.map((k) => k.slice(5));
      const tokens = keys.map((k) => Number((source[k] && source[k].totalTokens) || 0));
      const meetings = keys.map((k) => Number((source[k] && source[k].meetingsJoined) || 0));
      const messages = keys.map((k) => Number((source[k] && source[k].messages) || 0));
      const costs = keys.map((k) => Number((source[k] && source[k].costUsd) || 0));
      return { labels, tokens, meetings, messages, costs };
    }

    function drawChart(canvasId, labels, values, lineColor, fillColor) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const w = canvas.width = canvas.clientWidth * (window.devicePixelRatio || 1);
      const h = canvas.height = canvas.clientHeight * (window.devicePixelRatio || 1);
      const dpr = window.devicePixelRatio || 1;
      ctx.scale(dpr, dpr);
      const width = canvas.clientWidth;
      const height = canvas.clientHeight;
      ctx.clearRect(0, 0, width, height);

      const left = 28, right = width - 8, top = 8, bottom = height - 22;
      const maxVal = Math.max(1, ...values);
      const stepX = values.length > 1 ? (right - left) / (values.length - 1) : (right - left);

      const allZero = values.every((v) => v === 0);
      if (allZero) {
        ctx.fillStyle = '#aebddc';
        ctx.font = '12px Segoe UI';
        ctx.fillText('No usage in selected range', left, (top + bottom) / 2);
        return;
      }

      ctx.strokeStyle = 'rgba(47,85,151,0.35)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(left, bottom);
      ctx.lineTo(right, bottom);
      ctx.stroke();

      ctx.beginPath();
      values.forEach((v, i) => {
        const x = left + i * stepX;
        const y = bottom - ((v / maxVal) * (bottom - top));
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });

      ctx.strokeStyle = lineColor;
      ctx.lineWidth = 2;
      ctx.stroke();

      values.forEach((v, i) => {
        if (v <= 0) return;
        const x = left + i * stepX;
        const y = bottom - ((v / maxVal) * (bottom - top));
        ctx.beginPath();
        ctx.arc(x, y, 2.5, 0, Math.PI * 2);
        ctx.fillStyle = lineColor;
        ctx.fill();
      });

      ctx.lineTo(right, bottom);
      ctx.lineTo(left, bottom);
      ctx.closePath();
      ctx.fillStyle = fillColor;
      ctx.fill();

      ctx.fillStyle = '#aebddc';
      ctx.font = '10px Segoe UI';
      ctx.fillText(labels[0] || '', left, height - 8);
      ctx.fillText(labels[labels.length - 1] || '', right - 28, height - 8);
      ctx.fillText(String(maxVal), 2, top + 8);
    }

    function renderAllCharts() {
      const { labels, tokens, meetings, messages, costs } = getSeries(currentRange);
      const totals = {
        tokens: tokens.reduce((a, b) => a + b, 0),
        meetings: meetings.reduce((a, b) => a + b, 0),
        messages: messages.reduce((a, b) => a + b, 0),
        cost: costs.reduce((a, b) => a + b, 0),
      };
      const tenantFilterEl = document.getElementById('tenantFilter');
      const userFilterEl = document.getElementById('userFilter');
      const tenantLabel = tenantFilterEl.value === 'all' ? 'All tenants' : tenantFilterEl.value;
      const userLabel = userFilterEl.value === 'all' ? 'All users' : 'Selected user';
      document.getElementById('rangeSummary').textContent =
        tenantLabel + ' | ' + userLabel + ' | Range: ' + currentRange + 'd | Tokens: ' + totals.tokens +
        ' | Meetings: ' + totals.meetings + ' | Messages: ' + totals.messages +
        ' | Cost: $' + totals.cost.toFixed(6);

      document.getElementById('kpiMessages').textContent = String(totals.messages);
      document.getElementById('kpiMeetings').textContent = String(totals.meetings);
      document.getElementById('kpiTokens').textContent = String(totals.tokens);
      document.getElementById('kpiCost').textContent = '$' + totals.cost.toFixed(6);

      const tenantId = tenantFilterEl.value;
      const userId = userFilterEl.value;
      if (userId !== 'all') {
        document.getElementById('kpiUsers').textContent = '1';
      } else if (tenantId === 'all') {
        document.getElementById('kpiUsers').textContent = String(Object.keys(usersSnapshot || {}).length);
      } else {
        const count = Object.values(usersSnapshot || {}).filter((u) => ((u?.tenantId || 'unknown-tenant').trim() || 'unknown-tenant') === tenantId).length;
        document.getElementById('kpiUsers').textContent = String(count);
      }

      drawChart('tokensChart', labels, tokens, '#2F5597', 'rgba(47,85,151,0.18)');
      drawChart('meetingsChart', labels, meetings, '#4c7fd9', 'rgba(76,127,217,0.18)');
      drawChart('messagesChart', labels, messages, '#6fa1f2', 'rgba(111,161,242,0.16)');
      drawChart('costChart', labels, costs, '#8ab6ff', 'rgba(138,182,255,0.15)');
    }

    function onTenantFilterChanged() {
      const tenantId = document.getElementById('tenantFilter').value;
      const userFilter = document.getElementById('userFilter');
      const selectedUser = userFilter.value;
      let selectedUserStillVisible = selectedUser === 'all';

      Array.from(userFilter.options).forEach((opt) => {
        if (opt.value === 'all') {
          opt.hidden = false;
          return;
        }
        const optTenant = opt.getAttribute('data-tenant') || 'unknown-tenant';
        const visible = tenantId === 'all' || optTenant === tenantId;
        opt.hidden = !visible;
        if (visible && opt.value === selectedUser) {
          selectedUserStillVisible = true;
        }
      });

      if (!selectedUserStillVisible) {
        userFilter.value = 'all';
      }
      renderAllCharts();
    }

    window.addEventListener('resize', () => renderAllCharts());
    onTenantFilterChanged();
    setRange(30);
  `;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('overview', 'Mela Control - Overview', content, adminUser, script));
});

app.http.get('/admin/users', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const stats = loadBotAdminStats();
  const monthKey = getMonthKey();
  const userRows = Object.values(stats.users)
    .map((u) => {
      const monthly = u.monthlyMeetingsJoined?.[monthKey] || 0;
      const effectiveMeetingLimit = u.monthlyMeetingLimitOverride && u.monthlyMeetingLimitOverride > 0
        ? u.monthlyMeetingLimitOverride
        : stats.freeTierMonthlyMeetingLimit;
      const payload = encodeURIComponent(JSON.stringify({
        userId: u.userId,
        displayName: u.displayName,
        tokenPolicy: u.tokenPolicy,
        tokenLimit: u.tokenLimit,
        monthlyMeetingLimitOverride: u.monthlyMeetingLimitOverride,
        blocked: u.blocked,
        blockReason: u.blockReason || '',
        usedTokens: u.estimatedTotalTokens,
        estimatedCostUsd: u.estimatedCostUsd,
        monthlyUsed: monthly,
        monthlyLimit: effectiveMeetingLimit,
      }));
      return `<tr>
        <td>${u.displayName}</td>
        <td>${u.userId}</td>
        <td>${monthly}/${effectiveMeetingLimit}</td>
        <td>${u.estimatedTotalTokens}</td>
        <td>$${u.estimatedCostUsd.toFixed(6)}</td>
        <td><span class="status-pill ${u.blocked ? 'blocked' : ''}">${u.blocked ? 'Blocked' : 'Active'}</span></td>
        <td>
          <button onclick="openUserPolicyModal('${payload}')">Manage</button>
          <button onclick="toggleUserBlock('${u.userId}', ${u.blocked ? 'false' : 'true'})">${u.blocked ? 'Unblock' : 'Block'}</button>
        </td>
      </tr>`;
    })
    .join('');
  const content = `
    <section class="hero"><h2>User Management</h2><p class="subtitle">Control user status and monthly usage.</p></section>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Name</th><th>User ID</th><th>Monthly Meetings</th><th>Used Tokens</th><th>Estimated Cost</th><th>Status</th><th>Action</th></tr></thead>
        <tbody>${userRows || '<tr><td colspan="7">No users yet</td></tr>'}</tbody>
      </table>
    </div>
    <div id="userPolicyModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.55);z-index:999;align-items:center;justify-content:center;padding:16px;">
      <div style="width:min(640px,100%);max-height:90vh;overflow:auto;background:#0e1a30;border:1px solid rgba(47,85,151,0.45);border-radius:12px;padding:16px;box-shadow:0 18px 44px rgba(0,0,0,0.4);">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
          <h3 style="margin:0;">Manage User Policy</h3>
          <button onclick="closeUserPolicyModal()">Close</button>
        </div>
        <div class="muted" id="modalUserMeta"></div>
        <div class="muted" id="modalBlockReason" style="margin-top:6px;"></div>
        <div style="margin-top:12px;display:grid;grid-template-columns:1fr;gap:10px;">
          <div>
            <label>Token Policy</label>
            <select id="modalTokenPolicy" style="width:100%;padding:9px 10px;border-radius:8px;border:1px solid var(--line);background:rgba(1, 10, 12, 0.85);color:var(--text);">
              <option value="unlimited">Unlimited</option>
              <option value="limited">Limited</option>
            </select>
          </div>
          <div>
            <label>Token Assignment (limit)</label>
            <input id="modalTokenLimit" type="number" min="1" placeholder="Example: 50000" />
          </div>
          <div>
            <label>Monthly Meeting Limit Override</label>
            <input id="modalMeetingLimit" type="number" min="1" placeholder="Leave blank for default" />
          </div>
          <div>
            <label style="display:flex;gap:8px;align-items:center;">
              <input id="modalBlocked" type="checkbox" style="width:auto;margin:0;" />
              Block this user
            </label>
            <div style="display:flex;gap:8px;margin-top:8px;">
              <button type="button" onclick="setModalBlocked(true)">Lock User</button>
              <button type="button" onclick="setModalBlocked(false)">Unlock User</button>
            </div>
          </div>
        </div>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:14px;">
          <button onclick="closeUserPolicyModal()">Cancel</button>
          <button onclick="saveModalUserPolicy()">Save Changes</button>
        </div>
      </div>
    </div>`;
  const script = `
    let activeUserPolicy = null;

    async function toggleUserBlock(userId, blocked) {
      const response = await fetch('/api/admin/users/block-status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId, blocked })
      });
      if (!response.ok) {
        showToast('Failed to update user status', 'error', 3200);
        return;
      }
      showToast(blocked ? 'User blocked' : 'User unblocked', 'success', 1500);
      setTimeout(() => location.reload(), 450);
    }

    function openUserPolicyModal(encodedPayload) {
      try {
        activeUserPolicy = JSON.parse(decodeURIComponent(encodedPayload));
      } catch {
        showToast('Invalid user policy payload', 'error', 3200);
        return;
      }

      document.getElementById('modalUserMeta').textContent =
        activeUserPolicy.displayName + ' (' + activeUserPolicy.userId + ') | Monthly: ' +
        activeUserPolicy.monthlyUsed + '/' + activeUserPolicy.monthlyLimit + ' | Tokens used: ' +
        activeUserPolicy.usedTokens;
      document.getElementById('modalBlockReason').textContent = activeUserPolicy.blockReason
        ? ('Block reason: ' + activeUserPolicy.blockReason)
        : 'Block reason: none';

      document.getElementById('modalTokenPolicy').value = activeUserPolicy.tokenPolicy || 'unlimited';
      document.getElementById('modalTokenLimit').value = activeUserPolicy.tokenLimit || '';
      document.getElementById('modalMeetingLimit').value = activeUserPolicy.monthlyMeetingLimitOverride || '';
      document.getElementById('modalBlocked').checked = !!activeUserPolicy.blocked;

      document.getElementById('userPolicyModal').style.display = 'flex';
    }

    function setModalBlocked(value) {
      const cb = document.getElementById('modalBlocked');
      cb.checked = !!value;
    }

    function closeUserPolicyModal() {
      document.getElementById('userPolicyModal').style.display = 'none';
      activeUserPolicy = null;
    }

    async function saveModalUserPolicy() {
      if (!activeUserPolicy) return;

      const tokenPolicy = document.getElementById('modalTokenPolicy').value;
      const tokenLimitRaw = document.getElementById('modalTokenLimit').value;
      const meetingLimitRaw = document.getElementById('modalMeetingLimit').value;
      const blocked = document.getElementById('modalBlocked').checked;

      const payload = {
        userId: activeUserPolicy.userId,
        tokenPolicy,
        tokenLimit: tokenLimitRaw ? Number(tokenLimitRaw) : null,
        monthlyMeetingLimitOverride: meetingLimitRaw ? Number(meetingLimitRaw) : null,
      };

      const response = await fetch('/api/admin/users/policy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!response.ok) {
        const err = await response.json().catch(() => ({}));
        showToast('Failed to save policy: ' + (err.error || 'unknown_error'), 'error', 3600);
        return;
      }

      const blockResponse = await fetch('/api/admin/users/block-status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: activeUserPolicy.userId, blocked })
      });
      if (!blockResponse.ok) {
        showToast('Failed to update user status', 'error', 3200);
        return;
      }

      showToast('User policy saved successfully', 'success', 1700);
      setTimeout(() => location.reload(), 600);
    }`;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('users', 'Mela Control - Users', content, adminUser, script));
});

app.http.get('/admin/meetings', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const stats = loadBotAdminStats();
  const meetingRows = Object.values(stats.meetings)
    .map((m) => `<tr><td>${m.meetingName || 'Meeting'}</td><td>${m.meetingId}</td><td>${m.joinRequests}</td><td>${m.estimatedOutputTokens}</td><td>${m.estimatedTotalTokens}</td><td>$${m.estimatedCostUsd.toFixed(6)}</td><td>${m.users.length}</td></tr>`)
    .join('');
  const content = `
    <section class="hero"><h2>Meeting Usage</h2><p class="subtitle">Aggregated usage for meeting chats only.</p></section>
    <div class="table-wrap">
      <table>
        <thead><tr><th>Meeting Name</th><th>Meeting ID</th><th>Join Requests</th><th>Combined Sent Tokens</th><th>Estimated Total Tokens</th><th>Estimated Cost</th><th>Users</th></tr></thead>
        <tbody>${meetingRows || '<tr><td colspan="7">No meetings yet</td></tr>'}</tbody>
      </table>
    </div>`;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('meetings', 'Mela Control - Meetings', content, adminUser));
});

function summarizeMonthUsage(stats: BotAdminStats, monthKey: string) {
  const entries = Object.values(stats.dailyUsage || {}).filter((d) => (d.day || '').startsWith(monthKey));
  const hasDailyHistory = Object.keys(stats.dailyUsage || {}).length > 0;
  const summary = {
    messages: 0,
    meetingsJoined: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    costUsd: 0,
  };

  if (entries.length > 0) {
    for (const e of entries) {
      summary.messages += e.messages || 0;
      summary.meetingsJoined += e.meetingsJoined || 0;
      summary.inputTokens += e.inputTokens || 0;
      summary.outputTokens += e.outputTokens || 0;
      summary.totalTokens += e.totalTokens || 0;
      summary.costUsd += e.costUsd || 0;
    }
    return summary;
  }

  // If daily history exists, this month genuinely has no usage.
  if (hasDailyHistory) {
    return summary;
  }

  // Fallback for old data without daily history.
  return {
    messages: stats.totalMessages || 0,
    meetingsJoined: stats.totalMeetingsJoined || 0,
    inputTokens: stats.totalEstimatedInputTokens || 0,
    outputTokens: stats.totalEstimatedOutputTokens || 0,
    totalTokens: stats.totalEstimatedTokens || 0,
    costUsd: stats.totalEstimatedCostUsd || 0,
  };
}

function summarizeTenantMonthUsage(stats: BotAdminStats, monthKey: string, tenantId: string) {
  const summary = {
    messages: 0,
    meetingsJoined: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    costUsd: 0,
  };

  const targetTenant = normalizeTenantId(tenantId);
  const users = stats.users || {};
  const perUserDaily = stats.perUserDailyUsage || {};
  const hasAnyDailyHistory = Object.keys(stats.dailyUsage || {}).length > 0;

  for (const [userId, user] of Object.entries(users)) {
    if (normalizeTenantId(user.tenantId) !== targetTenant) continue;
    const dayMap = perUserDaily[userId] || {};
    let hasMonthData = false;
    for (const entry of Object.values(dayMap)) {
      if (!(entry.day || '').startsWith(monthKey)) continue;
      hasMonthData = true;
      summary.messages += entry.messages || 0;
      summary.meetingsJoined += entry.meetingsJoined || 0;
      summary.inputTokens += entry.inputTokens || 0;
      summary.outputTokens += entry.outputTokens || 0;
      summary.totalTokens += entry.totalTokens || 0;
      summary.costUsd += entry.costUsd || 0;
    }

    if (!hasMonthData && !hasAnyDailyHistory) {
      // Fallback for historical users before per-day usage tracking existed.
      summary.messages += user.totalMessages || 0;
      summary.inputTokens += user.estimatedInputTokens || 0;
      summary.outputTokens += user.estimatedOutputTokens || 0;
      summary.totalTokens += user.estimatedTotalTokens || 0;
      summary.costUsd += user.estimatedCostUsd || 0;
      summary.meetingsJoined += (user.monthlyMeetingsJoined?.[monthKey] || 0);
    }
  }

  return summary;
}

function summarizeTenantsForMonth(stats: BotAdminStats, monthKey: string, fallbackTenantId: string) {
  const rows = new Map<string, {
    tenantId: string;
    detectedUsers: number;
    activeUsers: number;
    messages: number;
    meetingsJoined: number;
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
    costUsd: number;
  }>();

  const users = stats.users || {};
  const perUserDaily = stats.perUserDailyUsage || {};
  const hasAnyDailyHistory = Object.keys(stats.dailyUsage || {}).length > 0;
  const defaultTenant = normalizeTenantId(fallbackTenantId);

  for (const [userId, user] of Object.entries(users)) {
    const tenantId = normalizeTenantId(user.tenantId || defaultTenant);
    if (!rows.has(tenantId)) {
      rows.set(tenantId, {
        tenantId,
        detectedUsers: 0,
        activeUsers: 0,
        messages: 0,
        meetingsJoined: 0,
        inputTokens: 0,
        outputTokens: 0,
        totalTokens: 0,
        costUsd: 0,
      });
    }

    const row = rows.get(tenantId)!;
    row.detectedUsers += 1;

    const dayMap = perUserDaily[userId] || {};
    let hasActivity = false;
    let hasMonthData = false;
    for (const entry of Object.values(dayMap)) {
      if (!(entry.day || '').startsWith(monthKey)) continue;
      hasMonthData = true;
      row.messages += entry.messages || 0;
      row.meetingsJoined += entry.meetingsJoined || 0;
      row.inputTokens += entry.inputTokens || 0;
      row.outputTokens += entry.outputTokens || 0;
      row.totalTokens += entry.totalTokens || 0;
      row.costUsd += entry.costUsd || 0;

      if ((entry.messages || 0) > 0 || (entry.totalTokens || 0) > 0 || (entry.meetingsJoined || 0) > 0) {
        hasActivity = true;
      }
    }

    if (!hasMonthData && !hasAnyDailyHistory) {
      // Fallback for historical users before per-day usage tracking existed.
      row.messages += user.totalMessages || 0;
      row.meetingsJoined += (user.monthlyMeetingsJoined?.[monthKey] || 0);
      row.inputTokens += user.estimatedInputTokens || 0;
      row.outputTokens += user.estimatedOutputTokens || 0;
      row.totalTokens += user.estimatedTotalTokens || 0;
      row.costUsd += user.estimatedCostUsd || 0;
    }

    if (!hasActivity) {
      // Fallback for users created before per-day entries existed.
      hasActivity = (user.totalMessages || 0) > 0 || (user.estimatedTotalTokens || 0) > 0 || (user.meetingJoinRequests || 0) > 0;
    }

    if (hasActivity) {
      row.activeUsers += 1;
    }
  }

  const tenantRows = Array.from(rows.values());
  if (tenantRows.length === 0) {
    const globalUsage = summarizeMonthUsage(stats, monthKey);
    tenantRows.push({
      tenantId: defaultTenant,
      detectedUsers: 0,
      activeUsers: 0,
      messages: globalUsage.messages,
      meetingsJoined: globalUsage.meetingsJoined,
      inputTokens: globalUsage.inputTokens,
      outputTokens: globalUsage.outputTokens,
      totalTokens: globalUsage.totalTokens,
      costUsd: globalUsage.costUsd,
    });
  }

  return tenantRows.sort((a, b) => b.costUsd - a.costUsd || b.totalTokens - a.totalTokens || a.tenantId.localeCompare(b.tenantId));
}

function summarizeInvoiceUserCounts(stats: BotAdminStats, monthKey: string): { activeUsers: number; detectedUsers: number } {
  const detectedUsers = Object.keys(stats.users || {}).length;
  const activeUserIds = new Set<string>();

  const perUserDaily = stats.perUserDailyUsage || {};
  for (const [userId, dayMap] of Object.entries(perUserDaily)) {
    const hasActivityInMonth = Object.values(dayMap || {}).some((d) => {
      if (!(d.day || '').startsWith(monthKey)) return false;
      return (d.messages || 0) > 0 || (d.totalTokens || 0) > 0 || (d.meetingsJoined || 0) > 0;
    });
    if (hasActivityInMonth) {
      activeUserIds.add(userId);
    }
  }

  // Fallback for pre-history records.
  if (activeUserIds.size === 0) {
    for (const [userId, u] of Object.entries(stats.users || {})) {
      if ((u.totalMessages || 0) > 0 || (u.estimatedTotalTokens || 0) > 0 || (u.meetingJoinRequests || 0) > 0) {
        activeUserIds.add(userId);
      }
    }
  }

  return {
    activeUsers: activeUserIds.size,
    detectedUsers,
  };
}

function buildTenantInvoiceHtml(tenantId: string, monthKey: string, usage: ReturnType<typeof summarizeMonthUsage>, activeUsers: number, generatedBy: string) {
  const invoiceNumber = `INV-${monthKey.replace('-', '')}-${tenantId.slice(0, 6).toUpperCase()}`;
  const generatedAt = new Date().toISOString();
  const subtotal = usage.costUsd;
  const tax = 0;
  const total = subtotal + tax;

  return `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Tenant Invoice ${invoiceNumber}</title>
  <style>
    body { font-family: "Segoe UI", Arial, sans-serif; margin: 0; padding: 24px; background: #f5f8ff; color: #1e2f4d; }
    .sheet { max-width: 960px; margin: 0 auto; background: #fff; border-radius: 14px; border: 1px solid #d6e1fb; overflow: hidden; box-shadow: 0 16px 38px rgba(20, 50, 120, 0.12); }
    .head { padding: 18px 22px; background: linear-gradient(135deg, #2F5597, #426cb4); color: #fff; display: flex; justify-content: space-between; align-items: start; }
    .head h1 { margin: 0; font-size: 22px; }
    .head p { margin: 6px 0 0; opacity: 0.95; }
    .meta { padding: 18px 22px; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; border-bottom: 1px solid #ebf0ff; }
    .card { border: 1px solid #e7edff; border-radius: 10px; padding: 10px; }
    .label { color: #60749a; font-size: 12px; text-transform: uppercase; letter-spacing: .4px; }
    .value { font-size: 15px; margin-top: 5px; font-weight: 600; }
    table { width: calc(100% - 44px); margin: 18px 22px 22px; border-collapse: collapse; }
    th, td { border-bottom: 1px solid #edf2ff; padding: 10px 8px; text-align: left; }
    th { background: #f1f5ff; font-size: 12px; text-transform: uppercase; color: #4f6493; }
    .totals { margin: 0 22px 22px; width: 320px; margin-left: auto; }
    .row { display: flex; justify-content: space-between; border-bottom: 1px dashed #dce5fb; padding: 8px 0; }
    .row.total { font-size: 18px; font-weight: 700; color: #2F5597; border-bottom: none; }
  </style>
</head>
<body>
  <div class="sheet">
    <div class="head">
      <div>
        <h1>Mela Control Invoice</h1>
        <p>Tenant usage billing statement</p>
      </div>
      <div>
        <div>Invoice #: <strong>${invoiceNumber}</strong></div>
        <div>Month: <strong>${monthKey}</strong></div>
      </div>
    </div>
    <div class="meta">
      <div class="card"><div class="label">Tenant</div><div class="value">${tenantId}</div></div>
      <div class="card"><div class="label">Generated By</div><div class="value">${generatedBy}</div></div>
      <div class="card"><div class="label">Generated At</div><div class="value">${generatedAt}</div></div>
      <div class="card"><div class="label">Active Users</div><div class="value">${activeUsers}</div></div>
    </div>
    <table>
      <thead><tr><th>Line Item</th><th>Quantity</th><th>Unit</th><th>Amount (USD)</th></tr></thead>
      <tbody>
        <tr><td>Input Tokens</td><td>${usage.inputTokens}</td><td>tokens</td><td>-</td></tr>
        <tr><td>Output Tokens</td><td>${usage.outputTokens}</td><td>tokens</td><td>-</td></tr>
        <tr><td>Total Tokens</td><td>${usage.totalTokens}</td><td>tokens</td><td>$${usage.costUsd.toFixed(6)}</td></tr>
        <tr><td>Messages Processed</td><td>${usage.messages}</td><td>messages</td><td>-</td></tr>
        <tr><td>Meetings Joined</td><td>${usage.meetingsJoined}</td><td>meetings</td><td>-</td></tr>
      </tbody>
    </table>
    <div class="totals">
      <div class="row"><span>Subtotal</span><span>$${subtotal.toFixed(6)}</span></div>
      <div class="row"><span>Tax</span><span>$${tax.toFixed(6)}</span></div>
      <div class="row total"><span>Total</span><span>$${total.toFixed(6)}</span></div>
    </div>
  </div>
</body>
</html>`;
}

function buildTenantInvoicePdfBuffer(
  tenantId: string,
  monthKey: string,
  usage: ReturnType<typeof summarizeMonthUsage>,
  activeUsers: number,
  generatedBy: string
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    try {
      const invoiceNumber = `INV-${monthKey.replace('-', '')}-${tenantId.slice(0, 6).toUpperCase()}`;
      const generatedAt = new Date().toISOString();
      const subtotal = usage.costUsd;
      const tax = 0;
      const total = subtotal + tax;

      const doc = new PDFDocument({ size: 'A4', margin: 42 });
      const chunks: Buffer[] = [];
      doc.on('data', (chunk: Buffer) => chunks.push(chunk));
      doc.on('end', () => resolve(Buffer.concat(chunks)));
      doc.on('error', reject);

      const pageWidth = doc.page.width;
      const contentWidth = pageWidth - 84;

      // Header
      doc.rect(42, 42, contentWidth, 88).fill('#2F5597');
      doc.fillColor('#FFFFFF').fontSize(22).font('Helvetica-Bold').text('Mela Control Invoice', 58, 62);
      doc.fontSize(11).font('Helvetica').text('Tenant usage billing statement', 58, 92);
      doc.fontSize(10).text(`Invoice #: ${invoiceNumber}`, 370, 62, { width: 220, align: 'right' });
      doc.fontSize(10).text(`Month: ${monthKey}`, 370, 78, { width: 220, align: 'right' });

      // Meta cards
      const cardY = 148;
      const cardW = (contentWidth - 16) / 2;
      const drawCard = (x: number, y: number, label: string, value: string) => {
        doc.roundedRect(x, y, cardW, 58, 6).lineWidth(1).strokeColor('#D6E1FB').stroke();
        doc.fillColor('#60749A').fontSize(9).font('Helvetica-Bold').text(label.toUpperCase(), x + 10, y + 9);
        doc.fillColor('#1E2F4D').fontSize(11).font('Helvetica').text(value, x + 10, y + 25, { width: cardW - 18 });
      };

      drawCard(42, cardY, 'Tenant', tenantId);
      drawCard(42 + cardW + 16, cardY, 'Generated By', generatedBy);
      drawCard(42, cardY + 70, 'Generated At', generatedAt);
      drawCard(42 + cardW + 16, cardY + 70, 'Active Users', String(activeUsers));

      // Table
      let y = cardY + 156;
      doc.roundedRect(42, y, contentWidth, 28, 4).fill('#F1F5FF');
      doc.fillColor('#4F6493').fontSize(9).font('Helvetica-Bold');
      doc.text('Line Item', 52, y + 9);
      doc.text('Quantity', 300, y + 9);
      doc.text('Unit', 390, y + 9);
      doc.text('Amount (USD)', 470, y + 9);

      const rows = [
        ['Input Tokens', String(usage.inputTokens), 'tokens', '-'],
        ['Output Tokens', String(usage.outputTokens), 'tokens', '-'],
        ['Total Tokens', String(usage.totalTokens), 'tokens', `$${usage.costUsd.toFixed(6)}`],
        ['Messages Processed', String(usage.messages), 'messages', '-'],
        ['Meetings Joined', String(usage.meetingsJoined), 'meetings', '-'],
      ];

      y += 30;
      doc.font('Helvetica').fontSize(10);
      for (const row of rows) {
        doc.strokeColor('#EDF2FF').lineWidth(1).moveTo(42, y + 20).lineTo(42 + contentWidth, y + 20).stroke();
        doc.fillColor('#1E2F4D').text(row[0], 52, y + 6, { width: 220 });
        doc.text(row[1], 300, y + 6, { width: 70 });
        doc.text(row[2], 390, y + 6, { width: 70 });
        doc.text(row[3], 470, y + 6, { width: 120 });
        y += 24;
      }

      // Totals
      const totalsX = 360;
      const totalsW = contentWidth - (totalsX - 42);
      y += 16;
      doc.roundedRect(totalsX, y, totalsW, 74, 6).lineWidth(1).strokeColor('#DCE5FB').stroke();
      doc.fillColor('#1E2F4D').font('Helvetica').fontSize(10);
      doc.text('Subtotal', totalsX + 12, y + 12);
      doc.text(`$${subtotal.toFixed(6)}`, totalsX + totalsW - 130, y + 12, { width: 118, align: 'right' });
      doc.text('Tax', totalsX + 12, y + 30);
      doc.text(`$${tax.toFixed(6)}`, totalsX + totalsW - 130, y + 30, { width: 118, align: 'right' });
      doc.font('Helvetica-Bold').fillColor('#2F5597').fontSize(12);
      doc.text('Total', totalsX + 12, y + 50);
      doc.text(`$${total.toFixed(6)}`, totalsX + totalsW - 130, y + 50, { width: 118, align: 'right' });

      doc.end();
    } catch (e) {
      reject(e);
    }
  });
}

app.http.get('/admin/invoices', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const stats = loadBotAdminStats();

  const currentMonth = getMonthKey();
  const month = ((req?.query?.month as string) || currentMonth).slice(0, 7);
  const tenantId = normalizeTenantId(getConfiguredTenantId() || adminUser.tenantId || 'unknown-tenant');
  const tenantRows = summarizeTenantsForMonth(stats, month, tenantId)
    .map((row) => {
      const safeTenant = row.tenantId.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
      return `<tr>
        <td>${safeTenant}</td>
        <td>${row.activeUsers}</td>
        <td>${row.detectedUsers}</td>
        <td>${row.messages}</td>
        <td>${row.meetingsJoined}</td>
        <td>${row.totalTokens}</td>
        <td>$${row.costUsd.toFixed(6)}</td>
        <td><button onclick="downloadTenantInvoice('${safeTenant}')">Download PDF</button></td>
      </tr>`;
    })
    .join('');

  const content = `
    <section class="hero"><h2>Tenant Invoice</h2><p class="subtitle">Generate and download usage invoices by month and tenant.</p></section>
    <section class="controls">
      <div class="control">
        <label>Invoice Month</label>
        <input id="invoiceMonth" type="month" value="${month}" />
        <button onclick="reloadInvoiceMonth()">Load Month</button>
      </div>
      <div class="control">
        <label>Filter Tenants</label>
        <input id="tenantFilter" type="text" placeholder="Search tenant id..." oninput="applyTenantFilter()" />
        <label style="display:flex;gap:8px;align-items:center;margin-top:4px;">
          <input id="activeOnlyFilter" type="checkbox" style="width:auto;margin:0;" onchange="applyTenantFilter()" />
          Show active tenants only
        </label>
      </div>
    </section>

    <div class="table-wrap" style="margin-top:14px;">
      <table>
        <thead><tr><th>Tenant</th><th>Active Users</th><th>Detected Users</th><th>Messages</th><th>Meetings</th><th>Total Tokens</th><th>Cost (USD)</th><th>Download</th></tr></thead>
        <tbody id="tenantInvoiceTableBody">${tenantRows || '<tr><td colspan="8">No tenant usage for selected month</td></tr>'}</tbody>
      </table>
    </div>`;

  const script = `
    function reloadInvoiceMonth() {
      const month = document.getElementById('invoiceMonth').value;
      const query = month ? ('?month=' + encodeURIComponent(month)) : '';
      window.location.href = '/admin/invoices' + query;
    }
    function downloadTenantInvoice(tenantId) {
      const month = document.getElementById('invoiceMonth').value;
      const query = new URLSearchParams();
      if (month) query.set('month', month);
      if (tenantId) query.set('tenantId', tenantId);
      window.location.href = '/api/admin/invoices/download.pdf?' + query.toString();
    }
    function applyTenantFilter() {
      const term = (document.getElementById('tenantFilter').value || '').trim().toLowerCase();
      const activeOnly = document.getElementById('activeOnlyFilter').checked;
      const body = document.getElementById('tenantInvoiceTableBody');
      if (!body) return;

      const rows = Array.from(body.querySelectorAll('tr'));
      for (const row of rows) {
        const cols = row.querySelectorAll('td');
        if (!cols || cols.length < 2) continue;

        const tenantText = (cols[0].textContent || '').toLowerCase();
        const activeUsers = Number((cols[1].textContent || '0').trim()) || 0;
        const matchesTerm = !term || tenantText.includes(term);
        const matchesActive = !activeOnly || activeUsers > 0;
        row.style.display = matchesTerm && matchesActive ? '' : 'none';
      }
    }`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('invoices', 'Mela Control - Invoices', content, adminUser, script));
});

app.http.get('/api/admin/invoices/download', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const adminUser = await getCurrentAdminUserProfile(adminSession);
    const stats = loadBotAdminStats();
    const currentMonth = getMonthKey();
    const month = ((req?.query?.month as string) || currentMonth).slice(0, 7);
    const defaultTenantId = normalizeTenantId(getConfiguredTenantId() || adminUser.tenantId || 'unknown-tenant');
    const selectedTenantId = normalizeTenantId((req?.query?.tenantId as string) || defaultTenantId);
    const usage = summarizeTenantMonthUsage(stats, month, selectedTenantId);
    const tenantSummary = summarizeTenantsForMonth(stats, month, defaultTenantId).find((t) => t.tenantId === selectedTenantId);
    const activeUsers = tenantSummary?.activeUsers || 0;
    const html = buildTenantInvoiceHtml(selectedTenantId, month, usage, activeUsers, adminUser.displayName || 'Admin');

    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="tenant-invoice-${selectedTenantId}-${month}.html"`);
    res.status(200).send(html);
  } catch (error) {
    logAdminError('server', '/api/admin/invoices/download', error);
    console.error('[ADMIN_INVOICE_DOWNLOAD] Failed:', error);
    res.status(500).json({ error: 'failed_to_generate_invoice' });
  }
});

app.http.get('/api/admin/invoices/download.pdf', async (req: any, res: any) => {
  try {
    const adminSession = await requireAdminAuth(req, res, true);
    if (!adminSession) return;
    const adminUser = await getCurrentAdminUserProfile(adminSession);
    const stats = loadBotAdminStats();
    const currentMonth = getMonthKey();
    const month = ((req?.query?.month as string) || currentMonth).slice(0, 7);
    const defaultTenantId = normalizeTenantId(getConfiguredTenantId() || adminUser.tenantId || 'unknown-tenant');
    const selectedTenantId = normalizeTenantId((req?.query?.tenantId as string) || defaultTenantId);
    const usage = summarizeTenantMonthUsage(stats, month, selectedTenantId);
    const tenantSummary = summarizeTenantsForMonth(stats, month, defaultTenantId).find((t) => t.tenantId === selectedTenantId);
    const activeUsers = tenantSummary?.activeUsers || 0;

    const pdfBuffer = await buildTenantInvoicePdfBuffer(selectedTenantId, month, usage, activeUsers, adminUser.displayName || 'Admin');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="tenant-invoice-${selectedTenantId}-${month}.pdf"`);
    res.status(200).send(pdfBuffer);
  } catch (error) {
    logAdminError('server', '/api/admin/invoices/download.pdf', error);
    console.error('[ADMIN_INVOICE_PDF_DOWNLOAD] Failed:', error);
    res.status(500).json({ error: 'failed_to_generate_invoice_pdf' });
  }
});

app.http.get('/admin/settings', async (req: any, res: any) => {
  const adminSession = await requireAdminAuth(req, res, false);
  if (!adminSession) return;
  const adminUser = await getCurrentAdminUserProfile(adminSession);
  const stats = loadBotAdminStats();
  const content = `
    <section class="hero"><h2>Settings</h2><p class="subtitle">Plan and capacity controls.</p></section>
    <section class="controls">
      <div class="control">
        <label>Global Limits Enforcement</label>
        <label style="display:flex;gap:8px;align-items:center;">
          <input id="enforceGlobalLimitsInput" type="checkbox" style="width:auto;margin:0;" ${stats.enforceGlobalLimits ? 'checked' : ''} />
          Enable capacity and monthly meeting limits
        </label>
        <button onclick="updateEnforceGlobalLimits()">Save Enforcement</button>
        <div class="muted">When disabled, users have unlimited access unless explicitly blocked or given a limited token policy.</div>
      </div>
      <div class="control">
        <label>Free-tier monthly meetings</label>
        <input id="freeTierLimitInput" type="number" min="1" max="1000" value="${stats.freeTierMonthlyMeetingLimit}" />
        <button onclick="updateFreeTierLimit()">Save Free-tier Limit</button>
        <div class="muted">Applies to new join attempts immediately.</div>
      </div>
      <div class="control">
        <label>Max active users</label>
        <input id="maxUsersInput" type="number" min="1" max="50000" value="${stats.maxUsers}" />
        <button onclick="updateMaxUsers()">Save Max Users</button>
        <div class="muted">Controls first-time access capacity.</div>
      </div>
    </section>`;
  const script = `
    async function updateEnforceGlobalLimits() {
      const enabled = document.getElementById('enforceGlobalLimitsInput').checked;
      const response = await fetch('/api/admin/config/enforce-global-limits', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled })
      });
      if (!response.ok) {
        showToast('Failed to update enforcement setting', 'error', 3200);
        return;
      }
      showToast('Enforcement setting updated', 'success', 1700);
      setTimeout(() => location.reload(), 500);
    }
    async function updateFreeTierLimit() {
      const limit = Number(document.getElementById('freeTierLimitInput').value);
      const response = await fetch('/api/admin/config/free-tier-limit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ limit })
      });
      if (!response.ok) {
        showToast('Failed to update free-tier limit', 'error', 3200);
        return;
      }
      showToast('Free-tier limit updated', 'success', 1700);
      setTimeout(() => location.reload(), 500);
    }
    async function updateMaxUsers() {
      const maxUsers = Number(document.getElementById('maxUsersInput').value);
      const response = await fetch('/api/admin/config/max-users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ maxUsers })
      });
      if (!response.ok) {
        showToast('Failed to update max users', 'error', 3200);
        return;
      }
      showToast('Max users updated', 'success', 1700);
      setTimeout(() => location.reload(), 500);
    }`;
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.status(200).send(renderAdminLayout('settings', 'Mela Control - Settings', content, adminUser, script));
});

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
          console.log(`[CALLS_WEBHOOK] Duplicate established event for callId=${callId} — ignoring`);
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
          // Clear stale data from previous calls on this conversation
          // (recurring meetings reuse the same conversationId across sessions)
          liveTranscriptMap.set(callEntry.conversationId, []);
          lastBotResponseMap.delete(callEntry.conversationId);
          // Auto-start transcription with retries (Teams can reject very early requests)
          void attemptBotStartTranscription(callId, callEntry.conversationId, callEntry.serviceUrl);
          
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
            `I'm now live in the meeting and setting up transcription. Just ask me to **summarize**, **transcribe**, or generate **minutes** whenever you're ready!`
          );
        }
      } else if (callState === 'terminated') {
        const callEntry = activeCallMap.get(callId);
        const conversationId = callToConversationMap.get(callId);
        stopLiveTranscriptPolling(callId);
        
        // Capture call timing before we delete the entry
        const callStartedAt = callEntry?.establishedAt || Date.now();
        const callEndedAt = Date.now();
        
        activeCallMap.delete(callId);
        callToConversationMap.delete(callId);
        console.log(`[CALLS_WEBHOOK] Call TERMINATED - bot left the meeting (duration: ${(callEndedAt - callStartedAt) / 1000}s)`);

        // Save any live transcript data captured during the call BEFORE cleaning up sessions
        if (conversationId) {
          const liveEntries = liveTranscriptMap.get(conversationId)?.filter(e => e.isFinal) || [];
          if (liveEntries.length > 0) {
            console.log(`[CALLS_WEBHOOK] Saving ${liveEntries.length} live transcript entries before cleanup`);
            saveTranscriptToFile(conversationId);
          }
        }

        // End the live session (clears pinnedTranscriptPaths so next call gets a fresh file)
        if (conversationId) {
          endLiveTranscriptSession(conversationId, callId);
        }

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

          // --- IMMEDIATE: Use live transcript data if available ---
          const liveData = liveTranscriptMap.get(convId)?.filter(e => e.isFinal) || [];
          if (liveData.length > 0) {
            console.log(`[POST_MEETING] Using ${liveData.length} live transcript entries for immediate summary`);
            try {
              // Always generate a FRESH summary from the current call's transcript
              // (never reuse a cached summary — it may be from a previous call session)
              const autoMeetingTitle = await resolveDisplayMeetingTitle(convId, orgId, getCachedMeetingContext(convId)?.subject);
              const summary = await generateFormattedSummaryHtml(liveData, autoMeetingTitle, 'Participant', [], new Date(callStartedAt));

              const emailResult = await autoEmailSummaryToParticipants(
                convId, orgId,
                `Meeting Summary from ${config.botDisplayName}`,
                summary
              );

              const emailNote = emailResult.sentCount > 0
                ? `\n\n---\n*I've also emailed this summary to **${emailResult.sentCount} participant(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.*`
                : '';

              await graphApiHelper.sendProactiveMessage(
                svcUrl, convId,
                `**Meeting ended!** Here's your AI-generated summary:\n\n${summary}${emailNote}`
              );

              if (emailResult.sentCount > 0) {
                console.log(`[POST_MEETING_EMAIL] Emailed summary to ${emailResult.sentCount} participant(s)`);
              }
            } catch (summaryErr) {
              console.error(`[POST_MEETING_SUMMARY_ERROR] Failed to generate immediate summary:`, summaryErr);
            }
          } else {
            // --- FALLBACK: No live data — retry via Graph API with delays ---
            console.log(`[POST_MEETING_TRANSCRIPT] No live transcript data — will retry via Graph API for organizer=${orgId}`);

            const attemptFetch = async (attempt: number, maxAttempts: number) => {
              try {
                console.log(`[POST_MEETING_TRANSCRIPT] Attempt ${attempt}/${maxAttempts}...`);
                const postMeetingFetch = await fetchTranscriptCacheFirst(
                  orgId, webUrl, callStartedAt, undefined
                );
                if (postMeetingFetch.entries.length > 0) {
                  const parsed = postMeetingFetch.entries;
                  console.log(`[POST_MEETING_TRANSCRIPT] Got ${parsed.length} entries (fromCache=${postMeetingFetch.fromCache})`);
                  liveTranscriptMap.set(convId, parsed);
                  saveTranscriptToFile(convId);

                  // Always generate fresh summary from the current transcript
                  const autoMeetingTitle = await resolveDisplayMeetingTitle(convId, orgId, getCachedMeetingContext(convId)?.subject);
                  const summary = await generateFormattedSummaryHtml(parsed, autoMeetingTitle, 'Participant', [], new Date(callStartedAt));

                  const emailResult = await autoEmailSummaryToParticipants(
                    convId, orgId,
                    `Meeting Summary from ${config.botDisplayName}`,
                    summary
                  );

                  const emailNote = emailResult.sentCount > 0
                    ? `\n\n---\n*I've also emailed this summary to **${emailResult.sentCount} participant(s)**${emailResult.failedCount > 0 ? ` (${emailResult.failedCount} failed)` : ''}.*`
                    : '';

                  await graphApiHelper.sendProactiveMessage(
                    svcUrl, convId,
                    `**Meeting ended!** Here's your AI-generated summary:\n\n${summary}${emailNote}`
                  );

                  if (emailResult.sentCount > 0) {
                    console.log(`[POST_MEETING_EMAIL] Emailed summary to ${emailResult.sentCount} participant(s)`);
                  }
                  return; // success
                } else {
                  console.log(`[POST_MEETING_TRANSCRIPT] No transcript available yet`);
                }
                if (attempt < maxAttempts) {
                  const delayMs = Math.min(attempt * 60_000, 180_000);
                  console.log(`[POST_MEETING_TRANSCRIPT] Retrying in ${delayMs / 1000}s...`);
                  setTimeout(() => attemptFetch(attempt + 1, maxAttempts), delayMs);
                } else {
                  console.log(`[POST_MEETING_TRANSCRIPT] All ${maxAttempts} attempts exhausted — transcript not available`);
                  await graphApiHelper.sendProactiveMessage(
                    svcUrl, convId,
                    `I couldn't retrieve a meeting transcript. Teams may not have generated one — make sure transcription or recording was active during the meeting.`
                  );
                }
              } catch (err) {
                console.error(`[POST_MEETING_TRANSCRIPT_ERROR] Attempt ${attempt}:`, err);
                if (attempt < maxAttempts) {
                  setTimeout(() => attemptFetch(attempt + 1, maxAttempts), 60_000);
                }
              }
            };

            // First attempt after 30s (Graph needs time to finalize), then up to 7 attempts
            setTimeout(() => attemptFetch(1, 7), 30_000);
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
              `I couldn't join the meeting call — it doesn't seem to be active right now. Please start the meeting and ask me to join again.`
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

// Handle Graph subscription notifications for transcript changes
app.http.post('/api/transcriptNotifications', async (req: any, res: any) => {
  try {
    const body = req.body;
    
    // Handle subscription validation request from Graph
    const validationToken = (req.query?.validationToken || req.url?.match(/[?&]validationToken=([^&]+)/)?.[1]);
    if (validationToken) {
      console.log(`[TRANSCRIPT_SUB] Subscription validation received`);
      res.status(200).send(decodeURIComponent(validationToken));
      return;
    }

    // Acknowledge immediately
    res.status(200).json({});

    const notifications = body?.value || [];
    for (const notification of notifications) {
      const resource = notification.resource || '';
      const callId = notification.clientState || '';
      console.log(`[TRANSCRIPT_SUB] Notification received: resource=${resource}, changeType=${notification.changeType}, callId=${callId}`);
      
      // Extract chatId from resource path: /chats/{chatId}/transcripts/{transcriptId}
      const chatMatch = resource.match(/chats\/([^/]+)\/transcripts/);
      const chatId = chatMatch ? decodeURIComponent(chatMatch[1]) : null;
      
      if (chatId) {
        console.log(`[TRANSCRIPT_SUB] Transcript created in chat ${chatId} — triggering immediate poll`);
        
        // Find the polling state for this call and trigger an immediate poll
        if (callId) {
          const polling = liveTranscriptPollingMap.get(callId);
          if (polling) {
            polling.consecutiveEmptyPolls = 0;
            void pollLiveTranscript(polling);
          }
        }
      }
    }
  } catch (error) {
    console.error(`[TRANSCRIPT_SUB_ERROR] Failed to process notification:`, error);
  }
});

export { activeCallMap };
export default app;