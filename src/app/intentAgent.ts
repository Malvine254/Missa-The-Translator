/**
 * Intent Agent - Smart Routing and Decision Making
 * 
 * This module provides an intelligent agent that:
 * 1. Maintains rich conversation context across turns
 * 2. Uses chain-of-thought reasoning BEFORE taking any action
 * 3. Understands tool capabilities and validates requirements
 * 4. Can ask clarifying questions when intent is ambiguous
 * 5. Handles multi-step requests and follow-ups naturally
 * 6. Never acts without clear understanding of user intent
 * 
 * CORE PRINCIPLE: Think deeply, then act precisely.
 * 
 * @module intentAgent
 */

import { ChatPrompt } from '@microsoft/teams.ai';
import { OpenAIChatModel } from '@microsoft/teams.openai';
import config from '../config';

// ============================================================================
// TYPES & INTERFACES
// ============================================================================

export type IntentLabel =
  | 'join_meeting'
  | 'summarize'
  | 'minutes'
  | 'transcribe'
  | 'meeting_overview'
  | 'list_meeting_groups'
  | 'read_chats'
  | 'insights'
  | 'meeting_question'
  | 'check_inbox'
  | 'reply_email'
  | 'send_email'
  | 'profile_details'
  | 'check_planner_tasks'
  | 'prepare_meeting'
  | 'check_calendar'
  | 'list_attendees'
  | 'general_chat'
  | 'clarification_needed';

export interface ConversationState {
  /** Last few user messages for context */
  recentUserMessages: Array<{ text: string; timestamp: number }>;
  /** Last few bot responses for context */
  recentBotResponses: Array<{ text: string; contentType?: string; timestamp: number }>;
  /** Current topic being discussed */
  activeTopic: string | null;
  /** Any pending action that needs confirmation */
  pendingAction: PendingAction | null;
  /** Last detected intent */
  lastIntent: IntentLabel | null;
  /** Entities mentioned in conversation */
  entities: ConversationEntities;
  /** User preferences learned */
  userPreferences: UserPreferences;
  /** Is this a meeting chat context */
  isMeetingContext: boolean;
  /** Meeting info if available */
  meetingInfo: MeetingContext | null;
}

export interface PendingAction {
  action: IntentLabel;
  description: string;
  requiredInfo: string[];
  gatheredInfo: Record<string, any>;
  createdAt: number;
}

export interface ConversationEntities {
  /** Recently mentioned people */
  people: Array<{ name: string; email?: string; role?: string; mentionedAt: number }>;
  /** Recently mentioned meetings */
  meetings: Array<{ subject: string; date?: string; mentionedAt: number }>;
  /** Recently mentioned emails */
  emails: Array<{ from?: string; subject?: string; mentionedAt: number }>;
  /** Other entities (dates, times, topics) */
  other: Array<{ type: string; value: string; mentionedAt: number }>;
}

export interface UserPreferences {
  /** Preferred summary style */
  summaryStyle?: 'brief' | 'detailed' | 'bullet_points';
  /** Preferred email format */
  emailFormat?: 'formal' | 'casual';
  /** Auto-send preferences */
  autoSend?: boolean;
}

export interface MeetingContext {
  subject?: string;
  organizerId?: string;
  joinWebUrl?: string;
  startTime?: string;
  endTime?: string;
  attendees?: Array<{ name: string; email: string }>;
  hasActiveCall?: boolean;
  hasTranscript?: boolean;
}

export interface AgentDecision {
  /** The primary action to take */
  intent: IntentLabel;
  /** Confidence level */
  confidence: 'high' | 'medium' | 'low';
  /** Chain of thought reasoning */
  reasoning: string;
  /** Should we ask for clarification instead of acting? */
  needsClarification: boolean;
  /** Clarification question if needed */
  clarificationQuestion?: string;
  /** Enriched/refined query after context resolution */
  refinedQuery: string;
  /** Extracted parameters for the action */
  parameters: ActionParameters;
  /** Alternative interpretations considered */
  alternatives?: Array<{ intent: IntentLabel; reasoning: string }>;
  /** Multi-step plan if this is a complex request */
  plan?: ActionPlan;
}

export interface ActionParameters {
  /** Target meeting (current, last, specific) */
  meetingTarget?: 'current' | 'last' | 'specific';
  /** Specific meeting subject if mentioned */
  meetingSubject?: string;
  /** Date reference */
  dateReference?: string;
  /** Recipients for email actions */
  recipients?: Array<{ name?: string; email?: string; type: 'specific' | 'self' | 'all_attendees' }>;
  /** Content type being requested */
  contentType?: 'summary' | 'minutes' | 'transcript' | 'email' | 'insights' | 'custom';
  /** Custom content description */
  customContent?: string;
  /** Person mentioned */
  personReference?: string;
  /** Email subject mentioned */
  emailSubject?: string;
  /** Time reference */
  timeReference?: string;
  /** Format style for follow-up requests (shorter, longer, bullets, etc.) */
  formatStyle?: 'shorter' | 'longer' | 'bullets' | 'detailed' | 'brief';
  /** True if this is a reformat request for the last shown content */
  isReformatRequest?: boolean;
}

export interface ActionPlan {
  steps: Array<{
    order: number;
    action: IntentLabel;
    description: string;
    dependsOn?: number;
  }>;
  estimatedSteps: number;
}

export interface AgentContext {
  userId: string;
  userName: string;
  conversationId: string;
  isMeetingConversation: boolean;
  meetingInfo?: MeetingContext;
  inboxContext?: {
    lastSender?: string;
    lastSubject?: string;
    /** All senders shown in the last inbox check */
    recentSenders?: string[];
    /** True if the bot just showed an inbox listing */
    justShowedInbox?: boolean;
  };
  lastBotResponse?: { contentType?: string; content?: string; subject?: string; timestamp?: number };
  /** If bot just asked a clarification question, what topic/person was it about? */
  pendingClarification?: {
    question: string;
    aboutPerson?: string;
    aboutTopic?: string;
  };
}

// ============================================================================
// CONVERSATION STATE MANAGEMENT
// ============================================================================

const conversationStates = new Map<string, ConversationState>();
const STATE_TTL = 60 * 60 * 1000; // 1 hour

export function getConversationState(conversationId: string): ConversationState {
  let state = conversationStates.get(conversationId);
  if (!state) {
    state = createEmptyState();
    conversationStates.set(conversationId, state);
  }
  return state;
}

export function updateConversationState(conversationId: string, updates: Partial<ConversationState>): void {
  const state = getConversationState(conversationId);
  Object.assign(state, updates);
  conversationStates.set(conversationId, state);
}

function createEmptyState(): ConversationState {
  return {
    recentUserMessages: [],
    recentBotResponses: [],
    activeTopic: null,
    pendingAction: null,
    lastIntent: null,
    entities: { people: [], meetings: [], emails: [], other: [] },
    userPreferences: {},
    isMeetingContext: false,
    meetingInfo: null,
  };
}

export function recordUserMessage(conversationId: string, text: string): void {
  const state = getConversationState(conversationId);
  state.recentUserMessages.push({ text, timestamp: Date.now() });
  // Keep only last 10 messages
  if (state.recentUserMessages.length > 10) {
    state.recentUserMessages = state.recentUserMessages.slice(-10);
  }
  conversationStates.set(conversationId, state);
}

export function recordBotResponse(conversationId: string, text: string, contentType?: string): void {
  const state = getConversationState(conversationId);
  state.recentBotResponses.push({ text, contentType, timestamp: Date.now() });
  // Keep only last 10 responses
  if (state.recentBotResponses.length > 10) {
    state.recentBotResponses = state.recentBotResponses.slice(-10);
  }
  conversationStates.set(conversationId, state);
}

function cleanupOldStates(): void {
  const now = Date.now();
  for (const [id, state] of conversationStates.entries()) {
    const lastActivity = Math.max(
      ...state.recentUserMessages.map(m => m.timestamp),
      ...state.recentBotResponses.map(m => m.timestamp),
      0
    );
    if (now - lastActivity > STATE_TTL) {
      conversationStates.delete(id);
    }
  }
}

// Cleanup every 30 minutes
setInterval(cleanupOldStates, 30 * 60 * 1000);

// ============================================================================
// TOOL CAPABILITY AWARENESS
// ============================================================================

const TOOL_CAPABILITIES = {
  join_meeting: {
    description: 'Join an active Teams meeting call to capture live transcription',
    requirements: ['Active meeting call in progress'],
    produces: ['Live transcript capture', 'Meeting presence'],
  },
  summarize: {
    description: 'Generate an AI summary of meeting content',
    requirements: ['Transcript available (live or fetched)'],
    produces: ['Summary text', 'Key points', 'Discussion overview'],
  },
  minutes: {
    description: 'Generate formal meeting minutes with action items',
    requirements: ['Transcript available'],
    produces: ['Formal meeting notes', 'Action items', 'Decisions made', 'Attendee list'],
  },
  transcribe: {
    description: 'Fetch and display the meeting transcript',
    requirements: ['Meeting recording/transcript exists in Teams'],
    produces: ['Full transcript text', 'Speaker identification'],
  },
  meeting_overview: {
    description: 'Provide details about a specific meeting',
    requirements: ['Meeting exists in calendar or history'],
    produces: ['Meeting details', 'Attendees', 'Schedule info'],
  },
  list_meeting_groups: {
    description: 'List meeting groups/chats the user is a member of',
    requirements: ['Chat membership visibility'],
    produces: ['Group and meeting chat list', 'Conversation scope context'],
  },
  read_chats: {
    description: 'Read recent messages in the current meeting/channel chat',
    requirements: ['Chat history access'],
    produces: ['Recent chat message list', 'Chat context'],
  },
  insights: {
    description: 'Extract key insights and takeaways from meeting content',
    requirements: ['Transcript available'],
    produces: ['Key takeaways', 'Important decisions', 'Follow-up items'],
  },
  meeting_question: {
    description: 'Answer a specific question about meeting content',
    requirements: ['Transcript available'],
    produces: ['Specific answer based on transcript content'],
  },
  check_inbox: {
    description: 'Check and display emails from inbox',
    requirements: ['Email access permissions'],
    produces: ['Email list', 'Email content preview'],
  },
  reply_email: {
    description: 'Fetch an email, read its content, and auto-draft a contextual reply',
    requirements: ['Sender name or email context identified - bot reads and drafts automatically'],
    produces: ['Email draft based on email content'],
  },
  send_email: {
    description: 'Send content via email to recipients',
    requirements: ['Content to send', 'Recipient(s) identified'],
    produces: ['Email sent confirmation'],
  },
  profile_details: {
    description: 'Show user profile information',
    requirements: ['User identity'],
    produces: ['Profile details', 'Contact information'],
  },
  prepare_meeting: {
    description: 'Help user prepare for an upcoming meeting by fetching calendar details, attendees, and giving prep advice',
    requirements: ['Calendar access', 'Meeting scheduled'],
    produces: ['Meeting prep tips', 'Attendee list', 'Logistics info', 'Agenda context'],
  },
  check_calendar: {
    description: 'Check calendar and schedule information',
    requirements: ['Calendar access'],
    produces: ['Schedule overview', 'Meeting list', 'Availability'],
  },
  check_planner_tasks: {
    description: 'Check Planner tasks and prioritize them',
    requirements: ['Planner task access permissions'],
    produces: ['Task list', 'Due-date grouping', 'Priority ordering'],
  },
  list_attendees: {
    description: 'List meeting attendees and their contact information',
    requirements: ['Meeting identified'],
    produces: ['Attendee list with names and emails'],
  },
} as const;

// ============================================================================
// INTENT AGENT - MAIN DECISION ENGINE
// ============================================================================

export class IntentAgent {
  private sendPrompt: (prompt: ChatPrompt, query: string, tracking?: any) => Promise<any>;
  
  constructor(promptSender: (prompt: ChatPrompt, query: string, tracking?: any) => Promise<any>) {
    this.sendPrompt = promptSender;
  }

  /**
   * Main entry point - Analyze user message and make a smart decision
   */
  async analyze(
    userMessage: string,
    context: AgentContext,
    tracking?: { userId: string; displayName: string; meetingId: string }
  ): Promise<AgentDecision> {
    const trimmedMessage = (userMessage || '').trim();
    if (!trimmedMessage) {
      return this.createDefaultDecision('general_chat', trimmedMessage, 'Empty message');
    }

    // Deterministic guardrail for short follow-ups so we don't lose context to generic chat.
    const deterministicDecision = this.tryDeterministicFollowupDecision(trimmedMessage, context);
    if (deterministicDecision) {
      return deterministicDecision;
    }

    // Get conversation state
    const state = getConversationState(context.conversationId);
    
    // Record this message
    recordUserMessage(context.conversationId, trimmedMessage);

    // Build comprehensive context for reasoning
    const contextSummary = this.buildContextSummary(state, context);
    
    // Use chain-of-thought reasoning to analyze the request
    const decision = await this.reasonAboutRequest(trimmedMessage, contextSummary, context, tracking);
    
    // Update state with decision
    state.lastIntent = decision.intent;
    if (decision.parameters.meetingSubject) {
      state.activeTopic = decision.parameters.meetingSubject;
    }
    conversationStates.set(context.conversationId, state);
    
    return decision;
  }

  private tryDeterministicFollowupDecision(message: string, context: AgentContext): AgentDecision | null {
    const text = (message || '').trim();
    if (!text) return null;

    // Keyword fast-path: "prepare/preparing" + meeting keyword → always prepare_meeting
    const lower = text.toLowerCase();
    if (/\bprepar(e|ing)\b/.test(lower) && /\bmeeting|call|standup|sync|session\b/.test(lower)) {
      console.log(`[DECISION_LOG] Fast-path match: "prepare + meeting" → prepare_meeting`);
      return {
        intent: 'prepare_meeting',
        confidence: 'high',
        reasoning: 'Keyword fast-path: message contains "prepare/preparing" with a meeting reference.',
        needsClarification: false,
        refinedQuery: text,
        parameters: {},
      };
    }

    const last = context.lastBotResponse;
    if (!last?.contentType || !last?.timestamp) return null;

    const isRecent = Date.now() - last.timestamp < 20 * 60 * 1000;
    if (!isRecent) return null;

    const isLikelyFollowup =
      lower.length <= 140 &&
      /(which|priority|prioritize|what should i|what next|what about|tell me more|who said|what did|focus|details|that|it|this)/i.test(lower);
    if (!isLikelyFollowup) return null;

    if (last.contentType === 'inbox_email') {
      return {
        intent: 'check_inbox',
        confidence: 'high',
        reasoning: 'Deterministic follow-up: recent inbox context detected, route to cached inbox Q&A.',
        needsClarification: false,
        refinedQuery: `Using the previously shown inbox emails, answer this follow-up: ${text}`,
        parameters: {
          isReformatRequest: true,
        },
      };
    }

    if (last.contentType === 'planner_tasks') {
      return {
        intent: 'check_planner_tasks',
        confidence: 'high',
        reasoning: 'Deterministic follow-up: recent planner task context detected, route to planner task handler.',
        needsClarification: false,
        refinedQuery: `Using the previously shown planner tasks, answer this follow-up: ${text}`,
        parameters: {
          isReformatRequest: true,
        },
      };
    }

    if (['summary', 'minutes', 'transcript', 'insights', 'meeting_overview'].includes(last.contentType)) {
      const isFormatOnlyFollowup = /\b(shorter|longer|bullet|bullets|reformat|rewrite|format|concise|brief|detailed|expand|compress)\b/i.test(lower);
      return {
        intent: isFormatOnlyFollowup ? 'summarize' : 'meeting_question',
        confidence: 'high',
        reasoning: 'Deterministic follow-up: recent meeting-content context detected, keep routing inside meeting context.',
        needsClarification: false,
        refinedQuery: `Using the previously shown meeting content, answer this follow-up: ${text}`,
        parameters: {
          isReformatRequest: true,
        },
      };
    }

    if (last.contentType === 'meeting_groups') {
      return {
        intent: 'list_meeting_groups',
        confidence: 'high',
        reasoning: 'Deterministic follow-up: recent meeting groups context detected, keep group listing scope.',
        needsClarification: false,
        refinedQuery: `Using the previously shown meeting groups, handle this follow-up: ${text}`,
        parameters: {
          isReformatRequest: true,
        },
      };
    }

    return null;
  }

  private buildContextSummary(state: ConversationState, context: AgentContext): string {
    const parts: string[] = [];
    
    // Recent conversation
    if (state.recentUserMessages.length > 0 || state.recentBotResponses.length > 0) {
      const recentTurns: string[] = [];
      const allMessages = [
        ...state.recentUserMessages.map(m => ({ role: 'User', ...m })),
        ...state.recentBotResponses.map(m => ({ role: 'Bot', ...m })),
      ].sort((a, b) => a.timestamp - b.timestamp).slice(-6);
      
      for (const msg of allMessages) {
        const preview = msg.text.slice(0, 200) + (msg.text.length > 200 ? '...' : '');
        recentTurns.push(`${msg.role}: ${preview}`);
      }
      if (recentTurns.length > 0) {
        parts.push(`**Recent Conversation:**\n${recentTurns.join('\n')}`);
      }
    }
    
    // Last bot response type
    if (context.lastBotResponse?.contentType) {
      parts.push(`**Last Bot Output Type:** ${context.lastBotResponse.contentType}`);
      if (context.lastBotResponse.subject) {
        parts.push(`**Last Bot Output Subject:** ${context.lastBotResponse.subject}`);
        if (context.lastBotResponse.subject.toLowerCase().startsWith('email reply draft')) {
          parts.push(`**⚠️ DRAFT READY TO SEND:** Bot just showed a reply draft. If user confirms ("yes", "send that", "go ahead"), route to send_email to send it in the original thread.`);
        }
      }
    }
    
    // Active topic
    if (state.activeTopic) {
      parts.push(`**Current Topic:** ${state.activeTopic}`);
    }
    
    // Meeting context
    if (context.isMeetingConversation) {
      parts.push(`**Context:** User is in a meeting chat`);
      if (context.meetingInfo?.subject) {
        parts.push(`**Meeting Subject:** ${context.meetingInfo.subject}`);
      }
      if (context.meetingInfo?.hasActiveCall) {
        parts.push(`**Meeting Status:** Active call in progress`);
      }
      if (context.meetingInfo?.hasTranscript) {
        parts.push(`**Transcript:** Available`);
      }
    }
    
    // Inbox context
    if (context.inboxContext?.justShowedInbox && context.inboxContext.recentSenders?.length) {
      parts.push(`**Recent Inbox Shown:** Bot just displayed inbox with emails from: ${context.inboxContext.recentSenders.join(', ')}`);
    } else if (context.inboxContext?.lastSender) {
      parts.push(`**Recent Email Context:** Last viewed email from ${context.inboxContext.lastSender}${context.inboxContext.lastSubject ? ` about "${context.inboxContext.lastSubject}"` : ''}`);
    }

    // Pending clarification context
    if (context.pendingClarification) {
      parts.push(`**PENDING CLARIFICATION:** Bot just asked: "${context.pendingClarification.question}"` +
        (context.pendingClarification.aboutPerson ? `\n  About person: ${context.pendingClarification.aboutPerson}` : '') +
        (context.pendingClarification.aboutTopic ? `\n  About topic: ${context.pendingClarification.aboutTopic}` : ''));
    }
    
    // Mentioned entities
    const recentPeople = state.entities.people.filter(p => Date.now() - p.mentionedAt < 10 * 60 * 1000);
    if (recentPeople.length > 0) {
      parts.push(`**Recently Mentioned People:** ${recentPeople.map(p => p.name).join(', ')}`);
    }
    
    return parts.length > 0 ? parts.join('\n\n') : 'No prior context available.';
  }

  private async reasonAboutRequest(
    message: string,
    contextSummary: string,
    context: AgentContext,
    tracking?: any
  ): Promise<AgentDecision> {
    const now = new Date();
    const dateContext = `Current: ${now.toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric' })} ${now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}`;

    const prompt = new ChatPrompt({
      messages: [
        {
          role: 'user',
          content: `You are an Intent Agent for a Teams meeting & productivity assistant bot.
Your ONLY job: pick the right action and enrich the query. ALWAYS BIAS TOWARD ACTION.
Use semantic understanding of user intent and conversation context, not rigid keyword matching.

USER: "${message}"
CONTEXT:
${contextSummary}
- User: ${context.userName} | Meeting chat: ${context.isMeetingConversation ? 'YES' : 'NO'} | ${dateContext}

════════════════════════════════════════════════════════════════
 ACTIONS — pick exactly ONE
════════════════════════════════════════════════════════════════

join_meeting   → User wants bot to join/enter an active Teams call
summarize      → Generate a summary/recap of a meeting (current, past, or last)
minutes        → Generate formal meeting minutes, notes, action items, decisions
transcribe     → Fetch/show the raw meeting transcript text
meeting_overview → Show meeting details, info about a PAST meeting already attended
list_meeting_groups → List group and meeting chats the user is part of
insights       → Extract key takeaways, highlights, important points from a meeting
meeting_question → Answer a SPECIFIC question about meeting content ("what did X say about Y?")
read_chats     → Read/show recent chat messages in the current conversation
check_inbox    → View/check/read emails, show inbox, filter by sender or date
check_planner_tasks → Show Planner tasks, due dates, and priority recommendations
reply_email    → Read an email and auto-draft a reply (bot reads + drafts, user does NOT need to specify content)
send_email     → Send content to someone: send last bot output, send a reply draft, compose new email
profile_details → Show user's own email address/profile
prepare_meeting → Help user PREPARE for an UPCOMING meeting: fetch today's meetings, attendees, give prep advice
check_calendar → Show upcoming meetings, schedule, availability, today/tomorrow meetings (viewing only, no prep)
list_attendees → List meeting attendees and their email addresses
general_chat   → Greetings, thanks, casual conversation, general questions, anything not above
clarification_needed → ONLY when request is absolute gibberish or self-contradictory

════════════════════════════════════════════════════════════════
 SCENARIO GUIDE — match user's message to these patterns
════════════════════════════════════════════════════════════════

INBOX & EMAIL SCENARIOS:
• "check my inbox" / "show my emails" / "any new emails?" / "top 5 emails" → check_inbox
• "emails from [person]" / "what did [person] email me?" / "show [person]'s email" → check_inbox (refined_query must include person name)
• "what did [person] say" AFTER inbox was shown → check_inbox (person was in the inbox listing)
• "reply to [person]" / "respond to [person]'s email" / "draft reply to [person]" → reply_email (refined_query must include person)
• "check email from [person] and respond" / "read [person]'s email and reply" → reply_email (bot fetches + auto-drafts)
• "respond to it" / "reply to that" (after seeing email content) → reply_email (use last email context)
• "read the email content and respond to it" → reply_email (auto-draft based on content)

SENDING SCENARIOS (send_email):
• "send it" / "send that" / "email this to me" AFTER bot showed any content → send_email
• "yes" / "send that" / "go ahead" AFTER bot showed a REPLY DRAFT → send_email ⚠️ CRITICAL: this sends the draft in-thread
• "yes send it" / "ok send" / "do it" AFTER any bot output → send_email
• "email the summary to John" / "send minutes to all" → send_email (with content type + recipient)
• "forward this to [person]" → send_email
• "send to all attendees" / "email everyone" → send_email (recipients.type = "all_attendees")

MEETING CONTENT SCENARIOS:
• "summarize" / "recap" / "what happened in the meeting" → summarize
• "summarize the Monday standup" / "recap yesterday's meeting" → summarize (refined_query must include meeting reference)
• "meeting minutes" / "create notes" / "action items from the meeting" → minutes
• "get transcript" / "show the transcript" / "transcribe the meeting" → transcribe
• "meeting details" / "tell me about this meeting" / "meeting overview" → meeting_overview
• "key takeaways" / "highlights" / "what were the main points" / "key insights" → insights
• "what did [person] say about [topic]?" / "did anyone mention [topic]?" → meeting_question
• "what was discussed about budget?" / "who talked about the deadline?" → meeting_question

CALENDAR & PREPARATION:
• "my schedule" / "what meetings do I have today" / "calendar" / "am I free at 3pm?" → check_calendar
• "meetings tomorrow" / "what's on my agenda this week" → check_calendar
• "help me prepare for my meeting" / "prepare for the meeting I have today" / "get ready for my meeting" / "help me prepare for meetings I have today" → prepare_meeting  ← ALWAYS this, never check_calendar or meeting_overview
• "I have a meeting today, help me prepare" / "preparing for the call" / "how do I prepare for the standup" → prepare_meeting
• "my email" / "my profile" / "what's my email address" → profile_details

⚠️ KEY DISAMBIGUATION: "prepare" + meeting context = prepare_meeting. "show/check/view" + calendar context = check_calendar. Never confuse them.

PLANNER TASK SCENARIOS:
• "check my planner tasks" / "show my planner tasks" / "planner tasks" / "my tasks in planner" → check_planner_tasks
• "what tasks are due today" / "which planner task is highest priority" / "prioritize my planner tasks" → check_planner_tasks
• If user explicitly says "planner" + "tasks", NEVER route to check_calendar

MEETING ACTIONS:
• "join the call" / "join the meeting" / "come join us" → join_meeting
• "who's in the meeting" / "list attendees" / "attendee emails" → list_attendees
• "list my meeting groups" / "which meeting groups am I in" / "show my group chats" → list_meeting_groups

CHAT:
• "read all chats" / "show chat messages" / "what's been said in chat" → read_chats

FOLLOW-UP & CONFIRMATION SCENARIOS:
• "yes" / "ok" / "do it" / "sure" / "go ahead" → resolve from context:
  - After reply draft shown → send_email (send the draft in-thread)
  - After summary/content shown → depends on what was asked; if sending implied → send_email
  - After question → execute what was suggested
  - If no clear prior context → general_chat
• "not that one, the other one" / "I meant the Monday meeting" → re-route with corrected context
• "actually send it to John instead" → send_email with new recipient
• "make it shorter" / "more details please" / "reformat this" / "in bullet points" → MATCH LAST CONTENT TYPE:
  - If lastBotResponse.contentType = 'inbox_email' → check_inbox (set is_reformat_request=true, format_style)
  - If lastBotResponse.contentType = 'summary'/'minutes'/'transcript'/'insights'/'meeting_overview' → summarize (set is_reformat_request=true, format_style)
  - ALWAYS set is_reformat_request=true for ANY reformat/follow-up on shown content
  - Include format in refined_query: "Reformat the previous [contentType]: make it shorter/longer/bullets"
• "what did X say" / "tell me more about Y" / "focus on Z" → FOLLOW-UP QUESTION (not reformat):
  - If lastBotResponse.contentType = 'inbox_email' → check_inbox (keep same context)
  - If lastBotResponse.contentType = 'summary'/'minutes'/'transcript'/'insights'/'meeting_overview' → meeting_question (set is_reformat_request=true, refined_query includes the question)
• Pronouns "it" / "that" / "this" / "them" → resolve from last bot output or conversation context

⚠️ CRITICAL FOLLOW-UP RULE (AVOID GRAPH CALLS):
When user asks about, reformats, or follows up on previously shown content:
- ALWAYS set is_reformat_request=true — this tells handlers to use CACHED content, not re-fetch from Graph API
- Look at lastBotResponse.contentType to determine routing:
  - 'inbox_email' → check_inbox
  - 'summary'/'minutes'/'transcript'/'insights'/'meeting_overview' → meeting_question (or summarize only for explicit reformat wording)
  - 'meeting_groups' → list_meeting_groups
- NEVER route to a different content type than what was just shown
- refined_query should describe what to do with the EXISTING content

════════════════════════════════════════════════════════════════
 GOLDEN RULES — never break these
════════════════════════════════════════════════════════════════

1. BIAS TOWARD ACTION. If you can reasonably infer what the user wants, pick the action and do it. Don't ask.
2. reply_email NEVER needs user to specify reply content — bot reads email and auto-drafts. NEVER ask "what would you like to say?"
3. Confirmations ("yes", "send that", "go ahead") after a REPLY DRAFT → send_email (NOT reply_email or check_inbox)
4. Confirmations after ANY bot output → send_email if sending is implied, otherwise execute the suggested action
5. "check email from X and respond" = ONE action: reply_email (bot finds email from X, reads it, drafts reply)
6. When inbox was recently shown, references to people in that inbox → check_inbox for that person. Don't ask "email or meeting?"
7. ⚠️ CRITICAL CONTEXT AWARENESS: "what did [person] say" depends on lastBotResponse.contentType:
   - If contentType = 'inbox_email' → check_inbox (user is asking about the EMAIL that was shown)
   - If contentType = 'summary'/'minutes'/'transcript' → meeting_question (user is asking about MEETING content)
   - If no prior context AND in meeting chat → meeting_question
   - NEVER ignore contentType context!
8. NEVER re-ask a clarification question the bot already asked. Resolve from context and act.
9. "all attendees" / "all participants" / "everyone" in send context → recipients type = all_attendees
10. For summarize/minutes/transcribe: include date/meeting references in refined_query so the handler can find the right meeting
11. "send to my email" / "email it to me" → send_email with self as recipient
12. NEVER set needs_clarification=true just because you're uncertain. Pick the best action. Clarification is for gibberish ONLY.
13. refined_query MUST be a clear, complete, context-enriched version of what the user asked — this is the ONLY thing handlers read.
14. ⚠️ FOLLOW-UP QUESTIONS after showing ANY content → route to SAME handler with is_reformat_request=true:
    - After inbox_email → check_inbox
  - After summary/minutes/transcript/insights/meeting_overview → meeting_question (summarize only for pure reformat asks)
  - After meeting_groups → list_meeting_groups
    - This tells handlers to use CACHED content, avoiding expensive Graph API re-fetches
15. ⚠️ GRAPH CALL AVOIDANCE: When is_reformat_request=true, handlers will use cached content. Set this flag for:
    - "make it shorter/longer/bullets" (reformat requests)
    - "what about X" / "tell me more" / "focus on Y" (follow-up questions)
    - "who said X" / "what did Y mention" (questions about shown content)

════════════════════════════════════════════════════════════════
 OUTPUT
════════════════════════════════════════════════════════════════

Think step-by-step, then output ONLY valid JSON:

{
  "thinking": "Brief reasoning about what user wants and why",
  "intent": "<action_label>",
  "confidence": "high|medium|low",
  "needs_clarification": false,
  "clarification_question": null,
  "refined_query": "Complete context-enriched query for the handler",
  "parameters": {
    "meeting_target": "current|last|specific|null",
    "meeting_subject": "string or null",
    "date_reference": "string or null",
    "recipients": [{"name": "...", "email": "...", "type": "specific|self|all_attendees"}],
    "content_type": "summary|minutes|transcript|insights|custom|null",
    "person_reference": "string or null",
    "format_style": "shorter|longer|bullets|detailed|brief|null",
    "is_reformat_request": true/false
  }
}`
        }
      ],
      instructions: `You route user requests to the correct action. ALWAYS pick an action. Output ONLY valid JSON. NEVER ask clarification unless the message is truly meaningless gibberish.`,
      model: new OpenAIChatModel({
        model: config.azureOpenAIDeploymentName,
        apiKey: config.azureOpenAIKey,
        endpoint: config.azureOpenAIEndpoint,
        apiVersion: '2024-10-21',
      }),
    });

    try {
      const response = await this.sendPrompt(prompt, message, tracking);
      const raw = (response.content || '').trim();
      const jsonStr = raw.replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
      const parsed = JSON.parse(jsonStr);

      const intent = this.validateIntent(parsed.intent);

      // ── Structured decision log — visible in Azure App Service log stream ──
      const prevIntent  = context.lastBotResponse?.contentType ?? 'none';
      const prevMessage = getConversationState(context.conversationId)
        .recentUserMessages.slice(-2, -1)[0]?.text ?? '(none)';
      console.log(
        `\n[DECISION_LOG] ══════════════════════════════════════════\n` +
        `[DECISION_LOG] User         : ${context.userName}\n` +
        `[DECISION_LOG] Prev message : ${prevMessage.slice(0, 120)}\n` +
        `[DECISION_LOG] Prev response: ${prevIntent}\n` +
        `[DECISION_LOG] Current msg  : ${message.slice(0, 120)}\n` +
        `[DECISION_LOG] Thinking     : ${(parsed.thinking || '').slice(0, 300)}\n` +
        `[DECISION_LOG] → Intent     : ${intent} (${parsed.confidence ?? 'unknown'})\n` +
        `[DECISION_LOG] Refined query: ${(parsed.refined_query || message).slice(0, 120)}\n` +
        `[DECISION_LOG] ══════════════════════════════════════════`
      );
      
      return {
        intent,
        confidence: parsed.confidence || 'medium',
        reasoning: parsed.thinking || '',
        needsClarification: parsed.needs_clarification === true,
        clarificationQuestion: parsed.clarification_question,
        refinedQuery: parsed.refined_query || message,
        parameters: this.normalizeParameters(parsed.parameters || {}),
        alternatives: parsed.alternatives,
        plan: parsed.plan,
      };
    } catch (error) {
      console.error('[INTENT_AGENT] Reasoning failed:', error);
      return this.createDefaultDecision('general_chat', message, 'Reasoning failed - defaulting to general chat');
    }
  }

  private validateIntent(intent: string): IntentLabel {
    const validIntents: IntentLabel[] = [
      'join_meeting', 'summarize', 'minutes', 'transcribe', 'meeting_overview', 'list_meeting_groups',
      'read_chats',
      'insights', 'meeting_question', 'check_inbox', 'reply_email', 'send_email',
      'profile_details', 'check_planner_tasks', 'prepare_meeting', 'check_calendar', 'list_attendees',
      'general_chat', 'clarification_needed',
    ];
    
    if (validIntents.includes(intent as IntentLabel)) {
      return intent as IntentLabel;
    }
    
    console.warn(`[INTENT_AGENT] Invalid intent "${intent}", defaulting to general_chat`);
    return 'general_chat';
  }

  private normalizeParameters(params: any): ActionParameters {
    return {
      meetingTarget: params.meeting_target || undefined,
      meetingSubject: params.meeting_subject || undefined,
      dateReference: params.date_reference || undefined,
      recipients: Array.isArray(params.recipients) ? params.recipients.map((r: any) => ({
        name: r.name,
        email: r.email,
        type: r.type || 'specific',
      })) : undefined,
      contentType: params.content_type || undefined,
      customContent: params.custom_content || undefined,
      personReference: params.person_reference || undefined,
      emailSubject: params.email_subject || undefined,
      timeReference: params.time_reference || undefined,
      formatStyle: params.format_style || undefined,
      isReformatRequest: !!params.is_reformat_request,
    };
  }

  private createDefaultDecision(intent: IntentLabel, query: string, reasoning: string): AgentDecision {
    return {
      intent,
      confidence: 'low',
      reasoning,
      needsClarification: false,
      refinedQuery: query,
      parameters: {},
    };
  }

  /**
   * Deprecated: keep method for compatibility but disable hardcoded matching.
   * Intent routing is now fully LLM-driven.
   */
  quickPatternMatch(message: string): { matched: boolean; intent?: IntentLabel; parameters?: ActionParameters } {
    void message;
    return { matched: false };
  }
  
  /**
   * Extract entities from message for context building
   */
  extractEntities(message: string): { people: string[]; dates: string[]; emails: string[] } {
    const people: string[] = [];
    const dates: string[] = [];
    const emails: string[] = [];
    
    // Extract email addresses
    const emailMatches = message.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g);
    if (emailMatches) emails.push(...emailMatches);
    
    // Extract date references
    const datePatterns = [
      /\b(today|tomorrow|yesterday)\b/gi,
      /\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/gi,
      /\b(this|next|last)\s+(week|month)\b/gi,
      /\b(\d{1,2}\/\d{1,2}|\d{1,2}-\d{1,2})\b/g,
    ];
    for (const pattern of datePatterns) {
      const matches = message.match(pattern);
      if (matches) dates.push(...matches);
    }
    
    // Extract potential names (capitalized words that aren't common words)
    const commonWords = new Set(['I', 'The', 'A', 'An', 'To', 'From', 'For', 'In', 'On', 'At', 'By', 'With', 'About']);
    const nameMatches = message.match(/\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b/g);
    if (nameMatches) {
      for (const match of nameMatches) {
        if (!commonWords.has(match) && match.length > 2) {
          people.push(match);
        }
      }
    }
    
    return { people, dates, emails };
  }
}

// ============================================================================
// EXPORTS
// ============================================================================

export function createIntentAgent(
  promptSender: (prompt: ChatPrompt, query: string, tracking?: any) => Promise<any>
): IntentAgent {
  return new IntentAgent(promptSender);
}

export { TOOL_CAPABILITIES };
