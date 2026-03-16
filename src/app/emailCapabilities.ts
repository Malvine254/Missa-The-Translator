import { ChatPrompt } from '@microsoft/teams.ai';
import { OpenAIChatModel } from '@microsoft/teams.openai';
import config from '../config';
import type { MailMessageSummary } from '../graphApiHelper';

export interface PromptTrackingContext {
  userId: string;
  displayName: string;
  tenantId?: string;
  meetingId: string;
  estimatedInputText: string;
}

export type PromptRunner = (
  prompt: ChatPrompt,
  input: string,
  tracking?: PromptTrackingContext
) => Promise<{ content?: string }>;

export interface EmailRequestAnalysis {
  isContextualReference: boolean;
  contentType: 'summary' | 'minutes' | 'transcript' | 'previous' | 'custom' | null;
  recipientType: 'self' | 'other' | 'multiple' | 'all_participants' | null;
  recipientNames: string[];
  recipientEmails: string[];
  specificContentRequest?: string;
  reasoning: string;
}

export interface EmailSendResult {
  success: boolean;
  error?: string;
  sentTo?: string[];
  failedRecipients?: Array<{ email: string; error: string; reason?: string }>;
  partialSuccess?: boolean;
}

export interface InboxRequestAnalysis {
  wantsReplyDraft: boolean;
  maxResults: number;
}

export interface InboxSearchResult {
  matchingMessages: MailMessageSummary[];
  searchReasoning: string;
  noMatchReason?: string;
}

function createModel() {
  return new OpenAIChatModel({
    model: config.azureOpenAIDeploymentName,
    apiKey: config.azureOpenAIKey,
    endpoint: config.azureOpenAIEndpoint,
    apiVersion: '2024-10-21',
  });
}

function parseJsonResponse(raw: string): any {
  const jsonStr = (raw || '').replace(/```json\n?/g, '').replace(/```\n?/g, '').trim();
  return JSON.parse(jsonStr);
}

/**
 * Simple inbox request parsing - just detect reply intent and result count.
 * The actual message matching is done by LLM in llmSearchInbox.
 */
export function parseInboxRequest(message: string): InboxRequestAnalysis {
  const text = (message || '').trim();
  const wantsReplyDraft = /(draft\s+(?:a\s+)?reply|write\s+(?:a\s+)?reply|respond\s+to|reply\s+to)/i.test(text);
  const explicitCount = text.match(/\b(?:top|last|latest)\s+(\d{1,2})\b/i);
  const maxResults = Math.min(Math.max(Number(explicitCount?.[1] || 10), 1), 20);

  return { wantsReplyDraft, maxResults };
}

/**
 * LLM-based inbox search - finds messages matching the user's natural language query.
 * No hardcoded filtering - the LLM decides which messages are relevant.
 */
export async function llmSearchInbox(
  userQuery: string,
  messages: MailMessageSummary[],
  runPrompt: PromptRunner,
  tracking?: PromptTrackingContext
): Promise<InboxSearchResult> {
  if (!messages.length) {
    return { matchingMessages: [], searchReasoning: 'No messages in inbox', noMatchReason: 'Your inbox is empty.' };
  }

  // Build a compact representation of messages for the LLM
  const messageList = messages.map((m, idx) => ({
    idx,
    from: m.fromName || m.fromAddress,
    subject: m.subject,
    preview: (m.bodyPreview || '').slice(0, 150),
    date: m.receivedDateTime,
  }));

  try {
    const prompt = new ChatPrompt({
      model: createModel(),
      messages: [
        {
          role: 'user',
          content:
            `Find emails matching this request: "${userQuery}"\n\n` +
            `Available emails:\n${JSON.stringify(messageList, null, 1)}\n\n` +
            `Return JSON with:\n` +
            `- "matchingIndices": array of idx numbers for matching emails (empty if none match)\n` +
            `- "reasoning": brief explanation of your selection\n` +
            `- "noMatchReason": if no matches, explain why (e.g., "No emails from Leonard found")\n\n` +
            `Match criteria:\n` +
            `- Sender name mentioned? Match on "from" field (partial match OK: "leonard" matches "Leonard Mwangi")\n` +
            `- Topic/content mentioned? Match on subject or preview\n` +
            `- Date mentioned? Check the date field\n` +
            `- If query is general (e.g., "check inbox"), return recent important emails\n\n` +
            `Respond ONLY with JSON: {"matchingIndices": [...], "reasoning": "...", "noMatchReason": "..." or null}`,
        },
      ],
    });

    const result = await runPrompt(prompt, userQuery, tracking);
    const parsed = parseJsonResponse(result.content || '{}');
    
    console.log(`[INBOX_LLM_SEARCH] Found ${parsed.matchingIndices?.length || 0} matches: ${parsed.reasoning}`);

    const indices: number[] = Array.isArray(parsed.matchingIndices) ? parsed.matchingIndices : [];
    const matchingMessages = indices
      .filter((i: number) => i >= 0 && i < messages.length)
      .map((i: number) => messages[i]);

    return {
      matchingMessages,
      searchReasoning: parsed.reasoning || '',
      noMatchReason: matchingMessages.length === 0 ? (parsed.noMatchReason || 'No matching emails found.') : undefined,
    };
  } catch (error) {
    console.error('[INBOX_LLM_SEARCH] Search failed:', error);
    // Fallback: return most recent messages
    return {
      matchingMessages: messages.slice(0, 5),
      searchReasoning: 'LLM search failed, showing recent emails',
    };
  }
}

/**
 * Simplified alias for backward compatibility.
 */
export async function smartParseInboxRequest(
  message: string,
  _runPrompt: PromptRunner,
  _tracking?: PromptTrackingContext
): Promise<InboxRequestAnalysis> {
  return parseInboxRequest(message);
}

export function formatRecipientDisplay(emails: string[], names: string[]): string {
  return emails.map((email, index) => {
    const name = names[index];
    return name ? `**${name}** (${email})` : `**${email}**`;
  }).join(', ');
}

export function formatEmailResult(
  sendResult: EmailSendResult,
  recipientNames: string[],
  contentTypeName: string
): string {
  if (sendResult.success && !sendResult.partialSuccess) {
    const recipientDisplay = formatRecipientDisplay(sendResult.sentTo || [], recipientNames);
    return `Done! I've sent the ${contentTypeName} to ${recipientDisplay}.`;
  }

  if (sendResult.partialSuccess) {
    const successDisplay = sendResult.sentTo?.length
      ? `✓ **Sent successfully to:** ${sendResult.sentTo.join(', ')}`
      : '';
    const failureDetails = sendResult.failedRecipients?.map((failure) =>
      `• ${failure.email}: ${failure.reason || failure.error}`
    ).join('\n') || '';

    return `${contentTypeName} delivery completed with some issues:\n\n${successDisplay}\n\n✗ **Failed to deliver:**\n${failureDetails}\n\n_These users may be external to the tenant or have mailbox restrictions._`;
  }

  const failureDetails = sendResult.failedRecipients?.map((failure) =>
    `• ${failure.email}: ${failure.reason || failure.error}`
  ).join('\n') || sendResult.error || 'Unknown error';

  return `Couldn't send the ${contentTypeName}:\n\n${failureDetails}\n\n_This may be due to external recipients or permission issues. Would you like me to try a different approach?_`;
}

export async function analyzeEmailRequest(
  message: string,
  hasRecentBotResponse: boolean,
  lastContentType: string | undefined,
  runPrompt: PromptRunner,
  tracking?: PromptTrackingContext
): Promise<EmailRequestAnalysis> {
  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `You are an intelligent email request analyzer for a meeting assistant bot. Your job is understand EXACTLY what the user wants when they ask to send/email something.\n\n` +
          `=== CONTEXT ===\n` +
          `User's request: "${message}"\n` +
          `Bot just showed content: ${hasRecentBotResponse ? 'YES' : 'NO'}\n` +
          `${hasRecentBotResponse && lastContentType ? `Type of content just shown: "${lastContentType}"` : 'No recent content shown'}\n\n` +
          `=== YOUR TASK ===\n` +
          `Analyze the request and determine:\n\n` +
          `1. **isContextualReference** (true/false): Is the user asking to send content that was ALREADY shown to them?\n` +
          `   - TRUE if they refer to previous content: "send it", "email that", "send the summary to email", "forward this"\n` +
          `   - FALSE if they want NEW content created first: "summarize and send", "create minutes and email"\n\n` +
          `2. **contentType**: What content to send?\n` +
          `   - "previous" = Send what was ALREADY shown (use when isContextualReference=true)\n` +
          `   - "summary" = CREATE a NEW summary first, then send\n` +
          `   - "minutes" = CREATE NEW minutes first, then send\n` +
          `   - "transcript" = CREATE a NEW transcript first, then send\n` +
          `   - "custom" = Compose a completely new email (not meeting content)\n\n` +
          `3. **recipientType**: Who receives the email?\n` +
          `   - "self" = User's own email (my inbox, my email, to me, email me, send me, or no specific recipient)\n` +
          `   - "other" = One specific person (send to John, email Sarah)\n` +
          `   - "multiple" = Multiple specific recipients (send to john@x.com and jane@y.com)\n` +
          `   - "all_participants" = ALL meeting/chat participants ("send to everyone", "email all participants", "send to the team", "distribute to all attendees")\n\n` +
          `4. **recipientNames**: Array of recipient names mentioned. Empty [] if self or all_participants.\n` +
          `5. **recipientEmails**: Array of ALL explicit email addresses. Extract EVERY email@domain.xxx pattern.\n\n` +
          `6. **specificContentRequest**: If user wants a SPECIFIC PART of content, extract it. Examples:\n` +
          `   - "send just what John said" → "what John said"\n` +
          `   - "email the budget discussion part" → "budget discussion"\n` +
          `   - "send the first 10 minutes" → "first 10 minutes"\n` +
          `   - "email Sarah's contributions only" → "Sarah's contributions"\n` +
          `   - null if they want the full content\n\n` +
          `=== CRITICAL PATTERNS ===\n` +
          `- "send to everyone", "email all participants", "distribute to the team", "share with all attendees" → recipientType="all_participants"\n` +
          `- Extract ALL email addresses with pattern: word+chars@domain.extension (handle .co.uk, .com, .org, etc.)\n` +
          `- If request mentions specific speaker/topic/timeframe, capture in specificContentRequest\n\n` +
          `Respond with ONLY valid JSON: {"isContextualReference": boolean, "contentType": "previous"|"summary"|"minutes"|"transcript"|"custom"|null, "recipientType": "self"|"other"|"multiple"|"all_participants"|null, "recipientNames": string[], "recipientEmails": string[], "specificContentRequest": string|null, "reasoning": "brief explanation"}`,
      },
    ],
    instructions: 'You are an intelligent assistant that extracts ALL email addresses, recipient types, and specific content requests. Always return arrays for recipientNames and recipientEmails. Output valid JSON only.',
    model: createModel(),
  });

  try {
    const response = await runPrompt(prompt, '', tracking ? {
      ...tracking,
      estimatedInputText: `${message}\n${lastContentType || ''}`,
    } : undefined);

    const parsed = parseJsonResponse(response.content || '');
    const recipientNames = Array.isArray(parsed.recipientNames)
      ? parsed.recipientNames
      : (parsed.recipientName ? [parsed.recipientName] : []);
    const recipientEmails = Array.isArray(parsed.recipientEmails)
      ? parsed.recipientEmails
      : (parsed.recipientEmail ? [parsed.recipientEmail] : []);

    let recipientType = parsed.recipientType || null;
    if (recipientType !== 'all_participants') {
      if (recipientEmails.length > 1 || recipientNames.length > 1) {
        recipientType = 'multiple';
      }
    }

    return {
      isContextualReference: !!parsed.isContextualReference,
      contentType: parsed.contentType || null,
      recipientType,
      recipientNames: recipientNames.filter((name: string) => name),
      recipientEmails: recipientEmails.filter((email: string) => email && email.includes('@')),
      specificContentRequest: parsed.specificContentRequest || undefined,
      reasoning: parsed.reasoning || '',
    };
  } catch (error) {
    console.warn('[EMAIL_ANALYSIS] LLM analysis failed, will retry with simpler prompt:', error);
  }

  const simplePrompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content: `Quick analysis: "${message}"\nRecent content shown: ${hasRecentBotResponse ? `YES (type: ${lastContentType})` : 'NO'}\n\nExtract ALL email addresses from the request. Answer JSON: {"isContextualReference": true/false, "contentType": "previous" if referring to shown content else null, "recipientType": "self"|"other"|"multiple", "recipientNames": [], "recipientEmails": ["extract@all.emails", "from@request.com"], "reasoning": "brief"}`,
      },
    ],
    instructions: 'Extract ALL email addresses. Output valid JSON with arrays for recipientNames and recipientEmails.',
    model: createModel(),
  });

  try {
    const response = await runPrompt(simplePrompt, '', tracking ? {
      ...tracking,
      estimatedInputText: message,
    } : undefined);
    const parsed = parseJsonResponse(response.content || '');
    const recipientNames = Array.isArray(parsed.recipientNames) ? parsed.recipientNames : [];
    const recipientEmails = Array.isArray(parsed.recipientEmails) ? parsed.recipientEmails : [];

    return {
      isContextualReference: !!parsed.isContextualReference,
      contentType: parsed.contentType || null,
      recipientType: parsed.recipientType || null,
      recipientNames,
      recipientEmails: recipientEmails.filter((email: string) => email && email.includes('@')),
      reasoning: parsed.reasoning || 'Fallback analysis',
    };
  } catch (error) {
    console.warn('[EMAIL_ANALYSIS] Fallback analysis failed:', error);
    return {
      isContextualReference: hasRecentBotResponse,
      contentType: hasRecentBotResponse ? 'previous' : null,
      recipientType: /\b(to me|email me|my inbox|my email)\b/i.test(message) ? 'self' : null,
      recipientNames: [],
      recipientEmails: [],
      reasoning: 'Heuristic fallback',
    };
  }
}

export async function summarizeInboxMessages(
  request: string,
  messages: MailMessageSummary[],
  runPrompt: PromptRunner,
  tracking?: PromptTrackingContext
): Promise<string> {
  if (!messages.length) {
    return 'No matching emails found.';
  }

  // Build message data with full body content
  const messageData = messages.map(m => ({
    from: m.fromName || m.fromAddress,
    subject: m.subject,
    date: m.receivedDateTime,
    body: m.bodyContent || m.bodyPreview || '',
    importance: m.importance,
    isRead: m.isRead,
  }));

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `User request: "${request}"\n\n` +
          `Emails:\n${JSON.stringify(messageData, null, 1)}\n\n` +
          `FORMAT REQUIREMENTS (STRICT):\n` +
          `1. Start with: **From:** [Sender] | **Date:** [Date]\n` +
          `2. Next line: **Subject:** [Subject]\n` +
          `3. Then a blank line, followed by the email content\n` +
          `4. Use proper markdown:\n` +
          `   - ## for main section headers (with blank line before/after)\n` +
          `   - **Bold** for emphasis\n` +
          `   - Bullet points with proper spacing\n` +
          `   - Numbered lists where appropriate\n` +
          `5. If email has structured content (meeting notes, action items), preserve that structure cleanly\n` +
          `6. Keep concise - summarize long emails to key points\n` +
          `7. NO run-on headers (each header on its own line)\n` +
          `8. NO walls of text - use paragraphs and spacing`,
      },
    ],
    instructions: 'Output clean, professional markdown. Headers on separate lines. Proper spacing between sections.',
    model: createModel(),
  });

  const response = await runPrompt(prompt, '', tracking ? {
    ...tracking,
    estimatedInputText: `${request}\n${JSON.stringify(messageData)}`,
  } : undefined);

  return response.content || 'Could not process inbox messages.';
}

export async function draftReplyFromInboxThread(
  request: string,
  messages: MailMessageSummary[],
  runPrompt: PromptRunner,
  tracking?: PromptTrackingContext,
  replyToSender?: { name: string; email: string }
): Promise<string> {
  if (!messages.length) {
    return 'I could not find an email thread to draft a reply for.';
  }

  const senderName = tracking?.displayName || '';
  const signatureNote = senderName
    ? `Sign the reply with the sender's real name: "${senderName}". Never use placeholders like [Your Name].`
    : 'Sign the reply with the sender\'s real name if known. Never use placeholders like [Your Name].';

  const replyToNote = replyToSender?.name
    ? `The reply MUST be addressed TO: ${replyToSender.name}${replyToSender.email ? ` <${replyToSender.email}>` : ''}. ` +
      `Open with "Hi ${replyToSender.name.split(' ')[0]}," or equivalent. Do NOT address anyone else.`
    : '';

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `Draft a reply to this email thread based on the user's request.\n\n` +
          `User request: "${request}"\n\n` +
          `Email thread:\n${JSON.stringify(messages, null, 2)}\n\n` +
          `Return markdown only with this structure:\n` +
          `## Suggested Reply\n` +
          `**Subject:** <reply subject>\n\n` +
          `<reply body>\n\n` +
          `## Rationale\n` +
          `<brief explanation of why this reply fits the thread and the user request>`
      },
    ],
    instructions: `You write professional email replies. Use only the provided thread. Be specific, concise, and safe. Do not claim actions were completed unless the thread says so. ${replyToNote} ${signatureNote}`.trim(),
    model: createModel(),
  });

  const response = await runPrompt(prompt, '', tracking ? {
    ...tracking,
    estimatedInputText: `${request}\n${JSON.stringify(messages)}`,
  } : undefined);

  // Replace any remaining placeholder the LLM may have emitted
  const content = (response.content || 'I could not draft a reply for that email thread.')
    .replace(/\[Your Name\]/gi, senderName || '[Your Name]')
    .replace(/\[Your name\]/gi, senderName || '[Your name]');

  return content;
}
