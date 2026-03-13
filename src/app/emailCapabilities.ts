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
  senderQuery?: string;
  wantsUrgentOnly: boolean;
  wantsUnreadOnly: boolean;
  wantsReplyDraft: boolean;
  maxResults: number;
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

function cleanSenderQuery(value?: string | null): string | undefined {
  if (!value) return undefined;
  const cleaned = value
    .replace(/^(what\s+did|show\s+me|check|find|did|can\s+you|you|respond\s+to|reply\s+to)\s+/i, '')
    .replace(/^(the|a|an|email|emails|message|messages|recent|last|latest)\s+/i, '')
    .replace(/\b(send|sent|sends)\s+me\b.*$/i, '')
    .replace(/\b(today|yesterday|this\s+week|last\s+week|recently|latest)\b.*$/i, '')
    .replace(/\s+(email|emails|message|messages|sent me|in my inbox).*$/i, '')
    .replace(/'s?$/i, '')       // apostrophe possessive: martin's → martin
    .replace(/s$/i, '')         // bare possessive: martins → martin
    .replace(/[?.!,]+$/g, '')
    .trim();
  return cleaned || undefined;
}

export function parseInboxRequest(message: string): InboxRequestAnalysis {
  const text = (message || '').trim();
  // Only treat as urgent-only when the user explicitly asks for urgent/critical emails.
  // Avoid matching on words like "priority" or "important" that can appear in email subjects
  // or casual phrasing ("what important emails do I have" should NOT hard-filter to urgent).
  const wantsUrgentOnly = /\b(urgent\s+(?:email|mail|message|inbox)|show\s+(?:me\s+)?(?:only\s+)?urgent|critical\s+(?:email|mail)|asap\s+(?:email|mail)|high[\s-]priority\s+email)\b/i.test(text);
  const wantsUnreadOnly = /(unread|new email|new emails|latest email|latest emails)/i.test(text);
  const wantsReplyDraft = /(draft\s+(?:a\s+)?reply|write\s+(?:a\s+)?reply|respond\s+to|reply\s+to)/i.test(text);

  const explicitCount = text.match(/\b(?:top|last|latest)\s+(\d{1,2})\b/i);
  const maxResults = Math.min(Math.max(Number(explicitCount?.[1] || 5), 1), 10);

  const patterns = [
    /\bfrom\s+([^?.!,]+)/i,
    /\bmessage\s+([^?.!,]+?)\s+(?:sent|send|sends)\s+me\b/i,
    /\bwhat\s+did\s+([^?.!,]+?)\s+(?:sent|send|sends)\s+me\b/i,
    /\b([^?.!,]+?)\s+(?:sent|send|sends)\s+me\b/i,
    // Possessive: "martin's email", "martin's last email", "martin's recent message"
    /\b([a-z]+(?:\s+[a-z]+)*)(?:'s?)\s+(?:recent\s+|last\s+|latest\s+)?(?:email|mail|message)s?\b/i,
    // "respond to martin", "reply to martin's email", "respond to martin's recent email"
    /\b(?:respond|reply)\s+to\s+(?:recent\s+)?([a-z]+(?:\s+[a-z]+)*)(?:'s?)?(?:\s+(?:recent\s+|last\s+|latest\s+)?(?:email|mail|message)s?)?\b/i,
    // "email martin sent", "email by martin"
    /\b(?:email|message|mail)\s+(?:by|that)\s+([^?.!,]+?)\s+(?:sent|wrote)\b/i,
  ];

  let senderQuery: string | undefined;
  for (const pattern of patterns) {
    const match = text.match(pattern);
    senderQuery = cleanSenderQuery(match?.[1]);
    if (senderQuery) {
      break;
    }
  }

  return {
    senderQuery,
    wantsUrgentOnly,
    wantsUnreadOnly,
    wantsReplyDraft,
    maxResults,
  };
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
    return 'I could not find any matching inbox messages.';
  }

  const prompt = new ChatPrompt({
    messages: [
      {
        role: 'user',
        content:
          `Review these inbox messages for the request: "${request}"\n\n` +
          `Messages:\n${JSON.stringify(messages, null, 2)}\n\n` +
          `Return a concise markdown summary with:\n` +
          `1. A one-line overview\n` +
          `2. Up to 5 bullet points for the most urgent or relevant emails\n` +
          `3. For each item include sender, subject, why it matters, and received time\n` +
          `4. If the request asks for urgent emails, prioritize importance=high, unread, flagged, or time-sensitive content\n` +
          `5. If nothing looks urgent, say that clearly`,
      },
    ],
    instructions: 'You triage inbox email. Be concise, practical, and do not invent details not present in the message list.',
    model: createModel(),
  });

  const response = await runPrompt(prompt, '', tracking ? {
    ...tracking,
    estimatedInputText: `${request}\n${JSON.stringify(messages)}`,
  } : undefined);

  return response.content || 'I could not summarize the inbox messages.';
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
