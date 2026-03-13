import axios, { AxiosInstance } from 'axios';
import { ManagedIdentityCredential } from '@azure/identity';
import config from './config';

/**
 * Convert markdown text to HTML for email formatting.
 * Handles: headers, bold, italic, bullet points, numbered lists, links, code blocks.
 */
function markdownToHtml(markdown: string): string {
  if (!markdown) return '';
  
  let html = markdown
    // Escape HTML entities first
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    
    // Headers (## Header → <h2>)
    .replace(/^### (.+)$/gm, '<h3 style="color:#333;margin:16px 0 8px 0;">$1</h3>')
    .replace(/^## (.+)$/gm, '<h2 style="color:#333;margin:20px 0 10px 0;">$1</h2>')
    .replace(/^# (.+)$/gm, '<h1 style="color:#333;margin:24px 0 12px 0;">$1</h1>')
    
    // Bold and italic
    .replace(/\*\*\*(.+?)\*\*\*/g, '<strong><em>$1</em></strong>')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/___(.+?)___/g, '<strong><em>$1</em></strong>')
    .replace(/__(.+?)__/g, '<strong>$1</strong>')
    .replace(/_(.+?)_/g, '<em>$1</em>')
    
    // Inline code
    .replace(/`([^`]+)`/g, '<code style="background:#f4f4f4;padding:2px 6px;border-radius:3px;font-family:monospace;">$1</code>')
    
    // Horizontal rules
    .replace(/^---+$/gm, '<hr style="border:none;border-top:1px solid #ddd;margin:16px 0;">')
    .replace(/^\*\*\*+$/gm, '<hr style="border:none;border-top:1px solid #ddd;margin:16px 0;">')
    
    // Bullet points (• or - or *)
    .replace(/^[•\-\*]\s+(.+)$/gm, '<li style="margin:4px 0;">$1</li>')
    
    // Numbered lists
    .replace(/^\d+\.\s+(.+)$/gm, '<li style="margin:4px 0;">$1</li>')
    
    // Line breaks
    .replace(/\n\n/g, '</p><p style="margin:12px 0;">')
    .replace(/\n/g, '<br>');
  
  // Wrap consecutive <li> items in <ul>
  html = html.replace(/(<li[^>]*>.*?<\/li>)(\s*<br>\s*)?(<li[^>]*>)/g, '$1$3');
  html = html.replace(/(<li[^>]*>.*?<\/li>)+/g, '<ul style="margin:8px 0;padding-left:24px;">$&</ul>');
  
  // Wrap in paragraph if not starting with block element
  if (!html.startsWith('<h') && !html.startsWith('<ul') && !html.startsWith('<p')) {
    html = '<p style="margin:12px 0;">' + html + '</p>';
  }
  
  return `<div style="font-family:Segoe UI,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.6;color:#333;">${html}</div>`;
}

interface UserInfo {
  id: string;
  displayName: string;
  mail?: string;
  userPrincipalName?: string;
}

interface ChatMessage {
  id: string;
  messageType?: string;
  from: {
    user?: {
      id: string;
      displayName: string;
    };
  } | null;
  body: {
    content: string;
    contentType?: string;
  };
  createdDateTime: string;
}

export interface MailMessageSummary {
  id: string;
  subject: string;
  fromName: string;
  fromAddress: string;
  receivedDateTime: string;
  importance: 'low' | 'normal' | 'high';
  isRead: boolean;
  bodyPreview: string;
  conversationId?: string;
  webLink?: string;
  categories?: string[];
  flagged?: boolean;
}

interface TranscriptionResult {
  status: string;
  id: string;
  recordingFile?: string;
  transcript?: string;
}

interface OnlineMeetingInfo {
  onlineMeetingId?: string;
  joinWebUrl?: string;
  joinMeetingId?: string;
  passcode?: string;
  organizer?: {
    id: string;
    displayName?: string;
    tenantId?: string;
  };
  subject?: string;
  startDateTime?: string;
  endDateTime?: string;
}

interface CallInfo {
  id: string;
  state: string;
  callbackUri?: string;
}

class GraphApiHelper {
  private graphClient: AxiosInstance;
  private tokenFactory: (() => Promise<string>) | null = null;
  private static readonly GRAPH_TIMEOUT_MS = 15000;
  private meetingIdLookupDeniedUntil = new Map<string, number>();
  private meetingIdLookupInFlight = new Map<string, Promise<string | null>>();
  private chatTranscriptDeniedUntil = new Map<string, number>();
  private chatTranscriptSkipLogUntil = new Map<string, number>();
  private transcriptDownloadDeniedUntil = new Map<string, number>();
  private transcriptDownloadSkipLogUntil = new Map<string, number>();

  private getMeetingLookupCacheKey(organizerId: string, joinWebUrl: string): string {
    const normalizedOrganizer = (organizerId || '').trim().toLowerCase();
    const normalizedJoinUrl = this.normalizeJoinWebUrl(joinWebUrl);
    return `${normalizedOrganizer}::${normalizedJoinUrl}`;
  }

  private normalizeJoinWebUrl(joinWebUrl: string): string {
    const raw = (joinWebUrl || '').trim();
    if (!raw) return '';

    // Normalize encoded variants and trivial formatting differences so cache keys are stable.
    let decoded = raw;
    try {
      decoded = decodeURIComponent(raw);
    } catch {
      // Keep original if decoding fails.
    }

    decoded = decoded.replace(/&amp;/gi, '&').replace(/\s+/g, '');

    try {
      const url = new URL(decoded);
      // Lowercase host and remove trailing slash on pathname for key stability.
      const path = url.pathname.replace(/\/+$/, '');
      return `${url.protocol}//${url.host.toLowerCase()}${path}${url.search}`;
    } catch {
      return decoded;
    }
  }

  private extractJoinWebUrlFromText(text: string): string | null {
    if (!text) return null;

    const decoded = text
      .replace(/&amp;/gi, '&')
      .replace(/&quot;/gi, '"')
      .replace(/&#x2F;/gi, '/')
      .replace(/&#47;/gi, '/');

    const match = decoded.match(/https:\/\/teams\.microsoft\.com\/l\/meetup-join\/[^\s"'<>]+/i);
    return match ? match[0] : null;
  }

  /**
   * Extract the organizer OID from a Teams joinWebUrl.
   * URL format: https://teams.microsoft.com/l/meetup-join/{threadId}/{organizerOid}?context=...
   * The context query param also contains {"Tid":"...","Oid":"organizerOid"}
   */
  private extractOrganizerIdFromJoinWebUrl(joinWebUrl: string): string | null {
    if (!joinWebUrl) return null;
    try {
      // Method 1: Parse from URL path - format: /l/meetup-join/{threadId}/{organizerOid}
      const url = new URL(joinWebUrl);
      const pathParts = url.pathname.split('/').filter(Boolean);
      // Expected: ['l', 'meetup-join', '{threadId}', '{organizerOid}']
      if (pathParts.length >= 4 && pathParts[1] === 'meetup-join') {
        const possibleOid = pathParts[3];
        // Validate it looks like a GUID (36 chars with dashes)
        if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(possibleOid)) {
          return possibleOid;
        }
      }

      // Method 2: Parse from context query param
      const context = url.searchParams.get('context');
      if (context) {
        const decoded = decodeURIComponent(context);
        const parsed = JSON.parse(decoded);
        if (parsed?.Oid && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(parsed.Oid)) {
          return parsed.Oid;
        }
      }
    } catch (e) {
      console.warn(`[GRAPH_API] Failed to parse organizer from joinWebUrl: ${e}`);
    }
    return null;
  }

  private async buildMeetingInfoFromRecentMessages(chatId: string, subject?: string): Promise<OnlineMeetingInfo | null> {
    try {
      const messages = await this.getChatMessages(chatId, 30);
      if (!messages.length) {
        return null;
      }

      for (const msg of messages) {
        const html = msg?.body?.content || '';
        const joinWebUrl = this.extractJoinWebUrlFromText(html);
        if (!joinWebUrl) {
          continue;
        }

        // Extract organizer ID from the joinWebUrl itself (authoritative)
        const urlOrganizerId = this.extractOrganizerIdFromJoinWebUrl(joinWebUrl);
        // Fallback to message sender only if URL parsing fails
        const organizerId = urlOrganizerId || msg?.from?.user?.id;
        if (!organizerId) {
          console.warn(`[GRAPH_API] Found joinWebUrl but could not determine organizer (URL parse failed, no msg sender)`);
          continue;
        }

        const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;
        console.log(`[GRAPH_API] Fallback meeting info from chat messages. organizer=${organizerId} (from ${urlOrganizerId ? 'URL' : 'msg sender'}), joinWebUrl=present`);

        return {
          joinWebUrl,
          organizer: {
            id: organizerId,
            tenantId: tenantId || '',
          },
          subject,
        };
      }

      return null;
    } catch (error) {
      const status = (error as any)?.response?.status;
      console.warn(`[GRAPH_API] Fallback meeting info lookup failed (status=${status || 'n/a'})`);
      return null;
    }
  }

  private async getManagedIdentityToken(scope: string): Promise<string> {
    try {
      const clientId = process.env.CLIENT_ID;
      if (!clientId) {
        console.warn(`[MSI_AUTH] Missing CLIENT_ID for managed identity token acquisition`);
        return '';
      }
      const credential = new ManagedIdentityCredential({ clientId });
      const tokenResponse = await credential.getToken(scope);
      if (!tokenResponse?.token) {
        console.warn(`[MSI_AUTH] Managed identity token acquisition returned empty token`);
        return '';
      }
      return tokenResponse.token;
    } catch (error) {
      console.error(`[MSI_AUTH_ERROR] Failed to obtain managed identity token:`, error);
      return '';
    }
  }

  private logGraphError(context: string, error: any) {
    const status = error?.response?.status;
    const message = error?.response?.data?.error?.message || error?.message || 'Unknown error';
    console.error(`[GRAPH_API_ERROR] ${context}. status=${status || 'n/a'} message=${message}`);
  }

  constructor(tokenFactory?: () => Promise<string>) {
    if (tokenFactory) {
      this.tokenFactory = tokenFactory;
    }
    this.graphClient = axios.create({
      baseURL: config.graphApiEndpoint,
    });

    // Add token to every request if token factory is available
    this.graphClient.interceptors.request.use(async (request) => {
      if (this.tokenFactory) {
        try {
          const token = await this.tokenFactory();
          request.headers.Authorization = `Bearer ${token}`;
        } catch (error) {
          console.warn('Token retrieval failed, proceeding without auth:', error);
        }
      }
      return request;
    });
      // Initialize default token factory if not provided
      this.initializeDefaultTokenFactory();
    }

  setTokenFactory(tokenFactory: () => Promise<string>) {
    this.tokenFactory = tokenFactory;
  }

  /**
   * Make a Graph API GET request using GRAPH_CLIENT_ID credentials (which have Chat.Read.All etc.).
   * Falls back to the default graphClient (managed identity) if client credentials are unavailable.
   */
  private async graphGetWithClientCredentials(path: string): Promise<any> {
    let token: string | null = null;
    try {
      token = await this.getTokenUsingClientCredentials();
    } catch {
      // fall through to default client
    }
    if (token) {
      const url = path.startsWith('http') ? path : `${config.graphApiEndpoint}${path}`;
      return axios.get(url, {
        headers: { Authorization: `Bearer ${token}` },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
      });
    }
    return this.graphClient.get(path);
  }

    /**
     * Get access token using client credentials flow (OAuth 2.0)
     */
    private async getTokenUsingClientCredentials(): Promise<string> {
      try {
        const clientId = config.graphClientId || process.env.CLIENT_ID;
        const clientSecret = config.graphClientSecret || process.env.CLIENT_SECRET;
        const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;

        if (!clientId || !clientSecret || !tenantId) {
          console.warn(`[GRAPH_AUTH] Missing credentials: CLIENT_ID=${!!clientId}, CLIENT_SECRET=${!!clientSecret}, TENANT_ID=${!!tenantId}`);
          return '';
        }

        console.log(`[GRAPH_AUTH] Requesting token using client credentials for tenant: ${tenantId}`);
        const form = new URLSearchParams();
        form.append('client_id', clientId);
        form.append('client_secret', clientSecret);
        form.append('scope', 'https://graph.microsoft.com/.default');
        form.append('grant_type', 'client_credentials');

        const response = await axios.post(
          `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
          form.toString(),
          {
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
            },
            timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
          }
        );

        const token = response.data.access_token;
        console.log(`[GRAPH_AUTH] Successfully obtained access token (expires in ${response.data.expires_in} seconds)`);
        
        return token;
      } catch (error) {
        console.error(`[GRAPH_AUTH_ERROR] Failed to obtain access token:`, error);
        return '';
      }
    }

    /**
     * Initialize token factory - tries client credentials flow first
     */
    private initializeDefaultTokenFactory() {
      // Only set if not already set
      if (this.tokenFactory) return;

      // Try to use client credentials if available
      if ((config.graphClientId || process.env.CLIENT_ID) && (config.graphClientSecret || process.env.CLIENT_SECRET)) {
        console.log(`[GRAPH_AUTH] Client credentials detected, setting up OAuth token factory`);
        this.tokenFactory = () => this.getTokenUsingClientCredentials();
      }
    }

  /**
   * Get user information by ID to identify them by name
   */
  async getUserInfo(userId: string): Promise<UserInfo | null> {
    try {
      const response = await this.graphClient.get(`/users/${encodeURIComponent(userId)}`);
      return {
        id: response.data.id,
        displayName: response.data.displayName || response.data.givenName || '',
        mail: response.data.mail,
        userPrincipalName: response.data.userPrincipalName,
      };
    } catch (error) {
      console.log(`[GRAPH_API] Using fallback user profile for ${userId}`);
      return {
        id: userId,
        displayName: '',
      };
    }
  }

  /**
   * Get user's timezone from mailbox settings
   */
  async getUserTimezone(userId: string): Promise<string> {
    try {
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        console.warn(`[TIMEZONE] No token available, defaulting to UTC`);
        return 'UTC';
      }
      
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${userId}/mailboxSettings`,
        {
          headers: { Authorization: `Bearer ${token}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
        }
      );
      
      const timezone = response.data?.timeZone || 'UTC';
      console.log(`[TIMEZONE] User ${userId} timezone: ${timezone}`);
      return timezone;
    } catch (error: any) {
      console.warn(`[TIMEZONE] Could not fetch timezone for ${userId}, defaulting to UTC:`, error?.message);
      return 'UTC';
    }
  }

  private stripHtmlToText(value: string): string {
    return (value || '')
      .replace(/<br\s*\/?>/gi, '\n')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/\s+/g, ' ')
      .trim();
  }

  private mapMailMessage(message: any): MailMessageSummary {
    const bodyPreview = message?.bodyPreview || this.stripHtmlToText(message?.body?.content || '');
    return {
      id: message?.id || '',
      subject: message?.subject || '(No subject)',
      fromName: message?.from?.emailAddress?.name || '',
      fromAddress: message?.from?.emailAddress?.address || '',
      receivedDateTime: message?.receivedDateTime || '',
      importance: message?.importance || 'normal',
      isRead: !!message?.isRead,
      bodyPreview,
      conversationId: message?.conversationId,
      webLink: message?.webLink,
      categories: Array.isArray(message?.categories) ? message.categories : [],
      flagged: !!message?.flag?.flagStatus && message.flag.flagStatus !== 'notFlagged',
    };
  }

  async getInboxMessages(
    userId: string,
    options?: { senderQuery?: string; top?: number; unreadOnly?: boolean }
  ): Promise<MailMessageSummary[]> {
    try {
      if (!this.tokenFactory) {
        console.warn(`[GRAPH_API] Token factory not available - inbox read disabled`);
        return [];
      }

      const top = Math.min(Math.max(options?.top || 10, 1), 25);
      const select = 'id,subject,from,receivedDateTime,importance,isRead,bodyPreview,conversationId,webLink,categories,flag,body';
      const response = await this.graphGetWithClientCredentials(
        `/users/${encodeURIComponent(userId)}/mailFolders/inbox/messages?$top=${top}&$orderby=receivedDateTime desc&$select=${encodeURIComponent(select)}`
      );

      let messages = (response.data?.value || []).map((message: any) => this.mapMailMessage(message));

      if (options?.senderQuery) {
        const senderQuery = options.senderQuery.toLowerCase();
        // Also try without trailing 's' for possessive-like forms ("martins" → "martin")
        const senderQueryBase = senderQuery.replace(/s$/, '');
        messages = messages.filter((message: MailMessageSummary) => {
          const senderText = `${message.fromName} ${message.fromAddress}`.toLowerCase();
          return senderText.includes(senderQuery) ||
            (senderQueryBase !== senderQuery && senderText.includes(senderQueryBase));
        });
      }

      if (options?.unreadOnly) {
        messages = messages.filter((message: MailMessageSummary) => !message.isRead);
      }

      console.log(`[GRAPH_API] Loaded ${messages.length} inbox message(s) for ${userId}`);
      return messages;
    } catch (error: any) {
      const status = error?.response?.status;
      if (status === 401) {
        console.warn(`[GRAPH_API] Inbox read failed with 401 - token invalid or missing`);
      } else if (status === 403) {
        console.warn(`[GRAPH_API] Inbox read failed with 403`);
      } else {
        this.logGraphError(`Failed to read inbox for ${userId}`, error);
      }
      return [];
    }
  }

  async getMailConversationMessages(
    userId: string,
    conversationId: string,
    top: number = 10
  ): Promise<MailMessageSummary[]> {
    try {
      if (!this.tokenFactory || !conversationId) {
        return [];
      }

      const safeConversationId = conversationId.replace(/'/g, "''");
      const select = 'id,subject,from,receivedDateTime,importance,isRead,bodyPreview,conversationId,webLink,categories,flag,body';
      // Note: Graph does not allow $filter + $orderby together on messages (400 "restriction too complex").
      // Fetch without ordering, then sort client-side.
      const response = await this.graphGetWithClientCredentials(
        `/users/${encodeURIComponent(userId)}/messages?$top=${Math.min(Math.max(top, 1), 20)}&$filter=${encodeURIComponent(`conversationId eq '${safeConversationId}'`)}&$select=${encodeURIComponent(select)}`
      );

      const msgs = (response.data?.value || []).map((message: any) => this.mapMailMessage(message)) as MailMessageSummary[];
      return msgs.sort((a, b) => new Date(b.receivedDateTime).getTime() - new Date(a.receivedDateTime).getTime());
    } catch (error) {
      this.logGraphError(`Failed to read mail conversation for ${userId}`, error);
      return [];
    }
  }

  /**
   * Read chat messages from a team chat
   */
  async getChatMessages(chatId: string, limit: number = 50): Promise<ChatMessage[]> {
    try {
      if (!this.tokenFactory) {
        console.warn(`[GRAPH_API] Token factory not available - Graph API disabled (local development mode). Using fallback to stored messages.`);
        return [];
      }

      console.log(`[GRAPH_API] Fetching ${limit} chat messages from: ${chatId}`);
      const response = await this.graphGetWithClientCredentials(
        `/chats/${chatId}/messages?$top=${limit}&$orderby=createdDateTime desc`
      );
      const count = response.data.value?.length || 0;
      console.log(`[GRAPH_API] Successfully fetched ${count} messages`);
      return response.data.value || [];
    } catch (error) {
      console.warn(`[GRAPH_API_WARN] Graph API unavailable - will use stored messages as fallback`)
      if ((error as any)?.response?.status === 401) {
        console.warn(`[GRAPH_API] Received 401 Unauthorized - Graph API credentials not available in local development`);
      } else if ((error as any)?.response?.status === 403) {
        console.warn(`[GRAPH_API] Received 403 Forbidden for chat read`);
      } else {
        this.logGraphError(`Failed to fetch chat messages for ${chatId}`, error);
      }
      return [];
    }
  }

  /**
   * Get chat metadata including participants
   */
  async getChatInfo(chatId: string) {
    try {
      if (!this.tokenFactory) {
        console.warn(`[GRAPH_API] Graph API disabled (local development) - returning empty chat info`);
        return null;
      }

      console.log(`[GRAPH_API] Fetching chat info for: ${chatId}`);
      const response = await this.graphGetWithClientCredentials(`/chats/${chatId}`);
      console.log(`[GRAPH_API] Successfully fetched chat info with ${response.data.members?.length || 0} members`);
      return response.data;
    } catch (error) {
      if ((error as any)?.response?.status === 401) {
        console.warn(`[GRAPH_API] Received 401 Unauthorized for chat info - using fallback`);
      } else if ((error as any)?.response?.status === 403) {
        console.warn(`[GRAPH_API] Received 403 Forbidden for chat info`);
      } else {
        this.logGraphError(`Failed to fetch chat info for ${chatId}`, error);
      }
      return null;
    }
  }

  /**
   * Get members of a chat.
   * GET /chats/{chatId}/members
   * Returns array of member display names (excluding bots).
   */
  async getChatMembers(chatId: string): Promise<string[]> {
    try {
      if (!this.tokenFactory) return [];
      console.log(`[GRAPH_API] Fetching chat members for: ${chatId}`);
      const response = await this.graphGetWithClientCredentials(`/chats/${chatId}/members`);
      const members = response.data?.value || [];
      const names: string[] = members
        .map((m: any) => m.displayName)
        .filter(
          (n: string) =>
            n &&
            !n.toLowerCase().includes('bot') &&
            n.toLowerCase() !== 'assistant'
        );
      console.log(`[GRAPH_API] Found ${names.length} human members`);
      return names;
    } catch (error: any) {
      const status = error?.response?.status;
      console.warn(`[GRAPH_API] Could not fetch chat members (status=${status})`);
      return [];
    }
  }

  /**
   * Get members of a chat with emails.
   * GET /chats/{chatId}/members
   * Returns array of member objects with displayName and email (excluding bots).
   */
  async getChatMembersDetailed(chatId: string): Promise<{ displayName: string; email: string; userId?: string }[]> {
    try {
      if (!this.tokenFactory) return [];
      console.log(`[GRAPH_API] Fetching detailed chat members for: ${chatId}`);
      const response = await this.graphGetWithClientCredentials(`/chats/${chatId}/members`);
      const members = response.data?.value || [];
      const detailed = members
        .filter((m: any) => {
          const name = (m.displayName || '').toLowerCase();
          return name && !name.includes('bot') && name !== 'assistant';
        })
        .map((m: any) => ({
          displayName: m.displayName || 'Unknown',
          email: m.email || m.microsoft?.graph?.user?.mail || '',
          userId: m.userId || m.id?.split("'")[1] || ''
        }));
      console.log(`[GRAPH_API] Found ${detailed.length} detailed members`);
      return detailed;
    } catch (error: any) {
      const status = error?.response?.status;
      console.warn(`[GRAPH_API] Could not fetch detailed chat members (status=${status})`);
      return [];
    }
  }

  /**
   * Find a member's email by partial name match (case-insensitive).
   * Returns the best match or null if no match found.
   */
  async findMemberEmailByName(chatId: string, searchName: string): Promise<{ displayName: string; email: string } | null> {
    const members = await this.getChatMembersDetailed(chatId);
    if (members.length === 0) return null;

    const searchLower = searchName.toLowerCase().trim();
    
    // First try exact match
    let match = members.find(m => m.displayName.toLowerCase() === searchLower);
    
    // Then try starts with (first name match)
    if (!match) {
      match = members.find(m => m.displayName.toLowerCase().startsWith(searchLower));
    }
    
    // Then try contains
    if (!match) {
      match = members.find(m => m.displayName.toLowerCase().includes(searchLower));
    }
    
    // Finally try fuzzy - first word of display name matches search
    if (!match) {
      match = members.find(m => {
        const firstName = m.displayName.split(' ')[0].toLowerCase();
        return firstName === searchLower || searchLower.includes(firstName) || firstName.includes(searchLower);
      });
    }

    if (match && match.email) {
      console.log(`[GRAPH_API] Found member "${match.displayName}" with email "${match.email}" for search "${searchName}"`);
      return { displayName: match.displayName, email: match.email };
    }
    
    console.log(`[GRAPH_API] No email found for member "${searchName}"`);
    return null;
  }

  /**
   * List user's recent chats and search by topic/name.
   * GET /users/{userId}/chats
   * Returns chats matching the search query (if provided) or all recent chats.
   */
  async getUserChats(userId: string, searchQuery?: string, limit: number = 50): Promise<{ id: string; topic: string; chatType: string; lastUpdated?: string }[]> {
    try {
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        console.warn(`[GRAPH_API] No token available for getUserChats`);
        return [];
      }

      console.log(`[GRAPH_API] Fetching chats for user ${userId}${searchQuery ? ` (searching: "${searchQuery}")` : ''}`);
      
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${userId}/chats?$top=${limit}&$expand=members&$orderby=lastUpdatedDateTime desc`,
        {
          headers: { Authorization: `Bearer ${token}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );

      const chats = response.data?.value || [];
      console.log(`[GRAPH_API] Found ${chats.length} chats for user`);

      // Map to simpler structure
      const mappedChats = chats.map((chat: any) => ({
        id: chat.id,
        topic: chat.topic || chat.members?.map((m: any) => m.displayName).filter((n: string) => n).join(', ') || 'Unnamed chat',
        chatType: chat.chatType,
        lastUpdated: chat.lastUpdatedDateTime,
      }));

      // If search query provided, filter by topic
      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        const filtered = mappedChats.filter((chat: any) => 
          chat.topic?.toLowerCase().includes(query)
        );
        console.log(`[GRAPH_API] Filtered to ${filtered.length} chats matching "${searchQuery}"`);
        return filtered;
      }

      return mappedChats;
    } catch (error: any) {
      const status = error?.response?.status;
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      console.error(`[GRAPH_API] Failed to fetch user chats: status=${status}, error=${errMsg}`);
      return [];
    }
  }

  /**
   * Retrieve recordings from a call or meeting
   */
  async getMeetingRecordings(meetingId: string) {
    try {
      if (!this.tokenFactory) {
        console.warn(`[GRAPH_API] Graph API disabled (local development) - no recordings available`);
        return [];
      }

      console.log(`[GRAPH_API] Fetching recordings for meeting: ${meetingId}`);
      const response = await this.graphClient.get(
        `/me/onlineMeetings/${meetingId}/recordings`
      );
      const count = response.data.value?.length || 0;
      console.log(`[GRAPH_API] Found ${count} recordings`);
      return response.data.value || [];
    } catch (error) {
      if ((error as any)?.response?.status === 401) {
        console.warn(`[GRAPH_API] Received 401 Unauthorized for recordings - Graph API not available`);
      } else if ((error as any)?.response?.status === 403) {
        console.warn(`[GRAPH_API] Received 403 Forbidden for recordings`);
      } else {
        this.logGraphError(`Failed to fetch recordings for ${meetingId}`, error);
      }
      return [];
    }
  }

  /**
   * Get the online meeting linked to a chat thread (meeting chats expose onlineMeetingInfo)
   */
  async getOnlineMeetingFromChat(chatId: string): Promise<OnlineMeetingInfo | null> {
    try {
      if (!this.tokenFactory) return null;
      console.log(`[GRAPH_API] Fetching online meeting info for chat: ${chatId}`);
      // Use GRAPH_CLIENT_ID credentials which have Chat.Read.All permission
      const response = await this.graphGetWithClientCredentials(`/chats/${chatId}`);
      const meetingInfo = response.data?.onlineMeetingInfo;
      const subject = response.data?.topic;

      let baseInfo: OnlineMeetingInfo | null = null;
      if (meetingInfo) {
        console.log(`[GRAPH_API] Got meeting info from chat:`);
        console.log(`[GRAPH_API]   - chat organizer ID: ${meetingInfo.organizer?.id || 'NONE'}`);
        console.log(`[GRAPH_API]   - chat tenantId: ${meetingInfo.organizer?.tenantId || 'NONE'}`);
        console.log(`[GRAPH_API]   - joinWebUrl: ${meetingInfo.joinWebUrl || 'NONE'}`);
        
        // Extract authoritative organizer ID from joinWebUrl (it's encoded in the URL)
        const urlOrganizerId = this.extractOrganizerIdFromJoinWebUrl(meetingInfo.joinWebUrl);
        console.log(`[GRAPH_API]   - URL-extracted organizer: ${urlOrganizerId || 'PARSE FAILED'}`);
        
        let organizerId = meetingInfo.organizer?.id;
        let tenantId = meetingInfo.organizer?.tenantId;
        
        if (urlOrganizerId && urlOrganizerId !== organizerId) {
          console.warn(`[GRAPH_API] ⚠️ Organizer ID MISMATCH: chat='${organizerId}' vs URL='${urlOrganizerId}' — using URL value`);
          organizerId = urlOrganizerId;
        } else if (urlOrganizerId) {
          console.log(`[GRAPH_API]   ✓ Organizer IDs match: ${organizerId}`);
        }
        
        console.log(`[GRAPH_API]   → Final organizer ID: ${organizerId}`);
        
        baseInfo = {
          joinWebUrl: meetingInfo.joinWebUrl,
          organizer: { id: organizerId, tenantId },
          subject,
        };
      } else {
        console.warn(`[GRAPH_API] No onlineMeetingInfo found for chat ${chatId}; trying recent-message fallback`);
        baseInfo = await this.buildMeetingInfoFromRecentMessages(chatId, subject);
        if (!baseInfo) {
          return null;
        }
      }

      // Try to enrich with joinMeetingId by querying the online meeting resource
      // Requires OnlineMeetings.Read.All — uses Graph API credentials
      if (baseInfo.joinWebUrl && baseInfo.organizer?.id) {
        try {
          const graphToken = await this.getTokenUsingClientCredentials();  // ← USE GRAPH TOKEN
          if (!graphToken) throw new Error('No graph token');
          const encodedUrl = encodeURIComponent(baseInfo.joinWebUrl);
          const meetingRes = await axios.get(
            `https://graph.microsoft.com/v1.0/users/${baseInfo.organizer.id}/onlineMeetings?$filter=joinWebUrl eq '${decodeURIComponent(encodedUrl)}'`,
            {
              headers: { Authorization: `Bearer ${graphToken}` },
              timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
            }
          );
          const onlineMeeting = meetingRes.data?.value?.[0];
          if (onlineMeeting?.joinMeetingIdSettings?.joinMeetingId) {
            baseInfo.joinMeetingId = onlineMeeting.joinMeetingIdSettings.joinMeetingId;
            baseInfo.passcode = onlineMeeting.joinMeetingIdSettings.passcode || '';
            baseInfo.onlineMeetingId = onlineMeeting.id || baseInfo.onlineMeetingId;
            console.log(`[GRAPH_API] Got joinMeetingId: ${baseInfo.joinMeetingId}`);
          } else if (onlineMeeting?.id) {
            baseInfo.onlineMeetingId = onlineMeeting.id;
            console.log(`[GRAPH_API] Online meeting found but no joinMeetingId (id=${onlineMeeting.id})`);
          } else {
            console.warn(`[GRAPH_API] No online meeting found via joinWebUrl filter`);
          }
          // Capture meeting start/end dates for accurate timestamping in summaries
          if (onlineMeeting?.startDateTime) {
            baseInfo.startDateTime = onlineMeeting.startDateTime;
            console.log(`[GRAPH_API] Got startDateTime: ${baseInfo.startDateTime}`);
          }
          if (onlineMeeting?.endDateTime) {
            baseInfo.endDateTime = onlineMeeting.endDateTime;
          }
        } catch (enrichErr: any) {
          const status = enrichErr?.response?.status;
          if (status === 403) {
            // Application Access Policy not configured - this is expected, use live transcription instead
            console.log(`[GRAPH_API] joinMeetingId enrichment skipped (403) - will use live transcription`);
          } else {
            const errMsg = enrichErr?.response?.data?.error?.message || '';
            console.warn(`[GRAPH_API] Could not fetch joinMeetingId (status=${status}): ${errMsg}`);
          }
        }
      }

      console.log(`[GRAPH_API] getOnlineMeetingFromChat returning baseInfo: organizer=${baseInfo?.organizer?.id}, joinWebUrl=${baseInfo?.joinWebUrl ? 'yes' : 'no'}`);
      return baseInfo;
    } catch (error) {
      this.logGraphError(`Failed to get online meeting info for chat ${chatId}`, error);
      return null;
    }
  }

  /**
   * Get an access token using the BOT app credentials (CLIENT_ID).
   * Used specifically for the Communications Calls API which requires the registered bot identity.
   * On Azure with UserAssignedMsi, uses managed identity (permissions must be granted via PowerShell).
   * Locally, uses CLIENT_ID/CLIENT_SECRET client credentials flow.
   */
  private async getTokenUsingBotCredentials(): Promise<string> {
    try {
      const clientId = process.env.CLIENT_ID;
      const clientSecret = process.env.CLIENT_SECRET;
      const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;

      const isUserAssignedMsi = (process.env.BOT_TYPE || '').toLowerCase() === 'userassignedmsi';
      if (isUserAssignedMsi || !clientSecret) {
        console.log(`[CALLS_AUTH] Using Managed Identity token for Calls API (app: ${clientId || 'n/a'})`);
        return await this.getManagedIdentityToken('https://graph.microsoft.com/.default');
      }

      if (!clientId || !clientSecret || !tenantId) {
        console.warn(`[CALLS_AUTH] Missing bot credentials: CLIENT_ID=${!!clientId}, CLIENT_SECRET=${!!clientSecret}, TENANT_ID=${!!tenantId}`);
        return '';
      }

      console.log(`[CALLS_AUTH] Requesting bot app token for Calls API (app: ${clientId})`);
      const form = new URLSearchParams();
      form.append('client_id', clientId);
      form.append('client_secret', clientSecret);
      form.append('scope', 'https://graph.microsoft.com/.default');
      form.append('grant_type', 'client_credentials');

      const response = await axios.post(
        `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
        form.toString(),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      console.log(`[CALLS_AUTH] Bot app token obtained (expires in ${response.data.expires_in}s)`);
      return response.data.access_token;
    } catch (error) {
      console.error(`[CALLS_AUTH_ERROR] Failed to obtain bot app token:`, error);
      return '';
    }
  }

  /**
   * Public getter for bot credentials token.
   * Used by external modules (like autoTranscription) that need to call Graph APIs
   * that require the bot app identity (e.g., Communications Calls API).
   */
  async getBotToken(): Promise<string | null> {
    const token = await this.getTokenUsingBotCredentials();
    return token || null;
  }

  /**
   * Join a Teams meeting as a bot participant using the Graph Communications Calls API.
   * Requires Calls.JoinGroupCall.All application permission on the BOT app (CLIENT_ID).
   * The bot will appear in the meeting participant list.
   */
  async joinMeetingCall(
    meetingInfo: OnlineMeetingInfo,
    callbackUri: string,
    tenantId: string,
    chatThreadId?: string
  ): Promise<CallInfo | null> {
    try {
      if (!meetingInfo.organizer?.id) {
        console.warn(`[CALLS_API] Missing organizer ID - cannot join meeting`);
        return null;
      }

      console.log(`[CALLS_API] Attempting to join meeting as bot participant`);
      console.log(`[CALLS_API] Organizer: ${meetingInfo.organizer.id}, Tenant: ${tenantId}`);
      console.log(`[CALLS_API] Callback URI: ${callbackUri}`);

      // Use bot app token (not Graph app token) - the Calls API requires the registered bot identity
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) {
        console.warn(`[CALLS_API] Could not obtain bot credentials token`);
        return null;
      }

      // Prefer joinMeetingIdMeetingInfo (more reliable) over organizerMeetingInfo
      let meetingInfoPayload: object;
      if (meetingInfo.joinMeetingId) {
        console.log(`[CALLS_API] Using joinMeetingIdMeetingInfo, joinMeetingId: ${meetingInfo.joinMeetingId}`);
        meetingInfoPayload = {
          '@odata.type': '#microsoft.graph.joinMeetingIdMeetingInfo',
          joinMeetingId: meetingInfo.joinMeetingId,
          ...(meetingInfo.passcode ? { passcode: meetingInfo.passcode } : {}),
        };
      } else {
        console.log(`[CALLS_API] Using organizerMeetingInfo (no joinMeetingId available), organizer: ${meetingInfo.organizer!.id}, tenant: ${meetingInfo.organizer!.tenantId || tenantId}`);
        meetingInfoPayload = {
          '@odata.type': '#microsoft.graph.organizerMeetingInfo',
          organizer: {
            '@odata.type': '#microsoft.graph.identitySet',
            user: {
              '@odata.type': '#microsoft.graph.identity',
              id: meetingInfo.organizer!.id,
              tenantId: meetingInfo.organizer!.tenantId || tenantId,
            },
          },
          allowConversationWithoutHost: true,
        };
      }

      const callPayload: Record<string, any> = {
        '@odata.type': '#microsoft.graph.call',
        callbackUri,
        requestedModalities: ['audio'],
        mediaConfig: {
          '@odata.type': '#microsoft.graph.serviceHostedMediaConfig',
        },
        meetingInfo: meetingInfoPayload,
        tenantId,
      };

      // Include chatInfo with the meeting thread ID - required for joining scheduled meetings
      if (chatThreadId) {
        callPayload.chatInfo = {
          '@odata.type': '#microsoft.graph.chatInfo',
          threadId: chatThreadId,
          messageId: '0',
        };
        console.log(`[CALLS_API] Including chatInfo, threadId: ${chatThreadId}`);
      }

      // Use a direct axios call with the bot token (not graphClient which uses Graph app token)
      const response = await axios.post(
        'https://graph.microsoft.com/v1.0/communications/calls',
        callPayload,
        { headers: { Authorization: `Bearer ${botToken}`, 'Content-Type': 'application/json' } }
      );
      const call = response.data;
      console.log(`[CALLS_API] Successfully joined meeting call. Call ID: ${call.id}, State: ${call.state}`);
      return { id: call.id, state: call.state, callbackUri };
    } catch (error) {
      this.logGraphError('Failed to join meeting via Calls API', error);
      return null;
    }
  }

  /**
   * Answer an incoming call notification from Teams.
   * Must be called within ~15 seconds of receiving the 'establishing' webhook event.
   */
  async answerCall(callId: string): Promise<boolean> {
    try {
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) {
        console.warn(`[CALLS_API] Could not get bot token for answerCall`);
        return false;
      }

      console.log(`[CALLS_API] Answering call: ${callId}`);
      await axios.post(
        `https://graph.microsoft.com/v1.0/communications/calls/${callId}/answer`,
        {
          callbackUri: process.env.BOT_ENDPOINT ? `${process.env.BOT_ENDPOINT}/api/calls` : undefined,
          requestedModalities: [],
          mediaConfig: {
            '@odata.type': '#microsoft.graph.serviceHostedMediaConfig',
          },
        },
        { headers: { Authorization: `Bearer ${botToken}`, 'Content-Type': 'application/json' } }
      );
      console.log(`[CALLS_API] Call answered successfully: ${callId}`);
      return true;
    } catch (error) {
      this.logGraphError(`Failed to answer call ${callId}`, error);
      return false;
    }
  }

  /**
   * Hang up / leave an active call.
   */
  async hangUp(callId: string): Promise<boolean> {
    try {
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) {
        console.warn(`[CALLS_API] Could not get bot token for hangUp`);
        return false;
      }

      console.log(`[CALLS_API] Hanging up call: ${callId}`);
      await axios.delete(
        `https://graph.microsoft.com/v1.0/communications/calls/${callId}`,
        { headers: { Authorization: `Bearer ${botToken}` } }
      );
      console.log(`[CALLS_API] Call hung up successfully: ${callId}`);
      return true;
    } catch (error) {
      this.logGraphError(`Failed to hang up call ${callId}`, error);
      return false;
    }
  }

  /**
   * Get participants in an active call.
   * Returns the list of participant objects from the Graph API.
   */
  async getCallParticipants(callId: string): Promise<any[]> {
    try {
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) return [];

      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/communications/calls/${callId}/participants`,
        { headers: { Authorization: `Bearer ${botToken}` } }
      );
      return response.data?.value || [];
    } catch (error) {
      this.logGraphError(`Failed to get participants for call ${callId}`, error);
      return [];
    }
  }

  /**
   * Start transcription on an active call.
   * Requires Calls.AccessMedia.All application permission.
   * Uses the beta API as startTranscription is not yet in v1.0.
   */
  async startTranscription(callId: string): Promise<boolean> {
    try {
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) {
        console.warn(`[TRANSCRIPTION] Could not get bot token for startTranscription`);
        return false;
      }

      console.log(`[TRANSCRIPTION] Starting transcription on call: ${callId}`);
      await axios.post(
        `https://graph.microsoft.com/beta/communications/calls/${callId}/startTranscription`,
        {
          languageTag: 'en-US',
          singlePerParticipant: false,
        },
        { headers: { Authorization: `Bearer ${botToken}`, 'Content-Type': 'application/json' } }
      );
      console.log(`[TRANSCRIPTION] Transcription started on call: ${callId}`);
      return true;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      console.error(`[TRANSCRIPTION_ERROR] Failed to start transcription on call ${callId}: status=${status}, message=${msg}`);
      // Log the full error body for debugging
      if (error?.response?.data) {
        console.error(`[TRANSCRIPTION_ERROR] Full response:`, JSON.stringify(error.response.data));
      }
      return false;
    }
  }

  /**
   * Stop transcription on an active call.
   */
  async stopTranscription(callId: string): Promise<boolean> {
    try {
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) return false;

      console.log(`[TRANSCRIPTION] Stopping transcription on call: ${callId}`);
      await axios.post(
        `https://graph.microsoft.com/beta/communications/calls/${callId}/stopTranscription`,
        {},
        { headers: { Authorization: `Bearer ${botToken}`, 'Content-Type': 'application/json' } }
      );
      console.log(`[TRANSCRIPTION] Transcription stopped on call: ${callId}`);
      return true;
    } catch (error) {
      this.logGraphError(`Failed to stop transcription on call ${callId}`, error);
      return false;
    }
  }

  /**
   * Get participants in a meeting
   */
  async getMeetingParticipants(meetingId: string) {
    try {
      const response = await this.graphClient.get(
        `/me/onlineMeetings/${meetingId}/attendanceReports`
      );
      return response.data.value || [];
    } catch (error) {
      console.error(`Error fetching meeting participants for ${meetingId}:`, error);
      return [];
    }
  }

  /**
   * Transcribe audio using Azure Cognitive Services
   */
  async transcribeAudio(audioUrl: string, language: string = 'en-US'): Promise<string> {
    try {
      if (!config.cognitiveServicesEndpoint || !config.cognitiveServicesKey) {
        console.warn('Cognitive Services not configured for transcription');
        return '';
      }

      const response = await axios.post(
        `${config.cognitiveServicesEndpoint}/speech/recognition/conversation/cognitiveservices/v1?language=${language}`,
        await axios.get(audioUrl, { responseType: 'arraybuffer' }).then((r) => r.data),
        {
          headers: {
            'Ocp-Apim-Subscription-Key': config.cognitiveServicesKey,
            'Content-Type': 'audio/wav',
          },
        }
      );

      return response.data.DisplayText || '';
    } catch (error) {
      console.error('Error transcribing audio:', error);
      return '';
    }
  }

  /**
   * Get a Bot Connector API token (scope: api.botframework.com) for proactive messaging.
   */
  private async getBotConnectorToken(): Promise<string> {
    try {
      const clientId = process.env.CLIENT_ID;
      const clientSecret = process.env.CLIENT_SECRET;
      const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;

      const isUserAssignedMsi = (process.env.BOT_TYPE || '').toLowerCase() === 'userassignedmsi';
      if (isUserAssignedMsi || !clientSecret) {
        return await this.getManagedIdentityToken('https://api.botframework.com/.default');
      }

      if (!clientId || !clientSecret || !tenantId) return '';

      const form = new URLSearchParams();
      form.append('client_id', clientId);
      form.append('client_secret', clientSecret);
      form.append('scope', 'https://api.botframework.com/.default');
      form.append('grant_type', 'client_credentials');

      const response = await axios.post(
        `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
        form.toString(),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      return response.data.access_token;
    } catch (error) {
      console.error(`[BOT_CONNECTOR_AUTH_ERROR] Failed to obtain Bot Connector token:`, error);
      return '';
    }
  }

  /**
   * Send a proactive message to a Teams conversation using the Bot Connector REST API.
   * @param serviceUrl - The serviceUrl from the original Teams activity (e.g. https://smba.trafficmanager.net/uk/)
   * @param conversationId - The conversation/thread ID to message
   * @param text - Message text (markdown supported)
   */
  /**
   * Get the online meeting resource ID for a given organizer + joinWebUrl.
   * Requires OnlineMeetings.Read.All — uses Graph API credentials.
   * NOTE: This endpoint has stricter access requirements when querying by joinWebUrl.
   * During active calls, we rely on live transcription polling instead.
   */
  async getOnlineMeetingId(organizerId: string, joinWebUrl: string): Promise<string | null> {
    const cacheKey = this.getMeetingLookupCacheKey(organizerId, joinWebUrl);
    try {
      const deniedUntil = this.meetingIdLookupDeniedUntil.get(cacheKey) || 0;
      if (deniedUntil > Date.now()) {
        console.log(`[GRAPH_API] Skipping meeting ID lookup due to recent 403 cache for organizer=${organizerId}`);
        return null;
      }

      const existingRequest = this.meetingIdLookupInFlight.get(cacheKey);
      if (existingRequest) {
        return await existingRequest;
      }

      const lookupPromise = (async (): Promise<string | null> => {
        const graphToken = await this.getTokenUsingClientCredentials();  // ← USE GRAPH TOKEN
        if (!graphToken) return null;
        const encodedUrl = encodeURIComponent(joinWebUrl);
        const response = await axios.get(
          `https://graph.microsoft.com/beta/users/${organizerId}/onlineMeetings?$filter=joinWebUrl eq '${decodeURIComponent(encodedUrl)}'`,
          {
            headers: { Authorization: `Bearer ${graphToken}` },
            timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
          }
        );
        const meeting = response.data?.value?.[0];
        if (meeting?.id) {
          console.log(`[GRAPH_API] Resolved online meeting ID: ${meeting.id}`);
          return meeting.id;
        }
        console.warn(`[GRAPH_API] No online meeting found for joinWebUrl`);
        return null;
      })();

      this.meetingIdLookupInFlight.set(cacheKey, lookupPromise);
      return await lookupPromise;
    } catch (error: any) {
      const status = error?.response?.status;
      if (status === 403) {
        // Cache for 5 minutes - no Application Access Policy, live transcription is primary
        this.meetingIdLookupDeniedUntil.set(cacheKey, Date.now() + (5 * 60 * 1000));
        console.log(`[GRAPH_API] Meeting ID lookup skipped (403 - no Application Access Policy) - using live transcription`);
      } else {
        const errMsg = error?.response?.data?.error?.message || '';
        console.warn(`[GRAPH_API] Could not resolve meeting ID (status=${status}): ${errMsg}`);
      }
      return null;
    } finally {
      this.meetingIdLookupInFlight.delete(cacheKey);
    }
  }

  /**
   * List transcripts for an online meeting.
   * GET /users/{organizerId}/onlineMeetings/{meetingId}/transcripts
   * Requires OnlineMeetingTranscript.Read.All
   * Handles pagination via @odata.nextLink
   */
  async listMeetingTranscripts(organizerId: string, meetingId: string): Promise<any[]> {
    try {
      const graphToken = await this.getTokenUsingClientCredentials();
      if (!graphToken) return [];
      console.log(`[GRAPH_API] Listing transcripts for meeting ${meetingId}`);
      
      let allTranscripts: any[] = [];
      let url: string | null = `https://graph.microsoft.com/v1.0/users/${organizerId}/onlineMeetings/${meetingId}/transcripts`;
      
      while (url) {
        const response = await axios.get(url, {
          headers: { Authorization: `Bearer ${graphToken}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        });
        const transcripts = response.data?.value || [];
        allTranscripts = allTranscripts.concat(transcripts);
        url = response.data?.['@odata.nextLink'] || null;
      }
      
      console.log(`[GRAPH_API] Found ${allTranscripts.length} transcript(s)`);
      for (const t of allTranscripts) {
        console.log(`[GRAPH_API]   Transcript: id=${t.id}, createdDateTime=${t.createdDateTime}`);
      }
      return allTranscripts;
    } catch (error: any) {
      const status = error?.response?.status;
      if (status === 403) {
        // No Application Access Policy - live transcription is primary
        console.log(`[GRAPH_API] listMeetingTranscripts skipped (403 - no Application Access Policy)`);
        return [];
      }
      const msg = error?.response?.data?.error?.message || error?.message;
      console.warn(`[GRAPH_API] Could not list transcripts (status=${status}): ${msg}`);
      return [];
    }
  }

  /**
   * Fetch the live transcript for an active call using the Communications calls API.
   * This works without needing a meetingId or joinWebUrl — it uses the bot's own callId.
   * Ideal for 1:1 calls and Meet Now calls where getAllTranscripts returns no matching results.
   * GET /beta/communications/calls/{callId}/transcripts
   * GET /beta/communications/calls/{callId}/transcripts/{id}/content
   */
  async fetchCallTranscriptContent(callId: string): Promise<string | null> {
    try {
      // Use bot credentials — the bot app (CLIENT_ID) owns the call, so only its
      // token can access /communications/calls/{callId} resources.
      const botToken = await this.getTokenUsingBotCredentials();
      if (!botToken) return null;

      const listUrl = `https://graph.microsoft.com/beta/communications/calls/${callId}/transcripts`;
      console.log(`[GRAPH_API] Fetching call transcript list for callId=${callId} (using bot credentials)`);
      const listResp = await axios.get(listUrl, {
        headers: { Authorization: `Bearer ${botToken}` },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
      });

      const transcripts: any[] = listResp.data?.value || [];
      if (transcripts.length === 0) {
        console.log(`[GRAPH_API] No transcripts yet for callId=${callId} via communications API`);
        return null;
      }

      transcripts.sort((a: any, b: any) =>
        new Date(b.createdDateTime || 0).getTime() - new Date(a.createdDateTime || 0).getTime()
      );
      const latest = transcripts[0];
      console.log(`[GRAPH_API] Found ${transcripts.length} call transcript(s) via communications API, using id=${latest.id}`);

      const contentUrl = `https://graph.microsoft.com/beta/communications/calls/${callId}/transcripts/${latest.id}/content?$format=text/vtt`;
      const contentResp = await axios.get(contentUrl, {
        headers: { Authorization: `Bearer ${botToken}`, Accept: 'text/vtt' },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
      });

      const content = typeof contentResp.data === 'string' ? contentResp.data : JSON.stringify(contentResp.data);
      console.log(`[GRAPH_API] Downloaded call transcript via communications API (${content.length} chars)`);
      return content || null;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      console.warn(`[GRAPH_API] fetchCallTranscriptContent failed (status=${status}): ${msg}`);
      return null;
    }
  }

  /**
   * Fetch the transcript for a meeting/call using the chat endpoint.
   * GET /chats/{chatId}/transcripts  (the chatId IS the meeting thread conversation ID)
   * This is the most universal strategy — works for 1:1 calls, Meet Now, group calls,
   * and scheduled meetings, without needing a meetingId or organizer lookup.
   * Requires OnlineMeetingTranscript.Read.All.
   * @param chatId - The chat/conversation ID
   * @param targetCallId - Optional callId to filter to only the current call's transcript
   * @param force - If true, bypass cooldown cache (for explicit user requests)
   */
  async fetchChatTranscriptText(chatId: string, targetCallId?: string, force: boolean = false): Promise<string | null> {
    try {
      const deniedUntil = this.chatTranscriptDeniedUntil.get(chatId) || 0;
      if (!force && deniedUntil > Date.now()) {
        const nextSkipLogAt = this.chatTranscriptSkipLogUntil.get(chatId) || 0;
        if (Date.now() >= nextSkipLogAt) {
          this.chatTranscriptSkipLogUntil.set(chatId, Date.now() + 60_000);
          console.log(`[GRAPH_API] Skipping /chats transcript lookup for ${chatId} during cooldown`);
        }
        return null;
      }
      
      // Clear cooldown if force is true (user explicitly requested)
      if (force && deniedUntil > Date.now()) {
        console.log(`[GRAPH_API] Force flag set - bypassing cooldown for ${chatId}`);
        this.chatTranscriptDeniedUntil.delete(chatId);
        this.chatTranscriptSkipLogUntil.delete(chatId);
      }

      const graphToken = await this.getTokenUsingClientCredentials();
      if (!graphToken) return null;

      const encodedChatId = encodeURIComponent(chatId);
      console.log(`[GRAPH_API] Fetching transcripts via /chats endpoint for: ${chatId}${targetCallId ? ` (filtering for callId=${targetCallId})` : ''}${force ? ' (forced)' : ''}`);
      const listResp = await axios.get(
        `https://graph.microsoft.com/beta/chats/${encodedChatId}/transcripts`,
        {
          headers: { Authorization: `Bearer ${graphToken}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );

      let transcripts: any[] = listResp.data?.value || [];
      if (transcripts.length === 0) {
        console.log(`[GRAPH_API] No transcripts found via /chats for ${chatId}`);
        return null;
      }

      // Log all transcripts with their metadata for debugging
      console.log(`[GRAPH_API] Found ${transcripts.length} transcript(s) via /chats:`);
      for (const t of transcripts) {
        console.log(`[GRAPH_API]   - id=${t.id}, callId=${t.callId || 'N/A'}, created=${t.createdDateTime}, meetingId=${t.meetingId || 'N/A'}`);
      }

      // If we have a target callId, filter to only that call's transcript
      if (targetCallId) {
        const filtered = transcripts.filter((t: any) => t.callId === targetCallId);
        if (filtered.length > 0) {
          console.log(`[GRAPH_API] Filtered to ${filtered.length} transcript(s) matching callId=${targetCallId}`);
          transcripts = filtered;
        } else {
          console.log(`[GRAPH_API] No transcripts match callId=${targetCallId}, will use most recent`);
        }
      }

      // Most recent first
      transcripts.sort((a: any, b: any) =>
        new Date(b.createdDateTime || 0).getTime() - new Date(a.createdDateTime || 0).getTime()
      );
      const latest = transcripts[0];
      console.log(`[GRAPH_API] Using transcript: id=${latest.id}, callId=${latest.callId || 'N/A'}, created=${latest.createdDateTime}`);

      // Download VTT content
      const contentResp = await axios.get(
        `https://graph.microsoft.com/beta/chats/${encodedChatId}/transcripts/${encodeURIComponent(latest.id)}/content?$format=text/vtt`,
        {
          headers: { Authorization: `Bearer ${graphToken}`, Accept: 'text/vtt' },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );
      const content = typeof contentResp.data === 'string' ? contentResp.data : JSON.stringify(contentResp.data);
      console.log(`[GRAPH_API] Downloaded chat transcript (${content.length} chars)`);
      return content || null;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      const isUnsupportedChatTranscriptEndpoint =
        status === 400 && /Resource not found for the segment 'transcripts'|segment 'transcripts'/i.test(msg || '');

      if (isUnsupportedChatTranscriptEndpoint) {
        // Hard failure for this chat type. Suppress repeated retries for a while.
        this.chatTranscriptDeniedUntil.set(chatId, Date.now() + (10 * 60 * 1000));
        this.chatTranscriptSkipLogUntil.set(chatId, Date.now() + 60_000);
        console.log(`[GRAPH_API] /chats transcript endpoint unavailable for this chat. Cooling down retries for 10 min.`);
        return null;
      }

      if (status === 403) {
        // No Application Access Policy - suppress noise, live transcription is primary
        this.chatTranscriptDeniedUntil.set(chatId, Date.now() + (5 * 60 * 1000));
        console.log(`[GRAPH_API] /chats transcript skipped (403 - no Application Access Policy)`);
        return null;
      }

      console.warn(`[GRAPH_API] fetchChatTranscriptText failed for ${chatId} (status=${status}): ${msg}`);
      return null;
    }
  }

  /**
   * List ALL transcripts for a user across ALL meetings (no meeting ID needed).
   * GET /users/{userId}/onlineMeetings/getAllTranscripts(meetingOrganizerUserId='{userId}')?$top=50
   * Requires OnlineMeetingTranscript.Read.All
   * Returns array of transcripts with meetingId, id, createdDateTime
   * NOTE: $orderby is NOT supported on this endpoint - sort client-side
   */
  async getAllTranscriptsForUser(userId: string, limit: number = 20): Promise<any[]> {
    try {
      const graphToken = await this.getTokenUsingClientCredentials();
      if (!graphToken) return [];
      console.log(`[GRAPH_API] Getting all transcripts for user ${userId}, limit=${limit}`);
      
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${userId}/onlineMeetings/getAllTranscripts(meetingOrganizerUserId='${userId}')?$top=${limit}`,
        {
          headers: { Authorization: `Bearer ${graphToken}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );
      
      let transcripts = response.data?.value || [];
      
      // Sort client-side by createdDateTime descending (newest first)
      transcripts.sort((a: any, b: any) => {
        const dateA = new Date(a.createdDateTime || 0).getTime();
        const dateB = new Date(b.createdDateTime || 0).getTime();
        return dateB - dateA;
      });
      
      console.log(`[GRAPH_API] Found ${transcripts.length} transcript(s) across all meetings`);
      for (const t of transcripts.slice(0, 5)) {
        console.log(`[GRAPH_API]   Transcript: id=${t.id}, meetingId=${t.meetingId}, created=${t.createdDateTime}`);
      }
      return transcripts;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      console.warn(`[GRAPH_API] Could not get all transcripts (status=${status}): ${msg}`);
      return [];
    }
  }

  /**
   * Download transcript content using the callTranscripts endpoint (no meetingId needed).
   * GET /users/{userId}/onlineMeetings/getAllTranscripts()?$filter=id eq '{transcriptId}'
   * or use the transcript metadata to get meetingId + transcriptId
   */
  async downloadTranscriptById(userId: string, meetingId: string, transcriptId: string): Promise<string | null> {
    const cacheKey = `${userId}:${meetingId}`;
    const deniedUntil = this.transcriptDownloadDeniedUntil.get(cacheKey) || 0;
    if (deniedUntil > Date.now()) {
      const nextSkipLogAt = this.transcriptDownloadSkipLogUntil.get(cacheKey) || 0;
      if (Date.now() >= nextSkipLogAt) {
        this.transcriptDownloadSkipLogUntil.set(cacheKey, Date.now() + 60_000);
        console.log(`[GRAPH_API] Transcript download denied (cooldown) for user=${userId.substring(0, 8)}...`);
      }
      return null;
    }
    try {
      const graphToken = await this.getTokenUsingClientCredentials();
      if (!graphToken) return null;
      console.log(`[GRAPH_API] Downloading transcript: meeting=${meetingId}, transcript=${transcriptId}`);
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${userId}/onlineMeetings/${meetingId}/transcripts/${transcriptId}/content?$format=text/vtt`,
        {
          headers: { Authorization: `Bearer ${graphToken}`, Accept: 'text/vtt' },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );
      const content = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      console.log(`[GRAPH_API] Downloaded transcript content (${content.length} chars)`);
      return content;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      if (status === 403) {
        // Cache 403 to prevent repeated log spam (5 min cooldown)
        this.transcriptDownloadDeniedUntil.set(cacheKey, Date.now() + (5 * 60 * 1000));
        if (msg?.includes('RSC permission')) {
          console.warn(`[GRAPH_API] Transcript access denied (403 RSC) - cooldown 5min: ${msg}`);
          console.warn(`[GRAPH_API] 💡 Fix: Admin must grant 'OnlineMeetingTranscript.Read.All' permission OR meeting organizer re-adds the app.`);
        } else {
          console.warn(`[GRAPH_API] Transcript download denied (403) - cooldown 5min: ${msg}`);
        }
      } else {
        console.warn(`[GRAPH_API] Could not download transcript by ID (status=${status}): ${msg}`);
      }
      return null;
    }
  }

  /**
   * Download the content of a specific meeting transcript.
   * Tries multiple endpoints: beta API, then v1.0 API
   * Returns plain text (vtt format by default)
   */
  async downloadTranscriptContent(organizerId: string, meetingId: string, transcriptId: string): Promise<string | null> {
    const graphToken = await this.getTokenUsingClientCredentials();
    if (!graphToken) return null;
    console.log(`[GRAPH_API] Downloading transcript content: ${transcriptId}`);
    
    // Try beta API first (sometimes has better permission handling)
    try {
      console.log(`[GRAPH_API] Trying beta endpoint for transcript download`);
      const response = await axios.get(
        `https://graph.microsoft.com/beta/users/${organizerId}/onlineMeetings/${meetingId}/transcripts/${transcriptId}/content?$format=text/vtt`,
        {
          headers: { Authorization: `Bearer ${graphToken}`, Accept: 'text/vtt' },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );
      const content = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      console.log(`[GRAPH_API] Downloaded transcript content via beta (${content.length} chars)`);
      return content;
    } catch (betaError: any) {
      const betaStatus = betaError?.response?.status;
      console.warn(`[GRAPH_API] Beta endpoint failed (status=${betaStatus}), trying v1.0...`);
    }
    
    // Fallback to v1.0 API
    try {
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${organizerId}/onlineMeetings/${meetingId}/transcripts/${transcriptId}/content?$format=text/vtt`,
        {
          headers: { Authorization: `Bearer ${graphToken}`, Accept: 'text/vtt' },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );
      const content = typeof response.data === 'string' ? response.data : JSON.stringify(response.data);
      console.log(`[GRAPH_API] Downloaded transcript content via v1.0 (${content.length} chars)`);
      return content;
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      console.warn(`[GRAPH_API] Could not download transcript (status=${status}): ${msg}`);
      return null;
    }
  }

  /**
   * Fetch the full transcript text for a meeting.
   * Primary: Try to get meeting ID by joinWebUrl filter, then list its transcripts.
   * Scope is intentionally limited to the current meeting/group; no global transcript scans.
   * Returns VTT text or null.
   * @param organizerId - The organizer's user ID
   * @param joinWebUrl - The meeting join URL
   * @param minCreatedTimestamp - Optional timestamp (ms since epoch) for earliest transcript to consider
   * @param maxCreatedTimestamp - Optional timestamp (ms since epoch) for latest transcript to consider (with 5min grace)
   * @param conversationId - Optional conversation/thread ID to match transcripts against (for Meet Now calls)
   * @param knownMeetingId - Optional pre-resolved meeting ID
   * @param targetCallId - Optional callId to filter to only the current call's transcript
   * @param force - If true, bypass cooldown cache (for explicit user requests)
   */
  async fetchMeetingTranscriptText(
    organizerId: string,
    joinWebUrl: string,
    minCreatedTimestamp?: number,
    maxCreatedTimestamp?: number,
    conversationId?: string,
    knownMeetingId?: string,
    targetCallId?: string,
    force: boolean = false
  ): Promise<string | null> {
    try {
      let transcripts: any[] = [];
      let meetingId: string | null = null;
      
      // Strategy 0: Direct /chats/{chatId}/transcripts endpoint.
      // Most reliable path — the meeting conversation thread IS a valid chatId, works for
      // 1:1 calls, Meet Now, group and scheduled meetings without any meeting ID lookup.
      if (conversationId) {
        const chatContent = await this.fetchChatTranscriptText(conversationId, targetCallId, force);
        if (chatContent) {
          console.log(`[GRAPH_API] Strategy 0 success: got transcript via /chats for ${conversationId}`);
          return chatContent;
        }
        console.log(`[GRAPH_API] Strategy 0 miss for ${conversationId} — trying meeting ID lookup`);
      }

      // Helper to extract thread ID from base64-encoded meetingId
      const extractThreadIdFromMeetingId = (encodedMeetingId: string): string | null => {
        try {
          const decoded = Buffer.from(encodedMeetingId, 'base64').toString('utf-8');
          // Format: "1*{userId}*0**{threadId}" e.g., "1*ceb9...*0**19:meeting_xxx@thread.v2"
          const threadMatch = decoded.match(/19:meeting_[^@]+@thread\.v2/);
          if (threadMatch) {
            return threadMatch[0];
          }
          return null;
        } catch {
          return null;
        }
      };

      // Strategy 1: use known meeting ID when available, otherwise resolve by joinWebUrl
      meetingId = knownMeetingId || null;
      if (meetingId) {
        console.log(`[GRAPH_API] Using cached online meeting ID for transcript lookup: ${meetingId}`);
      } else {
        meetingId = await this.getOnlineMeetingId(organizerId, joinWebUrl);
      }
      
      if (meetingId) {
        // Got meeting ID - list transcripts for this specific meeting
        transcripts = await this.listMeetingTranscripts(organizerId, meetingId);
      } else {
        // Fallback: scan ALL transcripts for this organizer (up to 20 most recent)
        // This enables accessing past meeting transcripts when meeting ID lookup fails
        console.log(`[GRAPH_API] Meeting ID lookup failed - falling back to getAllTranscripts scan`);
        const allTranscripts = await this.getAllTranscriptsForUser(organizerId, 20);
        
        if (allTranscripts.length > 0) {
          // If we have a conversationId, filter transcripts to match the current meeting thread
          if (conversationId) {
            console.log(`[GRAPH_API] Filtering transcripts by conversation thread: ${conversationId}`);
            const matchingTranscripts = allTranscripts.filter((t: any) => {
              if (!t.meetingId) return false;
              const threadId = extractThreadIdFromMeetingId(t.meetingId);
              const matches = threadId === conversationId;
              if (matches) {
                console.log(`[GRAPH_API] Found matching transcript for thread ${conversationId}`);
              }
              return matches;
            });
            
            if (matchingTranscripts.length > 0) {
              transcripts = matchingTranscripts;
              console.log(`[GRAPH_API] Found ${matchingTranscripts.length} transcripts matching current conversation`);
            } else {
              console.log(`[GRAPH_API] No transcripts match current conversation thread - showing all recent transcripts`);
              // Return all transcripts as fallback so user can still access past meetings
              transcripts = allTranscripts;
            }
          } else {
            // No conversationId provided - use all transcripts
            transcripts = allTranscripts;
          }
        }
      }
      
      if (transcripts.length === 0) {
        console.log(`[GRAPH_API] No transcripts found for organizer ${organizerId}`);
        return null;
      }

      // Filter transcripts created within the specified time window (if provided)
      let filteredTranscripts = transcripts;
      if (minCreatedTimestamp || maxCreatedTimestamp) {
        filteredTranscripts = transcripts.filter((t: any) => {
          const created = new Date(t.createdDateTime || 0).getTime();
          
          // Check minimum time
          if (minCreatedTimestamp && created < minCreatedTimestamp) {
            return false;
          }
          
          // Check maximum time with a generous grace period.
          // Meet Now transcripts can appear well after call end while Teams finalizes processing.
          if (maxCreatedTimestamp) {
            const maxWithGrace = maxCreatedTimestamp + (60 * 60 * 1000);
            if (created > maxWithGrace) {
              return false;
            }
          }
          
          return true;
        });

        const minDate = minCreatedTimestamp ? new Date(minCreatedTimestamp).toISOString() : 'N/A';
        const maxDate = maxCreatedTimestamp ? new Date(maxCreatedTimestamp + (60 * 60 * 1000)).toISOString() : 'N/A';
        console.log(`[GRAPH_API] Filtered ${transcripts.length} transcripts to ${filteredTranscripts.length} created between ${minDate} and ${maxDate}`);
        
        if (filteredTranscripts.length === 0) {
          console.log(`[GRAPH_API] No transcripts found in the specified time window`);
          return null;
        }
      }

      // Sort by createdDateTime descending — pick the latest transcript
      filteredTranscripts.sort((a: any, b: any) => {
        const dateA = new Date(a.createdDateTime || 0).getTime();
        const dateB = new Date(b.createdDateTime || 0).getTime();
        return dateB - dateA;
      });

      const latest = filteredTranscripts[0];
      // Use meetingId from transcript metadata (getAllTranscripts) or from earlier lookup
      const transcriptMeetingId = latest.meetingId || meetingId;
      console.log(`[GRAPH_API] Using latest transcript: id=${latest.id}, meetingId=${transcriptMeetingId}, created=${latest.createdDateTime}`);

      if (!transcriptMeetingId) {
        console.warn(`[GRAPH_API] No meeting ID available for transcript download`);
        return null;
      }

      const content = await this.downloadTranscriptContent(organizerId, transcriptMeetingId, latest.id);
      return content || null;
    } catch (error) {
      this.logGraphError('fetchMeetingTranscriptText', error);
      return null;
    }
  }

  async sendProactiveMessage(serviceUrl: string, conversationId: string, text: string): Promise<boolean> {
    try {
      const token = await this.getBotConnectorToken();
      if (!token) {
        console.warn('[PROACTIVE] No Bot Connector token — cannot send proactive message');
        return false;
      }

      const url = `${serviceUrl.replace(/\/$/, '')}/v3/conversations/${encodeURIComponent(conversationId)}/activities`;
      console.log(`[PROACTIVE] Sending message to conversation: ${conversationId}`);

      await axios.post(url, {
        type: 'message',
        text
      }, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });

      console.log(`[PROACTIVE] Message sent successfully`);
      return true;
    } catch (error) {
      this.logGraphError('sendProactiveMessage', error);
      return false;
    }
  }

  /**
   * Send an email on behalf of the user.
   * Body content can be markdown or plain text - it will be converted to HTML.
   * Supports single recipient (string) or multiple recipients (string array).
   * 
   * @param options.sendIndependently - If true, sends separate emails to each recipient so one failure doesn't affect others
   */
  async sendEmail(
    userId: string, 
    toEmail: string | string[], 
    subject: string, 
    body: string, 
    options?: { 
      replyToEmail?: string; 
      replyToName?: string;
      sendIndependently?: boolean;  // Send separate emails to each recipient
    }
  ): Promise<{ 
    success: boolean; 
    error?: string; 
    sentTo?: string[];
    failedRecipients?: Array<{ email: string; error: string; reason?: string }>;
    partialSuccess?: boolean;  // true if some succeeded and some failed
  }> {
    // Normalize to array and filter out empty/invalid emails
    const recipients = (Array.isArray(toEmail) ? toEmail : [toEmail])
      .map(e => e?.trim())
      .filter(e => e && e.includes('@'));
    
    if (recipients.length === 0) {
      return { success: false, error: 'No valid email recipients provided' };
    }

    // For multiple recipients with sendIndependently, use the independent sending method
    if (recipients.length > 1 && options?.sendIndependently) {
      return this.sendEmailsIndependently(userId, recipients, subject, body, options);
    }

    // Standard batch send to all recipients at once
    try {
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        return { success: false, error: 'Failed to acquire Graph token' };
      }

      // If a dedicated service sender is configured, use it so cross-tenant users can still receive emails.
      const senderUserId = config.emailSenderUserId || userId;
      const url = `https://graph.microsoft.com/v1.0/users/${senderUserId}/sendMail`;
      console.log(`[GRAPH_API] Sending email from ${senderUserId} to ${recipients.join(', ')}${senderUserId !== userId ? ` (on behalf of ${userId})` : ''}`);

      // Convert markdown/plain text to properly formatted HTML
      const htmlBody = markdownToHtml(body);

      // Build toRecipients array for all recipients
      const toRecipients = recipients.map(email => ({
        emailAddress: { address: email }
      }));

      const message: any = {
        subject: subject,
        body: {
          contentType: 'HTML',
          content: htmlBody
        },
        toRecipients
      };

      // When sending from a service account, set reply-to as the requesting user
      if (senderUserId !== userId && options?.replyToEmail) {
        message.replyTo = [
          {
            emailAddress: {
              address: options.replyToEmail,
              name: options.replyToName || ''
            }
          }
        ];
      }

      await axios.post(url, {
        message,
        saveToSentItems: true
      }, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
      });

      console.log(`[GRAPH_API] Email sent successfully to ${recipients.join(', ')}`);
      return { success: true, sentTo: recipients };
    } catch (error: any) {
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      this.logGraphError('sendEmail', error);
      return { 
        success: false, 
        error: errMsg,
        failedRecipients: recipients.map(email => ({ 
          email, 
          error: errMsg,
          reason: this.categorizeEmailError(errMsg, email)
        }))
      };
    }
  }

  /**
   * Reply to an existing email message so the response stays in the same thread.
   * Uses Graph: POST /users/{id}/messages/{messageId}/reply
   */
  async replyToMessageInThread(
    userId: string,
    messageId: string,
    replyBody: string
  ): Promise<{ success: boolean; error?: string }> {
    try {
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        return { success: false, error: 'Failed to acquire Graph token' };
      }

      const trimmedBody = (replyBody || '').trim();
      if (!trimmedBody) {
        return { success: false, error: 'Reply body is empty' };
      }

      const url = `https://graph.microsoft.com/v1.0/users/${encodeURIComponent(userId)}/messages/${encodeURIComponent(messageId)}/reply`;
      await axios.post(
        url,
        { comment: trimmedBody },
        {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS,
        }
      );

      console.log(`[GRAPH_API] Replied in-thread to message ${messageId}`);
      return { success: true };
    } catch (error: any) {
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      this.logGraphError('replyToMessageInThread', error);
      return { success: false, error: errMsg };
    }
  }

  /**
   * Categorize email error to provide user-friendly reason
   */
  private categorizeEmailError(errorMsg: string, email: string): string {
    const lowerError = errorMsg.toLowerCase();
    
    if (lowerError.includes('not found') || lowerError.includes('does not exist') || lowerError.includes('mailbox not found')) {
      return 'User mailbox not found - may be external to tenant';
    }
    if (lowerError.includes('permission') || lowerError.includes('access denied') || lowerError.includes('authorization')) {
      return 'Permission denied - cross-tenant or restricted mailbox';
    }
    if (lowerError.includes('invalid') && lowerError.includes('recipient')) {
      return 'Invalid email address format';
    }
    if (lowerError.includes('blocked') || lowerError.includes('spam') || lowerError.includes('rejected')) {
      return 'Email blocked by recipient server';
    }
    if (lowerError.includes('timeout') || lowerError.includes('timed out')) {
      return 'Request timed out - try again';
    }
    if (lowerError.includes('quota') || lowerError.includes('limit')) {
      return 'Sending quota exceeded';
    }
    
    // Check if external domain
    const domain = email.split('@')[1];
    if (domain && !domain.includes('.onmicrosoft.com')) {
      return 'External recipient - may require different permissions';
    }
    
    return 'Delivery failed';
  }

  /**
   * Send emails independently to each recipient - one failure won't affect others.
   * Best for post-meeting distribution where some participants may be external.
   */
  async sendEmailsIndependently(
    userId: string,
    recipients: string[],
    subject: string,
    body: string,
    options?: { replyToEmail?: string; replyToName?: string }
  ): Promise<{
    success: boolean;
    error?: string;
    sentTo?: string[];
    failedRecipients?: Array<{ email: string; error: string; reason?: string }>;
    partialSuccess?: boolean;
  }> {
    const token = await this.getTokenUsingClientCredentials();
    if (!token) {
      return { 
        success: false, 
        error: 'Failed to acquire Graph token',
        failedRecipients: recipients.map(email => ({ email, error: 'No auth token', reason: 'Authentication failed' }))
      };
    }

    const senderUserId = config.emailSenderUserId || userId;
    const url = `https://graph.microsoft.com/v1.0/users/${senderUserId}/sendMail`;
    const htmlBody = markdownToHtml(body);

    const sentTo: string[] = [];
    const failedRecipients: Array<{ email: string; error: string; reason?: string }> = [];

    console.log(`[GRAPH_API] Sending independent emails to ${recipients.length} recipients`);

    // Send to each recipient independently with a small delay to avoid rate limiting
    for (const email of recipients) {
      try {
        const message: any = {
          subject: subject,
          body: {
            contentType: 'HTML',
            content: htmlBody
          },
          toRecipients: [{ emailAddress: { address: email } }]
        };

        if (senderUserId !== userId && options?.replyToEmail) {
          message.replyTo = [{
            emailAddress: {
              address: options.replyToEmail,
              name: options.replyToName || ''
            }
          }];
        }

        await axios.post(url, {
          message,
          saveToSentItems: false  // Don't save multiple copies to sent items
        }, {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
          },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
        });

        sentTo.push(email);
        console.log(`[GRAPH_API] ✓ Email sent to ${email}`);

        // Small delay between sends to avoid rate limiting
        if (recipients.length > 3) {
          await new Promise(resolve => setTimeout(resolve, 200));
        }

      } catch (error: any) {
        const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
        const reason = this.categorizeEmailError(errMsg, email);
        failedRecipients.push({ email, error: errMsg, reason });
        console.warn(`[GRAPH_API] ✗ Failed to send email to ${email}: ${reason}`);
      }
    }

    // Determine overall result
    const allFailed = sentTo.length === 0;
    const allSucceeded = failedRecipients.length === 0;
    const partialSuccess = sentTo.length > 0 && failedRecipients.length > 0;

    if (allSucceeded) {
      console.log(`[GRAPH_API] All ${sentTo.length} emails sent successfully`);
      return { success: true, sentTo };
    } else if (partialSuccess) {
      console.log(`[GRAPH_API] Partial success: ${sentTo.length} sent, ${failedRecipients.length} failed`);
      return { 
        success: true, 
        partialSuccess: true, 
        sentTo, 
        failedRecipients,
        error: `${failedRecipients.length} recipient(s) failed`
      };
    } else {
      console.log(`[GRAPH_API] All ${recipients.length} emails failed`);
      return { 
        success: false, 
        error: 'All emails failed to send', 
        failedRecipients 
      };
    }
  }

  /**
   * Get calendar events for a user within a time range
   */
  async getCalendarEvents(userId: string, startDateTime?: string, endDateTime?: string): Promise<{ success: boolean; events?: any[]; timezone?: string; error?: string }> {
    try {
      console.log(`[CALENDAR_DEBUG] getCalendarEvents called with userId: '${userId}', start: '${startDateTime}', end: '${endDateTime}'`);
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        console.error(`[CALENDAR_DEBUG] Failed to acquire Graph token`);
        return { success: false, error: 'Failed to acquire Graph token' };
      }

      // Validate userId format - must be a valid GUID/AAD Object ID
      if (!userId || userId.length < 10) {
        console.error(`[CALENDAR_DEBUG] Invalid userId format: '${userId}'`);
        return { success: false, error: 'Invalid user ID format' };
      }

      // Get user's timezone for accurate time display
      const userTimezone = await this.getUserTimezone(userId);

      // Helper to normalize date strings to full ISO 8601 format
      // Graph API requires full datetime, not just date
      const normalizeDateTime = (dateStr: string | undefined, isEndDate: boolean): string => {
        if (!dateStr) {
          const now = new Date();
          if (isEndDate) {
            // Default end: 7 days from now at end of day
            const end = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
            end.setHours(23, 59, 59, 999);
            return end.toISOString();
          } else {
            // Default start: now
            return now.toISOString();
          }
        }
        
        // If it's already a full ISO string with time, use it
        if (dateStr.includes('T') && (dateStr.includes('Z') || dateStr.includes('+') || dateStr.includes('-', 10))) {
          return dateStr;
        }
        
        // Date-only format (e.g., "2026-03-06") - add time component
        if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
          if (isEndDate) {
            return `${dateStr}T23:59:59.999Z`; // End of day
          } else {
            return `${dateStr}T00:00:00.000Z`; // Start of day
          }
        }
        
        // Try to parse as date and convert
        try {
          const parsed = new Date(dateStr);
          if (!isNaN(parsed.getTime())) {
            if (isEndDate && parsed.getHours() === 0 && parsed.getMinutes() === 0) {
              parsed.setHours(23, 59, 59, 999);
            }
            return parsed.toISOString();
          }
        } catch {
          // Fall through
        }
        
        // Fallback: treat as date-only
        return isEndDate ? `${dateStr}T23:59:59.999Z` : `${dateStr}T00:00:00.000Z`;
      };

      const start = normalizeDateTime(startDateTime, false);
      const end = normalizeDateTime(endDateTime, true);
      
      console.log(`[CALENDAR_DEBUG] Normalized dates - start: ${start}, end: ${end}, timezone: ${userTimezone}`);

      const url = `https://graph.microsoft.com/v1.0/users/${userId}/calendarView?startDateTime=${encodeURIComponent(start)}&endDateTime=${encodeURIComponent(end)}&$orderby=start/dateTime&$top=20&$select=subject,start,end,location,organizer,attendees,isAllDay,onlineMeeting,onlineMeetingUrl,isCancelled`;
      console.log(`[GRAPH_API] Fetching calendar events for ${userId} from ${start} to ${end}`);
      console.log(`[CALENDAR_DEBUG] Full URL: ${url}`);

      const response = await axios.get(url, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
          'Prefer': `outlook.timezone="${userTimezone}"`
        },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
      });

      const events = response.data?.value || [];
      console.log(`[GRAPH_API] Retrieved ${events.length} calendar events in ${userTimezone} timezone`);
      if (events.length > 0) {
        console.log(`[CALENDAR_DEBUG] First event: ${events[0].subject} at ${events[0].start?.dateTime}`);
      }
      return { success: true, events, timezone: userTimezone };
    } catch (error: any) {
      const status = error?.response?.status;
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      const errCode = error?.response?.data?.error?.code || 'N/A';
      console.error(`[CALENDAR_DEBUG] API Error: status=${status}, code=${errCode}, message=${errMsg}`);
      if (status === 403) {
        console.error(`[CALENDAR_DEBUG] 403 Forbidden while fetching calendar events.`);
      } else if (status === 404) {
        console.error(`[CALENDAR_DEBUG] 404 Not Found - User ID may not exist or mailbox not provisioned: ${errMsg}`);
      }
      this.logGraphError('getCalendarEvents', error);
      return { success: false, error: errMsg };
    }
  }

  /**
   * Find a past Teams meeting from calendar by date or subject.
   * Returns meeting info needed to fetch transcripts.
   */
  async findPastMeeting(
    userId: string,
    searchDate?: string,
    searchSubject?: string
  ): Promise<{ success: boolean; meeting?: { subject: string; joinWebUrl: string; organizerId: string; start: string; end: string }; error?: string }> {
    try {
      console.log(`[CALENDAR] Looking for past meeting: date=${searchDate}, subject=${searchSubject}`);
      
      // Get calendar events for the specified date range
      let startDate = searchDate;
      let endDate = searchDate;
      
      if (!startDate) {
        // Default: look back 7 days
        const now = new Date();
        const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        startDate = weekAgo.toISOString().split('T')[0];
        endDate = now.toISOString().split('T')[0];
      }
      
      const result = await this.getCalendarEvents(userId, startDate, endDate);
      if (!result.success || !result.events?.length) {
        return { success: false, error: result.error || 'No meetings found in that time range' };
      }
      
      // Filter to Teams meetings only (have onlineMeeting info)
      const teamsMeetings = result.events.filter((evt: any) => 
        evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl
      );
      
      if (teamsMeetings.length === 0) {
        return { success: false, error: 'No Teams meetings found in that time range' };
      }
      
      // If subject provided, try to match
      let targetMeeting = teamsMeetings[0]; // default to first/most recent
      if (searchSubject) {
        const subjectLower = searchSubject.toLowerCase();
        const matched = teamsMeetings.find((evt: any) => 
          evt.subject?.toLowerCase().includes(subjectLower)
        );
        if (matched) {
          targetMeeting = matched;
        }
      }
      
      const joinWebUrl = targetMeeting.onlineMeeting?.joinUrl || targetMeeting.onlineMeetingUrl;
      const organizerEmail = targetMeeting.organizer?.emailAddress?.address || '';

      // Graph transcript APIs need the organizer's AAD object ID, not email.
      // Resolve it from the email; fall back to the calling user's ID.
      let organizerId = userId;
      if (organizerEmail) {
        try {
          const orgInfo = await this.getUserInfo(organizerEmail);
          if (orgInfo?.id && orgInfo.id !== organizerEmail) {
            organizerId = orgInfo.id;
            console.log(`[CALENDAR] Resolved organizer AAD ID: ${organizerId} from ${organizerEmail}`);
          }
        } catch {
          console.warn(`[CALENDAR] Could not resolve organizer AAD ID from ${organizerEmail}, using caller ID ${userId}`);
        }
      }
      
      console.log(`[CALENDAR] Found meeting: "${targetMeeting.subject}" with joinUrl`);
      
      return {
        success: true,
        meeting: {
          subject: targetMeeting.subject,
          joinWebUrl,
          organizerId,
          start: targetMeeting.start?.dateTime,
          end: targetMeeting.end?.dateTime
        }
      };
    } catch (error: any) {
      console.error(`[CALENDAR_ERROR] findPastMeeting failed:`, error);
      return { success: false, error: error?.message || 'Failed to find meeting' };
    }
  }

  /**
   * Check for scheduling conflicts at a specific time
   */
  async checkScheduleAvailability(userId: string, startDateTime: string, endDateTime: string): Promise<{ success: boolean; isFree?: boolean; conflicts?: any[]; error?: string }> {
    try {
      const result = await this.getCalendarEvents(userId, startDateTime, endDateTime);
      if (!result.success) {
        return { success: false, error: result.error };
      }

      const conflicts = result.events || [];
      return {
        success: true,
        isFree: conflicts.length === 0,
        conflicts
      };
    } catch (error: any) {
      return { success: false, error: error?.message || 'Unknown error' };
    }
  }
}

export default new GraphApiHelper();
export { GraphApiHelper, UserInfo, ChatMessage, TranscriptionResult, OnlineMeetingInfo, CallInfo };
