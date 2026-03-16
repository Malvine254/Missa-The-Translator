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
  /** Full email body content (HTML stripped) - available when fetched with body select */
  bodyContent?: string;
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
        console.warn(`[TIMEZONE] No token available, defaulting to Central Standard Time`);
        return 'Central Standard Time';
      }
      
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${userId}/mailboxSettings`,
        {
          headers: { Authorization: `Bearer ${token}` },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
        }
      );
      
      const timezone = response.data?.timeZone || 'Central Standard Time';
      console.log(`[TIMEZONE] User ${userId} timezone: ${timezone}`);
      return timezone;
    } catch (error: any) {
      console.warn(`[TIMEZONE] Could not fetch timezone for ${userId}, defaulting to Central Standard Time:`, error?.message);
      return 'Central Standard Time';
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
    const rawBody = message?.body?.content || '';
    const bodyContent = this.stripHtmlToText(rawBody);
    const bodyPreview = message?.bodyPreview || bodyContent.slice(0, 255);
    return {
      id: message?.id || '',
      subject: message?.subject || '(No subject)',
      fromName: message?.from?.emailAddress?.name || '',
      fromAddress: message?.from?.emailAddress?.address || '',
      receivedDateTime: message?.receivedDateTime || '',
      importance: message?.importance || 'normal',
      isRead: !!message?.isRead,
      bodyPreview,
      bodyContent: bodyContent || undefined,
      conversationId: message?.conversationId,
      webLink: message?.webLink,
      categories: Array.isArray(message?.categories) ? message.categories : [],
      flagged: !!message?.flag?.flagStatus && message.flag.flagStatus !== 'notFlagged',
    };
  }

  async getInboxMessages(
    userId: string,
    options?: { top?: number }
  ): Promise<MailMessageSummary[]> {
    try {
      if (!this.tokenFactory) {
        console.warn(`[GRAPH_API] Token factory not available - inbox read disabled`);
        return [];
      }

      // Fetch messages - filtering is done by LLM, not here
      const top = Math.min(Math.max(options?.top || 20, 1), 50);
      const select = 'id,subject,from,receivedDateTime,importance,isRead,bodyPreview,conversationId,webLink,categories,flag,body';
      const response = await this.graphGetWithClientCredentials(
        `/users/${encodeURIComponent(userId)}/mailFolders/inbox/messages?$top=${top}&$orderby=receivedDateTime desc&$select=${encodeURIComponent(select)}`
      );

      const messages = (response.data?.value || []).map((message: any) => this.mapMailMessage(message));
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
   * If email is missing from member object, looks up via user profile.
   */
  async getChatMembersDetailed(chatId: string): Promise<{ displayName: string; email: string; userId?: string }[]> {
    try {
      if (!this.tokenFactory) return [];
      console.log(`[GRAPH_API] Fetching detailed chat members for: ${chatId}`);
      const response = await this.graphGetWithClientCredentials(`/chats/${chatId}/members`);
      const members = response.data?.value || [];
      const detailed: { displayName: string; email: string; userId?: string }[] = [];
      
      for (const m of members) {
        const name = (m.displayName || '').toLowerCase();
        if (!name || name.includes('bot') || name === 'assistant') continue;
        
        let email = m.email || m.microsoft?.graph?.user?.mail || '';
        const userId = m.userId || m.id?.split("'")[1] || '';
        
        // If email is missing but we have userId, look it up
        if (!email && userId) {
          try {
            const userInfo = await this.getUserInfo(userId);
            if (userInfo?.mail) {
              email = userInfo.mail;
              console.log(`[GRAPH_API] Enriched member ${m.displayName} with email: ${email}`);
            }
          } catch { /* ignore lookup failures */ }
        }
        
        detailed.push({
          displayName: m.displayName || 'Unknown',
          email: email || '',
          userId
        });
      }
      
      console.log(`[GRAPH_API] Found ${detailed.length} detailed members (${detailed.filter(m => m.email).length} with emails)`);
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
        console.log(`[GRAPH_API]   - subject/topic: ${subject || 'NONE'}`);  // ADD THIS
        
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
        `https://graph.microsoft.com/v1.0/communications/calls/${callId}/startTranscription`,
        {},
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
        `https://graph.microsoft.com/v1.0/communications/calls/${callId}/stopTranscription`,
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
   */
  async getOnlineMeetingId(organizerId: string, joinWebUrl: string): Promise<string | null> {
    try {
      const graphToken = await this.getTokenUsingClientCredentials();
      if (!graphToken) return null;
      const encodedUrl = encodeURIComponent(joinWebUrl);
      const response = await axios.get(
        `https://graph.microsoft.com/v1.0/users/${organizerId}/onlineMeetings?$filter=joinWebUrl eq '${decodeURIComponent(encodedUrl)}'`,
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
    } catch (error: any) {
      const status = error?.response?.status;
      const msg = error?.response?.data?.error?.message || error?.message;
      console.warn(`[GRAPH_API] Could not resolve meeting ID (status=${status}): ${msg}`);
      return null;
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
        console.log(`[GRAPH_API]   Transcript: id=${t.id}, callId=${t.callId || 'N/A'}, createdDateTime=${t.createdDateTime}`);
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
   * Download the content of a specific meeting transcript.
   * Tries multiple endpoints: beta API, then v1.0 API
   * Returns plain text (vtt format by default)
   */
  async downloadTranscriptContent(organizerId: string, meetingId: string, transcriptId: string): Promise<string | null> {
    const graphToken = await this.getTokenUsingClientCredentials();
    if (!graphToken) return null;
    console.log(`[GRAPH_API] Downloading transcript content: ${transcriptId}`);
    
    // Try v1.0 API first (stable, GA)
    try {
      console.log(`[GRAPH_API] Trying v1.0 endpoint for transcript download`);
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
    } catch (v1Error: any) {
      const v1Status = v1Error?.response?.status;
      const msg = v1Error?.response?.data?.error?.message || v1Error?.message;
      console.warn(`[GRAPH_API] Could not download transcript (status=${v1Status}): ${msg}`);
      return null;
    }
  }

  /**
   * Fetch the full transcript text for a meeting.
   * Resolves meeting ID by joinWebUrl, lists its transcripts, downloads the latest.
   * Falls back to getAllTranscripts scan if meeting ID lookup fails.
   * Returns VTT text or null.
   */
  async fetchMeetingTranscriptText(
    organizerId: string,
    joinWebUrl: string,
    minCreatedTimestamp?: number,
    maxCreatedTimestamp?: number
  ): Promise<string | null> {
    try {
      let transcripts: any[] = [];
      let meetingId: string | null = null;

      // Try to resolve meeting ID by joinWebUrl, then list its transcripts
      meetingId = await this.getOnlineMeetingId(organizerId, joinWebUrl);

      if (meetingId) {
        transcripts = await this.listMeetingTranscripts(organizerId, meetingId);
      } else {
        // No meeting ID found — fall back to scanning organizer's transcripts
        console.log(`[GRAPH_API] Meeting ID lookup failed - falling back to getAllTranscripts scan`);
        transcripts = await this.getAllTranscriptsForUser(organizerId, 20);
      }

      if (transcripts.length === 0) {
        console.log(`[GRAPH_API] No transcripts found for organizer ${organizerId}`);
        return null;
      }

      // Time-window filtering:
      // ALWAYS filter by minCreatedTimestamp when provided — this is CRITICAL for recurring
      // meetings where multiple call sessions share the same meetingId but have different
      // transcripts. Without this filter, we'd pick up transcripts from old sessions.
      let filteredTranscripts = transcripts;
      if (minCreatedTimestamp) {
        // Filter by lower bound (allow 1-hour grace before call start for early transcripts)
        const lowerBound = minCreatedTimestamp - (60 * 60 * 1000);
        filteredTranscripts = transcripts.filter((t: any) => {
          const created = new Date(t.createdDateTime || 0).getTime();
          return created >= lowerBound;
        });

        const minDate = new Date(lowerBound).toISOString();
        console.log(`[GRAPH_API] Time-window filter: ${transcripts.length} transcripts -> ${filteredTranscripts.length} created after ${minDate}`);

        if (filteredTranscripts.length === 0) {
          console.log(`[GRAPH_API] No transcripts found after call start time - meeting transcript may not exist yet`);
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
      const transcriptMeetingId = latest.meetingId || meetingId;
      console.log(`[GRAPH_API] Using latest transcript: id=${latest.id}, callId=${latest.callId || 'N/A'}, meetingId=${transcriptMeetingId}, created=${latest.createdDateTime}`);

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

      // When sending from a service account, automatically set reply-to as the requesting user
      if (senderUserId !== userId) {
        let replyToEmail = options?.replyToEmail;
        let replyToName = options?.replyToName || '';
        
        // If no replyToEmail provided, fetch the user's email from their profile
        if (!replyToEmail) {
          try {
            const userProfile = await this.graphGetWithClientCredentials(`/users/${userId}?$select=mail,displayName,userPrincipalName`);
            replyToEmail = userProfile.data?.mail || userProfile.data?.userPrincipalName || '';
            replyToName = userProfile.data?.displayName || '';
            console.log(`[GRAPH_API] Auto-fetched reply-to: ${replyToName} <${replyToEmail}>`);
          } catch (profileError) {
            console.warn(`[GRAPH_API] Could not fetch user profile for reply-to, continuing without reply-to`);
          }
        }
        
        if (replyToEmail) {
          message.replyTo = [
            {
              emailAddress: {
                address: replyToEmail,
                name: replyToName
              }
            }
          ];
        }
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

    // When using service account, auto-fetch reply-to email if not provided
    let replyToEmail = options?.replyToEmail;
    let replyToName = options?.replyToName || '';
    if (senderUserId !== userId && !replyToEmail) {
      try {
        const userProfile = await this.graphGetWithClientCredentials(`/users/${userId}?$select=mail,displayName,userPrincipalName`);
        replyToEmail = userProfile.data?.mail || userProfile.data?.userPrincipalName || '';
        replyToName = userProfile.data?.displayName || '';
        console.log(`[GRAPH_API] Auto-fetched reply-to for batch send: ${replyToName} <${replyToEmail}>`);
      } catch (profileError) {
        console.warn(`[GRAPH_API] Could not fetch user profile for reply-to, continuing without reply-to`);
      }
    }

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

        if (senderUserId !== userId && replyToEmail) {
          message.replyTo = [{
            emailAddress: {
              address: replyToEmail,
              name: replyToName
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
      let userTimezone = await this.getUserTimezone(userId);

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

      const selectFields = 'subject,start,end,location,organizer,attendees,isAllDay,onlineMeeting,onlineMeetingUrl,isCancelled,originalStartTimeZone';
      const url = `https://graph.microsoft.com/v1.0/users/${userId}/calendarView?startDateTime=${encodeURIComponent(start)}&endDateTime=${encodeURIComponent(end)}&$orderby=start/dateTime&$top=200&$select=${selectFields}`;
      console.log(`[GRAPH_API] Fetching calendar events for ${userId} from ${start} to ${end}`);
      console.log(`[CALENDAR_DEBUG] Full URL: ${url}`);

      const fetchEvents = async (tz: string) => {
        const resp = await axios.get(url, {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
            'Prefer': `outlook.timezone="${tz}"`
          },
          timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
        });
        return resp.data?.value || [];
      };

      let events = await fetchEvents(userTimezone);
      console.log(`[GRAPH_API] Retrieved ${events.length} calendar events in ${userTimezone} timezone`);

      // Auto-detect timezone: if we defaulted to UTC but events originated in a
      // different timezone, re-fetch with the correct Prefer header so the
      // returned start/end datetimes are in the user's actual local time.
      if (userTimezone === 'UTC' && events.length > 0) {
        const firstNonCancelled = events.find((e: any) => !e.isCancelled) || events[0];
        const origTz = firstNonCancelled?.originalStartTimeZone;
        if (origTz && origTz !== 'UTC' && origTz !== 'tzone://Microsoft/Utc') {
          console.log(`[CALENDAR_DEBUG] Timezone was UTC but events originate in '${origTz}' — re-fetching with correct timezone`);
          userTimezone = origTz;
          events = await fetchEvents(userTimezone);
          console.log(`[GRAPH_API] Re-fetched ${events.length} events in ${userTimezone} timezone`);
        }
      }

      if (events.length > 0) {
        console.log(`[CALENDAR_DEBUG] First event: ${events[0].subject} at ${events[0].start?.dateTime} (tz: ${userTimezone})`);
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
  ): Promise<{ success: boolean; meeting?: { subject: string; joinWebUrl: string; organizerId: string; start: string; end: string; attendees: Array<{ name: string; email: string }> }; error?: string }> {
    try {
      console.log(`[CALENDAR] Looking for past meeting: date=${searchDate}, subject=${searchSubject}`);
      
      // Get calendar events for the specified date range
      let startDate: string;
      let endDate: string;
      
      if (!searchDate) {
        // Default: look back 7 days
        const now = new Date();
        const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        startDate = weekAgo.toISOString().split('T')[0];
        endDate = now.toISOString().split('T')[0];
      } else {
        // Extract date-only portion (handle both "2026-03-15" and "2026-03-15T05:12:00")
        startDate = searchDate.split('T')[0];
        endDate = startDate;
      }
      
      let result = await this.getCalendarEvents(userId, startDate, endDate);
      
      // If a specific date was given but returned no Teams meetings, widen to 7-day lookback
      // This handles timezone boundary issues (e.g. meeting created in CST but searched in UTC)
      if (searchDate) {
        const hasTeamsMeetings = result.success && result.events?.some((evt: any) =>
          evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl
        );
        if (!hasTeamsMeetings) {
          console.log(`[CALENDAR] No Teams meetings found on ${startDate}, widening search to last 7 days`);
          const now = new Date();
          const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
          result = await this.getCalendarEvents(userId, weekAgo.toISOString().split('T')[0], now.toISOString().split('T')[0]);
        }
      }
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

      const nowMs = Date.now();
      const sortedMeetings = [...teamsMeetings].sort((a: any, b: any) => {
        const aTime = new Date(a.end?.dateTime || a.start?.dateTime || 0).getTime();
        const bTime = new Date(b.end?.dateTime || b.start?.dateTime || 0).getTime();
        return bTime - aTime;
      });

      const candidateMeetings = searchDate
        ? sortedMeetings
        : sortedMeetings.filter((evt: any) => new Date(evt.end?.dateTime || evt.start?.dateTime || 0).getTime() <= nowMs + 60_000);
      const meetingsToSearch = candidateMeetings.length > 0 ? candidateMeetings : sortedMeetings;
      
      // If subject provided, try to match against the most recent meetings first.
      let targetMeeting = meetingsToSearch[0];
      if (searchSubject) {
        const subjectLower = searchSubject.toLowerCase();
        const matched = meetingsToSearch.find((evt: any) => 
          evt.subject?.toLowerCase().includes(subjectLower)
        );
        if (matched) {
          targetMeeting = matched;
        }
      }
      
      const joinWebUrl = targetMeeting.onlineMeeting?.joinUrl || targetMeeting.onlineMeetingUrl;
      const organizerEmail = targetMeeting.organizer?.emailAddress?.address || '';
      const attendees = Array.isArray(targetMeeting.attendees)
        ? targetMeeting.attendees
            .map((attendee: any) => ({
              name: attendee?.emailAddress?.name || attendee?.emailAddress?.address || '',
              email: attendee?.emailAddress?.address || '',
            }))
            .filter((attendee: { name: string; email: string }) => !!attendee.email)
        : [];

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
          end: targetMeeting.end?.dateTime,
          attendees,
        }
      };
    } catch (error: any) {
      console.error(`[CALENDAR_ERROR] findPastMeeting failed:`, error);
      return { success: false, error: error?.message || 'Failed to find meeting' };
    }
  }

  /**
   * Find multiple past Teams meetings, sorted by most recent first.
   * Used by summarize/minutes/transcribe handlers to try each meeting until one has a transcript.
   */
  async findPastMeetings(
    userId: string,
    searchDate?: string,
    searchSubject?: string,
    maxResults = 5
  ): Promise<{ success: boolean; meetings: Array<{ subject: string; joinWebUrl: string; organizerId: string; start: string; end: string; attendees: Array<{ name: string; email: string }> }>; error?: string }> {
    try {
      console.log(`[CALENDAR] Looking for past meetings (up to ${maxResults}): date=${searchDate}, subject=${searchSubject}`);
      
      let startDate: string;
      let endDate: string;
      
      if (!searchDate) {
        const now = new Date();
        const twoWeeksAgo = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
        startDate = twoWeeksAgo.toISOString().split('T')[0];
        endDate = now.toISOString().split('T')[0];
      } else {
        startDate = searchDate.split('T')[0];
        endDate = startDate;
      }
      
      let result = await this.getCalendarEvents(userId, startDate, endDate);
      
      if (searchDate) {
        const hasTeamsMeetings = result.success && result.events?.some((evt: any) =>
          evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl
        );
        if (!hasTeamsMeetings) {
          const now = new Date();
          const twoWeeksAgo = new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000);
          result = await this.getCalendarEvents(userId, twoWeeksAgo.toISOString().split('T')[0], now.toISOString().split('T')[0]);
        }
      }
      
      if (!result.success || !result.events?.length) {
        return { success: false, meetings: [], error: result.error || 'No meetings found in that time range' };
      }
      
      const teamsMeetings = result.events.filter((evt: any) =>
        (evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl) && !evt.isCancelled
      );
      
      console.log(`[CALENDAR] Total events: ${result.events.length}, Teams meetings: ${teamsMeetings.length} (range: ${startDate} to ${endDate})`);
      if (teamsMeetings.length > 0) {
        console.log(`[CALENDAR] Teams meetings found: ${teamsMeetings.map((e: any) => `"${e.subject}" (${e.start?.dateTime?.split('T')[0] || '?'})`).join(', ')}`);
      }
      
      if (teamsMeetings.length === 0) {
        return { success: false, meetings: [], error: `No Teams meetings found between ${startDate} and ${endDate}` };
      }
      
      const nowMs = Date.now();
      const sorted = [...teamsMeetings].sort((a: any, b: any) => {
        const aTime = new Date(a.end?.dateTime || a.start?.dateTime || 0).getTime();
        const bTime = new Date(b.end?.dateTime || b.start?.dateTime || 0).getTime();
        return bTime - aTime;
      });
      
      // Prefer meetings that have already ended
      const endedMeetings = sorted.filter((evt: any) =>
        new Date(evt.end?.dateTime || evt.start?.dateTime || 0).getTime() <= nowMs + 60_000
      );
      const meetingsToUse = endedMeetings.length > 0 ? endedMeetings : sorted;
      
      // If subject given, sort matches to front
      let ordered = meetingsToUse;
      if (searchSubject) {
        const subjectLower = searchSubject.toLowerCase();
        ordered = [
          ...meetingsToUse.filter((evt: any) => evt.subject?.toLowerCase().includes(subjectLower)),
          ...meetingsToUse.filter((evt: any) => !evt.subject?.toLowerCase().includes(subjectLower)),
        ];
      }
      
      const results: Array<{ subject: string; joinWebUrl: string; organizerId: string; start: string; end: string; attendees: Array<{ name: string; email: string }> }> = [];
      
      for (const evt of ordered.slice(0, maxResults)) {
        const joinWebUrl = evt.onlineMeeting?.joinUrl || evt.onlineMeetingUrl;
        const organizerEmail = evt.organizer?.emailAddress?.address || '';
        const attendees = Array.isArray(evt.attendees)
          ? evt.attendees
              .map((a: any) => ({ name: a?.emailAddress?.name || '', email: a?.emailAddress?.address || '' }))
              .filter((a: { name: string; email: string }) => !!a.email)
          : [];
        
        let organizerId = userId;
        if (organizerEmail) {
          try {
            const orgInfo = await this.getUserInfo(organizerEmail);
            if (orgInfo?.id && orgInfo.id !== organizerEmail) {
              organizerId = orgInfo.id;
            }
          } catch { /* use userId fallback */ }
        }
        
        results.push({
          subject: evt.subject || 'Untitled Meeting',
          joinWebUrl,
          organizerId,
          start: evt.start?.dateTime,
          end: evt.end?.dateTime,
          attendees,
        });
      }
      
      console.log(`[CALENDAR] Found ${results.length} meetings: ${results.map(m => `"${m.subject}" (${m.start ? new Date(m.start).toLocaleDateString() : 'unknown date'})`).join(', ')}`);
      return { success: true, meetings: results };
    } catch (error: any) {
      console.error(`[CALENDAR_ERROR] findPastMeetings failed:`, error);
      return { success: false, meetings: [], error: error?.message || 'Failed to find meetings' };
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
