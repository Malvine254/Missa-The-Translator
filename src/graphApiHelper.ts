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

interface TranscriptionResult {
  status: string;
  id: string;
  recordingFile?: string;
  transcript?: string;
}

interface OnlineMeetingInfo {
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

        const fallbackUserId = msg?.from?.user?.id;
        if (!fallbackUserId) {
          continue;
        }

        const tenantId = process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID;
        console.log(`[GRAPH_API] Fallback meeting info from chat messages. organizer=${fallbackUserId}, joinWebUrl=present`);

        return {
          joinWebUrl,
          organizer: {
            id: fallbackUserId,
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
        
        // Decode token to verify scopes (diagnostic)
        try {
          const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString());
          console.log(`[GRAPH_TOKEN_SCOPES] Token roles: ${payload.roles?.join(', ') || 'NONE'}`);
          console.log(`[GRAPH_TOKEN_SCOPES] Token app: ${payload.appid}, tenant: ${payload.tid}`);
        } catch (e) {
          console.warn('[GRAPH_TOKEN_SCOPES] Could not decode token for diagnostics');
        }
        
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
      console.warn(`Could not fetch full user info for ${userId}, using activity fallback name when available`);
      return {
        id: userId,
        displayName: '',
      };
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
        console.warn(`[GRAPH_API] Received 403 Forbidden - missing Graph application permissions/admin consent for chat read`);
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
        console.warn(`[GRAPH_API] Received 403 Forbidden for chat info - missing Graph application permissions/admin consent`);
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
        console.warn(`[GRAPH_API] Received 403 Forbidden for recordings - missing Graph permissions/admin consent`);
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
        console.log(`[GRAPH_API] Got meeting info, organizer: ${meetingInfo.organizer?.id}, joinWebUrl: ${meetingInfo.joinWebUrl ? 'present' : 'MISSING'}`);
        baseInfo = {
          joinWebUrl: meetingInfo.joinWebUrl,
          organizer: meetingInfo.organizer,
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
            console.log(`[GRAPH_API] Got joinMeetingId: ${baseInfo.joinMeetingId}`);
          } else if (onlineMeeting?.id) {
            // Store the meeting's graph resource id as fallback identifier
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
          console.warn(`[GRAPH_API] Could not fetch joinMeetingId (status=${status}) - will use organizerMeetingInfo fallback`);
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
    try {
      const graphToken = await this.getTokenUsingClientCredentials();  // ← USE GRAPH TOKEN
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
      if (status === 403) {
        console.warn(`[GRAPH_API] Access denied to meeting ID endpoint (403) — will use live transcription or post-meeting fallback`);
      } else {
        console.warn(`[GRAPH_API] Could not resolve online meeting ID (status=${status})`);
      }
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
        console.log(`[GRAPH_API]   Transcript: id=${t.id}, createdDateTime=${t.createdDateTime}`);
      }
      return allTranscripts;
    } catch (error: any) {
      const status = error?.response?.status;
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
   * Download transcript content using the callTranscripts endpoint (no meetingId needed).
   * GET /users/{userId}/onlineMeetings/getAllTranscripts()?$filter=id eq '{transcriptId}'
   * or use the transcript metadata to get meetingId + transcriptId
   */
  async downloadTranscriptById(userId: string, meetingId: string, transcriptId: string): Promise<string | null> {
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
      console.warn(`[GRAPH_API] Could not download transcript by ID (status=${status}): ${msg}`);
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
   * Fallback: If meeting ID lookup fails (403), scan ALL recent transcripts for the organizer.
   * Returns VTT text or null.
   * @param organizerId - The organizer's user ID
   * @param joinWebUrl - The meeting join URL
   * @param minCreatedTimestamp - Optional timestamp (ms since epoch) for earliest transcript to consider
   * @param maxCreatedTimestamp - Optional timestamp (ms since epoch) for latest transcript to consider (with 5min grace)
   */
  async fetchMeetingTranscriptText(organizerId: string, joinWebUrl: string, minCreatedTimestamp?: number, maxCreatedTimestamp?: number): Promise<string | null> {
    try {
      let transcripts: any[] = [];
      let meetingId: string | null = null;
      
      // Try primary approach: get meeting by joinWebUrl
      meetingId = await this.getOnlineMeetingId(organizerId, joinWebUrl);
      
      if (meetingId) {
        // Got meeting ID - list transcripts for this specific meeting
        transcripts = await this.listMeetingTranscripts(organizerId, meetingId);
      } else {
        // Fallback: scan ALL transcripts for this organizer (up to 20 most recent)
        console.log(`[GRAPH_API] Meeting ID lookup failed - falling back to getAllTranscripts scan`);
        const allTranscripts = await this.getAllTranscriptsForUser(organizerId, 20);
        
        if (allTranscripts.length > 0) {
          // Use these transcripts directly - they include meetingId
          transcripts = allTranscripts;
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
          
          // Check maximum time (with 5-minute grace period for Teams processing)
          if (maxCreatedTimestamp) {
            const maxWithGrace = maxCreatedTimestamp + (5 * 60 * 1000);
            if (created > maxWithGrace) {
              return false;
            }
          }
          
          return true;
        });

        const minDate = minCreatedTimestamp ? new Date(minCreatedTimestamp).toISOString() : 'N/A';
        const maxDate = maxCreatedTimestamp ? new Date(maxCreatedTimestamp + (5 * 60 * 1000)).toISOString() : 'N/A';
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
   */
  async sendEmail(userId: string, toEmail: string, subject: string, body: string): Promise<{ success: boolean; error?: string }> {
    try {
      const token = await this.getTokenUsingClientCredentials();
      if (!token) {
        return { success: false, error: 'Failed to acquire Graph token' };
      }

      const url = `https://graph.microsoft.com/v1.0/users/${userId}/sendMail`;
      console.log(`[GRAPH_API] Sending email from ${userId} to ${toEmail}`);

      // Convert markdown/plain text to properly formatted HTML
      const htmlBody = markdownToHtml(body);

      await axios.post(url, {
        message: {
          subject: subject,
          body: {
            contentType: 'HTML',
            content: htmlBody
          },
          toRecipients: [
            {
              emailAddress: {
                address: toEmail
              }
            }
          ]
        },
        saveToSentItems: true
      }, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
      });

      console.log(`[GRAPH_API] Email sent successfully to ${toEmail}`);
      return { success: true };
    } catch (error: any) {
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      this.logGraphError('sendEmail', error);
      return { success: false, error: errMsg };
    }
  }

  /**
   * Get calendar events for a user within a time range
   */
  async getCalendarEvents(userId: string, startDateTime?: string, endDateTime?: string): Promise<{ success: boolean; events?: any[]; error?: string }> {
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
      
      console.log(`[CALENDAR_DEBUG] Normalized dates - start: ${start}, end: ${end}`);

      const url = `https://graph.microsoft.com/v1.0/users/${userId}/calendarView?startDateTime=${encodeURIComponent(start)}&endDateTime=${encodeURIComponent(end)}&$orderby=start/dateTime&$top=20&$select=subject,start,end,location,organizer,attendees,isAllDay,onlineMeeting,onlineMeetingUrl,isCancelled`;
      console.log(`[GRAPH_API] Fetching calendar events for ${userId} from ${start} to ${end}`);
      console.log(`[CALENDAR_DEBUG] Full URL: ${url}`);

      const response = await axios.get(url, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
          'Prefer': 'outlook.timezone="UTC"'
        },
        timeout: GraphApiHelper.GRAPH_TIMEOUT_MS
      });

      const events = response.data?.value || [];
      console.log(`[GRAPH_API] Retrieved ${events.length} calendar events`);
      if (events.length > 0) {
        console.log(`[CALENDAR_DEBUG] First event: ${events[0].subject} at ${events[0].start?.dateTime}`);
      }
      return { success: true, events };
    } catch (error: any) {
      const status = error?.response?.status;
      const errMsg = error?.response?.data?.error?.message || error?.message || 'Unknown error';
      const errCode = error?.response?.data?.error?.code || 'N/A';
      console.error(`[CALENDAR_DEBUG] API Error: status=${status}, code=${errCode}, message=${errMsg}`);
      if (status === 403) {
        console.error(`[CALENDAR_DEBUG] 403 Forbidden - App likely missing Calendars.Read permission. Grant admin consent in Azure Portal.`);
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
      const organizerId = targetMeeting.organizer?.emailAddress?.address || userId;
      
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
