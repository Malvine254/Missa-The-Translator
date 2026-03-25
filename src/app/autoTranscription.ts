/**
 * Auto-Transcription Module for Teams Bot
 * 
 * This module handles automatic transcription using Microsoft Graph Beta API.
 * 
 * Architecture:
 *   Meeting starts → Bot auto joins → Bot calls Graph API → Transcription begins
 * 
 * Required Permissions (Azure Entra ID):
 *   - Calls.JoinGroupCall.All
 *   - Calls.AccessMedia.All  
 *   - OnlineMeetings.Read.All
 *   - OnlineMeetings.ReadWrite.All
 *   - OnlineMeetingTranscript.Read.All
 * 
 * Required Teams Policies:
 *   - Transcription must be enabled in tenant meeting policies
 *   - Application Access Policy must be configured for the bot app
 * 
 * Beta API Used:
 *   POST /beta/communications/calls/{callId}/startTranscription
 *   POST /beta/communications/calls/{callId}/stopTranscription
 *   GET  /beta/communications/calls/{callId}/transcripts
 * 
 * @module autoTranscription
 */

import axios from 'axios';

// ============================================================================
// CONFIGURATION
// ============================================================================

export interface AutoTranscriptionConfig {
  /** Whether auto-transcription is enabled globally */
  enabled: boolean;
  /** Default language for transcription (BCP-47 code) */
  defaultLanguage: string;
  /** Delay before first startTranscription attempt after call establishment (ms) */
  initialStartDelayMs: number;
  /** Retry delays in milliseconds (exponential backoff) */
  retryDelays: number[];
  /** Maximum time to wait for transcription to start (ms) */
  maxWaitTime: number;
  /** Whether to notify users on transcription status changes */
  notifyUsers: boolean;
  /** Logging level: 'debug' | 'info' | 'warn' | 'error' */
  logLevel: 'debug' | 'info' | 'warn' | 'error';
}

const DEFAULT_CONFIG: AutoTranscriptionConfig = {
  enabled: true,
  defaultLanguage: 'en-US',
  initialStartDelayMs: 7_000,
  retryDelays: [0, 3_000, 8_000, 15_000, 30_000, 60_000], // 0s, 3s, 8s, 15s, 30s, 60s
  maxWaitTime: 120_000, // 2 minutes
  notifyUsers: true,
  logLevel: 'info',
};

let config: AutoTranscriptionConfig = { ...DEFAULT_CONFIG };

// ============================================================================
// STATE TRACKING
// ============================================================================

export interface TranscriptionState {
  callId: string;
  conversationId: string;
  status: 'pending' | 'starting' | 'active' | 'failed' | 'stopped';
  attemptCount: number;
  lastAttemptAt?: number;
  startedAt?: number;
  failedReason?: string;
  language: string;
  lastErrorStatus?: number;
  lastErrorCode?: string;
  lastErrorMessage?: string;
  lastErrorAt?: number;
  lastErrorRecoverable?: boolean;
  nextRetryAt?: number;
  retryTimerId?: ReturnType<typeof setTimeout>;
  recordingStatusSet?: boolean;
  recordingStatusUnsupported?: boolean;
}

export interface TranscriptionCallDiagnostic {
  callId: string;
  status: TranscriptionState['status'];
  attemptCount: number;
  startedAt?: number;
  failedReason?: string;
  lastErrorStatus?: number;
  lastErrorCode?: string;
  lastErrorMessage?: string;
  lastErrorAt?: number;
  lastErrorRecoverable?: boolean;
  nextRetryAt?: number;
  summary: string;
}

/** Map of callId -> TranscriptionState */
const transcriptionStates = new Map<string, TranscriptionState>();

/** Token getter function - must be set before use */
let getTokenFn: (() => Promise<string | null>) | null = null;

/** Message sender function for notifications */
let sendMessageFn: ((serviceUrl: string, conversationId: string, message: string) => Promise<void>) | null = null;

// ============================================================================
// LOGGING
// ============================================================================

const LOG_PREFIX = '[AUTO_TRANSCRIPTION]';

function getErrorText(error: { status?: number; code?: string; message?: string }): string {
  const parts = [
    error.status ? `status=${error.status}` : '',
    error.code ? `code=${error.code}` : '',
    error.message || '',
  ].filter(Boolean);
  return parts.join(', ') || 'unknown error';
}

function isCallNotReadyError(status?: number, code?: string, message?: string): boolean {
  const combined = `${code || ''} ${message || ''}`.toLowerCase();
  return status === 400 && (
    combined.includes('2203') ||
    combined.includes('not active') ||
    combined.includes('not ready') ||
    combined.includes('not established') ||
    combined.includes('media platform') ||
    combined.includes('call state')
  );
}

function normalizeTranscriptionError(error: any): {
  status?: number;
  code?: string;
  message: string;
  isRecoverable: boolean;
} {
  const status = error?.response?.status ?? error?.status;
  const code = error?.response?.data?.error?.code || error?.code;
  const message = error?.response?.data?.error?.message || error?.message || 'Unknown transcription error';
  const isRecoverable = Boolean(
    status === 409 ||
    status === 429 ||
    status === 500 ||
    status === 502 ||
    status === 503 ||
    status === 504 ||
    status === 404 ||
    isCallNotReadyError(status, code, message)
  );

  return { status, code, message, isRecoverable };
}

function getDiagnosticSummary(state: TranscriptionState): string {
  if (state.status === 'active') {
    return `transcription active after ${state.attemptCount} attempt(s)`;
  }

  if (state.status === 'starting') {
    if (state.lastErrorMessage) {
      return `retrying after ${getErrorText({
        status: state.lastErrorStatus,
        code: state.lastErrorCode,
        message: state.lastErrorMessage,
      })}`;
    }
    return `attempt ${state.attemptCount} in progress`;
  }

  if (state.status === 'failed') {
    return state.failedReason || getErrorText({
      status: state.lastErrorStatus,
      code: state.lastErrorCode,
      message: state.lastErrorMessage,
    });
  }

  if (state.status === 'stopped') {
    return 'transcription stopped';
  }

  return 'waiting to start transcription';
}

function log(level: 'debug' | 'info' | 'warn' | 'error', message: string, data?: any) {
  const levels = { debug: 0, info: 1, warn: 2, error: 3 };
  if (levels[level] < levels[config.logLevel]) return;

  const timestamp = new Date().toISOString();
  const fullMsg = `${LOG_PREFIX} ${timestamp} [${level.toUpperCase()}] ${message}`;
  
  switch (level) {
    case 'debug':
    case 'info':
      console.log(fullMsg, data !== undefined ? data : '');
      break;
    case 'warn':
      console.warn(fullMsg, data !== undefined ? data : '');
      break;
    case 'error':
      console.error(fullMsg, data !== undefined ? data : '');
      break;
  }
}

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Initialize the auto-transcription module.
 * Must be called before using any other functions.
 * 
 * @param tokenGetter - Function that returns a valid Graph API token
 * @param messageSender - Function to send proactive messages to conversations
 * @param customConfig - Optional custom configuration overrides
 */
export function initAutoTranscription(
  tokenGetter: () => Promise<string | null>,
  messageSender: (serviceUrl: string, conversationId: string, message: string) => Promise<void>,
  customConfig?: Partial<AutoTranscriptionConfig>
): void {
  getTokenFn = tokenGetter;
  sendMessageFn = messageSender;
  
  if (customConfig) {
    config = { ...config, ...customConfig };
  }
  
  log('info', 'Auto-transcription module initialized', {
    enabled: config.enabled,
    language: config.defaultLanguage,
    retryDelays: config.retryDelays,
  });
}

/**
 * Update configuration at runtime.
 */
export function updateConfig(newConfig: Partial<AutoTranscriptionConfig>): void {
  config = { ...config, ...newConfig };
  log('info', 'Configuration updated', newConfig);
}

/**
 * Check if auto-transcription is enabled.
 */
export function isEnabled(): boolean {
  return config.enabled;
}

/**
 * Enable or disable auto-transcription globally.
 */
export function setEnabled(enabled: boolean): void {
  config.enabled = enabled;
  log('info', `Auto-transcription ${enabled ? 'enabled' : 'disabled'}`);
}

// ============================================================================
// CORE API CALLS
// ============================================================================

/**
 * Start transcription on an active call using Graph Beta API.
 * 
 * @param callId - The active call ID from Graph
 * @param language - BCP-47 language code (default: en-US)
 * @returns Success status
 */
async function callStartTranscriptionAPI(callId: string, language: string = config.defaultLanguage): Promise<boolean> {
  if (!getTokenFn) {
    throw normalizeTranscriptionError({
      status: 500,
      code: 'TokenGetterNotInitialized',
      message: 'Token getter not initialized',
    });
  }

  const token = await getTokenFn();
  if (!token) {
    throw normalizeTranscriptionError({
      status: 401,
      code: 'GraphTokenUnavailable',
      message: 'Failed to get Graph API token',
    });
  }

  const baseUrl = `https://graph.microsoft.com/beta/communications/calls/${callId}`;
  const normalizedLanguage = (language || config.defaultLanguage || 'en-us').toLowerCase();
  const startAttempts: Array<{ url: string; body: Record<string, any>; label: string }> = [
    {
      // Official documented action endpoint shape.
      url: `${baseUrl}/microsoft.graph.StartTranscription`,
      body: { language: normalizedLanguage },
      label: 'action-language',
    },
    {
      // Observed short endpoint behavior in tenant tests.
      url: `${baseUrl}/startTranscription`,
      body: { clientContext: `missa-transcription-${callId}`.slice(0, 256) },
      label: 'short-clientContext',
    },
    {
      // Legacy payload variant retained as final fallback.
      url: `${baseUrl}/startTranscription`,
      body: {
        languageTag: normalizedLanguage,
        singlePerParticipant: false,
      },
      label: 'short-legacy-languageTag',
    },
  ];

  let lastError: any;
  for (const attempt of startAttempts) {
    log('debug', `Calling startTranscription API`, {
      callId,
      language,
      normalizedLanguage,
      url: attempt.url,
      attempt: attempt.label,
      bodyKeys: Object.keys(attempt.body || {}),
    });
    try {
      const response = await axios.post(
        attempt.url,
        attempt.body,
        {
          headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
        }
      );

      // Log full response to understand what Graph actually returns
      const opData = response.data || {};
      log('info', `Transcription started successfully`, {
        callId,
        url: attempt.url,
        attempt: attempt.label,
        httpStatus: response.status,
        operationId: opData.id,
        operationStatus: opData.status,
        clientContext: opData.clientContext,
        resultInfo: opData.resultInfo,
        responseKeys: Object.keys(opData),
      });

      // Check if operation status indicates failure
      if (opData.status === 'failed') {
        log('warn', `startTranscription operation returned failed status`, {
          callId,
          operationId: opData.id,
          resultInfo: opData.resultInfo,
        });
        // Continue to next endpoint attempt
        lastError = new Error(`Operation status: failed - ${JSON.stringify(opData.resultInfo)}`);
        continue;
      }

      return true;
    } catch (error: any) {
      lastError = error;
      const normalizedError = normalizeTranscriptionError(error);
      log('warn', `startTranscription endpoint attempt failed`, {
        callId,
        url: attempt.url,
        attempt: attempt.label,
        status: normalizedError.status,
        errorCode: normalizedError.code,
        errorMsg: normalizedError.message,
      });
    }
  }

  const normalizedError = normalizeTranscriptionError(lastError);
  log('error', `Failed to start transcription`, {
    callId,
    status: normalizedError.status,
    errorCode: normalizedError.code,
    errorMsg: normalizedError.message,
    isRecoverable: normalizedError.isRecoverable,
    fullError: lastError?.response?.data,
  });

  throw normalizedError;
}

async function callUpdateRecordingStatusAPI(callId: string, status: 'recording' | 'notRecording'): Promise<boolean> {
  if (!getTokenFn) {
    throw normalizeTranscriptionError({
      status: 500,
      code: 'TokenGetterNotInitialized',
      message: 'Token getter not initialized',
    });
  }

  const token = await getTokenFn();
  if (!token) {
    throw normalizeTranscriptionError({
      status: 401,
      code: 'GraphTokenUnavailable',
      message: 'Failed to get Graph API token',
    });
  }

  const url = `https://graph.microsoft.com/v1.0/communications/calls/${callId}/updateRecordingStatus`;
  const clientContext = `missa-translator-${callId}-${status}`.slice(0, 255);

  log('info', `Calling updateRecordingStatus API (this controls Teams UI banner)`, { callId, status, url });

  const response = await axios.post(
    url,
    {
      clientContext,
      status,
    },
    {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    }
  );

  const opData = response.data || {};
  log('info', `Recording status updated - Teams UI should now show banner`, {
    callId,
    status,
    httpStatus: response.status,
    operationId: opData.id,
    operationStatus: opData.status,
    clientContext: opData.clientContext,
    resultInfo: opData.resultInfo,
    responseKeys: Object.keys(opData),
  });
  return true;
}

/**
 * Stop transcription on an active call.
 * 
 * @param callId - The active call ID
 * @returns Success status
 */
async function callStopTranscriptionAPI(callId: string): Promise<boolean> {
  if (!getTokenFn) {
    log('error', 'Token getter not initialized');
    return false;
  }

  const token = await getTokenFn();
  if (!token) {
    log('error', 'Failed to get Graph API token');
    return false;
  }

  const baseUrl = `https://graph.microsoft.com/beta/communications/calls/${callId}`;
  const stopAttempts: Array<{ url: string; body: Record<string, any>; label: string }> = [
    {
      url: `${baseUrl}/microsoft.graph.StopTranscription`,
      body: {},
      label: 'action-empty-body',
    },
    {
      url: `${baseUrl}/stopTranscription`,
      body: { clientContext: `missa-stop-${callId}`.slice(0, 256) },
      label: 'short-clientContext',
    },
    {
      url: `${baseUrl}/stopTranscription`,
      body: {},
      label: 'short-empty-body',
    },
  ];

  let lastError: any;
  for (const attempt of stopAttempts) {
    try {
      await axios.post(attempt.url, attempt.body, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      log('info', `Transcription stopped successfully`, { callId, url: attempt.url, attempt: attempt.label });
      return true;
    } catch (error: any) {
      lastError = error;
      const status = error?.response?.status;
      const errorMsg = error?.response?.data?.error?.message || error?.message;
      log('warn', `stopTranscription endpoint attempt failed`, {
        callId,
        url: attempt.url,
        attempt: attempt.label,
        status,
        errorMsg,
      });
    }
  }

  const status = lastError?.response?.status;
  const errorMsg = lastError?.response?.data?.error?.message || lastError?.message;
  log('error', `Failed to stop transcription`, { callId, status, errorMsg });
  return false;
}

// ============================================================================
// AUTO-START LOGIC
// ============================================================================

/**
 * Automatically start transcription when a call is established.
 * Implements retry logic with exponential backoff.
 * 
 * @param callId - The call ID from Graph Communications API
 * @param conversationId - The Teams conversation ID for notifications
 * @param serviceUrl - The Teams service URL for sending messages
 * @param language - Optional language override
 */
export async function autoStartTranscription(
  callId: string,
  conversationId: string,
  serviceUrl: string,
  language?: string
): Promise<void> {
  if (!config.enabled) {
    log('debug', 'Auto-transcription disabled, skipping', { callId });
    return;
  }

  // Check if we already have state for this call
  let state = transcriptionStates.get(callId);
  
  if (state?.status === 'active') {
    log('debug', 'Transcription already active', { callId });
    return;
  }

  if (state?.status === 'failed' || state?.status === 'stopped') {
    log('debug', 'Transcription is in a terminal state, skipping duplicate auto-start trigger', {
      callId,
      status: state.status,
      failedReason: state.failedReason,
    });
    return;
  }

  if (state?.status === 'starting') {
    log('debug', 'Transcription start is already in progress', {
      callId,
      attemptCount: state.attemptCount,
      nextRetryAt: state.nextRetryAt,
    });
    return;
  }

  if (state?.retryTimerId) {
    log('debug', 'Transcription start already scheduled/in progress', {
      callId,
      status: state.status,
      nextRetryAt: state.nextRetryAt,
    });
    return;
  }

  // Initialize state
  if (!state) {
    state = {
      callId,
      conversationId,
      status: 'pending',
      attemptCount: 0,
      language: language || config.defaultLanguage,
    };
    transcriptionStates.set(callId, state);
  }

  log('info', 'Received auto-start request', {
    callId,
    conversationId,
    currentStatus: state.status,
    attemptCount: state.attemptCount,
  });

  // Delay first attempt slightly to let Graph media state settle after establishment.
  if (state.attemptCount === 0 && config.initialStartDelayMs > 0) {
    const delay = config.initialStartDelayMs;
    state.status = 'pending';
    state.nextRetryAt = Date.now() + delay;
    log('info', 'Delaying first transcription attempt for call readiness', {
      callId,
      delay,
      nextRetryAt: state.nextRetryAt,
    });

    state.retryTimerId = setTimeout(() => {
      const currentState = transcriptionStates.get(callId);
      if (currentState) {
        currentState.retryTimerId = undefined;
        void attemptStart(callId, serviceUrl);
      }
    }, delay);
    return;
  }

  // Start the transcription attempt loop
  await attemptStart(callId, serviceUrl);
}

/**
 * Internal function to attempt starting transcription with retries.
 */
async function attemptStart(callId: string, serviceUrl: string): Promise<void> {
  const state = transcriptionStates.get(callId);
  if (!state) return;

  // Check if max retries exceeded
  if (state.attemptCount >= config.retryDelays.length) {
    state.status = 'failed';
    state.nextRetryAt = undefined;
    state.failedReason = `Max retries exceeded${state.lastErrorMessage ? `; last error: ${getErrorText({ status: state.lastErrorStatus, code: state.lastErrorCode, message: state.lastErrorMessage })}` : ''}`;
    log('warn', 'Max transcription start attempts exceeded', {
      callId,
      attempts: state.attemptCount,
      lastErrorStatus: state.lastErrorStatus,
      lastErrorCode: state.lastErrorCode,
      lastErrorMessage: state.lastErrorMessage,
    });
    
    if (config.notifyUsers && sendMessageFn) {
      await sendMessageFn(
        serviceUrl,
        state.conversationId,
        `I couldn't auto-start transcription after ${state.attemptCount} attempts. ` +
        `${state.lastErrorMessage ? `Last error: ${getErrorText({ status: state.lastErrorStatus, code: state.lastErrorCode, message: state.lastErrorMessage })}. ` : ''}` +
        `The meeting policy may not allow it, or there may be a permission issue. ` +
        `You can try starting transcription manually in Teams.`
      );
    }
    return;
  }

  // Check if max wait time exceeded
  if (state.lastAttemptAt && Date.now() - state.lastAttemptAt > config.maxWaitTime) {
    state.status = 'failed';
    state.nextRetryAt = undefined;
    state.failedReason = `Max wait time exceeded${state.lastErrorMessage ? `; last error: ${getErrorText({ status: state.lastErrorStatus, code: state.lastErrorCode, message: state.lastErrorMessage })}` : ''}`;
    log('warn', 'Max transcription wait time exceeded', {
      callId,
      lastErrorStatus: state.lastErrorStatus,
      lastErrorCode: state.lastErrorCode,
      lastErrorMessage: state.lastErrorMessage,
    });
    return;
  }

  state.status = 'starting';
  state.lastAttemptAt = Date.now();
  state.attemptCount++;
  state.nextRetryAt = undefined;

  log('info', `Attempting to start transcription`, {
    callId,
    attempt: state.attemptCount,
    maxAttempts: config.retryDelays.length,
  });

  try {
    if (!state.recordingStatusSet && !state.recordingStatusUnsupported) {
      try {
        await callUpdateRecordingStatusAPI(callId, 'recording');
        state.recordingStatusSet = true;
        log('info', 'updateRecordingStatus succeeded - Teams "Recording" banner should appear for all participants', { callId });
      } catch (recordingError: any) {
        const recordingErrorStatus = recordingError?.status ?? recordingError?.response?.status;
        const recordingErrorData = recordingError?.response?.data;
        const errorCode = recordingErrorData?.error?.code || recordingError?.code;
        const errorMsg = recordingErrorData?.error?.message || recordingError?.message;
        
        // Mark as unsupported so we don't retry
        state.recordingStatusUnsupported = true;
        
        if (errorCode === '8506' || errorMsg?.includes('non-Compliance Recording')) {
          // This is the expected error when tenant doesn't have compliance recording policy
          log('warn', 'updateRecordingStatus unavailable - bot not joined as compliance recorder. Teams UI will NOT show recording banner, but startTranscription may still capture speech.', {
            callId,
            note: 'This requires tenant admin to configure compliance recording policy in Teams Admin Center.',
          });
        } else if (recordingErrorStatus === 403) {
          log('error', 'updateRecordingStatus FAILED with 403 - Teams will NOT show recording banner.', {
            callId,
            status: recordingErrorStatus,
            code: errorCode,
            message: errorMsg,
            fullError: recordingErrorData,
          });
        } else {
          log('warn', 'updateRecordingStatus failed - Teams may not show recording banner', {
            callId,
            status: recordingErrorStatus,
            code: errorCode,
            message: errorMsg,
          });
        }
      }
    }

    const success = await callStartTranscriptionAPI(callId, state.language);
    
    if (success) {
      state.status = 'active';
      state.startedAt = Date.now();
      state.nextRetryAt = undefined;
      state.failedReason = undefined;
      state.lastErrorStatus = undefined;
      state.lastErrorCode = undefined;
      state.lastErrorMessage = undefined;
      state.lastErrorAt = undefined;
      state.lastErrorRecoverable = undefined;

      log('info', 'Transcription started successfully', {
        callId,
        attempts: state.attemptCount,
        timeToStart: Date.now() - (state.lastAttemptAt || Date.now()),
      });

      if (config.notifyUsers && sendMessageFn) {
        await sendMessageFn(
          serviceUrl,
          state.conversationId,
          `Live transcription has been enabled. I'm now capturing the meeting conversation.`
        );
      }
      return;
    }
  } catch (error: any) {
    const isRecoverable = error?.isRecoverable;
    const errorCode = error?.code;
    state.lastErrorStatus = error?.status;
    state.lastErrorCode = errorCode;
    state.lastErrorMessage = error?.message;
    state.lastErrorAt = Date.now();
    state.lastErrorRecoverable = isRecoverable;
    state.failedReason = undefined;
    
    log('warn', `Transcription start attempt ${state.attemptCount} failed`, {
      callId,
      status: error?.status,
      isRecoverable,
      errorCode,
      message: error?.message,
    });

    // If transcription is already running (409 Conflict), treat as success
    if (error?.status === 409 || errorCode === 'TranscriptionAlreadyStarted') {
      state.status = 'active';
      state.startedAt = Date.now();
      state.nextRetryAt = undefined;
      log('info', 'Transcription was already active', { callId });
      return;
    }

    // If not recoverable (e.g., 403 Forbidden), stop retrying
    if (!isRecoverable && error?.status === 403) {
      state.status = 'failed';
      state.nextRetryAt = undefined;
      state.failedReason = `Permission denied: ${getErrorText({ status: error?.status, code: errorCode, message: error?.message })}`;
      log('error', 'Transcription permission denied - stopping retries', {
        callId,
        status: error?.status,
        errorCode,
        message: error?.message,
      });
      
      if (config.notifyUsers && sendMessageFn) {
        await sendMessageFn(
          serviceUrl,
          state.conversationId,
          `I don't have permission to start transcription. ` +
          `Please check that the bot has \`Calls.AccessMedia.All\` permission and ` +
          `the Application Access Policy is configured correctly.`
        );
      }
      return;
    }

    if (!isRecoverable) {
      state.status = 'failed';
      state.nextRetryAt = undefined;
      state.failedReason = getErrorText({ status: error?.status, code: errorCode, message: error?.message });
      log('error', 'Transcription failed with non-recoverable error', {
        callId,
        status: error?.status,
        errorCode,
        message: error?.message,
      });

      if (config.notifyUsers && sendMessageFn) {
        await sendMessageFn(
          serviceUrl,
          state.conversationId,
          `I couldn't start transcription automatically. Last error: ${state.failedReason}. ` +
          `Please check the meeting policy, bot permissions, and application access policy.`
        );
      }
      return;
    }
  }

  // Schedule retry
  const delay = config.retryDelays[Math.min(state.attemptCount, config.retryDelays.length - 1)];
  state.nextRetryAt = Date.now() + delay;
  log('info', `Scheduling transcription retry`, {
    callId,
    delay,
    nextAttempt: state.attemptCount + 1,
    lastErrorStatus: state.lastErrorStatus,
    lastErrorCode: state.lastErrorCode,
    lastErrorMessage: state.lastErrorMessage,
  });

  state.retryTimerId = setTimeout(() => {
    const currentState = transcriptionStates.get(callId);
    if (currentState) {
      currentState.retryTimerId = undefined;
      void attemptStart(callId, serviceUrl);
    }
  }, delay);
}

/**
 * Stop auto-transcription and cleanup state.
 * 
 * @param callId - The call ID
 * @param sendStopCommand - Whether to send stop command to Graph (default: false)
 */
export async function stopAutoTranscription(callId: string, sendStopCommand: boolean = false): Promise<void> {
  const state = transcriptionStates.get(callId);
  
  if (state?.retryTimerId) {
    clearTimeout(state.retryTimerId);
    state.retryTimerId = undefined;
  }

  if (sendStopCommand && state?.status === 'active') {
    await callStopTranscriptionAPI(callId);
    if (state.recordingStatusSet) {
      try {
        await callUpdateRecordingStatusAPI(callId, 'notRecording');
      } catch (error: any) {
        log('warn', 'Failed to clear recording status during stop', {
          callId,
          status: error?.status,
          code: error?.code,
          message: error?.message,
        });
      }
    }
  }

  if (state) {
    state.status = 'stopped';
    state.nextRetryAt = undefined;
  }

  log('info', 'Auto-transcription stopped', { callId, hadActiveState: !!state });
}

/**
 * Cleanup state for a call (call when meeting ends).
 * 
 * @param callId - The call ID
 */
export function cleanupCall(callId: string): void {
  const state = transcriptionStates.get(callId);
  
  if (state?.retryTimerId) {
    clearTimeout(state.retryTimerId);
  }
  
  transcriptionStates.delete(callId);
  log('debug', 'Call state cleaned up', { callId });
}

// ============================================================================
// STATUS & DIAGNOSTICS
// ============================================================================

/**
 * Get the current transcription state for a call.
 */
export function getTranscriptionState(callId: string): TranscriptionState | undefined {
  return transcriptionStates.get(callId);
}

export function getCallDiagnostic(callId: string): TranscriptionCallDiagnostic | undefined {
  const state = transcriptionStates.get(callId);
  if (!state) {
    return undefined;
  }

  return {
    callId: state.callId,
    status: state.status,
    attemptCount: state.attemptCount,
    startedAt: state.startedAt,
    failedReason: state.failedReason,
    lastErrorStatus: state.lastErrorStatus,
    lastErrorCode: state.lastErrorCode,
    lastErrorMessage: state.lastErrorMessage,
    lastErrorAt: state.lastErrorAt,
    lastErrorRecoverable: state.lastErrorRecoverable,
    nextRetryAt: state.nextRetryAt,
    summary: getDiagnosticSummary(state),
  };
}

/**
 * Get all active transcription states.
 */
export function getAllStates(): Map<string, TranscriptionState> {
  return new Map(transcriptionStates);
}

/**
 * Check if transcription is active for a call.
 */
export function isTranscriptionActive(callId: string): boolean {
  const state = transcriptionStates.get(callId);
  return state?.status === 'active';
}

/**
 * Get diagnostic information for troubleshooting.
 */
export function getDiagnostics(): {
  config: AutoTranscriptionConfig;
  activeStates: number;
  states: { callId: string; status: string; attempts: number; summary: string }[];
} {
  const states: { callId: string; status: string; attempts: number; summary: string }[] = [];
  
  transcriptionStates.forEach((state, callId) => {
    states.push({
      callId,
      status: state.status,
      attempts: state.attemptCount,
      summary: getDiagnosticSummary(state),
    });
  });

  return {
    config,
    activeStates: states.filter(s => s.status === 'active').length,
    states,
  };
}

// ============================================================================
// LANGUAGE SUPPORT
// ============================================================================

/**
 * Supported transcription languages (BCP-47 codes).
 * See: https://learn.microsoft.com/en-us/microsoftteams/meeting-transcription-captions
 */
export const SUPPORTED_LANGUAGES = [
  'en-US', 'en-GB', 'en-AU', 'en-CA', 'en-IN', 'en-NZ',
  'fr-FR', 'fr-CA',
  'de-DE',
  'es-ES', 'es-MX',
  'it-IT',
  'pt-BR', 'pt-PT',
  'ja-JP',
  'ko-KR',
  'zh-CN', 'zh-TW',
  'ar-SA',
  'hi-IN',
  'ru-RU',
  'nl-NL',
  'pl-PL',
  'sv-SE',
  'da-DK',
  'fi-FI',
  'nb-NO',
  'tr-TR',
  'th-TH',
  'vi-VN',
  'cs-CZ',
  'el-GR',
  'he-IL',
  'hu-HU',
  'id-ID',
  'ms-MY',
  'ro-RO',
  'sk-SK',
  'uk-UA',
] as const;

export type SupportedLanguage = typeof SUPPORTED_LANGUAGES[number];

/**
 * Check if a language is supported for transcription.
 */
export function isLanguageSupported(language: string): boolean {
  return SUPPORTED_LANGUAGES.includes(language as SupportedLanguage);
}

// ============================================================================
// EXPORTS
// ============================================================================

export default {
  init: initAutoTranscription,
  updateConfig,
  isEnabled,
  setEnabled,
  autoStart: autoStartTranscription,
  stop: stopAutoTranscription,
  cleanup: cleanupCall,
  getState: getTranscriptionState,
  getAllStates,
  isActive: isTranscriptionActive,
  getDiagnostics,
  isLanguageSupported,
  SUPPORTED_LANGUAGES,
};
