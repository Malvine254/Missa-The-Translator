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
  retryTimerId?: ReturnType<typeof setTimeout>;
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

function log(level: 'debug' | 'info' | 'warn' | 'error', message: string, data?: any) {
  const levels = { debug: 0, info: 1, warn: 2, error: 3 };
  if (levels[level] < levels[config.logLevel]) return;

  const timestamp = new Date().toISOString();
  const fullMsg = `${LOG_PREFIX} [${level.toUpperCase()}] ${message}`;
  
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
    log('error', 'Token getter not initialized');
    return false;
  }

  const token = await getTokenFn();
  if (!token) {
    log('error', 'Failed to get Graph API token');
    return false;
  }

  const url = `https://graph.microsoft.com/beta/communications/calls/${callId}/startTranscription`;
  
  log('debug', `Calling startTranscription API`, { callId, language, url });

  try {
    await axios.post(
      url,
      {
        languageTag: language,
        singlePerParticipant: false,
      },
      {
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      }
    );

    log('info', `Transcription started successfully`, { callId });
    return true;
  } catch (error: any) {
    const status = error?.response?.status;
    const errorMsg = error?.response?.data?.error?.message || error?.message;
    const errorCode = error?.response?.data?.error?.code;
    
    log('error', `Failed to start transcription`, {
      callId,
      status,
      errorCode,
      errorMsg,
      fullError: error?.response?.data,
    });

    // Return specific error info for handling
    throw {
      status,
      code: errorCode,
      message: errorMsg,
      isRecoverable: status === 503 || status === 429 || status === 409,
    };
  }
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

  const url = `https://graph.microsoft.com/beta/communications/calls/${callId}/stopTranscription`;

  try {
    await axios.post(url, {}, {
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
    });

    log('info', `Transcription stopped successfully`, { callId });
    return true;
  } catch (error: any) {
    const status = error?.response?.status;
    const errorMsg = error?.response?.data?.error?.message || error?.message;
    log('error', `Failed to stop transcription`, { callId, status, errorMsg });
    return false;
  }
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

  if (state?.status === 'starting' && state.retryTimerId) {
    log('debug', 'Transcription start already in progress', { callId });
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
    state.failedReason = 'Max retries exceeded';
    log('warn', 'Max transcription start attempts exceeded', { callId, attempts: state.attemptCount });
    
    if (config.notifyUsers && sendMessageFn) {
      await sendMessageFn(
        serviceUrl,
        state.conversationId,
        `I couldn't auto-start transcription after ${state.attemptCount} attempts. ` +
        `The meeting policy may not allow it, or there may be a permission issue. ` +
        `You can try starting transcription manually in Teams.`
      );
    }
    return;
  }

  // Check if max wait time exceeded
  if (state.lastAttemptAt && Date.now() - state.lastAttemptAt > config.maxWaitTime) {
    state.status = 'failed';
    state.failedReason = 'Max wait time exceeded';
    log('warn', 'Max transcription wait time exceeded', { callId });
    return;
  }

  state.status = 'starting';
  state.lastAttemptAt = Date.now();
  state.attemptCount++;

  log('info', `Attempting to start transcription`, {
    callId,
    attempt: state.attemptCount,
    maxAttempts: config.retryDelays.length,
  });

  try {
    const success = await callStartTranscriptionAPI(callId, state.language);
    
    if (success) {
      state.status = 'active';
      state.startedAt = Date.now();

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
    
    log('warn', `Transcription start attempt ${state.attemptCount} failed`, {
      callId,
      isRecoverable,
      errorCode,
      message: error?.message,
    });

    // If transcription is already running (409 Conflict), treat as success
    if (error?.status === 409 || errorCode === 'TranscriptionAlreadyStarted') {
      state.status = 'active';
      state.startedAt = Date.now();
      log('info', 'Transcription was already active', { callId });
      return;
    }

    // If not recoverable (e.g., 403 Forbidden), stop retrying
    if (!isRecoverable && error?.status === 403) {
      state.status = 'failed';
      state.failedReason = `Permission denied: ${error?.message}`;
      log('error', 'Transcription permission denied - stopping retries', { callId });
      
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
  }

  // Schedule retry
  const delay = config.retryDelays[Math.min(state.attemptCount, config.retryDelays.length - 1)];
  log('debug', `Scheduling transcription retry`, { callId, delay, nextAttempt: state.attemptCount + 1 });

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
  }

  if (state) {
    state.status = 'stopped';
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
  states: { callId: string; status: string; attempts: number }[];
} {
  const states: { callId: string; status: string; attempts: number }[] = [];
  
  transcriptionStates.forEach((state, callId) => {
    states.push({
      callId,
      status: state.status,
      attempts: state.attemptCount,
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
