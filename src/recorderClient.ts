import * as crypto from 'crypto';

const RECORDER_ENABLED       = (process.env.RECORDER_ENABLED || 'false').toLowerCase() === 'true';
const RECORDER_BASE_URL      = (process.env.RECORDER_BASE_URL || '').replace(/\/+$/, '');
const RECORDER_SHARED_SECRET =
  process.env.RECORDER_SHARED_SECRET ||
  process.env.SECRET_RECORDER_SHARED_SECRET ||
  '';

// One-time startup log — confirms env vars are present in the live process.
// Expect: enabled=true, base=https://missa-recorder.azurewebsites.net, secretLen=64
console.log('[RECORDER] startup env', {
  enabled:   process.env.RECORDER_ENABLED,
  base:      process.env.RECORDER_BASE_URL,
  secretLen: (process.env.RECORDER_SHARED_SECRET || process.env.SECRET_RECORDER_SHARED_SECRET || '').length,
});

export function isRecorderEnabled(): boolean {
  if (!RECORDER_ENABLED)        return false;
  if (!RECORDER_BASE_URL)       { console.warn('[RECORDER] RECORDER_BASE_URL is empty');       return false; }
  if (!RECORDER_SHARED_SECRET)  { console.warn('[RECORDER] RECORDER_SHARED_SECRET is empty');  return false; }
  return true;
}

export interface EnableRecordingArgs {
  correlationId:   string;
  meetingId:       string;
  organizerUserId: string;
  tenantId:        string;
  conversationId:  string;
  joinWebUrl?:     string;
  watchUntilUtc?:  Date;
}

export interface EnableRecordingPayload {
  correlationId:   string;
  meetingId:       string;
  organizerUserId: string;
  tenantId:        string;
  conversationId:  string;
  joinWebUrl:      string;
  watchUntilUtc:   string;
}

export interface EnableRecordingResult {
  ok:                boolean;
  recordingWatchId?: string;
  status?:           string;
  error?:            string;
}

/** POST {RECORDER_BASE_URL}/api/recordings/enable. Best-effort, never throws. */
export async function enableRecording(args: EnableRecordingArgs): Promise<EnableRecordingResult> {
  if (!isRecorderEnabled()) return { ok: false, error: 'recorder_not_configured' };

  let payload: EnableRecordingPayload;
  try {
    payload = buildEnableRecordingPayload(args);
  } catch (err: any) {
    return { ok: false, error: err?.message || 'invalid_enable_payload' };
  }

  const body = JSON.stringify(payload);

  const ts  = Math.floor(Date.now() / 1000);
  const sig = signHmac(body, ts);
  const url = `${RECORDER_BASE_URL}/api/recordings/enable`;

  console.log(
    `[RECORDER] enable POST →\n` +
    `  url            : ${url}\n` +
    `  ts             : ${ts}\n` +
    `  sigLen         : ${sig.length}\n` +
    `  correlationId  : ${payload.correlationId}\n` +
    `  meetingId      : ${payload.meetingId}\n` +
    `  organizerUserId: ${payload.organizerUserId}\n` +
    `  tenantId       : ${payload.tenantId}\n` +
    `  conversationId : ${payload.conversationId}\n` +
    `  joinWebUrl     : ${payload.joinWebUrl}\n` +
    `  watchUntilUtc  : ${payload.watchUntilUtc}`
  );

  try {
    const ctl = new AbortController();
    const tm  = setTimeout(() => ctl.abort(), 10_000);
    let res: Response;
    try {
      res = await fetch(url, {
        method:  'POST',
        headers: {
          'Content-Type':      'application/json',
          'X-Missa-Timestamp': String(ts),
          'X-Missa-Signature': sig,
        },
        body,
        signal: ctl.signal,
      });
    } finally { clearTimeout(tm); }

    const text = await res.text();
    if (!res.ok) {
      console.warn(`[RECORDER] enableRecording failed: status=${res.status}, body=${truncate(text, 500)}`);
      return { ok: false, error: `http_${res.status}` };
    }

    let parsed: { recordingWatchId?: string; status?: string } = {};
    try { parsed = JSON.parse(text); } catch { /* 2xx with empty body is still ok */ }

    console.log(`[RECORDER] enableRecording ok: watchId=${parsed.recordingWatchId}, status=${parsed.status}`);
    return { ok: true, recordingWatchId: parsed.recordingWatchId, status: parsed.status };
  } catch (err: any) {
    console.warn(`[RECORDER] enableRecording threw: ${err?.message || err}`);
    return { ok: false, error: 'network_or_timeout' };
  }
}

export function buildEnableRecordingPayload(args: EnableRecordingArgs): EnableRecordingPayload {
  const required: Array<keyof EnableRecordingArgs> = [
    'correlationId',
    'meetingId',
    'organizerUserId',
    'tenantId',
    'conversationId',
  ];

  const missing = required.filter((key) => !isNonEmptyString(args[key]));
  if (missing.length > 0) {
    throw new Error(`missing_fields:${missing.join(',')}`);
  }

  if (!isNonEmptyString(args.joinWebUrl)) {
    throw new Error('missing_fields:joinWebUrl');
  }

  const watchUntil = args.watchUntilUtc || new Date(Date.now() + 6 * 60 * 60 * 1000);
  if (!(watchUntil instanceof Date) || !Number.isFinite(watchUntil.getTime())) {
    throw new Error('invalid_watchUntilUtc');
  }

  return {
    correlationId:   args.correlationId.trim(),
    meetingId:       args.meetingId.trim(),
    organizerUserId: args.organizerUserId.trim(),
    tenantId:        args.tenantId.trim(),
    conversationId:  args.conversationId.trim(),
    joinWebUrl:      args.joinWebUrl.trim(),
    watchUntilUtc:   watchUntil.toISOString(),
  };
}

/** Returns null on success, or a string error code on failure. */
export function verifyInboundBearer(authorizationHeader: string | undefined): string | null {
  if (!RECORDER_SHARED_SECRET) return 'no_shared_secret_configured';
  if (!authorizationHeader)    return 'missing_authorization';

  const m = /^Bearer\s+(.+)$/i.exec(authorizationHeader.trim());
  if (!m) return 'malformed_authorization';

  const expected = Buffer.from(RECORDER_SHARED_SECRET, 'utf8');
  const provided = Buffer.from(m[1].trim(), 'utf8');
  if (expected.length !== provided.length) return 'length_mismatch';
  return crypto.timingSafeEqual(expected, provided) ? null : 'token_mismatch';
}

export interface RecordingCompletedNotification {
  correlationId:    string;
  recordingWatchId: string;
  conversationId:   string;
  meetingId:        string;
  status:           'succeeded' | 'failed' | 'expired';
  blobUri?:         string;
  blobName?:        string;
  sha256?:          string;
  durationSec?:     number;
  sizeBytes?:       number;
  recordingId?:     string;
  createdDateTime?: string;
  endDateTime?:     string;
  error?:           { code: string; message: string };
}

/**
 * Verifies X-Missa-Timestamp + X-Missa-Signature on inbound HMAC-signed requests.
 * Returns null on success, or a string error code on failure.
 */
export function verifyInboundHmac(
  timestampHeader: string | undefined,
  signatureHeader: string | undefined,
  rawBody: string
): string | null {
  if (!RECORDER_SHARED_SECRET) return 'no_shared_secret_configured';
  if (!timestampHeader)        return 'missing_timestamp';
  if (!signatureHeader)        return 'missing_signature';

  const ts = parseInt(timestampHeader, 10);
  if (isNaN(ts))               return 'invalid_timestamp_format';

  const driftSec = Math.abs(Math.floor(Date.now() / 1000) - ts);
  if (driftSec > 300)          return `clock_skew_exceeded_${driftSec}s`;

  const expected = signHmac(rawBody, ts);
  const eBuf = Buffer.from(expected,          'utf8');
  const pBuf = Buffer.from(signatureHeader.trim(), 'utf8');
  if (eBuf.length !== pBuf.length)  return 'length_mismatch';
  return crypto.timingSafeEqual(eBuf, pBuf) ? null : 'signature_mismatch';
}

function signHmac(body: string, unixTimestamp: number): string {
  const h = crypto.createHmac('sha256', RECORDER_SHARED_SECRET);
  h.update(`${unixTimestamp}.${body}`, 'utf8');
  return 'sha256=' + h.digest('hex');
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function truncate(s: string, max: number): string {
  return s.length <= max ? s : s.slice(0, max) + '…';
}
