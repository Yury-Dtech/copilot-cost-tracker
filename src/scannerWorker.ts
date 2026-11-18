import * as fs from 'node:fs';
import { parentPort } from 'node:worker_threads';

type UsageEvent = {
  timestamp: number;
  model: string;
  charge: number;
  eventId: string;
  /** Conversation / thread id; empty when the record does not carry one. */
  sessionId: string;
  /** Agent or panel that spent the charge, e.g. `panel/editAgent`. */
  mode: string;
  /** Prompt tokens served from cache. */
  cachedTokens: number;
  /** Total prompt tokens, the cache denominator. */
  inputTokens: number;
  /** Assistant tokens produced. */
  outputTokens: number;
};
type Request = { files: string[]; monthBoundary: number; scanBoundary: number };
type Response = { file: string; size: number; mtimeMs: number; events: UsageEvent[] };

type FileState = {
  size: number;
  mtimeMs: number;
  /** Global offset already processed. */
  offset: number;
  events: UsageEvent[];
};

const cache = new Map<string, FileState>();
/** Chunk size used when streaming a previously unseen file from the start. */
const readChunkBytes = 4 * 1024 * 1024;

function toNumber(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 0;
}

function parseText(text: string, monthBoundary: number): UsageEvent[] {
  const events: UsageEvent[] = [];
  let previousAiu = 0;

  for (const line of text.split(/\r?\n/)) {
    if (!line) {
      continue;
    }
    // Cheap pre-filter: most log lines carry no charge at all.
    if (!line.includes('copilotUsageNanoAiu') && !line.includes('totalNanoAiu')) {
      continue;
    }

    let record: any;
    try {
      record = JSON.parse(line);
    } catch {
      continue;
    }

    let timestamp: number | undefined;
    let model: string | undefined;
    let charge: number | undefined;
    let eventId: string | undefined;
    let sessionId = '';
    let mode = '';
    let cachedTokens = 0;
    let inputTokens = 0;
    let outputTokens = 0;

    if (record?.type === 'llm_request') {
      timestamp = typeof record.ts === 'number' ? record.ts : undefined;
      model = typeof record.attrs?.model === 'string' ? record.attrs.model : undefined;
      charge = typeof record.attrs?.copilotUsageNanoAiu === 'number' ? record.attrs.copilotUsageNanoAiu : undefined;
      eventId = typeof record.spanId === 'string' ? `${timestamp}.${record.spanId}` : undefined;
      sessionId = typeof record.sid === 'string' ? record.sid : '';
      mode = typeof record.attrs?.debugName === 'string' ? record.attrs.debugName : '';
      cachedTokens = toNumber(record.attrs?.cachedTokens);
      inputTokens = toNumber(record.attrs?.inputTokens);
      outputTokens = toNumber(record.attrs?.outputTokens);
    } else if (record?.kind === 'modelCall') {
      timestamp = typeof record.ts === 'string' ? Date.parse(record.ts) : undefined;
      model = typeof record.model === 'string' ? record.model : undefined;
      const total = typeof record.totalNanoAiu === 'number' ? record.totalNanoAiu : 0;
      charge = total >= previousAiu ? total - previousAiu : total;
      previousAiu = total;
      eventId = typeof record.eventId === 'string' ? record.eventId : undefined;
      sessionId = typeof record.sessionId === 'string' ? record.sessionId : '';
    }

    if (!timestamp || !model || !charge || charge <= 0 || !Number.isFinite(charge) || !eventId) {
      continue;
    }
    if (timestamp < monthBoundary) {
      continue;
    }

    events.push({ timestamp, model, charge, eventId, sessionId, mode, cachedTokens, inputTokens, outputTokens });
  }

  return events;
}

/** Reads [start, end) synchronously, dropping a possibly truncated first line. */
function readRange(file: string, start: number, end: number, skipFirstLine: boolean): string {
  if (end <= start) {
    return '';
  }
  const length = end - start;
  const buffer = Buffer.allocUnsafe(length);
  const fd = fs.openSync(file, 'r');
  try {
    fs.readSync(fd, buffer, 0, length, start);
  } finally {
    fs.closeSync(fd);
  }
  const text = buffer.toString('utf8');
  if (!skipFirstLine) {
    return text;
  }
  const newline = text.indexOf('\n');
  return newline === -1 ? '' : text.slice(newline + 1);
}

/**
 * Streams a previously unseen file from byte 0 in chunks, keeping only
 * complete lines, so large logs are fully accounted for without loading the
 * whole file or the whole event list twice. Lines older than the scan window
 * are parsed and then dropped by `parseText` via `monthBoundary`.
 */
function scanWholeFile(file: string, size: number, monthBoundary: number): UsageEvent[] {
  const events: UsageEvent[] = [];
  const fd = fs.openSync(file, 'r');
  try {
    const buffer = Buffer.allocUnsafe(readChunkBytes);
    let carry = '';
    let position = 0;
    while (position < size) {
      const toRead = Math.min(readChunkBytes, size - position);
      const read = fs.readSync(fd, buffer, 0, toRead, position);
      if (read <= 0) {
        break;
      }
      position += read;
      const text = carry + buffer.toString('utf8', 0, read);
      const lastNewline = text.lastIndexOf('\n');
      if (lastNewline === -1) {
        // No line boundary yet; keep buffering (a single line this large is
        // pathological, so cap the carry to avoid unbounded growth).
        carry = text.length > readChunkBytes ? '' : text;
        continue;
      }
      carry = text.slice(lastNewline + 1);
      const complete = text.slice(0, lastNewline + 1);
      for (const event of parseText(complete, monthBoundary)) {
        events.push(event);
      }
    }
    if (carry) {
      for (const event of parseText(carry, monthBoundary)) {
        events.push(event);
      }
    }
  } finally {
    fs.closeSync(fd);
  }
  return events;
}

function merge(existing: UsageEvent[], added: UsageEvent[]): UsageEvent[] {
  if (added.length === 0) {
    return existing;
  }
  const seen = new Set(existing.map(event => event.eventId));
  for (const event of added) {
    if (!seen.has(event.eventId)) {
      seen.add(event.eventId);
      existing.push(event);
    }
  }
  return existing;
}

function scanFile(file: string, request: Request): Response | undefined {
  let stat: fs.Stats;
  try {
    stat = fs.statSync(file);
  } catch {
    return undefined;
  }

  const { size, mtimeMs } = stat;
  const previous = cache.get(file);
  if (previous && previous.size === size && previous.mtimeMs === mtimeMs) {
    return { file, size, mtimeMs, events: previous.events };
  }

  // A file untouched since before the scan window holds nothing relevant.
  if (!previous && mtimeMs < request.scanBoundary) {
    cache.set(file, { size, mtimeMs, offset: size, events: [] });
    return { file, size, mtimeMs, events: [] };
  }

  if (previous) {
    if (size < previous.size) {
      // File was truncated or rotated: forget offsets and re-read the tail.
      cache.delete(file);
      return scanFile(file, request);
    }
    if (size === previous.offset) {
      previous.mtimeMs = mtimeMs;
      return { file, size, mtimeMs, events: previous.events };
    }

    // Incremental: read only the appended bytes. `offset` always sits on a
    // line boundary, so nothing needs to be trimmed here.
    const added = parseText(readRange(file, previous.offset, size, false), request.monthBoundary);
    previous.events = merge(previous.events, added);
    previous.size = size;
    previous.mtimeMs = mtimeMs;
    previous.offset = size;
    return { file, size, mtimeMs, events: previous.events };
  }

  // First sighting: stream the whole file so no in-month record is missed.
  const events = scanWholeFile(file, size, request.monthBoundary);
  cache.set(file, { size, mtimeMs, offset: size, events });
  return { file, size, mtimeMs, events };
}

function scan(request: Request): Response[] {
  const responses: Response[] = [];
  for (const file of request.files) {
    const response = scanFile(file, request);
    if (response) {
      responses.push(response);
    }
  }

  const live = new Set(request.files);
  for (const known of cache.keys()) {
    if (!live.has(known)) {
      cache.delete(known);
    }
  }

  return responses;
}

parentPort?.on('message', (request: Request) => {
  parentPort?.postMessage(scan(request));
});

parentPort?.postMessage('ready');