import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
} from '@whiskeysockets/baileys';
import type { WASocket, WAMessage, GroupMetadata } from '@whiskeysockets/baileys';
import pino from 'pino';
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { format } from 'date-fns';
import qrcode from 'qrcode-terminal';
import {
  loadPolicy,
  isGroupJid,
  resolveAllowedJids,
  unmatchedEntries,
  describePolicy,
  type Policy,
} from './policy.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface WhatsAppGroupSummary {
  id: string;
  name: string;
  memberCount: number;
  lastMessage: string;
  lastActivityTimestamp: number;
}

export interface WhatsAppMessageEntry {
  id: string;
  body: string;
  author: string;
  authorName: string;
  timestamp: number;
  hasMedia: boolean;
  isForwarded: boolean;
  quotedMsg?: { body: string; author: string } | undefined;
}

export interface GroupParticipant {
  id: string;
  name: string;
  isAdmin: boolean;
}

export interface WhatsAppGroupInfo {
  id: string;
  name: string;
  description: string;
  participants: GroupParticipant[];
  createdAt: number;
}

export interface GetMessagesOptions {
  limit?: number;
  after?: number;
  before?: number;
}

// ---------------------------------------------------------------------------
// Logging — always stderr so MCP protocol (stdout) is not polluted
// ---------------------------------------------------------------------------

function log(level: 'info' | 'warn' | 'error', message: string, data?: unknown): void {
  const ts = new Date().toISOString();
  const prefix = `[${ts}] [whatsapp-client] [${level.toUpperCase()}]`;
  if (data !== undefined) {
    process.stderr.write(`${prefix} ${message} ${JSON.stringify(data)}\n`);
  } else {
    process.stderr.write(`${prefix} ${message}\n`);
  }
}

// ---------------------------------------------------------------------------
// Async Mutex — serializes WhatsApp WebSocket calls (cheap insurance)
// ---------------------------------------------------------------------------

class AsyncMutex {
  private queue: Array<() => void> = [];
  private locked = false;
  private lastRelease = 0;

  constructor(private readonly minIntervalMs: number = 100) {}

  async acquire(): Promise<void> {
    if (this.locked) {
      await new Promise<void>((resolve) => this.queue.push(resolve));
    }
    this.locked = true;

    const now = Date.now();
    const elapsed = now - this.lastRelease;
    if (elapsed < this.minIntervalMs && this.lastRelease > 0) {
      const delay = this.minIntervalMs - elapsed;
      await new Promise<void>((resolve) => setTimeout(resolve, delay));
    }
  }

  release(): void {
    this.lastRelease = Date.now();
    const next = this.queue.shift();
    if (next) {
      next();
    } else {
      this.locked = false;
    }
  }

  async run<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

// ---------------------------------------------------------------------------
// BufferEntry — internal type for the per-group ring buffer
// ---------------------------------------------------------------------------

interface BufferEntry {
  id: string;
  body: string;
  author: string;
  authorName: string;
  timestamp: number;
  hasMedia: boolean;
  isForwarded: boolean;
  fromMe: boolean;
  quotedMsg?: { body: string; author: string };
  waKey: any;
  waMessage: any;
}

// ---------------------------------------------------------------------------
// MessageBuffer — bounded per-group ring buffer with disk persistence
//
// Persists snapshot every 60s AND on graceful shutdown.  Rehydrates on
// startup.  This snapshot is load-bearing — messages.history-set is a
// one-shot on first pair and does NOT re-fire on reconnect.
// ---------------------------------------------------------------------------

class MessageBuffer {
  private buffers = new Map<string, BufferEntry[]>();
  private readonly maxPerGroup = 500;
  private readonly snapshotPath: string;
  private snapshotInterval: NodeJS.Timeout | null = null;
  private _lastUpsertTs = 0;

  // Set of group JIDs the policy allows. `null` until the allowlist has been
  // resolved against the live group list. Before resolution, group messages
  // are accepted (and purged on resolution); non-group JIDs are never stored.
  private allowedJids: Set<string> | null = null;

  constructor(authDir: string) {
    this.snapshotPath = join(authDir, 'buffer.json');
  }

  get allowed(): Set<string> | null {
    return this.allowedJids;
  }

  isAllowedJid(jid: string): boolean {
    if (!isGroupJid(jid)) return false;
    if (this.allowedJids === null) return true;
    return this.allowedJids.has(jid);
  }

  /** Apply a resolved allowlist and drop every buffer outside it. */
  setAllowedJids(allowed: Set<string>): void {
    this.allowedJids = allowed;
    let dropped = 0;
    for (const jid of [...this.buffers.keys()]) {
      if (!allowed.has(jid)) {
        this.buffers.delete(jid);
        dropped++;
      }
    }
    if (dropped > 0) log('info', `Buffer purge: dropped ${dropped} chats outside the allowlist`);
  }

  get lastUpsertTs(): number {
    return this._lastUpsertTs;
  }

  get totalSize(): number {
    let total = 0;
    for (const buf of this.buffers.values()) total += buf.length;
    return total;
  }

  get groupCount(): number {
    return this.buffers.size;
  }

  rehydrate(): boolean {
    try {
      if (!existsSync(this.snapshotPath)) return false;
      const raw = readFileSync(this.snapshotPath, 'utf-8');
      const data: Record<string, BufferEntry[]> = JSON.parse(raw);
      for (const [jid, entries] of Object.entries(data)) {
        if (!isGroupJid(jid)) continue; // never rehydrate personal chats
        this.buffers.set(jid, entries.slice(-this.maxPerGroup));
      }
      log('info', `Buffer rehydrated: ${this.totalSize} messages across ${this.groupCount} groups`);
      return true;
    } catch (err) {
      log('warn', 'Buffer rehydration failed', err);
      return false;
    }
  }

  upsert(jid: string, entries: BufferEntry[]): void {
    if (!this.isAllowedJid(jid)) return;
    let buf = this.buffers.get(jid);
    if (!buf) {
      buf = [];
      this.buffers.set(jid, buf);
    }

    const existingIds = new Set(buf.map((e) => e.id));
    for (const entry of entries) {
      if (!existingIds.has(entry.id)) {
        buf.push(entry);
        existingIds.add(entry.id);
      }
    }

    buf.sort((a, b) => a.timestamp - b.timestamp);
    if (buf.length > this.maxPerGroup) {
      this.buffers.set(jid, buf.slice(-this.maxPerGroup));
    }

    this._lastUpsertTs = Date.now();
  }

  get(jid: string, limit: number, after?: number, before?: number): BufferEntry[] {
    const buf = this.buffers.get(jid) || [];
    let filtered: BufferEntry[] = buf;

    if (after !== undefined) {
      filtered = filtered.filter((e) => e.timestamp >= after);
    }
    if (before !== undefined) {
      filtered = filtered.filter((e) => e.timestamp <= before);
    }

    return filtered.slice(-limit);
  }

  getForGroup(jid: string): BufferEntry[] {
    return this.buffers.get(jid) || [];
  }

  search(query: string, jid?: string, limit = 50): BufferEntry[] {
    const lowerQuery = query.toLowerCase();
    const results: BufferEntry[] = [];

    const jids = jid ? [jid] : [...this.buffers.keys()];
    for (const j of jids) {
      if (!this.isAllowedJid(j)) continue;
      const buf = this.buffers.get(j) || [];
      for (const entry of buf) {
        if (entry.body.toLowerCase().includes(lowerQuery)) {
          results.push(entry);
          if (results.length >= limit) return results;
        }
      }
    }

    return results;
  }

  findById(messageId: string): BufferEntry | undefined {
    for (const buf of this.buffers.values()) {
      const found = buf.find((e) => e.id === messageId);
      if (found) return found;
    }
    return undefined;
  }

  snapshot(): void {
    try {
      const data: Record<string, BufferEntry[]> = {};
      for (const [jid, entries] of this.buffers.entries()) {
        data[jid] = entries;
      }
      const dir = join(this.snapshotPath, '..');
      if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
      writeFileSync(this.snapshotPath, JSON.stringify(data), 'utf-8');
      log('info', `Buffer snapshot: ${this.totalSize} messages`);
    } catch (err) {
      log('error', 'Buffer snapshot failed', err);
    }
  }

  startPeriodicSnapshot(): void {
    if (this.snapshotInterval) return;
    this.snapshotInterval = setInterval(() => this.snapshot(), 60_000);
  }

  stopPeriodicSnapshot(): void {
    if (this.snapshotInterval) {
      clearInterval(this.snapshotInterval);
      this.snapshotInterval = null;
    }
  }
}

// ---------------------------------------------------------------------------
// Group list hygiene — pure, exported for unit testing
//
// groupFetchAllParticipating() can surface more than one JID with the same
// subject: the live group, a Community parent/announce shell, and stale or
// migrated JIDs left behind after a group upgrade. Those orphans show up with
// a single participant (just you) and no buffered activity. Dedupe by JID and
// drop the degenerate (<2 member) entries so name resolution and the daily
// brief are never fed phantom groups. Real targeting is by JID — see
// resolveGroup in tools.ts.
// ---------------------------------------------------------------------------

export function dedupeAndFilterGroups(
  summaries: WhatsAppGroupSummary[],
): WhatsAppGroupSummary[] {
  const byJid = new Map<string, WhatsAppGroupSummary>();
  for (const s of summaries) {
    if (s.memberCount < 2) continue; // orphaned / migrated JID — never a real target
    const existing = byJid.get(s.id);
    if (!existing || s.lastActivityTimestamp > existing.lastActivityTimestamp) {
      byJid.set(s.id, s);
    }
  }
  return [...byJid.values()].sort(
    (a, b) => b.lastActivityTimestamp - a.lastActivityTimestamp,
  );
}

// ---------------------------------------------------------------------------
// WhatsAppClient — Baileys WebSocket client
// ---------------------------------------------------------------------------

export class WhatsAppClient {
  private sock: WASocket | null = null;
  private ready = false;
  private buffer: MessageBuffer;
  private mutex = new AsyncMutex(100);
  private contacts = new Map<string, string>();
  private connectionOpen = false;
  private bufferWarm = false;
  private readyResolve: (() => void) | null = null;
  private destroying = false;
  private saveCreds: (() => Promise<void>) | null = null;
  private readonly policy: Policy;
  private readonly authDir: string;
  private allowlistResolved = false;

  private static readonly BAILEYS_LOGGER = pino({ level: 'silent' }, pino.destination(2));

  // Auth state lives next to the repo, not in the caller's cwd: an MCP host
  // may spawn this server from any directory.
  private static defaultAuthDir(sessionName: string): string {
    const here = dirname(fileURLToPath(import.meta.url));
    const repoRoot = resolve(here, '..', '..');
    return join(repoRoot, `.baileys_auth-${sessionName}`);
  }

  constructor(private readonly sessionName: string, policy?: Policy) {
    this.policy = policy ?? loadPolicy();
    this.authDir = process.env.WHATSAPP_AUTH_DIR
      ? resolve(process.env.WHATSAPP_AUTH_DIR)
      : WhatsAppClient.defaultAuthDir(sessionName);
    this.buffer = new MessageBuffer(this.authDir);
    log('info', `Policy: ${describePolicy(this.policy)}`);
    log('info', `Auth dir: ${this.authDir}`);
  }

  getPolicy(): Policy {
    return this.policy;
  }

  isReadOnly(): boolean {
    return this.policy.readOnly;
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  async initialize(): Promise<void> {
    log('info', 'Initializing WhatsApp client (Baileys)...');

    const rehydrated = this.buffer.rehydrate();
    this.bufferWarm = rehydrated;

    await this.createSocket();
    await this.waitForReady();

    await this.refreshAllowedJids();
    this.buffer.startPeriodicSnapshot();
    log(
      'info',
      `WhatsApp client ready (buffer: ${this.buffer.totalSize} messages across ${this.buffer.groupCount} groups)`,
    );
  }

  private async createSocket(): Promise<void> {
    const { state, saveCreds } = await useMultiFileAuthState(this.authDir);
    this.saveCreds = saveCreds;

    const { version } = await fetchLatestBaileysVersion();
    log('info', `WA Web version: ${version.join('.')}`);

    this.sock = makeWASocket({
      auth: state,
      version,
      logger: WhatsAppClient.BAILEYS_LOGGER,
      getMessage: async (key) => {
        const entry = this.buffer.findById(key.id || '');
        return entry?.waMessage || undefined;
      },
    });

    this.sock.ev.on('creds.update', () => this.saveCreds?.());
    this.registerEventHandlers();
  }

  isReady(): boolean {
    return this.ready;
  }

  async destroy(): Promise<void> {
    log('info', 'Shutting down WhatsApp client...');
    this.destroying = true;
    this.ready = false;
    this.buffer.snapshot();
    this.buffer.stopPeriodicSnapshot();
    try {
      this.sock?.end(undefined);
    } catch {
      // socket may already be closed
    }
    log('info', 'WhatsApp client destroyed.');
  }

  // -----------------------------------------------------------------------
  // Groups
  // -----------------------------------------------------------------------

  /**
   * Every group on the account, ignoring the allowlist. Not reachable from
   * any MCP tool: only the pairing script uses it so the user can pick
   * which groups to allow.
   */
  async getAllGroupsUnfiltered(): Promise<WhatsAppGroupSummary[]> {
    this.ensureReady();
    return this.mutex.run(() => this.fetchGroupSummaries());
  }

  /** Fetch the live group list and refresh the buffer's allowed-JID set. */
  private async refreshAllowedJids(): Promise<void> {
    try {
      const all = await this.mutex.run(() => this.fetchGroupSummaries());
      const allowed = resolveAllowedJids(all, this.policy.allowlist);
      this.buffer.setAllowedJids(allowed);
      this.allowlistResolved = true;
      const missing = unmatchedEntries(all, this.policy.allowlist);
      log(
        'info',
        `Allowlist resolved: ${allowed.size} of ${all.length} groups exposed` +
          (missing.length ? `; unmatched entries: ${missing.map((m) => `"${m}"`).join(', ')}` : ''),
      );
    } catch (err) {
      log('error', 'Allowlist resolution failed; buffer stays fail-closed', err);
      if (!this.allowlistResolved) this.buffer.setAllowedJids(new Set());
    }
  }

  private assertAllowed(groupId: string): void {
    const allowed = this.buffer.allowed;
    if (!allowed || !allowed.has(groupId)) {
      throw new Error(`Group ${groupId} is not in WHATSAPP_GROUP_ALLOWLIST`);
    }
  }

  private async fetchGroupSummaries(): Promise<WhatsAppGroupSummary[]> {
      log('info', 'getGroups: fetching participating groups...');
      const groups = await this.sock!.groupFetchAllParticipating();

      const summaries: WhatsAppGroupSummary[] = Object.values(groups).map(
        (g: GroupMetadata) => {
          const buf = this.buffer.getForGroup(g.id);
          const last = buf.length > 0 ? buf[buf.length - 1] : null;

          return {
            id: g.id,
            name: g.subject,
            memberCount: g.participants?.length ?? 0,
            lastMessage: last?.body ?? '',
            lastActivityTimestamp: last?.timestamp ?? 0,
          };
        },
      );

      const filtered = dedupeAndFilterGroups(summaries);
      log(
        'info',
        `getGroups: ${filtered.length} groups ` +
          `(${summaries.length - filtered.length} phantom/duplicate entries filtered)`,
      );
      return filtered;
  }

  /** Groups visible to MCP tools: the live list intersected with the allowlist. */
  async getGroups(): Promise<WhatsAppGroupSummary[]> {
    this.ensureReady();
    const all = await this.mutex.run(() => this.fetchGroupSummaries());
    const allowed = resolveAllowedJids(all, this.policy.allowlist);
    this.buffer.setAllowedJids(allowed);
    this.allowlistResolved = true;
    const visible = all.filter((g) => allowed.has(g.id));
    log('info', `getGroups: returning ${visible.length} allowlisted groups`);
    return visible;
  }

  async getGroupMessages(
    groupId: string,
    options: GetMessagesOptions = {},
  ): Promise<WhatsAppMessageEntry[]> {
    this.ensureReady();
    this.assertAllowed(groupId);
    const { limit = 200, after, before } = options;
    log('info', `getGroupMessages: groupId=${groupId}, limit=${limit}`);

    const entries = this.buffer.get(groupId, limit, after, before);

    const result: WhatsAppMessageEntry[] = entries.map((e) => ({
      id: e.id,
      body: e.body,
      author: e.author,
      authorName: e.authorName,
      timestamp: e.timestamp,
      hasMedia: e.hasMedia,
      isForwarded: e.isForwarded,
      quotedMsg: e.quotedMsg,
    }));

    log('info', `getGroupMessages: returning ${result.length} messages`);
    return result;
  }

  async getGroupInfo(groupId: string): Promise<WhatsAppGroupInfo> {
    this.ensureReady();
    this.assertAllowed(groupId);
    return this.mutex.run(async () => {
      log('info', `getGroupInfo: groupId=${groupId}`);
      const meta = await this.sock!.groupMetadata(groupId);

      const participants: GroupParticipant[] = (meta.participants ?? []).map((p) => ({
        id: p.id,
        name: this.contacts.get(p.id) || p.id.replace(/@.*/, ''),
        isAdmin: p.admin === 'admin' || p.admin === 'superadmin',
      }));

      log(
        'info',
        `getGroupInfo: returning "${meta.subject}" with ${participants.length} participants`,
      );
      return {
        id: meta.id,
        name: meta.subject,
        description: meta.desc ?? '',
        participants,
        createdAt: meta.creation ?? 0,
      };
    });
  }

  // -----------------------------------------------------------------------
  // Search
  // -----------------------------------------------------------------------

  async searchMessages(
    query: string,
    groupId?: string,
    limit = 50,
  ): Promise<WhatsAppMessageEntry[]> {
    this.ensureReady();
    if (groupId) this.assertAllowed(groupId);
    log('info', `searchMessages: query="${query}", groupId=${groupId ?? 'all'}, limit=${limit}`);

    const entries = this.buffer.search(query, groupId, limit);

    const results: WhatsAppMessageEntry[] = entries.map((e) => ({
      id: e.id,
      body: e.body,
      author: e.author,
      authorName: e.authorName,
      timestamp: e.timestamp,
      hasMedia: e.hasMedia,
      isForwarded: e.isForwarded,
      quotedMsg: e.quotedMsg,
    }));

    log('info', `searchMessages: returning ${results.length} results`);
    return results;
  }

  // -----------------------------------------------------------------------
  // Export
  // -----------------------------------------------------------------------

  async exportChat(groupId: string, limit = 500): Promise<string> {
    this.ensureReady();
    this.assertAllowed(groupId);
    log('info', `exportChat: groupId=${groupId}, limit=${limit}`);

    const entries = this.buffer.get(groupId, limit);
    const lines: string[] = [];

    for (const e of entries) {
      const date = new Date(e.timestamp * 1000);
      const dateStr = format(date, 'dd/MM/yyyy, HH:mm:ss');
      const body = e.hasMedia ? '<Media omitted>' : (e.body || '');

      const bodyLines = body.split('\n');
      lines.push(`[${dateStr}] ${e.authorName}: ${bodyLines[0]}`);
      for (let i = 1; i < bodyLines.length; i++) {
        lines.push(bodyLines[i]);
      }
    }

    log('info', `exportChat: returning ${lines.length} lines`);
    return lines.join('\n');
  }

  // -----------------------------------------------------------------------
  // Send / Reply
  // -----------------------------------------------------------------------

  async sendMessage(
    chatId: string,
    text: string,
    quotedMessageId?: string,
  ): Promise<{ id: string; timestamp: number }> {
    this.ensureReady();
    if (this.policy.readOnly) {
      throw new Error('Sending is disabled: WHATSAPP_READ_ONLY is true');
    }
    this.assertAllowed(chatId);
    return this.mutex.run(async () => {
      log(
        'info',
        `sendMessage: chatId=${chatId}, quotedMessageId=${quotedMessageId ?? 'none'}`,
      );

      const options: any = {};
      if (quotedMessageId) {
        const quotedEntry = this.buffer.findById(quotedMessageId);
        if (quotedEntry?.waKey) {
          options.quoted = {
            key: quotedEntry.waKey,
            message: quotedEntry.waMessage,
            messageTimestamp: quotedEntry.timestamp,
          } as WAMessage;
        } else {
          log('warn', `sendMessage: quoted message ${quotedMessageId} not in buffer`);
        }
      }

      const sent = await this.sock!.sendMessage(chatId, { text }, options);
      const msgId = sent?.key?.id || '';
      const timestamp =
        typeof sent?.messageTimestamp === 'number'
          ? sent.messageTimestamp
          : Number(sent?.messageTimestamp) || Math.floor(Date.now() / 1000);

      if (sent) {
        const entry = this.waMessageToEntry(sent);
        if (entry) this.buffer.upsert(chatId, [entry]);
      }

      log('info', `sendMessage: sent id=${msgId}`);
      return { id: msgId, timestamp };
    });
  }

  // -----------------------------------------------------------------------
  // Private — event handlers
  // -----------------------------------------------------------------------

  private registerEventHandlers(): void {
    if (!this.sock) return;

    this.sock.ev.on('messages.upsert', ({ messages }) => {
      for (const msg of messages) {
        const jid = msg.key.remoteJid;
        if (jid) {
          const entry = this.waMessageToEntry(msg);
          if (entry) this.buffer.upsert(jid, [entry]);
        }
      }
    });

    this.sock.ev.on(
      'messaging-history.set',
      ({ messages, contacts: histContacts, isLatest }) => {
        log('info', `messaging-history.set: ${messages.length} msgs, isLatest=${isLatest}`);
        for (const c of histContacts) {
          const name = (c as any).notify || (c as any).name || '';
          if (name && c.id) this.contacts.set(c.id, name);
        }
        for (const msg of messages) {
          const jid = msg.key.remoteJid;
          if (jid) {
            const entry = this.waMessageToEntry(msg);
            if (entry) this.buffer.upsert(jid, [entry]);
          }
        }
        if (isLatest) {
          this.bufferWarm = true;
          if (this.connectionOpen) this.tryMarkReady();
        }
      },
    );

    this.sock.ev.on('connection.update', ({ connection, lastDisconnect, qr }) => {
      if (qr) {
        // First run only: Baileys no longer prints the QR itself.
        process.stderr.write('\n[whatsapp-client] Scan this QR with WhatsApp > Linked devices > Link a device:\n\n');
        qrcode.generate(qr, { small: true }, (ascii: string) => {
          process.stderr.write(ascii + '\n');
        });
      }
      if (connection === 'open') {
        log('info', 'Connection open');
        this.connectionOpen = true;
        if (this.bufferWarm) this.tryMarkReady();
        if (this.allowlistResolved) {
          // Reconnect: group membership may have changed while we were away.
          this.refreshAllowedJids().catch((err) =>
            log('warn', 'Allowlist refresh after reconnect failed', err),
          );
        }
      } else if (connection === 'close') {
        this.connectionOpen = false;
        this.ready = false;
        if (this.destroying) return;
        const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
        if (statusCode === DisconnectReason.loggedOut) {
          log('error', 'Logged out — exiting hard');
          setTimeout(() => process.exit(1), 50);
        } else {
          log('warn', `Connection closed (statusCode=${statusCode}), reconnecting in 3s`);
          setTimeout(() => {
            if (!this.destroying) {
              this.createSocket().catch((err) =>
                log('error', 'Reconnect failed', err),
              );
            }
          }, 3000);
        }
      }
    });

    this.sock.ev.on('contacts.upsert', (contacts) => {
      for (const c of contacts) {
        const name = (c as any).notify || (c as any).name || '';
        if (name && c.id) this.contacts.set(c.id, name);
      }
    });

    this.sock.ev.on('contacts.update', (updates) => {
      for (const u of updates) {
        const name = (u as any).notify || (u as any).name;
        if (name && u.id) this.contacts.set(u.id, name);
      }
    });
  }

  private tryMarkReady(): void {
    if (this.ready) return;
    this.ready = true;
    if (this.readyResolve) {
      this.readyResolve();
      this.readyResolve = null;
    }
  }

  private waitForReady(): Promise<void> {
    if (this.ready) return Promise.resolve();

    return new Promise<void>((resolve) => {
      this.readyResolve = resolve;
      setTimeout(() => {
        if (!this.ready) {
          log('warn', 'Ready timeout (30s) — proceeding with current buffer state');
          this.bufferWarm = true;
          this.connectionOpen = true;
          this.tryMarkReady();
        }
      }, 30_000);
    });
  }

  private ensureReady(): void {
    if (!this.ready) {
      throw new Error(
        'WhatsApp client is not ready. Call initialize() first and wait for it to resolve.',
      );
    }
  }

  // -----------------------------------------------------------------------
  // Private — message conversion
  // -----------------------------------------------------------------------

  private waMessageToEntry(msg: WAMessage): BufferEntry | null {
    if (!msg.key.remoteJid || !msg.message) return null;

    const body = this.extractBody(msg);
    const hasMedia = this.checkMedia(msg);

    if (!body && !hasMedia) return null;

    const contextInfo = this.extractContextInfo(msg);

    let quotedMsg: { body: string; author: string } | undefined;
    if (contextInfo?.quotedMessage) {
      quotedMsg = {
        body:
          contextInfo.quotedMessage.conversation ||
          contextInfo.quotedMessage.extendedTextMessage?.text ||
          '',
        author: contextInfo.participant || '',
      };
    }

    const timestamp =
      typeof msg.messageTimestamp === 'number'
        ? msg.messageTimestamp
        : Number(msg.messageTimestamp) || 0;

    return {
      id: msg.key.id || '',
      body,
      author: msg.key.participant || msg.key.remoteJid || '',
      authorName: this.resolveAuthorName(msg),
      timestamp,
      hasMedia,
      isForwarded: !!contextInfo?.isForwarded,
      fromMe: msg.key.fromMe || false,
      quotedMsg,
      waKey: msg.key,
      waMessage: msg.message,
    };
  }

  private resolveAuthorName(msg: WAMessage): string {
    if (msg.pushName) return msg.pushName;
    if (msg.key.fromMe) return this.sock?.user?.name || 'Me';
    const jid = msg.key.participant || msg.key.remoteJid || '';
    return this.contacts.get(jid) || jid.replace(/@.*/, '') || 'Unknown';
  }

  private extractBody(msg: WAMessage): string {
    const m = msg.message;
    if (!m) return '';
    return (
      m.conversation ||
      m.extendedTextMessage?.text ||
      m.imageMessage?.caption ||
      m.videoMessage?.caption ||
      m.documentMessage?.caption ||
      m.listResponseMessage?.title ||
      m.buttonsResponseMessage?.selectedDisplayText ||
      m.templateButtonReplyMessage?.selectedDisplayText ||
      ''
    );
  }

  private checkMedia(msg: WAMessage): boolean {
    const m = msg.message;
    if (!m) return false;
    return !!(
      m.imageMessage ||
      m.videoMessage ||
      m.audioMessage ||
      m.documentMessage ||
      m.stickerMessage
    );
  }

  private extractContextInfo(msg: WAMessage): any {
    const m = msg.message;
    if (!m) return null;
    return (
      m.extendedTextMessage?.contextInfo ||
      m.imageMessage?.contextInfo ||
      m.videoMessage?.contextInfo ||
      m.audioMessage?.contextInfo ||
      m.documentMessage?.contextInfo ||
      null
    );
  }
}

// ---------------------------------------------------------------------------
// Singleton factory
// ---------------------------------------------------------------------------

const instances = new Map<string, WhatsAppClient>();

export function getWhatsAppClient(sessionName = 'default', policy?: Policy): WhatsAppClient {
  let instance = instances.get(sessionName);
  if (!instance) {
    instance = new WhatsAppClient(sessionName, policy);
    instances.set(sessionName, instance);
  }
  return instance;
}
