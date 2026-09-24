// ---------------------------------------------------------------------------
// Access policy — group allowlist and read-only mode.
//
// Both are read from the environment once at startup and are deliberately
// fail-closed: with no allowlist configured, the server exposes no groups at
// all, and sending is off unless WHATSAPP_READ_ONLY is literally "false".
//
//   WHATSAPP_GROUP_ALLOWLIST   pipe-separated group names or JIDs. Names are
//                              matched case-insensitively after collapsing
//                              whitespace and punctuation; JIDs exactly.
//                              e.g. "Columbia MBA 2028|CBS Cluster A|1203...@g.us"
//   WHATSAPP_READ_ONLY         "true" (default) or "false".
// ---------------------------------------------------------------------------

export interface Policy {
  /** Raw allowlist entries as configured (trimmed, non-empty). */
  allowlist: string[];
  readOnly: boolean;
}

export interface GroupRef {
  id: string;
  name: string;
}

export function normalizeName(input: string): string {
  return input
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .replace(/\s+/g, ' ')
    .trim();
}

export function isGroupJid(jid: string): boolean {
  return /@g\.us$/i.test(jid);
}

export function loadPolicy(env: NodeJS.ProcessEnv = process.env): Policy {
  const raw = env.WHATSAPP_GROUP_ALLOWLIST ?? '';
  const allowlist = raw
    .split('|')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  const ro = (env.WHATSAPP_READ_ONLY ?? 'true').trim().toLowerCase();
  const readOnly = ro !== 'false';

  return { allowlist, readOnly };
}

/** True when the group is named (or JID-listed) in the allowlist. */
export function isAllowed(group: GroupRef, allowlist: string[]): boolean {
  if (allowlist.length === 0) return false;
  const name = normalizeName(group.name);
  for (const entry of allowlist) {
    if (isGroupJid(entry)) {
      if (entry === group.id) return true;
    } else if (normalizeName(entry) === name) {
      return true;
    }
  }
  return false;
}

/** Resolve the allowlist against the live group list to a set of JIDs. */
export function resolveAllowedJids(
  groups: GroupRef[],
  allowlist: string[],
): Set<string> {
  const out = new Set<string>();
  for (const g of groups) {
    if (isAllowed(g, allowlist)) out.add(g.id);
  }
  return out;
}

/** Allowlist entries that matched no live group — surfaced at startup. */
export function unmatchedEntries(
  groups: GroupRef[],
  allowlist: string[],
): string[] {
  return allowlist.filter((entry) => {
    if (isGroupJid(entry)) return !groups.some((g) => g.id === entry);
    const n = normalizeName(entry);
    return !groups.some((g) => normalizeName(g.name) === n);
  });
}

export function describePolicy(p: Policy): string {
  const list = p.allowlist.length ? p.allowlist.map((e) => `"${e}"`).join(', ') : '(empty — no groups exposed)';
  return `read-only=${p.readOnly}; allowlist=${list}`;
}
