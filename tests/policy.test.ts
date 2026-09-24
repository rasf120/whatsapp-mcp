import { describe, it, expect } from 'vitest';
import {
  loadPolicy,
  isAllowed,
  resolveAllowedJids,
  unmatchedEntries,
  normalizeName,
  isGroupJid,
} from '../src/mcp-server/policy.js';

const groups = [
  { id: '111@g.us', name: 'Columbia MBA 2028' },
  { id: '222@g.us', name: 'CBS Cluster A — Fall' },
  { id: '333@g.us', name: 'Family' },
];

describe('loadPolicy', () => {
  it('fails closed with no env', () => {
    const p = loadPolicy({});
    expect(p.allowlist).toEqual([]);
    expect(p.readOnly).toBe(true);
  });
  it('parses a pipe-separated allowlist and trims', () => {
    const p = loadPolicy({ WHATSAPP_GROUP_ALLOWLIST: ' Columbia MBA 2028 | 222@g.us |' });
    expect(p.allowlist).toEqual(['Columbia MBA 2028', '222@g.us']);
  });
  it('only literal "false" disables read-only', () => {
    expect(loadPolicy({ WHATSAPP_READ_ONLY: 'no' }).readOnly).toBe(true);
    expect(loadPolicy({ WHATSAPP_READ_ONLY: '0' }).readOnly).toBe(true);
    expect(loadPolicy({ WHATSAPP_READ_ONLY: 'FALSE' }).readOnly).toBe(false);
  });
});

describe('isAllowed', () => {
  it('denies everything on an empty allowlist', () => {
    for (const g of groups) expect(isAllowed(g, [])).toBe(false);
  });
  it('matches names case- and punctuation-insensitively', () => {
    expect(isAllowed(groups[1], ['cbs cluster a fall'])).toBe(true);
    expect(isAllowed(groups[0], ['COLUMBIA   MBA 2028'])).toBe(true);
  });
  it('does not substring-match names', () => {
    expect(isAllowed(groups[0], ['Columbia'])).toBe(false);
    expect(isAllowed(groups[2], ['Fam'])).toBe(false);
  });
  it('matches JIDs exactly', () => {
    expect(isAllowed(groups[2], ['333@g.us'])).toBe(true);
    expect(isAllowed(groups[2], ['33@g.us'])).toBe(false);
  });
});

describe('resolveAllowedJids / unmatchedEntries', () => {
  it('resolves names and JIDs to a JID set', () => {
    const set = resolveAllowedJids(groups, ['columbia mba 2028', '222@g.us']);
    expect([...set].sort()).toEqual(['111@g.us', '222@g.us']);
  });
  it('reports entries that matched nothing', () => {
    expect(unmatchedEntries(groups, ['Family', 'Nope', '999@g.us'])).toEqual(['Nope', '999@g.us']);
  });
});

describe('helpers', () => {
  it('normalizeName keeps unicode letters', () => {
    expect(normalizeName('Ñandú — Club!')).toBe('ñandú club');
  });
  it('isGroupJid', () => {
    expect(isGroupJid('123@g.us')).toBe(true);
    expect(isGroupJid('123@s.whatsapp.net')).toBe(false);
  });
});
