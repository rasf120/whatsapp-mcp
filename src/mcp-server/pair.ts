// One-off pairing and discovery script.
//
//   npm run pair
//
// Links this machine as a WhatsApp device (QR on first run), then prints every
// group on the account so the user can choose what to put in
// WHATSAPP_GROUP_ALLOWLIST. This is the only code path that lists groups
// outside the allowlist, and it is never reachable through MCP.

import { getWhatsAppClient } from './whatsapp.js';
import { loadPolicy, isAllowed } from './policy.js';

async function main(): Promise<void> {
  const sessionName = process.env.WHATSAPP_SESSION_NAME ?? 'default';
  const policy = loadPolicy();
  const client = getWhatsAppClient(sessionName, policy);

  process.stderr.write(`[pair] session "${sessionName}" — connecting...\n`);
  await client.initialize();

  const groups = await client.getAllGroupsUnfiltered();
  const sorted = [...groups].sort((a, b) => a.name.localeCompare(b.name));

  process.stdout.write(`\n${sorted.length} groups on this account:\n\n`);
  for (const g of sorted) {
    const mark = isAllowed(g, policy.allowlist) ? '[allowed]' : '         ';
    process.stdout.write(`${mark} ${g.name}  (${g.memberCount} members)  ${g.id}\n`);
  }
  process.stdout.write(
    '\nSet WHATSAPP_GROUP_ALLOWLIST to a pipe-separated list of the names (or JIDs) to expose,\n' +
      'e.g. WHATSAPP_GROUP_ALLOWLIST="Group One|Group Two"\n',
  );

  await client.destroy();
  process.exit(0);
}

main().catch((err) => {
  process.stderr.write(`[pair] fatal: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
