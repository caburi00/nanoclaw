import { describe, expect, it } from 'vitest';

import { shouldWipeAuthOnClose } from './whatsapp-auth-policy.js';

// Baileys DisconnectReason.loggedOut is 401; the test uses a literal so it
// doesn't pull in the heavy Baileys module.
const LOGGED_OUT = 401;

describe('shouldWipeAuthOnClose', () => {
  it('wipes auth on a real logout when not shutting down', () => {
    expect(shouldWipeAuthOnClose(false, LOGGED_OUT, LOGGED_OUT)).toBe(true);
  });

  it('does NOT wipe during shutdown even with a loggedOut-shaped close (SIGTERM creds-wipe regression)', () => {
    expect(shouldWipeAuthOnClose(true, LOGGED_OUT, LOGGED_OUT)).toBe(false);
  });

  it('does not wipe on a non-logout disconnect', () => {
    expect(shouldWipeAuthOnClose(false, 503, LOGGED_OUT)).toBe(false);
  });

  it('does not wipe when the disconnect reason is undefined', () => {
    expect(shouldWipeAuthOnClose(false, undefined, LOGGED_OUT)).toBe(false);
  });
});
