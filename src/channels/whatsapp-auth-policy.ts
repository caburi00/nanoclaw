/**
 * Whether a WhatsApp 'close' event should wipe the stored auth dir (which forces
 * a fresh QR / pairing on next start).
 *
 * It must ONLY wipe on a *real* logout and NOT during host shutdown: SIGTERM
 * makes Baileys emit a logged-out-shaped close event, and wiping there would
 * destroy the credentials on every restart — the credential-wipe-on-shutdown
 * bug this guard exists to prevent. Kept as a standalone pure function so the
 * regression can be unit-tested without importing the Baileys-heavy adapter.
 */
export function shouldWipeAuthOnClose(
  shuttingDown: boolean,
  reason: number | undefined,
  loggedOutCode: number,
): boolean {
  return !shuttingDown && reason === loggedOutCode;
}
