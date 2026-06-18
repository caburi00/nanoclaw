/**
 * Tests for the pending-card rehydration data path: persisting an approval
 * card's delivery target and querying still-open cards per channel so a
 * channel adapter can repopulate its in-memory map on startup.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';

import { closeDb, initTestDb, runMigrations } from './index.js';
import {
  createPendingApproval,
  getOpenCardsForChannel,
  updatePendingApprovalDelivery,
  updatePendingApprovalStatus,
} from './sessions.js';

function now(): string {
  return new Date().toISOString();
}

function seedApproval(id: string): void {
  createPendingApproval({
    approval_id: id,
    request_id: id,
    action: 'cli_command',
    payload: '{}',
    created_at: now(),
    title: `CLI: ${id}`,
    options_json: JSON.stringify([
      { label: 'Approve', selectedLabel: '✅ Approved', value: 'approve' },
      { label: 'Reject', selectedLabel: '❌ Rejected', value: 'reject' },
    ]),
  });
}

beforeEach(() => {
  const db = initTestDb();
  runMigrations(db);
});

afterEach(() => {
  closeDb();
});

describe('getOpenCardsForChannel', () => {
  it('returns a delivered approval card with parsed options, scoped to its channel', () => {
    seedApproval('appr-1');
    updatePendingApprovalDelivery('appr-1', 'whatsapp', '6594599775@s.whatsapp.net', 'wamid-1');

    const wa = getOpenCardsForChannel('whatsapp');
    expect(wa).toHaveLength(1);
    expect(wa[0]).toMatchObject({ questionId: 'appr-1', platformId: '6594599775@s.whatsapp.net' });
    expect(wa[0].options.map((o) => o.value)).toEqual(['approve', 'reject']);

    // Scoped by channel.
    expect(getOpenCardsForChannel('telegram')).toHaveLength(0);
  });

  it('excludes approvals with no recorded delivery target (NULL platform_id)', () => {
    seedApproval('appr-undelivered');
    // No updatePendingApprovalDelivery call → platform_id stays NULL.
    expect(getOpenCardsForChannel('whatsapp')).toHaveLength(0);
  });

  it('excludes resolved approvals (status no longer pending)', () => {
    seedApproval('appr-done');
    updatePendingApprovalDelivery('appr-done', 'whatsapp', '6594599775@s.whatsapp.net', 'wamid-2');
    // A non-pending status must be excluded from rehydration.
    updatePendingApprovalStatus('appr-done', 'approved');

    expect(getOpenCardsForChannel('whatsapp').find((c) => c.questionId === 'appr-done')).toBeUndefined();
  });

  it('orders oldest-first so the most recent card wins a single-slot map', () => {
    seedApproval('appr-old');
    updatePendingApprovalDelivery('appr-old', 'whatsapp', 'chat@s.whatsapp.net', null);
    seedApproval('appr-new');
    updatePendingApprovalDelivery('appr-new', 'whatsapp', 'chat@s.whatsapp.net', null);

    const ids = getOpenCardsForChannel('whatsapp').map((c) => c.questionId);
    expect(ids).toEqual(['appr-old', 'appr-new']);
  });
});
