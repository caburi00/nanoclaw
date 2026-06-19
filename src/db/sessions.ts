import type { PendingApproval, PendingQuestion, Session } from '../types.js';
import { getDb, hasTable } from './connection.js';

// ── Sessions ──

export function createSession(session: Session): void {
  getDb()
    .prepare(
      `INSERT INTO sessions (id, agent_group_id, messaging_group_id, thread_id, agent_provider, status, container_status, last_active, created_at)
       VALUES (@id, @agent_group_id, @messaging_group_id, @thread_id, @agent_provider, @status, @container_status, @last_active, @created_at)`,
    )
    .run(session);
}

export function getSession(id: string): Session | undefined {
  return getDb().prepare('SELECT * FROM sessions WHERE id = ?').get(id) as Session | undefined;
}

export function findSession(messagingGroupId: string, threadId: string | null): Session | undefined {
  if (threadId) {
    return getDb()
      .prepare('SELECT * FROM sessions WHERE messaging_group_id = ? AND thread_id = ? AND status = ?')
      .get(messagingGroupId, threadId, 'active') as Session | undefined;
  }
  return getDb()
    .prepare('SELECT * FROM sessions WHERE messaging_group_id = ? AND thread_id IS NULL AND status = ?')
    .get(messagingGroupId, 'active') as Session | undefined;
}

/**
 * Session lookup scoped to a specific agent group. Needed when multiple
 * agents are wired to the same messaging group + thread (fan-out) — the
 * plain `findSession` would return whichever agent's session happened to
 * be first and route to the wrong container.
 */
export function findSessionForAgent(
  agentGroupId: string,
  messagingGroupId: string,
  threadId: string | null,
): Session | undefined {
  if (threadId) {
    return getDb()
      .prepare(
        "SELECT * FROM sessions WHERE agent_group_id = ? AND messaging_group_id = ? AND thread_id = ? AND status = 'active'",
      )
      .get(agentGroupId, messagingGroupId, threadId) as Session | undefined;
  }
  return getDb()
    .prepare(
      "SELECT * FROM sessions WHERE agent_group_id = ? AND messaging_group_id = ? AND thread_id IS NULL AND status = 'active'",
    )
    .get(agentGroupId, messagingGroupId) as Session | undefined;
}

/** Find an active session scoped to an agent group (ignoring messaging group). */
export function findSessionByAgentGroup(agentGroupId: string): Session | undefined {
  return getDb()
    .prepare("SELECT * FROM sessions WHERE agent_group_id = ? AND status = 'active' ORDER BY created_at DESC LIMIT 1")
    .get(agentGroupId) as Session | undefined;
}

export function getSessionsByAgentGroup(agentGroupId: string): Session[] {
  return getDb().prepare('SELECT * FROM sessions WHERE agent_group_id = ?').all(agentGroupId) as Session[];
}

export function getActiveSessions(): Session[] {
  return getDb().prepare("SELECT * FROM sessions WHERE status = 'active'").all() as Session[];
}

/** All known session ids (any status) — for on-disk folder reconciliation. */
export function getAllSessionIds(): Set<string> {
  return new Set((getDb().prepare('SELECT id FROM sessions').all() as Array<{ id: string }>).map((r) => r.id));
}

export function getRunningSessions(): Session[] {
  return getDb().prepare("SELECT * FROM sessions WHERE container_status IN ('running', 'idle')").all() as Session[];
}

export function updateSession(
  id: string,
  updates: Partial<Pick<Session, 'status' | 'container_status' | 'last_active' | 'agent_provider'>>,
): void {
  const fields: string[] = [];
  const values: Record<string, unknown> = { id };

  for (const [key, value] of Object.entries(updates)) {
    if (value !== undefined) {
      fields.push(`${key} = @${key}`);
      values[key] = value;
    }
  }
  if (fields.length === 0) return;

  getDb()
    .prepare(`UPDATE sessions SET ${fields.join(', ')} WHERE id = @id`)
    .run(values);
}

export function deleteSession(id: string): void {
  getDb().prepare('DELETE FROM sessions WHERE id = ?').run(id);
}

// ── Pending Questions ──

/**
 * Insert a pending question row. Idempotent: when delivery fails and retries,
 * the second attempt calls this with the same question_id — without `OR
 * IGNORE` that would throw UNIQUE and prevent the retry from reaching the
 * actual send step. Returns true if a new row was inserted.
 */
export function createPendingQuestion(pq: PendingQuestion): boolean {
  const result = getDb()
    .prepare(
      `INSERT OR IGNORE INTO pending_questions (question_id, session_id, message_out_id, platform_id, channel_type, thread_id, title, options_json, created_at)
       VALUES (@question_id, @session_id, @message_out_id, @platform_id, @channel_type, @thread_id, @title, @options_json, @created_at)`,
    )
    .run({
      question_id: pq.question_id,
      session_id: pq.session_id,
      message_out_id: pq.message_out_id,
      platform_id: pq.platform_id,
      channel_type: pq.channel_type,
      thread_id: pq.thread_id,
      title: pq.title,
      options_json: JSON.stringify(pq.options),
      created_at: pq.created_at,
    });
  return result.changes > 0;
}

export function getPendingQuestion(questionId: string): PendingQuestion | undefined {
  const row = getDb().prepare('SELECT * FROM pending_questions WHERE question_id = ?').get(questionId) as
    | (Omit<PendingQuestion, 'options'> & { options_json: string })
    | undefined;
  if (!row) return undefined;
  const { options_json, ...rest } = row;
  return { ...rest, options: JSON.parse(options_json) };
}

export function deletePendingQuestion(questionId: string): void {
  getDb().prepare('DELETE FROM pending_questions WHERE question_id = ?').run(questionId);
}

// ── Pending Approvals ──

/**
 * Insert a pending approval row. Idempotent for the same reason as
 * createPendingQuestion: delivery retries with the same approval_id must not
 * fail on UNIQUE before the send step gets a chance to succeed.
 */
export function createPendingApproval(
  pa: Partial<PendingApproval> &
    Pick<
      PendingApproval,
      'approval_id' | 'request_id' | 'action' | 'payload' | 'created_at' | 'title' | 'options_json'
    >,
): boolean {
  const result = getDb()
    .prepare(
      `INSERT OR IGNORE INTO pending_approvals
         (approval_id, session_id, request_id, action, payload, created_at,
          agent_group_id, channel_type, platform_id, platform_message_id, expires_at, status,
          title, options_json)
       VALUES
         (@approval_id, @session_id, @request_id, @action, @payload, @created_at,
          @agent_group_id, @channel_type, @platform_id, @platform_message_id, @expires_at, @status,
          @title, @options_json)`,
    )
    .run({
      session_id: null,
      agent_group_id: null,
      channel_type: null,
      platform_id: null,
      platform_message_id: null,
      expires_at: null,
      status: 'pending',
      ...pa,
    });
  return result.changes > 0;
}

export function getPendingApproval(approvalId: string): PendingApproval | undefined {
  return getDb().prepare('SELECT * FROM pending_approvals WHERE approval_id = ?').get(approvalId) as
    | PendingApproval
    | undefined;
}

export function updatePendingApprovalStatus(approvalId: string, status: PendingApproval['status']): void {
  getDb().prepare('UPDATE pending_approvals SET status = ? WHERE approval_id = ?').run(status, approvalId);
}

/**
 * Record where an approval card was actually delivered. requestApproval()
 * creates the row before it knows the approver, then calls this once the
 * channel adapter has sent the card. Persisting the target is what lets the
 * channel adapter rehydrate its in-memory pending-question map after a
 * restart (otherwise an approval issued before a restart can never be
 * answered) and makes `approvals list` show the real destination instead of
 * NULL columns.
 */
export function updatePendingApprovalDelivery(
  approvalId: string,
  channelType: string,
  platformId: string,
  platformMessageId: string | null,
): void {
  getDb()
    .prepare(
      'UPDATE pending_approvals SET channel_type = ?, platform_id = ?, platform_message_id = ? WHERE approval_id = ?',
    )
    .run(channelType, platformId, platformMessageId, approvalId);
}

export function deletePendingApproval(approvalId: string): void {
  getDb().prepare('DELETE FROM pending_approvals WHERE approval_id = ?').run(approvalId);
}

/**
 * An open (unanswered) question/approval card that was delivered to a given
 * channel. Used by channel adapters to rehydrate their in-memory
 * "chatJid → pending card" map on startup so cards issued before a restart
 * stay answerable. `platformId` is the chat the card was delivered to.
 */
export interface OpenChannelCard {
  questionId: string;
  platformId: string;
  options: import('../channels/ask-question.js').NormalizedOption[];
  createdAt: string;
}

/**
 * All still-open cards delivered to `channelType`, oldest first. Covers every
 * table whose cards are answered via the channel adapter's slash-command path:
 *   - pending_questions        (ask_user_question)
 *   - pending_approvals        (cli_command / self-mod, status='pending')
 *   - pending_sender_approvals (unknown-sender Allow/Deny)
 *   - pending_channel_approvals(bot-mentioned-in-new-channel)
 *
 * The first two record their delivery target directly (channel_type +
 * platform_id) and are skipped if platform_id is NULL (older rows predating
 * delivery-target persistence). The latter two don't have those columns — the
 * card went to the approver's DM, whose chatJid is `approver_user_id` minus
 * its "channelType:" prefix — so the target is derived from approver_user_id.
 * questionId matches what each card was delivered with: the row id for sender
 * approvals, the messaging_group_id for channel approvals.
 *
 * Oldest-first so a caller folding these into a single-slot-per-chat map ends
 * on the most recent card, matching live delivery order.
 */
export function getOpenCardsForChannel(channelType: string): OpenChannelCard[] {
  const db = getDb();
  const out: OpenChannelCard[] = [];

  // Cards that store their delivery target explicitly.
  const direct = db
    .prepare(
      `SELECT question_id AS questionId, platform_id AS platformId, options_json AS optionsJson, created_at AS createdAt
         FROM pending_questions
        WHERE channel_type = ? AND platform_id IS NOT NULL
       UNION ALL
       SELECT approval_id AS questionId, platform_id AS platformId, options_json AS optionsJson, created_at AS createdAt
         FROM pending_approvals
        WHERE channel_type = ? AND platform_id IS NOT NULL AND status = 'pending'`,
    )
    .all(channelType, channelType) as Array<{
    questionId: string;
    platformId: string;
    optionsJson: string;
    createdAt: string;
  }>;
  for (const r of direct) {
    out.push({
      questionId: r.questionId,
      platformId: r.platformId,
      options: JSON.parse(r.optionsJson),
      createdAt: r.createdAt,
    });
  }

  // Cards whose delivery target is derived from approver_user_id. These are
  // optional-module tables, so guard with hasTable.
  const derived: Array<{ questionId: string; approver: string; optionsJson: string; createdAt: string }> = [];
  if (hasTable(db, 'pending_sender_approvals')) {
    derived.push(
      ...(db
        .prepare(
          'SELECT id AS questionId, approver_user_id AS approver, options_json AS optionsJson, created_at AS createdAt FROM pending_sender_approvals',
        )
        .all() as typeof derived),
    );
  }
  if (hasTable(db, 'pending_channel_approvals')) {
    derived.push(
      ...(db
        .prepare(
          'SELECT messaging_group_id AS questionId, approver_user_id AS approver, options_json AS optionsJson, created_at AS createdAt FROM pending_channel_approvals',
        )
        .all() as typeof derived),
    );
  }
  for (const r of derived) {
    const sep = r.approver.indexOf(':');
    if (sep < 0 || r.approver.slice(0, sep) !== channelType) continue;
    const platformId = r.approver.slice(sep + 1);
    if (!platformId) continue;
    out.push({ questionId: r.questionId, platformId, options: JSON.parse(r.optionsJson), createdAt: r.createdAt });
  }

  // Oldest-first → most recent card wins a single-slot-per-chat consumer.
  out.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
  return out;
}

export function getPendingApprovalsByAction(action: string): PendingApproval[] {
  return getDb().prepare('SELECT * FROM pending_approvals WHERE action = ?').all(action) as PendingApproval[];
}

/**
 * Resolve ask_question render metadata (title + normalized options) for any
 * card, regardless of whether it was persisted as a pending_question (generic
 * ask_user_question) or a pending_approval (self-mod / OneCLI credential).
 */
export function getAskQuestionRender(
  id: string,
): { title: string; options: import('../channels/ask-question.js').NormalizedOption[] } | undefined {
  const q = getPendingQuestion(id);
  if (q) return { title: q.title, options: q.options };
  const a = getDb().prepare('SELECT title, options_json FROM pending_approvals WHERE approval_id = ?').get(id) as
    | { title: string; options_json: string }
    | undefined;
  if (a?.title) return { title: a.title, options: JSON.parse(a.options_json) };

  // Channel-registration + unknown-sender approvals persist title/options_json
  // the same way pending_approvals does — just SELECT and return.
  if (hasTable(getDb(), 'pending_channel_approvals')) {
    const c = getDb()
      .prepare('SELECT title, options_json FROM pending_channel_approvals WHERE messaging_group_id = ?')
      .get(id) as { title: string; options_json: string } | undefined;
    if (c?.title) return { title: c.title, options: JSON.parse(c.options_json) };
  }

  if (hasTable(getDb(), 'pending_sender_approvals')) {
    const s = getDb().prepare('SELECT title, options_json FROM pending_sender_approvals WHERE id = ?').get(id) as
      | { title: string; options_json: string }
      | undefined;
    if (s?.title) return { title: s.title, options: JSON.parse(s.options_json) };
  }

  return undefined;
}
