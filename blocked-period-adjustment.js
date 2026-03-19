// =============================================================================
// BLOCKED DATE ADJUSTMENT SERVICE
// Post-processing layer over rule-generated tasks.
// Never mutates canonical tasks — writes TaskAdjustment overlays only.
// UI reads effective_due_date = adjustment.adjusted_due_date ?? task.due_date
// =============================================================================

// ---------------------------------------------------------------------------
// Task type policy map
// Derive movement rules at runtime from task_type — do NOT store on task rows.
// To tune tolerances: edit this map and redeploy. No schema migration needed.
// ---------------------------------------------------------------------------
const TASK_POLICIES = {
  feed: {
    moveEarlierMaxDays: 3,
    moveLaterMaxDays:   3,
    preferDirection:    "earlier",
    riskIfUnmovable:    false,
  },
  water: {
    moveEarlierMaxDays: 0,
    moveLaterMaxDays:   1,
    preferDirection:    "earlier",
    riskIfUnmovable:    true,
  },
  sow: {
    moveEarlierMaxDays: 5,
    moveLaterMaxDays:   7,
    preferDirection:    "later",
    riskIfUnmovable:    true,
  },
  transplant: {
    moveEarlierMaxDays: 2,
    moveLaterMaxDays:   3,
    preferDirection:    "later",
    riskIfUnmovable:    true,
  },
  harvest: {
    moveEarlierMaxDays: 2,
    moveLaterMaxDays:   2,
    preferDirection:    "earlier",
    riskIfUnmovable:    true,
  },
  protect: {
    moveEarlierMaxDays: 0,
    moveLaterMaxDays:   0,
    preferDirection:    "earlier",
    riskIfUnmovable:    true,
  },
  monitor: {
    moveEarlierMaxDays: 2,
    moveLaterMaxDays:   5,
    preferDirection:    "later",
    riskIfUnmovable:    false,
  },
  prune: {
    moveEarlierMaxDays: 3,
    moveLaterMaxDays:   5,
    preferDirection:    "later",
    riskIfUnmovable:    false,
  },
  thin: {
    moveEarlierMaxDays: 3,
    moveLaterMaxDays:   5,
    preferDirection:    "later",
    riskIfUnmovable:    false,
  },
  other: {
    moveEarlierMaxDays: 2,
    moveLaterMaxDays:   5,
    preferDirection:    "later",
    riskIfUnmovable:    false,
  },
};

// ---------------------------------------------------------------------------
// Date helpers — plain JS, no external deps
// ---------------------------------------------------------------------------
function addDays(dateStr, n) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + n);
  return d.toISOString().split("T")[0];
}

function diffDays(laterDateStr, earlierDateStr) {
  const a = new Date(laterDateStr);
  const b = new Date(earlierDateStr);
  return Math.round((a - b) / 86400000);
}

// ---------------------------------------------------------------------------
// Core decision logic for a single task
// ---------------------------------------------------------------------------
function evaluateTask(task, blockedPeriod, policy) {
  const dueDate   = task.due_date;
  const bStart    = blockedPeriod.start_date;
  const bEnd      = blockedPeriod.end_date;

  // Date just before blocked window starts
  const earlierCandidate = addDays(bStart, -1);
  // Date just after blocked window ends
  const laterCandidate   = addDays(bEnd, 1);

  // How many days would this move relative to original due date
  const daysMovedEarlier = diffDays(dueDate, earlierCandidate); // how far back
  const daysMovedLater   = diffDays(laterCandidate, dueDate);   // how far forward

  const earlierValid =
    policy.moveEarlierMaxDays > 0 &&
    daysMovedEarlier >= 0 &&
    daysMovedEarlier <= policy.moveEarlierMaxDays;

  const laterValid =
    policy.moveLaterMaxDays > 0 &&
    daysMovedLater >= 0 &&
    daysMovedLater <= policy.moveLaterMaxDays;

  const meta = {
    task_type:        task.task_type,
    blocked_start:    bStart,
    blocked_end:      bEnd,
    earlier_candidate: earlierCandidate,
    later_candidate:   laterCandidate,
    days_moved_earlier: daysMovedEarlier,
    days_moved_later:   daysMovedLater,
    earlier_valid:      earlierValid,
    later_valid:        laterValid,
    tolerance_used: {
      move_earlier_max_days: policy.moveEarlierMaxDays,
      move_later_max_days:   policy.moveLaterMaxDays,
    },
    decision_source: "task_type_config",
  };

  // Preferred direction first, fallback to other direction, then at_risk
  if (policy.preferDirection === "earlier" && earlierValid) {
    return { type: "moved_earlier", adjustedDueDate: earlierCandidate, metadata: meta };
  }
  if (policy.preferDirection === "later" && laterValid) {
    return { type: "moved_later", adjustedDueDate: laterCandidate, metadata: meta };
  }
  if (earlierValid) {
    return { type: "moved_earlier", adjustedDueDate: earlierCandidate, metadata: meta };
  }
  if (laterValid) {
    return { type: "moved_later", adjustedDueDate: laterCandidate, metadata: meta };
  }
  if (policy.riskIfUnmovable) {
    return { type: "at_risk", adjustedDueDate: null, metadata: { ...meta, reason: "no_safe_date" } };
  }

  return null; // no adjustment needed
}

// ---------------------------------------------------------------------------
// Apply adjustments for one blocked period
// ---------------------------------------------------------------------------
async function applyBlockedPeriodAdjustments(db, userId, blockedPeriodId) {
  // Fetch the blocked period
  const { data: bp, error: bpErr } = await db
    .from("blocked_periods")
    .select("*")
    .eq("id", blockedPeriodId)
    .eq("user_id", userId)
    .eq("status", "active")
    .single();

  if (bpErr || !bp) {
    console.error("[TimeAway] Blocked period not found:", blockedPeriodId);
    return { movedEarlier: 0, movedLater: 0, atRisk: 0, total: 0 };
  }

  // Delete any prior adjustments for this blocked period (clean slate)
  await db
    .from("task_adjustments")
    .delete()
    .eq("blocked_period_id", blockedPeriodId)
    .eq("user_id", userId);

  // Fetch active incomplete tasks falling within the blocked period
  const { data: tasks } = await db
    .from("tasks")
    .select("id, task_type, due_date, crop_instance_id, area_id")
    .eq("user_id", userId)
    .is("completed_at", null)
    .gte("due_date", bp.start_date)
    .lte("due_date", bp.end_date);

  if (!tasks?.length) {
    return { movedEarlier: 0, movedLater: 0, atRisk: 0, total: 0 };
  }

  const toInsert = [];
  let movedEarlier = 0, movedLater = 0, atRisk = 0;

  for (const task of tasks) {
    const policy = TASK_POLICIES[task.task_type];

    if (!policy) {
      // Unknown task type — mark at risk conservatively
      toInsert.push({
        user_id:           userId,
        task_id:           task.id,
        blocked_period_id: blockedPeriodId,
        adjustment_reason: "blocked_dates",
        adjustment_type:   "at_risk",
        original_due_date: task.due_date,
        adjusted_due_date: null,
        metadata:          { reason: "no_policy_for_task_type", task_type: task.task_type },
      });
      atRisk++;
      continue;
    }

    const decision = evaluateTask(task, bp, policy);
    if (!decision) continue; // no adjustment needed (riskIfUnmovable: false and no valid moves)

    toInsert.push({
      user_id:           userId,
      task_id:           task.id,
      blocked_period_id: blockedPeriodId,
      adjustment_reason: "blocked_dates",
      adjustment_type:   decision.type,
      original_due_date: task.due_date,
      adjusted_due_date: decision.adjustedDueDate,
      metadata:          decision.metadata,
    });

    if (decision.type === "moved_earlier") movedEarlier++;
    else if (decision.type === "moved_later") movedLater++;
    else if (decision.type === "at_risk")    atRisk++;
  }

  if (toInsert.length > 0) {
    await db.from("task_adjustments").insert(toInsert);
  }

  console.log(`[TimeAway] ${bp.label || blockedPeriodId}: ${movedEarlier} earlier, ${movedLater} later, ${atRisk} at risk`);

  return {
    movedEarlier,
    movedLater,
    atRisk,
    total: toInsert.length,
  };
}

// ---------------------------------------------------------------------------
// Re-apply ALL active blocked periods for a user
// Call this immediately after any task regeneration
// ---------------------------------------------------------------------------
async function reapplyAllBlockedPeriods(db, userId) {
  const { data: periods } = await db
    .from("blocked_periods")
    .select("id")
    .eq("user_id", userId)
    .eq("status", "active")
    .order("start_date", { ascending: true });

  if (!periods?.length) return;

  // Clear all existing blocked-date adjustments for this user first
  await db
    .from("task_adjustments")
    .delete()
    .eq("user_id", userId)
    .eq("adjustment_reason", "blocked_dates");

  // Re-apply each period in date order
  for (const p of periods) {
    await applyBlockedPeriodAdjustments(db, userId, p.id);
  }
}

module.exports = { applyBlockedPeriodAdjustments, reapplyAllBlockedPeriods, TASK_POLICIES };
