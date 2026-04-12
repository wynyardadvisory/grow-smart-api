"use strict";

/**
 * Vercro Push Notification System
 * ─────────────────────────────────────────────────────────────
 * Reliable habit loop — every eligible user gets exactly 1 morning
 * and 1 evening push per day, regardless of recent app activity.
 *
 * Design principles:
 * - Never return no candidate for a valid push-enabled user
 * - Morning send never blocks evening send
 * - Activity suppression removed — engaged users still get pushes
 * - Cooldowns removed from scheduled sends
 * - Guaranteed fallback if no real task/alert exists
 *
 * Performance architecture (bulk send path):
 * - buildEligibleUserSet() in api.js does 3 bulk queries upfront
 * - buildCandidatesFromCache() uses pre-fetched task data — zero DB calls per user
 * - runNotificationsForUser() still works for single-user /notifications/test
 */

const webpush = require("web-push");

// ── VAPID setup ───────────────────────────────────────────────────────────────
function setupVapid() {
  if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY) {
    console.warn("[Push] VAPID keys not configured — push disabled");
    return false;
  }
  webpush.setVapidDetails(
    process.env.VAPID_SUBJECT || "mailto:hello@vercro.com",
    process.env.VAPID_PUBLIC_KEY,
    process.env.VAPID_PRIVATE_KEY
  );
  return true;
}

// ── Admin accounts — always receive notifications ────────────────────────────
const ADMIN_USER_IDS = new Set([
  "c1c730ff-acb2-4969-9c74-32a84041d9b3",
]);

// ── Window tracking — prevent duplicate sends within same window ─────────────
async function didSendInWindow(supabase, userId, window) {
  const windowStart = getWindowStart(window);
  const { data } = await supabase
    .from("notification_events")
    .select("id")
    .eq("user_id", userId)
    .in("status", ["sent", "queued"])
    .gte("created_at", windowStart)
    .limit(1)
    .maybeSingle();
  return !!data;
}

function getWindowStart(window) {
  const now = new Date();
  const today = now.toISOString().split("T")[0];
  return window === "morning"
    ? `${today}T05:00:00.000Z`
    : `${today}T15:00:00.000Z`;
}

// ── Fallback candidate pools ──────────────────────────────────────────────────
const MORNING_FALLBACKS = [
  { title: "Good morning — check your garden today 🌱",    body: "A quick look now keeps your crops on track." },
  { title: "Step outside and see what's changed 🌿",       body: "Plants move fast — 30 seconds is all it takes." },
  { title: "Quick crop check this morning",                 body: "Notice anything new? Log it while it's fresh." },
  { title: "Start your day with a garden check 🌱",        body: "Small daily checks prevent big problems later." },
  { title: "Your garden is waiting ☀️",                    body: "A quick check now can save a crop later." },
];

const EVENING_FALLBACKS = [
  { title: "Evening check — how's the garden looking? 🌿", body: "A quiet moment with your crops before dark." },
  { title: "Keep your streak going 🌱",                    body: "Don't break the habit — one quick check tonight." },
  { title: "Anything changed today? Log it in Vercro",     body: "A quick note now helps your future self." },
  { title: "End of day garden check",                      body: "Stay in the rhythm — your plants change fast." },
  { title: "One small check tonight 🌿",                   body: "Just 30 seconds with your crops makes a difference." },
];

function getFallbackCandidate(window, userId) {
  const pool = window === "morning" ? MORNING_FALLBACKS : EVENING_FALLBACKS;
  const idx = userId.charCodeAt(0) % pool.length;
  const fb = pool[idx];
  return {
    notification_type: "habit_nudge",
    priority:          "low",
    title:             fb.title,
    body:              fb.body,
    task_id:           null,
    payload: { url: "/", section: "dashboard" },
  };
}

// ── BULK candidate builder — zero DB calls ────────────────────────────────────
// Used by the scheduled cron path. All task data is pre-fetched in bulk
// by buildEligibleUserSet() in api.js and passed in as tasksByUser.
// tasksByUser = Map<userId, task[]>
function buildCandidatesFromCache(userId, window, tasksByUser) {
  const today    = new Date().toISOString().split("T")[0];
  const tomorrow = new Date(Date.now() + 86400000).toISOString().split("T")[0];
  const in3days  = new Date(Date.now() + 3 * 86400000).toISOString().split("T")[0];
  const tasks    = tasksByUser.get(userId) || [];
  const candidates = [];

  const URGENCY_RANK = { high: 3, medium: 2, low: 1 };

  if (window === "morning") {
    // Priority 1: frost alert
    const frost = tasks.find(t =>
      t.record_type === "alert" && t.rule_id === "frost_alert" &&
      !t.completed_at && t.status !== "expired"
    );
    if (frost) {
      candidates.push({
        notification_type: "weather_alert",
        priority:          "critical",
        title:             "❄️ Frost tonight — protect tender plants",
        body:              "Check your crops now and cover anything frost-sensitive.",
        task_id:           frost.id,
        payload:           { url: "/?section=alerts", section: "alerts", task_id: frost.id },
      });
    }

    // Priority 2: high urgency alert
    if (!candidates.length) {
      const pest = tasks.find(t =>
        t.record_type === "alert" && t.urgency === "high" &&
        !t.completed_at && t.status !== "expired"
      );
      if (pest) {
        const cropName = pest.crop?.name;
        candidates.push({
          notification_type: "pest_alert",
          priority:          "high",
          title:             cropName ? `⚠️ Check ${cropName} today` : "⚠️ Garden alert needs attention",
          body:              pest.action || "Check your crops for signs of pest or disease.",
          task_id:           pest.id,
          payload:           { url: "/?section=alerts", section: "alerts", task_id: pest.id },
        });
      }
    }

    // Priority 3: task due today — exclude insights (informational only, not push-worthy)
    if (!candidates.length) {
      const dueTasks = tasks
        .filter(t =>
          !t.completed_at && t.status !== "expired" &&
          t.due_date <= today &&
          t.task_type !== "check" && t.record_type !== "alert" &&
          t.surface_class !== "insight"
        )
        .sort((a, b) => (URGENCY_RANK[b.urgency] || 0) - (URGENCY_RANK[a.urgency] || 0));

      if (dueTasks[0]) {
        const t = dueTasks[0];
        const cropName = t.crop?.name;
        const TITLES = {
          feed:       cropName ? `🌱 Today: Feed your ${cropName}` : "🌱 Today: Feeding task due",
          harvest:    cropName ? `🌱 Today: Harvest ${cropName}` : "🌱 Today: Harvest ready",
          sow:        cropName ? `🌱 Today: Sow ${cropName}` : "🌱 Today: Sowing task due",
          transplant: cropName ? `🌱 Today: Transplant ${cropName}` : "🌱 Today: Transplanting due",
          protect:    cropName ? `🌱 Today: Protect ${cropName}` : "🌱 Today: Protection needed",
          water:      cropName ? `🌱 Today: Water ${cropName}` : "🌱 Today: Watering due",
          prune:      cropName ? `🌱 Today: Prune ${cropName}` : "🌱 Today: Pruning due",
          mulch:      cropName ? `🌱 Today: Mulch ${cropName}` : "🌱 Today: Mulching due",
        };
        candidates.push({
          notification_type: "due_today",
          priority:          t.urgency === "high" ? "high" : "medium",
          title:             TITLES[t.task_type] || (cropName ? `🌱 Today: ${cropName} needs attention` : "🌱 Garden task due today"),
          body:              "Do this today to stay on track.",
          task_id:           t.id,
          payload:           { url: "/?section=focus", section: "focus", task_id: t.id },
        });
      }
    }

    // Priority 4: upcoming task in next 3 days
    if (!candidates.length) {
      const upcoming = tasks
        .filter(t =>
          !t.completed_at && t.status === "upcoming" &&
          t.due_date >= tomorrow && t.due_date <= in3days &&
          ["sow", "transplant", "harvest", "harden_off"].includes(t.task_type)
        )
        .sort((a, b) => a.due_date.localeCompare(b.due_date))[0];

      if (upcoming) {
        const cropName = upcoming.crop?.name;
        const daysAway = Math.ceil((new Date(upcoming.due_date) - Date.now()) / 86400000);
        const when = daysAway === 1 ? "tomorrow" : `in ${daysAway} days`;
        const label = upcoming.task_type.replace(/_/g, " ");
        candidates.push({
          notification_type: "upcoming",
          priority:          "medium",
          title:             cropName ? `⚠️ ${cropName} — ${label} ${when}` : `⚠️ Garden task due ${when}`,
          body:              "Miss this and your timing slips.",
          task_id:           upcoming.id,
          payload:           { url: "/?section=upcoming", section: "upcoming", task_id: upcoming.id },
        });
      }
    }

  } else {
    // Evening priority 1: missed due-today task — exclude insights
    const missed = tasks
      .filter(t =>
        !t.completed_at && t.status !== "expired" &&
        t.due_date <= today &&
        ["high", "medium"].includes(t.urgency) &&
        t.surface_class !== "insight"
      )
      .sort((a, b) => (URGENCY_RANK[b.urgency] || 0) - (URGENCY_RANK[a.urgency] || 0))[0];

    if (missed) {
      const cropName = missed.crop?.name;
      candidates.push({
        notification_type: "due_today",
        priority:          "medium",
        title:             cropName ? `⚠️ Last chance today: ${cropName}` : "⚠️ Last chance — garden task still due",
        body:              "Takes 2 mins — keeps you on track.",
        task_id:           missed.id,
        payload:           { url: "/?section=focus", section: "focus", task_id: missed.id },
      });
    }

    // Evening priority 2: fallback nudge
    if (!candidates.length) {
      candidates.push({
        notification_type: "habit_nudge",
        priority:          "low",
        title:             "Evening garden check 🌿",
        body:              "Anything changed today? Log a quick update.",
        task_id:           null,
        payload:           { url: "/", section: "dashboard" },
      });
    }
  }

  // Guaranteed fallback
  if (!candidates.length) {
    candidates.push(getFallbackCandidate(window, userId));
  }

  return candidates;
}

// ── PER-USER candidate builder — used by /notifications/test only ─────────────
// Makes individual DB queries. Fine for single-user test endpoint, not for bulk.
async function buildCandidates(supabase, userId, window) {
  const today    = new Date().toISOString().split("T")[0];
  const tomorrow = new Date(Date.now() + 86400000).toISOString().split("T")[0];
  const in3days  = new Date(Date.now() + 3 * 86400000).toISOString().split("T")[0];
  const candidates = [];

  if (window === "morning") {
    const { data: frostTask } = await supabase
      .from("tasks").select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId).is("completed_at", null)
      .eq("record_type", "alert").eq("rule_id", "frost_alert")
      .not("status", "eq", "expired").limit(1).maybeSingle();
    if (frostTask) {
      candidates.push({ notification_type: "weather_alert", priority: "critical",
        title: "❄️ Frost tonight — protect tender plants",
        body: "Check your crops now and cover anything frost-sensitive.",
        task_id: frostTask.id, payload: { url: "/?section=alerts", section: "alerts", task_id: frostTask.id } });
    }
    if (!candidates.length) {
      const { data: pestAlert } = await supabase
        .from("tasks").select("*, crop:crop_instance_id(name)")
        .eq("user_id", userId).is("completed_at", null)
        .eq("record_type", "alert").eq("urgency", "high")
        .not("status", "eq", "expired").limit(1).maybeSingle();
      if (pestAlert) {
        const cropName = pestAlert.crop?.name;
        candidates.push({ notification_type: "pest_alert", priority: "high",
          title: cropName ? `⚠️ Check ${cropName} today` : "⚠️ Garden alert needs attention",
          body: pestAlert.action || "Check your crops for signs of pest or disease.",
          task_id: pestAlert.id, payload: { url: "/?section=alerts", section: "alerts", task_id: pestAlert.id } });
      }
    }
    if (!candidates.length) {
      const URGENCY_RANK = { high: 3, medium: 2, low: 1 };
      const { data: todayTasks } = await supabase
        .from("tasks").select("*, crop:crop_instance_id(name, variety)")
        .eq("user_id", userId).is("completed_at", null)
        .not("status", "eq", "expired").lte("due_date", today)
        .order("urgency", { ascending: false }).limit(10);
      const eligible = (todayTasks || [])
        .filter(t => t.task_type !== "check" && t.record_type !== "alert")
        .sort((a, b) => (URGENCY_RANK[b.urgency] || 0) - (URGENCY_RANK[a.urgency] || 0));
      if (eligible[0]) {
        const t = eligible[0]; const cropName = t.crop?.name;
        const TITLES = { feed: cropName ? `🌱 Today: Feed your ${cropName}` : "🌱 Today: Feeding task due",
          harvest: cropName ? `🌱 Today: Harvest ${cropName}` : "🌱 Today: Harvest ready",
          sow: cropName ? `🌱 Today: Sow ${cropName}` : "🌱 Today: Sowing task due",
          transplant: cropName ? `🌱 Today: Transplant ${cropName}` : "🌱 Today: Transplanting due",
          protect: cropName ? `🌱 Today: Protect ${cropName}` : "🌱 Today: Protection needed",
          water: cropName ? `🌱 Today: Water ${cropName}` : "🌱 Today: Watering due",
          prune: cropName ? `🌱 Today: Prune ${cropName}` : "🌱 Today: Pruning due",
          mulch: cropName ? `🌱 Today: Mulch ${cropName}` : "🌱 Today: Mulching due" };
        candidates.push({ notification_type: "due_today",
          priority: t.urgency === "high" ? "high" : "medium",
          title: TITLES[t.task_type] || (cropName ? `🌱 Today: ${cropName} needs attention` : "🌱 Garden task due today"),
          body: "Do this today to stay on track.",
          task_id: t.id, payload: { url: "/?section=focus", section: "focus", task_id: t.id } });
      }
    }
    if (!candidates.length) {
      const { data: upcoming } = await supabase
        .from("tasks").select("*, crop:crop_instance_id(name)")
        .eq("user_id", userId).is("completed_at", null).eq("status", "upcoming")
        .gte("due_date", tomorrow).lte("due_date", in3days)
        .in("task_type", ["sow", "transplant", "harvest", "harden_off"])
        .order("due_date", { ascending: true }).limit(1).maybeSingle();
      if (upcoming) {
        const cropName = upcoming.crop?.name;
        const daysAway = Math.ceil((new Date(upcoming.due_date) - Date.now()) / 86400000);
        const when = daysAway === 1 ? "tomorrow" : `in ${daysAway} days`;
        candidates.push({ notification_type: "upcoming", priority: "medium",
          title: cropName ? `⚠️ ${cropName} — ${upcoming.task_type.replace(/_/g, " ")} ${when}` : `⚠️ Garden task due ${when}`,
          body: "Miss this and your timing slips.",
          task_id: upcoming.id, payload: { url: "/?section=upcoming", section: "upcoming", task_id: upcoming.id } });
      }
    }
  } else {
    const { data: missedTasks } = await supabase
      .from("tasks").select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId).is("completed_at", null)
      .not("status", "eq", "expired").lte("due_date", today)
      .in("urgency", ["high", "medium"]).order("urgency", { ascending: false }).limit(1).maybeSingle();
    if (missedTasks) {
      const cropName = missedTasks.crop?.name;
      candidates.push({ notification_type: "due_today", priority: "medium",
        title: cropName ? `⚠️ Last chance today: ${cropName}` : "⚠️ Last chance — garden task still due",
        body: "Takes 2 mins — keeps you on track.",
        task_id: missedTasks.id, payload: { url: "/?section=focus", section: "focus", task_id: missedTasks.id } });
    }
    if (!candidates.length) {
      candidates.push({ notification_type: "habit_nudge", priority: "low",
        title: "Evening garden check 🌿", body: "Anything changed today? Log a quick update.",
        task_id: null, payload: { url: "/", section: "dashboard" } });
    }
  }

  if (!candidates.length) candidates.push(getFallbackCandidate(window, userId));
  return candidates;
}

// ── Select best candidate ─────────────────────────────────────────────────────
function selectCandidate(candidates) {
  const PRIORITY_RANK = { critical: 4, high: 3, medium: 2, low: 1 };
  return [...candidates].sort(
    (a, b) => (PRIORITY_RANK[b.priority] || 0) - (PRIORITY_RANK[a.priority] || 0)
  )[0] || null;
}

// ── OneSignal send helper — native iOS/Android only ───────────────────────────
async function sendViaOneSignal(subscriptionIds, candidate) {
  const res = await fetch("https://api.onesignal.com/notifications", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Key ${process.env.ONESIGNAL_API_KEY}`,
    },
    body: JSON.stringify({
      app_id:                  process.env.ONESIGNAL_APP_ID,
      include_subscription_ids: subscriptionIds,
      headings:                { en: candidate.title },
      contents:                { en: candidate.body },
      data: {
        notification_type: candidate.notification_type,
        priority:          candidate.priority,
        ...candidate.payload,
      },
    }),
  });
  const data = await res.json();
  if (data.errors?.length) {
    console.warn("[Push] OneSignal errors:", JSON.stringify(data.errors));
  }
  return data;
}

// ── Send notification ─────────────────────────────────────────────────────────
async function sendNotification(supabase, userId, candidate, preloadedTokens) {
  // preloadedTokens: token array passed in from bulk path — avoids a DB query
  let tokens;
  if (preloadedTokens) {
    tokens = preloadedTokens;
  } else {
    const { data } = await supabase
      .from("device_push_tokens")
      .select("push_token, endpoint, platform, onesignal_subscription_id")
      .eq("user_id", userId)
      .eq("is_active", true);
    tokens = data;
  }

  if (!tokens?.length) return { sent: false, reason: "no_tokens" };

  const { data: event } = await supabase.from("notification_events").insert({
    user_id:           userId,
    task_id:           candidate.task_id || null,
    notification_type: candidate.notification_type,
    priority:          candidate.priority,
    title:             candidate.title,
    body:              candidate.body,
    payload_json:      candidate.payload || {},
    scheduled_send_at: new Date().toISOString(),
    status:            "queued",
  }).select("id").single();

  const eventId = event?.id;
  let sentCount = 0;

  // Split tokens: native (OneSignal) vs web (VAPID)
  const nativeTokens = tokens.filter(t => t.platform === "ios" || t.platform === "android");
  const webTokens    = tokens.filter(t => t.platform === "web");

  // ── Native send via OneSignal ─────────────────────────────────────────────
  const osSubscriptionIds = nativeTokens
    .map(t => t.onesignal_subscription_id)
    .filter(Boolean);

  if (osSubscriptionIds.length) {
    try {
      const osResult = await sendViaOneSignal(osSubscriptionIds, candidate);
      const osSent = osResult.recipients || 0;
      sentCount += osSent;
      console.log(`[Push] OneSignal sent to ${userId}: ${candidate.notification_type} — "${candidate.title}" — recipients=${osSent}`);
    } catch (err) {
      console.error(`[Push] OneSignal send failed for ${userId}:`, err.message);
    }
  } else if (nativeTokens.length) {
    // Native tokens exist but no OneSignal subscription ID — log for investigation
    console.warn(`[Push] ${nativeTokens.length} native token(s) for ${userId} have no OneSignal subscription ID — skipping`);
  }

  // ── Web send via VAPID ────────────────────────────────────────────────────
  for (const token of webTokens) {
    try {
      const subscription = JSON.parse(token.push_token);
      const payload = JSON.stringify({
        title:             candidate.title,
        body:              candidate.body,
        notification_type: candidate.notification_type,
        priority:          candidate.priority,
        tag:               candidate.notification_type,
        event_id:          eventId,
        ...candidate.payload,
      });
      await webpush.sendNotification(subscription, payload);
      sentCount++;
      console.log(`[Push] Web sent to ${userId}: ${candidate.notification_type} — "${candidate.title}"`);
    } catch (err) {
      console.error(`[Push] Web send failed for ${userId}:`, err.statusCode, err.body);
      if (err.statusCode === 410 || err.statusCode === 404) {
        await supabase.from("device_push_tokens")
          .update({ is_active: false })
          .eq("user_id", userId).eq("endpoint", token.endpoint);
        console.log(`[Push] Deactivated expired web token for ${userId}`);
      }
    }
  }

  if (eventId) {
    await supabase.from("notification_events")
      .update({ status: sentCount > 0 ? "sent" : "failed", sent_at: sentCount > 0 ? new Date().toISOString() : null })
      .eq("id", eventId);
  }

  return { sent: sentCount > 0, sentCount, eventId };
}

// ── BULK send — used by cron handlers in api.js ───────────────────────────────
// Accepts pre-fetched data — no DB calls except the actual send + event insert.
async function sendBulkNotifications(supabase, eligible, window, tokenMap, tasksByUser) {
  if (!setupVapid()) return { sent: 0, reason: "vapid_not_configured" };
  const counts = { sent: 0, failed: 0, no_candidate: 0 };
  for (const userId of eligible) {
    try {
      const candidates = buildCandidatesFromCache(userId, window, tasksByUser);
      const candidate  = selectCandidate(candidates);
      if (!candidate) { counts.no_candidate++; continue; }
      const tokens = tokenMap[userId] || [];
      const result = await sendNotification(supabase, userId, candidate, tokens);
      if (result.sent) counts.sent++;
      else counts.failed++;
    } catch(e) {
      console.error(`[Push] Bulk send error for ${userId}:`, e.message);
      counts.failed++;
    }
  }
  return counts;
}

// ── Single-user runner — used by /notifications/test only ─────────────────────
async function runNotificationsForUser(supabase, userId, window = "morning") {
  if (!setupVapid()) return { sent: 0, reason: "vapid_not_configured" };

  const isAdmin = ADMIN_USER_IDS.has(userId);

  if (!isAdmin) {
    const { data: prefs } = await supabase
      .from("notification_preferences").select("push_enabled")
      .eq("user_id", userId).single();
    if (!prefs?.push_enabled) return { sent: 0, reason: "push_disabled" };
  }

  const { data: tokens } = await supabase
    .from("device_push_tokens").select("id")
    .eq("user_id", userId).eq("is_active", true).limit(1);
  if (!tokens?.length) return { sent: 0, reason: "no_valid_token" };

  if (!isAdmin) {
    const alreadySent = await didSendInWindow(supabase, userId, window);
    if (alreadySent) return { sent: 0, reason: `already_sent_in_${window}_window` };
  }

  const candidates = await buildCandidates(supabase, userId, window);
  const candidate  = selectCandidate(candidates);
  if (!candidate) {
    console.log(`[Push] Skipping ${userId}: no_candidate (unexpected)`);
    return { sent: 0, reason: "no_candidate" };
  }

  const result = await sendNotification(supabase, userId, candidate);
  if (isAdmin) {
    console.log(`[Push] Admin send (${window}): ${candidate.notification_type} — "${candidate.title}" — sent=${result.sent}`);
  }
  return { sent: result.sent ? 1 : 0, type: candidate.notification_type, ...result };
}

module.exports = { runNotificationsForUser, sendBulkNotifications, buildCandidates, sendNotification };