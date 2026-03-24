"use strict";

/**
 * Vercro Push Notification System
 * ─────────────────────────────────────────────────────────────
 * Candidate generation + Web Push delivery
 * 
 * Install: npm install web-push
 * 
 * Generate VAPID keys once:
 *   node -e "const wp=require('web-push'); const k=wp.generateVAPIDKeys(); console.log(JSON.stringify(k))"
 * 
 * Add to Vercel env:
 *   VAPID_PUBLIC_KEY=...
 *   VAPID_PRIVATE_KEY=...
 *   VAPID_SUBJECT=mailto:hello@vercro.com
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

// ── Daily cap check ───────────────────────────────────────────────────────────
async function getDailyCounter(supabase, userId, dateLocal) {
  const { data } = await supabase
    .from("notification_daily_counters")
    .select("*")
    .eq("user_id", userId)
    .eq("date_local", dateLocal)
    .single();
  return data || { total_sent: 0, critical_sent: 0, high_sent: 0, medium_sent: 0, low_sent: 0 };
}

async function incrementCounter(supabase, userId, dateLocal, priority) {
  const counter = await getDailyCounter(supabase, userId, dateLocal);
  await supabase.from("notification_daily_counters").upsert({
    user_id:       userId,
    date_local:    dateLocal,
    total_sent:    (counter.total_sent    || 0) + 1,
    critical_sent: (counter.critical_sent || 0) + (priority === "critical" ? 1 : 0),
    high_sent:     (counter.high_sent     || 0) + (priority === "high"     ? 1 : 0),
    medium_sent:   (counter.medium_sent   || 0) + (priority === "medium"   ? 1 : 0),
    low_sent:      (counter.low_sent      || 0) + (priority === "low"      ? 1 : 0),
    last_sent_at:  new Date().toISOString(),
  }, { onConflict: "user_id,date_local" });
}

// ── Suppression check ─────────────────────────────────────────────────────────
async function isRecentlySent(supabase, userId, notificationType, cooldownHours) {
  const since = new Date(Date.now() - cooldownHours * 3600000).toISOString();
  const { data } = await supabase
    .from("notification_events")
    .select("id")
    .eq("user_id", userId)
    .eq("notification_type", notificationType)
    .in("status", ["sent", "queued"])
    .gte("created_at", since)
    .limit(1)
    .maybeSingle();
  return !!data;
}

// Cooldowns by type (hours)
const COOLDOWNS = {
  due_today:        20,
  weather_alert:    6,
  pest_alert:       24,
  crop_check:       48,
  upcoming:         24,
  weekly_summary:   168,
  milestone:        48,
};

// ── Candidate builder ─────────────────────────────────────────────────────────

async function buildCandidates(supabase, userId, prefs) {
  const today   = new Date().toISOString().split("T")[0];
  const weekEnd = new Date(Date.now() + 7 * 86400000).toISOString().split("T")[0];
  const candidates = [];

  // ── 1. Due today tasks ────────────────────────────────────────────────────
  if (prefs.due_today_enabled) {
    const { data: todayTasks } = await supabase
      .from("tasks")
      .select("*, crop:crop_instance_id(name, variety)")
      .eq("user_id", userId)
      .is("completed_at", null)
      .not("status", "eq", "expired")
      .lte("due_date", today)
      .order("urgency", { ascending: false })
      .limit(10);

    // Pick the single best task — highest urgency, most specific
    const URGENCY_RANK = { high: 3, medium: 2, low: 1 };
    const eligible = (todayTasks || [])
      .filter(t => t.task_type !== "check" && t.record_type !== "alert")
      .sort((a, b) => (URGENCY_RANK[b.urgency] || 0) - (URGENCY_RANK[a.urgency] || 0));

    if (eligible[0]) {
      const t = eligible[0];
      const cropName = t.crop?.name;
      const variety  = t.crop?.variety;
      const label    = [cropName, variety].filter(Boolean).join(" ");

      const titles = {
        feed:       cropName ? `🌱 Today: Feed ${cropName}` : "🌱 Today: Feed your crops",
        harvest:    cropName ? `🌱 Today: Harvest ${cropName}` : "🌱 Today: Check crops for harvest",
        sow:        cropName ? `🌱 Today: Sow ${cropName}` : "🌱 Today: Sowing task due",
        transplant: cropName ? `🌱 Today: Transplant ${cropName}` : "🌱 Today: Transplanting task due",
        protect:    cropName ? `🌱 Today: Protect ${cropName}` : "🌱 Today: Protection task due",
        harden_off: cropName ? `🌱 Today: Harden off ${cropName}` : "🌱 Today: Hardening off task due",
        prune:      cropName ? `🌱 Today: Prune ${cropName}` : "🌱 Today: Pruning task due",
        mulch:      cropName ? `🌱 Today: Mulch ${cropName}` : "🌱 Today: Mulching task due",
      };

      candidates.push({
        notification_type: "due_today",
        priority:          t.urgency === "high" ? "high" : "medium",
        title:             titles[t.task_type] || (cropName ? `🌱 Today: ${cropName} needs attention` : "🌱 Garden task due today"),
        body:              "Do this today to stay on track",
        task_id:           t.id,
        payload: {
          url:     "/?section=focus",
          section: "focus",
          task_id: t.id,
          actions: [
            { action: "complete", title: "✓ Done" },
            { action: "snooze",   title: "Later" },
          ],
        },
      });
    }
  }

  // ── 2. Weather/frost alerts ───────────────────────────────────────────────
  if (prefs.weather_alerts_enabled) {
    const { data: alerts } = await supabase
      .from("tasks")
      .select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId)
      .is("completed_at", null)
      .eq("record_type", "alert")
      .eq("engine_type", "risk")
      .not("status", "eq", "expired")
      .in("task_type", ["protect", "water"])
      .order("urgency", { ascending: false })
      .limit(3);

    const frostAlert = (alerts || []).find(a => a.rule_id === "frost_alert");
    if (frostAlert) {
      candidates.push({
        notification_type: "weather_alert",
        priority:          frostAlert.urgency === "high" ? "critical" : "high",
        title:             "❄️ Frost tonight",
        body:              "Protect your plants now",
        task_id:           frostAlert.id,
        payload: {
          url:     "/?section=alerts",
          section: "alerts",
          task_id: frostAlert.id,
        },
      });
    }
  }

  // ── 3. Pest alerts ────────────────────────────────────────────────────────
  if (prefs.pest_alerts_enabled) {
    const { data: pestAlerts } = await supabase
      .from("tasks")
      .select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId)
      .is("completed_at", null)
      .eq("record_type", "alert")
      .eq("engine_type", "risk")
      .not("status", "eq", "expired")
      .in("task_type", ["inspect_pests", "inspect_disease"])
      .in("urgency", ["high", "medium"])
      .order("urgency", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (pestAlerts) {
      const cropName = pestAlerts.crop?.name;
      candidates.push({
        notification_type: "pest_alert",
        priority:          "medium",
        title:             cropName ? `Check ${cropName} today` : "Pest check needed",
        body:              pestAlerts.action,
        task_id:           pestAlerts.id,
        payload: {
          url:     "/?section=alerts",
          section: "alerts",
          task_id: pestAlerts.id,
        },
      });
    }
  }

  // ── 4. Crop checks ────────────────────────────────────────────────────────
  if (prefs.crop_checks_enabled) {
    const { data: checkTasks } = await supabase
      .from("tasks")
      .select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId)
      .is("completed_at", null)
      .eq("task_type", "check")
      .not("status", "eq", "expired")
      .limit(1)
      .maybeSingle();

    if (checkTasks) {
      const cropName = checkTasks.crop?.name;
      candidates.push({
        notification_type: "crop_check",
        priority:          "low",
        title:             cropName ? `Quick check on your ${cropName}` : "Quick crop check",
        body:              checkTasks.action,
        task_id:           checkTasks.id,
        payload: {
          url:     "/?section=checks",
          section: "checks",
          task_id: checkTasks.id,
        },
      });
    }
  }

  // ── 5. Upcoming key task ──────────────────────────────────────────────────
  if (prefs.coming_up_enabled) {
    const tomorrow = new Date(Date.now() + 86400000).toISOString().split("T")[0];
    const in3days  = new Date(Date.now() + 3 * 86400000).toISOString().split("T")[0];

    const { data: upcomingTasks } = await supabase
      .from("tasks")
      .select("*, crop:crop_instance_id(name)")
      .eq("user_id", userId)
      .is("completed_at", null)
      .eq("status", "upcoming")
      .gte("due_date", tomorrow)
      .lte("due_date", in3days)
      .in("task_type", ["sow", "transplant", "harden_off", "harvest"])
      .order("due_date", { ascending: true })
      .limit(1)
      .maybeSingle();

    if (upcomingTasks) {
      const cropName = upcomingTasks.crop?.name;
      const daysAway = Math.ceil((new Date(upcomingTasks.due_date) - Date.now()) / 86400000);
      const when = daysAway === 1 ? "tomorrow" : `in ${daysAway} days`;
      const taskLabel = upcomingTasks.task_type.replace(/_/g, " ");
      candidates.push({
        notification_type: "upcoming",
        priority:          "medium",
        title:             cropName ? `⚠️ ${cropName} — ${taskLabel} ${when}` : `⚠️ Garden task due ${when}`,
        body:              `Miss this and your timing slips`,
        task_id:           upcomingTasks.id,
        payload: {
          url:     "/?section=upcoming",
          section: "upcoming",
          task_id: upcomingTasks.id,
        },
      });
    }
  }

  // ── 6. Weekly summary (Sunday evenings) ──────────────────────────────────
  if (prefs.weekly_summary_enabled) {
    const dayOfWeek = new Date().getDay(); // 0 = Sunday
    if (dayOfWeek === 0) {
      // Count upcoming this week
      const { count: upcomingCount } = await supabase
        .from("tasks")
        .select("*", { count: "exact", head: true })
        .eq("user_id", userId)
        .is("completed_at", null)
        .not("status", "eq", "expired")
        .lte("due_date", weekEnd);

      const { count: completedCount } = await supabase
        .from("tasks")
        .select("*", { count: "exact", head: true })
        .eq("user_id", userId)
        .not("completed_at", "is", null)
        .gte("completed_at", new Date(Date.now() - 7 * 86400000).toISOString());

      candidates.push({
        notification_type: "weekly_summary",
        priority:          "low",
        title:             "Your garden this week",
        body:              upcomingCount > 0
          ? `${upcomingCount} task${upcomingCount !== 1 ? "s" : ""} coming up${completedCount > 0 ? ` · ${completedCount} completed last week` : ""}`
          : completedCount > 0
            ? `Great week — ${completedCount} task${completedCount !== 1 ? "s" : ""} completed. Your garden is on track.`
            : "Check in on your garden this week.",
        payload: {
          url:     "/",
          section: "dashboard",
        },
      });
    }
  }

  return candidates;
}

// ── Select best candidate ─────────────────────────────────────────────────────
// Priority: critical > high > medium > low
// Apply daily caps and cooldowns

async function selectCandidates(supabase, userId, candidates, window, isAdmin = false) {
  const today   = new Date().toISOString().split("T")[0];
  const counter = await getDailyCounter(supabase, userId, today);
  const PRIORITY_RANK = { critical: 4, high: 3, medium: 2, low: 1 };

  // Daily caps — low:1 allows engagement nudges, high:2 allows stacking
  const CAPS = { total: 3, critical: 2, high: 2, medium: 1, low: 1 };

  if (counter.total_sent >= CAPS.total) return [];

  const selected = [];

  // Sort by priority
  const sorted = [...candidates].sort(
    (a, b) => (PRIORITY_RANK[b.priority] || 0) - (PRIORITY_RANK[a.priority] || 0)
  );

  for (let c of sorted) {
    // Evening window: upgrade due_today to last-chance copy
    if (window === "evening" && c.notification_type === "due_today") {
      if (counter.total_sent > 0) continue; // already sent something today
      // Rewrite to evening last-chance tone
      c = {
        ...c,
        title: c.title.replace("🌱 Today:", "⚠️ Last chance today:"),
        body:  "Takes 2 mins — keeps you on track",
      };
    }

    // Check daily cap for this priority
    const capKey = `${c.priority}_sent`;
    if ((counter[capKey] || 0) >= (CAPS[c.priority] || 0)) continue;
    if (counter.total_sent + selected.length >= CAPS.total) break;

    // Check cooldown — admin accounts bypass this so they always get notified
    if (!isAdmin) {
      const cooldownHours = COOLDOWNS[c.notification_type] || 20;
      const suppressed = await isRecentlySent(supabase, userId, c.notification_type, cooldownHours);
      if (suppressed && c.priority !== "critical") continue;
    }

    // Window routing
    if (window === "morning" && c.notification_type === "weekly_summary") continue;
    if (window === "evening" && c.notification_type === "due_today" && c.priority !== "high") continue;

    selected.push(c);
    if (c.priority !== "critical") break; // only batch criticals
  }

  return selected;
}

// ── Send notification ─────────────────────────────────────────────────────────
async function sendNotification(supabase, userId, candidate) {
  // Get active push tokens for user
  const { data: tokens } = await supabase
    .from("device_push_tokens")
    .select("push_token, endpoint")
    .eq("user_id", userId)
    .eq("is_active", true);

  if (!tokens?.length) return { sent: false, reason: "no_tokens" };

  // Log event
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
  for (const token of tokens) {
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
    } catch (err) {
      console.error(`[Push] Send failed for token:`, err.statusCode, err.body);
      // Deactivate invalid tokens
      if (err.statusCode === 410 || err.statusCode === 404) {
        await supabase.from("device_push_tokens")
          .update({ is_active: false })
          .eq("user_id", userId)
          .eq("endpoint", token.endpoint);
      }
    }
  }

  if (sentCount > 0 && eventId) {
    await supabase.from("notification_events")
      .update({ status: "sent", sent_at: new Date().toISOString() })
      .eq("id", eventId);

    const today = new Date().toISOString().split("T")[0];
    await incrementCounter(supabase, userId, today, candidate.priority);
  }

  return { sent: sentCount > 0, sentCount, eventId };
}

// ── Admin accounts — always receive notifications regardless of activity ──────
// These users bypass the last_seen_at suppression so they can verify push works
const ADMIN_USER_IDS = new Set([
  "c1c730ff-acb2-4969-9c74-32a84041d9b3", // Mark (founder)
]);

// ── Main notification runner ──────────────────────────────────────────────────
async function runNotificationsForUser(supabase, userId, window = "morning") {
  if (!setupVapid()) return { sent: 0, reason: "vapid_not_configured" };

  const isAdmin = ADMIN_USER_IDS.has(userId);

  // Get preferences
  const { data: prefs } = await supabase
    .from("notification_preferences")
    .select("*")
    .eq("user_id", userId)
    .single();

  if (!prefs?.push_enabled) return { sent: 0, reason: "push_disabled" };

  // Suppress if user already opened the app today (skip for admin accounts)
  if (!isAdmin) {
    const { data: profile } = await supabase
      .from("profiles")
      .select("last_seen_at")
      .eq("id", userId)
      .single();

    if (profile?.last_seen_at) {
      const lastSeen = new Date(profile.last_seen_at).getTime();
      const hoursSinceOpen = (Date.now() - lastSeen) / 3600000;
      // Morning: skip if opened in last 2 hours. Evening: skip if opened in last 6 hours.
      const suppressWindow = window === "evening" ? 6 : 2;
      if (hoursSinceOpen < suppressWindow) {
        return { sent: 0, reason: "recently_active" };
      }
    }
  }

  // Build candidates
  const candidates = await buildCandidates(supabase, userId, prefs);
  if (!candidates.length) return { sent: 0, reason: "no_candidates" };

  // Select best — admin accounts bypass cooldown suppression for evening window
  // so they always get both morning and evening notifications
  const selected = await selectCandidates(supabase, userId, candidates, window, isAdmin);
  if (!selected.length) return { sent: 0, reason: "all_suppressed" };

  // Send
  let totalSent = 0;
  for (const candidate of selected) {
    const result = await sendNotification(supabase, userId, candidate);
    if (result.sent) totalSent++;
  }

  return { sent: totalSent, candidates: selected.length };
}

module.exports = { runNotificationsForUser, buildCandidates, sendNotification };