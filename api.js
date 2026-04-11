"use strict";

/**
 * GROW SMART — API
 * ─────────────────────────────────────────────────────────────
 * Requires Node >= 18 (native fetch).
 * Deployment: Vercel + Supabase.
 * Scheduling: Vercel Cron calls POST /cron/daily at 06:00 UTC.
 * No in-process scheduler.
 *
 * Install:
 *   npm install express @supabase/supabase-js
 *               express-validator cors dotenv helmet morgan @sentry/node
 *
 * .env:
 *   SUPABASE_URL=
 *   SUPABASE_SERVICE_KEY=
 *   SUPABASE_ANON_KEY=
 *   OPENWEATHER_API_KEY=
 *   FRONTEND_URL=
 *   CRON_SECRET=
 *   SENTRY_DSN=
 *   PORT=3001
 */

const express    = require("express");
const cors       = require("cors");
const helmet     = require("helmet");
const morgan     = require("morgan");
const { createClient } = require("@supabase/supabase-js");
const { body, validationResult } = require("express-validator");
const Sentry     = require("@sentry/node");
require("dotenv").config();

// ── Sentry — initialise before anything else ──────────────────────────────────
Sentry.init({
  dsn: process.env.SENTRY_DSN,
  environment: process.env.VERCEL_ENV || "development",
  tracesSampleRate: 0.1, // 10% of requests — plenty for free tier
});

// ── Sentry helper — use instead of console.error for caught exceptions ────────
function captureError(context, err, extra = {}) {
  console.error(`[${context}]`, err.message);
  Sentry.captureException(err, { tags: { context }, extra });
}

const { RuleEngine } = require("./rule-engine");
const Stripe = require("stripe");
const stripe = Stripe(process.env.STRIPE_SECRET_KEY);
const { applyBlockedPeriodAdjustments, reapplyAllBlockedPeriods } = require("./blocked-period-adjustment");
const { runNotificationsForUser, sendBulkNotifications } = require("./notifications");
const { runNudgeUnactivated, runNudgeUnconfirmed, runFeedbackSequence, runWaitlistInvites, runWaitlistNudges, runWaitlistNudges2, runWaitlistNudges3, runReengagement, runWeeklyEmailDigest } = require("./emails");

// ── Supabase (service role — server only) ─────────────────────────────────────
const supabaseService = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ── Paginated auth user fetch — bypasses Supabase 1000-user hard cap ─────────
async function getAllAuthUsers() {
  let allUsers = [];
  let page = 1;
  while (true) {
    const { data: { users }, error } = await supabaseService.auth.admin.listUsers({ perPage: 1000, page });
    if (error || !users || users.length === 0) break;
    allUsers = allUsers.concat(users);
    if (users.length < 1000) break;
    page++;
  }
  return allUsers;
}

// ── App ───────────────────────────────────────────────────────────────────────
const app = express();
app.disable("etag"); // Prevent 304 caching — responses must always be fresh
app.use(helmet());
const allowedOrigins = [
  "https://vercro.com",
  "https://www.vercro.com",
  "https://app.vercro.com",
  "https://grow-smart-frontend.vercel.app",
  "https://grow-smart-frontend-staging.vercel.app",
  "capacitor://localhost",   // iOS Capacitor WebView
  "http://localhost",        // Android Capacitor WebView
  process.env.FRONTEND_URL,
].filter(Boolean);
app.use(cors({ origin: (origin, cb) => {
  const allowed = !origin
    || allowedOrigins.includes(origin)
    || /^https:\/\/grow-smart-frontend-staging.*\.vercel\.app$/.test(origin)
    || /^https:\/\/grow-smart-frontend.*wynyardadvisorys-projects\.vercel\.app$/.test(origin);
  cb(null, allowed);
}}));
app.use(express.json({ limit: "10mb" }));
app.use(morgan("dev"));

// ── Helpers ───────────────────────────────────────────────────────────────────
function validate(req, res) {
  const errors = validationResult(req);
  if (!errors.isEmpty()) { res.status(400).json({ errors: errors.array() }); return false; }
  return true;
}

function todayISO() {
  return new Date().toISOString().split("T")[0];
}

function weekEndISO() {
  return new Date(Date.now() + 7 * 86400000).toISOString().split("T")[0];
}

// ── Auth middleware ───────────────────────────────────────────────────────────
async function requireAuth(req, res, next) {
  const header = req.headers.authorization;
  if (!header?.startsWith("Bearer ")) return res.status(401).json({ error: "Missing auth token" });
  const { data: { user }, error } = await supabaseService.auth.getUser(header.split(" ")[1]);
  if (error || !user) return res.status(401).json({ error: "Invalid or expired token" });
  req.user = user;
  req.db = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_ANON_KEY,
    { global: { headers: { Authorization: header } } }
  );
  next();
}

// ── Rule engine runner ────────────────────────────────────────────────────────
// Always re-applies blocked period adjustments after task generation.
// This ensures time-away overlays survive any task reset or regeneration.
async function runRuleEngine(userId) {
  try {
    const engine = new RuleEngine(supabaseService);
    const tasks  = await engine.runForUser(userId);
    console.log(`[RuleEngine] ${tasks.length} tasks generated for ${userId}`);
    // Re-apply any active blocked periods so adjustments are never lost on regeneration
    await reapplyAllBlockedPeriods(supabaseService, userId);
    return tasks;
  } catch (err) {
    captureError("RuleEngine", err, { userId });
    return [];
  }
}

// =============================================================================
// ADMIN — email + middleware (defined early so all routes can use requireAdmin)
// =============================================================================
const ADMIN_EMAIL = "mark@wynyardadvisory.co.uk";
const PARTNER_ADMIN_IDS = [
  "bf938854-8c62-482c-a657-d149ff39c229",
];
async function requireAdmin(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Unauthorised" });
  if (req.user.email !== ADMIN_EMAIL) return res.status(403).json({ error: "Forbidden" });
  next();
}

// Partner admins can access metrics endpoints but not user/feedback/queue data
async function requireMetricsAccess(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Unauthorised" });
  if (req.user.email !== ADMIN_EMAIL && !PARTNER_ADMIN_IDS.includes(req.user.id)) {
    return res.status(403).json({ error: "Forbidden" });
  }
  next();
}

// =============================================================================
// HEALTH
// =============================================================================
app.get("/health", (_req, res) => res.json({ status: "ok", ts: new Date().toISOString() }));

// ── Timeline builder ──────────────────────────────────────────────────────────
function buildTimeline(crop) {
  const def     = crop.crop_def || {};
  const variety = crop.variety  || {};

  const rawSowDate = crop.sown_date || crop.transplanted_date || null;
  const dtm        = variety.days_to_maturity_max || variety.days_to_maturity_min
                   || def.days_to_maturity_max    || def.days_to_maturity_min || null;

  if (!rawSowDate) return null;

  const addDays = (d, n) => {
    const dt = new Date(d);
    dt.setDate(dt.getDate() + n);
    return dt.toISOString().split("T")[0];
  };
  const fmt = (d) => d ? new Date(d).toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric" }) : null;
  const today = new Date().toISOString().split("T")[0];

  // Apply timeline_offset_days (positive = behind schedule = harvest later)
  const offsetDays = crop.timeline_offset_days || 0;
  const sowDate    = offsetDays !== 0 ? addDays(rawSowDate, offsetDays) : rawSowDate;

  const daysSown = Math.floor((Date.now() - new Date(sowDate).getTime()) / 86400000);

  // Stage DTM percentages
  const STAGE_PCT = {
    seed:       0,
    seedling:   0.08,
    vegetative: 0.25,
    flowering:  0.55,
    fruiting:   0.70,
    harvesting: 0.90,
  };

  // Build predicted dates from sow date + DTM
  const stageDates = {};
  if (dtm) {
    for (const [stage, pct] of Object.entries(STAGE_PCT)) {
      stageDates[stage] = addDays(sowDate, Math.round(dtm * pct));
    }
  }

  // Infer current stage from days grown
  let currentStage = "seed";
  if (dtm) {
    const pct = daysSown / dtm;
    if      (pct >= 0.90) currentStage = "harvesting";
    else if (pct >= 0.70) currentStage = "fruiting";
    else if (pct >= 0.55) currentStage = "flowering";
    else if (pct >= 0.25) currentStage = "vegetative";
    else if (pct >= 0.08) currentStage = "seedling";
    else                  currentStage = "seed";
  }

  // Override with confirmed stage from crop record
  if (crop.stage && crop.stage !== "seed") currentStage = crop.stage;

  const STAGE_ORDER = ["seed", "seedling", "vegetative", "flowering", "fruiting", "harvesting"];
  const currentIdx  = STAGE_ORDER.indexOf(currentStage);

  // Harvest date calculation
  // Priority: DTM (sow date + days to maturity) — most accurate for this plant
  // Fallback: fixed calendar month from crop_definitions — only when no DTM
  // Safety: harvest date must always be after sow date
  const harvestStart = def.harvest_month_start;
  const harvestEnd   = def.harvest_month_end;
  const sowDateObj   = new Date(sowDate);
  let harvestDate    = dtm ? addDays(sowDate, dtm) : null;

  // Only use fixed month if there is no DTM to calculate from
  if (!harvestDate && harvestStart && offsetDays === 0) {
    let year = sowDateObj.getFullYear();
    let candidate = new Date(year, harvestStart - 1, 15);
    // If the fixed month date is before or within 7 days of sow date, try next year
    if (candidate <= new Date(sowDateObj.getTime() + 7 * 86400000)) {
      candidate = new Date(year + 1, harvestStart - 1, 15);
    }
    harvestDate = candidate.toISOString().split("T")[0];
  }

  // Final safety check — harvest must always be after sow date
  if (harvestDate && new Date(harvestDate) <= sowDateObj) {
    harvestDate = dtm ? addDays(sowDate, dtm) : addDays(sowDate, 60);
  }

  const LABELS = {
    seed:       "Seed",
    seedling:   "Seedling",
    vegetative: "Vegetative",
    flowering:  "Flowering",
    fruiting:   "Fruiting",
    harvesting: "Harvest",
  };

  const DESCRIPTIONS = {
    seed:       "Germinating — keep warm and moist.",
    seedling:   "First leaves appearing. Keep on a sunny windowsill.",
    vegetative: "Strong leaf and stem growth. Begin feeding fortnightly.",
    flowering:  "Flowers forming. Switch to high potash feed.",
    fruiting:   "Fruit setting. Feed weekly and water consistently.",
    harvesting: "Ready to harvest. Pick regularly to encourage more.",
  };

  const SYMPTOM_CODES = {
    seedling:   "seedling_emerged",
    vegetative: "vegetative_confirmed",
    flowering:  "flowering_confirmed",
    fruiting:   "fruit_set_confirmed",
    harvesting: "harvest_started",
  };

  const nodes = STAGE_ORDER.map((stage, i) => {
    const status = i < currentIdx ? "completed" : i === currentIdx ? "current" : "upcoming";
    const date   = stageDates[stage] || null;
    const isHarvest = stage === "harvesting";
    return {
      key:          stage,
      label:        LABELS[stage],
      status,
      formatted_date: isHarvest && harvestDate ? fmt(harvestDate) : date ? fmt(date) : null,
      description:  DESCRIPTIONS[stage],
      can_confirm:  status !== "completed",
      confirm_symptom_code: SYMPTOM_CODES[stage] || null,
      source:       "estimated",
    };
  });

  // Find next stage node
  const nextNode = nodes.find(n => n.status === "upcoming");

  // progress_pct reflects confirmed growth stage position, not raw calendar time.
  // Using STAGE_PCT means the slider shows where the plant actually is in its
  // lifecycle — a plant confirmed at "vegetative" sits at ~25% regardless of
  // how many days have elapsed since sowing.
  const stagePctValue = STAGE_PCT[currentStage] ?? (currentIdx / (STAGE_ORDER.length - 1));
  const progressPct   = Math.min(100, Math.max(0, Math.round(stagePctValue * 100)));

  return {
    nodes,
    current_stage:       currentStage,
    current_stage_label: LABELS[currentStage],
    current_stage_description: DESCRIPTIONS[currentStage],
    next_stage_label:    nextNode ? LABELS[nextNode.key] : null,
    next_stage_date:     nextNode?.formatted_date || null,
    harvest_date:        harvestDate ? fmt(harvestDate) : null,
    harvest_date_iso:    harvestDate || null,
    progress_pct:        progressPct,
    confidence:          dtm ? "medium" : "low",
    observation_offset_days: 0,
  };
}

// =============================================================================
// AUTH / PROFILE
// =============================================================================

app.post("/auth/profile", requireAuth,
  [body("name").trim().notEmpty(), body("postcode").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { name, postcode, measurement_unit } = req.body;
    const updates = { id: req.user.id, name, postcode };
    if (measurement_unit === "metric" || measurement_unit === "imperial") {
      updates.measurement_unit = measurement_unit;
    }
    const { data, error } = await req.db.from("profiles")
      .upsert(updates).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.get("/auth/profile", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("profiles").select("*").eq("id", req.user.id).single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST /auth/email-preferences — toggle marketing emails on/off
// Uses existing email_unsubscribed boolean on profiles (true = unsubscribed)
app.post("/auth/email-preferences", requireAuth, async (req, res) => {
  const { marketing_emails_enabled } = req.body;
  if (typeof marketing_emails_enabled !== "boolean") {
    return res.status(400).json({ error: "marketing_emails_enabled must be a boolean" });
  }
  // email_unsubscribed is the inverse of marketing_emails_enabled
  const { error } = await req.db.from("profiles")
    .update({ email_unsubscribed: !marketing_emails_enabled })
    .eq("id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true, marketing_emails_enabled });
});

// DELETE /auth/account — anonymise and delete user account
// Retains behavioural data (harvest_log, crop_instances, observation_logs, manual_activity_logs)
// under a new anonymous ID. Deletes all personal/operational data and auth record.
app.delete("/auth/account", requireAuth, async (req, res) => {
  const userId = req.user.id;

  try {
    // 1. Generate anonymous ID to replace user_id in retained tables
    const { v4: uuidv4 } = require("uuid");
    const anonId = uuidv4();

    // 2. Record the anonymisation mapping (for internal audit/GDPR log only — no PII stored)
    await supabaseService.from("deleted_user_anonymous_map").insert({
      anonymous_id: anonId,
      deleted_at: new Date().toISOString(),
    });

    // 3. Reassign retained behavioural data to anonymous ID
    // harvest_log — sowing/harvesting history is the core monetisable dataset
    await supabaseService.from("harvest_log")
      .update({ user_id: anonId })
      .eq("user_id", userId);

    // crop_instances — variety selections, sow dates, grow history
    await supabaseService.from("crop_instances")
      .update({ user_id: anonId })
      .eq("user_id", userId);

    // observation_logs — pest/disease/growth observations
    await supabaseService.from("observation_logs")
      .update({ user_id: anonId })
      .eq("user_id", userId);

    // manual_activity_logs — watering, feeding, pruning actions
    await supabaseService.from("manual_activity_logs")
      .update({ user_id: anonId })
      .eq("user_id", userId);

    // 4. Delete operational / account-specific data
    // Tasks (all — no long-term value vs privacy tradeoff)
    await supabaseService.from("tasks")
      .delete().eq("user_id", userId);

    // Push tokens
    await supabaseService.from("device_push_tokens")
      .delete().eq("user_id", userId);

    // Notification events and preferences
    await supabaseService.from("notification_events")
      .delete().eq("user_id", userId);
    await supabaseService.from("notification_preferences")
      .delete().eq("user_id", userId);

    // Active locations, areas (structural — personal, not worth retaining)
    const { data: areas } = await supabaseService.from("growing_areas")
      .select("id").in("location_id",
        (await supabaseService.from("locations").select("id").eq("user_id", userId)).data?.map(l => l.id) || []
      );
    if (areas?.length) {
      await supabaseService.from("growing_areas")
        .delete().in("id", areas.map(a => a.id));
    }
    await supabaseService.from("locations").delete().eq("user_id", userId);

    // Succession groups
    await supabaseService.from("succession_groups")
      .delete().eq("user_id", userId);

    // Pending crop AI jobs
    await supabaseService.from("pending_crops")
      .delete().eq("user_id", userId);

    // Garden plans
    await supabaseService.from("garden_plans")
      .delete().eq("user_id", userId);

    // Planting suggestions
    await supabaseService.from("planting_suggestions")
      .delete().eq("user_id", userId);

    // Badge progress and unlocks
    await supabaseService.from("user_badge_progress")
      .delete().eq("user_id", userId);
    await supabaseService.from("badge_unlock_events")
      .delete().eq("user_id", userId);

    // Activity counters
    await supabaseService.from("user_activity_counters")
      .delete().eq("user_id", userId);

    // Funnel events
    await supabaseService.from("funnel_events")
      .delete().eq("user_id", userId);

    // Blocked periods / task adjustments
    await supabaseService.from("blocked_periods")
      .delete().eq("user_id", userId);
    await supabaseService.from("task_adjustments")
      .delete().eq("user_id", userId);

    // Diagnosis log (may contain photos/personal context — delete)
    await supabaseService.from("diagnosis_log")
      .delete().eq("user_id", userId);

    // Feedback (personal text — delete)
    await supabaseService.from("feedback")
      .delete().eq("user_id", userId);

    // Email log (operational)
    await supabaseService.from("email_log")
      .delete().eq("user_id", userId);

    // 5. Delete profile row
    await supabaseService.from("profiles")
      .delete().eq("id", userId);

    // 6. Delete auth user — must be last
    const { error: authError } = await supabaseService.auth.admin.deleteUser(userId);
    if (authError) {
      console.error("[DeleteAccount] Auth delete failed:", authError.message);
      // Don't expose to client — personal data already removed
    }

    console.log(`[DeleteAccount] User ${userId} anonymised to ${anonId} and deleted`);
    res.json({ ok: true });

  } catch (err) {
    console.error("[DeleteAccount] Error:", err.message);
    res.status(500).json({ error: "Account deletion failed. Please try again or contact support." });
  }
});

// =============================================================================
// LOCATIONS
// =============================================================================

app.get("/locations", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("locations")
    .select("*, growing_areas(*)")
    .eq("user_id", req.user.id)
    .order("created_at");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/locations", requireAuth,
  [body("name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { name, postcode, latitude, longitude, orientation, notes } = req.body;
    const width_m  = req.body.width_m  !== "" && req.body.width_m  != null ? Number(req.body.width_m)  : null;
    const length_m = req.body.length_m !== "" && req.body.length_m != null ? Number(req.body.length_m) : null;
    if (width_m  !== null && (isNaN(width_m)  || width_m  <= 0)) return res.status(400).json({ error: "width_m must be a positive number" });
    if (length_m !== null && (isNaN(length_m) || length_m <= 0)) return res.status(400).json({ error: "length_m must be a positive number" });
    const { data, error } = await req.db.from("locations")
      .insert({ user_id: req.user.id, name, postcode, latitude, longitude, orientation, notes, width_m, length_m })
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.put("/locations/:id", requireAuth, async (req, res) => {
  const allowed = ["name","postcode","latitude","longitude","orientation","notes","width_m","length_m"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  // Normalise numeric fields — empty string → null, validate positive
  for (const field of ["width_m","length_m"]) {
    if (field in updates) {
      const raw = updates[field];
      if (raw === "" || raw == null) { updates[field] = null; }
      else {
        const n = Number(raw);
        if (isNaN(n) || n <= 0) return res.status(400).json({ error: `${field} must be a positive number` });
        updates[field] = n;
      }
    }
  }
  const { data, error } = await req.db.from("locations")
    .update(updates).eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.delete("/locations/:id", requireAuth, async (req, res) => {
  const locId = req.params.id;
  const userId = req.user.id;

  // Get all areas in this location
  const { data: areas } = await req.db.from("growing_areas")
    .select("id").eq("location_id", locId).eq("user_id", userId);
  const areaIds = (areas || []).map(a => a.id);

  // Delete tasks for all crops in those areas
  if (areaIds.length > 0) {
    const { data: crops } = await req.db.from("crop_instances")
      .select("id").in("area_id", areaIds).eq("user_id", userId);
    const cropIds = (crops || []).map(c => c.id);
    if (cropIds.length > 0) {
      await req.db.from("tasks").delete().in("crop_instance_id", cropIds).eq("user_id", userId);
      // Soft-delete active crops — preserves harvested crop history for future rotation suggestions
      // Harvested crops (status=harvested) are kept with area_id intact so rotation is queryable
      const { data: activeCrops } = await req.db.from("crop_instances")
        .select("id").in("id", cropIds).eq("active", true);
      const activeCropIds = (activeCrops || []).map(c => c.id);
      if (activeCropIds.length > 0) {
        await req.db.from("crop_instances")
          .update({ active: false, updated_at: new Date().toISOString() })
          .in("id", activeCropIds).eq("user_id", userId);
      }
    }
    await req.db.from("growing_areas").delete().in("id", areaIds).eq("user_id", userId);
  }

  // Delete the location itself
  const { error } = await req.db.from("locations").delete()
    .eq("id", locId).eq("user_id", userId);
  if (error) return res.status(500).json({ error: error.message });
  res.status(204).send();
});

// PUT /locations/:id/area-order — save reordered area sort_order for a location
app.put("/locations/:id/area-order", requireAuth, async (req, res) => {
  const locId  = req.params.id;
  const userId = req.user.id;
  const { area_ids } = req.body; // ordered array of area UUIDs

  if (!Array.isArray(area_ids) || area_ids.length === 0) {
    return res.status(400).json({ error: "area_ids must be a non-empty array" });
  }

  // Verify this location belongs to the user
  const { data: loc, error: locErr } = await supabaseService
    .from("locations")
    .select("id")
    .eq("id", locId)
    .eq("user_id", userId)
    .single();
  if (locErr || !loc) return res.status(403).json({ error: "Location not found" });

  // Verify all supplied area IDs belong to this location
  const { data: ownedAreas, error: areaErr } = await supabaseService
    .from("growing_areas")
    .select("id")
    .eq("location_id", locId)
    .in("id", area_ids);
  if (areaErr) return res.status(500).json({ error: areaErr.message });

  const ownedIds = new Set((ownedAreas || []).map(a => a.id));
  if (area_ids.some(id => !ownedIds.has(id))) {
    return res.status(400).json({ error: "One or more area_ids do not belong to this location" });
  }

  // Update sort_order sequentially (1-based) matching the supplied order
  const updates = area_ids.map((id, i) =>
    supabaseService.from("growing_areas").update({ sort_order: i + 1 }).eq("id", id)
  );
  const results = await Promise.all(updates);
  const failed  = results.find(r => r.error);
  if (failed) return res.status(500).json({ error: failed.error.message });

  res.json({ ok: true });
});

// =============================================================================
// GROWING AREAS
// =============================================================================

app.get("/areas", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("growing_areas")
    .select("*, location:location_id(name, postcode), crop_instances(id, name, variety, stage)")
    .order("name");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/areas", requireAuth,
  [
    body("location_id").isUUID(),
    body("name").trim().notEmpty(),
    body("type").isIn(["raised_bed","greenhouse","polytunnel","container","open_ground"]),
  ],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { location_id, name, type, sun_exposure, notes } = req.body;
    // Normalise and validate numeric fields
    const parseNumeric = (val) => (val === "" || val == null) ? null : Number(val);
    const width_m          = parseNumeric(req.body.width_m);
    const length_m         = parseNumeric(req.body.length_m);
    const soil_ph          = parseNumeric(req.body.soil_ph);
    const soil_temperature_c = parseNumeric(req.body.soil_temperature_c);
    if (width_m          !== null && (isNaN(width_m)          || width_m  <= 0))        return res.status(400).json({ error: "width_m must be a positive number" });
    if (length_m         !== null && (isNaN(length_m)         || length_m <= 0))        return res.status(400).json({ error: "length_m must be a positive number" });
    if (soil_ph          !== null && (isNaN(soil_ph)          || soil_ph < 0 || soil_ph > 14)) return res.status(400).json({ error: "soil_ph must be between 0 and 14" });
    if (soil_temperature_c !== null && (isNaN(soil_temperature_c) || soil_temperature_c < -20 || soil_temperature_c > 60)) return res.status(400).json({ error: "soil_temperature_c must be between -20 and 60" });
    const { data, error } = await req.db.from("growing_areas")
      .insert({ location_id, name, type, width_m, length_m, sun_exposure, notes, soil_ph, soil_temperature_c })
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.put("/areas/:id", requireAuth, async (req, res) => {
  const allowed = ["name","type","width_m","length_m","sun_exposure","notes","soil_ph","soil_ph_logged_at","soil_ph_source","soil_temperature_c","soil_temperature_logged_at","soil_temperature_source","layout_x","layout_y","rotation","shape_type","soil_moisture","soil_moisture_source"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  // Normalise numeric fields — empty string → null, validate ranges
  for (const field of ["width_m","length_m","soil_ph","soil_temperature_c"]) {
    if (field in updates) {
      const raw = updates[field];
      if (raw === "" || raw == null) { updates[field] = null; }
      else {
        const n = Number(raw);
        if (isNaN(n)) return res.status(400).json({ error: `${field} must be a number` });
        if ((field === "width_m" || field === "length_m") && n <= 0) return res.status(400).json({ error: `${field} must be a positive number` });
        if (field === "soil_ph" && (n < 0 || n > 14)) return res.status(400).json({ error: "soil_ph must be between 0 and 14" });
        if (field === "soil_temperature_c" && (n < -20 || n > 60)) return res.status(400).json({ error: "soil_temperature_c must be between -20 and 60" });
        updates[field] = n;
      }
    }
  }
  // soil_moisture: validate enum, auto-stamp logged_at
  if ("soil_moisture" in updates) {
    const VALID = ["dry", "ok", "wet"];
    if (updates.soil_moisture === "" || updates.soil_moisture == null) {
      updates.soil_moisture = null;
      updates.soil_moisture_logged_at = null;
    } else if (!VALID.includes(updates.soil_moisture)) {
      return res.status(400).json({ error: "soil_moisture must be dry, ok, or wet" });
    } else {
      updates.soil_moisture_logged_at = new Date().toISOString();
    }
  }
  // soil_ph and soil_temperature_c: auto-stamp logged_at when a real value is saved
  if ("soil_ph" in updates) {
    updates.soil_ph_logged_at = updates.soil_ph !== null ? new Date().toISOString() : null;
  }
  if ("soil_temperature_c" in updates) {
    updates.soil_temperature_logged_at = updates.soil_temperature_c !== null ? new Date().toISOString() : null;
  }
  const { data, error } = await req.db.from("growing_areas")
    .update(updates).eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// POST /areas/:id/soil-reading — log a soil moisture, temperature or pH reading
// source: sensor | manual | weather_estimate
// type:   moisture | temperature | ph
app.post("/areas/:id/soil-reading", requireAuth, async (req, res) => {
  const { type, value, source } = req.body;

  const VALID_TYPES   = ["moisture", "temperature", "ph"];
  const VALID_SOURCES = ["sensor", "manual", "weather_estimate"];

  if (!VALID_TYPES.includes(type))   return res.status(400).json({ error: "type must be moisture, temperature or ph" });
  if (!VALID_SOURCES.includes(source)) return res.status(400).json({ error: "source must be sensor, manual or weather_estimate" });
  if (type === "ph" && source !== "manual") return res.status(400).json({ error: "pH source must be manual" });

  // Verify area ownership via location
  const { data: area } = await supabaseService
    .from("growing_areas")
    .select("id, location_id, locations!inner(user_id)")
    .eq("id", req.params.id)
    .single();
  if (!area || area.locations?.user_id !== req.user.id) return res.status(404).json({ error: "Area not found" });

  const now = new Date().toISOString();
  const updates = {};

  if (type === "moisture") {
    const VALID_MOISTURE = ["dry", "ok", "wet"];
    if (!VALID_MOISTURE.includes(value)) return res.status(400).json({ error: "moisture value must be dry, ok or wet" });
    updates.soil_moisture            = value;
    updates.soil_moisture_logged_at  = now;
    updates.soil_moisture_source     = source;
  } else if (type === "temperature") {
    const n = Number(value);
    if (isNaN(n) || n < -20 || n > 60) return res.status(400).json({ error: "temperature must be between -20 and 60" });
    updates.soil_temperature_c        = n;
    updates.soil_temperature_logged_at = now;
    updates.soil_temperature_source   = source;
  } else if (type === "ph") {
    const n = Number(value);
    if (isNaN(n) || n < 0 || n > 14) return res.status(400).json({ error: "pH must be between 0 and 14" });
    updates.soil_ph            = n;
    updates.soil_ph_logged_at  = now;
    updates.soil_ph_source     = source;
  }

  const { data, error } = await supabaseService
    .from("growing_areas")
    .update(updates)
    .eq("id", req.params.id)
    .select()
    .single();
  if (error) return res.status(500).json({ error: error.message });

  res.json(data);
});

// POST /areas/:id/duplicate — copy area (name, type, dimensions) without crops or soil data
app.post("/areas/:id/duplicate", requireAuth, async (req, res) => {
  // Fetch the original area, verifying ownership via location join
  const { data: orig, error: fetchErr } = await req.db
    .from("growing_areas")
    .select("id, name, type, width_m, length_m, sun_exposure, notes, location_id, locations!inner(user_id)")
    .eq("id", req.params.id)
    .eq("locations.user_id", req.user.id)
    .single();
  if (fetchErr || !orig) return res.status(404).json({ error: "Area not found" });

  const { data, error } = await req.db.from("growing_areas").insert({
    location_id:  orig.location_id,
    name:         orig.name + " (copy)",
    type:         orig.type,
    width_m:      orig.width_m,
    length_m:     orig.length_m,
    sun_exposure: orig.sun_exposure,
    notes:        orig.notes,
    // Soil data deliberately excluded — could be different bed, impacts rule engine
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

app.delete("/areas/:id", requireAuth, async (req, res) => {
  const { error } = await req.db.from("growing_areas").delete().eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.status(204).send();
});

// =============================================================================
// CROP DEFINITIONS + VARIETIES (public)
// =============================================================================

app.get("/crop-definitions", async (_req, res) => {
  const { data, error } = await supabaseService.from("crop_definitions")
    .select("id, name, category, default_establishment, is_perennial, sow_indoors_start, sow_indoors_end, sow_direct_start, sow_direct_end, plant_out_start, plant_out_end, harvest_month_start, harvest_month_end, days_to_maturity_min, days_to_maturity_max, frost_sensitive, preferred_position, feed_type, feed_interval_days, companions, avoid, pest_notes, grower_notes")
    .eq("hidden", false)
    .order("name");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.get("/varieties", async (req, res) => {
  const { crop_def_id } = req.query;
  let query = supabaseService.from("varieties")
    .select("id, crop_def_id, name, classification, days_to_maturity_min, days_to_maturity_max, is_default, notes")
    .eq("active", true).order("name");
  if (crop_def_id) query = query.eq("crop_def_id", crop_def_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// Sow advice for current month
app.get("/crop-definitions/:id/sow-advice", async (req, res) => {
  const { data, error } = await supabaseService.from("crop_definitions")
    .select("*").eq("id", req.params.id).single();
  if (error || !data) return res.status(404).json({ error: "Crop not found" });
  const m = new Date().getMonth() + 1;
  const canSowIndoors = data.sow_indoors_start && m >= data.sow_indoors_start && m <= data.sow_indoors_end;
  const canSowDirect  = data.sow_direct_start  && m >= data.sow_direct_start  && m <= data.sow_direct_end;
  const canPlantOut   = data.plant_out_start   && m >= data.plant_out_start   && m <= data.plant_out_end;
  res.json({
    crop: data.name, current_month: m,
    can_sow_indoors: !!canSowIndoors,
    can_sow_direct:  !!canSowDirect,
    can_plant_out:   !!canPlantOut,
    advice: canSowIndoors ? `Good time to sow ${data.name} indoors.`
          : canSowDirect  ? `Good time to direct sow ${data.name}.`
          : canPlantOut   ? `Good time to plant out ${data.name}.`
          : `Not the ideal month for ${data.name}.`,
  });
});

// =============================================================================
// CROP ENRICHMENT — AI-powered background worker
// Fires when a user submits an "other" crop or variety name.
// Calls Claude to validate, correct spelling, and build full crop data.
// On success: inserts into crop_definitions/varieties and links the instance.
// =============================================================================

// ── Crop name canonicalisation ────────────────────────────────────────────────
// Normalises free-text crop names entered by users:
//   - trim whitespace and title-case
//   - map common plurals and aliases to canonical singular names
//   - strip obvious method suffixes (handled further down in succession code)
// Only applied to "other crop" free-text entries, not crop_def names.
// crop_def names are already canonical — always use crop_def.name directly.

const CROP_ALIASES = {
  // Plurals → singular
  "carrots":       "Carrot",
  "tomatoes":      "Tomato",
  "peppers":       "Pepper",
  "onions":        "Onion",
  "potatoes":      "Potato",
  "strawberries":  "Strawberry",
  "raspberries":   "Raspberry",
  "blackberries":  "Blackberry",
  "gooseberries":  "Gooseberry",
  "courgettes":    "Courgette",
  "zucchinis":     "Courgette",
  "zucchini":      "Courgette",
  "lettuces":      "Lettuce",
  "leeks":         "Leek",
  "parsnips":      "Parsnip",
  "beetroots":     "Beetroot",
  "beets":         "Beetroot",
  "beans":         "Bean",
  "peas":          "Pea",
  "cabbages":      "Cabbage",
  "kales":         "Kale",
  "spinaches":     "Spinach",
  "radishes":      "Radish",
  "turnips":       "Turnip",
  "swedes":        "Swede",
  "aubergines":    "Aubergine",
  "eggplants":     "Aubergine",
  "cucumbers":     "Cucumber",
  "pumpkins":      "Pumpkin",
  "squashes":      "Squash",
  "melons":        "Melon",
  "sunflowers":    "Sunflower",
  "marigolds":     "Marigold",
  "nasturtiums":   "Nasturtium",
  "herbs":         "Herb",
  "chillies":      "Chilli",
  "chillis":       "Chilli",
  "chilis":        "Chilli",
  "chiles":        "Chilli",
  "chilies":       "Chilli",
  "chili":         "Chilli",
  "chilli peppers":"Chilli",
  "chili peppers": "Chilli",
  "courgette marrows": "Courgette",
  "french beans":  "French Bean",
  "runner beans":  "Runner Bean",
  "broad beans":   "Broad Bean",
  "climbing beans":"Climbing Bean",
  "dwarf beans":   "Dwarf Bean",
  "spring onions": "Spring Onion",
  "salad leaves":  "Salad Leaves",
  "mixed salad":   "Salad Leaves",
  "salad":         "Salad Leaves",
  "pak choi":      "Pak Choi",
  "bok choy":      "Pak Choi",
  "brussel sprouts":"Brussels Sprout",
  "brussels sprouts":"Brussels Sprout",
  "sweetcorn":     "Sweet Corn",
  "sweet corn":    "Sweet Corn",
  "corn":          "Sweet Corn",
  "psb":           "Purple Sprouting Broccoli",
  "purple sprouting broccoli": "Purple Sprouting Broccoli",
  "broccoli":      "Broccoli",
  "cauliflowers":  "Cauliflower",
  "artichokes":    "Artichoke",
  "asparaguses":   "Asparagus",
  "rhubarbs":      "Rhubarb",
};

function canonicaliseCropName(rawName) {
  if (!rawName || typeof rawName !== "string") return rawName;
  const trimmed = rawName.trim();
  if (!trimmed) return trimmed;
  // Check alias map (case-insensitive)
  const lower = trimmed.toLowerCase();
  if (CROP_ALIASES[lower]) return CROP_ALIASES[lower];
  // Title-case the raw input as a fallback
  return trimmed.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

async function enrichCrop(cropInstanceId, submittedName, submittedVariety) {
  const db = supabaseService;

  // Create a pending record immediately
  const { data: pending, error: pendingErr } = await db
    .from("pending_crops")
    .insert({
      crop_instance_id:  cropInstanceId,
      submitted_name:    submittedName,
      submitted_variety: submittedVariety || null,
      status:            "processing",
    })
    .select().single();

  if (pendingErr) {
    console.error("[Enrich] Failed to create pending record:", pendingErr.message);
    return;
  }

  try {
    const prompt = `You are a horticultural expert for UK home growers and allotment holders.
A user has added a crop to their garden with the following details:
- Crop name: "${submittedName}"
- Variety: "${submittedVariety || "not specified"}"

Your task:
1. Determine if this is a real, growable crop in the UK (vegetables, fruit, herbs). If it is nonsense, misspelled beyond recognition, or not a real crop, reject it.
2. If real, correct any spelling errors in both the crop name and variety name.
3. Return comprehensive UK growing data.

Respond ONLY with a JSON object — no markdown, no explanation. Use this exact structure:
{
  "valid": true,
  "rejection_reason": null,
  "crop": {
    "name": "corrected crop name",
    "category": "one of: fruiting, root, brassica, legume, allium, salad, herb, perennial, fruit",
    "default_establishment": "one of: indoors, direct_sow, tuber, crown, runner, cane",
    "is_perennial": false,  // true for fruit trees, bushes, asparagus, rhubarb, artichokes — anything that lives for multiple years
    "sow_indoors_start": 2,
    "sow_indoors_end": 4,
    "sow_direct_start": null,
    "sow_direct_end": null,
    "plant_out_start": 5,
    "plant_out_end": 6,
    "harvest_month_start": 7,
    "harvest_month_end": 10,
    "days_to_maturity_min": 60,
    "days_to_maturity_max": 90,
    "feed_type": "high potash liquid feed",
    "feed_interval_days": 14,
    "frost_sensitive": true,
    "preferred_position": "one of: full_sun, partial_shade, full_shade",
    "companions": ["basil", "marigold"],
    "avoid": ["fennel"],
    "pest_window_start": 5,
    "pest_window_end": 9,
    "pest_notes": "brief pest notes",
    "grower_notes": "key growing tips for UK growers",
    "soil_temp_min_c": 10.0,
    "soil_temp_optimal_min_c": 15.0,
    "soil_temp_optimal_max_c": 25.0,
    "soil_temp_max_c": 30.0,
    "soil_ph_min": 6.0,
    "soil_ph_optimal_min": 6.2,
    "soil_ph_optimal_max": 6.8,
    "soil_ph_max": 7.0,
    "soil_moisture_pref": "medium"
  },
  "variety": {
    "name": "corrected variety name or null if not provided",
    "classification": "e.g. Early, Maincrop, Late, Heritage, F1 — or null",
    "days_to_maturity_min": 65,
    "days_to_maturity_max": 75,
    "sow_window_start": 5,
    "sow_window_end": 6,
    "transplant_window_start": null,
    "transplant_window_end": null,
    "notes": "what makes this variety distinctive"
  }

CRITICAL RULE FOR VARIETY SOW WINDOWS:
You MUST set sow_window_start and sow_window_end on the variety whenever you have reliable knowledge of that variety's sow timing.
Do NOT default to null — most named varieties have known sow windows.
Examples:
- Tweed F1 Swede: sow_window_start=5, sow_window_end=6 (late maincrop, sow May-June)
- Gardener's Delight Tomato: sow_window_start=2, sow_window_end=4 (sow Feb-Apr indoors)
- Early Nantes Carrot: sow_window_start=3, sow_window_end=6 (early variety, sow Mar-Jun)
Only use null if you genuinely have no information about that specific variety's timing.
}

If the crop is not valid, return:
{ "valid": false, "rejection_reason": "brief reason", "crop": null, "variety": null }

Use null for any fields you don't have reliable data for. All month values are integers 1-12. Base everything on UK growing conditions.
For soil_moisture_pref use one of: low, medium, high, even_moisture, dry_down_before_harvest
For soil temperature fields: use null for perennial crops where soil temp is not a germination limiting factor.`;

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type":      "application/json",
        "x-api-key":         process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model:      "claude-sonnet-4-20250514",
        max_tokens: 1500,
        messages:   [{ role: "user", content: prompt }],
      }),
    });

    const raw = await response.json();
    console.log(`[Enrich] Anthropic status: ${response.status}, type: ${raw.type}, error: ${raw.error?.message || 'none'}`);
    const text = raw.content?.[0]?.text || "";
    console.log(`[Enrich] Claude raw response (first 300 chars): ${text.slice(0, 300)}`);

    let parsed;
    try {
      // Extract JSON robustly — find the outermost { } block regardless of surrounding text
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error("No JSON object found in response");
      parsed = JSON.parse(jsonMatch[0]);
    } catch {
      throw new Error(`Claude returned unparseable JSON: ${text.slice(0, 200)}`);
    }

    // Store raw response for debugging
    await db.from("pending_crops").update({ claude_response: parsed }).eq("id", pending.id);

    if (!parsed.valid) {
      await db.from("pending_crops").update({
        status:           "rejected",
        rejection_reason: parsed.rejection_reason,
        resolved_at:      new Date().toISOString(),
      }).eq("id", pending.id);
      console.log(`[Enrich] Rejected "${submittedName}": ${parsed.rejection_reason}`);
      return;
    }

    const cropData = parsed.crop;
    const varietyData = parsed.variety;

    // ── Check if crop already exists — fuzzy match to prevent duplicates ────────
    // Normalise: lowercase, strip trailing 's', strip leading/trailing spaces
    const normaliseName = (n) => n.toLowerCase().trim().replace(/s$/i, '');
    let cropDefId;

    // First try exact case-insensitive match
    const { data: exactMatch } = await db.from("crop_definitions")
      .select("id, name").ilike("name", cropData.name).eq("hidden", false).maybeSingle();

    // Then try fuzzy: fetch all non-hidden definitions and compare normalised names
    let existing = exactMatch;
    if (!existing) {
      const { data: allDefs } = await db.from("crop_definitions")
        .select("id, name").eq("hidden", false);
      const normNew = normaliseName(cropData.name);
      const fuzzyMatch = (allDefs || []).find(d => normaliseName(d.name) === normNew);
      if (fuzzyMatch) {
        existing = fuzzyMatch;
        console.log(`[Enrich] Fuzzy match "${cropData.name}" → "${fuzzyMatch.name}" — using existing`);
      }
    }

    if (existing) {
      cropDefId = existing.id;
      console.log(`[Enrich] Crop "${cropData.name}" already exists — using existing`);
    } else {
      // Insert new crop definition
      const { data: newCrop, error: cropErr } = await db.from("crop_definitions").insert({
        name:                  cropData.name,
        category:              cropData.category,
        default_establishment: cropData.default_establishment,
        is_perennial:          cropData.is_perennial || false,
        sow_indoors_start:     cropData.sow_indoors_start,
        sow_indoors_end:       cropData.sow_indoors_end,
        sow_direct_start:      cropData.sow_direct_start,
        sow_direct_end:        cropData.sow_direct_end,
        // Map to rule engine columns
        sow_window_start:      cropData.sow_direct_start || cropData.sow_indoors_start || null,
        sow_window_end:        cropData.sow_direct_end   || cropData.sow_indoors_end   || null,
        sow_method:            cropData.sow_direct_start && cropData.sow_indoors_start ? "either"
                             : cropData.sow_direct_start ? "outdoors"
                             : cropData.sow_indoors_start ? "indoors" : "either",
        transplant_window_start: cropData.plant_out_start || null,
        transplant_window_end:   cropData.plant_out_end   || null,
        plant_out_start:       cropData.plant_out_start,
        plant_out_end:         cropData.plant_out_end,
        harvest_month_start:   cropData.harvest_month_start,
        harvest_month_end:     cropData.harvest_month_end,
        days_to_maturity_min:  cropData.days_to_maturity_min,
        days_to_maturity_max:  cropData.days_to_maturity_max,
        feed_type:             cropData.feed_type,
        feed_interval_days:    cropData.feed_interval_days,
        frost_sensitive:       cropData.frost_sensitive,
        preferred_position:    cropData.preferred_position,
        companions:            cropData.companions || [],
        avoid:                 cropData.avoid || [],
        pest_window_start:     cropData.pest_window_start,
        pest_window_end:       cropData.pest_window_end,
        pest_notes:            cropData.pest_notes,
        grower_notes:          cropData.grower_notes,
        soil_temp_min_c:         cropData.soil_temp_min_c         || null,
        soil_temp_optimal_min_c: cropData.soil_temp_optimal_min_c || null,
        soil_temp_optimal_max_c: cropData.soil_temp_optimal_max_c || null,
        soil_temp_max_c:         cropData.soil_temp_max_c         || null,
        soil_ph_min:             cropData.soil_ph_min             || null,
        soil_ph_optimal_min:     cropData.soil_ph_optimal_min     || null,
        soil_ph_optimal_max:     cropData.soil_ph_optimal_max     || null,
        soil_ph_max:             cropData.soil_ph_max             || null,
        soil_moisture_pref:      cropData.soil_moisture_pref      || null,
      }).select("id").single();

      if (cropErr) throw new Error(`Crop insert failed: ${cropErr.message}`);
      cropDefId = newCrop.id;
      console.log(`[Enrich] Added new crop "${cropData.name}" (${cropDefId})`);
    }

    // ── Insert variety if provided ────────────────────────────────────────────
    let varietyId = null;
    if (varietyData?.name) {
      // Check if variety already exists for this crop
      const { data: existingVar } = await db.from("varieties")
        .select("id").eq("crop_def_id", cropDefId).ilike("name", varietyData.name).maybeSingle();

      if (existingVar) {
        varietyId = existingVar.id;
        console.log(`[Enrich] Variety "${varietyData.name}" already exists`);
      } else {
        // Fall back to crop-level sow windows if variety doesn't have its own
        const varSowStart = varietyData.sow_window_start
          || cropData.sow_direct_start
          || cropData.sow_indoors_start
          || null;
        const varSowEnd = varietyData.sow_window_end
          || cropData.sow_direct_end
          || cropData.sow_indoors_end
          || null;

        const { data: newVar, error: varErr } = await db.from("varieties").insert({
          crop_def_id:             cropDefId,
          name:                    varietyData.name,
          classification:          varietyData.classification || null,
          days_to_maturity_min:    varietyData.days_to_maturity_min || null,
          days_to_maturity_max:    varietyData.days_to_maturity_max || null,
          sow_window_start:        varSowStart,
          sow_window_end:          varSowEnd,
          transplant_window_start: varietyData.transplant_window_start || cropData.plant_out_start || null,
          transplant_window_end:   varietyData.transplant_window_end   || cropData.plant_out_end   || null,
          notes:                   varietyData.notes || null,
          is_default:              false,
          active:                  true,
        }).select("id").single();

        if (varErr) throw new Error(`Variety insert failed: ${varErr.message}`);
        varietyId = newVar.id;
        console.log(`[Enrich] Added new variety "${varietyData.name}" (${varietyId})`);
      }
    }

    // ── Update the crop instance with the real linked records ─────────────────
    await db.from("crop_instances").update({
      name:        cropData.name,   // corrected spelling
      crop_def_id: cropDefId,
      variety_id:  varietyId,
      variety:     varietyData?.name || null,
      updated_at:  new Date().toISOString(),
    }).eq("id", cropInstanceId);

    // ── Mark pending as complete ──────────────────────────────────────────────
    await db.from("pending_crops").update({
      status:               "completed",
      result_crop_def_id:   cropDefId,
      result_variety_id:    varietyId,
      resolved_at:          new Date().toISOString(),
    }).eq("id", pending.id);

    console.log(`[Enrich] ✓ Completed enrichment for instance ${cropInstanceId}`);

  } catch (err) {
    console.error("[Enrich] Error:", err.message);
    await supabaseService.from("pending_crops").update({
      status:      "failed",
      rejection_reason: err.message,
      resolved_at: new Date().toISOString(),
    }).eq("id", pending.id);
  }
}

// =============================================================================
// CROPS
// =============================================================================

app.get("/crops", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("crop_instances")
    .select("*, area:area_id(name, type, location_id, location:location_id(name)), crop_def:crop_def_id(name, harvest_month_start, harvest_month_end, harvest_month_start, harvest_month_end, days_to_maturity_min, days_to_maturity_max, sow_method, is_perennial, default_establishment, pest_window_start, pest_window_end), variety:variety_id(name, days_to_maturity_min, days_to_maturity_max)")
    .eq("user_id", req.user.id).eq("active", true)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// GET /crops/history?location_id=xxx
// Returns the most recent harvested crop_instance per area for a location.
// Used by AreaTimelineBlock to show "Last season" in the area detail sheet.
// Only harvested crops with harvested_at are included.
app.get("/crops/history", requireAuth, async (req, res) => {
  try {
    const { location_id } = req.query;
    if (!location_id) return res.status(400).json({ error: "location_id required" });

    // Verify location belongs to user
    const { data: loc } = await req.db
      .from("locations").select("id").eq("id", location_id).eq("user_id", req.user.id).single();
    if (!loc) return res.status(403).json({ error: "Location not found" });

    // Get all area IDs for this location
    const { data: areas } = await req.db
      .from("growing_areas").select("id").eq("location_id", location_id);
    if (!areas?.length) return res.json([]);

    const areaIds = areas.map(a => a.id);

    // Fetch all harvested crops for these areas — one per area (most recent)
    const { data: crops, error } = await supabaseService
      .from("crop_instances")
      .select("id, area_id, name, status, harvested_at, crop_def_id, variety_id, crop_definitions(name, category)")
      .eq("user_id", req.user.id)
      .in("area_id", areaIds)
      .eq("status", "harvested")
      .not("harvested_at", "is", null)
      .order("harvested_at", { ascending: false });

    if (error) throw error;

    // Deduplicate — keep only the most recent per area
    const byArea = {};
    for (const c of (crops || [])) {
      if (!byArea[c.area_id]) {
        byArea[c.area_id] = {
          id:           c.id,
          area_id:      c.area_id,
          name:         c.crop_definitions?.name || c.name,
          category:     c.crop_definitions?.category || null,
          harvested_at: c.harvested_at,
        };
      }
    }

    res.json(Object.values(byArea));
  } catch (err) {
    captureError("CropsHistory", err);
    res.status(500).json({ error: err.message });
  }
});

app.get("/crops/:id", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("crop_instances")
    .select("*, area:area_id(*), crop_def:crop_def_id(*), variety:variety_id(*), tasks(*)")
    .eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (error) return res.status(404).json({ error: "Crop not found" });

  // ── Build timeline ────────────────────────────────────────────────────────
  const timeline = buildTimeline(data);

  res.json({ ...data, timeline });
});

// POST /crops/preview — AI crop profile for confirmation screen
app.post("/crops/preview", requireAuth, async (req, res) => {
  const { name, variety } = req.body;
  if (!name) return res.status(400).json({ error: "name required" });
  try {
    const label = name + (variety ? ` (${variety})` : "");
    const prompt = `You are a UK horticultural expert. Build a concise growing profile for: ${label}

Respond ONLY with a JSON object, no markdown:
{"name":"Canonical crop name","description":"2 sentence description for UK home growers","sow_window":"e.g. Mar - May or null","harvest_window":"e.g. Jul - Sep or null","spacing_cm":25,"days_to_maturity":"60-80 days or null","sow_method":"indoors or outdoors or both or null","feeding_notes":"Brief feeding guidance or null","companion_plants":"2-3 companions or null","common_issues":"2-3 common problems or null","known":false}`;

    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 600, messages: [{ role: "user", content: prompt }] }),
    });
    const raw  = await r.json();
    const text = raw.content?.[0]?.text || "";
    const m    = text.match(/\{[\s\S]*\}/);
    if (!m) throw new Error("No JSON in response");
    res.json(JSON.parse(m[0]));
  } catch (e) {
    console.error("[Preview]", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post("/crops", requireAuth,
  [body("area_id").isUUID(), body("name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const {
      area_id, name: rawName, variety, variety_id, crop_def_id,
      sown_date, transplanted_date, planted_out_date, transplant_date,
      establishment_method, quantity, notes,
      start_date_confidence, source, status,
      is_other_crop, is_other_variety,
      barcode,
    } = req.body;

    // For "other crop" free-text entries, normalise the name.
    // For known crops (crop_def_id set), use the name as-is — it comes from
    // the frontend which already uses the canonical crop_def.name.
    const name = (is_other_crop || !crop_def_id) ? canonicaliseCropName(rawName) : rawName;

    // Derive location_id from area
    const { data: area } = await req.db.from("growing_areas")
      .select("location_id").eq("id", area_id).single();

    // Infer status from data if not explicitly set
    const derivedStatus = status || (sown_date ? "growing" : "planned");

    const { data, error } = await req.db.from("crop_instances").insert({
      user_id:              req.user.id,
      location_id:          area?.location_id || null,
      area_id,
      name,
      variety:              variety || null,
      variety_id:           variety_id || null,
      crop_def_id:          crop_def_id || null,
      status:               derivedStatus,
      sown_date:            sown_date || null,
      transplanted_date:    transplanted_date || null,
      transplant_date:      transplant_date || null,
      planted_out_date:     planted_out_date || null,
      establishment_method: establishment_method || null,
      quantity:             quantity || 1,
      notes:                notes || null,
      photo_url:            null,
      start_date_confidence:start_date_confidence || "exact",
      source:               source || "manual",
    }).select().single();

    if (error) return res.status(500).json({ error: error.message });

    // Save barcode against crop_def for instant future lookups
    if (barcode && crop_def_id) {
      await req.db.from("crop_definitions")
        .update({ barcode }).eq("id", crop_def_id);
    }

    // Trigger enrichment if:
    // - crop is unknown (no crop_def_id), OR
    // - variety was typed as free text (variety present but no variety_id)
    const needsEnrichment = !data.crop_def_id || (!data.variety_id && data.variety);
    if (needsEnrichment) {
      await enrichCrop(data.id, name, variety || null);
    }

    await runRuleEngine(req.user.id);

    // Clear planting suggestions for this area — bed is no longer empty
    if (data.area_id) await clearSuggestions(data.area_id, req.db);

    res.status(201).json({ ...data, enriching: needsEnrichment });
  }
);

app.put("/crops/:id", requireAuth, async (req, res) => {
  const allowed = [
    "variety","variety_id","sown_date","transplanted_date","transplant_date","planted_out_date",
    "establishment_method","stage","quantity","notes","area_id","photo_url",
    "start_date_confidence","last_fed_at","status",
    "grown_from","lifecycle_mode","sow_preference",
  ];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  updates.updated_at = new Date().toISOString();
  if (updates.stage) updates.stage_confidence = "exact";

  const { data, error } = await req.db.from("crop_instances")
    .update(updates).eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });

  // Trigger enrichment if variety was just set as free text with no variety_id
  if (updates.variety && !updates.variety_id && !data.variety_id) {
    await enrichCrop(data.id, data.name, updates.variety);
  }

  // Always run rule engine after any crop update — status/sow_date changes affect task generation
  await runRuleEngine(req.user.id);
  res.json(data);
});

app.get("/crops/:id/enrichment", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("pending_crops")
    .select("status, rejection_reason, result_crop_def_id, result_variety_id")
    .eq("crop_instance_id", req.params.id)
    .order("created_at", { ascending: false })
    .limit(1).maybeSingle();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || { status: "none" });
});

app.delete("/crops/:id", requireAuth, async (req, res) => {
  // Fetch area_id + succession_group_id before soft-deleting
  const { data: crop } = await req.db.from("crop_instances")
    .select("area_id, succession_group_id").eq("id", req.params.id).eq("user_id", req.user.id).single();

  const { error } = await req.db.from("crop_instances")
    .update({ active: false, updated_at: new Date().toISOString() })
    .eq("id", req.params.id).eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });

  // Auto-delete empty succession group if this was the last active sowing
  if (crop?.succession_group_id) {
    const { count } = await supabaseService.from("crop_instances")
      .select("id", { count: "exact", head: true })
      .eq("succession_group_id", crop.succession_group_id)
      .eq("active", true);
    if ((count || 0) === 0) {
      await supabaseService.from("succession_groups")
        .delete().eq("id", crop.succession_group_id).eq("user_id", req.user.id);
      console.log(`[Succession] Auto-deleted empty group ${crop.succession_group_id}`);
    }
  }

  // Invalidate suggestions cache — area contents have changed
  if (crop?.area_id) await clearSuggestions(crop.area_id, req.db);

  res.status(204).send();
});

// =============================================================================
// SUCCESSION GROUPS
// =============================================================================

// POST /succession-groups — create group + first sowing
app.post("/succession-groups", requireAuth,
  [
    body("crop_name").trim().notEmpty(),
    body("area_id").isUUID(),
    body("target_sowings").isInt({ min: 1 }),
  ],
  async (req, res) => {
    if (!validate(req, res)) return;
    const {
      crop_def_id, variety_id, variety_name,
      area_id, target_sowings, interval_days, notes,
      first_sown_date, first_status,
    } = req.body;

    // Normalise free-text crop name; known crops already have canonical name from crop_def
    const crop_name = (crop_def_id) ? req.body.crop_name : canonicaliseCropName(req.body.crop_name);

    // Derive location_id from area
    const { data: area } = await req.db.from("growing_areas")
      .select("location_id").eq("id", area_id).single();

    // Create the parent group
    const { data: group, error: groupErr } = await supabaseService.from("succession_groups").insert({
      user_id:      req.user.id,
      crop_def_id:  crop_def_id  || null,
      crop_name,
      variety_id:   variety_id   || null,
      variety_name: variety_name || null,
      area_id,
      target_sowings: Number(target_sowings),
      interval_days:  interval_days ? Number(interval_days) : null,
      notes:          notes || null,
    }).select().single();
    if (groupErr) return res.status(500).json({ error: groupErr.message });

    // Create Sow 1 as a real crop instance
    const derivedStatus = first_status || (first_sown_date ? "growing" : "planned");
    const { data: sowing, error: sowErr } = await supabaseService.from("crop_instances").insert({
      user_id:             req.user.id,
      location_id:         area?.location_id || null,
      area_id,
      name:                crop_name,
      variety:             variety_name || null,
      variety_id:          variety_id   || null,
      crop_def_id:         crop_def_id  || null,
      status:              derivedStatus,
      sown_date:           first_sown_date || null,
      quantity:            1,
      source:              "manual",
      succession_group_id: group.id,
      succession_index:    1,
    }).select().single();
    if (sowErr) {
      // Rollback group if sowing creation fails
      await supabaseService.from("succession_groups").delete().eq("id", group.id);
      return res.status(500).json({ error: sowErr.message });
    }

    await runRuleEngine(req.user.id);
    res.status(201).json({ group, sowings: [sowing] });
  }
);

// GET /succession-groups — list all groups with their active sowings
app.get("/succession-groups", requireAuth, async (req, res) => {
  const { data: groups, error } = await supabaseService.from("succession_groups")
    .select("*, area:area_id(name, type, location_id, location:location_id(name))")
    .eq("user_id", req.user.id)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  // Fetch all active sowings for these groups
  const groupIds = (groups || []).map(g => g.id);
  let sowingsByGroup = {};
  if (groupIds.length > 0) {
    const { data: sowings } = await supabaseService.from("crop_instances")
      .select("*, crop_def:crop_def_id(days_to_maturity_min, days_to_maturity_max, harvest_month_start, harvest_month_end), variety:variety_id(days_to_maturity_min, days_to_maturity_max)")
      .in("succession_group_id", groupIds)
      .eq("active", true)
      .order("succession_index", { ascending: true });
    for (const s of (sowings || [])) {
      if (!sowingsByGroup[s.succession_group_id]) sowingsByGroup[s.succession_group_id] = [];
      sowingsByGroup[s.succession_group_id].push(s);
    }
  }

  const result = (groups || []).map(g => ({
    ...g,
    sowings: sowingsByGroup[g.id] || [],
  }));
  res.json(result);
});

// GET /succession-groups/:id
app.get("/succession-groups/:id", requireAuth, async (req, res) => {
  const { data: group, error } = await supabaseService.from("succession_groups")
    .select("*").eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (error || !group) return res.status(404).json({ error: "Not found" });

  const { data: sowings } = await supabaseService.from("crop_instances")
    .select("*, crop_def:crop_def_id(*), variety:variety_id(*)")
    .eq("succession_group_id", group.id)
    .eq("active", true)
    .order("succession_index", { ascending: true });

  res.json({ ...group, sowings: sowings || [] });
});

// PUT /succession-groups/:id
app.put("/succession-groups/:id", requireAuth, async (req, res) => {
  const allowed = ["target_sowings", "interval_days", "notes", "variety_name"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  if (updates.target_sowings) updates.target_sowings = Number(updates.target_sowings);
  if (updates.interval_days)  updates.interval_days  = Number(updates.interval_days);
  updates.updated_at = new Date().toISOString();
  const { data, error } = await supabaseService.from("succession_groups")
    .update(updates).eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// DELETE /succession-groups/:id — deletes group and soft-deletes all child sowings
app.delete("/succession-groups/:id", requireAuth, async (req, res) => {
  // Soft-delete all active child sowings
  await supabaseService.from("crop_instances")
    .update({ active: false, updated_at: new Date().toISOString() })
    .eq("succession_group_id", req.params.id)
    .eq("user_id", req.user.id);
  // Delete the group
  const { error } = await supabaseService.from("succession_groups")
    .delete().eq("id", req.params.id).eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.status(204).send();
});

// POST /succession-groups/:id/sowings — add next sowing to a group
app.post("/succession-groups/:id/sowings", requireAuth, async (req, res) => {
  const { data: group, error: groupErr } = await supabaseService.from("succession_groups")
    .select("*").eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (groupErr || !group) return res.status(404).json({ error: "Group not found" });

  // Determine next succession_index
  const { data: existing } = await supabaseService.from("crop_instances")
    .select("succession_index").eq("succession_group_id", group.id).eq("active", true)
    .order("succession_index", { ascending: false }).limit(1);
  const nextIndex = ((existing?.[0]?.succession_index) || 0) + 1;

  const { sown_date, notes, status } = req.body;
  const { data: area } = await req.db.from("growing_areas")
    .select("location_id").eq("id", group.area_id).single();

  const derivedStatus = status || (sown_date ? "growing" : "planned");
  const { data: sowing, error: sowErr } = await supabaseService.from("crop_instances").insert({
    user_id:             req.user.id,
    location_id:         area?.location_id || null,
    area_id:             group.area_id,
    name:                group.crop_name,
    variety:             group.variety_name || null,
    variety_id:          group.variety_id   || null,
    crop_def_id:         group.crop_def_id  || null,
    status:              derivedStatus,
    sown_date:           sown_date || null,
    notes:               notes || null,
    quantity:            1,
    source:              "manual",
    succession_group_id: group.id,
    succession_index:    nextIndex,
  }).select().single();
  if (sowErr) return res.status(500).json({ error: sowErr.message });

  await runRuleEngine(req.user.id);
  res.status(201).json(sowing);
});

// POST /crops/:id/convert-to-succession — convert a standalone crop into a succession group
app.post("/crops/:id/convert-to-succession", requireAuth, async (req, res) => {
  const cropId = req.params.id;
  const { target_sowings = 3, interval_days = 14 } = req.body;

  // Fetch the existing crop — ownership check
  const { data: crop, error: cropErr } = await supabaseService
    .from("crop_instances")
    .select("*")
    .eq("id", cropId)
    .eq("user_id", req.user.id)
    .single();
  if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });

  // Already in a succession group — don't double-convert
  if (crop.succession_group_id) {
    return res.status(400).json({ error: "Crop is already part of a succession group" });
  }

  // Create the succession group
  const { data: group, error: groupErr } = await supabaseService
    .from("succession_groups")
    .insert({
      user_id:        req.user.id,
      crop_name:      crop.name,
      variety_name:   crop.variety    || null,
      variety_id:     crop.variety_id  || null,
      crop_def_id:    crop.crop_def_id || null,
      area_id:        crop.area_id,
      target_sowings: Number(target_sowings),
      interval_days:  Number(interval_days),
    })
    .select()
    .single();
  if (groupErr || !group) return res.status(500).json({ error: groupErr?.message || "Failed to create group" });

  // Update the existing crop to be sowing #1
  const { error: updateErr } = await supabaseService
    .from("crop_instances")
    .update({ succession_group_id: group.id, succession_index: 1 })
    .eq("id", cropId)
    .eq("user_id", req.user.id);

  if (updateErr) {
    // Roll back the group
    await supabaseService.from("succession_groups").delete().eq("id", group.id);
    return res.status(500).json({ error: updateErr.message });
  }

  // Create planned follow-on sowings (indices 2 → target_sowings)
  const baseDate = crop.sown_date ? new Date(crop.sown_date) : null;
  const followOns = [];
  for (let i = 2; i <= Math.max(2, Number(target_sowings)); i++) {
    let plannedDate = null;
    if (baseDate) {
      const d = new Date(baseDate);
      d.setDate(d.getDate() + (i - 1) * Number(interval_days));
      plannedDate = d.toISOString().slice(0, 10);
    }
    followOns.push({
      user_id:             req.user.id,
      location_id:         crop.location_id || null,
      area_id:             crop.area_id,
      name:                crop.name,
      variety:             crop.variety      || null,
      variety_id:          crop.variety_id   || null,
      crop_def_id:         crop.crop_def_id  || null,
      status:              "planned",
      sown_date:           plannedDate,
      quantity:            1,
      source:              "manual",
      succession_group_id: group.id,
      succession_index:    i,
    });
  }

  if (followOns.length) {
    const { error: sowErr } = await supabaseService.from("crop_instances").insert(followOns);
    if (sowErr) console.error("[convert-to-succession] Follow-on insert error:", sowErr.message);
  }

  await runRuleEngine(req.user.id);
  res.status(201).json({ group_id: group.id, message: "Converted successfully" });
});

// =============================================================================
// TASKS
// =============================================================================

app.get("/tasks", requireAuth, async (req, res) => {
  const { view = "all", completed } = req.query;
  const today   = todayISO();
  const weekEnd = weekEndISO();

  let query = req.db.from("tasks")
    .select("*, crop:crop_instance_id(name, variety, succession_group_id, succession_index), area:area_id(name)")
    .eq("user_id", req.user.id)
    .order("urgency",  { ascending: false })
    .order("due_date", { ascending: true });

  if (completed === "false") query = query.is("completed_at", null);
  if (completed === "true")  query = query.not("completed_at", "is", null);
  if (view === "today") query = query.eq("due_date", today);
  if (view === "week")  query = query.lte("due_date", weekEnd);

  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });

  // Fetch any active adjustments for this user and merge onto tasks
  const { data: adjustments } = await req.db.from("task_adjustments")
    .select("task_id, adjustment_type, adjusted_due_date, original_due_date")
    .eq("user_id", req.user.id);

  const adjMap = {};
  for (const a of (adjustments || [])) adjMap[a.task_id] = a;

  const enriched = data.map(t => {
    const adj = adjMap[t.id];
    if (!adj) return t;
    return {
      ...t,
      effective_due_date: adj.adjusted_due_date || t.due_date,
      adjustment_type:    adj.adjustment_type,
      original_due_date:  adj.original_due_date,
    };
  });

  // Use effective_due_date for grouping so adjusted tasks appear in the right bucket
  res.json({
    tasks: enriched,
    grouped: {
      today:     enriched.filter(t => (t.effective_due_date || t.due_date) === today),
      this_week: enriched.filter(t => (t.effective_due_date || t.due_date) > today && (t.effective_due_date || t.due_date) <= weekEnd),
      coming_up: enriched.filter(t => (t.effective_due_date || t.due_date) > weekEnd),
    },
  });
});

app.post("/tasks/:id/complete", requireAuth, async (req, res) => {
  const completedAt = new Date().toISOString();
  const today       = completedAt.split("T")[0];

  const { data, error } = await req.db.from("tasks")
    .update({ completed_at: completedAt })
    .eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });

  processBadgeEvent(req.user.id, "task_completed").catch(console.error);
  if (data.crop_instance_id) {
    const meta = data.meta ? (typeof data.meta === "string" ? JSON.parse(data.meta) : data.meta) : {};
    const transition = meta.status_transition;

    if (data.task_type === "water") {
      // Water tasks are area-level — update last_watered_at on all crops in the area
      if (data.area_id) {
        await supabaseService.from("crop_instances")
          .update({ last_watered_at: completedAt, updated_at: completedAt })
          .eq("area_id", data.area_id).eq("user_id", req.user.id);
      } else if (data.crop_instance_id) {
        // Fallback: crop-level water task
        await supabaseService.from("crop_instances")
          .update({ last_watered_at: completedAt, updated_at: completedAt })
          .eq("id", data.crop_instance_id).eq("user_id", req.user.id);
      }
      await runRuleEngine(req.user.id);

    } else if (data.task_type === "feed") {
      // Use supabaseService — req.db (user JWT) is blocked by RLS on crop_instances writes
      await supabaseService.from("crop_instances")
        .update({ last_fed_at: completedAt, updated_at: completedAt })
        .eq("id", data.crop_instance_id).eq("user_id", req.user.id);
      // Re-run engine so next feed task is anchored from today
      await runRuleEngine(req.user.id);

    } else if (data.task_type === "mulch" || data.task_type === "prune" || data.task_type === "thin" || data.task_type === "monitor" || data.task_type === "check") {
      // These tasks use window-based source keys (month/week anchor).
      // Completing them means the current window key is marked done — engine
      // will not regenerate until next window period. No crop state update needed.
      // Touch updated_at so dashboard reflects activity.
      await supabaseService.from("crop_instances")
        .update({ updated_at: completedAt })
        .eq("id", data.crop_instance_id).eq("user_id", req.user.id);

    } else if (data.task_type === "sow" && transition === "sown") {
      const sowMethod = meta.sow_method || "outdoors";
      const newStatus = sowMethod === "indoors" ? "sown_indoors" : "sown_outdoors";
      await supabaseService.from("crop_instances")
        .update({ status: newStatus, sown_date: today, updated_at: completedAt })
        .eq("id", data.crop_instance_id).eq("user_id", req.user.id);
      await runRuleEngine(req.user.id);

    } else if (data.task_type === "sow" && !transition) {
      // Fallback: sow task completed but no status_transition meta — still set sown_date
      const sowMethod = meta.sow_method || "outdoors";
      const newStatus = sowMethod === "indoors" ? "sown_indoors" : "sown_outdoors";
      await supabaseService.from("crop_instances")
        .update({ status: newStatus, sown_date: today, updated_at: completedAt })
        .eq("id", data.crop_instance_id).eq("user_id", req.user.id)
        .is("sown_date", null); // only update if not already set
      await runRuleEngine(req.user.id);

    } else if (data.task_type === "transplant" && transition === "transplanted") {
      await supabaseService.from("crop_instances")
        .update({ status: "transplanted", transplant_date: today, updated_at: completedAt })
        .eq("id", data.crop_instance_id).eq("user_id", req.user.id);
      await runRuleEngine(req.user.id);
    }
  }

  res.json(data);
});

app.post("/tasks/:id/uncomplete", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("tasks")
    .update({ completed_at: null })
    .eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });

  // Reverse last_fed_at if it was a feed task — set back to null
  if (data.task_type === "feed" && data.crop_instance_id) {
    await supabaseService.from("crop_instances")
      .update({ last_fed_at: null })
      .eq("id", data.crop_instance_id).eq("user_id", req.user.id);
  }
  res.json(data);
});

app.post("/tasks/:id/snooze", requireAuth,
  [body("days").isInt({ min: 1, max: 14 })],
  async (req, res) => {
    if (!validate(req, res)) return;
    const snoozeDate = new Date(Date.now() + req.body.days * 86400000).toISOString().split("T")[0];
    const { data, error } = await req.db.from("tasks")
      .update({ snoozed_until: snoozeDate, due_date: snoozeDate })
      .eq("id", req.params.id).eq("user_id", req.user.id).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.json(data);
  }
);

app.post("/tasks", requireAuth,
  [
    body("action").trim().notEmpty(),
    body("task_type").isIn(["feed","water","sow","transplant","harvest","protect","monitor","prune","thin","other"]),
    body("due_date").isISO8601(),
  ],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { action, task_type, urgency, due_date, crop_instance_id, area_id } = req.body;
    const { data, error } = await req.db.from("tasks").insert({
      user_id: req.user.id, action, task_type,
      urgency: urgency || "low", due_date,
      crop_instance_id: crop_instance_id || null,
      area_id: area_id || null,
      source: "manual",
    }).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);


// =============================================================================
// USER FEEDS
// Users register feeds they own. Claude enriches them with dosage/compatibility
// data. Rule engine uses them to generate personalised feeding tasks.
// =============================================================================

async function enrichFeed(feedId, brand, productName) {
  const db = supabaseService;
  try {
    const prompt = `You are a horticultural expert for UK home growers and allotment holders.
A user has registered a plant feed they own:
- Brand: "${brand || "unknown"}"
- Product name: "${productName}"

Your task: identify this feed product and return accurate UK growing data.

Respond ONLY with a JSON object — no markdown, no explanation:
{
  "valid": true,
  "product_name": "corrected product name",
  "brand": "corrected brand name or null",
  "form": "one of: liquid, granular, powder, pellet",
  "feed_type": "one of: high_potash, balanced, high_nitrogen, low_nitrogen, specialist_tomato, specialist_rose, seaweed, organic_general",
  "npk": "e.g. 4-4-4 or null if unknown",
  "dilution_ml_per_litre": 10,
  "frequency_days": 14,
  "suitable_crop_types": ["fruiting", "brassica", "root", "allium", "salad", "herb", "perennial", "fruit"],
  "application_method": "one of: drench, foliar, broadcast, base",
  "notes": "brief usage notes for UK home growers"
}

suitable_crop_types should list ALL crop categories this feed is appropriate for.
If the product is not a real plant feed, return: { "valid": false }`;

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 800,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const raw = await response.json();
    const text = raw.content?.[0]?.text || "";
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error("No JSON in response");
    const parsed = JSON.parse(jsonMatch[0]);

    if (!parsed.valid) {
      console.log(`[FeedEnrich] Invalid feed: ${productName}`);
      return;
    }

    await db.from("user_feeds").update({
      brand:                 parsed.brand || brand || null,
      product_name:          parsed.product_name || productName,
      form:                  parsed.form || "liquid",
      feed_type:             parsed.feed_type,
      npk:                   parsed.npk || null,
      dilution_ml_per_litre: parsed.dilution_ml_per_litre || null,
      frequency_days:        parsed.frequency_days || null,
      suitable_crop_types:   parsed.suitable_crop_types || [],
      application_method:    parsed.application_method || "drench",
      notes:                 parsed.notes || null,
      enriched:              true,
      updated_at:            new Date().toISOString(),
    }).eq("id", feedId);

    console.log(`[FeedEnrich] Enriched feed "${productName}" (${feedId})`);
  } catch (err) {
    console.error("[FeedEnrich] Error:", err.message);
  }
}

// GET /feed-catalog — return the full product catalog for dropdowns
app.get("/feed-catalog", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("feed_catalog")
    .select("brand, product_name, form, feed_type, npk, dilution_ml_per_litre, frequency_days, suitable_crop_types, application_method, notes")
    .eq("active", true)
    .order("brand").order("product_name");
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// GET /feeds — list user's feeds
app.get("/feeds", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("user_feeds")
    .select("*")
    .eq("user_id", req.user.id)
    .eq("active", true)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// POST /feeds — add a new feed
app.post("/feeds", requireAuth,
  [body("product_name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { brand, product_name, form, notes, feed_type, npk,
            dilution_ml_per_litre, frequency_days, suitable_crop_types,
            application_method, pre_enriched, barcode } = req.body;

    // Save barcode against feed_catalog entry for instant future lookups
    if (barcode && brand && product_name) {
      const { data: catEntry } = await req.db.from("feed_catalog")
        .select("id").eq("brand", brand).ilike("product_name", product_name).maybeSingle();
      if (catEntry?.id) {
        await req.db.from("feed_catalog").update({ barcode }).eq("id", catEntry.id);
      }
    }

    const { data, error } = await req.db.from("user_feeds").insert({
      user_id:               req.user.id,
      brand:                 brand || null,
      product_name,
      form:                  form || "liquid",
      notes:                 notes || null,
      feed_type:             feed_type || "balanced",
      npk:                   npk || null,
      dilution_ml_per_litre: dilution_ml_per_litre || null,
      frequency_days:        frequency_days || null,
      suitable_crop_types:   suitable_crop_types || [],
      application_method:    application_method || "drench",
      enriched:              pre_enriched ? true : false,
    }).select().single();

    if (error) return res.status(500).json({ error: error.message });

    // Only enrich if not already known
    if (!pre_enriched) enrichFeed(data.id, brand, product_name);

    res.status(201).json({ ...data, enriching: !pre_enriched });
  }
);

// DELETE /feeds/:id — remove a feed
app.delete("/feeds/:id", requireAuth, async (req, res) => {
  const { error } = await req.db.from("user_feeds")
    .update({ active: false, updated_at: new Date().toISOString() })
    .eq("id", req.params.id)
    .eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});




// =============================================================================
// TIME AWAY — BLOCKED PERIODS
// Lets users mark date ranges as unavailable. Tasks falling in those ranges
// are adjusted (moved earlier/later/at_risk) via the adjustment service.
// Canonical task due_dates are never mutated — overlays only.
// =============================================================================

app.get("/blocked-periods", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("blocked_periods")
    .select("*")
    .eq("user_id", req.user.id)
    .eq("status", "active")
    .order("start_date", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

app.post("/blocked-periods", requireAuth, async (req, res) => {
  const { start_date, end_date, label, note } = req.body;
  if (!start_date || !end_date)
    return res.status(400).json({ error: "start_date and end_date required" });
  if (end_date < start_date)
    return res.status(400).json({ error: "end_date must be on or after start_date" });

  const durationDays = Math.round((new Date(end_date) - new Date(start_date)) / 86400000);
  if (durationDays > 90)
    return res.status(400).json({ error: "Blocked period cannot exceed 90 days" });

  const { data: bp, error } = await req.db.from("blocked_periods").insert({
    user_id:    req.user.id,
    start_date,
    end_date,
    label:      label || null,
    note:       note  || null,
    status:     "active",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }).select().single();

  if (error) return res.status(500).json({ error: error.message });

  // Apply adjustments immediately
  const summary = await applyBlockedPeriodAdjustments(req.db, req.user.id, bp.id);

  res.status(201).json({ blockedPeriod: bp, summary });
});

app.patch("/blocked-periods/:id", requireAuth, async (req, res) => {
  const { start_date, end_date, label, note } = req.body;

  if (start_date && end_date && end_date < start_date)
    return res.status(400).json({ error: "end_date must be on or after start_date" });

  const updates = { updated_at: new Date().toISOString() };
  if (start_date) updates.start_date = start_date;
  if (end_date)   updates.end_date   = end_date;
  if (label !== undefined) updates.label = label;
  if (note  !== undefined) updates.note  = note;

  const { data: bp, error } = await req.db.from("blocked_periods")
    .update(updates)
    .eq("id", req.params.id)
    .eq("user_id", req.user.id)
    .eq("status", "active")
    .select().single();

  if (error || !bp) return res.status(404).json({ error: "Not found" });

  // Recompute adjustments with new dates
  const summary = await applyBlockedPeriodAdjustments(req.db, req.user.id, bp.id);

  res.json({ blockedPeriod: bp, summary });
});

app.delete("/blocked-periods/:id", requireAuth, async (req, res) => {
  // Verify ownership
  const { data: bp } = await req.db.from("blocked_periods")
    .select("id")
    .eq("id", req.params.id)
    .eq("user_id", req.user.id)
    .eq("status", "active")
    .single();

  if (!bp) return res.status(404).json({ error: "Not found" });

  // Remove all adjustments created by this blocked period
  await req.db.from("task_adjustments")
    .delete()
    .eq("blocked_period_id", req.params.id)
    .eq("user_id", req.user.id);

  // Soft delete the period
  await req.db.from("blocked_periods")
    .update({ status: "deleted", updated_at: new Date().toISOString() })
    .eq("id", req.params.id)
    .eq("user_id", req.user.id);

  res.status(204).send();
});

// GET /blocked-periods/:id/adjustments — summary of affected tasks for a period
app.get("/blocked-periods/:id/adjustments", requireAuth, async (req, res) => {
  const { data: bp } = await req.db.from("blocked_periods")
    .select("*")
    .eq("id", req.params.id)
    .eq("user_id", req.user.id)
    .single();

  if (!bp) return res.status(404).json({ error: "Not found" });

  const { data: adjustments } = await req.db.from("task_adjustments")
    .select("*, task:task_id(id, action, task_type, due_date, crop:crop_instance_id(name, variety))")
    .eq("blocked_period_id", req.params.id)
    .eq("user_id", req.user.id)
    .order("adjustment_type", { ascending: true });

  const grouped = {
    moved_earlier: (adjustments || []).filter(a => a.adjustment_type === "moved_earlier"),
    moved_later:   (adjustments || []).filter(a => a.adjustment_type === "moved_later"),
    at_risk:       (adjustments || []).filter(a => a.adjustment_type === "at_risk"),
  };

  res.json({
    blockedPeriod: bp,
    summary: {
      total:        (adjustments || []).length,
      movedEarlier: grouped.moved_earlier.length,
      movedLater:   grouped.moved_later.length,
      atRisk:       grouped.at_risk.length,
    },
    grouped,
  });
});

// =============================================================================
// AREA OPTIMISER
// AI-powered suggestions for every area — empty or populated.
// ── Boost This Area — feature usage tracking ──────────────────────────────────
// Free users get 3 lifetime uses tracked server-side.
// Pro users and Mark bypass entirely.
// GET  /features/boost-status — returns current usage and whether they can use
// POST /features/boost-use    — increments counter (only if allowed)

const BOOST_LIMIT = 3;

app.get("/features/boost-status", requireAuth, async (req, res) => {
  try {
    const { data: profile, error } = await req.db.from("profiles")
      .select("boost_uses, plan").eq("id", req.user.id).single();
    if (error) throw error;

    const uses   = profile?.boost_uses || 0;
    const isPro  = profile?.plan === "pro";
    const canUse = isPro || uses < BOOST_LIMIT;

    res.json({ uses, limit: BOOST_LIMIT, is_pro: isPro, can_use: canUse });
  } catch (err) {
    captureError("BoostStatus", err);
    res.status(500).json({ error: err.message });
  }
});

app.post("/features/boost-use", requireAuth, async (req, res) => {
  try {
    const { data: profile, error } = await req.db.from("profiles")
      .select("boost_uses, plan").eq("id", req.user.id).single();
    if (error) throw error;

    const isPro = profile?.plan === "pro";
    const uses  = profile?.boost_uses || 0;

    // Pro users don't consume the counter
    if (isPro) return res.json({ uses, limit: BOOST_LIMIT, is_pro: true, can_use: true });

    // Block if already at limit
    if (uses >= BOOST_LIMIT) {
      return res.status(403).json({ error: "boost_limit_reached", uses, limit: BOOST_LIMIT, can_use: false });
    }

    // Increment
    const newUses = uses + 1;
    await req.db.from("profiles").update({ boost_uses: newUses }).eq("id", req.user.id);

    res.json({ uses: newUses, limit: BOOST_LIMIT, is_pro: false, can_use: newUses < BOOST_LIMIT });
  } catch (err) {
    captureError("BoostUse", err);
    res.status(500).json({ error: err.message });
  }
});

// Empty areas: "what to plant here". Populated areas: "what to add / boost with".
// Cache is invalidated whenever crops are added, deleted or harvested in the area.
// =============================================================================

app.get("/areas/:id/suggestions", requireAuth, async (req, res) => {
  const { data: area } = await supabaseService.from("growing_areas")
    .select("id, location_id, locations(user_id)")
    .eq("id", req.params.id).single();
  if (!area || area.locations?.user_id !== req.user.id)
    return res.status(403).json({ error: "Not authorised" });

  const { data } = await supabaseService.from("planting_suggestions")
    .select("*").eq("area_id", req.params.id).single();

  res.json(data || null);
});

app.post("/areas/:id/suggestions/generate", requireAuth, async (req, res) => {
  const db = req.db;

  // Verify ownership + get area details
  const { data: area } = await db.from("growing_areas")
    .select("id, name, type, location_id, locations(user_id, postcode)")
    .eq("id", req.params.id).single();
  if (!area || area.locations?.user_id !== req.user.id)
    return res.status(403).json({ error: "Not authorised" });

  // Get active crops in THIS area
  const { data: areaCrops } = await db.from("crop_instances")
    .select("name, variety, status, sown_date")
    .eq("area_id", req.params.id)
    .eq("active", true)
    .not("status", "eq", "harvested")
    .not("status", "eq", "planned");

  const isEmpty = !areaCrops || areaCrops.length === 0;

  // Check cache — only use if the area state matches what was cached
  // We store a crop_fingerprint so we can detect staleness
  const cropFingerprint = (areaCrops || [])
    .map(c => `${c.name}|${c.variety || ""}`)
    .sort()
    .join(",");

  const { data: existing } = await db.from("planting_suggestions")
    .select("*").eq("area_id", req.params.id).single();

  if (existing && existing.crop_fingerprint === cropFingerprint) {
    return res.json({
      suggestions:   existing.suggestions,
      generated_at:  existing.generated_at,
      summary:       existing.summary || null,
      is_empty_area: existing.is_empty_area || false,
    });
  }

  // Get crop history for this area (rotation awareness)
  const { data: history } = await db.from("crop_instances")
    .select("name, variety")
    .eq("area_id", req.params.id)
    .eq("active", false)
    .order("created_at", { ascending: false })
    .limit(6);

  // Get ALL active crops across the whole location (location-aware signal)
  const { data: locationCrops } = await db.from("crop_instances")
    .select("name, variety, area_id")
    .eq("active", true)
    .not("status", "eq", "harvested")
    .not("status", "eq", "planned")
    .in("area_id",
      (await db.from("growing_areas")
        .select("id")
        .eq("location_id", area.location_id)
      ).data?.map(a => a.id) || []
    );

  const month        = new Date().toLocaleString("en-GB", { month: "long" });
  const areaType     = area.type?.replace(/_/g, " ") || "growing area";
  const postcode     = area.locations?.postcode || "UK";
  const areaNameStr  = area.name || areaType;

  const areaCropsStr = areaCrops?.length
    ? areaCrops.map(c => `${c.name}${c.variety ? " (" + c.variety + ")" : ""}${c.status ? " [" + c.status + "]" : ""}`).join(", ")
    : "nothing currently in this area";

  const historyStr = history?.length
    ? history.map(c => `${c.name}${c.variety ? " (" + c.variety + ")" : ""}`).join(", ")
    : "nothing previously recorded";

  // Deduplicate location crops, exclude this area's own crops
  const locationCropsElsewhere = (locationCrops || [])
    .filter(c => c.area_id !== req.params.id);
  const locationStr = locationCropsElsewhere.length
    ? [...new Set(locationCropsElsewhere.map(c => c.name))].join(", ")
    : "nothing else in this location";

  // Check if any beneficial flowers/herbs exist in the wider location
  const beneficialKeywords = ["marigold", "nasturtium", "borage", "calendula", "lavender", "phacelia"];
  const hasBeneficials = locationCropsElsewhere.some(c =>
    beneficialKeywords.some(b => c.name.toLowerCase().includes(b))
  );

  const emptyPrompt = `You are a UK horticultural expert advising a home grower or allotment holder.

This area is currently empty. Suggest what they should plant here now.

Area details:
- Name: ${areaNameStr}
- Type: ${areaType}
- Location postcode: ${postcode}
- Current month: ${month}
- Previously grown here: ${historyStr}
- Other crops growing elsewhere in the same garden: ${locationStr}
- Beneficial flowers/herbs already in garden: ${hasBeneficials ? "yes" : "none detected"}

Rules:
1. Seasonality — only suggest crops that can realistically be sown or planted outdoors (or started indoors if appropriate) in ${month} in the UK
2. Rotation — avoid the same crop family as previous crops in this area
3. Suggest specific named varieties, not just species names
4. Since no beneficial flowers/herbs exist in the garden${hasBeneficials ? " elsewhere" : ""}, consider including one as a companion suggestion
5. Bias toward high-confidence crops — fast-growing, beginner-friendly options that work in most ${areaType} setups

Return a JSON object with:
- "summary": one sentence starting with "This area is empty —" explaining the best starting move
- "suggestions": array of exactly 3 items

Suggestion types and schema:
[
  {
    "type": "crop",
    "confidence": "high" | "medium",
    "crop": "Crop name",
    "variety": "Specific variety name",
    "reason": "One sentence why this is the best choice for this area right now",
    "sow_note": "When and how to sow/plant in one sentence",
    "placement_note": "Soft placement guidance e.g. 'sow in rows across the bed' or null",
    "companion_note": "Companion benefit if relevant or null",
    "benefit_tags": ["quick crop", "easy to grow"] — pick from: quick crop, easy to grow, succession candidate, good for ${areaType}, frost hardy, good for pollinators, nitrogen fixing
  },
  {
    "type": "companion",
    "confidence": "high",
    "crop": "Flower or herb name",
    "variety": "Specific variety or null",
    "reason": "One sentence benefit",
    "sow_note": "When and how in one sentence",
    "placement_note": "e.g. 'sow around the edges' or null",
    "companion_note": "What it helps with",
    "benefit_tags": ["good for pollinators"] — pick relevant tags
  },
  { ... third suggestion as crop or companion ... }
]

Respond ONLY with a JSON object — no markdown, no explanation:
{ "summary": "...", "suggestions": [...] }`;

  const populatedPrompt = `You are a UK horticultural expert advising a home grower or allotment holder.

This area already has crops growing. Suggest what they could add to boost it.

Area details:
- Name: ${areaNameStr}
- Type: ${areaType}
- Location postcode: ${postcode}
- Current month: ${month}
- Crops currently in THIS area: ${areaCropsStr}
- Previously grown here: ${historyStr}
- Other crops growing elsewhere in the same garden: ${locationStr}
- Beneficial flowers/herbs already in garden: ${hasBeneficials ? "yes" : "none detected"}

Scoring priorities (apply in this order):
1. Direct companion benefit to crops already in this area (highest weight)
2. Seasonal fit — only suggest things that can realistically be added in ${month} in the UK
3. Space confidence — prefer HIGH confidence options (compact, low-space crops like lettuce, radish, spring onion, marigold) over large sprawling crops
4. Location benefit — if no beneficial flowers/herbs exist in the garden, prioritise one as a companion suggestion
5. Avoid suggesting crops already well-represented in the wider garden

Because exact free space in the area is unknown, only suggest HIGH or MEDIUM confidence additions.
Do NOT suggest large space-hungry crops (courgette, cabbage, squash) unless the area appears very lightly planted.

Return a JSON object with:
- "summary": one sentence starting with "Best next step:" or "Good option for this area:" explaining the top recommendation in plain English
- "suggestions": array of 1 to 3 items (only include items with genuine value — do not force 3 if only 1 or 2 are strong)

Suggestion schema:
[
  {
    "type": "primary_crop" | "companion" | "beneficial",
    "confidence": "high" | "medium",
    "crop": "Crop name",
    "variety": "Specific variety or null",
    "reason": "One sentence — why this works well with what's already in this area",
    "sow_note": "When and how to add this in one sentence",
    "placement_note": "Soft placement e.g. 'sow between rows', 'around the edges', 'in gaps if available' — never claim guaranteed space",
    "companion_note": "Specific benefit to named crops in this area e.g. 'deters aphids from the potatoes' or null",
    "benefit_tags": [] — pick from: quick crop, supports pollinators, deters pests, nitrogen fixing, improves soil, easy to grow, succession candidate, compact, good for ${areaType}
  }
]

Respond ONLY with a JSON object — no markdown, no explanation:
{ "summary": "...", "suggestions": [...] }`;

  try {
    const prompt = isEmpty ? emptyPrompt : populatedPrompt;

    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1400,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const raw  = await response.json();
    const text = raw.content?.[0]?.text || "";
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) throw new Error("No JSON object in response");
    const parsed = JSON.parse(match[0]);
    const suggestions = parsed.suggestions || [];
    const summary     = parsed.summary || null;

    await db.from("planting_suggestions").upsert({
      area_id:          req.params.id,
      suggestions,
      summary,
      is_empty_area:    isEmpty,
      crop_fingerprint: cropFingerprint,
      generated_at:     new Date().toISOString(),
    }, { onConflict: "area_id" });

    res.json({
      suggestions,
      summary,
      is_empty_area: isEmpty,
      generated_at:  new Date().toISOString(),
    });
  } catch (e) {
    console.error("[Area Optimiser] Error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// Clear suggestions cache for an area — call whenever crops change in that area
async function clearSuggestions(areaId, db) {
  await db.from("planting_suggestions").delete().eq("area_id", areaId);
}

// =============================================================================
// CROP PHOTOS — growth diary
// =============================================================================

// GET /crops/:id/photos
app.get("/crops/:id/photos", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("crop_photos")
    .select("*")
    .eq("crop_instance_id", req.params.id)
    .eq("user_id", req.user.id)
    .order("taken_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// POST /crops/:id/photos
app.post("/crops/:id/photos", requireAuth, async (req, res) => {
  const { base64, caption } = req.body;
  if (!base64) return res.status(400).json({ error: "base64 image required" });

  // Verify crop belongs to user
  const { data: crop } = await req.db.from("crop_instances")
    .select("id").eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (!crop) return res.status(404).json({ error: "Crop not found" });

  try {
    const buffer   = Buffer.from(base64, "base64");
    const filename = `${req.user.id}/${req.params.id}/${Date.now()}.jpg`;
    const { error: uploadErr } = await supabaseService.storage
      .from("crop-photos").upload(filename, buffer, { contentType: "image/jpeg", upsert: false });
    if (uploadErr) throw new Error(uploadErr.message);

    const { data: { publicUrl } } = supabaseService.storage
      .from("crop-photos").getPublicUrl(filename);

    const { data, error } = await req.db.from("crop_photos").insert({
      crop_instance_id: req.params.id,
      user_id:          req.user.id,
      photo_url:        publicUrl,
      caption:          caption?.trim() || null,
    }).select().single();

    if (error) throw new Error(error.message);
    res.status(201).json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /crops/:id/photos/:photoId
app.delete("/crops/:id/photos/:photoId", requireAuth, async (req, res) => {
  const { error } = await req.db.from("crop_photos")
    .delete()
    .eq("id", req.params.photoId)
    .eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ deleted: true });
});

// =============================================================================
// BARCODE LOOKUP
// Checks Open Food Facts + UPC Item DB, then falls back to Claude enrichment
// =============================================================================

// POST /barcode/scan-image — Claude Vision reads barcode photo
app.post("/barcode/scan-image", requireAuth, async (req, res) => {
  const { image, mode = "crop" } = req.body;
  if (!image) return res.status(400).json({ error: "image required" });

  try {
    const prompt = mode === "crop"
      ? `This is a photo of a seed packet. Identify the crop name, variety, and brand from the text visible on the packet. Respond ONLY with JSON: {"found":true,"name":"Carrot","variety":"Nantes 2","brand":"Thompson & Morgan","description":"Short growing note","sow_window":"Mar - Jun","is_seed":true} If you cannot identify a crop: {"found":false}`
      : `This is a photo of a garden feed or fertiliser product. Identify the product name, brand, NPK values, and form from the text on the packaging. Respond ONLY with JSON: {"found":true,"name":"Tomorite","brand":"Levington","product_name":"Tomorite Concentrated Tomato Food","form":"liquid","feed_type":"tomato","npk":"4-3-8","description":"Short description","is_feed":true} If you cannot identify it: {"found":false}`;

    const r = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 500,
        messages: [{ role: "user", content: [
          { type: "image", source: { type: "base64", media_type: "image/jpeg", data: image } },
          { type: "text", text: prompt },
        ]}],
      }),
    });

    const raw  = await r.json();
    const text = raw.content?.[0]?.text || "";
    const m    = text.match(/\{[\s\S]*\}/);
    if (!m) return res.json({ found: false });
    const parsed = JSON.parse(m[0]);
    if (!parsed.found) return res.json({ found: false });

    // Store barcode against DB entry if identified
    if (parsed.barcode) {
      if (mode === "crop" && parsed.name) {
        const { data: cropDef } = await supabaseService.from("crop_definitions").select("id").ilike("name", parsed.name).maybeSingle();
        if (cropDef?.id) { await supabaseService.from("crop_definitions").update({ barcode: parsed.barcode }).eq("id", cropDef.id); parsed.crop_def_id = cropDef.id; }
      } else if (mode === "feed" && parsed.product_name) {
        const { data: feedEntry } = await supabaseService.from("feed_catalog").select("id").ilike("product_name", parsed.product_name).maybeSingle();
        if (feedEntry?.id) await supabaseService.from("feed_catalog").update({ barcode: parsed.barcode }).eq("id", feedEntry.id);
      }
    }

    res.json(parsed);
  } catch (e) {
    console.error("[ScanImage]", e.message);
    res.json({ found: false });
  }
});

app.get("/barcode/:code", requireAuth, async (req, res) => {
  const { code } = req.params;
  const mode = req.query.mode || "crop"; // "crop" or "feed"

  try {
    // 1. Check our own database first — crop_definitions for crops, feed_catalog for feeds
    if (mode === "crop") {
      const { data: existing } = await req.db
        .from("crop_definitions")
        .select("id, name, description, sow_window_start, sow_window_end")
        .eq("barcode", code)
        .single();
      if (existing) {
        const monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
        return res.json({
          found: true,
          source: "vercro",
          crop_def_id: existing.id,
          name: existing.name,
          description: existing.description,
          sow_window: existing.sow_window_start
            ? `${monthNames[existing.sow_window_start-1]} – ${monthNames[existing.sow_window_end-1]}`
            : null,
        });
      }
    } else {
      const { data: existing } = await req.db
        .from("feed_catalog")
        .select("*")
        .eq("barcode", code)
        .single();
      if (existing) return res.json({ found: true, source: "vercro", ...existing });
    }

    // 2. Try Open Food Facts (good for garden products in UK)
    const offRes = await fetch(`https://world.openfoodfacts.org/api/v0/product/${code}.json`);
    const offData = await offRes.json();
    if (offData.status === 1 && offData.product) {
      const p = offData.product;
      const name = p.product_name_en || p.product_name || p.generic_name || null;
      if (name) {
        // Use Claude to interpret whether it's a seed packet or feed
        const profile = await enrichBarcodeWithClaude(name, p.brands || null, mode, code);
        return res.json({ found: true, source: "openfoodfacts", ...profile });
      }
    }

    // 3. Try UPC Item DB (free tier)
    const upcRes = await fetch(`https://api.upcitemdb.com/prod/trial/lookup?upc=${code}`);
    const upcData = await upcRes.json();
    if (upcData.code === "OK" && upcData.items?.length > 0) {
      const item = upcData.items[0];
      const name = item.title || item.brand || null;
      if (name) {
        const profile = await enrichBarcodeWithClaude(name, item.brand || null, mode, code);
        return res.json({ found: true, source: "upcitemdb", ...profile });
      }
    }

    // 4. Not found in any database
    res.json({ found: false, barcode: code });

  } catch (e) {
    console.error("[Barcode]", e.message);
    res.json({ found: false, barcode: code, error: e.message });
  }
});

async function enrichBarcodeWithClaude(productName, brand, mode, barcode) {
  const prompt = mode === "crop"
    ? `A UK gardener scanned a barcode. Product: "${productName}"${brand ? ` by ${brand}` : ""}. 
Is this a seed packet? If yes, identify the crop and variety. If it is NOT a seed packet, say so.
Respond ONLY with JSON: {"is_seed":true,"name":"Carrot","variety":"Nantes 2","description":"Brief growing note","sow_window":"Mar - Jun","brand":"${brand||""}"}`
    : `A UK gardener scanned a barcode. Product: "${productName}"${brand ? ` by ${brand}` : ""}.
Is this a garden feed or fertiliser? If yes, identify it. If NOT, say so.
Respond ONLY with JSON: {"is_feed":true,"name":"Product name","brand":"${brand||""}","product_name":"${productName}","form":"liquid","feed_type":"tomato","npk":"4-3-8","description":"Brief description"}`;

  const r = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": process.env.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01" },
    body: JSON.stringify({ model: "claude-sonnet-4-20250514", max_tokens: 400, messages: [{ role: "user", content: prompt }] }),
  });
  const raw  = await r.json();
  const text = raw.content?.[0]?.text || "";
  const m    = text.match(/\{[\s\S]*\}/);
  if (!m) return { name: productName, brand };
  const parsed = JSON.parse(m[0]);
  // Store barcode against crop_def / feed_catalog for instant future lookups
  if (mode === "crop" && parsed.is_seed && parsed.name) {
    const { data: cropDef } = await supabaseService
      .from("crop_definitions").select("id").ilike("name", parsed.name).maybeSingle();
    if (cropDef?.id) {
      await supabaseService.from("crop_definitions").update({ barcode: code }).eq("id", cropDef.id);
      parsed.crop_def_id = cropDef.id;
    }
  } else if (mode === "feed" && parsed.is_feed && parsed.product_name) {
    const { data: feedEntry } = await supabaseService
      .from("feed_catalog").select("id").ilike("product_name", parsed.product_name).maybeSingle();
    if (feedEntry?.id) {
      await supabaseService.from("feed_catalog").update({ barcode: code }).eq("id", feedEntry.id);
    }
  }
  return { ...parsed, barcode };
}

// =============================================================================
// FEEDBACK
// =============================================================================

// POST /feedback
app.post("/feedback", requireAuth, async (req, res) => {
  const { category, message, rating } = req.body;
  if (!category || !message?.trim()) return res.status(400).json({ error: "category and message required" });
  const { data, error } = await req.db.from("feedback").insert({
    user_id:  req.user.id,
    category: category,
    message:  message.trim(),
    rating:   rating || null,
  }).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.status(201).json(data);
});

// GET /admin/metrics — full founder dashboard
app.get("/admin/metrics", requireAuth, requireMetricsAccess, async (req, res) => {
  try {
    const db = supabaseService; // service role for cross-table queries
    const now = new Date();
    const day7ago  = new Date(now - 7  * 86400000).toISOString();
    const day28ago = new Date(now - 28 * 86400000).toISOString();

    // Get all demo user IDs to exclude from every metric
    const { data: demoProfiles } = await db.from("profiles").select("id").eq("is_demo", true);
    const demoUserIds = (demoProfiles || []).map(p => p.id);
    const demoExclude = demoUserIds.length > 0 ? `(${demoUserIds.join(",")})` : `('00000000-0000-0000-0000-000000000000')`;

    // User growth — auth users (everyone) vs profiles (completed onboarding)
    const authUsers     = await getAllAuthUsers();
    const realAuthUsers = authUsers.filter(u => !demoUserIds.includes(u.id));
    const totalSignups    = realAuthUsers.length;
    const newSignupsWeek  = realAuthUsers.filter(u => new Date(u.created_at) >= new Date(day7ago)).length;
    const newSignupsLastWeek = realAuthUsers.filter(u => new Date(u.created_at) >= new Date(day28ago) && new Date(u.created_at) < new Date(day7ago)).length;

    // ── Activity signal — fetch all genuine user-action events ────────────────
    // These tables are only ever written to by real user actions (not the rule
    // engine or any background process). We union them to get a clean activity
    // signal for DAU/WAU/MAU and cohort retention.
    //
    // crop_instances.created_at — user added a crop (day 2+ only to exclude
    //   onboarding auto-creates on day 1)
    // tasks.completed_at        — user completed a task
    // harvest_log.harvested_at  — user logged a harvest
    // manual_activity_logs.performed_at — user logged any manual activity
    // crop_observations.created_at — user submitted a plant check / observation
    // crop_photos.created_at    — user added a photo
    // user_feeds.created_at     — user added a feed entry
    //
    // Note: locations and growing_areas have no created_at column — excluded.

    const [
      // Activated (completed onboarding = have a profile)
      { count: totalActivated },

      // Garden usage counts
      { count: totalLocations },
      { count: totalAreas },
      { count: totalCrops },

      // Crop lifecycle
      { count: cropsSown },
      { count: cropsHarvested },
      { count: harvestLogs },

      // Task engine
      { count: tasksGenerated },
      { count: tasksCompleted },

      // Feeds & photos
      { count: totalFeeds },
      { count: totalPhotos },

      // Dataset
      { count: totalVarieties },
      { count: yieldDataPoints },

      // Email sequences
      { count: emailWaitlistInvites },
      { count: emailFeedbackDay3 },
      { count: emailFeedbackDay7 },
      { count: emailReengageDay14 },
      { count: emailReengageDay30 },
      { count: emailDailyFallback },

      // Push tokens
      { count: pushTokens },

      // Feedback ratings
      { data: feedbackRatings },

      // Activity signals — all user-initiated actions with timestamps
      // Used for DAU / WAU / MAU / retention. Pulled wide (30 days) so we
      // can filter down per metric in JS rather than making 6+ separate calls.

    ] = await Promise.all([
      db.from("profiles").select("*", { count: "exact", head: true }).eq("is_demo", false),

      db.from("locations").select("*", { count: "exact", head: true }).not("user_id", "in", demoExclude),
      db.from("growing_areas").select("*, locations!inner(user_id)", { count: "exact", head: true }).not("locations.user_id", "in", demoExclude),
      db.from("crop_instances").select("*", { count: "exact", head: true }).not("user_id", "in", demoExclude),

      db.from("crop_instances").select("*", { count: "exact", head: true }).not("sown_date", "is", null).not("user_id", "in", demoExclude),
      db.from("crop_instances").select("*", { count: "exact", head: true }).eq("status", "harvested").not("user_id", "in", demoExclude),
      db.from("harvest_log").select("*", { count: "exact", head: true }).not("user_id", "in", demoExclude),

      db.from("tasks").select("*", { count: "exact", head: true }).is("completed_at", null).not("status", "eq", "expired").not("user_id", "in", demoExclude),
      db.from("tasks").select("*", { count: "exact", head: true }).not("completed_at", "is", null).not("user_id", "in", demoExclude),

      db.from("user_feeds").select("*", { count: "exact", head: true }).not("user_id", "in", demoExclude),
      db.from("crop_photos").select("*", { count: "exact", head: true }).not("user_id", "in", demoExclude),

      db.from("varieties").select("*", { count: "exact", head: true }),
      db.from("harvest_log").select("*", { count: "exact", head: true }).not("quantity_value", "is", null).not("user_id", "in", demoExclude),

      // Email sequences
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "waitlist_invite").not("user_id", "in", demoExclude),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "feedback_day3").not("user_id", "in", demoExclude),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "feedback_day7").not("user_id", "in", demoExclude),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "reengage_day14").not("user_id", "in", demoExclude),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "reengage_day30").not("user_id", "in", demoExclude),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "daily_fallback").not("user_id", "in", demoExclude),

      // Push tokens
      db.from("device_push_tokens").select("*", { count: "exact", head: true }).eq("is_active", true).not("user_id", "in", demoExclude),

      // Feedback avg rating
      db.from("feedback").select("rating").not("rating", "is", null).not("user_id", "in", demoExclude),

    ]);

    // ── DAU / WAU / MAU / Retention — all via SQL RPC ────────────────────────
    // get_retention_metrics() computes everything in Postgres — no JS timestamp
    // comparison, no row limits, no format mismatches.
    const { data: retentionData, error: retentionError } = await supabaseService.rpc("get_retention_metrics");
    if (retentionError) console.error("[Metrics] retention RPC error:", retentionError.message);

    const rm  = retentionData || {};
    const dau = Number(rm.dau || 0);
    const wau = Number(rm.wau || 0);
    const mau = Number(rm.mau || 0);
    const d7RetentionRate  = rm.d7_rate  != null ? Number(rm.d7_rate)  : null;
    const d14RetentionRate = rm.d14_rate != null ? Number(rm.d14_rate) : null;
    const d21RetentionRate = rm.d21_rate != null ? Number(rm.d21_rate) : null;
    const d28RetentionRate = rm.d28_rate != null ? Number(rm.d28_rate) : null;
    const d7Retained  = Number(rm.d7_retained  || 0);
    const d28Retained = Number(rm.d28_retained || 0);
    const d7Cohort    = { length: Number(rm.d7_cohort  || 0) };
    const d28Cohort   = { length: Number(rm.d28_cohort || 0) };

    // ── Derived metrics ───────────────────────────────────────────────────────
    const activationRate    = totalSignups > 0 ? Math.round((totalActivated / totalSignups) * 100) : 0;
    const avgCropsPerUser   = totalActivated > 0 ? (totalCrops / totalActivated).toFixed(1) : 0;
    const tasksPending      = tasksGenerated || 0;
    const taskCompletionRate = (tasksPending + tasksCompleted) > 0
      ? Math.round((tasksCompleted / (tasksPending + tasksCompleted)) * 100) : 0;
    const wowGrowth         = newSignupsLastWeek > 0 ? Math.round(((newSignupsWeek - newSignupsLastWeek) / newSignupsLastWeek) * 100) : null;
    const avgFeedsPerUser   = totalActivated > 0 ? (totalFeeds / totalActivated).toFixed(1) : 0;

    res.json({
      // User growth
      totalSignups,
      totalActivated,
      newSignupsWeek,
      wowGrowth,
      activationRate,

      // Engagement — DAU/WAU/MAU based on genuine user-action signals
      dau,
      wau,
      mau,
      dauWauRatio: wau > 0 ? (dau / wau).toFixed(2) : null,
      wauMauRatio: mau > 0 ? (wau / mau).toFixed(2) : null,

      // Garden usage
      totalLocations,
      totalAreas,
      totalCrops,
      avgCropsPerUser,

      // Crop lifecycle
      cropsSown,
      cropsHarvested,
      harvestLogs,

      // Tasks
      tasksPending,
      tasksCompleted,
      taskCompletionRate,

      // Feeds & photos
      totalFeeds,
      avgFeedsPerUser,
      totalPhotos,

      // Retention — true cohort-window retention using user-action signals
      // D7:  did they interact in days 2–7 after their own signup date?
      // D28: did they interact in days 21–28 after their own signup date?
      d7Retention:  d7RetentionRate,
      d14Retention: d14RetentionRate,
      d21Retention: d21RetentionRate,
      d28Retention: d28RetentionRate,
      d7RetentionRaw:  { retained: d7Retained,  cohort: d7Cohort.length },
      d14RetentionRaw: { retained: Number(rm.d14_retained || 0), cohort: Number(rm.d14_cohort || 0) },
      d21RetentionRaw: { retained: Number(rm.d21_retained || 0), cohort: Number(rm.d21_cohort || 0) },
      d28RetentionRaw: { retained: d28Retained, cohort: d28Cohort.length },

      // Keep old field names as aliases so the frontend doesn't break
      week1Retention: d7RetentionRate,
      week4Retention: d28RetentionRate,

      // Dataset
      totalVarieties,
      yieldDataPoints,

      // Email
      emailWaitlistInvites: emailWaitlistInvites || 0,
      emailFeedbackDay3:    emailFeedbackDay3    || 0,
      emailFeedbackDay7:    emailFeedbackDay7    || 0,
      emailReengageDay14:   emailReengageDay14   || 0,
      emailReengageDay30:   emailReengageDay30   || 0,
      emailDailyFallback:   emailDailyFallback   || 0,

      // Push
      pushTokens: pushTokens || 0,
      pushOptIn:  totalActivated > 0 ? Math.round((pushTokens / totalActivated) * 100) : 0,

      // Feedback
      avgRating: feedbackRatings?.length > 0
        ? (feedbackRatings.reduce((s, f) => s + (f.rating || 0), 0) / feedbackRatings.length).toFixed(1)
        : null,
      totalFeedback: feedbackRatings?.length || 0,
    });
  } catch (e) {
    console.error("[Metrics]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// GET /admin/metrics/funnel — activation funnel, D1/D7 cohort retention,
// push vs no-push comparison, 14-day cohort table, post-fix health check
// =============================================================================
app.get("/admin/metrics/funnel", requireAuth, requireMetricsAccess, async (req, res) => {
  try {
    const db = supabaseService;
    const FIX_DATE = "2026-03-24T13:00:00.000Z";
    const now = new Date();

    const { data: demoProfiles } = await db.from("profiles").select("id").eq("is_demo", true);
    const demoUserIds = (demoProfiles || []).map(p => p.id);

    const authUsers = await getAllAuthUsers();
    const realAuthUsers = authUsers.filter(u => !demoUserIds.includes(u.id));
    const totalSignups = realAuthUsers.length;

    // Activated = have a profile
    const { data: profiles } = await db.from("profiles").select("id, created_at").eq("is_demo", false);
    const activatedIds = new Set((profiles || []).map(p => p.id));
    const totalActivated = activatedIds.size;

    // ── Fetch all task data once — used for funnel, retention, and cohort ──────
    // Scope to activated user IDs to avoid the 1000-row Supabase default limit
    const activatedUserIds = [...activatedIds].filter(id => !demoUserIds.includes(id));

    // Fetch all tasks for activated users in chunks of 200 to stay under limits
    let allTasks = [];
    for (let i = 0; i < activatedUserIds.length; i += 200) {
      const chunk = activatedUserIds.slice(i, i + 200);
      const { data } = await db.from("tasks").select("user_id, completed_at").in("user_id", chunk);
      allTasks = allTasks.concat(data || []);
    }

    // Fetch all active crops for activated users
    let allCrops = [];
    for (let i = 0; i < activatedUserIds.length; i += 200) {
      const chunk = activatedUserIds.slice(i, i + 200);
      const { data } = await db.from("crop_instances").select("user_id").eq("active", true).in("user_id", chunk);
      allCrops = allCrops.concat(data || []);
    }

    // Per-user task completion counts
    const taskCountMap = {};
    for (const t of allTasks) {
      if (t.completed_at) taskCountMap[t.user_id] = (taskCountMap[t.user_id] || 0) + 1;
    }

    // Funnel sets — scoped correctly
    const usersWithTasksSet    = new Set(allTasks.map(t => t.user_id));
    const usersWithCompletedSet = new Set(Object.keys(taskCountMap));
    const usersWithCropsSet    = new Set(allCrops.map(c => c.user_id));

    // ── D1 / D3 / D7 retention ───────────────────────────────────────────────
    const day1ago = new Date(now - 1 * 86400000).toISOString();
    const day3ago = new Date(now - 3 * 86400000).toISOString();
    const day7ago = new Date(now - 7 * 86400000).toISOString();

    const eligibleD1 = realAuthUsers.filter(u => new Date(u.created_at) <= new Date(day1ago));
    const eligibleD3 = realAuthUsers.filter(u => new Date(u.created_at) <= new Date(day3ago));
    const eligibleD7 = realAuthUsers.filter(u => new Date(u.created_at) <= new Date(day7ago));

    // Activity signal: completed a task in the retention window
    const d1ActiveSet = new Set(allTasks.filter(t => t.completed_at && t.completed_at >= day1ago).map(t => t.user_id));
    const d3ActiveSet = new Set(allTasks.filter(t => t.completed_at && t.completed_at >= day3ago).map(t => t.user_id));
    const d7ActiveSet = new Set(allTasks.filter(t => t.completed_at && t.completed_at >= day7ago).map(t => t.user_id));

    // Split into three groups: no tasks, exactly 1 task, 2+ tasks
    const noTask   = (arr) => arr.filter(u => !taskCountMap[u.id] || taskCountMap[u.id] === 0);
    const oneTask  = (arr) => arr.filter(u =>  taskCountMap[u.id] === 1);
    const twoPlus  = (arr) => arr.filter(u => (taskCountMap[u.id] || 0) >= 2);

    const retRate = (users, activeSet) => {
      if (!users.length) return null;
      return Math.round((users.filter(u => activeSet.has(u.id)).length / users.length) * 100);
    };

    const retGroup = (filterFn) => ({
      d1_eligible: filterFn(eligibleD1).length,
      d1_rate:     retRate(filterFn(eligibleD1), d1ActiveSet),
      d3_eligible: filterFn(eligibleD3).length,
      d3_rate:     retRate(filterFn(eligibleD3), d3ActiveSet),
      d7_eligible: filterFn(eligibleD7).length,
      d7_rate:     retRate(filterFn(eligibleD7), d7ActiveSet),
    });

    const retention = {
      no_task:  retGroup(noTask),
      one_task: retGroup(oneTask),
      two_plus: retGroup(twoPlus),
    };

    // ── Push vs no-push 7-day retention ──────────────────────────────────────
    const { data: pushUsers } = await db.from("device_push_tokens").select("user_id").eq("is_active", true).not("user_id", "in", `(${demoUserIds.join(",") || "null"})`);
    const pushUserSet    = new Set((pushUsers || []).map(r => r.user_id));
    const pushEligible   = eligibleD7.filter(u =>  pushUserSet.has(u.id));
    const noPushEligible = eligibleD7.filter(u => !pushUserSet.has(u.id));

    const pushRetention = {
      push: {
        eligible: pushEligible.length,
        rate:     retRate(pushEligible, d7ActiveSet),
      },
      no_push: {
        eligible: noPushEligible.length,
        rate:     retRate(noPushEligible, d7ActiveSet),
      },
    };

    // ── 14-day cohort table — reuse allTasks, latest date first ────────────────
    const allCompletions = allTasks.filter(t => t.completed_at);

    const cohortDays = [];
    // i=0 → today, i=13 → 13 days ago — so latest date is first in the array
    for (let i = 0; i <= 13; i++) {
      const dayStart = new Date(now);
      dayStart.setDate(dayStart.getDate() - i);
      dayStart.setHours(0, 0, 0, 0);
      const dayEnd = new Date(dayStart);
      dayEnd.setHours(23, 59, 59, 999);

      const cohortUsers = realAuthUsers.filter(u => {
        const d = new Date(u.created_at);
        return d >= dayStart && d <= dayEnd;
      });
      const cohortActivated = cohortUsers.filter(u => activatedIds.has(u.id));
      const cohortIds       = new Set(cohortUsers.map(u => u.id));
      const cohortCompleted = allCompletions.filter(c => cohortIds.has(c.user_id));
      const firstTaskSet    = new Set(cohortCompleted.map(c => c.user_id));

      // Per-cohort D1/D3/D7: i days have elapsed since signup day
      // i=0 means today's signups — not enough time for any retention yet
      const retForDay = (nDays) => {
        if (i < nDays || !cohortUsers.length) return null;
        const retained = cohortUsers.filter(u => {
          const signupEnd = new Date(u.created_at);
          signupEnd.setHours(23, 59, 59, 999);
          const windowEnd = new Date(signupEnd.getTime() + nDays * 86400000);
          return allCompletions.some(c =>
            c.user_id === u.id &&
            new Date(c.completed_at) > signupEnd &&
            new Date(c.completed_at) <= windowEnd
          );
        }).length;
        return { rate: Math.round((retained / cohortUsers.length) * 100), eligible: cohortUsers.length };
      };

      const dateStr = dayStart.toISOString().split("T")[0];
      cohortDays.push({
        date:           dateStr,
        signups:        cohortUsers.length,
        activated:      cohortActivated.length,
        activation_pct: cohortUsers.length > 0 ? Math.round((cohortActivated.length / cohortUsers.length) * 100) : null,
        first_task_pct: cohortActivated.length > 0 ? Math.round((cohortUsers.filter(u => firstTaskSet.has(u.id)).length / cohortActivated.length) * 100) : null,
        d1:  retForDay(1),
        d3:  retForDay(3),
        d7:  retForDay(7),
        is_post_fix: new Date(dateStr) >= new Date("2026-03-24"),
      });
    }

    // ── Post-fix health check ─────────────────────────────────────────────────
    const postFixUsers = realAuthUsers
      .filter(u => new Date(u.created_at) >= new Date(FIX_DATE))
      .map(u => u.id);

    let noCropsPostFix = 0;
    let noTasksPostFix = 0;

    if (postFixUsers.length > 0) {
      // Scope to specific IDs to avoid 1000-row Supabase limit
      const { data: postFixCrops } = await db.from("crop_instances").select("user_id").in("user_id", postFixUsers);
      const { data: postFixTasks } = await db.from("tasks").select("user_id").in("user_id", postFixUsers);
      const postFixCropSet = new Set((postFixCrops || []).map(r => r.user_id));
      const postFixTaskSet = new Set((postFixTasks || []).map(r => r.user_id));
      const postFixActivated = postFixUsers.filter(id => activatedIds.has(id));
      noCropsPostFix = postFixActivated.filter(id => !postFixCropSet.has(id)).length;
      noTasksPostFix = postFixActivated.filter(id => !postFixTaskSet.has(id)).length;
    }

    res.json({
      funnel: {
        signed_up:       totalSignups,
        onboarded:       totalActivated,
        tasks_generated: usersWithTasksSet.size,
        first_task_done: usersWithCompletedSet.size,
        active_crops:    usersWithCropsSet.size,
      },
      retention,
      push_retention: pushRetention,
      cohort_days: cohortDays,
      health_check: {
        no_crops_post_fix: noCropsPostFix,
        no_tasks_post_fix: noTasksPostFix,
        fix_date: FIX_DATE,
        post_fix_user_count: postFixUsers.length,
      },
    });
  } catch (e) {
    console.error("[FunnelMetrics]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// GET /admin/metrics/viewer — second-user viewer admin (signup count only)
// Auth by user ID (not email) — viewer is not the full admin
// =============================================================================
const VIEWER_ADMIN_ID = "448095f2-d379-4232-90f2-6ac7cebe1c70";
app.get("/admin/metrics/viewer", requireAuth, async (req, res) => {
  if (req.user.id !== VIEWER_ADMIN_ID) return res.status(403).json({ error: "Forbidden" });
  try {
    const authUsers = await getAllAuthUsers();
    const { data: demoProfiles } = await supabaseService.from("profiles").select("id").eq("is_demo", true);
    const demoIds = new Set((demoProfiles || []).map(p => p.id));
    const totalSignups = authUsers.filter(u => !demoIds.has(u.id)).length;
    res.json({ totalSignups });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// FIRST ACTION — fallback card for users with no tasks today
// =============================================================================
app.get("/first-action", requireAuth, async (req, res) => {
  try {
    const { data: crops } = await req.db.from("crop_instances")
      .select("id, name, variety, stage, status, sown_date, crop_def:crop_def_id(name, grower_notes)")
      .eq("user_id", req.user.id)
      .eq("active", true)
      .limit(10);

    if (!crops?.length) return res.json(null);

    // Pick the crop most likely to benefit from a check — prioritise recently sown
    const sorted = crops
      .filter(c => c.sown_date)
      .sort((a, b) => new Date(b.sown_date) - new Date(a.sown_date));
    const crop = sorted[0] || crops[0];

    const stageMap = {
      seed:       { action: "Check germination",   detail: "Look for signs of germination — seedlings should appear soon." },
      seedling:   { action: "Check seedlings",      detail: "Check moisture and look for leggy growth — ensure good light." },
      vegetative: { action: "Check plant health",   detail: "Inspect leaves for pests or yellowing. Water if top inch is dry." },
      flowering:  { action: "Check flowers",        detail: "Look for pollination and ensure consistent watering." },
      fruiting:   { action: "Check fruit set",      detail: "Inspect developing fruit and feed with high potash if due." },
      harvesting: { action: "Check harvest",        detail: "Your crop may be ready to pick — check size and ripeness." },
    };

    const stage = crop.stage || "seedling";
    const { action, detail } = stageMap[stage] || stageMap.vegetative;

    res.json({
      crop_id:     crop.id,
      crop_name:   crop.name,
      variety:     crop.variety || null,
      stage,
      action,
      detail,
      source_key: `first_action_${crop.id}_${new Date().toISOString().split("T")[0]}`,
    });
  } catch (e) {
    console.error("[FirstAction]", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post("/first-action/complete", requireAuth, async (req, res) => {
  try {
    const { crop_id, source_key } = req.body;
    if (!crop_id) return res.status(400).json({ error: "crop_id required" });

    // Log to funnel_events if table exists — graceful fail if not
    try {
      await supabaseService.from("funnel_events").insert({
        user_id:    req.user.id,
        event_type: "first_action_completed",
        meta:       JSON.stringify({ crop_id, source_key }),
        created_at: new Date().toISOString(),
      });
    } catch (_) { /* table may not exist yet */ }

    // Set stage_confidence = confirmed on the crop
    await req.db.from("crop_instances")
      .update({ stage_confidence: "confirmed", updated_at: new Date().toISOString() })
      .eq("id", crop_id)
      .eq("user_id", req.user.id);

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /admin/feedback — admin only
app.get("/admin/feedback", requireAuth, requireAdmin, async (req, res) => {
  const { data, error } = await supabaseService
    .from("feedback")
    .select("*, profiles(name)")
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  // Enrich with email from auth.users
  const users = await getAllAuthUsers();
  const emailMap = {};
  (users || []).forEach(u => { emailMap[u.id] = u.email; });

  const enriched = (data || []).map(f => ({
    ...f,
    user_email: emailMap[f.user_id] || null,
  }));

  res.json(enriched);
});

// =============================================================================
// ADMIN ENDPOINTS — restricted to mark@wynyardadvisory.co.uk
// =============================================================================

// GET /admin/crop-queue — AI-added crop_definitions pending review
app.get("/admin/crop-queue", requireAuth, requireAdmin, async (req, res) => {
  const { data, error } = await req.db
    .from("crop_definitions")
    .select("*, profiles(email)")
    .eq("admin_approved", false)
    .eq("ai_generated", true)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  const result = (data || []).map(c => ({
    ...c,
    added_by_email: c.profiles?.email || null,
  }));
  res.json(result);
});

// POST /admin/crop-queue/:id/approve
app.post("/admin/crop-queue/:id/approve", requireAuth, requireAdmin, async (req, res) => {
  const { error } = await req.db
    .from("crop_definitions")
    .update({ admin_approved: true, admin_reviewed_at: new Date().toISOString() })
    .eq("id", req.params.id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// POST /admin/crop-queue/:id/reject — deletes the definition
app.post("/admin/crop-queue/:id/reject", requireAuth, requireAdmin, async (req, res) => {
  const { error } = await req.db
    .from("crop_definitions")
    .delete()
    .eq("id", req.params.id)
    .eq("ai_generated", true); // safety — never delete hand-curated crops
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

// GET /admin/users — all users with crop counts and last seen
app.get("/admin/users", requireAuth, requireAdmin, async (req, res) => {
  // Use service role to access auth.users — covers users who never completed onboarding
  const users = await getAllAuthUsers();

  // Get profiles for name lookup
  const { data: profiles } = await supabaseService.from("profiles").select("id, name");
  const profileMap = {};
  (profiles || []).forEach(p => { profileMap[p.id] = p.name; });

  // Get crop counts per user
  const { data: crops } = await supabaseService.from("crop_instances").select("user_id").eq("active", true);
  const cropCounts = {};
  (crops || []).forEach(c => { cropCounts[c.user_id] = (cropCounts[c.user_id] || 0) + 1; });

  // Get last task completion per user
  const { data: tasks } = await supabaseService.from("tasks").select("user_id, completed_at").not("completed_at", "is", null).order("completed_at", { ascending: false });
  const lastSeen = {};
  (tasks || []).forEach(t => { if (!lastSeen[t.user_id]) lastSeen[t.user_id] = t.completed_at; });

  const result = users
    .sort((a, b) => new Date(a.created_at) - new Date(b.created_at))
    .map(u => ({
      id:         u.id,
      email:      u.email,
      name:       profileMap[u.id] || null,
      created_at: u.created_at,
      crop_count: cropCounts[u.id] || 0,
      last_seen:  lastSeen[u.id]   || null,
    }));

  res.json(result);
});

// =============================================================================
// PHOTO UPLOADS
// Profile, location, and area photos stored in Supabase Storage.
// All photos are base64 encoded on upload, stored privately, served via signed URLs.
// =============================================================================

async function uploadPhoto(bucket, path, base64, mimeType) {
  const buffer = Buffer.from(base64, "base64");
  const { error } = await supabaseService.storage
    .from(bucket)
    .upload(path, buffer, { contentType: mimeType || "image/jpeg", upsert: true });
  if (error) throw new Error(error.message);

  // Use public URL (buckets are set to public)
  const { data } = supabaseService.storage.from(bucket).getPublicUrl(path);
  return data.publicUrl;
}

// POST /photos/profile — upload profile photo
app.post("/photos/profile", requireAuth, async (req, res) => {
  const { base64, mime_type } = req.body;
  if (!base64) return res.status(400).json({ error: "base64 required" });
  try {
    const path = `${req.user.id}/profile.jpg`;
    const url  = await uploadPhoto("profile-photos", path, base64, mime_type);
    await req.db.from("profiles")
      .update({ photo_url: url })
      .eq("id", req.user.id);
    res.json({ photo_url: url });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /photos/location/:id — upload location photo
app.post("/photos/location/:id", requireAuth, async (req, res) => {
  const { base64, mime_type } = req.body;
  if (!base64) return res.status(400).json({ error: "base64 required" });
  try {
    const path = `${req.user.id}/location-${req.params.id}.jpg`;
    const url  = await uploadPhoto("garden-photos", path, base64, mime_type);
    await req.db.from("locations")
      .update({ photo_url: url })
      .eq("id", req.params.id)
      .eq("user_id", req.user.id);
    res.json({ photo_url: url });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /photos/area/:id — upload area photo
app.post("/photos/area/:id", requireAuth, async (req, res) => {
  const { base64, mime_type } = req.body;
  if (!base64) return res.status(400).json({ error: "base64 required" });
  try {
    const path = `${req.user.id}/area-${req.params.id}.jpg`;
    const url  = await uploadPhoto("garden-photos", path, base64, mime_type);
    // Find area and verify ownership via location
    const { data: area } = await req.db.from("growing_areas")
      .select("id, location_id, locations(user_id)")
      .eq("id", req.params.id)
      .single();
    if (!area || area.locations?.user_id !== req.user.id)
      return res.status(403).json({ error: "Not authorised" });
    await req.db.from("growing_areas")
      .update({ photo_url: url })
      .eq("id", req.params.id);
    res.json({ photo_url: url });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// =============================================================================
// =============================================================================
// TRANSITION AUTOMATION
// Triggered on final harvest of a crop in a bed that has a locked plan
// assignment. Generates:
//   1. Bed prep task — immediate
//   2. Sow/plant task — next valid window for the assigned crop
// Updates assignment status: locked → ready
// Both tasks use idempotent source_key upserts — safe to re-trigger.
// =============================================================================

// Returns the next calendar date (as ISO string) on or after `now`
// when `startMonth` (1-based) is in season.
function _nextWindowDate(startMonth, now = new Date()) {
  if (!startMonth) return null;
  const mi   = startMonth - 1; // 0-based
  const year = now.getFullYear();

  const thisYear = new Date(year, mi, 1);

  if (now.getMonth() < mi) {
    // Window hasn't opened yet this year — use this year
    return thisYear.toISOString().split("T")[0];
  }
  if (now.getMonth() === mi) {
    // We're currently in the window — task is due now
    return now.toISOString().split("T")[0];
  }
  // Window already passed this year — roll to next year
  return new Date(year + 1, mi, 1).toISOString().split("T")[0];
}

// Choose the right sow mode and month for a crop definition.
// Returns { taskType, actionVerb, dueDate, windowLabel }
function _resolveNextCropTask(cropDef, now = new Date()) {
  if (!cropDef) return null;

  const est = cropDef.default_establishment || "direct_sow";

  // Non-seed establishment — use plant_out window
  if (["tuber","crown","runner","cane"].includes(est)) {
    const month = cropDef.plant_out_start || cropDef.sow_direct_start || cropDef.sow_indoors_start;
    if (!month) return null;
    return {
      taskType:    "transplant",
      actionVerb:  est === "tuber" ? "Plant" : est === "crown" ? "Plant out" : "Plant",
      dueDate:     _nextWindowDate(month, now),
      windowLabel: `${["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month-1]}`,
    };
  }

  // Indoor-started — prefer indoor sow window
  if (est === "indoors" && cropDef.sow_indoors_start) {
    return {
      taskType:    "sow",
      actionVerb:  "Sow indoors",
      dueDate:     _nextWindowDate(cropDef.sow_indoors_start, now),
      windowLabel: `${["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][cropDef.sow_indoors_start-1]}`,
    };
  }

  // Direct sow
  const month = cropDef.sow_direct_start || cropDef.sow_indoors_start;
  if (!month) return null;
  return {
    taskType:    "sow",
    actionVerb:  "Sow",
    dueDate:     _nextWindowDate(month, now),
    windowLabel: `${["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][month-1]}`,
  };
}

async function _generateTransitionTasks(supabase, userId, areaId, harvestedAt) {
  try {
    // ── Find locked assignment for this bed ──────────────────────────────────
    const { data: assignments } = await supabase
      .from("area_plan_assignments")
      .select("id, crop_def_id, crop_name, category, planned_year, status")
      .eq("user_id", userId)
      .eq("area_id", areaId)
      .in("status", ["locked", "ready"])
      .order("planned_year", { ascending: true })
      .limit(1);

    const assignment = assignments?.[0];
    if (!assignment) return; // no locked plan for this bed — nothing to do

    // ── Fetch area name ──────────────────────────────────────────────────────
    const { data: area } = await supabase
      .from("growing_areas")
      .select("name")
      .eq("id", areaId)
      .single();
    const areaName = area?.name?.replace(/^"|"$/g, "") || "Bed";

    // ── Fetch crop definition ────────────────────────────────────────────────
    let cropDef = null;
    if (assignment.crop_def_id) {
      const { data: def } = await supabase
        .from("crop_definitions")
        .select("id, name, default_establishment, sow_indoors_start, sow_indoors_end, sow_direct_start, sow_direct_end, plant_out_start, plant_out_end, category")
        .eq("id", assignment.crop_def_id)
        .single();
      cropDef = def || null;
    }

    const cropName  = cropDef?.name || assignment.crop_name || "next crop";
    const now       = new Date();
    const today     = now.toISOString().split("T")[0];

    // ── Bed prep task — due now ──────────────────────────────────────────────
    // Visible immediately. Expires in 60 days (plenty of time to act).
    const prepKey = `u:${userId}|a:${areaId}|r:transition_bed_prep|harvest:${harvestedAt}`;
    const prepExpiry = new Date(now.getTime() + 60 * 86400000).toISOString();

    await supabase
      .from("tasks")
      .upsert({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Prepare ${areaName} for ${cropName}`,
        task_type:        "other",
        urgency:          "normal",
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       prepExpiry,
        status:           "due",
        engine_type:      "scheduled",
        record_type:      "task",
        source:           "rule_engine",
        rule_id:          "transition_bed_prep",
        source_key:       prepKey,
        date_confidence:  "exact",
        meta:             JSON.stringify({
          transition:     true,
          next_crop:      cropName,
          assignment_id:  assignment.id,
          note:           "Clear crop debris, loosen soil, and add compost before sowing your next crop.",
        }),
        risk_payload:     null,
      }, {
        onConflict:       "source_key",
        ignoreDuplicates: true,
      });

    // ── Sow / plant task — next valid window ─────────────────────────────────
    const nextTask = _resolveNextCropTask(cropDef, now);

    if (nextTask) {
      // Expires 6 weeks after due date — enough buffer for late sowings
      const sowExpiry = new Date(new Date(nextTask.dueDate).getTime() + 42 * 86400000).toISOString();
      // Visible 3 weeks before due so it appears in "Coming up soon"
      const visibleFrom = new Date(new Date(nextTask.dueDate).getTime() - 21 * 86400000)
        .toISOString().split("T")[0];
      const sowKey = `u:${userId}|a:${areaId}|r:transition_sow|assignment:${assignment.id}`;

      await supabase
        .from("tasks")
        .upsert({
          user_id:          userId,
          crop_instance_id: null,
          area_id:          areaId,
          action:           `${nextTask.actionVerb} ${cropName} in ${areaName}`,
          task_type:        nextTask.taskType,
          urgency:          "normal",
          due_date:         nextTask.dueDate,
          scheduled_for:    nextTask.dueDate,
          visible_from:     visibleFrom < today ? today : visibleFrom,
          expires_at:       sowExpiry,
          status:           nextTask.dueDate <= today ? "due" : "upcoming",
          engine_type:      "scheduled",
          record_type:      "task",
          source:           "rule_engine",
          rule_id:          "transition_sow",
          source_key:       sowKey,
          date_confidence:  "exact",
          meta:             JSON.stringify({
            transition:       true,
            next_crop:        cropName,
            assignment_id:    assignment.id,
            sow_window_label: nextTask.windowLabel,
            establishment:    cropDef?.default_establishment || "direct_sow",
          }),
          risk_payload:     null,
        }, {
          onConflict:       "source_key",
          ignoreDuplicates: true,
        });
    }

    // ── Advance assignment status: locked → ready ────────────────────────────
    // "ready" = bed is prepped and sow task has been issued
    if (assignment.status === "locked") {
      await supabase
        .from("area_plan_assignments")
        .update({
          status:     "ready",
          updated_at: new Date().toISOString(),
        })
        .eq("id", assignment.id)
        .eq("user_id", userId);
    }

    console.log(`[Transition] ${areaName} → ${cropName}: bed prep + ${nextTask ? nextTask.taskType + " " + nextTask.dueDate : "no sow window"} | assignment ${assignment.id} → ready`);

  } catch (err) {
    // Non-fatal — harvest has already been recorded, don't fail the response
    console.error("[Transition] Error generating transition tasks:", err.message);
  }
}

// HARVEST LOG
// Users log harvests from the forecast screen. Photos stored in Supabase Storage.
// =============================================================================

// GET /harvest-log — list user's harvest entries
app.get("/harvest-log", requireAuth, async (req, res) => {
  const { year } = req.query;
  let query = req.db.from("harvest_log")
    .select("*")
    .eq("user_id", req.user.id)
    .order("harvested_at", { ascending: false });

  if (year) {
    query = query
      .gte("harvested_at", `${year}-01-01`)
      .lte("harvested_at", `${year}-12-31`);
  }

  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// POST /harvest-log — create a harvest entry + optionally mark crop as harvested
app.post("/harvest-log", requireAuth,
  [body("crop_name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const {
      crop_instance_id, crop_name, variety,
      harvested_at, yield_score, quality_score,
      quantity_value, quantity_unit, notes,
      partial, // if true, crop stays active — more harvests to come
    } = req.body;

    // Insert using actual DB columns only
    const { data, error } = await req.db.from("harvest_log").insert({
      user_id:          req.user.id,
      crop_instance_id: crop_instance_id || null,
      harvested_at:     harvested_at || new Date().toISOString().split("T")[0],
      yield_score:      yield_score   || null,
      quality:          quality_score || null,
      quantity_g:       quantity_value ? parseFloat(quantity_value) : null,
      notes:            notes || null,
      partial:          partial ? true : false,
    }).select().single();

    if (error) return res.status(500).json({ error: error.message });

    // Only mark crop as harvested if this is the final harvest
    if (crop_instance_id && !partial) {
      const harvestedAt = new Date().toISOString().split("T")[0];
      const { data: harvestedCrop } = await req.db.from("crop_instances")
        .update({
          status:       "harvested",
          active:       false,
          harvested_at: harvestedAt,
          updated_at:   new Date().toISOString(),
        })
        .eq("id", crop_instance_id)
        .eq("user_id", req.user.id)
        .select("area_id").single();

      // Invalidate suggestions cache — area contents have changed
      if (harvestedCrop?.area_id) await clearSuggestions(harvestedCrop.area_id, req.db);

      // Re-run rule engine so tasks for this crop are cleaned up immediately
      await runRuleEngine(req.user.id);

      // Check for a locked plan assignment and generate bed prep + sow tasks
      if (harvestedCrop?.area_id) {
        await _generateTransitionTasks(supabaseService, req.user.id, harvestedCrop.area_id, harvestedAt);
      }
    }

    res.status(201).json(data);
  }
);

// DELETE /harvest-log/:id — undo a harvest entry + revert crop status
app.delete("/harvest-log/:id", requireAuth, async (req, res) => {
  // Get the entry first so we know the crop_instance_id
  const { data: entry, error: fetchErr } = await req.db.from("harvest_log")
    .select("*")
    .eq("id", req.params.id)
    .eq("user_id", req.user.id)
    .single();

  if (fetchErr || !entry) return res.status(404).json({ error: "Not found" });

  const { error } = await req.db.from("harvest_log")
    .delete()
    .eq("id", req.params.id)
    .eq("user_id", req.user.id);

  if (error) return res.status(500).json({ error: error.message });

  // Revert crop status back to growing
  if (entry.crop_instance_id) {
    await req.db.from("crop_instances")
      .update({ status: "growing", updated_at: new Date().toISOString() })
      .eq("id", entry.crop_instance_id)
      .eq("user_id", req.user.id);
  }

  res.json({ success: true, reverted_crop: entry.crop_instance_id });
});

// GET /harvest-log/summary — season summary grouped by crop with averages + individual records
app.get("/harvest-log/summary", requireAuth, async (req, res) => {
  const year = req.query.year || new Date().getFullYear();
  const { data, error } = await req.db.from("harvest_log")
    .select("id, crop_instance_id, harvested_at, yield_score, quality, quantity_g, notes, photo_url, partial, crop_instances(name, variety)")
    .gte("harvested_at", `${year}-01-01`)
    .lte("harvested_at", `${year}-12-31`)
    .order("harvested_at", { ascending: false });

  if (error) return res.status(500).json({ error: error.message });

  // Group by crop_instance_id
  const groups = {};
  for (const entry of data || []) {
    const cropName = entry.crop_instances?.name || "Unknown crop";
    const variety  = entry.crop_instances?.variety || null;
    const key      = entry.crop_instance_id || cropName;

    if (!groups[key]) {
      groups[key] = {
        crop_instance_id: entry.crop_instance_id,
        crop_name:        cropName,
        variety:          variety,
        entries:          [],
      };
    }
    groups[key].entries.push({
      id:           entry.id,
      harvested_at: entry.harvested_at,
      yield_score:  entry.yield_score,
      quality:      entry.quality,
      quantity_g:   entry.quantity_g,
      notes:        entry.notes,
      photo_url:    entry.photo_url,
      partial:      entry.partial,
    });
  }

  // Calculate averages and totals per group
  const summary = Object.values(groups).map(g => {
    const yields    = g.entries.map(e => e.yield_score).filter(Boolean);
    const qualities = g.entries.map(e => e.quality).filter(Boolean);
    const quantities = g.entries.map(e => e.quantity_g).filter(Boolean);

    return {
      crop_instance_id:  g.crop_instance_id,
      crop_name:         g.crop_name,
      variety:           g.variety,
      harvest_count:     g.entries.length,
      avg_yield_score:   yields.length    ? Math.round((yields.reduce((a, b) => a + b, 0) / yields.length) * 10) / 10 : null,
      avg_quality_score: qualities.length ? Math.round((qualities.reduce((a, b) => a + b, 0) / qualities.length) * 10) / 10 : null,
      total_quantity_g:  quantities.length ? quantities.reduce((a, b) => a + b, 0) : null,
      latest_harvest:    g.entries[0]?.harvested_at || null,
      entries:           g.entries,
    };
  });

  // Sort by latest harvest date
  summary.sort((a, b) => new Date(b.latest_harvest) - new Date(a.latest_harvest));

  res.json(summary);
});

// POST /harvest-log/:id/photo — upload a photo for a harvest entry
app.post("/harvest-log/:id/photo", requireAuth, async (req, res) => {
  const { base64, filename, mime_type } = req.body;
  if (!base64 || !filename) return res.status(400).json({ error: "base64 and filename required" });

  try {
    const buffer = Buffer.from(base64, "base64");
    const path   = `${req.user.id}/${req.params.id}/${filename}`;

    const { error: uploadErr } = await supabaseService.storage
      .from("harvest-photos")
      .upload(path, buffer, { contentType: mime_type || "image/jpeg", upsert: true });

    if (uploadErr) return res.status(500).json({ error: uploadErr.message });

    const { data: urlData } = supabaseService.storage
      .from("harvest-photos")
      .getPublicUrl(path);

    // Save URL to harvest log entry
    await req.db.from("harvest_log")
      .update({ photo_url: urlData.publicUrl, updated_at: new Date().toISOString() })
      .eq("id", req.params.id)
      .eq("user_id", req.user.id);

    res.json({ photo_url: urlData.publicUrl });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// WEATHER
// Weather is fetched and cached per location postcode.
// All areas within a location share the same weather context (Phase 1).
// =============================================================================

app.get("/weather", requireAuth, async (req, res) => {
  const { location_id } = req.query;
  let postcode;

  if (location_id) {
    const { data } = await req.db.from("locations")
      .select("postcode").eq("id", location_id).single();
    postcode = data?.postcode;
  }
  if (!postcode) {
    const { data } = await req.db.from("profiles")
      .select("postcode").eq("id", req.user.id).single();
    postcode = data?.postcode;
  }
  if (!postcode) return res.status(400).json({ error: "No postcode set" });

  // Return cached if valid
  const { data: cached } = await supabaseService.from("weather_cache")
    .select("temp_c, frost_risk, rain_mm, condition, expires_at")
    .eq("postcode", postcode)
    .gt("expires_at", new Date().toISOString())
    .single();
  if (cached) return res.json(cached);

  // Fetch fresh
  try {
    const apiKey = process.env.OPENWEATHER_API_KEY;
    const r    = await fetch(`https://api.openweathermap.org/data/2.5/forecast?q=${postcode},GB&appid=${apiKey}&units=metric`);
    const json = await r.json();
    if (!json.list) return res.status(502).json({ error: "Weather API error" });

    const next24  = json.list.slice(0, 8);
    const minTemp = Math.min(...next24.map(f => f.main.temp_min));
    const weather = {
      postcode,
      temp_c:     json.list[0].main.temp,
      frost_risk: minTemp <= 2,
      rain_mm:    next24.reduce((s, f) => s + (f.rain?.["3h"] || 0), 0),
      condition:  json.list[0].weather[0].description,
      data:       json,
      expires_at: new Date(Date.now() + 3600000).toISOString(),
    };
    await supabaseService.from("weather_cache").upsert(weather);
    const { data: _raw, ...clean } = weather;
    res.json(clean);
  } catch (err) {
    res.status(502).json({ error: "Failed to fetch weather" });
  }
});

// =============================================================================
// DASHBOARD
// Single endpoint returning everything the home screen needs.
// =============================================================================

app.get("/dashboard", requireAuth, async (req, res) => {
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate");
  const today   = todayISO();
  const weekEnd = weekEndISO();
  // Run expiry first
  await expireOverdueTasks(req.user.id, req.db);

  // ── Synchronous on-demand engine ────────────────────────────────────────────
  // Check whether the user has anything useful for today or this week.
  // If today + this_week are both empty (even if coming_up has items), run the
  // engine synchronously and re-query — so the response always contains fresh data.
  // Guard: only runs when today AND this_week are empty to avoid running on every load.
  // Timeout: engine is given 8 seconds max to avoid hanging the response.
  let engineRanSync = false;
  const { data: usefulTasks } = await req.db
    .from("tasks")
    .select("id, due_date, surface_class")
    .eq("user_id", req.user.id)
    .is("completed_at", null)
    .not("status", "eq", "expired")
    .lte("due_date", weekEnd) // only today + this week
    .neq("surface_class", "insight"); // exclude insights — not actionable tasks

  const hasUsefulTasks = (usefulTasks || []).length > 0;

  if (!hasUsefulTasks) {
    console.log(`[Dashboard] No useful tasks for ${req.user.id} — running engine synchronously`);
    try {
      await Promise.race([
        runRuleEngine(req.user.id),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Engine timeout")), 8000)),
      ]);
      engineRanSync = true;
    } catch (err) {
      console.error("[Dashboard] Sync engine error:", err.message);
    }
  }

  // Start of current week (Monday)
  const nowDate    = new Date();
  const dayOfWeek  = nowDate.getDay();
  const daysToMon  = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
  const weekStart  = new Date(nowDate);
  weekStart.setDate(nowDate.getDate() - daysToMon);
  weekStart.setHours(0, 0, 0, 0);
  const weekStartISO = weekStart.toISOString();

  // Track last seen — used for email fallback suppression
  req.db.from("profiles")
    .update({ last_seen_at: new Date().toISOString() })
    .eq("id", req.user.id)
    .then(() => {}).catch(() => {});


  const [tasksRes, cropsRes, profileRes, harvestRes, completedThisWeekRes] = await Promise.all([
    req.db.from("tasks")
      .select("*, crop:crop_instance_id(name, variety, succession_group_id, succession_index), area:area_id(name)")
      .eq("user_id", req.user.id).is("completed_at", null)
      .order("urgency",  { ascending: false })
      .order("due_date", { ascending: true }),
    req.db.from("crop_instances")
      .select("id, name, variety, variety_id, sown_date, stage, timeline_offset_days, area_id, missed_task_note, succession_index, crop_def:crop_def_id(harvest_month_start, harvest_month_end, days_to_maturity_min, days_to_maturity_max, pest_window_start, pest_window_end, pest_notes, is_perennial)")
      .eq("user_id", req.user.id).eq("active", true),
    req.db.from("profiles").select("name, plan, postcode, photo_url").eq("id", req.user.id).single(),
    req.db.from("harvest_log")
      .select("id, harvested_at, quantity_g, crop:crop_instance_id(name)")
      .eq("user_id", req.user.id)
      .gte("harvested_at", new Date(new Date().getFullYear(), 0, 1).toISOString().split("T")[0])
      .order("harvested_at", { ascending: false })
      .limit(5),
    req.db.from("tasks")
      .select("id", { count: "exact", head: true })
      .eq("user_id", req.user.id)
      .not("completed_at", "is", null)
      .gte("completed_at", weekStartISO),
  ]);

  const tasks   = tasksRes.data  || [];
  const crops   = cropsRes.data  || [];
  const profile = profileRes.data;
  const tasksCompletedThisWeek = completedThisWeekRes.count || 0;
  const year    = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;

  // ── Harvest forecast ──────────────────────────────────────────────────────
  const harvestForecast = crops
    .filter(c => c.crop_def?.harvest_month_start || c.sown_date)
    .map(c => {
      const isPerennial = c.crop_def?.is_perennial;
      let windowStart, windowEnd;

      if (isPerennial && c.crop_def?.harvest_month_start) {
        // Perennial crops: always use the seasonal harvest window, not days-from-planting.
        // Days-to-maturity from planting is meaningless for established perennials and
        // produces past dates that incorrectly show as "Ready now".
        const hStart = c.crop_def.harvest_month_start;
        const hEnd   = c.crop_def.harvest_month_end || hStart;
        // Use next upcoming harvest window — if this year's window has passed, show next year's
        const thisYearStart = new Date(year, hStart - 1, 1).toISOString().split("T")[0];
        const useYear = thisYearStart < new Date().toISOString().split("T")[0] && hEnd < currentMonth
          ? year + 1 : year;
        windowStart = new Date(useYear, hStart - 1, 1).toISOString().split("T")[0];
        windowEnd   = new Date(useYear, hEnd   - 1, 28).toISOString().split("T")[0];
      } else {
        const tl = buildTimeline(c);
        if (tl?.harvest_date_iso) {
          windowStart = tl.harvest_date_iso;
          windowEnd   = tl.harvest_date_iso;
        } else if (c.crop_def?.harvest_month_start) {
          windowStart = new Date(year, c.crop_def.harvest_month_start - 1, 1).toISOString().split("T")[0];
          windowEnd   = new Date(year, c.crop_def.harvest_month_end   - 1, 28).toISOString().split("T")[0];
        } else {
          return null;
        }
      }
      return { crop: c.succession_index ? `${c.name} (Sow ${c.succession_index})` : c.name, variety: c.variety || null, crop_instance_id: c.id, window_start: windowStart, window_end: windowEnd };
    })
    .filter(Boolean);

  // ── Missing data prompts ──────────────────────────────────────────────────
  const missingData = crops
    .filter(c => (!c.variety_id && !c.variety) || (!c.sown_date && c.status !== "planned" && !c.crop_def?.is_perennial))
    .map(c => ({
      id:      c.id,
      name:    c.name,
      missing: [
        (!c.variety_id && !c.variety) && "variety not set",
        (!c.sown_date && c.status !== "planned" && !c.crop_def?.is_perennial) && "sow date not recorded yet"
      ].filter(Boolean),
    }));

  // ── Pest risk — how many crops are in their peak pest window this month ───
  const cropsInPestWindow = crops.filter(c => {
    const ps = c.crop_def?.pest_window_start;
    const pe = c.crop_def?.pest_window_end;
    if (!ps || !pe) return false;
    return currentMonth >= ps && currentMonth <= pe;
  });
  const pestRisk = cropsInPestWindow.length === 0 ? "low"
                 : cropsInPestWindow.length <= 2   ? "medium"
                 : "high";
  const pestCrops = cropsInPestWindow.map(c => c.name);

  // ── Weather + frost risk (7-day) ──────────────────────────────────────────
  let weather = null;
  try {
    const rawPostcode = profile?.postcode;
    if (rawPostcode) {
      // Always use outward code only (e.g. "TS22" not "TS22 5BQ") — OpenWeather requires it
      const postcode = rawPostcode.trim().split(" ")[0].toUpperCase();

      // Check cache first
      const { data: cached } = await supabaseService.from("weather_cache")
        .select("temp_c, condition, frost_risk, frost_risk_7day, icon_code, expires_at")
        .eq("postcode", postcode)
        .gt("expires_at", new Date().toISOString())
        .single();

      if (cached) {
        weather = cached;
      } else {
        // Fetch fresh from OpenWeather forecast API
        const apiKey = process.env.OPENWEATHER_API_KEY;
        const r    = await fetch(`https://api.openweathermap.org/data/2.5/forecast?q=${postcode},GB&appid=${apiKey}&units=metric&cnt=40`);
        const json = await r.json();

        if (json.list) {
          const allSlots   = json.list;
          const next7days  = allSlots.slice(0, 56);
          const minTemp7d  = Math.min(...next7days.map(f => f.main.temp_min));
          const minTemp24h = Math.min(...allSlots.slice(0, 8).map(f => f.main.temp_min));

          weather = {
            postcode,
            temp_c:          Math.round(json.list[0].main.temp),
            condition:       json.list[0].weather[0].description,
            icon_code:       json.list[0].weather[0].icon,
            frost_risk:      minTemp24h <= 2,
            frost_risk_7day: minTemp7d,
            rain_mm:         allSlots.slice(0, 8).reduce((s, f) => s + (f.rain?.["3h"] || 0), 0),
            data:            json,
            expires_at:      new Date(Date.now() + 3600000).toISOString(),
          };
          await supabaseService.from("weather_cache").upsert(weather);
        }
      }
    }
  } catch (err) {
    console.error("[Dashboard] Weather fetch error:", err.message);
  }

  // ── Frost risk traffic light ──────────────────────────────────────────────
  let frostRisk = "low";
  if (weather) {
    const min7 = weather.frost_risk_7day;
    if (min7 <= 0)      frostRisk = "high";    // actual frost forecast
    else if (min7 <= 3) frostRisk = "medium";  // close to freezing
    else                frostRisk = "low";
  }

  // Re-query tasks if engine ran synchronously — ensure fresh data in response
  let finalTasks = tasks;
  if (engineRanSync) {
    const { data: freshTasks } = await req.db
      .from("tasks")
      .select("*, crop:crop_instance_id(name, variety, succession_group_id, succession_index), area:area_id(name)")
      .eq("user_id", req.user.id).is("completed_at", null)
      .not("status", "eq", "expired")
      .order("urgency",  { ascending: false })
      .order("due_date", { ascending: true });
    finalTasks = freshTasks || tasks;
  }

  // Separate real tasks from insights — insights are informational, not actionable
  const actionableTasks = finalTasks.filter(t => t.surface_class !== "insight");
  const insightTasks    = finalTasks.filter(t => t.surface_class === "insight");

  res.json({
    user:             profile?.name,
    profile_photo:    profile?.photo_url || null,
    plan:             profile?.plan || "free",
    engine_ran_sync:  engineRanSync,
    tasks: {
      tasks:     actionableTasks, // actionable only — insights excluded from main feed
      today:     actionableTasks.filter(t => !t.record_type || t.record_type !== "alert").filter(t => t.due_date <= today),
      this_week: actionableTasks.filter(t => !t.record_type || t.record_type !== "alert").filter(t => t.due_date > today && t.due_date <= weekEnd),
      coming_up: actionableTasks.filter(t => !t.record_type || t.record_type !== "alert").filter(t => t.due_date > weekEnd),
      alerts:    actionableTasks.filter(t => t.record_type === "alert"),
      insights:  insightTasks, // separate bucket — frontend can show these differently
    },
    crop_count:       crops.length,
    crops_with_flags: crops.filter(c => c.missed_task_note).map(c => ({ id: c.id, name: c.name, missed_task_note: c.missed_task_note })),
    harvest_forecast: harvestForecast,
    missing_data:     missingData,
    recent_harvests:  harvestRes.data || [],
    weather: weather ? {
      temp_c:    weather.temp_c,
      condition: weather.condition,
      icon_code: weather.icon_code,
    } : null,
    frost_risk:  frostRisk,
    pest_risk:   pestRisk,
    pest_crops:  pestCrops,
    tasks_completed_this_week: tasksCompletedThisWeek,
  });
});

// =============================================================================
// TASK EXPIRY — expire tasks whose window has passed, flag crop with red dot
// =============================================================================

async function expireOverdueTasks(userId, db) {
  const today = todayISO();
  try {
    // Find incomplete tasks with a due_window_end that has passed
    const { data: expiredTasks } = await db
      .from("tasks")
      .select("id, crop_instance_id, action, rule_id")
      .eq("user_id", userId)
      .is("completed_at", null)
      .not("due_window_end", "is", null)
      .lt("due_window_end", today);

    if (!expiredTasks?.length) return;

    for (const task of expiredTasks) {
      // Mark task as expired (use a special completed_at marker)
      await db.from("tasks")
        .update({ completed_at: new Date().toISOString(), meta: JSON.stringify({ expired: true, reason: "Window passed" }) })
        .eq("id", task.id);

      // Flag the crop instance with a missed_task note
      if (task.crop_instance_id) {
        await db.from("crop_instances")
          .update({ missed_task_note: `Missed: ${task.action}. Window has now passed — update this crop if you've since completed this task.` })
          .eq("id", task.crop_instance_id);
      }
    }
    console.log(`[Expiry] Expired ${expiredTasks.length} overdue tasks for user ${userId}`);
  } catch (err) {
    console.error("[Expiry] Error:", err.message);
  }
}

// =============================================================================
// TIPS — AI generated per user, cached weekly in Supabase
// =============================================================================

app.get("/tips", requireAuth, async (req, res) => {
  const userId = req.user.id;
  const oneWeekAgo = new Date(Date.now() - 7 * 86400000).toISOString();

  // Check cache first
  const { data: cached } = await req.db
    .from("tips_cache")
    .select("tips, created_at")
    .eq("user_id", userId)
    .gte("created_at", oneWeekAgo)
    .order("created_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (cached?.tips) {
    return res.json({ tips: cached.tips, cached: true });
  }

  // Load user's crops, areas and feeds for context
  const { data: crops } = await req.db
    .from("crop_instances")
    .select("name, variety, status, area:area_id(name, type)")
    .eq("user_id", userId)
    .eq("active", true)
    .limit(20);

  const { data: profile } = await req.db
    .from("profiles")
    .select("postcode")
    .eq("id", userId)
    .single();

  const { data: userFeeds } = await req.db
    .from("user_feeds")
    .select("brand, product_name, feed_type, form, dilution_ml_per_litre")
    .eq("user_id", userId)
    .limit(10);

  if (!crops?.length) {
    return res.json({ tips: [], cached: false });
  }

  const cropList = crops.map(c => `${c.name}${c.variety ? ` (${c.variety})` : ""} — ${c.status || "growing"} in ${c.area?.name || "unknown area"} (${c.area?.type || "garden"})`).join("\n");
  const feedList = userFeeds?.length
    ? userFeeds.map(f => `${[f.brand, f.product_name].filter(Boolean).join(" ")} (${f.feed_type}, ${f.form}${f.dilution_ml_per_litre ? `, ${f.dilution_ml_per_litre}ml/L` : ""})`).join("\n")
    : null;
  const month = new Date().toLocaleString("en-GB", { month: "long" });

  try {
    const aiRes = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 800,
        messages: [{
          role: "user",
          content: `You are a practical UK gardening advisor. Generate exactly 3 concise, actionable tips for a UK grower in ${month}.

Their current crops:
${cropList}
${profile?.postcode ? `Location: ${profile.postcode}` : ""}
${feedList ? `Their feeds/fertilisers:\n${feedList}` : "They have no feeds recorded yet."}

Rules:
- Each tip must be specific to their actual crops or growing setup
- If a tip involves feeding, reference their actual product by name if they have a suitable one. If they don't have one, suggest the feed type and mention they can add it to their feeds section
- Tips should be practical tasks or preparation advice (not generic advice)
- Vary the topics: e.g. soil prep, pest prevention, companion planting, feeding, protection, tools
- Keep each tip to 1-2 sentences max
- Respond ONLY with a JSON array, no preamble, no markdown. Format:
[{"title":"Short title","tip":"The full tip text","emoji":"relevant emoji"}]`
        }],
      }),
    });

    const aiData = await aiRes.json();
    const text = aiData.content?.[0]?.text || "[]";
    const clean = text.replace(/```json|```/g, "").trim();
    const tips = JSON.parse(clean);

    // Cache the tips
    await req.db.from("tips_cache").insert({ user_id: userId, tips });

    return res.json({ tips, cached: false });
  } catch (err) {
    console.error("[Tips] Error:", err.message);
    return res.json({ tips: [], cached: false });
  }
});



app.get("/harvest", requireAuth, async (req, res) => {
  const { crop_instance_id } = req.query;
  let query = req.db.from("harvest_log")
    .select("*, crop:crop_instance_id(name, variety)")
    .eq("user_id", req.user.id)
    .order("harvested_at", { ascending: false });
  if (crop_instance_id) query = query.eq("crop_instance_id", crop_instance_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/harvest", requireAuth,
  [
    body("crop_instance_id").isUUID(),
    body("harvested_at").optional().isISO8601(),
    body("quality").optional().isInt({ min: 1, max: 5 }),
  ],
  async (req, res) => {
    if (!validate(req, res)) return;
    const { crop_instance_id, harvested_at, quantity_g, quantity_units, quantity_notes, quality, notes, photo_url } = req.body;
    const { data, error } = await req.db.from("harvest_log").insert({
      user_id: req.user.id,
      crop_instance_id,
      harvested_at: harvested_at || new Date().toISOString().split("T")[0],
      quantity_g:     quantity_g     || null,
      quantity_units: quantity_units || null,
      quantity_notes: quantity_notes || null,
      quality:        quality        || null,
      notes:          notes          || null,
      photo_url:      photo_url      || null,
    }).select().single();
    if (error) return res.status(500).json({ error: error.message });

    // Mark the crop task as complete if there's an open harvest task
    await req.db.from("tasks")
      .update({ completed_at: new Date().toISOString() })
      .eq("crop_instance_id", crop_instance_id)
      .eq("task_type", "harvest")
      .is("completed_at", null);

    res.status(201).json(data);
  }
);

app.delete("/harvest/:id", requireAuth, async (req, res) => {
  const { error } = await req.db.from("harvest_log")
    .delete().eq("id", req.params.id).eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
  res.status(204).send();
});

// =============================================================================
// DIAGNOSIS LOG
// Free plan: 3 lifetime diagnoses then paywall.
// Mark's account (ADMIN_EMAIL) always bypasses plan checks — full Pro access.
// =============================================================================

// ── Pro bypass helper ─────────────────────────────────────────────────────────
// Mark's account always has Pro access regardless of plan or PRO_ENABLED flag.
// This lets the founder test all Pro features in production without affecting users.
function isMarkAccount(req) {
  return req.user?.email === ADMIN_EMAIL || PARTNER_ADMIN_IDS.includes(req.user?.id);
}

async function userIsPro(req) {
  if (isMarkAccount(req)) return true;
  const { data: profile } = await req.db.from("profiles")
    .select("plan, is_demo").eq("id", req.user.id).single();
  // Demo accounts get unlimited Plant Check — they exist to showcase the product
  if (profile?.is_demo) return true;
  return profile?.plan === "pro";
}

app.get("/diagnoses", requireAuth, async (req, res) => {
  const { crop_instance_id } = req.query;
  let query = req.db.from("diagnosis_log")
    .select("*, crop:crop_instance_id(name, variety)")
    .eq("user_id", req.user.id)
    .order("created_at", { ascending: false });
  if (crop_instance_id) query = query.eq("crop_instance_id", crop_instance_id);
  const { data, error } = await query;
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

// GET /diagnoses/count — how many lifetime diagnoses this user has used
app.get("/diagnoses/count", requireAuth, async (req, res) => {
  try {
    const pro = await userIsPro(req);
    const { count } = await req.db.from("diagnosis_log")
      .select("id", { count: "exact", head: true })
      .eq("user_id", req.user.id);
    res.json({
      count: count || 0,
      limit: 3,
      is_pro: pro,
      remaining: pro ? null : Math.max(0, 3 - (count || 0)),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post("/diagnoses", requireAuth,
  [body("crop_instance_id").optional().isUUID()],
  async (req, res) => {
    if (!validate(req, res)) return;

    // Plan check — free users capped at 3 lifetime diagnoses
    // Mark's account always bypasses
    const pro = await userIsPro(req);
    if (!pro) {
      const { count } = await req.db.from("diagnosis_log")
        .select("id", { count: "exact", head: true })
        .eq("user_id", req.user.id);
      if ((count || 0) >= 3) {
        return res.status(403).json({
          error: "You've used your 3 free plant checks. Upgrade to Pro for unlimited diagnosis.",
          upgrade_required: true,
        });
      }
    }

    const { crop_instance_id, photo_url, diagnosis, severity, confidence, ai_model } = req.body;
    const { data, error } = await req.db.from("diagnosis_log").insert({
      user_id:         req.user.id,
      crop_instance_id:crop_instance_id || null,
      photo_url:       photo_url        || null,
      diagnosis:       diagnosis        || null,
      severity:        severity         || null,
      confidence:      confidence       || null,
      ai_model:        ai_model         || null,
    }).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

// =============================================================================
// POST /diagnoses/analyze
// Full Claude Vision plant check. Assembles rich context from crop, area,
// location, neighbours, weather, and recent activity before calling Claude.
// Returns structured diagnosis result. Logs to diagnosis_log on success.
// Mark's account always bypasses plan limits.
// =============================================================================

app.post("/diagnoses/analyze", requireAuth, async (req, res) => {
  const { crop_instance_id, image } = req.body;
  if (!image)             return res.status(400).json({ error: "image required" });
  if (!crop_instance_id)  return res.status(400).json({ error: "crop_instance_id required" });

  try {
    // ── Plan check ────────────────────────────────────────────────────────────
    const pro = await userIsPro(req);
    if (!pro) {
      const { count } = await req.db.from("diagnosis_log")
        .select("id", { count: "exact", head: true })
        .eq("user_id", req.user.id);
      if ((count || 0) >= 3) {
        return res.status(403).json({
          error: "You've used your 3 free plant checks. Upgrade to Pro for unlimited diagnosis.",
          upgrade_required: true,
          count: count || 0,
        });
      }
    }

    // ── Assemble crop context ─────────────────────────────────────────────────
    const { data: crop, error: cropErr } = await req.db.from("crop_instances")
      .select("*, area:area_id(id, name, type, location_id), crop_def:crop_def_id(name, category, pest_notes, pest_window_start, pest_window_end, days_to_maturity_min, days_to_maturity_max, companions, avoid), variety:variety_id(name, days_to_maturity_min, days_to_maturity_max)")
      .eq("id", crop_instance_id)
      .eq("user_id", req.user.id)
      .single();

    if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });

    // ── Neighbouring crops in same area ──────────────────────────────────────
    const { data: neighbours } = await supabaseService.from("crop_instances")
      .select("name, variety")
      .eq("area_id", crop.area_id)
      .eq("active", true)
      .neq("id", crop_instance_id)
      .limit(8);

    // ── Weather context ───────────────────────────────────────────────────────
    let weatherCtx = null;
    try {
      const { data: loc } = await req.db.from("locations")
        .select("postcode").eq("id", crop.area?.location_id).single();
      if (loc?.postcode) {
        const postcode = loc.postcode.trim().split(" ")[0].toUpperCase();
        const { data: wx } = await supabaseService.from("weather_cache")
          .select("temp_c, condition, frost_risk, frost_risk_7day, rain_mm")
          .eq("postcode", postcode)
          .gt("expires_at", new Date().toISOString())
          .single();
        if (wx) weatherCtx = wx;
      }
    } catch(_) {}

    // ── Recent activity ───────────────────────────────────────────────────────
    const { data: recentTasks } = await req.db.from("tasks")
      .select("action, task_type, completed_at")
      .eq("crop_instance_id", crop_instance_id)
      .not("completed_at", "is", null)
      .order("completed_at", { ascending: false })
      .limit(5);

    // ── Previous diagnoses for this crop ─────────────────────────────────────
    const { data: prevDiagnoses } = await req.db.from("diagnosis_log")
      .select("diagnosis, severity, created_at")
      .eq("crop_instance_id", crop_instance_id)
      .order("created_at", { ascending: false })
      .limit(3);

    // ── Build prompt ──────────────────────────────────────────────────────────
    const cropName    = crop.crop_def?.name || crop.name || "Unknown crop";
    const variety     = crop.variety?.name || crop.variety || null;
    const areaType    = crop.area?.type?.replace(/_/g, " ") || "growing area";
    const currentStage = crop.stage || "unknown";
    const sowDate     = crop.sown_date || crop.transplanted_date || null;
    const lastWatered = crop.last_watered_at ? new Date(crop.last_watered_at).toLocaleDateString("en-GB") : "unknown";
    const lastFed     = crop.last_fed_at ? new Date(crop.last_fed_at).toLocaleDateString("en-GB") : "unknown";

    const neighbourStr = neighbours?.length
      ? neighbours.map(n => `${n.name}${n.variety ? " (" + n.variety + ")" : ""}`).join(", ")
      : "none recorded";

    const weatherStr = weatherCtx
      ? `Current temp: ${weatherCtx.temp_c}°C, Conditions: ${weatherCtx.condition}, Frost risk next 7 days: ${weatherCtx.frost_risk_7day !== undefined ? weatherCtx.frost_risk_7day + "°C min" : "unknown"}, Recent rain: ${weatherCtx.rain_mm || 0}mm`
      : "Weather data unavailable";

    const recentStr = recentTasks?.length
      ? recentTasks.map(t => `${t.task_type}: ${t.action} (${new Date(t.completed_at).toLocaleDateString("en-GB")})`).join("; ")
      : "No recent tasks logged";

    const prevStr = prevDiagnoses?.length
      ? prevDiagnoses.map(d => `${new Date(d.created_at).toLocaleDateString("en-GB")}: ${d.diagnosis} (${d.severity})`).join("; ")
      : "No previous diagnoses";

    const currentMonth = new Date().getMonth() + 1;
    const pestWindow = crop.crop_def?.pest_window_start && crop.crop_def?.pest_window_end
      ? currentMonth >= crop.crop_def.pest_window_start && currentMonth <= crop.crop_def.pest_window_end
      : false;

    const prompt = `You are an expert UK horticulturalist and plant pathologist helping a home grower or allotment holder.

Analyse this photo of a growing plant and provide a structured diagnosis.

CROP CONTEXT:
- Crop: ${cropName}${variety ? " (" + variety + ")" : ""}
- Recorded lifecycle stage: ${currentStage}
- Sow/transplant date: ${sowDate || "not recorded"}
- Growing in: ${areaType}
- Last watered: ${lastWatered}
- Last fed: ${lastFed}
- Neighbouring crops in same area: ${neighbourStr}
- Currently in peak pest risk window: ${pestWindow ? "YES" : "no"}
- Pest notes: ${crop.crop_def?.pest_notes || "none"}
- Companion plants (beneficial): ${(crop.crop_def?.companions || []).join(", ") || "none recorded"}

ENVIRONMENTAL CONTEXT:
- ${weatherStr}
- Month: ${new Date().toLocaleString("en-GB", { month: "long" })}

RECENT ACTIVITY:
- ${recentStr}

PREVIOUS DIAGNOSES FOR THIS CROP:
- ${prevStr}

YOUR TASK:
Examine the photo carefully. Respond ONLY with a JSON object — no markdown, no preamble.

Return this exact structure:
{
  "problem_detected": true or false,
  "problem_name": "name of disease/pest/deficiency or null",
  "problem_description": "1-2 sentence description of what you can see or null",
  "severity": "low" | "medium" | "high" | null,
  "stage_detected": "seed" | "seedling" | "vegetative" | "flowering" | "fruiting" | "harvesting" | null,
  "stage_confidence": "low" | "medium" | "high" | null,
  "stage_matches_record": true or false or null,
  "harvest_readiness": "ready" | "soon" | "not_ready" | null,
  "harvest_readiness_detail": "1 sentence e.g. 'Ready to pick now' or 'Allow another 1-2 weeks' or null",
  "yield_impact_pct": integer between -80 and 0 or null (negative = yield reduction, null = no impact detected),
  "quality_impact": "none" | "low" | "medium" | "high" | null,
  "treatment_steps": ["step 1", "step 2"] or [],
  "prevention_tips": ["tip 1", "tip 2"] or [],
  "reasoning_summary": "2-3 sentence plain English summary of your overall assessment",
  "requires_confirmation": true or false,
  "confirmation_prompt": "Short question to ask user before updating their crop record, e.g. 'Your tomatoes look like they are at flowering stage — update your crop record?' or null",
  "looks_healthy": true or false
}

RULES:
- stage_detected: only fill if you can clearly see the growth stage from the photo
- harvest_readiness: only fill if this is a fruiting/harvesting crop and readiness is visible
- requires_confirmation: set true ONLY if stage_detected differs from recorded stage OR harvest_readiness is "ready" or "soon"
- yield_impact_pct: estimate realistically for UK conditions. A mild mildew might be -10%, severe blight -50%
- treatment_steps: practical, UK-specific steps. Maximum 4 steps.
- If the photo is unclear or not a plant, set problem_detected: false and explain in reasoning_summary
- Base everything on UK growing conditions and the specific crop context provided`;

    // ── Call Claude Vision ────────────────────────────────────────────────────
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": process.env.ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: "claude-sonnet-4-20250514",
        max_tokens: 1200,
        messages: [{
          role: "user",
          content: [
            {
              type: "image",
              source: {
                type: "base64",
                media_type: "image/jpeg",
                data: image,
              },
            },
            { type: "text", text: prompt },
          ],
        }],
      }),
    });

    const raw  = await response.json();
    const text = raw.content?.[0]?.text || "";

    let result;
    try {
      const match = text.match(/\{[\s\S]*\}/);
      if (!match) throw new Error("No JSON in response");
      result = JSON.parse(match[0]);
    } catch {
      throw new Error(`Claude returned unparseable response: ${text.slice(0, 200)}`);
    }

    // ── Validate result before logging ───────────────────────────────────────
    // Only count as a use if Claude returned a meaningful, usable result.
    // A response missing both problem_detected and looks_healthy is a failed
    // analysis — don't charge the user a use for an upstream failure.
    const isUsableResult = (
      typeof result.problem_detected === "boolean" ||
      typeof result.looks_healthy === "boolean" ||
      result.stage_detected ||
      result.harvest_readiness
    );

    if (!isUsableResult) {
      throw new Error("Claude returned an incomplete analysis result — not counted as a use");
    }

    // ── Log to diagnosis_log — only on successful analysis ────────────────────
    const { data: logEntry, error: logErr } = await req.db.from("diagnosis_log").insert({
      user_id:          req.user.id,
      crop_instance_id: crop_instance_id,
      diagnosis:        result.problem_name || (result.looks_healthy ? "Healthy" : "No issue detected"),
      severity:         result.severity || null,
      confidence:       result.stage_confidence || null,
      ai_model:         "claude-sonnet-4-20250514",
    }).select().single();

    if (logErr) console.error("[DiagnosisAnalyze] Log error:", logErr.message);

    // ── Return result with usage info ─────────────────────────────────────────
    const { count: newCount } = await req.db.from("diagnosis_log")
      .select("id", { count: "exact", head: true })
      .eq("user_id", req.user.id);

    res.json({
      ...result,
      diagnosis_id: logEntry?.id || null,
      is_pro: pro,
      diagnoses_used: newCount || 1,
      diagnoses_remaining: pro ? null : Math.max(0, 3 - (newCount || 1)),
    });

  } catch (err) {
    console.error("[DiagnosisAnalyze] Error:", err.message);
    captureError("DiagnosisAnalyze", err, { crop_instance_id });
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// RULE ENGINE — manual trigger
// =============================================================================

app.post("/run-rules", requireAuth, async (req, res) => {
  const tasks = await runRuleEngine(req.user.id);
  res.json({ generated: tasks.length, tasks });
});

// =============================================================================
// ADMIN — reset all tasks and regenerate for every user
// Hit this once after deploying rule engine fixes to clear stale dedup locks
// =============================================================================

app.post("/admin/reset-tasks", requireAuth, requireAdmin, async (req, res) => {
  try {
    // Delete all incomplete tasks and rule_log entries for all users
    await supabaseService.from("rule_log").delete().neq("id", "00000000-0000-0000-0000-000000000000");
    await supabaseService.from("tasks").delete().is("completed_at", null);

    // Re-run rule engine for every user with a profile
    const { data: profiles } = await supabaseService.from("profiles").select("id");
    if (!profiles?.length) return res.json({ reset: true, users: 0, tasks_generated: 0 });

    let total = 0;
    for (const p of profiles) {
      const tasks = await runRuleEngine(p.id);
      total += tasks.length;
    }

    res.json({ reset: true, users: profiles.length, tasks_generated: total });
  } catch (err) {
    console.error("[AdminResetTasks]", err.message);
    res.status(500).json({ error: err.message });
  }
});


// =============================================================================
// ONBOARDING — complete setup in one call
// Creates location + area silently, creates crop instances, runs rule engine.
// Stage → sow_date mapping so user never has to enter exact dates.
// =============================================================================

// Stage → sow date offset (days before today)
const STAGE_OFFSETS = {
  not_sown:    null,       // no sow date — status = planned
  just_sown:   3,          // sown ~3 days ago
  growing:     21,         // sown ~3 weeks ago
  near_harvest: null,      // calculated per crop DTM
};

function inferSowDate(stage, cropDef) {
  const today = new Date();
  const pad = n => String(n).padStart(2, "0");
  const fmt = d => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;

  if (stage === "not_sown") return null;
  if (stage === "just_sown") {
    const d = new Date(today); d.setDate(d.getDate() - 3);
    return fmt(d);
  }
  if (stage === "growing") {
    const d = new Date(today); d.setDate(d.getDate() - 21);
    return fmt(d);
  }
  if (stage === "near_harvest") {
    // Work back from DTM — set sow date so harvest is ~14 days away
    const dtm = cropDef?.days_to_maturity_min || 60;
    const d = new Date(today); d.setDate(d.getDate() - (dtm - 14));
    return fmt(d);
  }
  return null;
}

function starterTaskForCrop(cropName, stage) {
  if (stage === "not_sown") return `Sow ${cropName} — now is a good time to get started`;
  if (stage === "just_sown") return `Check ${cropName} seedlings — water if soil is dry`;
  if (stage === "growing")   return `Check ${cropName} — inspect for pests and feed if needed`;
  if (stage === "near_harvest") return `Check ${cropName} — harvest may be approaching`;
  return `Check on your ${cropName}`;
}

app.post("/onboarding/complete", requireAuth, async (req, res) => {
  const db = req.db;
  const userId = req.user.id;
  const {
    name, postcode,
    crops,       // [{ name, crop_def_id, stage }]
    area_type,
    area_name,
  } = req.body;

  if (!name || !postcode || !crops?.length || !area_type) {
    return res.status(400).json({ error: "name, postcode, crops and area_type required" });
  }

  try {
    // 1. Save profile — use supabaseService so it bypasses RLS and definitely commits
    // locations.user_id FK references profiles.id so profile MUST exist before location insert
    const { error: profileErr } = await supabaseService.from("profiles")
      .upsert({ id: userId, name, postcode }, { onConflict: "id" });
    if (profileErr) throw new Error("Profile: " + profileErr.message);

    // 2. Create default location (or reuse existing)
    // Use supabaseService to bypass RLS — user record may not be visible to user-scoped client yet
    let locationId;
    const { data: existingLocs } = await supabaseService.from("locations").select("id").eq("user_id", userId).limit(1);
    if (existingLocs?.length) {
      locationId = existingLocs[0].id;
    } else {
      const { data: loc, error: locErr } = await supabaseService.from("locations").insert({
        user_id: userId, name: "My garden", postcode,
      }).select("id").single();
      if (locErr) throw new Error("Location: " + locErr.message);
      locationId = loc.id;
    }

    // 3. Create first area (or reuse existing)
    let areaId;
    const { data: existingAreas } = await supabaseService.from("growing_areas").select("id").eq("location_id", locationId).limit(1);
    if (existingAreas?.length) {
      areaId = existingAreas[0].id;
    } else {
      const finalAreaName = area_name?.trim() || "My first area";
      const { data: area, error: areaErr } = await supabaseService.from("growing_areas").insert({
        location_id: locationId, name: finalAreaName, type: area_type,
      }).select("id").single();
      if (areaErr) throw new Error("Area: " + areaErr.message);
      areaId = area.id;
    }

    // 4. Look up crop definitions for stage → sow date mapping
    const defIds = crops.filter(c => c.crop_def_id).map(c => c.crop_def_id);
    let cropDefs = {};
    if (defIds.length) {
      const { data: defs } = await supabaseService.from("crop_definitions")
        .select("id, days_to_maturity_min").in("id", defIds);
      for (const d of (defs || [])) cropDefs[d.id] = d;
    }

    // 5. Create crop instances
    const cropInserts = crops.map(c => {
      const def = cropDefs[c.crop_def_id] || null;
      const sowDate = inferSowDate(c.stage, def);
      const status = sowDate ? "growing" : "planned";
      return {
        user_id:               userId,
        location_id:           locationId,
        area_id:               areaId,
        name:                  c.name,
        crop_def_id:           c.crop_def_id || null,
        sown_date:             sowDate,
        status,
        start_date_confidence: "inferred",
        source:                "onboarding",
        quantity:              1,
      };
    });

    await supabaseService.from("crop_instances").insert(cropInserts);

    // 6. Run rule engine
    const tasks = await runRuleEngine(userId);

    // 7. Fallback — if engine generated nothing, insert starter tasks
    if (!tasks?.length) {
      const today = new Date().toISOString().split("T")[0];
      const starterTasks = crops.map(c => ({
        user_id:    userId,
        area_id:    areaId,
        action:     starterTaskForCrop(c.name, c.stage),
        task_type:  "monitor",
        due_date:   today,
        urgency:    "low",
        source:     "onboarding",
        source_key: `onboarding_starter_${c.name}_${today}`,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      }));
      await supabaseService.from("tasks").upsert(starterTasks, { onConflict: "source_key" });
    }

    res.json({ ok: true, locationId, areaId, tasksGenerated: tasks?.length || 0 });
  } catch (err) {
    console.error("[Onboarding] Error:", err.message);
    res.status(500).json({ error: err.message });
  }
});


// =============================================================================
// POST /admin/onboarding-recovery-email
// One-shot: finds last 50 signups with no crops (unactivated due to bug),
// skips anyone already sent this email, sends recovery email via Resend.
// =============================================================================
app.post("/admin/onboarding-recovery-email", requireAuth, requireAdmin, async (req, res) => {
  try {
    // Get last 50 auth users by signup date
    const { data: { users: allUsers } } = await supabaseService.auth.admin.listUsers({ perPage: 50, page: 1 });
    const recent = (allUsers || [])
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
      .slice(0, 50);

    // Find which have no crop_instances (unactivated)
    const userIds = recent.map(u => u.id);
    const { data: activatedCrops } = await supabaseService.from("crop_instances")
      .select("user_id").in("user_id", userIds);
    const activatedIds = new Set((activatedCrops || []).map(c => c.user_id));
    const unactivated = recent.filter(u => !activatedIds.has(u.id));

    // Skip anyone already sent this recovery email
    const { data: alreadySent } = await supabaseService.from("email_log")
      .select("user_id").eq("email_type", "onboarding_recovery").in("user_id", userIds);
    const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));
    const toEmail = unactivated.filter(u => !alreadySentIds.has(u.id) && u.email);

    const results = [];
    for (const user of toEmail) {
      try {
        const resp = await fetch("https://api.resend.com/emails", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer \${process.env.RESEND_API_KEY}`,
          },
          body: JSON.stringify({
            from: "Vercro <hello@vercro.com>",
            to: user.email,
            subject: "Your Vercro garden plan is ready 🌱",
            html: `
              <div style="font-family: Georgia, serif; max-width: 520px; margin: 0 auto; padding: 40px 24px; color: #1A2E28;">
                <div style="font-size: 28px; margin-bottom: 8px;">🌱</div>
                <h1 style="font-size: 24px; font-weight: 900; margin: 0 0 16px; color: #2F5D50;">
                  Sorry — we had a setup issue
                </h1>
                <p style="font-size: 16px; line-height: 1.6; margin: 0 0 16px; color: #444;">
                  When you signed up for Vercro, a technical issue meant we couldn't finish setting up your garden plan. That's now fixed.
                </p>
                <p style="font-size: 16px; line-height: 1.6; margin: 0 0 24px; color: #444;">
                  It takes under 60 seconds to complete — and once you do, you'll have a personalised daily task plan waiting for you based on exactly what you're growing.
                </p>
                <a href="https://app.vercro.com" style="display: inline-block; background: #2F5D50; color: #fff; text-decoration: none; padding: 14px 32px; border-radius: 10px; font-size: 16px; font-weight: 700; margin-bottom: 32px;">
                  Complete my garden setup →
                </a>
                <p style="font-size: 14px; color: #888; line-height: 1.6; margin: 0;">
                  If you have any questions just reply to this email.<br>
                  — The Vercro team
                </p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 32px 0;" />
                <p style="font-size: 12px; color: #aaa;">vercro.com · Built for UK growers</p>
              </div>
            `,
          }),
        });

        if (resp.ok) {
          // Log so we never send twice
          await supabaseService.from("email_log").insert({
            user_id:    user.id,
            email_type: "onboarding_recovery",
            sent_at:    new Date().toISOString(),
          });
          results.push({ email: user.email, status: "sent" });
        } else {
          const err = await resp.json();
          results.push({ email: user.email, status: "failed", error: err });
        }
      } catch (e) {
        results.push({ email: user.email, status: "error", error: e.message });
      }
    }

    res.json({
      total_recent:    recent.length,
      unactivated:     unactivated.length,
      already_sent:    alreadySentIds.size,
      emails_sent:     results.filter(r => r.status === "sent").length,
      results,
    });
  } catch (err) {
    console.error("[OnboardingRecovery]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /demo/reset — wipe and re-seed the demo account (demo users only)
app.post("/demo/reset", requireAuth, async (req, res) => {
  const userId = req.user.id;

  // Verify this is the demo account
  const { data: profile } = await req.db.from("profiles").select("is_demo").eq("id", userId).single();
  if (!profile?.is_demo) return res.status(403).json({ error: "Not a demo account" });

  try {
    // Wipe all data for demo user
    await supabaseService.from("tasks").delete().eq("user_id", userId);
    await supabaseService.from("harvest_log").delete().eq("user_id", userId);
    await supabaseService.from("crop_instances").delete().eq("user_id", userId);

    // Get area IDs to delete areas
    const { data: locs } = await supabaseService.from("locations").select("id").eq("user_id", userId);
    const locIds = (locs || []).map(l => l.id);
    if (locIds.length > 0) {
      const { data: areas } = await supabaseService.from("growing_areas").select("id").in("location_id", locIds);
      const areaIds = (areas || []).map(a => a.id);
      if (areaIds.length > 0) await supabaseService.from("growing_areas").delete().in("id", areaIds);
    }
    await supabaseService.from("locations").delete().eq("user_id", userId);

    // Re-seed locations and areas
    const loc1 = crypto.randomUUID();
    const loc2 = crypto.randomUUID();
    const area1 = crypto.randomUUID();
    const area2 = crypto.randomUUID();
    const area3 = crypto.randomUUID();
    const area4 = crypto.randomUUID();
    const area5 = crypto.randomUUID();

    await supabaseService.from("locations").insert([
      { id: loc1, user_id: userId, name: "Back Garden", postcode: "M1 1AE" },
      { id: loc2, user_id: userId, name: "Allotment",   postcode: "M1 1AE" },
    ]);

    await supabaseService.from("growing_areas").insert([
      { id: area1, location_id: loc1, name: "Raised bed 1", type: "raised_bed" },
      { id: area2, location_id: loc1, name: "Raised bed 2", type: "raised_bed" },
      { id: area3, location_id: loc1, name: "Greenhouse",   type: "greenhouse" },
      { id: area4, location_id: loc2, name: "Plot A",       type: "open_ground" },
      { id: area5, location_id: loc2, name: "Fruit corner", type: "open_ground" },
    ]);

    // Get crop def IDs
    const cropNames = ["tomato", "courgette", "carrot", "potato", "apple", "strawberry", "lettuce", "brussels", "garlic", "mint", "onion", "bean", "pea"];
    const { data: defs } = await supabaseService.from("crop_definitions").select("id, name");
    const def = (keyword) => defs.find(d => d.name.toLowerCase().includes(keyword))?.id || null;

    // Active crops
    const now = new Date();
    const crops = [
      { user_id: userId, area_id: area1, name: "Tomatoes",         crop_def_id: def("tomato"),     sown_date: "2026-02-15", stage: "seedling",   stage_confidence: "inferred", status: "sown_indoors",  active: true },
      { user_id: userId, area_id: area1, name: "Lettuce",          crop_def_id: def("lettuce"),    sown_date: "2026-03-01", stage: "seedling",   stage_confidence: "inferred", status: "sown_outdoors", active: true },
      { user_id: userId, area_id: area1, name: "Carrot",           crop_def_id: def("carrot"),     sown_date: "2026-02-20", stage: "seedling",   stage_confidence: "inferred", status: "sown_outdoors", active: true },
      { user_id: userId, area_id: area2, name: "Courgette",        crop_def_id: def("courgette"),  sown_date: "2026-03-10", stage: "seed",       stage_confidence: "inferred", status: "sown_indoors",  active: true },
      { user_id: userId, area_id: area2, name: "Brussels Sprouts", crop_def_id: def("brussels"),   sown_date: "2026-03-12", stage: "seed",       stage_confidence: "inferred", status: "sown_indoors",  active: true },
      { user_id: userId, area_id: area2, name: "Garlic",           crop_def_id: def("garlic"),     sown_date: "2025-10-15", stage: "vegetative", stage_confidence: "inferred", status: "growing",       active: true },
      { user_id: userId, area_id: area3, name: "Tomatoes",         crop_def_id: def("tomato"),     sown_date: "2026-01-20", stage: "vegetative", stage_confidence: "inferred", status: "sown_indoors",  active: true },
      { user_id: userId, area_id: area3, name: "Mint",             crop_def_id: def("mint"),       sown_date: "2025-04-01", stage: "vegetative", stage_confidence: "inferred", status: "growing",       active: true },
      { user_id: userId, area_id: area4, name: "Potato",           crop_def_id: def("potato"),     sown_date: "2026-02-23", stage: "seed",       stage_confidence: "inferred", status: "transplanted",  active: true },
      { user_id: userId, area_id: area4, name: "Carrot",           crop_def_id: def("carrot"),     sown_date: "2026-03-05", stage: "seed",       stage_confidence: "inferred", status: "sown_outdoors", active: true },
      { user_id: userId, area_id: area4, name: "Lettuce",          crop_def_id: def("lettuce"),    sown_date: "2026-03-10", stage: "seed",       stage_confidence: "inferred", status: "sown_outdoors", active: true },
      { user_id: userId, area_id: area5, name: "Apple",            crop_def_id: def("apple"),      sown_date: "2025-04-01", stage: "vegetative", stage_confidence: "inferred", status: "growing",       active: true },
      { user_id: userId, area_id: area5, name: "Strawberry",       crop_def_id: def("strawberry"), sown_date: "2025-04-01", stage: "vegetative", stage_confidence: "inferred", status: "growing",       active: true },
    ];
    const { data: insertedCrops } = await supabaseService.from("crop_instances").insert(crops).select("id, name, area_id");

    // 2025 completed crops
    const completed = [
      { user_id: userId, area_id: area1, name: "Tomatoes",    crop_def_id: def("tomato"),     sown_date: "2025-02-10", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area1, name: "Courgette",   crop_def_id: def("courgette"),  sown_date: "2025-04-15", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area2, name: "Carrot",      crop_def_id: def("carrot"),     sown_date: "2025-03-20", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area4, name: "Potato",      crop_def_id: def("potato"),     sown_date: "2025-03-01", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area4, name: "Onion",       crop_def_id: def("onion"),      sown_date: "2025-02-28", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area4, name: "Runner Bean", crop_def_id: def("bean"),       sown_date: "2025-04-20", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area4, name: "Peas",        crop_def_id: def("pea"),        sown_date: "2025-03-15", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area5, name: "Apple",       crop_def_id: def("apple"),      sown_date: "2024-04-01", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area5, name: "Strawberry",  crop_def_id: def("strawberry"), sown_date: "2024-04-01", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
      { user_id: userId, area_id: area1, name: "Lettuce",     crop_def_id: def("lettuce"),    sown_date: "2025-05-01", stage: "finished", stage_confidence: "inferred", status: "harvested", active: false },
    ];
    const { data: completedCrops } = await supabaseService.from("crop_instances").insert(completed).select("id, name");

    // Harvest logs
    const ci = (name, arr) => arr?.find(c => c.name === name)?.id;
    const harvests = [
      { user_id: userId, crop_instance_id: ci("Tomatoes",   completedCrops), harvested_at: "2025-07-28", quantity_g: 420,  quality: 5, notes: "First pick — brilliant colour, Gardener's Delight never disappoints" },
      { user_id: userId, crop_instance_id: ci("Tomatoes",   completedCrops), harvested_at: "2025-08-04", quantity_g: 680,  quality: 5, notes: "Best week yet — blight-free and loads of fruit" },
      { user_id: userId, crop_instance_id: ci("Tomatoes",   completedCrops), harvested_at: "2025-08-11", quantity_g: 510,  quality: 4, notes: "Slight split on a few but taste excellent" },
      { user_id: userId, crop_instance_id: ci("Tomatoes",   completedCrops), harvested_at: "2025-08-18", quantity_g: 390,  quality: 4, notes: "Tailing off but still going strong" },
      { user_id: userId, crop_instance_id: ci("Tomatoes",   completedCrops), harvested_at: "2025-08-25", quantity_g: 220,  quality: 3, notes: "End of season — some blight on lower leaves" },
      { user_id: userId, crop_instance_id: ci("Courgette",  completedCrops), harvested_at: "2025-07-01", quantity_g: 620,  quality: 4, notes: "First courgette — left it a day too long" },
      { user_id: userId, crop_instance_id: ci("Courgette",  completedCrops), harvested_at: "2025-07-08", quantity_g: 480,  quality: 5, notes: "Perfect size, brilliant flavour" },
      { user_id: userId, crop_instance_id: ci("Courgette",  completedCrops), harvested_at: "2025-07-15", quantity_g: 890,  quality: 3, notes: "Missed one — turned into a marrow!" },
      { user_id: userId, crop_instance_id: ci("Courgette",  completedCrops), harvested_at: "2025-07-22", quantity_g: 540,  quality: 5, notes: "Back on track — picking every 3 days" },
      { user_id: userId, crop_instance_id: ci("Courgette",  completedCrops), harvested_at: "2025-07-29", quantity_g: 720,  quality: 4, notes: "Still producing well" },
      { user_id: userId, crop_instance_id: ci("Carrot",     completedCrops), harvested_at: "2025-07-20", quantity_g: 380,  quality: 4, notes: "Good first pull — decent size, sweet flavour" },
      { user_id: userId, crop_instance_id: ci("Carrot",     completedCrops), harvested_at: "2025-08-02", quantity_g: 520,  quality: 5, notes: "Best carrots yet — perfect shape and size" },
      { user_id: userId, crop_instance_id: ci("Carrot",     completedCrops), harvested_at: "2025-08-15", quantity_g: 290,  quality: 3, notes: "A few forked ones — probably hit a stone" },
      { user_id: userId, crop_instance_id: ci("Potato",     completedCrops), harvested_at: "2025-07-10", quantity_g: 1200, quality: 5, notes: "Charlotte first early — superb, waxy and nutty" },
      { user_id: userId, crop_instance_id: ci("Potato",     completedCrops), harvested_at: "2025-07-10", quantity_g: 980,  quality: 4, notes: "Second row — slightly smaller but good" },
      { user_id: userId, crop_instance_id: ci("Potato",     completedCrops), harvested_at: "2025-07-17", quantity_g: 1450, quality: 5, notes: "Main haul — excellent yield this year" },
      { user_id: userId, crop_instance_id: ci("Onion",      completedCrops), harvested_at: "2025-08-10", quantity_g: 1800, quality: 3, notes: "Dried off well but some smaller than hoped" },
      { user_id: userId, crop_instance_id: ci("Onion",      completedCrops), harvested_at: "2025-08-10", quantity_g: 1200, quality: 4, notes: "Second batch much better sized" },
      { user_id: userId, crop_instance_id: ci("Runner Bean",completedCrops), harvested_at: "2025-07-25", quantity_g: 340,  quality: 5, notes: "Tender and stringless — perfect timing" },
      { user_id: userId, crop_instance_id: ci("Runner Bean",completedCrops), harvested_at: "2025-08-01", quantity_g: 520,  quality: 5, notes: "Peak of the season" },
      { user_id: userId, crop_instance_id: ci("Runner Bean",completedCrops), harvested_at: "2025-08-08", quantity_g: 480,  quality: 4, notes: "Still excellent" },
      { user_id: userId, crop_instance_id: ci("Runner Bean",completedCrops), harvested_at: "2025-08-15", quantity_g: 310,  quality: 3, notes: "Getting a bit stringy now" },
      { user_id: userId, crop_instance_id: ci("Peas",       completedCrops), harvested_at: "2025-06-28", quantity_g: 180,  quality: 5, notes: "Sweet as anything — worth every pod" },
      { user_id: userId, crop_instance_id: ci("Peas",       completedCrops), harvested_at: "2025-07-05", quantity_g: 220,  quality: 5, notes: "Last of the peas — froze half of them" },
      { user_id: userId, crop_instance_id: ci("Apple",      completedCrops), harvested_at: "2025-09-14", quantity_g: 2200, quality: 4, notes: "First real harvest from this tree — really pleased" },
      { user_id: userId, crop_instance_id: ci("Apple",      completedCrops), harvested_at: "2025-09-21", quantity_g: 1800, quality: 4, notes: "Second pick — some windfall included" },
      { user_id: userId, crop_instance_id: ci("Strawberry", completedCrops), harvested_at: "2025-06-15", quantity_g: 280,  quality: 5, notes: "First strawberries of summer — incredible flavour" },
      { user_id: userId, crop_instance_id: ci("Strawberry", completedCrops), harvested_at: "2025-06-22", quantity_g: 420,  quality: 5, notes: "Peak season — eating straight from the plant" },
      { user_id: userId, crop_instance_id: ci("Strawberry", completedCrops), harvested_at: "2025-06-29", quantity_g: 380,  quality: 4, notes: "Still going strong" },
      { user_id: userId, crop_instance_id: ci("Strawberry", completedCrops), harvested_at: "2025-07-06", quantity_g: 210,  quality: 3, notes: "End of main flush — a few botrytis affected" },
      { user_id: userId, crop_instance_id: ci("Lettuce",    completedCrops), harvested_at: "2025-06-10", quantity_g: 150,  quality: 5, notes: "First cut — perfect for salads" },
      { user_id: userId, crop_instance_id: ci("Lettuce",    completedCrops), harvested_at: "2025-06-24", quantity_g: 180,  quality: 5, notes: "Second cut even better" },
      { user_id: userId, crop_instance_id: ci("Lettuce",    completedCrops), harvested_at: "2025-07-08", quantity_g: 120,  quality: 4, notes: "Starting to bolt slightly in the heat" },
    ].filter(h => h.crop_instance_id); // skip any with no match
    await supabaseService.from("harvest_log").insert(harvests);

    // Run rule engine for fresh tasks
    const { RuleEngine } = require("./rule-engine");
    const engine = new RuleEngine(supabaseService);
    const tasks = await engine.runForUser(userId);

    res.json({ ok: true, crops: crops.length + completed.length, harvests: harvests.length, tasks: tasks.length });
  } catch (err) {
    console.error("[DemoReset]", err.message);
    res.status(500).json({ error: err.message });
  }
});
// =============================================================================
app.post("/crops/:id/observe", requireAuth, async (req, res) => {
  const { observation_type, symptom_code, severity, notes, confirmed_stage, timeline_offset_days } = req.body;
  if (!observation_type) return res.status(400).json({ error: "observation_type required" });
  const { data: crop, error: cropErr } = await req.db.from("crop_instances")
    .select("id, name, status, stage, crop_def_id, sown_date")
    .eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });
  const { data: obs, error: obsErr } = await req.db.from("observation_logs").insert({
    user_id: req.user.id, crop_id: req.params.id,
    observed_at: new Date().toISOString().split("T")[0],
    observation_type: "other", symptom_code: symptom_code || null,
    severity: severity || null, notes: notes || null,
  }).select().single();
  if (obsErr) return res.status(500).json({ error: obsErr.message });
  const updates = {};
  const engineActions = [];
  if (symptom_code === "flowering_confirmed" || confirmed_stage === "flowering") { updates.stage = "flowering"; updates.stage_confidence = "confirmed"; updates.stage_check_snoozed_until = null; engineActions.push("stage_updated"); }
  if (symptom_code === "fruit_set_confirmed" || confirmed_stage === "fruiting") { updates.stage = "fruiting"; updates.stage_confidence = "confirmed"; updates.stage_check_snoozed_until = null; engineActions.push("stage_updated"); }
  if (symptom_code === "seedling_emerged" || confirmed_stage === "seedling") { updates.stage = "seedling"; updates.stage_confidence = "confirmed"; updates.stage_check_snoozed_until = null; engineActions.push("stage_updated"); }
  if (symptom_code === "vegetative_confirmed" || confirmed_stage === "vegetative") { updates.stage = "vegetative"; updates.stage_confidence = "confirmed"; updates.stage_check_snoozed_until = null; engineActions.push("stage_updated"); }
  if (symptom_code === "harvest_started" || confirmed_stage === "harvesting") { updates.status = "harvesting"; updates.stage = "harvesting"; updates.stage_confidence = "confirmed"; updates.stage_check_snoozed_until = null; engineActions.push("harvest_started"); processBadgeEvent(req.user.id, "harvest_logged").catch(console.error); }
  if (symptom_code === "transplant_done") { updates.status = "transplanted"; updates.transplant_date = new Date().toISOString().split("T")[0]; engineActions.push("transplant_done"); }
  if (symptom_code === "plant_struggling") { updates.missed_task_note = "Plant reported as struggling — check growing conditions"; engineActions.push("struggling_flagged"); }
  if (symptom_code === "looks_healthy") { updates.missed_task_note = null; engineActions.push("health_confirmed"); }
  if (typeof timeline_offset_days === "number") {
    updates.timeline_offset_days = timeline_offset_days;
    engineActions.push("timeline_offset_applied");
  }
  if (Object.keys(updates).length > 0) { updates.updated_at = new Date().toISOString(); await supabaseService.from("crop_instances").update(updates).eq("id", req.params.id).eq("user_id", req.user.id); }

  // ── Crop-scoped task purge — runs when stage or timeline offset changes ────
  // Clears open engine-generated tasks for this crop so the rule engine
  // rebuilds a clean, correct set for the new effective lifecycle position.
  // Preserves completed tasks and manual activity logs.
  if (confirmed_stage || typeof timeline_offset_days === "number") {
    try {
      const cropId = req.params.id;

      // 1. Direct crop-linked open engine tasks
      const { data: directTasks } = await supabaseService
        .from("tasks")
        .select("id")
        .eq("user_id", req.user.id)
        .eq("crop_instance_id", cropId)
        .is("completed_at", null)
        .eq("source", "rule_engine");

      // 2. Area-level open engine tasks whose source_key references this crop UUID
      const { data: areaTasks } = await supabaseService
        .from("tasks")
        .select("id, source_key")
        .eq("user_id", req.user.id)
        .is("crop_instance_id", null)
        .is("completed_at", null)
        .eq("source", "rule_engine");

      const staleAreaTasks = (areaTasks || []).filter(t => t.source_key?.includes(cropId));

      const allStaleIds = [
        ...(directTasks || []).map(t => t.id),
        ...staleAreaTasks.map(t => t.id),
      ];

      if (allStaleIds.length > 0) {
        await supabaseService.from("rule_log").delete().in("task_id", allStaleIds);
        await supabaseService.from("tasks").delete().in("id", allStaleIds);
        console.log(`[Observe] Purged ${allStaleIds.length} stale engine tasks for crop ${cropId}`);
        engineActions.push(`tasks_purged:${allStaleIds.length}`);
      }
    } catch (purgeErr) {
      console.error("[Observe] Task purge error:", purgeErr.message);
      // Non-fatal — continue to rule engine run
    }
  }

  await runRuleEngine(req.user.id);
  res.json({ observation: obs, crop_updated: Object.keys(updates).length > 0, engine_actions: engineActions });
});

app.get("/crops/:id/observations", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("observation_logs")
    .select("*").eq("crop_id", req.params.id).eq("user_id", req.user.id)
    .order("observed_at", { ascending: false }).limit(20);
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// ── Shared helper — write a manual_activity_logs row ─────────────────────────
async function writeActivityLog(userId, activityType, scopeType, scopeId, performedAt, notes, customLabel) {
  await supabaseService.from("manual_activity_logs").insert({
    user_id:       userId,
    activity_type: activityType,
    scope_type:    scopeType,
    scope_id:      scopeId,
    performed_at:  performedAt,
    notes:         notes    || null,
    custom_label:  customLabel || null,
  });
}

// POST /crops/:id/log-action
// Crop-scoped activity logging. Existing endpoint — upgraded in place.
// Backward compatible: still accepts pruned/note, maps to new types internally.
app.post("/crops/:id/log-action", requireAuth, async (req, res) => {
  const { action_type, notes, performed_at, custom_label } = req.body;
  if (!action_type) return res.status(400).json({ error: "action_type required" });

  // Validate: other requires a label
  if (action_type === "other" && !custom_label?.trim()) {
    return res.status(400).json({ error: "custom_label required for activity type 'other'" });
  }

  const { data: crop, error: cropErr } = await req.db.from("crop_instances")
    .select("id, name, area_id, last_watered_at, last_fed_at, crop_def_id")
    .eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });

  const now        = performed_at ? new Date(performed_at).toISOString() : new Date().toISOString();
  const today      = now.split("T")[0];

  // Map legacy types to canonical — keep backward compat
  const canonicalType = action_type === "note"   ? "other"
                      : action_type === "pruned" ? "pruned_mulched"
                      : action_type;

  // Write to observation_logs (existing behaviour — non-fatal if fails)
  try {
    await req.db.from("observation_logs").insert({
      user_id: req.user.id, crop_id: req.params.id,
      observed_at: today, observation_type: canonicalType, notes: notes || null,
    });
  } catch(obsErr) { console.error("[LogAction] observation_logs insert failed:", obsErr.message); }

  // Write to manual_activity_logs (non-fatal if fails)
  try {
    await writeActivityLog(req.user.id, canonicalType, "crop", req.params.id, now, notes, custom_label);
  } catch(malErr) { console.error("[LogAction] manual_activity_logs insert failed:", malErr.message); }

  // Update summary timestamps + build hint
  const updates = { updated_at: now };
  let nextActionHint = null;

  if (canonicalType === "watered") {
    updates.last_watered_at = now;
    nextActionHint = "Watering logged — next check suppressed for a couple of days";

  } else if (canonicalType === "fed") {
    updates.last_fed_at = now;
    const { data: def } = await supabaseService.from("crop_definitions")
      .select("feed_interval_days").eq("id", crop.crop_def_id).single();
    const interval = def?.feed_interval_days || 14;
    nextActionHint = `Next feed in about ${interval} days`;

  } else if (canonicalType === "pruned_mulched") {
    updates.last_pruned_or_mulched_at = now;
    nextActionHint = "Pruning/mulching logged";

  } else if (canonicalType === "weeded") {
    updates.last_weeded_at = now;
    nextActionHint = "Weeding logged";

  } else if (canonicalType === "other") {
    nextActionHint = "Activity logged";
  }

  await req.db.from("crop_instances").update(updates).eq("id", req.params.id).eq("user_id", req.user.id);
  if (canonicalType === "watered" || canonicalType === "fed") await runRuleEngine(req.user.id);
  res.json({ ok: true, action_type: canonicalType, next_action_hint: nextActionHint });
});

// POST /activity/log
// Generic activity logging — supports scope_ids (array) for multi-area logging.
// activity_type: watered | fed | pruned_mulched | weeded | other
// scope_type:    area | location | crop
// scope_ids:     array of UUIDs (preferred) OR scope_id: single UUID
//
// Water  → timestamps area.last_watered_at (rule engine suppresses tasks)
// Feed   → timestamps crop.last_fed_at for all crops in selected areas
// Prune  → timestamps area.last_pruned_or_mulched_at
// Weed   → timestamps area.last_weeded_at
// Other  → free-text log only
app.post("/activity/log", requireAuth, async (req, res) => {
  const { activity_type, scope_type, performed_at, notes, custom_label } = req.body;
  // Accept scope_ids (array) or legacy scope_id (single)
  const scopeIds = req.body.scope_ids
    ? (Array.isArray(req.body.scope_ids) ? req.body.scope_ids : [req.body.scope_ids])
    : req.body.scope_id ? [req.body.scope_id] : [];

  // Validation
  if (!activity_type) return res.status(400).json({ error: "activity_type required" });
  if (!scope_type)    return res.status(400).json({ error: "scope_type required" });
  if (!scopeIds.length) return res.status(400).json({ error: "scope_id or scope_ids required" });
  if (!["watered","fed","pruned_mulched","weeded","other"].includes(activity_type)) {
    return res.status(400).json({ error: "Invalid activity_type" });
  }
  if (!["crop","area","location"].includes(scope_type)) {
    return res.status(400).json({ error: "Invalid scope_type" });
  }
  if (activity_type === "other" && !custom_label?.trim()) {
    return res.status(400).json({ error: "custom_label required for activity type 'other'" });
  }

  const now    = performed_at ? new Date(performed_at).toISOString() : new Date().toISOString();
  const userId = req.user.id;
  let nextActionHint = null;

  // ── Area scope (main path from new multi-step UI) ───────────────────────────
  if (scope_type === "area") {
    // Verify ownership of all areas
    const { data: areas } = await supabaseService.from("growing_areas")
      .select("id, name, location_id, locations!inner(user_id)")
      .in("id", scopeIds);
    const ownedAreas = (areas || []).filter(a => a.locations?.user_id === userId);
    if (!ownedAreas.length) return res.status(404).json({ error: "No valid areas found" });
    const ownedIds = ownedAreas.map(a => a.id);

    if (activity_type === "watered") {
      await supabaseService.from("growing_areas")
        .update({ last_watered_at: now }).in("id", ownedIds);
      nextActionHint = `Watering logged for ${ownedIds.length} area${ownedIds.length !== 1 ? "s" : ""}`;

    } else if (activity_type === "fed") {
      // Feed: stamp last_fed_at on all active crops in these areas
      const { data: crops } = await supabaseService.from("crop_instances")
        .select("id, crop_def_id")
        .in("area_id", ownedIds)
        .eq("user_id", userId)
        .eq("active", true);
      if (crops?.length) {
        await supabaseService.from("crop_instances")
          .update({ last_fed_at: now, updated_at: now })
          .in("id", crops.map(c => c.id));
      }
      nextActionHint = `Feeding logged for ${crops?.length || 0} crop${crops?.length !== 1 ? "s" : ""}`;

    } else if (activity_type === "pruned_mulched") {
      await supabaseService.from("growing_areas")
        .update({ last_pruned_or_mulched_at: now }).in("id", ownedIds);
      nextActionHint = `Pruning/mulching logged for ${ownedIds.length} area${ownedIds.length !== 1 ? "s" : ""}`;

    } else if (activity_type === "weeded") {
      await supabaseService.from("growing_areas")
        .update({ last_weeded_at: now }).in("id", ownedIds);
      nextActionHint = `Weeding logged for ${ownedIds.length} area${ownedIds.length !== 1 ? "s" : ""}`;

    } else {
      nextActionHint = "Activity logged";
    }

  // ── Location scope ─────────────────────────────────────────────────────────
  } else if (scope_type === "location") {
    const { data: location } = await supabaseService.from("locations")
      .select("id, name").eq("id", scopeIds[0]).eq("user_id", userId).single();
    if (!location) return res.status(404).json({ error: "Location not found" });

    if (activity_type === "watered") {
      // Stamp all areas in this location
      await supabaseService.from("growing_areas")
        .update({ last_watered_at: now }).eq("location_id", scopeIds[0]);
      nextActionHint = "Watering logged for all areas in this location";
    } else if (activity_type === "fed") {
      const { data: crops } = await supabaseService.from("crop_instances")
        .select("id")
        .eq("location_id", scopeIds[0])
        .eq("user_id", userId)
        .eq("active", true);
      if (crops?.length) {
        await supabaseService.from("crop_instances")
          .update({ last_fed_at: now, updated_at: now })
          .in("id", crops.map(c => c.id));
      }
      nextActionHint = `Feeding logged for ${crops?.length || 0} crops`;
    } else if (activity_type === "pruned_mulched") {
      await supabaseService.from("growing_areas")
        .update({ last_pruned_or_mulched_at: now }).eq("location_id", scopeIds[0]);
      nextActionHint = "Pruning/mulching logged for this location";
    } else if (activity_type === "weeded") {
      await supabaseService.from("growing_areas")
        .update({ last_weeded_at: now }).eq("location_id", scopeIds[0]);
      nextActionHint = "Weeding logged for this location";
    } else {
      nextActionHint = "Activity logged";
    }

  // ── Crop scope — handles single or multiple crop IDs ─────────────────────────
  } else if (scope_type === "crop") {
    if (scopeIds.length === 1) {
      // Single crop — fetch def for feed interval hint
      const { data: crop, error: cropErr } = await req.db.from("crop_instances")
        .select("id, name, area_id, crop_def_id")
        .eq("id", scopeIds[0]).eq("user_id", userId).single();
      if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });

      const updates = { updated_at: now };
      if (activity_type === "watered") {
        updates.last_watered_at = now;
        nextActionHint = "Watering logged — next check suppressed for a couple of days";
      } else if (activity_type === "fed") {
        updates.last_fed_at = now;
        const { data: def } = await supabaseService.from("crop_definitions")
          .select("feed_interval_days").eq("id", crop.crop_def_id).single();
        const interval = def?.feed_interval_days || 14;
        nextActionHint = `Next feed in about ${interval} days`;
      } else if (activity_type === "pruned_mulched") {
        updates.last_pruned_or_mulched_at = now;
        nextActionHint = "Pruning/mulching logged";
      } else if (activity_type === "weeded") {
        updates.last_weeded_at = now;
        nextActionHint = "Weeding logged";
      } else {
        nextActionHint = "Activity logged";
      }
      await supabaseService.from("crop_instances").update(updates).eq("id", scopeIds[0]).eq("user_id", userId);

    } else {
      // Bulk crop update — verify ownership then update all at once
      const { data: crops } = await supabaseService.from("crop_instances")
        .select("id").in("id", scopeIds).eq("user_id", userId).eq("active", true);
      const ownedIds = (crops || []).map(c => c.id);
      if (!ownedIds.length) return res.status(404).json({ error: "No valid crops found" });

      const updates = { updated_at: now };
      if (activity_type === "watered")        updates.last_watered_at = now;
      else if (activity_type === "fed")        updates.last_fed_at = now;
      else if (activity_type === "pruned_mulched") updates.last_pruned_or_mulched_at = now;
      else if (activity_type === "weeded")     updates.last_weeded_at = now;

      await supabaseService.from("crop_instances").update(updates).in("id", ownedIds).eq("user_id", userId);
      nextActionHint = `${activity_type === "fed" ? "Feeding" : "Activity"} logged for ${ownedIds.length} crop${ownedIds.length !== 1 ? "s" : ""}`;
    }
  }

  // Always write to manual_activity_logs (non-fatal if fails)
  try {
    await writeActivityLog(userId, activity_type, scope_type, scopeIds[0], now, notes, custom_label);
  } catch(malErr) { console.error("[ActivityLog] manual_activity_logs insert failed:", malErr.message); }

  // Re-run engine for actions that affect scheduling
  if (activity_type === "watered" || activity_type === "fed") {
    await runRuleEngine(userId);
  }

  res.json({ ok: true, activity_type, scope_type, next_action_hint: nextActionHint });
});


// =============================================================================
// SHARE GARDEN — data endpoint for Share My Garden card
// =============================================================================
app.get("/share/garden-data", requireAuth, async (req, res) => {
  const { mode = "recent" } = req.query;
  const now        = new Date();
  const today      = now.toISOString().split("T")[0];
  const dayOfMonth = now.getDate();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split("T")[0];
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().split("T")[0];
  const prevMonthStart = new Date(now.getFullYear(), now.getMonth() - 1, 1).toISOString().split("T")[0];
  const prevMonthEnd   = new Date(now.getFullYear(), now.getMonth(), 0).toISOString().split("T")[0];
  const threeDaysAgo   = new Date(Date.now() - 3 * 86400000).toISOString().split("T")[0];
  const fourteenDaysAgo = new Date(Date.now() - 14 * 86400000).toISOString().split("T")[0];
  const isEarlyMonth   = dayOfMonth <= 5;

  let completedQuery, upcomingQuery;

  if (mode === "recent") {
    const { data: recent } = await req.db.from("tasks")
      .select("action, task_type, crop:crop_instance_id(name)")
      .eq("user_id", req.user.id)
      .gte("completed_at", threeDaysAgo)
      .not("completed_at", "is", null)
      .order("completed_at", { ascending: false })
      .limit(8);
    let completed = recent || [];
    if (completed.length === 0) {
      const { data: fallback } = await req.db.from("tasks")
        .select("action, task_type, crop:crop_instance_id(name)")
        .eq("user_id", req.user.id)
        .gte("completed_at", fourteenDaysAgo)
        .not("completed_at", "is", null)
        .order("completed_at", { ascending: false })
        .limit(8);
      completed = fallback || [];
    }
    const { data: upcoming } = await req.db.from("tasks")
      .select("action, task_type, due_date, crop:crop_instance_id(name)")
      .eq("user_id", req.user.id)
      .is("completed_at", null)
      .gt("due_date", today)
      .order("due_date", { ascending: true })
      .limit(2);
    completedQuery = completed;
    upcomingQuery  = upcoming || [];
  } else {
    const completedFrom = isEarlyMonth ? prevMonthStart : monthStart;
    const completedTo   = isEarlyMonth ? prevMonthEnd   : today;
    const { data: completed } = await req.db.from("tasks")
      .select("action, task_type, crop:crop_instance_id(name), completed_at")
      .eq("user_id", req.user.id)
      .gte("completed_at", completedFrom)
      .lte("completed_at", completedTo)
      .not("completed_at", "is", null)
      .order("completed_at", { ascending: false })
      .limit(5);
    const { data: upcoming } = await req.db.from("tasks")
      .select("action, task_type, due_date, crop:crop_instance_id(name)")
      .eq("user_id", req.user.id)
      .is("completed_at", null)
      .gt("due_date", today)
      .lte("due_date", monthEnd)
      .order("due_date", { ascending: true })
      .limit(3);
    completedQuery = completed || [];
    upcomingQuery  = upcoming  || [];
  }

  const { data: profile } = await req.db.from("profiles")
    .select("name, postcode").eq("id", req.user.id).single();
  const { count: cropCount } = await req.db.from("crop_instances")
    .select("*", { count: "exact", head: true })
    .eq("user_id", req.user.id).eq("active", true);
  const { count: harvestCount } = await req.db.from("harvest_log")
    .select("*", { count: "exact", head: true })
    .eq("user_id", req.user.id)
    .gte("harvested_at", monthStart);

  processBadgeEvent(req.user.id, "garden_shared").catch(console.error);

  res.json({
    mode,
    is_early_month: isEarlyMonth,
    profile: { name: profile?.name || null, postcode: profile?.postcode || null },
    completed: completedQuery,
    upcoming:  upcomingQuery,
    stats: { crop_count: cropCount || 0, harvest_count: harvestCount || 0, completed_count: completedQuery.length },
    month_name: now.toLocaleString("en-GB", { month: "long" }),
    prev_month_name: new Date(now.getFullYear(), now.getMonth() - 1, 1).toLocaleString("en-GB", { month: "long" }),
  });
});

// =============================================================================
// BADGES & CHALLENGES ENGINE
// =============================================================================

function getCurrentSeason() {
  const m = new Date().getMonth() + 1;
  if (m >= 3 && m <= 5)  return "spring";
  if (m >= 6 && m <= 8)  return "summer";
  if (m >= 9 && m <= 11) return "autumn";
  return "winter";
}

function getMonthKey() {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
}

function getTodayISO() {
  return new Date().toISOString().split("T")[0];
}

async function ensureCounters(userId) {
  const { data } = await supabaseService.from("user_activity_counters").select("user_id").eq("user_id", userId).single();
  if (!data) {
    await supabaseService.from("user_activity_counters").insert({
      user_id: userId,
      current_month_key:  getMonthKey(),
      current_season_key: getCurrentSeason(),
    });
  }
}

async function checkAndResetCounters(userId) {
  const { data: counters } = await supabaseService.from("user_activity_counters").select("*").eq("user_id", userId).single();
  if (!counters) return;
  const updates = {};
  const monthKey  = getMonthKey();
  const seasonKey = getCurrentSeason();
  if (counters.current_month_key !== monthKey) {
    updates.tasks_completed_this_month          = 0;
    updates.sowing_logged_this_month            = 0;
    updates.harvest_logged_this_month           = 0;
    updates.photos_uploaded_this_month          = 0;
    updates.tasks_completed_distinct_days_month = 0;
    updates.active_dates_this_month             = [];
    updates.current_month_key                   = monthKey;
  }
  if (counters.current_season_key !== seasonKey) {
    updates.tasks_completed_this_season  = 0;
    updates.sowing_logged_this_season    = 0;
    updates.current_season_key           = seasonKey;
  }
  if (Object.keys(updates).length > 0) {
    await supabaseService.from("user_activity_counters").update(updates).eq("user_id", userId);
  }
}

async function updateStreak(userId, counters) {
  const today = getTodayISO();
  const last  = counters.last_qualifying_activity_date;
  if (last === today) return {};
  const yesterday = new Date(Date.now() - 86400000).toISOString().split("T")[0];
  const newStreak = last === yesterday ? (counters.current_streak_days || 0) + 1 : 1;
  const longest   = Math.max(counters.longest_streak_days || 0, newStreak);
  return { current_streak_days: newStreak, longest_streak_days: longest, last_qualifying_activity_date: today };
}

async function evaluateBadges(userId, eventType, counters) {
  const unlocks = [];
  const relevantThresholds = {
    task_completed:   ["tasks_completed_total","tasks_completed_this_month","tasks_completed_this_season","tasks_completed_7_days","current_streak_days"],
    crop_added:       ["crops_added_total"],
    area_created:     ["growing_areas_created_total"],
    sow_logged:       ["sowing_logged_total","sowing_logged_this_month","sowing_logged_this_season","current_streak_days"],
    harvest_logged:   ["harvest_logged_total","harvest_logged_this_month","current_streak_days"],
    photo_uploaded:   ["photos_uploaded_total","photos_uploaded_this_month"],
    garden_shared:    ["share_count_total"],
  }[eventType] || [];

  const { data: badges } = await supabaseService.from("badge_definitions").select("*").in("threshold_type", relevantThresholds).eq("is_active", true);
  if (!badges?.length) return unlocks;

  const monthKey  = getMonthKey();
  const seasonKey = getCurrentSeason();

  for (const badge of badges) {
    const { data: existing } = await supabaseService.from("user_badge_progress").select("is_completed, current_progress").eq("user_id", userId).eq("badge_id", badge.id).eq("month_key", badge.time_scope === "monthly" ? monthKey : "").maybeSingle();
    if (existing?.is_completed) continue;
    // Spring Starter: only award during spring (March–May UK)
    if (badge.id === "spring_starter" && seasonKey !== "spring") continue;
    const currentValue = counters[badge.threshold_type] || 0;
    const progress     = Math.min(currentValue, badge.threshold_value);
    const completed    = currentValue >= badge.threshold_value;
    await supabaseService.from("user_badge_progress").upsert({
      user_id: userId, badge_id: badge.id, current_progress: progress, is_completed: completed,
      completed_at: completed ? new Date().toISOString() : null,
      month_key: badge.time_scope === "monthly" ? monthKey : "",
      season_key: badge.time_scope === "seasonal" ? seasonKey : "",
      last_progress_event_at: new Date().toISOString(),
    }, { onConflict: "user_id,badge_id,month_key" });
    if (completed && !existing?.is_completed) {
      await supabaseService.from("badge_unlock_events").insert({
        user_id: userId, badge_id: badge.id,
        month_key: badge.time_scope === "monthly" ? monthKey : null,
        season_key: badge.time_scope === "seasonal" ? seasonKey : null,
      });
      let nextBadge = null;
      if (badge.next_badge_id) {
        const { data: nb } = await supabaseService.from("badge_definitions").select("title").eq("id", badge.next_badge_id).single();
        nextBadge = nb?.title || null;
      }
      unlocks.push({ badge_id: badge.id, title: badge.title, description: badge.description, celebration_copy: badge.celebration_copy, icon_key: badge.icon_key, type: badge.type, next_badge_title: nextBadge });
    }
  }
  return unlocks;
}

async function processBadgeEvent(userId, eventType, extraCounterUpdates = {}) {
  try {
    await ensureCounters(userId);
    await checkAndResetCounters(userId);
    const { data: counters } = await supabaseService.from("user_activity_counters").select("*").eq("user_id", userId).single();
    if (!counters) return [];
    const today    = getTodayISO();
    const updates  = { updated_at: new Date().toISOString(), ...extraCounterUpdates };
    // Recalculate rolling 7-day task count from scratch using tasks table
    const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString();
    const { data: recent7 } = await supabaseService.from("tasks")
      .select("id").eq("user_id", userId).not("completed_at", "is", null).gte("completed_at", sevenDaysAgo);
    const rolling7 = (recent7 || []).length;
    // Write rolling7 into extraCounterUpdates so evaluateBadges sees the live value
    extraCounterUpdates.tasks_completed_7_days = rolling7;
    updates.tasks_completed_7_days = rolling7;
    if (eventType === "task_completed") {
      updates.tasks_completed_total         = (counters.tasks_completed_total        || 0) + 1;
      updates.tasks_completed_this_month    = (counters.tasks_completed_this_month   || 0) + 1;
      updates.tasks_completed_this_season   = (counters.tasks_completed_this_season  || 0) + 1;
      const activeDates = counters.active_dates_this_month || [];
      if (!activeDates.includes(today)) {
        updates.active_dates_this_month             = [...activeDates, today];
        updates.tasks_completed_distinct_days_month = (counters.tasks_completed_distinct_days_month || 0) + 1;
      }
    } else if (eventType === "crop_added") {
      updates.crops_added_total = (counters.crops_added_total || 0) + 1;
    } else if (eventType === "area_created") {
      updates.growing_areas_created_total = (counters.growing_areas_created_total || 0) + 1;
    } else if (eventType === "sow_logged") {
      updates.sowing_logged_total        = (counters.sowing_logged_total       || 0) + 1;
      updates.sowing_logged_this_month   = (counters.sowing_logged_this_month  || 0) + 1;
      updates.sowing_logged_this_season  = (counters.sowing_logged_this_season || 0) + 1;
    } else if (eventType === "harvest_logged") {
      updates.harvest_logged_total       = (counters.harvest_logged_total      || 0) + 1;
      updates.harvest_logged_this_month  = (counters.harvest_logged_this_month || 0) + 1;
    } else if (eventType === "photo_uploaded") {
      updates.photos_uploaded_total      = (counters.photos_uploaded_total     || 0) + 1;
      updates.photos_uploaded_this_month = (counters.photos_uploaded_this_month|| 0) + 1;
    } else if (eventType === "garden_shared") {
      updates.share_count_total = (counters.share_count_total || 0) + 1;
    }
    const qualifyingEvents = ["task_completed","sow_logged","harvest_logged","crop_added","photo_uploaded","garden_shared"];
    if (qualifyingEvents.includes(eventType)) {
      const streakUpdates = await updateStreak(userId, { ...counters, ...updates });
      Object.assign(updates, streakUpdates);
    }
    await supabaseService.from("user_activity_counters").update(updates).eq("user_id", userId);
    const freshCounters = { ...counters, ...updates };
    return await evaluateBadges(userId, eventType, freshCounters);
  } catch (err) {
    console.error("[BadgeEngine]", err.message);
    return [];
  }
}

// GET /badges
app.get("/badges", requireAuth, async (req, res) => {
  try {
    const userId    = req.user.id;
    const monthKey  = getMonthKey();
    const seasonKey = getCurrentSeason();
    await ensureCounters(userId);
    await checkAndResetCounters(userId);
    const [{ data: allBadges }, { data: progress }, { data: counters }, { data: recentUnlocks }] = await Promise.all([
      supabaseService.from("badge_definitions").select("*").eq("is_active", true).order("sort_order"),
      supabaseService.from("user_badge_progress").select("*").eq("user_id", userId),
      supabaseService.from("user_activity_counters").select("*").eq("user_id", userId).single(),
      supabaseService.from("badge_unlock_events").select("*, badge:badge_id(*)").eq("user_id", userId).order("unlocked_at", { ascending: false }).limit(10),
    ]);
    const progressMap = {};
    (progress || []).forEach(p => { progressMap[p.badge_id + (p.month_key || "")] = p; });
    const enriched = (allBadges || []).map(badge => {
      const key = badge.id + (badge.time_scope === "monthly" ? monthKey : "");
      const p   = progressMap[key];
      const currentValue = counters ? (counters[badge.threshold_type] || 0) : 0;
      return { ...badge, current_progress: p?.current_progress ?? Math.min(currentValue, badge.threshold_value), is_completed: p?.is_completed ?? false, completed_at: p?.completed_at ?? null };
    });
    const monthlyChallenge = {
      title: `${new Date().toLocaleString("en-GB", { month: "long" })} Grower`,
      description: `Complete 12 garden tasks in ${new Date().toLocaleString("en-GB", { month: "long" })}`,
      icon_key: "🌿", type: "monthly", threshold: 12,
      progress: counters?.tasks_completed_this_month || 0,
      is_completed: (counters?.tasks_completed_this_month || 0) >= 12,
    };
    res.json({ badges: enriched, counters: counters || {}, recent_unlocks: recentUnlocks || [], monthly_challenge: monthlyChallenge, season: seasonKey, month_key: monthKey });
  } catch (e) {
    console.error("[Badges]", e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /badges/pending-unlocks
app.get("/badges/pending-unlocks", requireAuth, async (req, res) => {
  const { data, error } = await supabaseService.from("badge_unlock_events").select("*, badge:badge_id(*)").eq("user_id", req.user.id).eq("shown_to_user", false).order("unlocked_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// POST /badges/mark-shown
app.post("/badges/mark-shown", requireAuth, async (req, res) => {
  const { ids } = req.body;
  if (!ids?.length) return res.json({ ok: true });
  await supabaseService.from("badge_unlock_events").update({ shown_to_user: true }).in("id", ids).eq("user_id", req.user.id);
  res.json({ ok: true });
});

// =============================================================================
// ADMIN — One-time badge backfill for all existing users
// =============================================================================
app.post("/admin/backfill-badges", requireAuth, requireAdmin, async (req, res) => {
  try {
    const now         = new Date();
    const monthKey    = now.toISOString().slice(0, 7);
    const m           = now.getMonth() + 1;
    const currentSeason = m>=3&&m<=5?"spring":m>=6&&m<=8?"summer":m>=9&&m<=11?"autumn":"winter";
    const monthStart  = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split("T")[0];
    const seasonStart = m>=3&&m<=5?`${now.getFullYear()}-03-01`:m>=6&&m<=8?`${now.getFullYear()}-06-01`:m>=9&&m<=11?`${now.getFullYear()}-09-01`:m===12?`${now.getFullYear()}-12-01`:`${now.getFullYear()-1}-12-01`;
    const today       = now.toISOString().split("T")[0];
    const yesterday   = new Date(Date.now()-86400000).toISOString().split("T")[0];

    const users = await getAllAuthUsers();
    if (!users?.length) return res.json({ ok: true, processed: 0 });

    const [{ data: allTasks }, { data: allCrops }, { data: allLocations }, { data: allAreas }, { data: allHarvests }, { data: allPhotos }, { data: allBadges }] = await Promise.all([
      supabaseService.from("tasks").select("user_id, task_type, completed_at").not("completed_at", "is", null),
      supabaseService.from("crop_instances").select("user_id, active").eq("active", true),
      supabaseService.from("locations").select("id, user_id"),
      supabaseService.from("growing_areas").select("id, location_id"),
      supabaseService.from("harvest_log").select("user_id, harvested_at"),
      supabaseService.from("crop_photos").select("user_id"),
      supabaseService.from("badge_definitions").select("*").eq("is_active", true),
    ]);

    const locUserMap = {};
    (allLocations || []).forEach(l => { locUserMap[l.id] = l.user_id; });

    const counterUpserts = [], badgeUpserts = [], unlockInserts = [], results = [];

    for (const user of users) {
      const uid = user.id;
      const userTasks    = (allTasks    || []).filter(t => t.user_id === uid);
      const userCrops    = (allCrops    || []).filter(c => c.user_id === uid);
      const userHarvests = (allHarvests || []).filter(h => h.user_id === uid);
      const userPhotos   = (allPhotos   || []).filter(p => p.user_id === uid);
      const userLocIds   = (allLocations|| []).filter(l => l.user_id === uid).map(l => l.id);
      const userAreas    = (allAreas    || []).filter(a => userLocIds.includes(a.location_id));

      const tasksTotal  = userTasks.length;
      const tasksMonth  = userTasks.filter(t => t.completed_at >= monthStart).length;
      const tasksSeason = userTasks.filter(t => t.completed_at >= seasonStart).length;
      const sevenDaysAgo = new Date(now - 7 * 86400000).toISOString();
      const tasks7Days  = userTasks.filter(t => t.completed_at >= sevenDaysAgo).length;
      const sowTotal    = userTasks.filter(t => t.task_type === "sow").length;
      const sowMonth    = userTasks.filter(t => t.task_type === "sow" && t.completed_at >= monthStart).length;
      const sowSeason   = userTasks.filter(t => t.task_type === "sow" && t.completed_at >= seasonStart).length;
      const harvestTotal = userHarvests.length;
      const harvestMonth = userHarvests.filter(h => h.harvested_at >= monthStart).length;

      const distinctDays = [...new Set(userTasks.map(t => t.completed_at.split("T")[0]))].sort().reverse();
      let streak = 0, longest = 0, cur = 0;
      for (let i = 0; i < distinctDays.length; i++) {
        if (i === 0) { cur = 1; continue; }
        const diff = (new Date(distinctDays[i-1]) - new Date(distinctDays[i])) / 86400000;
        if (diff === 1) cur++;
        else { longest = Math.max(longest, cur); cur = 1; }
      }
      longest = Math.max(longest, cur);
      if (distinctDays[0] === today || distinctDays[0] === yesterday) streak = cur;
      const activeDatesMonth = distinctDays.filter(d => d >= monthStart);

      const counters = {
        user_id: uid, tasks_completed_total: tasksTotal, tasks_completed_this_month: tasksMonth,
        tasks_completed_this_season: tasksSeason, tasks_completed_distinct_days_month: activeDatesMonth.length,
        crops_added_total: userCrops.length, growing_areas_created_total: userAreas.length,
        sowing_logged_total: sowTotal, sowing_logged_this_month: sowMonth, sowing_logged_this_season: sowSeason,
        harvest_logged_total: harvestTotal, harvest_logged_this_month: harvestMonth,
        photos_uploaded_total: userPhotos.length, share_count_total: 0,
        current_streak_days: streak, longest_streak_days: longest,
        last_qualifying_activity_date: distinctDays[0] || null,
        active_dates_this_month: activeDatesMonth,
        tasks_completed_7_days: tasks7Days,
        current_month_key: monthKey, current_season_key: currentSeason, updated_at: now.toISOString(),
      };
      counterUpserts.push(counters);

      for (const badge of (allBadges || [])) {
        // Spring Starter: only award if user actually sowed in spring — skip in backfill
        // (we can't know the season of historical sows reliably without per-task date checks)
        if (badge.id === "spring_starter") continue;
        const val       = counters[badge.threshold_type] || 0;
        const completed = val >= badge.threshold_value;
        const progress  = Math.min(val, badge.threshold_value);
        if (progress === 0 && !completed) continue;
        badgeUpserts.push({
          user_id: uid, badge_id: badge.id, current_progress: progress, is_completed: completed,
          completed_at: completed ? now.toISOString() : null,
          month_key: badge.time_scope === "monthly" ? monthKey : "",
          season_key: badge.time_scope === "seasonal" ? currentSeason : "",
          last_progress_event_at: now.toISOString(),
        });
        if (completed) {
          unlockInserts.push({ user_id: uid, badge_id: badge.id, month_key: badge.time_scope === "monthly" ? monthKey : null, season_key: badge.time_scope === "seasonal" ? currentSeason : null, shown_to_user: true });
        }
      }
      results.push({ user_id: uid, email: user.email, tasks: tasksTotal, crops: userCrops.length, harvests: harvestTotal });
    }

    await supabaseService.from("user_activity_counters").upsert(counterUpserts, { onConflict: "user_id" });
    if (badgeUpserts.length) {
      await supabaseService.from("user_badge_progress").upsert(badgeUpserts, { onConflict: "user_id,badge_id,month_key" });
    }
    for (const unlock of unlockInserts) {
      await supabaseService.from("badge_unlock_events").upsert(unlock, { onConflict: "user_id,badge_id" }).then(() => {}).catch(() => {});
    }
    res.json({ ok: true, processed: users.length, results });
  } catch (e) {
    console.error("[Backfill]", e.message);
    res.status(500).json({ error: e.message });
  }
});


// =============================================================================
// PUSH NOTIFICATIONS
// =============================================================================
// POST /push/register — register a native device push token (iOS/Android via Capacitor)
// Called automatically on app launch after the user grants notification permission.
app.post("/push/register", requireAuth, async (req, res) => {
  const { token, platform } = req.body;
  if (!token) return res.status(400).json({ error: "token required" });
  const { error } = await supabaseService.from("device_push_tokens").upsert({
    user_id: req.user.id,
    platform: platform || "ios",
    push_token: token,
    endpoint: token,          // reuse endpoint col as unique key for native tokens
    is_active: true,
    last_seen_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }, { onConflict: "user_id,endpoint" });
  if (error) return res.status(500).json({ error: error.message });
  await supabaseService.from("notification_preferences").upsert({
    user_id: req.user.id, push_enabled: true, updated_at: new Date().toISOString(),
  }, { onConflict: "user_id", ignoreDuplicates: true });
  res.json({ ok: true });
});

app.get("/notifications/vapid-key", (_req, res) => {
  if (!process.env.VAPID_PUBLIC_KEY) return res.status(503).json({ error: "Push not configured" });
  res.json({ publicKey: process.env.VAPID_PUBLIC_KEY });
});

app.post("/notifications/register-token", requireAuth, async (req, res) => {
  const { subscription, platform = "web", device_label } = req.body;
  if (!subscription) return res.status(400).json({ error: "subscription required" });
  const endpoint = subscription.endpoint;
  const { error } = await supabaseService.from("device_push_tokens").upsert({
    user_id: req.user.id, platform, push_token: JSON.stringify(subscription),
    endpoint, device_label: device_label || null, is_active: true,
    last_seen_at: new Date().toISOString(), updated_at: new Date().toISOString(),
  }, { onConflict: "user_id,endpoint" });
  if (error) return res.status(500).json({ error: error.message });
  await supabaseService.from("notification_preferences").upsert({
    user_id: req.user.id, push_enabled: true, updated_at: new Date().toISOString(),
  }, { onConflict: "user_id", ignoreDuplicates: true });
  res.json({ ok: true });
});

app.delete("/notifications/token", requireAuth, async (req, res) => {
  const { endpoint } = req.body;
  if (!endpoint) return res.status(400).json({ error: "endpoint required" });
  await supabaseService.from("device_push_tokens").update({ is_active: false }).eq("user_id", req.user.id).eq("endpoint", endpoint);
  res.json({ ok: true });
});

app.get("/notifications/preferences", requireAuth, async (req, res) => {
  const { data } = await supabaseService.from("notification_preferences").select("*").eq("user_id", req.user.id).single();
  res.json(data || { push_enabled: false });
});

app.put("/notifications/preferences", requireAuth, async (req, res) => {
  const allowed = ["push_enabled","due_today_enabled","coming_up_enabled","weather_alerts_enabled","pest_alerts_enabled","crop_checks_enabled","weekly_summary_enabled","daily_plan_enabled","milestones_enabled","morning_time_local","evening_time_local","do_not_disturb_start","do_not_disturb_end","critical_alerts_anytime","timezone"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  updates.updated_at = new Date().toISOString();
  const { error } = await supabaseService.from("notification_preferences").upsert({ user_id: req.user.id, ...updates }, { onConflict: "user_id" });
  if (error) return res.status(500).json({ error: error.message });
  res.json({ ok: true });
});

app.post("/notifications/:id/opened", async (req, res) => {
  await supabaseService.from("notification_events").update({ status: "opened", opened_at: new Date().toISOString() }).eq("id", req.params.id);
  res.json({ ok: true });
});

// ── Shared bulk pre-fetch — used by both push cron handlers ──────────────────
// 3 bulk queries upfront replace thousands of per-user queries.
// Returns eligible user IDs, their tokens, and all their relevant tasks.
async function buildEligibleUserSet(window) {
  const db  = supabaseService;
  const today   = new Date().toISOString().split("T")[0];
  const in3days = new Date(Date.now() + 3 * 86400000).toISOString().split("T")[0];

  // Query 1: all active push tokens
  const { data: tokenRows } = await db
    .from("device_push_tokens")
    .select("user_id, push_token, endpoint")
    .eq("is_active", true);

  if (!tokenRows?.length) return { eligible: [], counts: { total_with_token: 0 }, tokenMap: {}, tasksByUser: new Map() };

  const tokenMap = {};
  for (const row of tokenRows) {
    if (!tokenMap[row.user_id]) tokenMap[row.user_id] = [];
    tokenMap[row.user_id].push({ push_token: row.push_token, endpoint: row.endpoint });
  }
  const usersWithTokens = Object.keys(tokenMap);

  // Query 2: push preferences for all token holders
  const { data: prefRows } = await db
    .from("notification_preferences")
    .select("user_id, push_enabled")
    .in("user_id", usersWithTokens);
  const prefMap = {};
  for (const row of prefRows || []) prefMap[row.user_id] = row.push_enabled;

  // Query 3: already-sent-today dedup
  const windowStart = window === "morning"
    ? new Date().toISOString().split("T")[0] + "T05:00:00.000Z"
    : new Date().toISOString().split("T")[0] + "T15:00:00.000Z";
  const { data: sentRows } = await db
    .from("notification_events")
    .select("user_id")
    .in("status", ["sent", "queued"])
    .gte("created_at", windowStart)
    .in("user_id", usersWithTokens);
  const alreadySentSet = new Set((sentRows || []).map(r => r.user_id));

  const ADMIN_IDS = new Set(["c1c730ff-acb2-4969-9c74-32a84041d9b3"]);

  // Filter to eligible users
  const counts = { total_with_token: usersWithTokens.length, push_disabled: 0, already_sent: 0, eligible: 0 };
  const eligible = [];
  for (const userId of usersWithTokens) {
    const isAdmin = ADMIN_IDS.has(userId);
    if (!isAdmin && !prefMap[userId])          { counts.push_disabled++; continue; }
    if (!isAdmin && alreadySentSet.has(userId)) { counts.already_sent++;  continue; }
    eligible.push(userId);
    counts.eligible++;
  }

  if (!eligible.length) return { eligible: [], counts, tokenMap, tasksByUser: new Map() };

  // Query 4: all relevant tasks for eligible users in one go
  // Fetch enough to cover all priority levels for both morning and evening.
  // Supabase default row limit is 1000 — use range to get all rows.
  const tasksByUser = new Map();
  let taskOffset = 0;
  const TASK_BATCH = 1000;
  while (true) {
    const { data: taskBatch } = await db
      .from("tasks")
      .select("id, user_id, task_type, record_type, rule_id, urgency, status, due_date, completed_at, action, crop:crop_instance_id(name)")
      .in("user_id", eligible)
      .is("completed_at", null)
      .not("status", "eq", "expired")
      .lte("due_date", in3days)
      .range(taskOffset, taskOffset + TASK_BATCH - 1);

    for (const task of taskBatch || []) {
      if (!tasksByUser.has(task.user_id)) tasksByUser.set(task.user_id, []);
      tasksByUser.get(task.user_id).push(task);
    }

    if (!taskBatch || taskBatch.length < TASK_BATCH) break;
    taskOffset += TASK_BATCH;
  }

  return { eligible, counts, tokenMap, tasksByUser };
}

app.post("/cron/push-morning", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { eligible, counts: preCounts, tokenMap, tasksByUser } = await buildEligibleUserSet("morning");
    console.log(`[PushMorning] Pre-filter: ${JSON.stringify(preCounts)}`);
    let sendCounts = { sent: 0, failed: 0, no_candidate: 0 };
    if (!eligible.length) {
      console.log("[PushMorning] No eligible users — done.");
    } else {
      sendCounts = await sendBulkNotifications(supabaseService, eligible, "morning", tokenMap, tasksByUser);
      console.log(`[PushMorning] Eligible=${eligible.length} Sent=${sendCounts.sent} Failed=${sendCounts.failed} Other=${sendCounts.no_candidate}`);
    }
    // Email fallback removed — now handled by POST /cron/weekly-digest (Sundays only)
    res.json({ ok: true, eligible: eligible.length, ...sendCounts });
  } catch(e) {
    captureError("PushMorning", e);
    res.status(500).json({ error: e.message });
  }
});

app.post("/cron/push-evening", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { eligible, counts: preCounts, tokenMap, tasksByUser } = await buildEligibleUserSet("evening");
    console.log(`[PushEvening] Pre-filter: ${JSON.stringify(preCounts)}`);
    let sendCounts = { sent: 0, failed: 0, no_candidate: 0 };
    if (!eligible.length) {
      console.log("[PushEvening] No eligible users — done.");
    } else {
      sendCounts = await sendBulkNotifications(supabaseService, eligible, "evening", tokenMap, tasksByUser);
      console.log(`[PushEvening] Eligible=${eligible.length} Sent=${sendCounts.sent} Failed=${sendCounts.failed} Other=${sendCounts.no_candidate}`);
    }
    res.json({ ok: true, eligible: eligible.length, ...sendCounts });
  } catch(e) {
    captureError("PushEvening", e);
    res.status(500).json({ error: e.message });
  }
});

// POST /cron/push-dry-run — verify eligibility without sending anything
app.post("/cron/push-dry-run", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const window = req.body?.window || "morning";
    const { eligible, counts, tasksByUser } = await buildEligibleUserSet(window);
    const usersWithTasks = [...tasksByUser.keys()].length;
    const totalTasks = [...tasksByUser.values()].reduce((n, arr) => n + arr.length, 0);
    res.json({ ok: true, window, would_send_to: eligible.length, breakdown: counts, tasks_fetched: totalTasks, users_with_tasks: usersWithTasks });
  } catch(e) {
    captureError("PushDryRun", e);
    res.status(500).json({ error: e.message });
  }
});

app.post("/notifications/test", requireAuth, requireAdmin, async (req, res) => {
  const result = await runNotificationsForUser(supabaseService, req.user.id, "morning");
  res.json(result);
});

// =============================================================================
// EMAIL SEQUENCES
// =============================================================================

app.post("/cron/nudge-unactivated", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  res.json({ ok: true, status: "processing" });
  try {
    const result = await runNudgeUnactivated(supabaseService);
    console.log("[NudgeUnactivated]", result);
  } catch(e) { captureError("NudgeUnactivated", e); }
});

app.post("/cron/nudge-unconfirmed", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  res.json({ ok: true, status: "processing" });
  try {
    const result = await runNudgeUnconfirmed(supabaseService);
    console.log("[NudgeUnconfirmed]", result);
  } catch(e) { captureError("NudgeUnconfirmed", e); }
});

app.post("/cron/feedback-sequence", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  res.json({ ok: true, status: "processing" });
  try {
    const result = await runFeedbackSequence(supabaseService);
    console.log("[FeedbackSequence]", result);
  } catch(e) { captureError("FeedbackSequence", e); }
});

app.post("/cron/waitlist-invites", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  res.json({ ok: true, status: "processing" });
  try {
    const result = await runWaitlistInvites(supabaseService);
    console.log("[WaitlistInvites]", result);
  } catch(e) { captureError("WaitlistInvites", e); }
});

app.post("/cron/waitlist-nudges", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runWaitlistNudges(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("WaitlistNudges", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/waitlist-nudges-2", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runWaitlistNudges2(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("WaitlistNudges2", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/waitlist-nudges-3", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runWaitlistNudges3(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("WaitlistNudges3", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/reengagement", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  res.json({ ok: true, status: "processing" });
  try {
    const result = await runReengagement(supabaseService);
    console.log("[Reengagement]", result);
  } catch(e) { captureError("Reengagement", e); }
});

// POST /cron/weekly-digest — Sunday weekly email digest for no-push users with due tasks
// Schedule in vercel.json: { "path": "/cron/weekly-digest", "schedule": "0 8 * * 0" }
// (08:00 UTC every Sunday)
app.post("/cron/weekly-digest", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runWeeklyEmailDigest(supabaseService);
    console.log("[WeeklyDigest]", result);
    res.json({ ok: true, ...result });
  } catch(e) {
    captureError("WeeklyDigest", e);
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// CRON — called by Vercel Cron at 06:00 UTC daily
// Protected by CRON_SECRET header.
// POST /cron/price-ingestion — disabled, prices managed via static seed in produce_price_aggregates
app.post("/cron/price-ingestion", async (req, res) => {
  return res.json({ ok: true, status: "disabled — using static price table" });
});

// Configure in vercel.json: { "crons": [{ "path": "/cron/daily", "schedule": "0 6 * * *" }] }
// =============================================================================

app.post("/cron/daily", async (req, res) => {
  if (req.headers["x-cron-secret"] !== process.env.CRON_SECRET) {
    return res.status(403).json({ error: "Forbidden" });
  }
  const { data: profiles } = await supabaseService.from("profiles").select("id");
  if (!profiles?.length) return res.json({ processed: 0 });
  let total = 0;
  for (const p of profiles) {
    const tasks = await runRuleEngine(p.id);
    total += tasks.length;
  }
  console.log(`[Cron] ${total} tasks generated across ${profiles.length} users`);
  res.json({ processed: profiles.length, tasks_generated: total });
});

// =============================================================================
// ERROR HANDLER + START
// =============================================================================

// ── Sentry test endpoint — remove after confirming Sentry works ───────────────
app.get("/sentry-test", (_req, _res) => {
  throw new Error("Sentry test error — Vercro API staging");
});

// =============================================================================
// REVENUECAT WEBHOOK
// =============================================================================
// RevenueCat calls this endpoint when subscription events occur.
// We update the user's plan in profiles based on the event type.
// Docs: https://www.revenuecat.com/docs/integrations/webhooks

app.post("/webhooks/revenuecat",
  express.raw({ type: "application/json" }), // raw body needed for auth header check
  async (req, res) => {
    try {
      // Verify the request is from RevenueCat using the shared secret
      // RevenueCat sends the secret as a plain string OR as "Bearer <secret>"
      const authHeader = req.headers.authorization;
      const expectedSecret = process.env.REVENUECAT_WEBHOOK_SECRET;
      if (expectedSecret) {
        const isValid = authHeader === expectedSecret ||
                        authHeader === `Bearer ${expectedSecret}`;
        if (!isValid) {
          console.warn("[RevenueCat] Webhook auth failed — header:", authHeader?.slice(0, 20));
          return res.status(401).json({ error: "Unauthorised" });
        }
      }

      const body = req.body;
      const event = body.event;

      if (!event) {
        return res.status(400).json({ error: "No event in payload" });
      }

      const { type, app_user_id, expiration_at_ms, store, offered_product_id, offering_id } = event;

      console.log(`[RevenueCat] Event: ${type} for user: ${app_user_id} offering: ${offering_id}`);

      // Map RevenueCat app_user_id to our Supabase user ID
      const userId = app_user_id;

      // Determine new plan state based on event type
      // INITIAL_PURCHASE, RENEWAL, UNCANCELLATION → pro
      // CANCELLATION, EXPIRATION, BILLING_ISSUE → free
      // NON_SUBSCRIPTION_PURCHASE → pro (lifetime/one-off)

      const proEvents = [
        "INITIAL_PURCHASE",
        "RENEWAL",
        "UNCANCELLATION",
        "NON_SUBSCRIPTION_PURCHASE",
        "SUBSCRIBER_ALIAS",
      ];

      const freeEvents = [
        "CANCELLATION",
        "EXPIRATION",
        "BILLING_ISSUE",
      ];

      let newPlan = null;
      let proExpiresAt = null;

      if (proEvents.includes(type)) {
        newPlan = "pro";
        proExpiresAt = expiration_at_ms
          ? new Date(expiration_at_ms).toISOString()
          : null;
      } else if (freeEvents.includes(type)) {
        newPlan = "free";
        proExpiresAt = null;
      }

      if (!newPlan) {
        // Unhandled event type — acknowledge and ignore
        console.log(`[RevenueCat] Unhandled event type: ${type}`);
        return res.status(200).json({ received: true });
      }

      // Update profile — record which offering/tier was purchased
      const updates = {
        plan:                   newPlan,
        pro_expires_at:         proExpiresAt,
        pro_source:             store || "revenuecat",
        revenuecat_app_user_id: userId,
      };

      // If this is a new purchase, record the offering so we know which tier they paid
      if (newPlan === "pro" && offering_id) {
        console.log(`[RevenueCat] Purchased offering: ${offering_id}`);
        // offering_id will be "loyalty", "early_supporter", or "default"
        // We don't change price_tier here — that was set at subscription time
        // Just log it for audit purposes
        updates.pro_source = `${store || "revenuecat"}:${offering_id}`;
      }

      const { error } = await supabaseService
        .from("profiles")
        .update(updates)
        .eq("id", userId);

      if (error) {
        console.error("[RevenueCat] Profile update error:", error.message);
        // Still return 200 so RevenueCat doesn't retry indefinitely
        return res.status(200).json({ received: true, error: error.message });
      }

      console.log(`[RevenueCat] Updated user ${userId} to plan: ${newPlan}`);
      return res.status(200).json({ received: true });

    } catch (err) {
      console.error("[RevenueCat] Webhook error:", err.message);
      captureError("RevenueCat Webhook", err);
      // Return 200 to prevent RevenueCat retrying on our parsing errors
      return res.status(200).json({ received: true });
    }
  }
);

// =============================================================================
// SUBSCRIPTION STATUS ENDPOINT
// =============================================================================
// Frontend calls this to get current subscription state for the user.

app.get("/subscription/status", requireAuth, async (req, res) => {
  try {
    const { data: profile, error } = await req.db
      .from("profiles")
      .select("plan, pro_expires_at, pro_source")
      .eq("id", req.user.id)
      .single();

    if (error) throw error;

    const isPro = profile?.plan === "pro";
    const isExpired = profile?.pro_expires_at
      ? new Date(profile.pro_expires_at) < new Date()
      : false;

    // If pro but expired, downgrade automatically
    if (isPro && isExpired) {
      await supabaseService
        .from("profiles")
        .update({ plan: "free", pro_expires_at: null })
        .eq("id", req.user.id);

      return res.json({ plan: "free", is_pro: false, expired: true });
    }

    return res.json({
      plan:           profile?.plan || "free",
      is_pro:         isPro && !isExpired,
      pro_expires_at: profile?.pro_expires_at || null,
      pro_source:     profile?.pro_source || null,
    });

  } catch (err) {
    captureError("SubscriptionStatus", err);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// STRIPE — WEB SUBSCRIPTION CHECKOUT
// =============================================================================

// ── Stripe price IDs — all tiers and intervals ───────────────────────────────
const STRIPE_PRICES = {
  loyalty:        { monthly: "price_1TL2MGD44o8wCiOZpMYxjCJV", annual: "price_1TL2MeD44o8wCiOZGML8xig4" },
  early_supporter:{ monthly: "price_1TGz5jD44o8wCiOZIsEcIwBT", annual: "price_1TGz47D44o8wCiOZ9mG7HREJ" },
  standard:       { monthly: "price_1TL2MyD44o8wCiOZQlbK4l1e", annual: "price_1TL2NGD44o8wCiOZG5yJjCKE" },
};

// ── Launch date ───────────────────────────────────────────────────────────────
// Set this to the ISO date string when PRO_ENABLED goes live (e.g. "2026-04-15").
// Users registered on or before this date get loyalty pricing for 28 days.
// Leave empty string until you go live — existing users will be backfilled to
// price_tier=loyalty in profiles at that point.
const LAUNCH_DATE = process.env.LAUNCH_DATE || "";
const LOYALTY_WINDOW_DAYS = 28;

// ── Resolve the correct price tier for a user ─────────────────────────────────
// loyalty    → registered user who subscribes within 28 days of launch
// early_supporter → new user subscribing after launch (or loyalty window closed)
// standard   → full price
function resolveUserPriceTier(profilePriceTier) {
  if (!LAUNCH_DATE) return "early_supporter"; // pre-launch: use early supporter as default
  const launchMs = new Date(LAUNCH_DATE).getTime();
  const nowMs    = Date.now();
  const daysSinceLaunch = (nowMs - launchMs) / 86400000;

  if (profilePriceTier === "loyalty" && daysSinceLaunch <= LOYALTY_WINDOW_DAYS) {
    return "loyalty";
  }
  if (daysSinceLaunch <= 90) return "early_supporter"; // 90-day early supporter window
  return "standard";
}

// GET /subscription/pricing
// Returns the correct prices to show this user in the paywall.
app.get("/subscription/pricing", requireAuth, async (req, res) => {
  try {
    const { data: profile } = await supabaseService
      .from("profiles").select("price_tier").eq("id", req.user.id).single();

    const tier   = resolveUserPriceTier(profile?.price_tier || "standard");
    const prices = STRIPE_PRICES[tier] || STRIPE_PRICES.standard;

    const DISPLAY = {
      loyalty:         { monthly: "£2.99", annual: "£29",  label: "Loyalty offer",         badge: "Your special price" },
      early_supporter: { monthly: "£4.99", annual: "£49",  label: "Early supporter offer", badge: "Best value" },
      standard:        { monthly: "£5.99", annual: "£59",  label: null,                    badge: "Best value" },
    };

    res.json({
      tier,
      monthly_price_id: prices.monthly,
      annual_price_id:  prices.annual,
      display:          DISPLAY[tier],
    });
  } catch (err) {
    captureError("SubscriptionPricing", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /subscription/create-checkout
// Creates a Stripe Checkout session for web subscribers.
// Resolves correct price based on user's price_tier and loyalty window.
app.post("/subscription/create-checkout", requireAuth, async (req, res) => {
  try {
    const { interval = "annual" } = req.body; // "monthly" or "annual"

    // Resolve correct tier and price for this user
    const { data: profile } = await supabaseService
      .from("profiles")
      .select("email, stripe_customer_id, price_tier")
      .eq("id", req.user.id)
      .single();

    const tier    = resolveUserPriceTier(profile?.price_tier || "standard");
    const prices  = STRIPE_PRICES[tier] || STRIPE_PRICES.standard;
    const priceId = interval === "monthly" ? prices.monthly : prices.annual;

    let customerId = profile?.stripe_customer_id;

    if (!customerId) {
      const customer = await stripe.customers.create({
        email: profile?.email || req.user.email,
        metadata: { supabase_user_id: req.user.id },
      });
      customerId = customer.id;
      await supabaseService.from("profiles")
        .update({ stripe_customer_id: customerId }).eq("id", req.user.id);
    }

    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      mode: "subscription",
      payment_method_types: ["card"],
      line_items: [{ price: priceId, quantity: 1 }],
      success_url: "https://app.vercro.com/?subscribed=true",
      cancel_url:  "https://app.vercro.com/?subscription_cancelled=true",
      metadata: { supabase_user_id: req.user.id, tier, interval },
      subscription_data: {
        metadata: { supabase_user_id: req.user.id, tier, interval },
      },
    });

    res.json({ url: session.url, session_id: session.id });
  } catch (err) {
    captureError("StripeCheckout", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /subscription/stripe-webhook
// Stripe calls this when subscription events occur.
// Uses raw body for Stripe signature verification.
app.post("/subscription/stripe-webhook",
  express.raw({ type: "application/json" }),
  async (req, res) => {
    const sig = req.headers["stripe-signature"];
    const webhookSecret = process.env.STRIPE_WEBHOOK_SECRET;

    let event;
    try {
      event = stripe.webhooks.constructEvent(req.body, sig, webhookSecret);
    } catch (err) {
      console.error("[Stripe] Webhook signature failed:", err.message);
      return res.status(400).json({ error: `Webhook error: ${err.message}` });
    }

    console.log(`[Stripe] Event: ${event.type}`);

    try {
      if (event.type === "checkout.session.completed") {
        const session = event.data.object;
        const userId = session.metadata?.supabase_user_id;
        if (userId) {
          await supabaseService.from("profiles").update({
            plan:       "pro",
            pro_source: "stripe",
          }).eq("id", userId);
          console.log(`[Stripe] checkout.session.completed — upgraded user ${userId} to pro`);
        }
      }

      if (event.type === "invoice.payment_succeeded") {
        const invoice = event.data.object;
        const customerId = invoice.customer;
        const { data: profile } = await supabaseService
          .from("profiles")
          .select("id")
          .eq("stripe_customer_id", customerId)
          .single();
        if (profile) {
          const periodEnd = invoice.lines?.data?.[0]?.period?.end;
          await supabaseService.from("profiles").update({
            plan:           "pro",
            pro_source:     "stripe",
            pro_expires_at: periodEnd ? new Date(periodEnd * 1000).toISOString() : null,
          }).eq("id", profile.id);
          console.log(`[Stripe] invoice.payment_succeeded — renewed pro for user ${profile.id}`);
        }
      }

      if (event.type === "customer.subscription.deleted" ||
          event.type === "invoice.payment_failed") {
        const obj = event.data.object;
        const customerId = obj.customer;
        const { data: profile } = await supabaseService
          .from("profiles")
          .select("id")
          .eq("stripe_customer_id", customerId)
          .single();
        if (profile) {
          await supabaseService.from("profiles").update({
            plan:           "free",
            pro_expires_at: null,
          }).eq("id", profile.id);
          console.log(`[Stripe] ${event.type} — downgraded user ${profile.id} to free`);
        }
      }
    } catch (err) {
      console.error("[Stripe] Webhook handler error:", err.message);
      captureError("StripeWebhook", err);
    }

    res.json({ received: true });
  }
);

// GET /subscription/manage
// Returns a Stripe billing portal URL so users can manage/cancel their subscription.
app.get("/subscription/manage", requireAuth, async (req, res) => {
  try {
    const { data: profile } = await supabaseService
      .from("profiles")
      .select("stripe_customer_id")
      .eq("id", req.user.id)
      .single();

    if (!profile?.stripe_customer_id) {
      return res.status(404).json({ error: "No subscription found" });
    }

    const session = await stripe.billingPortal.sessions.create({
      customer:   profile.stripe_customer_id,
      return_url: "https://app.vercro.com/",
    });

    res.json({ url: session.url });
  } catch (err) {
    captureError("StripeManage", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Garden Health Score ───────────────────────────────────────────────────────
// GET /garden/health?location_id=
// Computes a garden health score (0–100) for the given location.
// v1 components: task adherence, timing adherence, weather suitability.
// Returns: score, confidence_level (High/Medium/Low), summary, components.

app.get("/garden/health", requireAuth, async (req, res) => {
  try {
    const { location_id } = req.query;
    const userId = req.user.id;
    const today  = new Date().toISOString().split("T")[0];
    const win14  = new Date(Date.now() - 14 * 86400000).toISOString().split("T")[0];

    // ── 1. Verify location ownership ─────────────────────────────────────────
    let postcode = null;
    if (location_id) {
      const { data: loc } = await supabaseService.from("locations")
        .select("id, postcode").eq("id", location_id).eq("user_id", userId).single();
      if (!loc) return res.status(404).json({ error: "Location not found" });
      postcode = loc.postcode;
    } else {
      const { data: prof } = await supabaseService.from("profiles")
        .select("postcode").eq("id", userId).single();
      postcode = prof?.postcode;
    }

    // ── 2. Task adherence (last 14 days + today) ──────────────────────────────
    let taskQuery = supabaseService.from("tasks")
      .select("id, due_date, completed_at, urgency")
      .eq("user_id", userId)
      .gte("due_date", win14)
      .lte("due_date", today);
    if (location_id) taskQuery = taskQuery.eq("location_id", location_id);

    const { data: windowTasks } = await taskQuery;
    const tasks = windowTasks || [];

    let taskAdherence   = 70;
    let timingAdherence = 70;

    if (tasks.length > 0) {
      const urgencyWeight = u => u === "high" ? 3 : u === "medium" ? 2 : 1;
      let totalWeight = 0, completedWeight = 0;
      let timingTotal = 0, timingWeight = 0;

      for (const t of tasks) {
        const w = urgencyWeight(t.urgency);
        totalWeight += w;
        if (t.completed_at) {
          completedWeight += w;
          const daysLate = Math.round(
            (new Date(t.completed_at).getTime() - new Date(t.due_date).getTime()) / 86400000
          );
          const timingScore =
            daysLate <= 0  ? 100 :
            daysLate <= 3  ? 85  :
            daysLate <= 7  ? 65  :
            daysLate <= 14 ? 40  : 15;
          timingTotal  += timingScore * w;
          timingWeight += w;
        }
      }

      taskAdherence   = Math.round((completedWeight / totalWeight) * 100);
      timingAdherence = timingWeight > 0 ? Math.round(timingTotal / timingWeight) : 70;
    }

    // ── 3. Weather suitability ────────────────────────────────────────────────
    let weatherSuitability = 70;
    let hasWeatherData = false;
    if (postcode) {
      const { data: wx } = await supabaseService.from("weather_cache")
        .select("temp_c, frost_risk, rain_mm")
        .eq("postcode", postcode)
        .gt("expires_at", new Date().toISOString())
        .single();
      if (wx) {
        hasWeatherData = true;
        let wxScore = 80;
        if (wx.frost_risk)                           wxScore -= 25;
        if (wx.temp_c < 5)                           wxScore -= 15;
        else if (wx.temp_c > 30)                     wxScore -= 10;
        if (wx.rain_mm > 20)                         wxScore -= 10;
        else if (wx.rain_mm < 1 && wx.temp_c > 18)  wxScore -= 5;
        weatherSuitability = Math.max(10, Math.min(100, wxScore));
      }
    }

    // ── 4. Observation freshness ──────────────────────────────────────────────
    // Score per active crop: days since last observation log
    let observationFreshness = 60; // default — no observation data
    let hasObservationData   = false;
    {
      // Get active crop IDs for this location
      let cropQuery = supabaseService.from("crop_instances")
        .select("id, area_id")
        .eq("user_id", userId)
        .eq("active", true);

      if (location_id) {
        const { data: areaRows } = await supabaseService.from("growing_areas")
          .select("id").eq("location_id", location_id);
        const areaIds = (areaRows || []).map(a => a.id);
        if (areaIds.length) cropQuery = cropQuery.in("area_id", areaIds);
        else cropQuery = null;
      }

      if (cropQuery) {
        const { data: activeCrops } = await cropQuery;
        const cropIds = (activeCrops || []).map(c => c.id);

        if (cropIds.length > 0) {
          // Most recent observation per crop
          const { data: obsRows } = await supabaseService.from("observation_logs")
            .select("crop_id, observed_at")
            .eq("user_id", userId)
            .in("crop_id", cropIds)
            .order("observed_at", { ascending: false });

          const latestByCrop = {};
          for (const o of (obsRows || [])) {
            if (!latestByCrop[o.crop_id]) latestByCrop[o.crop_id] = o.observed_at;
          }

          const nowMs = Date.now();
          const scores = cropIds.map(id => {
            const lastObs = latestByCrop[id];
            if (!lastObs) return 10; // never observed
            const days = Math.round((nowMs - new Date(lastObs).getTime()) / 86400000);
            return days <= 7  ? 100 :
                   days <= 14 ? 80  :
                   days <= 21 ? 55  :
                   days <= 35 ? 30  : 10;
          });

          observationFreshness = Math.round(scores.reduce((a, b) => a + b, 0) / scores.length);
          hasObservationData   = obsRows?.length > 0;
        }
      }
    }

    // ── 5. Crop condition ─────────────────────────────────────────────────────
    // Derived from crop_instances status and missed_task_note
    let cropCondition   = 75; // default
    let hasCropData     = false;
    {
      let ccQuery = supabaseService.from("crop_instances")
        .select("id, status, missed_task_note, active")
        .eq("user_id", userId)
        .eq("active", true);

      if (location_id) {
        const { data: areaRows } = await supabaseService.from("growing_areas")
          .select("id").eq("location_id", location_id);
        const areaIds = (areaRows || []).map(a => a.id);
        if (areaIds.length) ccQuery = ccQuery.in("area_id", areaIds);
        else ccQuery = null;
      }

      if (ccQuery) {
        const { data: activeCrops } = await ccQuery;
        if (activeCrops?.length > 0) {
          hasCropData = true;
          let total = 0;
          for (const c of activeCrops) {
            let s = 80;
            if (c.missed_task_note) s -= 20;
            if (c.status === "harvesting") s = Math.max(s, 85);
            total += s;
          }
          cropCondition = Math.round(total / activeCrops.length);
        }
      }
    }

    // ── 6. Soil data quality ──────────────────────────────────────────────────
    // Scores how complete and recent area soil data is
    let soilDataQuality = 0;
    let hasSoilData     = false;
    {
      let soilQuery = supabaseService.from("growing_areas")
        .select("soil_moisture, soil_moisture_logged_at, soil_ph, soil_ph_logged_at, soil_temperature_c, soil_temperature_logged_at");
      if (location_id) soilQuery = soilQuery.eq("location_id", location_id);
      else {
        // Get all locations for user
        const { data: locs } = await supabaseService.from("locations").select("id").eq("user_id", userId);
        const locIds = (locs || []).map(l => l.id);
        if (locIds.length) soilQuery = soilQuery.in("location_id", locIds);
      }

      const { data: soilAreas } = await soilQuery;
      if (soilAreas?.length > 0) {
        const nowMs = Date.now();
        const isRecent = (loggedAt, maxDays) =>
          loggedAt && (nowMs - new Date(loggedAt).getTime()) / 86400000 <= maxDays;

        const areaScores = soilAreas.map(a => {
          let fields = 0;
          // moisture: valid 30 days (changes slowly in most UK conditions)
          // temperature: valid 30 days
          // pH: valid 90 days (stable — changes very slowly)
          if (a.soil_moisture && isRecent(a.soil_moisture_logged_at, 30))          fields++;
          if (a.soil_temperature_c !== null && isRecent(a.soil_temperature_logged_at, 30)) fields++;
          if (a.soil_ph !== null && isRecent(a.soil_ph_logged_at, 90))              fields++;
          if (fields > 0) hasSoilData = true;
          return (fields / 3) * 100;
        });

        // Also count areas with ANY soil data (even older) as having data
        if (!hasSoilData) {
          hasSoilData = soilAreas.some(a =>
            a.soil_moisture || a.soil_temperature_c !== null || a.soil_ph !== null
          );
        }

        soilDataQuality = Math.round(areaScores.reduce((a, b) => a + b, 0) / areaScores.length);
      }
    }

    // ── 7. Final weighted score (extended) ────────────────────────────────────
    // Weights: task 30%, timing 20%, observation 10%, soil 10%, weather 10%, crop condition 10%
    // Remaining 10% distributed back to task+timing when soil/obs missing
    const score = Math.round(
      0.30 * taskAdherence   +
      0.20 * timingAdherence +
      0.15 * observationFreshness +
      0.10 * cropCondition   +
      0.15 * weatherSuitability +
      0.10 * soilDataQuality
    );

    // ── 8. Confidence level (extended) ───────────────────────────────────────
    const confInputs = [
      { weight: 0.30, score: tasks.length >= 3 ? 1 : tasks.length >= 1 ? 0.5 : 0 },
      { weight: 0.20, score: hasObservationData ? 1 : 0.2 },
      { weight: 0.20, score: hasSoilData ? 1 : 0 },
      { weight: 0.15, score: hasWeatherData ? 1 : 0.4 },
      { weight: 0.15, score: hasCropData ? 0.8 : 0.2 },
    ];
    const confRaw = confInputs.reduce((sum, c) => sum + c.weight * c.score, 0) * 100;
    const confidence_level =
      confRaw >= 70 ? "High" : confRaw >= 40 ? "Medium" : "Low";

    const confidenceNote =
      !hasObservationData && !hasSoilData ? "Log crop observations and soil data to improve accuracy" :
      !hasSoilData                         ? "Add soil data to your areas to improve accuracy" :
      !hasObservationData                  ? "Log crop observations to improve accuracy" :
      confidence_level === "High"          ? "Based on tasks, observations, soil data and weather" :
                                             "Based on recent activity and weather";

    // ── 9. Summary copy ───────────────────────────────────────────────────────
    const summary =
      score >= 80 ? "Your garden is in good shape — keep it up." :
      score >= 65 ? "Good progress, but a few tasks could use attention." :
      score >= 50 ? "Some missed tasks may be affecting your garden's condition." :
                    "Several tasks are overdue — your garden needs attention.";

    // ── 10. Risk flags ────────────────────────────────────────────────────────
    const risk_flags = [];
    if (taskAdherence < 50)          risk_flags.push("Several tasks are overdue");
    if (timingAdherence < 50)        risk_flags.push("Task timing has been consistently late");
    if (observationFreshness < 40)   risk_flags.push("Crops haven't been checked recently");
    if (!hasSoilData)                risk_flags.push("No soil data — health score is an estimate");
    if (weatherSuitability < 50)     risk_flags.push("Current weather conditions are challenging");

    res.json({
      score:            Math.max(0, Math.min(100, score)),
      confidence_level,
      confidence_note:  confidenceNote,
      summary,
      risk_flags,
      components: {
        task_adherence:       taskAdherence,
        timing_adherence:     timingAdherence,
        observation_freshness: observationFreshness,
        crop_condition:       cropCondition,
        weather_suitability:  weatherSuitability,
        soil_data_quality:    soilDataQuality,
      },
    });
  } catch (err) {
    captureError("GardenHealth", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Plan Quality Score ───────────────────────────────────────────────────────
// GET /garden/plan-quality?plan_id=
// Computes quality metrics for a draft or committed garden plan.
// Returns: score, label, rotation_quality, space_efficiency, effort_level,
//          yield_potential, confidence_level, risk_flags

app.get("/garden/plan-quality", requireAuth, async (req, res) => {
  try {
    const { plan_id } = req.query;
    if (!plan_id) return res.status(400).json({ error: "plan_id required" });
    const userId = req.user.id;

    // ── 1. Load plan + verify ownership ──────────────────────────────────────
    const { data: plan, error: planErr } = await supabaseService.from("garden_plans")
      .select("id, location_id, name, status")
      .eq("id", plan_id).eq("user_id", userId).single();
    if (planErr || !plan) return res.status(404).json({ error: "Plan not found" });

    // ── 2. Load plan assignments with crop category ───────────────────────────
    const { data: assignments } = await supabaseService.from("plan_area_assignments")
      .select("area_id, crop_name, crop_definition:crop_definitions(name, category, days_to_maturity_min, days_to_maturity_max)")
      .eq("plan_id", plan_id);

    // ── 3. Load all areas for location (for space efficiency) ─────────────────
    const { data: allAreas } = await supabaseService.from("growing_areas")
      .select("id, width_m, length_m, type")
      .eq("location_id", plan.location_id);

    // ── 4. Load crop history per area (for rotation quality) ──────────────────
    const areaIds = (allAreas || []).map(a => a.id);
    let cropHistory = []; // { area_id, category }
    if (areaIds.length) {
      const { data: hist } = await supabaseService.from("crop_instances")
        .select("area_id, crop_definitions(category)")
        .eq("user_id", userId)
        .in("area_id", areaIds)
        .eq("status", "harvested")
        .not("harvested_at", "is", null)
        .order("harvested_at", { ascending: false });

      // Keep only most recent per area
      const seen = new Set();
      for (const c of (hist || [])) {
        if (!seen.has(c.area_id)) {
          seen.add(c.area_id);
          cropHistory.push({ area_id: c.area_id, category: c.crop_definitions?.category || null });
        }
      }
    }

    const assignArr  = assignments || [];
    const assignMap  = Object.fromEntries(assignArr.map(a => [a.area_id, a]));
    const histMap    = Object.fromEntries(cropHistory.map(h => [h.area_id, h.category]));
    const totalAreas = allAreas?.length || 0;
    const plannedAreas = assignArr.length;

    // ── 5. Rotation quality ───────────────────────────────────────────────────
    // Start at 100, apply penalties for same-family repeats and bonuses for diversity
    let rotationScore = 100;
    const familiesUsed = new Set();
    let legumePlanned  = false;

    for (const a of assignArr) {
      const plannedCat = a.crop_definition?.category || null;
      const prevCat    = histMap[a.area_id] || null;
      if (plannedCat) familiesUsed.add(plannedCat);
      if (plannedCat === "legume") legumePlanned = true;

      if (plannedCat && prevCat) {
        if (plannedCat === prevCat)                                    rotationScore -= 30;
        else if (plannedCat === "brassica" && prevCat === "brassica")  rotationScore -= 20;
        else if (plannedCat === "fruiting" && prevCat === "fruiting")  rotationScore -= 15;
      }
    }

    // Bonus: good diversity
    if (familiesUsed.size >= 3) rotationScore += 5;
    if (legumePlanned)          rotationScore += 5;
    // Penalty: no assignments at all
    if (plannedAreas === 0)     rotationScore  = 50;

    rotationScore = Math.max(0, Math.min(100, rotationScore));

    // ── 6. Space efficiency ───────────────────────────────────────────────────
    let spaceEfficiency = 0;
    if (totalAreas > 0) {
      // Area-based: what fraction of available areas have a crop planned
      const occupancy = plannedAreas / totalAreas;

      // If we have dimensions, use m²
      const totalM2   = (allAreas || []).reduce((s, a) => s + ((a.width_m || 0) * (a.length_m || 0)), 0);
      const plannedM2 = assignArr.reduce((s, a) => {
        const area = (allAreas || []).find(ar => ar.id === a.area_id);
        return s + ((area?.width_m || 0) * (area?.length_m || 0));
      }, 0);

      spaceEfficiency = totalM2 > 0
        ? Math.round((plannedM2 / totalM2) * 100)
        : Math.round(occupancy * 100);

      // Cap at 95 — 100% is theoretically overplanted
      spaceEfficiency = Math.min(95, spaceEfficiency);
    }

    // ── 7. Effort level ───────────────────────────────────────────────────────
    // Based on crop categories — fruiting/brassica = higher effort, salad/herb = lower
    const effortByCategory = {
      fruiting: 3, brassica: 3, root: 2, allium: 2,
      legume: 2, salad: 1, herb: 1, perennial: 1, fruit: 2,
    };
    let effortTotal = 0, effortCount = 0;
    for (const a of assignArr) {
      const cat = a.crop_definition?.category;
      if (cat && effortByCategory[cat] !== undefined) {
        effortTotal += effortByCategory[cat];
        effortCount++;
      }
    }
    const effortAvg = effortCount > 0 ? effortTotal / effortCount : 2;
    const effort_level =
      effortAvg >= 2.5 ? "High" : effortAvg >= 1.7 ? "Moderate" : "Low";
    const effort_score = Math.round((1 - (effortAvg - 1) / 2) * 100); // invert for 0-100

    // ── 8. Yield potential ────────────────────────────────────────────────────
    // Directional — based on crop selection and rotation quality
    let yieldScore = 60; // baseline
    if (assignArr.length > 0) {
      // Fruiting and root crops tend to yield more by weight
      const highYieldCats = new Set(["fruiting", "root"]);
      const highYieldCount = assignArr.filter(a => highYieldCats.has(a.crop_definition?.category)).length;
      yieldScore += (highYieldCount / assignArr.length) * 20;
      // Rotation bonus
      yieldScore += (rotationScore / 100) * 15;
      // Space utilisation
      yieldScore += (spaceEfficiency / 100) * 10;
    }
    yieldScore = Math.round(Math.max(0, Math.min(100, yieldScore)));
    const yield_potential =
      yieldScore >= 75 ? "Strong" : yieldScore >= 55 ? "Moderate" : "Limited";

    // ── 9. Plan quality score (composite) ────────────────────────────────────
    // Weights: rotation 35%, yield 25%, space 20%, effort 10%, assignments 10%
    const assignmentCompleteness = totalAreas > 0 ? Math.min(100, (plannedAreas / totalAreas) * 100) : 0;
    const planScore = Math.round(
      0.35 * rotationScore          +
      0.25 * yieldScore             +
      0.20 * spaceEfficiency        +
      0.10 * effort_score           +
      0.10 * assignmentCompleteness
    );

    const plan_label =
      planScore >= 85 ? "Strong"   :
      planScore >= 70 ? "Good"     :
      planScore >= 50 ? "Balanced" : "Needs attention";

    // ── 10. Confidence ────────────────────────────────────────────────────────
    const hasHistory     = cropHistory.length > 0;
    const hasDimensions  = (allAreas || []).some(a => a.width_m && a.length_m);
    const confRaw =
      (plannedAreas > 0 ? 0.4 : 0) +
      (hasHistory       ? 0.3 : 0) +
      (hasDimensions    ? 0.2 : 0) +
      0.1; // base
    const confidence_level =
      confRaw >= 0.8 ? "High" : confRaw >= 0.5 ? "Medium" : "Low";

    // ── 11. Risk flags ────────────────────────────────────────────────────────
    const risk_flags = [];
    if (plannedAreas === 0)       risk_flags.push("No crops assigned to areas yet");
    if (rotationScore < 60)       risk_flags.push("Repeated crop families increase disease risk");
    if (spaceEfficiency < 40)     risk_flags.push("Most areas are unplanned — space underused");
    if (!hasHistory)              risk_flags.push("No crop history — rotation quality is estimated");
    if (plannedAreas < totalAreas * 0.5 && totalAreas > 1)
                                  risk_flags.push(`${totalAreas - plannedAreas} area${totalAreas - plannedAreas > 1 ? "s" : ""} without a planned crop`);

    res.json({
      score:             Math.max(0, Math.min(100, planScore)),
      label:             plan_label,
      confidence_level,
      rotation_quality:  rotationScore,
      space_efficiency:  spaceEfficiency,
      effort_level,
      yield_potential,
      yield_score:       yieldScore,
      risk_flags,
      meta: {
        planned_areas:  plannedAreas,
        total_areas:    totalAreas,
        has_history:    hasHistory,
        has_dimensions: hasDimensions,
      },
    });
  } catch (err) {
    captureError("PlanQuality", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Garden Plans CRUD ─────────────────────────────────────────────────────────

app.get("/plans", requireAuth, async (req, res) => {
  try {
    const { data, error } = await req.db
      .from("garden_plans").select("*").eq("user_id", req.user.id).order("created_at", { ascending: false });
    if (error) throw error;
    res.json(data || []);
  } catch (err) { captureError("GetPlans", err); res.status(500).json({ error: err.message }); }
});

app.post("/plans", requireAuth, async (req, res) => {
  try {
    const { location_id, name, effective_from_date } = req.body;
    if (!location_id) return res.status(400).json({ error: "location_id required" });
    if (!name || !name.trim()) return res.status(400).json({ error: "name required" });
    const { data: loc, error: locErr } = await req.db
      .from("locations").select("id").eq("id", location_id).eq("user_id", req.user.id).single();
    if (locErr || !loc) return res.status(403).json({ error: "Location not found" });
    const { data, error } = await req.db.from("garden_plans")
      .insert({ user_id: req.user.id, location_id, name: name.trim(), status: "draft", effective_from_date: effective_from_date || null })
      .select().single();
    if (error) throw error;
    res.json(data);
  } catch (err) { captureError("CreatePlan", err); res.status(500).json({ error: err.message }); }
});

app.put("/plans/:id", requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, effective_from_date, status } = req.body;
    if (status && !["draft", "archived"].includes(status))
      return res.status(400).json({ error: "Use POST /plans/:id/commit to commit a plan" });
    const updates = { updated_at: new Date().toISOString() };
    if (name !== undefined) updates.name = name.trim();
    if (effective_from_date !== undefined) updates.effective_from_date = effective_from_date || null;
    if (status !== undefined) updates.status = status;
    const { data, error } = await req.db.from("garden_plans")
      .update(updates).eq("id", id).eq("user_id", req.user.id).select().single();
    if (error) throw error;
    if (!data) return res.status(404).json({ error: "Plan not found" });
    res.json(data);
  } catch (err) { captureError("UpdatePlan", err); res.status(500).json({ error: err.message }); }
});

app.delete("/plans/:id", requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { error } = await req.db.from("garden_plans").delete().eq("id", id).eq("user_id", req.user.id);
    if (error) throw error;
    res.json({ ok: true });
  } catch (err) { captureError("DeletePlan", err); res.status(500).json({ error: err.message }); }
});

app.post("/plans/:id/commit", requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { data: plan, error: planErr } = await req.db
      .from("garden_plans").select("*").eq("id", id).eq("user_id", req.user.id).single();
    if (planErr || !plan) return res.status(404).json({ error: "Plan not found" });
    await req.db.from("garden_plans")
      .update({ status: "archived", updated_at: new Date().toISOString() })
      .eq("user_id", req.user.id).eq("location_id", plan.location_id).eq("status", "committed").neq("id", id);
    const { data, error } = await req.db.from("garden_plans")
      .update({ status: "committed", updated_at: new Date().toISOString() })
      .eq("id", id).eq("user_id", req.user.id).select().single();
    if (error) throw error;
    res.json(data);
  } catch (err) { captureError("CommitPlan", err); res.status(500).json({ error: err.message }); }
});

app.get("/plans/:id/assignments", requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { data: plan, error: planErr } = await req.db
      .from("garden_plans").select("id").eq("id", id).eq("user_id", req.user.id).single();
    if (planErr || !plan) return res.status(404).json({ error: "Plan not found" });
    const { data, error } = await req.db.from("plan_area_assignments")
      .select("*, crop_definition:crop_definitions(id, name, category), area:growing_areas(id, name, location_id)")
      .eq("plan_id", id).order("sequence_order", { ascending: true });
    if (error) throw error;
    res.json(data || []);
  } catch (err) { captureError("GetPlanAssignments", err); res.status(500).json({ error: err.message }); }
});

app.post("/plans/:id/assignments", requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const { area_id, crop_definition_id, crop_name, variety_id, planned_start_date, planned_end_date, sequence_order, notes } = req.body;
    if (!area_id) return res.status(400).json({ error: "area_id required" });
    const { data: plan, error: planErr } = await req.db
      .from("garden_plans").select("id, location_id").eq("id", id).eq("user_id", req.user.id).single();
    if (planErr || !plan) return res.status(404).json({ error: "Plan not found" });
    const { data: area, error: areaErr } = await req.db
      .from("growing_areas").select("id, location_id").eq("id", area_id).single();
    if (areaErr || !area) return res.status(404).json({ error: "Area not found" });
    if (area.location_id !== plan.location_id) return res.status(400).json({ error: "Area does not belong to this plan's location" });
    const { data, error } = await req.db.from("plan_area_assignments")
      .upsert({ plan_id: id, area_id, crop_definition_id: crop_definition_id || null, crop_name: crop_name || null, variety_id: variety_id || null, planned_start_date: planned_start_date || null, planned_end_date: planned_end_date || null, sequence_order: sequence_order ?? null, notes: notes || null }, { onConflict: "plan_id,area_id" })
      .select().single();
    if (error) throw error;
    res.json(data);
  } catch (err) { captureError("UpsertPlanAssignment", err); res.status(500).json({ error: err.message }); }
});

app.delete("/plans/:id/assignments/:assignmentId", requireAuth, async (req, res) => {
  try {
    const { id, assignmentId } = req.params;
    const { data: plan, error: planErr } = await req.db
      .from("garden_plans").select("id").eq("id", id).eq("user_id", req.user.id).single();
    if (planErr || !plan) return res.status(404).json({ error: "Plan not found" });
    const { error } = await req.db.from("plan_area_assignments").delete().eq("id", assignmentId).eq("plan_id", id);
    if (error) throw error;
    res.json({ ok: true });
  } catch (err) { captureError("DeletePlanAssignment", err); res.status(500).json({ error: err.message }); }
});

// ── Plan Engine helpers ──────────────────────────────────────────────────────
// ── Popular UK crops by category — used when user has no suitable gap crop ─────
const POPULAR_BY_CATEGORY = {
  root:     ["Carrot","Parsnip","Beetroot","Radish"],
  brassica: ["Kale","Cabbage","Broccoli","Cauliflower"],
  allium:   ["Onion","Leek","Spring Onion","Garlic"],
  legume:   ["Pea","Broad Bean","French Bean","Runner Bean"],
  fruiting: ["Tomato","Courgette","Pepper","Cucumber"],
  salad:    ["Lettuce","Spinach","Rocket","Mixed Salad Leaves"],
};

const ROTATION_NEXT = {
  brassica: ["legume","root","allium","fruiting"],
  fruiting: ["legume","brassica","root","allium"],
  root:     ["legume","allium","fruiting","brassica"],
  allium:   ["legume","fruiting","root","brassica"],
  legume:   ["brassica","fruiting","root","allium"],
  salad:    ["legume","root","allium","fruiting"],
};

const YIELD_SCORE = { fruiting:9, brassica:7, root:7, legume:6, allium:5, salad:5, herb:3, flower:1, fruit:4, perennial:2 };
const EASE_SCORE  = { salad:9, herb:8, legume:8, allium:7, root:6, brassica:5, fruiting:5, flower:7, fruit:5, perennial:6 };

// ── Area type grouping ─────────────────────────────────────────────────────────
function _areaGroup(area) {
  if (area.type === "container")                              return "container";
  if (area.type === "greenhouse" || area.type === "polytunnel") return "protected";
  if (area.type === "open_ground")                            return "open_ground";
  return "raised_bed";
}

// ── Build display name with variety ───────────────────────────────────────────
function _displayName(cropDef, varietyMap) {
  if (!cropDef) return null;
  const vars   = varietyMap[cropDef.id] || [];
  const defVar = vars.find(v => v.is_default) || vars[0] || null;
  return defVar ? `${cropDef.name} (${defVar.name})` : cropDef.name;
}

// ── Build baseline — same crops, rotated within same area group ────────────────
function _buildBaseline({ areas, activeCropsByArea, sequenceByArea, cropDefs, varietyMap, historyByArea = {}, planYear = null }) {
  const FIXED_CATEGORIES = new Set(["fruit","perennial"]);

  // Reference year for 3-year history: the year being planned for (next season)
  const refYear = planYear || (new Date().getFullYear() + 1);

  // History penalty: how badly we want to avoid repeating a category in a bed
  // that had that same category N years before the planned season.
  // year-1 = most recent = strongest penalty; falls off over 3 years.
  const HISTORY_PENALTIES = { 1: -12, 2: -6, 3: -3 };

  function _historyPenalty(bedId, slotCategory) {
    if (!slotCategory) return 0;
    const history = historyByArea[bedId] || [];
    let penalty = 0;
    for (const h of history) {
      if (h.category !== slotCategory) continue;
      const yearsAgo = refYear - h.year;
      if (yearsAgo >= 1 && yearsAgo <= 3) {
        // Take the worst (most negative) penalty if multiple entries match the same distance
        const p = HISTORY_PENALTIES[yearsAgo] || 0;
        if (p < penalty) penalty = p;
      }
    }
    return penalty;
  }

  const assignments = [];

  const groups = {};
  for (const area of areas) {
    const g = _areaGroup(area);
    if (!groups[g]) groups[g] = [];
    groups[g].push(area);
  }

  for (const [groupName, groupAreas] of Object.entries(groups)) {

    // ── Containers — keep as-is ───────────────────────────────────────────────
    if (groupName === "container") {
      for (const area of groupAreas) {
        const crops   = activeCropsByArea[area.id] || [];
        const display = crops.length ? crops.map(c => c.name).join(" + ") : "As current";
        assignments.push({
          area_id: area.id, area_name: area.name, area_type: area.type, area_group: groupName,
          category: crops[0]?.category || "herb", crop_definition_id: crops[0]?.crop_def_id || null,
          crop_name: display, locked: true, is_fixed: false,
        });
      }
      continue;
    }

    // ── Fixed areas (all fruit/perennial) — never move ────────────────────────
    const fixedAreas     = groupAreas.filter(a => {
      const crops = activeCropsByArea[a.id] || [];
      return crops.length > 0 && crops.every(c => FIXED_CATEGORIES.has(c.category));
    });
    const rotatableAreas = groupAreas.filter(a => !fixedAreas.includes(a));

    for (const area of fixedAreas) {
      const crops = activeCropsByArea[area.id] || [];
      assignments.push({
        area_id: area.id, area_name: area.name, area_type: area.type, area_group: groupName,
        category: crops[0].category, crop_definition_id: crops[0].crop_def_id || null,
        crop_name: crops.map(c => c.name).join(" + "), locked: true, is_fixed: true,
      });
    }

    if (!rotatableAreas.length) continue;

    // ── Build one slot per rotatable bed from sequenceByArea ─────────────────
    // sequenceByArea is the authoritative source — it has already resolved
    // primary crops and committed follow-ons correctly.
    const slots = rotatableAreas.map(area => {
      const seq     = sequenceByArea[area.id] || {};
      const primary = seq.primary || null;

      if (!primary) {
        return {
          sourceAreaId: area.id,
          primary:      null,
          followOns:    [],
          displayName:  "To be decided",
          category:     null,
          cropDefId:    null,
          harvestStart: null,
          harvestEnd:   null,
        };
      }

      const followOns   = seq.followOns || [];
      const cropDef     = cropDefs.find(d => d.id === primary.crop_def_id);
      // Use variety from the instance itself — not varietyMap default — to preserve distinct varieties
      const instanceVariety = primary.variety_name || null;
      const baseName    = instanceVariety
        ? `${primary.name} (${instanceVariety})`
        : (_displayName(cropDef, varietyMap) || primary.name);
      const followLabel = followOns.length ? followOns.map(f => f.name).join(" + ") : null;

      return {
        sourceAreaId: area.id,
        primary,
        followOns,
        displayName:  followLabel ? `${primary.name} → then ${followLabel}` : baseName,
        category:     primary.category,
        cropDefId:    cropDef?.id || null,
        harvestStart: cropDef?.harvest_month_start || null,
        harvestEnd:   cropDef?.harvest_month_end   || null,
      };
    });

    const n = rotatableAreas.length;

    // ── Build score matrix: score[bi][si] = goodness of slot si in bed bi ────
    const score = rotatableAreas.map((bed, bi) => {
      // What category is currently in this bed?
      const curCat = sequenceByArea[bed.id]?.primary?.category || null;

      return slots.map((slot, si) => {
        if (!slot.primary) return 1;
        const sameBed      = slot.sourceAreaId === bed.id ? -20 : 0;
        const catPenalty   = slot.category === curCat      ? -10 : 0;
        const rotBonus     = (ROTATION_NEXT[curCat] || []).indexOf(slot.category) >= 0 ? 6 : 0;
        // 3-year history: penalise repeating the same category in the destination bed
        const histPenalty  = _historyPenalty(bed.id, slot.category);
        return 5 + sameBed + catPenalty + rotBonus + histPenalty;
      });
    });

    // ── 1-to-1 assignment: each slot → exactly one bed, each bed → one slot ──
    const pairs = [];
    for (let bi = 0; bi < n; bi++)
      for (let si = 0; si < n; si++)
        pairs.push({ bi, si, sc: score[bi][si] });
    pairs.sort((a, b) => b.sc - a.sc);

    const assignedBeds  = new Set();
    const assignedSlots = new Set();
    const bedToSlot     = new Array(n).fill(null);

    for (const { bi, si } of pairs) {
      if (assignedBeds.has(bi) || assignedSlots.has(si)) continue;
      bedToSlot[bi] = si;
      assignedBeds.add(bi);
      assignedSlots.add(si);
      if (assignedBeds.size === n) break;
    }

    // ── Force rotation: if any slot ended up in its source bed, swap it ──────
    // This guarantees every crop moves, even when scoring produces no better option.
    let swapAttempts = 0;
    let madeSwap = true;
    while (madeSwap && swapAttempts < n) {
      madeSwap = false;
      swapAttempts++;
      for (let bi = 0; bi < n; bi++) {
        const si = bedToSlot[bi];
        if (si === null) continue;
        const slot = slots[si];
        if (!slot.primary) continue;
        if (slot.sourceAreaId !== rotatableAreas[bi].id) continue;
        // This slot is still in its source bed — swap with any neighbour
        const swapTarget = (bi + 1) % n;
        const swapSi = bedToSlot[swapTarget];
        bedToSlot[bi]         = swapSi;
        bedToSlot[swapTarget] = si;
        madeSwap = true;
        break;
      }
    }

    // ── Emit assignments ──────────────────────────────────────────────────────
    for (let bi = 0; bi < n; bi++) {
      const bed  = rotatableAreas[bi];
      const si   = bedToSlot[bi];
      const slot = si !== null ? slots[si] : null;

      if (!slot || !slot.primary) {
        assignments.push({
          area_id: bed.id, area_name: bed.name, area_type: bed.type, area_group: groupName,
          category: null, crop_definition_id: null,
          crop_name: "To be decided", locked: false, is_fixed: false, _empty: true,
        });
        continue;
      }

      assignments.push({
        area_id:             bed.id,
        area_name:           bed.name,
        area_type:           bed.type,
        area_group:          groupName,
        category:            slot.category,
        crop_definition_id:  slot.cropDefId,
        crop_name:           slot.displayName,
        harvest_month_start: slot.harvestStart,
        harvest_month_end:   slot.harvestEnd,
        // Carry structured follow-ons for gap-fill and improvement logic
        followOns:           slot.followOns,
        locked:              false,
        is_fixed:            false,
      });
    }
  }

  return assignments;
}

// ── Gap finder ─────────────────────────────────────────────────────────────────
function _findGaps(baselineAssignments, sequenceByArea) {
  const gaps = [];
  for (const a of baselineAssignments) {
    if (a.locked || a.is_fixed || a._empty) continue;
    if (a.area_group === "container")       continue;
    const harvestEnd = a.harvest_month_end;
    if (!harvestEnd) continue;
    const seq     = sequenceByArea[a.area_id];
    const nextSow = seq?.followOns?.[0]?.sow_month || null;
    const gapStart = harvestEnd > 12 ? 1 : harvestEnd + 1;
    const gapEnd   = nextSow ? nextSow - 1 : 3;
    if (gapEnd >= gapStart && (gapEnd - gapStart) >= 1) {
      gaps.push({
        area_id:          a.area_id,
        area_name:        a.area_name,
        area_type:        a.area_type,
        area_group:       a.area_group,
        gap_start_month:  gapStart,
        gap_end_month:    gapEnd,
        after_crop:       a.crop_name,
        current_category: a.category,
      });
    }
  }
  return gaps;
}

// ── Pick a gap-fill crop ───────────────────────────────────────────────────────
function _pickGapCrop(gap, cropDefs, userCropNames, usedGapNames, usedGapCategories) {
  const eligible = cropDefs.filter(d => {
    if (!d.harvest_month_start || !d.harvest_month_end) return false;
    if (d.harvest_month_start < gap.gap_start_month)   return false;
    if (d.harvest_month_end   > gap.gap_end_month)     return false;
    if (usedGapNames.has(d.name))                      return false;
    if (usedGapCategories.has(d.category))             return false;
    if (d.category === gap.current_category)           return false;
    return true;
  });
  if (!eligible.length) return null;
  const fromUser = eligible.filter(d => userCropNames.has(d.name));
  if (fromUser.length) return fromUser[0];
  for (const names of Object.values(POPULAR_BY_CATEGORY)) {
    for (const name of names) {
      const found = eligible.find(d => d.name === name);
      if (found) return found;
    }
  }
  return eligible[0];
}

// ── Generate targeted improvements ────────────────────────────────────────────
function _generateImprovements({ baselineAssignments, cropDefs, varietyMap, userCropNames, preference, improveCount }) {
  if (!improveCount || improveCount < 1) return [];

  const candidates = baselineAssignments.filter(a =>
    !a.locked && !a.is_fixed && !a._empty && a.area_group !== "container"
  );
  if (!candidates.length) return [];

  const scored = candidates.map(a => {
    const s = preference === "yield" ? (YIELD_SCORE[a.category]||5)
            : preference === "easy"  ? (EASE_SCORE[a.category]||5)
            : (YIELD_SCORE[a.category]||5) * 0.5 + (EASE_SCORE[a.category]||5) * 0.5;
    return { a, currentScore: s };
  }).sort((a,b) => a.currentScore - b.currentScore);

  const improvements     = [];
  const usedCategories   = new Set(baselineAssignments.map(a => a.category));
  const usedCropNames    = new Set(baselineAssignments.map(a => (a.crop_name||"").split(" (")[0].trim()));

  for (const { a } of scored) {
    if (improvements.length >= improveCount) break;

    const targetCats = preference === "yield"
      ? ["fruiting","root","brassica","legume","allium","salad"]
      : preference === "easy"
      ? ["salad","legume","allium","root","brassica","fruiting"]
      : ["legume","root","allium","brassica","fruiting","salad"];

    let bestDef = null;
    for (const targetCat of targetCats) {
      if (targetCat === a.category)       continue;
      if (usedCategories.has(targetCat))  continue;

      // Prefer user's crops
      const userCrop = cropDefs.find(d =>
        d.category === targetCat &&
        userCropNames.has(d.name) &&
        !usedCropNames.has(d.name)
      );
      if (userCrop) { bestDef = userCrop; break; }

      // Popular crops
      for (const name of (POPULAR_BY_CATEGORY[targetCat]||[])) {
        const found = cropDefs.find(d => d.name === name && !usedCropNames.has(d.name));
        if (found) { bestDef = found; break; }
      }
      if (bestDef) break;
    }

    if (!bestDef) continue;

    const display = _displayName(bestDef, varietyMap) || bestDef.name;

    // Reason string
    let reason;
    if (preference === "yield") {
      const gain = (YIELD_SCORE[bestDef.category]||5) - (YIELD_SCORE[a.category]||5);
      reason = gain > 0
        ? `Higher yield — roughly ${gain * 12}% more food from this bed`
        : "Better use of this growing space";
    } else if (preference === "easy") {
      const easierBy = (EASE_SCORE[bestDef.category]||5) - (EASE_SCORE[a.category]||5);
      reason = easierBy > 0
        ? `Easier to manage — less work than ${a.category}`
        : "Lower effort crop for this bed";
    } else {
      reason = "Improves rotation and overall garden balance";
    }

    usedCategories.add(bestDef.category);
    usedCropNames.add(bestDef.name);

    improvements.push({
      area_id:            a.area_id,
      area_name:          a.area_name,
      from_crop:          a.crop_name,
      from_category:      a.category,
      to_crop:            display,
      to_category:        bestDef.category,
      crop_definition_id: bestDef.id,
      harvest_month_start: bestDef.harvest_month_start || null,
      harvest_month_end:   bestDef.harvest_month_end   || null,
      reason,
    });
  }

  return improvements;
}

// ── Build passive tip ──────────────────────────────────────────────────────────
function _buildTip(baselineAssignments, cropDefs, varietyMap) {
  const candidates = baselineAssignments.filter(a =>
    !a.locked && !a.is_fixed && !a._empty && a.area_group !== "container"
  );
  if (!candidates.length) return null;

  const usedCats  = new Set(baselineAssignments.map(a => a.category));
  const usedNames = new Set(baselineAssignments.map(a => (a.crop_name||"").split(" (")[0].trim()));

  const worst = [...candidates].sort((a,b) =>
    (YIELD_SCORE[a.category]||5) - (YIELD_SCORE[b.category]||5)
  )[0];

  for (const cat of ["fruiting","root","brassica","legume"]) {
    if (cat === worst.category || usedCats.has(cat)) continue;
    const def = cropDefs.find(d => d.category === cat && !usedNames.has(d.name));
    if (!def) continue;
    const display = _displayName(def, varietyMap) || def.name;
    const gain    = ((YIELD_SCORE[cat]||5) - (YIELD_SCORE[worst.category]||5)) * 12;
    if (gain > 0) {
      return `Swapping ${worst.area_name} to ${display} could increase yield by around ${gain}%.`;
    }
  }
  return null;
}


// ── Plan Generation ───────────────────────────────────────────────────────────
// POST /plans/generate
// Core principle: DEFAULT = rotate current crops, not invent a new garden.
// Only "suggest_new" goal opens the crop pool to alternatives.

// ── Constraints ───────────────────────────────────────────────────────────────

// Categories never assigned to raised beds in standard rotation
const INVASIVE_CATEGORIES = new Set(["herb", "flower", "perennial", "fruit"]);

// Categories that are fixed — never moved, never included in rotation suggestions
const FIXED_CATEGORIES = new Set(["fruit", "perennial"]);

// Crops never suggested in raised beds (containment-only or invasive)
const RAISED_BED_EXCLUDE_NAMES = new Set([
  "Mint", "Lemon Balm", "Comfrey", "Horseradish", "Jerusalem Artichoke",
  "Rhubarb", "Asparagus", "Fennel",
]);

// Max times any single crop can appear across a plan
const CROP_MAX_AREAS = {
  default:    1,   // most crops: 1 area max
  Potato:     2,   // potatoes are commonly grown in bulk
  Courgette:  1,
  Garlic:     2,
  Onion:      2,
  Leek:       1,
  Carrot:     1,
  Beetroot:   1,
  Pea:        2,
  Bean:       2,
};

// Area type pools — what crops are appropriate for each area type
// Based on area name heuristics (no area_type column exists)
function _classifyArea(area) {
  // Use area.type from DB first — most reliable
  if (area.type === "container")               return "pot";
  if (area.type === "greenhouse" ||
      area.type === "polytunnel")              return "greenhouse";
  // Fall back to name/size heuristics
  const n    = (area.name || "").toLowerCase();
  const size = (area.width_m || 1) * (area.length_m || 1);
  if (n.includes("pot") || n.includes("tub") || size < 0.4) return "pot";
  if (n.includes("allotment") || n.includes("perennial") || n.includes("fruit")) return "fixed";
  return "bed";
}

// Rotation scoring — does this category follow well from the previous?
const ROTATION_AVOID = {
  brassica: ["brassica"],
  fruiting: ["fruiting"],
  root:     ["root"],
  allium:   ["allium"],
  legume:   [],
  salad:    [],
};

const ROTATION_PREFER_AFTER = {
  brassica: ["legume"],
  fruiting: ["legume", "root"],
  root:     ["legume", "salad"],
  allium:   ["fruiting", "root"],
  legume:   ["brassica", "fruiting", "root", "allium"],
  salad:    ["legume", "root"],
};

function _scoreRotation(curCat, nextCat) {
  if (!curCat) return 5;
  if (FIXED_CATEGORIES.has(curCat)) return 5;
  if ((ROTATION_AVOID[curCat] || []).includes(nextCat)) return 0;
  if ((ROTATION_PREFER_AFTER[nextCat] || []).includes(curCat)) return 10;
  const idx = (ROTATION_NEXT[curCat] || []).indexOf(nextCat);
  if (idx === -1) return 3;
  return Math.max(1, 8 - idx * 2);
}

// Pick the best matching crop definition for a crop name from user's current crops
function _findCropDef(cropName, cropDefs) {
  if (!cropName) return null;
  const exact = cropDefs.find(d => d.name.toLowerCase() === cropName.toLowerCase());
  if (exact) return exact;
  const partial = cropDefs.find(d => d.name.toLowerCase().includes(cropName.toLowerCase()) ||
    cropName.toLowerCase().includes(d.name.toLowerCase()));
  return partial || null;
}

// Pick a crop def for a category, preferring user's current crops
function _pickCropForCategory(category, cropDefs, preferredNames = [], usedNames = new Set()) {
  // First: prefer a crop from user's current garden in this category that hasn't been used
  for (const name of preferredNames) {
    const def = cropDefs.find(d => d.category === category &&
      d.name.toLowerCase() === name.toLowerCase() &&
      !usedNames.has(d.name) &&
      !RAISED_BED_EXCLUDE_NAMES.has(d.name));
    if (def) return def;
  }
  // Second: any crop in category not yet used
  const candidates = cropDefs.filter(d =>
    d.category === category &&
    !usedNames.has(d.name) &&
    !RAISED_BED_EXCLUDE_NAMES.has(d.name) &&
    (d.sow_direct_start || d.sow_indoors_start)
  );
  if (candidates.length) return candidates[0];
  // Fallback: any in category
  return cropDefs.find(d => d.category === category && !RAISED_BED_EXCLUDE_NAMES.has(d.name)) || null;
}

// Build display name with variety
function _displayName(cropDef, varietyMap) {
  if (!cropDef) return null;
  const vars = varietyMap[cropDef.id] || [];
  const defaultVar = vars.find(v => v.is_default) || vars[0] || null;
  return defaultVar ? `${cropDef.name} (${defaultVar.name})` : cropDef.name;
}

// ── Core generation function ──────────────────────────────────────────────────
// currentCropsByArea: { area_id: [{ name, category, crop_def_id }] }
// rotatableAreas: areas classified as "bed" type
// Returns array of plan option objects

// ── Plan scoring helpers ──────────────────────────────────────────────────────

const CROP_EFFORT_BY_CATEGORY = {
  salad:4, herb:3, legume:3, allium:4, root:4, brassica:6, fruiting:8, fruit:5, perennial:3, flower:2,
};

const YIELD_PER_M2_BY_CATEGORY = {
  salad:3.5, herb:1.5, legume:1.8, allium:3.0, root:4.5,
  brassica:3.0, fruiting:5.5, fruit:3.5, perennial:2.5, flower:0.5,
};

const EFFORT_RANK   = { "Easy":0, "Moderate":1, "High":2 };
const ROTATION_RANK = { "Excellent":3, "Good":2, "Fair":1, "Weak":0 };
const SPREAD_RANK   = { "Excellent":3, "Good":2, "Short Peak":1, "Heavy Mid-Season":0 };

// Crops allowed to appear in multiple full beds
const MULTI_BED_ALLOWED = ["Potato","Potatoes","Carrot","Carrots","Onion","Onions","Lettuce","Broad Bean","Broad beans","Pea","Peas","Runner Bean","Runner beans"];

// Duplicate penalty applied during plan scoring — soft constraint, not hard block
function _duplicatePenalty(cropName, usedCropNames) {
  if (MULTI_BED_ALLOWED.some(n => cropName?.toLowerCase().includes(n.toLowerCase()))) return 0;
  const count = [...usedCropNames].filter(n => n === cropName).length;
  return count * 15; // 15 point penalty per duplicate
}

// Fix 4: value note explaining yield/value mismatch
function _getValueNote(metrics) {
  if (!metrics.shop_value_gbp || !metrics.harvest_kg) return null;
  const valuePerKg = metrics.shop_value_gbp / metrics.harvest_kg;
  if (valuePerKg < 1.5) return "Higher yield but lower-value crops (e.g. potatoes, roots).";
  if (valuePerKg > 3.5) return "Includes higher-value crops like berries and salads.";
  return null;
}

function _calcEffortLevel(assignments) {
  if (!assignments.length) return { level:"Easy", index:20 };
  const totalArea      = assignments.reduce((s,a) => s+(a._area_m2||1), 0);
  const weightedEffort = assignments.reduce((s,a) => s+(CROP_EFFORT_BY_CATEGORY[a.category]||4)*(a._area_m2||1), 0);
  const index = totalArea > 0 ? Math.round((weightedEffort/totalArea)*10) : 40;
  return { level: index<=25?"Easy":index<=55?"Moderate":"High", index };
}

function _calcRotationScore(assignments, sequenceByArea) {
  let score=0, count=0;
  for (const a of assignments) {
    const prevFamily = sequenceByArea[a.area_id]?.primary?.family || sequenceByArea[a.area_id]?.primary?.category || null;
    const newFamily  = a.family || a.category || null;
    if (!prevFamily || !newFamily) { score+=5; count++; continue; }
    if (prevFamily === newFamily)  { score+=0; }
    else if ((ROTATION_NEXT[prevFamily]||[]).indexOf(newFamily) >= 0) { score+=8; }
    else { score+=4; }
    count++;
  }
  const index = count ? Math.round((score/count)*12.5) : 50;
  return { level: index>=80?"Excellent":index>=60?"Good":index>=40?"Fair":"Weak", index };
}

function _calcHarvestSpread(assignments) {
  const coverage = new Array(12).fill(0);
  for (const a of assignments) {
    const hs = (a._harvest_month_start||1)-1;
    const he = (a._harvest_month_end||12)-1;
    for (let m=hs; m<=he; m++) coverage[m]++;
  }
  const activeMonths  = coverage.filter(v=>v>0).length;
  const maxInAnyMonth = Math.max(...coverage);
  const glutPenalty   = maxInAnyMonth>5 ? (maxInAnyMonth-5)*3 : 0;
  const index = Math.min(100, Math.max(0, Math.round((activeMonths/12)*100) - glutPenalty));
  return { level: index>=75?"Excellent":index>=50?"Good":index>=30?"Short Peak":"Heavy Mid-Season", index, active_months:activeMonths };
}

function _buildExplanation(archetypeGoal, metrics, vsBaseline) {
  const harvestUp  = vsBaseline?.harvest_kg_delta > 0;
  const harvestStr = harvestUp ? ` Expected harvest up ${vsBaseline.harvest_kg_delta.toFixed(1)}kg vs your current layout.` : "";
  if (archetypeGoal==="rotate_mine"||archetypeGoal==="balanced")
    return `Best overall balance of harvest, value, effort and crop rotation for your garden.${harvestStr}`;
  if (archetypeGoal==="max_yield"||archetypeGoal==="max_harvest")
    return `Highest total harvest and shop value. Makes more productive use of your space, but ${metrics.effort_level==="High"?"needs more work through peak season":"with manageable effort"}.`;
  if (archetypeGoal==="easy")
    return `Simplest to manage with lower weekly effort. Fewer demanding crops — still makes good use of your space.`;
  return "Tailored to your garden.";
}

// ── Diff helper — compare option assignments against baseline ─────────────────
const REASON_TAGS = {
  "rotate_mine":    { crop_swap: "better_rotation",  sequence_adjustment: "better_rotation"  },
  "max_yield":      { crop_swap: "higher_yield",      sequence_adjustment: "better_space_use" },
  "easy":           { crop_swap: "lower_effort",      sequence_adjustment: "simpler_layout"   },
  "best_rotation":  { crop_swap: "healthier_soil",    sequence_adjustment: "better_rotation"  },
  "balanced":       { crop_swap: "better_balance",    sequence_adjustment: "maintains_rotation"},
  "favourites":     { crop_swap: "keeps_favourites",  sequence_adjustment: "preference_match" },
};
const REASON_TEXT = {
  higher_yield:       "Higher yield in this bed",
  better_space_use:   "Better use of growing space",
  lower_effort:       "Lower maintenance crop",
  simpler_layout:     "Simpler follow-on crop",
  better_rotation:    "Improves crop rotation",
  healthier_soil:     "Better for soil health",
  better_balance:     "Improves overall balance",
  maintains_rotation: "Keeps rotation on track",
  keeps_favourites:   "Matches your preferences",
  preference_match:   "Matches your preferences",
};

function _diffFromBaseline(optionAssignments, baselineAssignments, planGoal) {
  const reasonMap = REASON_TAGS[planGoal] || REASON_TAGS["balanced"];
  const changes   = [];

  // Build baseline lookup by area_id
  const baselineByArea = {};
  for (const a of baselineAssignments) baselineByArea[a.area_id] = a;

  for (const a of optionAssignments) {
    const base = baselineByArea[a.area_id];
    if (!base) continue; // new area, skip

    // Normalise crop name for comparison — strip parentheticals and variety suffixes
    const normBase   = (base.crop_name   || "").replace(/\s*\(.*?\)/g,"").replace(/\s*→.*/,"").trim().toLowerCase();
    const normOption = (a.crop_name      || "").replace(/\s*\(.*?\)/g,"").replace(/\s*→.*/,"").trim().toLowerCase();

    if (normBase === normOption) continue; // no change

    // Determine change type
    const hasSequence = (a.crop_name || "").includes("→") || (base.crop_name || "").includes("→");
    const changeType  = hasSequence ? "sequence_adjustment" : "crop_swap";
    const tag         = reasonMap[changeType] || "better_balance";

    changes.push({
      area_id:    a.area_id,
      area_name:  a.area_name,
      change_type: changeType,
      from_label: base.crop_name   || base.category  || "—",
      to_label:   a.crop_name      || a.category     || "—",
      reason_tag:  tag,
      reason_text: REASON_TEXT[tag] || "Adjusted for your goal",
    });
  }

  const changeCount = changes.length;
  const goalLabel   = planGoal === "max_yield" ? "more food" :
                      planGoal === "easy"       ? "less work" :
                      planGoal === "best_rotation" ? "healthier soil" : "better balance";

  const changeSummary = changeCount === 0 ? "No layout changes — keeps your rotated baseline" :
                        changeCount === 1 ? `Changes 1 area for ${goalLabel}` :
                        `Changes ${changeCount} areas for ${goalLabel}`;

  return { changes, change_count: changeCount, change_summary: changeSummary };
}

function _recommendPlan(scoredOptions) {
  const balanced = scoredOptions.find(o => o.id==="balanced");
  const maxYield = scoredOptions.find(o => o.id==="max_harvest");
  const easiest  = scoredOptions.find(o => o.id==="easiest");
  if (!balanced) return scoredOptions[0]?.id || "balanced";
  let recommended = balanced;
  if (maxYield) {
    const harvestGain   = (maxYield.metrics.harvest_kg||0)    - (balanced.metrics.harvest_kg||0);
    const effortDelta   = (EFFORT_RANK[maxYield.metrics.effort_level]||1)    - (EFFORT_RANK[balanced.metrics.effort_level]||1);
    const rotationDelta = (ROTATION_RANK[maxYield.metrics.rotation_level]||2) - (ROTATION_RANK[balanced.metrics.rotation_level]||2);
    const valueDelta    = (maxYield.metrics.shop_value_gbp||0) - (balanced.metrics.shop_value_gbp||0);
    if (harvestGain >= 4 && effortDelta <= 1 && rotationDelta >= -1 && valueDelta >= -10) recommended = maxYield;
  }
  if (easiest && recommended === balanced) {
    const effortDrop  = (EFFORT_RANK[balanced.metrics.effort_level]||1) - (EFFORT_RANK[easiest.metrics.effort_level]||1);
    const harvestLoss = (balanced.metrics.harvest_kg||0) - (easiest.metrics.harvest_kg||0);
    if (effortDrop >= 1 && harvestLoss <= 3) recommended = easiest;
  }
  return recommended.id;
}

function _generatePlanOptions(goal, areas, currentCropsByArea, cropDefs, varietyMap, preferredNames, sequenceByArea = {}) {
  // Separate areas by type
  const bedAreas   = areas.filter(a => _classifyArea(a) === "bed");
  const potAreas   = areas.filter(a => _classifyArea(a) === "pot");
  const fixedAreas = areas.filter(a => ["fixed", "greenhouse"].includes(_classifyArea(a)));

  // Beds with existing planned follow-ons are locked — respect the user's own sequence
  // e.g. Bed 5: Garlic (active) → Swede + Brussels Sprout (planned) should not be overwritten
  const lockedBedIds = new Set();
  for (const area of bedAreas) {
    const seq = sequenceByArea[area.id];
    if (seq && seq.primary && seq.followOns && seq.followOns.length > 0) {
      lockedBedIds.add(area.id);
    }
  }

  // Free beds = beds the generator can reassign
  const freeBedAreas   = bedAreas.filter(a => !lockedBedIds.has(a.id));
  const lockedBedAreas = bedAreas.filter(a =>  lockedBedIds.has(a.id));

  // Current category per bed area (use most common category in that area)
  const currentCatByArea = {};
  for (const area of freeBedAreas) {
    const crops = currentCropsByArea[area.id] || [];
    const cats  = crops.map(c => c.category).filter(c => c && !FIXED_CATEGORIES.has(c));
    if (cats.length) {
      const counts = {};
      cats.forEach(c => counts[c] = (counts[c]||0)+1);
      currentCatByArea[area.id] = Object.entries(counts).sort((a,b)=>b[1]-a[1])[0][0];
    }
  }

  // Current named crops in bed areas — these are the crops we rotate by default
  const currentBedCrops = [];
  for (const area of freeBedAreas) {
    for (const crop of (currentCropsByArea[area.id] || [])) {
      if (!FIXED_CATEGORIES.has(crop.category) && !INVASIVE_CATEGORIES.has(crop.category)) {
        if (!currentBedCrops.find(c => c.name === crop.name)) {
          currentBedCrops.push(crop);
        }
      }
    }
  }

  // Build category → best crop name mapping from user's current crops
  const userCropsByCategory = {};
  for (const crop of currentBedCrops) {
    if (!userCropsByCategory[crop.category]) userCropsByCategory[crop.category] = [];
    userCropsByCategory[crop.category].push(crop.name);
  }

  // Rotation categories in use by user (plus legume if not present — always useful)
  const userCategories = [...new Set(currentBedCrops.map(c => c.category))];
  if (!userCategories.includes("legume")) userCategories.push("legume");

  function _buildOption(name, bedAssignments, potAssignments) {
    const assignments = [];
    const usedCropNames = new Set();
    let totalRot=0, totalYield=0, totalEase=0, count=0;

    // First add locked bed assignments (user's existing planned sequences — honoured as-is)
    for (const area of lockedBedAreas) {
      const seq = sequenceByArea[area.id];
      const primary = seq.primary;
      const followLabel = seq.followOns.map(f => f.name).join(" + ");
      const displayName = followLabel ? `${primary.name} → then ${followLabel}` : primary.name;
      const cropDef = _findCropDef(primary.name, cropDefs);
      const curCat  = currentCatByArea[area.id];
      const rotScore = _scoreRotation(curCat, primary.category);
      totalRot   += rotScore;
      totalYield += YIELD_SCORE[primary.category] || 5;
      totalEase  += EASE_SCORE[primary.category]  || 5;
      count++;
      if (cropDef) usedCropNames.add(cropDef.name);
      assignments.push({
        area_id:            area.id,
        area_name:          area.name,
        category:           primary.category || "unknown",
        crop_definition_id: cropDef?.id || null,
        variety_id:         null,
        crop_name:          displayName,
        crop_emoji:         "🌱",
        rotation_score:     rotScore,
      });
    }

    for (const { area, category, cropName } of bedAssignments) {
      const curCat = currentCatByArea[area.id];
      const rotScore = _scoreRotation(curCat, category);
      totalRot   += rotScore;
      totalYield += YIELD_SCORE[category] || 5;
      totalEase  += EASE_SCORE[category]  || 5;
      count++;

      let cropDef = null;
      if (cropName) cropDef = _findCropDef(cropName, cropDefs);
      if (!cropDef) cropDef = _pickCropForCategory(category, cropDefs, userCropsByCategory[category] || preferredNames, usedCropNames);
      if (cropDef) usedCropNames.add(cropDef.name);

      const display = cropDef ? _displayName(cropDef, varietyMap) : (category || "TBC");
      assignments.push({
        area_id:            area.id,
        area_name:          area.name,
        category:           category || "unknown",
        crop_definition_id: cropDef?.id || null,
        variety_id:         null,
        crop_name:          display || category,
        crop_emoji:         "🌱",
        rotation_score:     rotScore,
      });
    }

    // Pot assignments — keep current
    for (const { area, cropName, category } of (potAssignments || [])) {
      const cropDef = cropName ? _findCropDef(cropName, cropDefs) : null;
      const display = cropDef ? _displayName(cropDef, varietyMap) : (cropName || "As current");
      assignments.push({
        area_id:            area.id,
        area_name:          area.name,
        category:           category || "pot",
        crop_definition_id: cropDef?.id || null,
        variety_id:         null,
        crop_name:          display || cropName || "As current",
        crop_emoji:         "🪴",
        rotation_score:     5,
      });
    }

    const n = count || 1;
    return {
      name,
      goal,
      assignments,
      fixed_areas: fixedAreas.map(a => ({ area_id:a.id, area_name:a.name, note:"Fixed — kept as-is" })),
      scores: {
        rotation: Math.round(totalRot/n),
        yield:    Math.round(totalYield/n),
        ease:     Math.round(totalEase/n),
        overall:  Math.round((totalRot+totalYield+totalEase)/(n*3)),
      },
    };
  }

  // Pot handling — keep current crops or suggest compact alternatives
  function _potAssignments() {
    return potAreas.map(area => {
      const crops  = currentCropsByArea[area.id] || [];
      const active = crops.filter(c => !["fruit","perennial"].includes(c.category));
      const fixed  = crops.filter(c =>  ["fruit","perennial"].includes(c.category));
      const display = active.length ? active.map(c=>c.name).join(" + ") :
                      fixed.length  ? fixed.map(c=>c.name).join(" + ") : "Herbs / as current";
      return { area, cropName: display, category: crops[0]?.category || "herb" };
    });
  }

  // ── ROTATE MINE / BEST ROTATION ──────────────────────────────────────────────
  // Core: keep user's crop universe, just move to better rotational positions
  if (goal === "rotate_mine" || goal === "best_rotation") {
    // Core principle: preserve the crops the user already grows, move them to better beds.
    // Respect existing sequences (e.g. Bed 5: Garlic → Sprouts + Swede already planned).
    // Each bed gets a primary crop + optional follow-on string.

    // Step 1: build the rotation pool from current BED crops only (not pots/fixed)
    // Deduplicate by name, preserve order of importance (most beds = most important)
    const cropBedCount = {};
    for (const area of freeBedAreas) {
      const seq = sequenceByArea[area.id];
      if (seq && seq.primary) {
        const n = seq.primary.name;
        cropBedCount[n] = (cropBedCount[n] || 0) + 1;
      }
    }

    // Pool: unique primary crops from bed areas, sorted by how many beds they occupy
    const rotationPool = Object.entries(cropBedCount)
      .sort((a,b) => b[1] - a[1])
      .map(([name]) => {
        const seq = Object.values(sequenceByArea).find(s => s.primary && s.primary.name === name);
        return seq ? seq.primary : { name, category: null };
      });

    // If pool is smaller than bed count, pad with legume (soil improver)
    while (rotationPool.length < freeBedAreas.length) {
      const legDef = _pickCropForCategory("legume", cropDefs, preferredNames, new Set(rotationPool.map(c=>c.name)));
      if (legDef) rotationPool.push({ name: legDef.name, category: "legume" });
      else break;
    }

    // Step 2: helper to build follow-on string for a crop's typical follow-ons
    function _followOnLabel(cropName, seq) {
      // If this exact sequence is already planned in user's garden, use it
      const existingSeq = Object.values(sequenceByArea).find(s =>
        s.primary && s.primary.name === cropName && s.followOns && s.followOns.length
      );
      if (existingSeq) return existingSeq.followOns.map(f => f.name).join(" + ");
      // Otherwise derive sensible follow-on from rotation logic
      const cat = seq && seq.category;
      const followCats = (ROTATION_NEXT[cat] || []).slice(0, 2);
      if (!followCats.length) return null;
      const followCrops = followCats.map(fc => {
        const def = _pickCropForCategory(fc, cropDefs, preferredNames, new Set([cropName]));
        return def ? def.name : null;
      }).filter(Boolean);
      return followCrops.length ? followCrops.join(" + ") : null;
    }

    // Step 3: assign crops to beds — rotate away from current category
    function _rotateCrops(variantName, biasYield = false, injectLegume = false) {
      const pool = [...rotationPool];
      if (injectLegume && !pool.find(c => c.category === "legume")) {
        const ld = _pickCropForCategory("legume", cropDefs, preferredNames, new Set(pool.map(c=>c.name)));
        if (ld) pool.push({ name: ld.name, category: "legume" });
      }

      const usedIdx = new Set();
      const bedAssignments = [];
      for (const area of freeBedAreas) {
        const curCat = currentCatByArea[area.id];
        const usedCropNames = bedAssignments.map(b => b.cropName).filter(Boolean);

        const scored = pool.map((crop, i) => {
          if (usedIdx.has(i)) return { i, score: -1 };
          const rotScore  = _scoreRotation(curCat, crop.category);
          const yieldBias = biasYield ? (YIELD_SCORE[crop.category] || 5) * 0.3 : 0;
          const dupPenalty = _duplicatePenalty(crop.name, usedCropNames);
          return { i, crop, score: rotScore * 0.7 + yieldBias - dupPenalty };
        }).filter(s => s.score >= 0).sort((a, b) => b.score - a.score);

        const best = scored[0];
        if (!best) { bedAssignments.push({ area, category: curCat || "root", cropName: null }); continue; }
        usedIdx.add(best.i);

        const followOn = _followOnLabel(best.crop.name, best.crop);
        const displayName = followOn ? `${best.crop.name} → then ${followOn}` : best.crop.name;

        bedAssignments.push({ area, category: best.crop.category || "root", cropName: displayName });
      }

      return _buildOption(variantName, bedAssignments, _potAssignments());
    }

    return [
      _rotateCrops("Rotate Your Crops",            false, false),
      _rotateCrops("Rotation + Yield Boost",        true,  false),
      _rotateCrops("Rotation + Legume for Soil",    false, true),
    ];
  }

  // ── MAX YIELD ────────────────────────────────────────────────────────────────
  if (goal === "max_yield") {
    // Keep user's crops but prioritise high-yield categories in best beds
    const highYieldOrder = ["fruiting", "brassica", "root", "legume", "allium", "salad"];

    function _yieldPlan(variantName, mixRotation = false) {
      const usedCropIndices = new Set();
      const pool = [...currentBedCrops];
      // Add high-yield alternatives if user pool is thin
      for (const cat of highYieldOrder) {
        if (!pool.find(c => c.category === cat)) {
          const def = _pickCropForCategory(cat, cropDefs, preferredNames, new Set(pool.map(c=>c.name)));
          if (def) pool.push({ name: def.name, category: cat });
        }
        if (pool.length >= freeBedAreas.length + 2) break;
      }

      const bedAssignments = freeBedAreas.map(area => {
        const curCat = currentCatByArea[area.id];
        const scored = pool.map((crop, i) => {
          if (usedCropIndices.has(i)) return { i, score:-1 };
          const yScore  = (YIELD_SCORE[crop.category]||5) * (mixRotation ? 0.6 : 0.9);
          const rotScore = mixRotation ? _scoreRotation(curCat, crop.category) * 0.4 : 0;
          return { i, crop, score: yScore + rotScore };
        }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
        const best = scored[0];
        if (best) { usedCropIndices.add(best.i); return { area, category: best.crop.category, cropName: best.crop.name }; }
        return { area, category: "root", cropName: null };
      });
      return _buildOption(variantName, bedAssignments, _potAssignments());
    }

    return [
      _yieldPlan("Maximum Yield",          false),
      _yieldPlan("Yield + Rotation",       true),
      (() => {
        // Diversified: each bed gets a different category
        const usedCats = new Set();
        const usedIdx  = new Set();
        const pool = [...currentBedCrops];
        for (const cat of highYieldOrder) {
          if (!pool.find(c => c.category === cat)) {
            const def = _pickCropForCategory(cat, cropDefs, preferredNames, new Set(pool.map(c=>c.name)));
            if (def) pool.push({ name: def.name, category: cat });
          }
        }
        const bedAssignments = freeBedAreas.map(area => {
          const scored = pool.map((crop,i) => {
            if (usedIdx.has(i)) return { i, score:-1 };
            const diversity = usedCats.has(crop.category) ? -3 : 0;
            return { i, crop, score: (YIELD_SCORE[crop.category]||5) + diversity };
          }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
          const best = scored[0];
          if (best) { usedIdx.add(best.i); usedCats.add(best.crop.category); return { area, category: best.crop.category, cropName: best.crop.name }; }
          return { area, category: "root", cropName: null };
        });
        return _buildOption("Diversified Yield", bedAssignments, _potAssignments());
      })(),
    ];
  }

  // ── EASY ─────────────────────────────────────────────────────────────────────
  if (goal === "easy") {
    const easyOrder = ["salad", "legume", "allium", "root", "brassica", "fruiting"];

    function _easyPlan(variantName, addRotation = false) {
      const pool = [...currentBedCrops];
      for (const cat of easyOrder) {
        if (!pool.find(c => c.category === cat)) {
          const def = _pickCropForCategory(cat, cropDefs, preferredNames, new Set(pool.map(c=>c.name)));
          if (def) pool.push({ name: def.name, category: cat });
        }
        if (pool.length >= freeBedAreas.length + 2) break;
      }
      const usedIdx = new Set();
      const bedAssignments = freeBedAreas.map(area => {
        const curCat = currentCatByArea[area.id];
        const scored = pool.map((crop,i) => {
          if (usedIdx.has(i)) return { i, score:-1 };
          const eScore  = (EASE_SCORE[crop.category]||5) * (addRotation?0.6:0.9);
          const rotScore = addRotation ? _scoreRotation(curCat, crop.category)*0.4 : 0;
          return { i, crop, score: eScore+rotScore };
        }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
        const best = scored[0];
        if (best) { usedIdx.add(best.i); return { area, category: best.crop.category, cropName: best.crop.name }; }
        return { area, category: "salad", cropName: null };
      });
      return _buildOption(variantName, bedAssignments, _potAssignments());
    }

    return [
      _easyPlan("Easiest Season",   false),
      _easyPlan("Easy + Rotation",  true),
      (() => {
        // Rest the garden — all legumes to fix nitrogen
        const legumeDef = _pickCropForCategory("legume", cropDefs, preferredNames, new Set());
        const bedAssignments = freeBedAreas.map(area => ({
          area, category: "legume", cropName: legumeDef?.name || "Peas"
        }));
        return _buildOption("Rest the Garden (Legumes)", bedAssignments, _potAssignments());
      })(),
    ];
  }

  // ── FAVOURITES ────────────────────────────────────────────────────────────────
  if (goal === "favourites") {
    // Prioritise user's most-grown crops, use rotation as tiebreaker
    const favPool = currentBedCrops.length ? currentBedCrops : [];
    const usedIdx = new Set();
    const bedAssignments = freeBedAreas.map(area => {
      const curCat = currentCatByArea[area.id];
      const scored = favPool.map((crop,i) => {
        if (usedIdx.has(i)) return { i, score:-1 };
        return { i, crop, score: _scoreRotation(curCat, crop.category) };
      }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
      const best = scored[0];
      if (best) { usedIdx.add(best.i); return { area, category: best.crop.category, cropName: best.crop.name }; }
      return { area, category: "root", cropName: null };
    });
    const opt1 = _buildOption("Your Favourites (Rotated)", bedAssignments, _potAssignments());

    // Option 2: spread favourites evenly with one legume for soil
    const usedIdx2 = new Set();
    const bedAssignments2 = freeBedAreas.map((area, i) => {
      if (i === 0 && !favPool.find(c=>c.category==="legume")) {
        const ld = _pickCropForCategory("legume", cropDefs, [], new Set());
        return { area, category: "legume", cropName: ld?.name || "Peas" };
      }
      const curCat = currentCatByArea[area.id];
      const scored = favPool.map((crop,j) => {
        if (usedIdx2.has(j)) return { j, score:-1 };
        return { j, crop, score: _scoreRotation(curCat, crop.category) };
      }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
      const best = scored[0];
      if (best) { usedIdx2.add(best.i||0); return { area, category: best.crop.category, cropName: best.crop.name }; }
      return { area, category: "root", cropName: null };
    });
    const opt2 = _buildOption("Favourites + Soil Rest", bedAssignments2, _potAssignments());

    // Option 3: same as opt1 but yield-biased
    const usedIdx3 = new Set();
    const bedAssignments3 = freeBedAreas.map(area => {
      const curCat = currentCatByArea[area.id];
      const scored = favPool.map((crop,i) => {
        if (usedIdx3.has(i)) return { i, score:-1 };
        return { i, crop, score: _scoreRotation(curCat, crop.category)*0.5 + (YIELD_SCORE[crop.category]||5)*0.5 };
      }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
      const best = scored[0];
      if (best) { usedIdx3.add(best.i); return { area, category: best.crop.category, cropName: best.crop.name }; }
      return { area, category: "root", cropName: null };
    });
    const opt3 = _buildOption("Favourites + Yield", bedAssignments3, _potAssignments());

    return [opt1, opt2, opt3];
  }

  // ── BALANCED (default) ────────────────────────────────────────────────────────
  const pool = [...currentBedCrops];
  const balancedCats = ["legume", "brassica", "root", "fruiting", "allium", "salad"];
  for (const cat of balancedCats) {
    if (!pool.find(c => c.category === cat)) {
      const def = _pickCropForCategory(cat, cropDefs, preferredNames, new Set(pool.map(c=>c.name)));
      if (def) pool.push({ name: def.name, category: cat });
    }
  }

  function _balancedPlan(variantName, weights = { rot:0.4, yield:0.3, ease:0.3 }) {
    const usedIdx = new Set();
    const bedAssignments = freeBedAreas.map(area => {
      const curCat = currentCatByArea[area.id];
      const scored = pool.map((crop,i) => {
        if (usedIdx.has(i)) return { i, score:-1 };
        const s = _scoreRotation(curCat, crop.category)*weights.rot +
                  (YIELD_SCORE[crop.category]||5)*weights.yield +
                  (EASE_SCORE[crop.category]||5)*weights.ease;
        return { i, crop, score: s };
      }).filter(s=>s.score>=0).sort((a,b)=>b.score-a.score);
      const best = scored[0];
      if (best) { usedIdx.add(best.i); return { area, category: best.crop.category, cropName: best.crop.name }; }
      return { area, category: "root", cropName: null };
    });
    return _buildOption(variantName, bedAssignments, _potAssignments());
  }

  return [
    _balancedPlan("Balanced Garden",    { rot:0.4, yield:0.3, ease:0.3 }),
    _balancedPlan("Balanced + Yield",   { rot:0.3, yield:0.5, ease:0.2 }),
    _balancedPlan("Balanced + Rotation",{ rot:0.6, yield:0.2, ease:0.2 }),
  ];
}

app.post("/plans/generate", requireAuth, async (req, res) => {
  try {
    const {
      location_id,
      year_round    = false,
      improve_count = 0,
      preference    = "balanced",
    } = req.body;
    if (!location_id) return res.status(400).json({ error: "location_id required" });

    const { resolveShopValue } = require("./value-resolver");

    // ── Fetch location ────────────────────────────────────────────────────────
    const { data: loc, error: locErr } = await req.db
      .from("locations").select("id, name").eq("id", location_id).eq("user_id", req.user.id).single();
    if (locErr || !loc) return res.status(403).json({ error: "Location not found" });

    // ── Fetch areas (include type) ────────────────────────────────────────────
    const { data: areas, error: areasErr } = await req.db
      .from("growing_areas").select("id, name, type, width_m, length_m").eq("location_id", location_id);
    if (areasErr) throw areasErr;
    if (!areas || !areas.length) return res.status(400).json({ error: "No areas found" });

    const areaIds = areas.map(a => a.id);

    // ── Fetch crop instances ──────────────────────────────────────────────────
    const { data: allCrops } = await supabaseService
      .from("crop_instances")
      .select("area_id, crop_def_id, variety_id, name, active, status, sown_date, varieties(name), crop_definitions(name, category, harvest_month_start, harvest_month_end, sow_direct_start, sow_indoors_start)")
      .eq("user_id", req.user.id)
      .in("area_id", areaIds);

    const cropRows = allCrops || [];

    // ── Build activeCropsByArea and plannedCropsByArea ────────────────────────
    const activeCropsByArea  = {};
    const plannedCropsByArea = {};

    for (const crop of cropRows) {
      const name     = crop.crop_definitions?.name || crop.name || "Unknown";
      const category = crop.crop_definitions?.category || null;
      const entry    = {
        name, category,
        crop_def_id:         crop.crop_def_id,
        variety_id:          crop.variety_id || null,
        variety_name:        crop.varieties?.name || null,
        status:              crop.status,
        sown_date:           crop.sown_date,
        harvest_month_start: crop.crop_definitions?.harvest_month_start || null,
        harvest_month_end:   crop.crop_definitions?.harvest_month_end   || null,
        sow_month:           crop.crop_definitions?.sow_direct_start || crop.crop_definitions?.sow_indoors_start || null,
      };
      if (crop.active) {
        if (!activeCropsByArea[crop.area_id])  activeCropsByArea[crop.area_id]  = [];
        activeCropsByArea[crop.area_id].push(entry);
      } else if (crop.crop_def_id) {
        // Include all non-active crops with a known definition as potential follow-ons
        // (whether status is "planned", "sown_indoors", "sown_outdoors", etc.)
        if (!plannedCropsByArea[crop.area_id]) plannedCropsByArea[crop.area_id] = [];
        plannedCropsByArea[crop.area_id].push(entry);
      }
    }

    // ── Build sequenceByArea ──────────────────────────────────────────────────
    // primary  = the main active crop in the bed (non-fixed, non-invasive)
    // followOns = committed follow-on crops, from two sources:
    //   1. Other active crops in same bed (e.g. Swede/Sprouts sown alongside Garlic)
    //   2. Planned crops that are physically started (sown_date or committed status)
    const COMMITTED_STATUSES = new Set(["sown_indoors","sown_outdoors","transplanted","growing","harvesting"]);
    const FIXED_CATS_SEQ     = new Set(["fruit","perennial"]);
    const INV_CATS_SEQ       = new Set(["herb","flower","perennial","fruit"]);
    const sequenceByArea = {};
    for (const area of areas) {
      const active  = activeCropsByArea[area.id]  || [];
      const planned = plannedCropsByArea[area.id] || [];

      // Primary = first active crop with valid crop_def_id, not fixed/invasive
      const primary = active.find(c =>
        c.crop_def_id && c.category &&
        !FIXED_CATS_SEQ.has(c.category) &&
        !INV_CATS_SEQ.has(c.category)
      ) || active[0] || null;

      // Follow-ons from active crops (same bed, different crop from primary)
      const activeFollowOns = primary ? active.filter(c =>
        c.crop_def_id && c.category &&
        !FIXED_CATS_SEQ.has(c.category) &&
        c.crop_def_id !== primary.crop_def_id &&
        c.name !== primary.name
      ) : [];

      // Follow-ons from planned/committed crops
      const plannedFollowOns = planned.filter(p =>
        !FIXED_CATS_SEQ.has(p.category) &&
        (p.sown_date || COMMITTED_STATUSES.has(p.status))
      );

      // Merge, deduplicate by name
      const seenNames   = new Set(primary ? [primary.name] : []);
      const uniqueFollowOns = [];
      for (const f of [...activeFollowOns, ...plannedFollowOns]) {
        if (!seenNames.has(f.name)) {
          seenNames.add(f.name);
          uniqueFollowOns.push(f);
        }
      }

      sequenceByArea[area.id] = {
        primary,
        allActive: active,
        followOns: uniqueFollowOns,
      };
    }

    // ── Fetch crop definitions and varieties ──────────────────────────────────
    const { data: cropDefs } = await supabaseService
      .from("crop_definitions")
      .select("id, name, category, harvest_month_start, harvest_month_end, sow_direct_start, sow_indoors_start")
      .eq("hidden", false);

    const { data: allVarieties } = await supabaseService
      .from("varieties").select("id, crop_def_id, name, is_default").eq("active", true);
    const varietyMap = {};
    for (const v of (allVarieties || [])) {
      if (!varietyMap[v.crop_def_id]) varietyMap[v.crop_def_id] = [];
      varietyMap[v.crop_def_id].push(v);
    }

    // ── User crop names set (for gap-fill preference) ─────────────────────────
    const userCropNames = new Set(cropRows.map(c => c.crop_definitions?.name || c.name).filter(Boolean));

    // ── Build 3-year rotation history per bed ────────────────────────────────
    // Query harvested crop_instances for this user's areas within the last 3 seasons.
    // Used to penalise repeating the same crop category in the same bed.
    const planYear      = new Date().getFullYear() + 1;
    const historyFromYr = planYear - 3;

    const { data: historyRows } = await supabaseService
      .from("crop_instances")
      .select("area_id, harvested_at, crop_definitions(category)")
      .eq("user_id", req.user.id)
      .in("area_id", areaIds)
      .not("harvested_at", "is", null)
      .gte("harvested_at", `${historyFromYr}-01-01`);

    // Build historyByArea: area_id -> [{ category, year }] deduplicated by area+year+category
    const historyByArea = {};
    const _histSeen     = new Set();
    for (const row of (historyRows || [])) {
      const category = row.crop_definitions?.category;
      if (!category || !row.area_id || !row.harvested_at) continue;
      const year = new Date(row.harvested_at).getFullYear();
      const key  = `${row.area_id}:${year}:${category}`;
      if (_histSeen.has(key)) continue;
      _histSeen.add(key);
      if (!historyByArea[row.area_id]) historyByArea[row.area_id] = [];
      historyByArea[row.area_id].push({ category, year });
    }

    // ── Build baseline ────────────────────────────────────────────────────────
    const baselineAssignments = _buildBaseline({
      areas, activeCropsByArea, sequenceByArea, cropDefs: cropDefs || [], varietyMap,
      historyByArea, planYear,
    });

    // ── Helper: score and resolve metrics for an assignment list ──────────────
    const currentMonth = new Date().getMonth() + 1;
    const currentYear  = new Date().getFullYear();

    async function _scoreAssignments(asgn) {
      let totalYieldKg = 0, totalAreaM2 = 0, totalValueGbp = 0;
      for (const a of asgn) {
        const areaObj = areas.find(ar => ar.id === a.area_id);
        const m2      = areaObj ? (areaObj.width_m||1.5) * (areaObj.length_m||1.5) : 1.5;
        const ypm2    = YIELD_SCORE[a.category] ? YIELD_SCORE[a.category] * 0.5 : 1.5; // rough kg/m²
        totalYieldKg += ypm2 * m2;
        totalAreaM2  += m2;
        if (a.crop_definition_id) {
          const hMonth    = a.harvest_month_start || currentMonth;
          const hYear     = hMonth < currentMonth ? currentYear+1 : currentYear;
          const monthKey  = `${hYear}-${String(hMonth).padStart(2,"0")}`;
          const resolved  = await resolveShopValue(supabaseService, a.crop_definition_id, ypm2*m2, monthKey);
          if (resolved) totalValueGbp += resolved.value_gbp;
        }
      }
      return {
        harvest_kg:    Math.round(totalYieldKg * 10) / 10,
        shop_value_gbp: totalValueGbp > 0 ? Math.round(totalValueGbp) : null,
        yield_per_m2:  totalAreaM2 > 0 ? Math.round((totalYieldKg/totalAreaM2)*10)/10 : null,
      };
    }

    // ── Build enhanced plan (apply year_round and improvements) ───────────────
    // Start with a copy of baseline
    let enhancedAssignments = baselineAssignments.map(a => ({ ...a }));
    const gapFills    = [];
    const improvements = [];

    // Year-round gap filling
    if (year_round) {
      const gaps = _findGaps(baselineAssignments, sequenceByArea);
      const usedGapNames      = new Set();
      const usedGapCategories = new Set();
      for (const gap of gaps) {
        const def = _pickGapCrop(gap, cropDefs || [], userCropNames, usedGapNames, usedGapCategories);
        if (!def) continue;
        usedGapNames.add(def.name);
        usedGapCategories.add(def.category);
        const display = (varietyMap[def.id]?.[0]) ? `${def.name} (${varietyMap[def.id][0].name})` : def.name;
        gapFills.push({
          area_id:            gap.area_id,
          area_name:          gap.area_name,
          crop_name:          display,
          crop_definition_id: def.id,
          gap_start_month:    gap.gap_start_month,
          gap_end_month:      gap.gap_end_month,
          after_crop:         gap.after_crop,
          note:               `Fills the gap between ${gap.after_crop} harvest and spring sowing`,
        });
      }
    }

    // Targeted improvements
    if (improve_count > 0) {
      const imps = _generateImprovements({
        baselineAssignments,
        cropDefs:    cropDefs || [],
        varietyMap,
        userCropNames,
        preference,
        improveCount: improve_count,
      });
      // Apply improvements to enhanced assignments
      for (const imp of imps) {
        const idx = enhancedAssignments.findIndex(a => a.area_id === imp.area_id);
        if (idx >= 0) {
          enhancedAssignments[idx] = {
            ...enhancedAssignments[idx],
            category:           imp.to_category,
            crop_definition_id: imp.crop_definition_id,
            crop_name:          imp.to_crop,
            harvest_month_start: imp.harvest_month_start,
            harvest_month_end:   imp.harvest_month_end,
          };
        }
        improvements.push(imp);
      }
    }

    // ── Score baseline and enhanced ───────────────────────────────────────────
    const [baselineMetrics, enhancedMetrics] = await Promise.all([
      _scoreAssignments(baselineAssignments),
      _scoreAssignments(enhancedAssignments),
    ]);

    // ── Tip (passive suggestion when no enhancements) ─────────────────────────
    const tip = (!year_round && improve_count === 0)
      ? _buildTip(baselineAssignments, cropDefs || [], varietyMap)
      : null;

    // ── Build change summary for enhanced plan ────────────────────────────────
    const changes = improvements.map(imp => ({
      area_id:     imp.area_id,
      area_name:   imp.area_name,
      change_type: "crop_swap",
      from_label:  imp.from_crop,
      to_label:    imp.to_crop,
      reason_text: imp.reason,
    }));

    const hasEnhancements = year_round || improve_count > 0;
    const changeCount = changes.length + gapFills.length;
    const changeSummary = changeCount === 0 ? null
      : changeCount === 1 ? "1 change from your rotated baseline"
      : `${changeCount} changes from your rotated baseline`;

    res.json({
      location_id,
      baseline: {
        label:       "Your rotated garden",
        assignments: baselineAssignments,
        metrics:     baselineMetrics,
      },
      plan: {
        label:          hasEnhancements ? "Enhanced plan" : "Your rotated garden",
        assignments:    enhancedAssignments,
        metrics:        enhancedMetrics,
        gap_fills:      gapFills,
        improvements,
        changes,
        change_count:   changeCount,
        change_summary: changeSummary,
      },
      tip,
    });

  } catch (err) {
    captureError("PlanGenerate", err);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// INFRASTRUCTURE ROI ENGINE
// Stateless — computes yield/value/ROI impact of adding infrastructure.
// No DB write on model. Infrastructure metadata persisted only on plan commit.
// =============================================================================

app.post("/infrastructure/model", requireAuth, async (req, res) => {
  try {
    const {
      MODIFIER_VERSION,
      COST_RANGES,
      YIELD_MULTIPLIERS,
      SEASON_EXTENSION,
      EFFORT_CHANGE,
      CROP_UNLOCKS,
      THINGS_TO_KNOW,
      CARD_BENEFIT,
      COMPATIBLE_AREA_TYPES,
      INCOMPATIBILITY_NOTES,
    } = require("./infrastructure-modifiers");

    const { resolveShopValue } = require("./value-resolver");

    const {
      location_id,
      infrastructure_type,
      size           = "medium",
      target_area_ids = [],   // empty/omitted = whole garden
      custom_cost_gbp,
    } = req.body;

    if (!location_id)         return res.status(400).json({ error: "location_id required" });
    if (!infrastructure_type) return res.status(400).json({ error: "infrastructure_type required" });

    const VALID_TYPES = ["greenhouse","polytunnel","raised_bed","irrigation","water_butt","compost_system"];
    const VALID_SIZES = ["small","medium","large"];
    if (!VALID_TYPES.includes(infrastructure_type)) return res.status(400).json({ error: "Invalid infrastructure_type" });
    if (!VALID_SIZES.includes(size))                return res.status(400).json({ error: "size must be small, medium or large" });

    // ── Fetch location ──────────────────────────────────────────────────────
    const { data: loc, error: locErr } = await req.db
      .from("locations").select("id, name").eq("id", location_id).eq("user_id", req.user.id).single();
    if (locErr || !loc) return res.status(403).json({ error: "Location not found" });

    // ── Fetch all areas for this location ───────────────────────────────────
    const { data: allAreas, error: areasErr } = await req.db
      .from("growing_areas").select("id, name, type, width_m, length_m").eq("location_id", location_id);
    if (areasErr) throw areasErr;
    if (!allAreas?.length) return res.status(400).json({ error: "No areas found" });

    // ── Filter to target areas ──────────────────────────────────────────────
    // Empty array or omitted = whole garden
    const targetIds  = Array.isArray(target_area_ids) && target_area_ids.length > 0
      ? new Set(target_area_ids)
      : null;
    const areas = targetIds
      ? allAreas.filter(a => targetIds.has(a.id))
      : allAreas;

    if (!areas.length) return res.status(400).json({ error: "No matching areas found for target_area_ids" });

    const areaIds = areas.map(a => a.id);

    // ── Fetch active crop instances for these areas ─────────────────────────
    const { data: cropRows } = await supabaseService
      .from("crop_instances")
      .select("area_id, crop_def_id, name, active, crop_definitions(name, category, harvest_month_start, harvest_month_end)")
      .eq("user_id", req.user.id)
      .in("area_id", areaIds)
      .eq("active", true);

    // Group active crops by area
    const cropsByArea = {};
    for (const crop of (cropRows || [])) {
      if (!cropsByArea[crop.area_id]) cropsByArea[crop.area_id] = [];
      cropsByArea[crop.area_id].push({
        name:                crop.crop_definitions?.name || crop.name,
        category:            crop.crop_definitions?.category || null,
        crop_def_id:         crop.crop_def_id,
        harvest_month_start: crop.crop_definitions?.harvest_month_start || null,
        harvest_month_end:   crop.crop_definitions?.harvest_month_end   || null,
      });
    }

    // ── Incompatibility check ───────────────────────────────────────────────
    // If none of the target areas are compatible with the chosen infrastructure,
    // return a graceful incompatibility response rather than zero-value numbers.
    const compatTypes      = COMPATIBLE_AREA_TYPES[infrastructure_type] || [];
    const compatibleAreas  = areas.filter(a =>
      compatTypes.includes(a.type) || compatTypes.includes("new")
    );
    const incompatibleOnly = compatibleAreas.length === 0;

    // ── Cost ────────────────────────────────────────────────────────────────
    const [costLow, costHigh] = (COST_RANGES[infrastructure_type]?.[size]) || [0, 0];
    const assumedCost = custom_cost_gbp != null
      ? Number(custom_cost_gbp)
      : Math.round((costLow + costHigh) / 2);
    const costRangeLabel = costLow > 0 ? `£${costLow}–£${costHigh}` : "Variable";

    // ── Yield multiplier helpers ────────────────────────────────────────────
    const multiplierFor = (category) => {
      const mods = YIELD_MULTIPLIERS[infrastructure_type] || {};
      return mods[category] || mods["default"] || 1.0;
    };

    // ── Build baseline and modelled per area ────────────────────────────────
    const currentYear  = new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1;

    let baselineKg   = 0;
    let modelledKg   = 0;
    let baselineVal  = 0;
    let modelledVal  = 0;
    let totalAreaM2  = 0;

    const affectedAreas = [];

    // Track model quality signals for confidence scoring
    let areasWithCrops       = 0;
    let areasWithGoodMatch   = 0;
    let shopValueResolved    = 0;
    let shopValueAttempted   = 0;

    for (const area of areas) {
      const m2    = (area.width_m || 1.5) * (area.length_m || 1.5);
      const crops = cropsByArea[area.id] || [];
      totalAreaM2 += m2;

      // Use primary crop category for yield calculation — same approach as _scoreAssignments
      // If no active crops, use a conservative generic yield
      const primaryCrop     = crops.find(c => c.category && !["fruit","perennial"].includes(c.category));
      const category        = primaryCrop?.category || null;
      const yieldPerM2      = category ? (YIELD_SCORE[category] || 5) * 0.5 : 1.5;
      const areaBaselineKg  = yieldPerM2 * m2;
      const multiplier      = multiplierFor(category);
      const areaModelledKg  = areaBaselineKg * multiplier;

      baselineKg  += areaBaselineKg;
      modelledKg  += areaModelledKg;

      if (crops.length > 0) areasWithCrops++;
      if (category && multiplier > (YIELD_MULTIPLIERS[infrastructure_type]?.default || 1.0)) areasWithGoodMatch++;

      // Shop value — baseline and modelled
      if (primaryCrop?.crop_def_id) {
        shopValueAttempted++;
        const hMonth   = primaryCrop.harvest_month_start || currentMonth;
        const hYear    = hMonth < currentMonth ? currentYear + 1 : currentYear;
        const monthKey = `${hYear}-${String(hMonth).padStart(2, "0")}`;

        const [bVal, mVal] = await Promise.all([
          resolveShopValue(supabaseService, primaryCrop.crop_def_id, areaBaselineKg, monthKey),
          resolveShopValue(supabaseService, primaryCrop.crop_def_id, areaModelledKg, monthKey),
        ]);
        if (bVal) { baselineVal += bVal.value_gbp; shopValueResolved++; }
        if (mVal)   modelledVal += mVal.value_gbp;
      }

      affectedAreas.push({
        area_id:            area.id,
        area_name:          area.name,
        area_type:          area.type,
        category:           category || "mixed",
        baseline_yield_kg:  Math.round(areaBaselineKg * 10) / 10,
        modelled_yield_kg:  Math.round(areaModelledKg * 10) / 10,
        multiplier_applied: multiplier,
      });
    }

    // ── Confidence scoring ──────────────────────────────────────────────────
    // Based on overall model quality, not just harvest history.
    // High:   3+ areas have good category matches, shop value resolved for most gain
    // Medium: some matching, partial shop value, reasonable modifier fit
    // Low:    mostly generic assumptions, no meaningful crop/value grounding
    let confidence;
    const shopValueCoverage = shopValueAttempted > 0 ? shopValueResolved / shopValueAttempted : 0;
    if (areasWithGoodMatch >= 3 && shopValueCoverage >= 0.6) {
      confidence = "high";
    } else if (areasWithCrops >= 1 && (areasWithGoodMatch >= 1 || shopValueCoverage >= 0.3)) {
      confidence = "medium";
    } else {
      confidence = "low";
    }

    const confidenceNote = confidence === "high"
      ? "Based on your actual crops and typical UK shop prices."
      : confidence === "medium"
      ? "Based on your crops and typical UK yields. Actual results vary with weather and management."
      : "Based on typical UK yields — actual results will depend on your specific setup and crops.";

    // ── Season extension ────────────────────────────────────────────────────
    const seasonExt = SEASON_EXTENSION[infrastructure_type] || { earlier_sow_weeks: 0, later_harvest_weeks: 0 };
    const totalExtWeeks = seasonExt.earlier_sow_weeks + seasonExt.later_harvest_weeks;
    const seasonLabel   = totalExtWeeks > 0
      ? `Up to ${totalExtWeeks} weeks extra growing`
      : null;

    // ── ROI ─────────────────────────────────────────────────────────────────
    // Use shop value delta if resolved, otherwise fall back to yield delta × rough price
    const ROUGH_PRICE_PER_KG = 2.50; // conservative fallback when no shop value data
    const yieldDelta  = Math.round((modelledKg   - baselineKg)  * 10) / 10;
    const rawValDelta = modelledVal > 0 && baselineVal > 0
      ? modelledVal - baselineVal
      : yieldDelta * ROUGH_PRICE_PER_KG;
    const valueDelta  = Math.round(rawValDelta);
    const valueGain1yr = valueDelta > 0 ? valueDelta : 0;
    const valueGain3yr = valueGain1yr * 3;
    const net3yr       = valueGain3yr - assumedCost;
    const paybackSeasons = valueGain1yr > 0
      ? Math.round((assumedCost / valueGain1yr) * 10) / 10
      : null;
    const paybackLabel = paybackSeasons === null
      ? "Payback period unclear — add crops to this area for a better estimate"
      : paybackSeasons <= 1
      ? "Could pay back within a season"
      : paybackSeasons <= 3
      ? `Pays back in ~${paybackSeasons} seasons`
      : `Estimated payback: ${paybackSeasons} seasons`;

    // ── Incompatibility response ────────────────────────────────────────────
    if (incompatibleOnly) {
      return res.json({
        infrastructure_type,
        size,
        modifier_version: MODIFIER_VERSION,
        incompatible:     true,
        incompatibility_note: INCOMPATIBILITY_NOTES[infrastructure_type] || "This infrastructure may not be well suited to your current setup",
        card_benefit:     CARD_BENEFIT[infrastructure_type],
        cost: { range_low: costLow, range_high: costHigh, assumed_gbp: assumedCost, cost_range_label: costRangeLabel },
      });
    }

    // ── Response ────────────────────────────────────────────────────────────
    res.json({
      infrastructure_type,
      size,
      modifier_version:  MODIFIER_VERSION,
      incompatible:      false,
      cost: {
        range_low:       costLow,
        range_high:      costHigh,
        assumed_gbp:     assumedCost,
        cost_range_label: costRangeLabel,
      },
      baseline: {
        harvest_kg:      Math.round(baselineKg * 10) / 10,
        shop_value_gbp:  baselineVal > 0 ? Math.round(baselineVal) : null,
        area_m2:         Math.round(totalAreaM2 * 10) / 10,
      },
      modelled: {
        harvest_kg:      Math.round(modelledKg * 10) / 10,
        shop_value_gbp:  modelledVal > 0 ? Math.round(modelledVal) : null,
        area_m2:         Math.round(totalAreaM2 * 10) / 10,
      },
      gains: {
        harvest_kg_delta:    yieldDelta,
        value_gbp_delta:     valueDelta > 0 ? valueDelta : null,
        yield_per_m2_delta:  totalAreaM2 > 0 ? Math.round((yieldDelta / totalAreaM2) * 10) / 10 : null,
        season_extension: {
          earlier_sow_weeks:   seasonExt.earlier_sow_weeks,
          later_harvest_weeks: seasonExt.later_harvest_weeks,
          label:               seasonLabel,
        },
        effort_change:  EFFORT_CHANGE[infrastructure_type],
        crop_unlocks:   CROP_UNLOCKS[infrastructure_type] || [],
        things_to_know: THINGS_TO_KNOW[infrastructure_type] || [],
      },
      roi: {
        payback_seasons:   paybackSeasons,
        payback_label:     paybackLabel,
        value_gain_year_1: valueGain1yr > 0 ? valueGain1yr : null,
        value_gain_3yr:    valueGain3yr > 0 ? valueGain3yr : null,
        net_3yr:           net3yr,
        confidence,
        confidence_note:   confidenceNote,
      },
      affected_areas:    affectedAreas,
      card_benefit:      CARD_BENEFIT[infrastructure_type],
    });

  } catch (err) {
    captureError("InfrastructureModel", err);
    res.status(500).json({ error: err.message });
  }
});

// ── Area Plan Assignments ─────────────────────────────────────────────────────
// Locked future crop assignments per bed per year.
// Status: draft → locked → ready → active → completed | cancelled | replaced

// GET /area-plan-assignments?location_id=xxx
app.get("/area-plan-assignments", requireAuth, async (req, res) => {
  try {
    const { location_id } = req.query;
    if (!location_id) return res.status(400).json({ error: "location_id required" });
    const { data: areas } = await req.db
      .from("growing_areas").select("id").eq("location_id", location_id);
    if (!areas?.length) return res.json([]);
    const areaIds = areas.map(a => a.id);
    const { data, error } = await supabaseService
      .from("area_plan_assignments")
      .select("*, crop_definitions(name, category)")
      .eq("user_id", req.user.id)
      .in("area_id", areaIds)
      .in("status", ["locked","ready","active"])
      .order("planned_year", { ascending: false });
    if (error) throw error;
    res.json(data || []);
  } catch (err) { captureError("GetAreaPlanAssignments", err); res.status(500).json({ error: err.message }); }
});

// POST /area-plan-assignments/commit
// Commits plan assignments as locked for next growing year.
// Body: {
//   location_id,
//   assignments: [{ area_id, crop_def_id, crop_name, category }],
//   planned_year?,
//   infrastructure_type?,      ← optional: set if plan was influenced by ROI scenario
//   infrastructure_cost_label?, ← e.g. "£300–£600"
//   roi_summary?,              ← { size, yield_gain_kg, value_gain_gbp, payback_seasons, confidence }
// }
app.post("/area-plan-assignments/commit", requireAuth, async (req, res) => {
  try {
    const {
      location_id,
      assignments,
      planned_year,
      // Optional infrastructure metadata — persisted for traceability
      infrastructure_type   = null,
      infrastructure_cost_label = null,
      roi_summary           = null,
    } = req.body;
    if (!location_id || !assignments?.length) return res.status(400).json({ error: "location_id and assignments required" });
    const year = planned_year || new Date().getFullYear() + 1;
    const { data: loc } = await req.db
      .from("locations").select("id").eq("id", location_id).eq("user_id", req.user.id).single();
    if (!loc) return res.status(403).json({ error: "Location not found" });
    // Mark any existing draft/locked/ready for these areas+year as replaced
    const areaIds = assignments.map(a => a.area_id);
    await supabaseService
      .from("area_plan_assignments")
      .update({ status: "replaced", updated_at: new Date().toISOString() })
      .eq("user_id", req.user.id)
      .in("area_id", areaIds)
      .eq("planned_year", year)
      .in("status", ["draft","locked","ready"]);

    // Infrastructure metadata: only attach if infrastructure_type is present
    // Gives the UI enough to say "This plan assumes a medium greenhouse"
    const { MODIFIER_VERSION } = infrastructure_type
      ? require("./infrastructure-modifiers")
      : { MODIFIER_VERSION: null };

    // Insert new locked rows
    const rows = assignments.filter(a => a.area_id && a.crop_name).map(a => ({
      user_id:                  req.user.id,
      area_id:                  a.area_id,
      crop_def_id:              a.crop_def_id || null,
      crop_name:                a.crop_name,
      category:                 a.category || null,
      planned_year:             year,
      status:                   "locked",
      source:                   "plan_flow",
      locked_at:                new Date().toISOString(),
      // Infrastructure metadata (all nullable — no impact if not supplied)
      infrastructure_type:      infrastructure_type || null,
      infrastructure_cost_label: infrastructure_cost_label || null,
      roi_summary:              roi_summary ? JSON.stringify(roi_summary) : null,
      modifier_version:         infrastructure_type ? MODIFIER_VERSION : null,
    }));
    const { data, error } = await supabaseService
      .from("area_plan_assignments").insert(rows).select();
    if (error) throw error;
    res.json({ ok: true, committed: data.length, year, assignments: data });
  } catch (err) { captureError("CommitAreaPlanAssignments", err); res.status(500).json({ error: err.message }); }
});

// PATCH /area-plan-assignments/:id  — update status
app.patch("/area-plan-assignments/:id", requireAuth, async (req, res) => {
  try {
    const { status, notes } = req.body;
    const valid = ["draft","locked","ready","active","completed","cancelled","replaced"];
    if (status && !valid.includes(status)) return res.status(400).json({ error: "Invalid status" });
    const updates = { updated_at: new Date().toISOString() };
    if (status) updates.status = status;
    if (notes)  updates.notes  = notes;
    if (status === "active")    updates.activated_at  = new Date().toISOString();
    if (status === "completed") updates.completed_at  = new Date().toISOString();
    const { data, error } = await supabaseService
      .from("area_plan_assignments")
      .update(updates).eq("id", req.params.id).eq("user_id", req.user.id)
      .select().single();
    if (error) throw error;
    res.json(data);
  } catch (err) { captureError("PatchAreaPlanAssignment", err); res.status(500).json({ error: err.message }); }
});

// DELETE /area-plan-assignments/:id  (soft delete)
app.delete("/area-plan-assignments/:id", requireAuth, async (req, res) => {
  try {
    const { error } = await supabaseService
      .from("area_plan_assignments")
      .update({ status: "cancelled", updated_at: new Date().toISOString() })
      .eq("id", req.params.id).eq("user_id", req.user.id);
    if (error) throw error;
    res.json({ ok: true });
  } catch (err) { captureError("DeleteAreaPlanAssignment", err); res.status(500).json({ error: err.message }); }
});

// ── Sentry error handler — must be before any other error middleware ──────────
Sentry.setupExpressErrorHandler(app);

// ── Fallback error handler ────────────────────────────────────────────────────
app.use((err, _req, res, _next) => {
  console.error(err.stack);
  Sentry.captureException(err);
  res.status(500).json({ error: "Internal server error" });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Grow Smart API on :${PORT}`));

module.exports = app;