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
const { applyBlockedPeriodAdjustments, reapplyAllBlockedPeriods } = require("./blocked-period-adjustment");
const { runNotificationsForUser } = require("./notifications");
const { runNudgeUnactivated, runNudgeUnconfirmed, runFeedbackSequence, runWaitlistInvites, runWaitlistNudges, runWaitlistNudges2, runWaitlistNudges3, runReengagement, runDailyEmailFallback } = require("./emails");

// ── Supabase (service role — server only) ─────────────────────────────────────
const supabaseService = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

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
async function requireAdmin(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Unauthorised" });
  if (req.user.email !== ADMIN_EMAIL) return res.status(403).json({ error: "Forbidden" });
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

  // Apply timeline_offset_days (signed: positive = behind schedule, negative = ahead)
  // This shifts the effective sow date without changing the real sow date
  const offsetDays = crop.timeline_offset_days || 0;
  const sowDate    = offsetDays !== 0 ? addDays(rawSowDate, -offsetDays) : rawSowDate;

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

  // Harvest date — apply offset so it moves when user adjusts stage
  const harvestStart = def.harvest_month_start;
  const year         = new Date().getFullYear();
  let harvestDate    = dtm ? addDays(sowDate, dtm) : null;
  if (harvestStart && offsetDays === 0) {
    // Only use fixed calendar date when no offset — offset means crop is behind/ahead
    harvestDate = new Date(year, harvestStart - 1, 15).toISOString().split("T")[0];
  } else if (harvestStart && offsetDays !== 0) {
    // Apply offset to the calendar harvest date
    const baseHarvest = new Date(year, harvestStart - 1, 15).toISOString().split("T")[0];
    harvestDate = addDays(baseHarvest, offsetDays);
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

  return {
    nodes,
    current_stage:       currentStage,
    current_stage_label: LABELS[currentStage],
    current_stage_description: DESCRIPTIONS[currentStage],
    next_stage_label:    nextNode ? LABELS[nextNode.key] : null,
    next_stage_date:     nextNode?.formatted_date || null,
    harvest_date:        harvestDate ? fmt(harvestDate) : null,
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
    const { name, postcode } = req.body;
    const { data, error } = await req.db.from("profiles")
      .upsert({ id: req.user.id, name, postcode }).select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.get("/auth/profile", requireAuth, async (req, res) => {
  const { data, error } = await req.db.from("profiles").select("*").eq("id", req.user.id).single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
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
    const { data, error } = await req.db.from("locations")
      .insert({ user_id: req.user.id, name, postcode, latitude, longitude, orientation, notes })
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.put("/locations/:id", requireAuth, async (req, res) => {
  const allowed = ["name","postcode","latitude","longitude","orientation","notes"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
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
    const { location_id, name, type, width_m, length_m, sun_exposure, notes } = req.body;
    const { data, error } = await req.db.from("growing_areas")
      .insert({ location_id, name, type, width_m, length_m, sun_exposure, notes })
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    res.status(201).json(data);
  }
);

app.put("/areas/:id", requireAuth, async (req, res) => {
  const allowed = ["name","type","width_m","length_m","sun_exposure","notes"];
  const updates = Object.fromEntries(Object.entries(req.body).filter(([k]) => allowed.includes(k)));
  const { data, error } = await req.db.from("growing_areas")
    .update(updates).eq("id", req.params.id).select().single();
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
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
    "grower_notes": "key growing tips for UK growers"
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

Use null for any fields you don't have reliable data for. All month values are integers 1-12. Base everything on UK growing conditions.`;

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

    // ── Check if crop already exists (case-insensitive) ──────────────────────
    let cropDefId;
    const { data: existing } = await db.from("crop_definitions")
      .select("id").ilike("name", cropData.name).maybeSingle();

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
    .select("*, area:area_id(name, type), crop_def:crop_def_id(name, harvest_month_start, harvest_month_end, harvest_month_start, harvest_month_end, days_to_maturity_min, days_to_maturity_max, sow_method, is_perennial, default_establishment, pest_window_start, pest_window_end), variety:variety_id(name, days_to_maturity_min, days_to_maturity_max)")
    .eq("user_id", req.user.id).eq("active", true)
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
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
      area_id, name, variety, variety_id, crop_def_id,
      sown_date, transplanted_date, planted_out_date, transplant_date,
      establishment_method, quantity, notes,
      start_date_confidence, source, status,
      is_other_crop, is_other_variety,
      barcode,
    } = req.body;

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
  // Fetch area_id before soft-deleting so we can invalidate suggestions cache
  const { data: crop } = await req.db.from("crop_instances")
    .select("area_id").eq("id", req.params.id).eq("user_id", req.user.id).single();

  const { error } = await req.db.from("crop_instances")
    .update({ active: false, updated_at: new Date().toISOString() })
    .eq("id", req.params.id).eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });

  // Invalidate suggestions cache — area contents have changed
  if (crop?.area_id) await clearSuggestions(crop.area_id, req.db);

  res.status(204).send();
});

// =============================================================================
// TASKS
// =============================================================================

app.get("/tasks", requireAuth, async (req, res) => {
  const { view = "all", completed } = req.query;
  const today   = todayISO();
  const weekEnd = weekEndISO();

  let query = req.db.from("tasks")
    .select("*, crop:crop_instance_id(name, variety), area:area_id(name)")
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
app.get("/admin/metrics", requireAuth, requireAdmin, async (req, res) => {
  try {
    const db = supabaseService; // service role for cross-table queries
    const now = new Date();
    const day7ago  = new Date(now - 7  * 86400000).toISOString();
    const day28ago = new Date(now - 28 * 86400000).toISOString();
    const day1ago  = new Date(now - 1  * 86400000).toISOString();

    // Get all demo user IDs to exclude from every metric
    const { data: demoProfiles } = await db.from("profiles").select("id").eq("is_demo", true);
    const demoUserIds = (demoProfiles || []).map(p => p.id);

    // User growth — auth users (everyone) vs profiles (completed onboarding)
    const { data: { users: authUsers } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
    const realAuthUsers   = authUsers.filter(u => !demoUserIds.includes(u.id));
    const totalSignups    = realAuthUsers.length;
    const newSignupsWeek  = realAuthUsers.filter(u => new Date(u.created_at) >= new Date(day7ago)).length;
    const newSignupsLastWeek = realAuthUsers.filter(u => new Date(u.created_at) >= new Date(day28ago) && new Date(u.created_at) < new Date(day7ago)).length;

    const [
      // Activated (completed onboarding = have a profile)
      { count: totalActivated },

      // Engagement
      { data: wauData },
      { data: dauData },

      // Garden usage
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

      // Feeds
      { count: totalFeeds },

      // Photos
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

      // Push
      { count: pushTokens },

      // Feedback ratings
      { data: feedbackRatings },

    ] = await Promise.all([
      db.from("profiles").select("*", { count: "exact", head: true }).eq("is_demo", false),

      db.from("crop_instances").select("user_id").gte("updated_at", day7ago).not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("crop_instances").select("user_id").gte("updated_at", day1ago).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("locations").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("growing_areas").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("crop_instances").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("crop_instances").select("*", { count: "exact", head: true }).not("sown_date", "is", null).not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("crop_instances").select("*", { count: "exact", head: true }).eq("status", "harvested").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("harvest_log").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("tasks").select("*", { count: "exact", head: true }).is("completed_at", null).not("status", "eq", "expired").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("tasks").select("*", { count: "exact", head: true }).not("completed_at", "is", null).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("user_feeds").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("crop_photos").select("*", { count: "exact", head: true }).not("user_id", "in", `(${demoUserIds.join(",")})`),

      db.from("varieties").select("*", { count: "exact", head: true }),
      db.from("harvest_log").select("*", { count: "exact", head: true }).not("quantity_value", "is", null).not("user_id", "in", `(${demoUserIds.join(",")})`),

      // Email sequences
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "waitlist_invite").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "feedback_day3").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "feedback_day7").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "reengage_day14").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "reengage_day30").not("user_id", "in", `(${demoUserIds.join(",")})`),
      db.from("email_log").select("*", { count: "exact", head: true }).eq("email_type", "daily_fallback").not("user_id", "in", `(${demoUserIds.join(",")})`),

      // Push tokens
      db.from("device_push_tokens").select("*", { count: "exact", head: true }).eq("is_active", true).not("user_id", "in", `(${demoUserIds.join(",")})`),

      // Feedback avg rating
      db.from("feedback").select("rating").not("rating", "is", null).not("user_id", "in", `(${demoUserIds.join(",")})`),
    ]);

    // Unique active users
    const wau = new Set((wauData || []).map(r => r.user_id)).size;
    const dau = new Set((dauData || []).map(r => r.user_id)).size;

    // Retention: users who signed up 7+ days ago and were active in last 7 days
    const oldUserIds = new Set(realAuthUsers.filter(u => new Date(u.created_at) < new Date(day7ago)).map(u => u.id));
    const { data: recentActivity } = await db.from("crop_instances").select("user_id").gte("updated_at", day7ago).not("user_id", "in", `(${demoUserIds.join(",")})`);
    const retainedWeek1 = (recentActivity || []).filter(r => oldUserIds.has(r.user_id));
    const week1Retention = oldUserIds.size > 0 ? Math.round((new Set(retainedWeek1.map(r => r.user_id)).size / oldUserIds.size) * 100) : null;

    // Week 4 retention
    const users28agoIds = new Set(realAuthUsers.filter(u => new Date(u.created_at) < new Date(day28ago)).map(u => u.id));
    const retained28 = (recentActivity || []).filter(r => users28agoIds.has(r.user_id));
    const week4Retention = users28agoIds.size > 0 ? Math.round((new Set(retained28.map(r => r.user_id)).size / users28agoIds.size) * 100) : null;

    // Activation: users who completed onboarding (have a profile)
    const activationRate = totalSignups > 0 ? Math.round((totalActivated / totalSignups) * 100) : 0;

    // Average crops per activated user
    const avgCropsPerUser = totalActivated > 0 ? (totalCrops / totalActivated).toFixed(1) : 0;

    // Task completion rate
    const tasksPending = tasksGenerated || 0; // active incomplete tasks
    const taskCompletionRate = (tasksPending + tasksCompleted) > 0
      ? Math.round((tasksCompleted / (tasksPending + tasksCompleted)) * 100)
      : 0;

    // Week on week growth
    const wowGrowth = newSignupsLastWeek > 0 ? Math.round(((newSignupsWeek - newSignupsLastWeek) / newSignupsLastWeek) * 100) : null;

    // Average feeds per user
    const avgFeedsPerUser = totalActivated > 0 ? (totalFeeds / totalActivated).toFixed(1) : 0;

    res.json({
      // User growth
      totalSignups,
      totalActivated,
      newSignupsWeek,
      wowGrowth,
      activationRate,

      // Engagement
      wau,
      dau,
      dauWauRatio: wau > 0 ? (dau / wau).toFixed(2) : null,

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

      // Retention
      week1Retention,
      week4Retention,

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

// GET /admin/feedback — admin only
app.get("/admin/feedback", requireAuth, requireAdmin, async (req, res) => {
  const { data, error } = await supabaseService
    .from("feedback")
    .select("*, profiles(name)")
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });

  // Enrich with email from auth.users
  const { data: { users } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
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
  const { data: authUsers } = await supabaseService.auth.admin.listUsers();
  const users = authUsers?.users || [];

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
  // Run expiry only — rule engine runs on cron and crop changes, not every page view
  await expireOverdueTasks(req.user.id, req.db);

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
      .select("*, crop:crop_instance_id(name, variety), area:area_id(name)")
      .eq("user_id", req.user.id).is("completed_at", null)
      .order("urgency",  { ascending: false })
      .order("due_date", { ascending: true }),
    req.db.from("crop_instances")
      .select("id, name, variety, variety_id, sown_date, area_id, missed_task_note, crop_def:crop_def_id(harvest_month_start, harvest_month_end, days_to_maturity_min, pest_window_start, pest_window_end, pest_notes)")
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
    .filter(c => c.crop_def?.harvest_month_start)
    .map(c => ({
      crop:             c.name,
      variety:          c.variety || null,
      crop_instance_id: c.id,   // required to mark crop as harvested + preserve rotation history
      window_start:     new Date(year, c.crop_def.harvest_month_start - 1, 1).toISOString().split("T")[0],
      window_end:       new Date(year, c.crop_def.harvest_month_end   - 1, 28).toISOString().split("T")[0],
    }));

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

  res.json({
    user:             profile?.name,
    profile_photo:    profile?.photo_url || null,
    plan:             profile?.plan || "free",
    tasks: {
      tasks:     tasks, // full list including overdue
      today:     tasks.filter(t => t.due_date <= today),
      this_week: tasks.filter(t => t.due_date > today && t.due_date <= weekEnd),
      coming_up: tasks.filter(t => t.due_date > weekEnd),
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
// Phase 1: routes exist, table stores records. AI call + UI are Phase 2.
// Free plan: 3 diagnoses/month. Grow/Pro: unlimited.
// =============================================================================

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

app.post("/diagnoses", requireAuth,
  [body("crop_instance_id").optional().isUUID()],
  async (req, res) => {
    if (!validate(req, res)) return;

    // Plan check — free users capped at 3 diagnoses per calendar month
    const { data: profile } = await req.db.from("profiles")
      .select("plan").eq("id", req.user.id).single();
    if (!profile?.plan || profile.plan === "free") {
      const monthStart = new Date();
      monthStart.setDate(1); monthStart.setHours(0, 0, 0, 0);
      const { count } = await req.db.from("diagnosis_log")
        .select("id", { count: "exact", head: true })
        .eq("user_id", req.user.id)
        .gte("created_at", monthStart.toISOString());
      if (count >= 3) {
        return res.status(403).json({
          error: "Monthly diagnosis limit reached on free plan. Upgrade to Grow for unlimited diagnoses.",
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
    .select("id, name, status, stage, crop_def_id, sown_date, stage")
    .eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });
  const { data: obs, error: obsErr } = await req.db.from("observation_logs").insert({
    user_id: req.user.id, crop_id: req.params.id,
    observed_at: new Date().toISOString().split("T")[0],
    observation_type, symptom_code: symptom_code || null,
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

  // Apply timeline offset if provided (signed integer: positive = behind, negative = ahead)
  if (typeof timeline_offset_days === "number") {
    updates.timeline_offset_days = timeline_offset_days;
    engineActions.push("timeline_offset_applied");
  }

  if (Object.keys(updates).length > 0) { updates.updated_at = new Date().toISOString(); await req.db.from("crop_instances").update(updates).eq("id", req.params.id).eq("user_id", req.user.id); }
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

// POST /crops/:id/log-action — log a manual action (watered, fed, pruned, weeded, note)
// Updates relevant crop fields, writes observation, recalculates schedule
app.post("/crops/:id/log-action", requireAuth, async (req, res) => {
  const { action_type, notes } = req.body;
  // action_type: "watered" | "fed" | "pruned" | "weeded" | "note"
  if (!action_type) return res.status(400).json({ error: "action_type required" });

  const { data: crop, error: cropErr } = await req.db.from("crop_instances")
    .select("id, name, area_id, last_watered_at, last_fed_at, crop_def_id")
    .eq("id", req.params.id).eq("user_id", req.user.id).single();
  if (cropErr || !crop) return res.status(404).json({ error: "Crop not found" });

  const now = new Date().toISOString();
  const today = now.split("T")[0];

  // Log the observation
  await req.db.from("observation_logs").insert({
    user_id:          req.user.id,
    crop_id:          req.params.id,
    observed_at:      today,
    observation_type: action_type,
    notes:            notes || null,
  });

  const updates = { updated_at: now };
  let nextActionHint = null;

  if (action_type === "watered") {
    // Update last_watered_at on the crop and all crops in the same area
    updates.last_watered_at = now;
    if (crop.area_id) {
      await supabaseService.from("crop_instances")
        .update({ last_watered_at: now, updated_at: now })
        .eq("area_id", crop.area_id).eq("user_id", req.user.id);
    }
    nextActionHint = "Next watering check in a few days";
  } else if (action_type === "fed") {
    updates.last_fed_at = now;
    // Get feed interval from crop def to give a useful hint
    const { data: def } = await supabaseService.from("crop_definitions")
      .select("feed_interval_days").eq("id", crop.crop_def_id).single();
    const interval = def?.feed_interval_days || 14;
    nextActionHint = `Next feed in about ${interval} days`;
  } else if (action_type === "pruned") {
    nextActionHint = "Pruning logged — growth should improve over the next few days";
  } else if (action_type === "weeded") {
    nextActionHint = "Weeding logged";
  } else if (action_type === "note") {
    nextActionHint = "Note saved";
  }

  await req.db.from("crop_instances").update(updates).eq("id", req.params.id).eq("user_id", req.user.id);

  // Recalculate tasks so watered/fed timings reset from today
  if (action_type === "watered" || action_type === "fed") {
    await runRuleEngine(req.user.id);
  }

  res.json({ ok: true, action_type, next_action_hint: nextActionHint });
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
    task_completed:   ["tasks_completed_total","tasks_completed_this_month","tasks_completed_this_season","current_streak_days"],
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

    const { data: { users } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
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
        current_month_key: monthKey, current_season_key: currentSeason, updated_at: now.toISOString(),
      };
      counterUpserts.push(counters);

      for (const badge of (allBadges || [])) {
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

app.post("/cron/push-morning", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { data: profiles } = await supabaseService.from("profiles").select("id");
    if (!profiles?.length) return res.json({ processed: 0, sent: 0 });
    let sent = 0;
    for (const p of profiles) {
      try { const result = await runNotificationsForUser(supabaseService, p.id, "morning"); if (result.sent > 0) sent++; } catch(e) { captureError("PushMorning", e, { userId: p.id }); }
    }
    console.log(`[PushMorning] Sent to ${sent}/${profiles.length} users`);
    // Email fallback — for users with no push token or push disabled
    const emailResult = await runDailyEmailFallback(supabaseService);
    console.log(`[EmailFallback] Sent: ${emailResult.sent}, Skipped: ${emailResult.skipped}`);
    res.json({ ok: true, processed: profiles.length, push_sent: sent, email_sent: emailResult.sent });
  } catch(e) { captureError("PushMorning", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/push-evening", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { data: profiles } = await supabaseService.from("profiles").select("id");
    if (!profiles?.length) return res.json({ processed: 0, sent: 0 });
    let sent = 0;
    for (const p of profiles) {
      try { const result = await runNotificationsForUser(supabaseService, p.id, "evening"); if (result.sent > 0) sent++; } catch(e) { captureError("PushEvening", e, { userId: p.id }); }
    }
    console.log(`[PushEvening] Sent to ${sent}/${profiles.length} users`);
    res.json({ ok: true, processed: profiles.length, sent });
  } catch(e) { captureError("PushEvening", e); res.status(500).json({ error: e.message }); }
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
  try {
    const result = await runNudgeUnactivated(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("NudgeUnactivated", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/nudge-unconfirmed", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runNudgeUnconfirmed(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("NudgeUnconfirmed", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/feedback-sequence", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runFeedbackSequence(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("FeedbackSequence", e); res.status(500).json({ error: e.message }); }
});

app.post("/cron/waitlist-invites", async (req, res) => {
  const cronAuth = req.headers["x-cron-secret"] === process.env.CRON_SECRET || req.headers["authorization"] === `Bearer ${process.env.CRON_SECRET}`;
  if (!cronAuth) return res.status(401).json({ error: "Unauthorised" });
  try {
    const result = await runWaitlistInvites(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("WaitlistInvites", e); res.status(500).json({ error: e.message }); }
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
  try {
    const result = await runReengagement(supabaseService);
    res.json({ ok: true, ...result });
  } catch(e) { captureError("Reengagement", e); res.status(500).json({ error: e.message }); }
});

// =============================================================================
// CRON — called by Vercel Cron at 06:00 UTC daily
// Protected by CRON_SECRET header.
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