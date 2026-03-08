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
 *               express-validator cors dotenv helmet morgan
 *
 * .env:
 *   SUPABASE_URL=
 *   SUPABASE_SERVICE_KEY=
 *   SUPABASE_ANON_KEY=
 *   OPENWEATHER_API_KEY=
 *   FRONTEND_URL=
 *   CRON_SECRET=
 *   PORT=3001
 */

const express    = require("express");
const cors       = require("cors");
const helmet     = require("helmet");
const morgan     = require("morgan");
const { createClient } = require("@supabase/supabase-js");
const { body, validationResult } = require("express-validator");
require("dotenv").config();

const { RuleEngine } = require("./rule-engine");

// ── Supabase (service role — server only) ─────────────────────────────────────
const supabaseService = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ── App ───────────────────────────────────────────────────────────────────────
const app = express();
app.use(helmet());
app.use(cors({ origin: process.env.FRONTEND_URL || "*" }));
app.use(express.json());
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
async function runRuleEngine(userId) {
  try {
    const engine = new RuleEngine(supabaseService);
    const tasks  = await engine.runForUser(userId);
    console.log(`[RuleEngine] ${tasks.length} tasks generated for ${userId}`);
    return tasks;
  } catch (err) {
    console.error("[RuleEngine] Error:", err.message);
    return [];
  }
}

// =============================================================================
// HEALTH
// =============================================================================
app.get("/health", (_req, res) => res.json({ status: "ok", ts: new Date().toISOString() }));

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
  const { error } = await req.db.from("locations").delete()
    .eq("id", req.params.id).eq("user_id", req.user.id);
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
    "is_perennial": false,
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
    "classification": "e.g. Cherry, Maincrop, Heritage — or null",
    "days_to_maturity_min": 65,
    "days_to_maturity_max": 75,
    "notes": "what makes this variety distinctive"
  }
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
    const text = raw.content?.[0]?.text || "";

    let parsed;
    try {
      parsed = JSON.parse(text.replace(/```json|```/g, "").trim());
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
        const { data: newVar, error: varErr } = await db.from("varieties").insert({
          crop_def_id:          cropDefId,
          name:                 varietyData.name,
          classification:       varietyData.classification || null,
          days_to_maturity_min: varietyData.days_to_maturity_min || null,
          days_to_maturity_max: varietyData.days_to_maturity_max || null,
          notes:                varietyData.notes || null,
          is_default:           false,
          active:               true,
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
    .select("*, area:area_id(name, type), crop_def:crop_def_id(name, harvest_month_start, harvest_month_end, days_to_maturity_min), variety:variety_id(name, days_to_maturity_min)")
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
  res.json(data);
});

app.post("/crops", requireAuth,
  [body("area_id").isUUID(), body("name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const {
      area_id, name, variety, variety_id, crop_def_id,
      sown_date, transplanted_date, planted_out_date,
      establishment_method, quantity, notes,
      start_date_confidence, source,
      is_other_crop, is_other_variety,  // flags from UI "Other" selections
    } = req.body;

    // Derive location_id from area
    const { data: area } = await req.db.from("growing_areas")
      .select("location_id").eq("id", area_id).single();

    const { data, error } = await req.db.from("crop_instances").insert({
      user_id:              req.user.id,
      location_id:          area?.location_id || null,
      area_id,
      name,
      variety:              variety || null,
      variety_id:           variety_id || null,
      crop_def_id:          crop_def_id || null,
      sown_date:            sown_date || null,
      transplanted_date:    transplanted_date || null,
      planted_out_date:     planted_out_date || null,
      establishment_method: establishment_method || null,
      quantity:             quantity || 1,
      notes:                notes || null,
      photo_url:            null,
      start_date_confidence:start_date_confidence || "exact",
      source:               source || "manual",
    }).select().single();

    if (error) return res.status(500).json({ error: error.message });

    // Trigger enrichment if:
    // - crop is unknown (no crop_def_id), OR
    // - variety was typed as free text (variety present but no variety_id)
    const needsEnrichment = !data.crop_def_id || (!data.variety_id && data.variety);
    if (needsEnrichment) {
      await enrichCrop(data.id, name, variety || null);
    }

    await runRuleEngine(req.user.id);
    res.status(201).json({ ...data, enriching: needsEnrichment });
  }
);

app.put("/crops/:id", requireAuth, async (req, res) => {
  const allowed = [
    "variety","variety_id","sown_date","transplanted_date","planted_out_date",
    "establishment_method","stage","quantity","notes","area_id","photo_url",
    "start_date_confidence","last_fed_at",
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

  if (req.body.stage) await runRuleEngine(req.user.id);
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
  const { error } = await req.db.from("crop_instances")
    .update({ active: false, updated_at: new Date().toISOString() })
    .eq("id", req.params.id).eq("user_id", req.user.id);
  if (error) return res.status(500).json({ error: error.message });
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

  res.json({
    tasks: data,
    grouped: {
      today:     data.filter(t => t.due_date === today),
      this_week: data.filter(t => t.due_date > today && t.due_date <= weekEnd),
      coming_up: data.filter(t => t.due_date > weekEnd),
    },
  });
});

app.post("/tasks/:id/complete", requireAuth, async (req, res) => {
  const completedAt = new Date().toISOString();
  const { data, error } = await req.db.from("tasks")
    .update({ completed_at: completedAt })
    .eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });

  // Update last_fed_at when a feed task is completed
  if (data.task_type === "feed" && data.crop_instance_id) {
    await req.db.from("crop_instances")
      .update({ last_fed_at: completedAt, updated_at: completedAt })
      .eq("id", data.crop_instance_id);
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
    await req.db.from("crop_instances")
      .update({ last_fed_at: null })
      .eq("id", data.crop_instance_id);
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
  const today   = todayISO();
  const weekEnd = weekEndISO();

  const [tasksRes, cropsRes, profileRes, harvestRes] = await Promise.all([
    req.db.from("tasks")
      .select("*, crop:crop_instance_id(name, variety), area:area_id(name)")
      .eq("user_id", req.user.id).is("completed_at", null)
      .order("urgency",  { ascending: false })
      .order("due_date", { ascending: true }),
    req.db.from("crop_instances")
      .select("id, name, variety, variety_id, sown_date, area_id, crop_def:crop_def_id(harvest_month_start, harvest_month_end, days_to_maturity_min)")
      .eq("user_id", req.user.id).eq("active", true),
    req.db.from("profiles").select("name, plan").eq("id", req.user.id).single(),
    req.db.from("harvest_log")
      .select("id, harvested_at, quantity_g, crop:crop_instance_id(name)")
      .eq("user_id", req.user.id)
      .gte("harvested_at", new Date(new Date().getFullYear(), 0, 1).toISOString().split("T")[0])
      .order("harvested_at", { ascending: false })
      .limit(5),
  ]);

  const tasks  = tasksRes.data  || [];
  const crops  = cropsRes.data  || [];
  const year   = new Date().getFullYear();

  const harvestForecast = crops
    .filter(c => c.crop_def?.harvest_month_start)
    .map(c => ({
      crop:         c.name,
      variety:      c.variety || null,
      window_start: new Date(year, c.crop_def.harvest_month_start - 1, 1).toISOString().split("T")[0],
      window_end:   new Date(year, c.crop_def.harvest_month_end   - 1, 28).toISOString().split("T")[0],
    }));

  // Surface crops with missing data to prompt user
  // variety_id = linked variety from dropdown, variety = free text — either counts
  const missingData = crops
    .filter(c => (!c.variety_id && !c.variety) || !c.sown_date)
    .map(c => ({
      id:      c.id,
      name:    c.name,
      missing: [(!c.variety_id && !c.variety) && "variety", !c.sown_date && "sow date"].filter(Boolean),
    }));

  res.json({
    user:             profileRes.data?.name,
    plan:             profileRes.data?.plan || "free",
    tasks: {
      today:     tasks.filter(t => t.due_date === today),
      this_week: tasks.filter(t => t.due_date > today && t.due_date <= weekEnd),
      coming_up: tasks.filter(t => t.due_date > weekEnd),
    },
    crop_count:       crops.length,
    harvest_forecast: harvestForecast,
    missing_data:     missingData,
    recent_harvests:  harvestRes.data || [],
  });
});

// =============================================================================
// HARVEST LOG
// Phase 1: routes exist and data is stored. UI screen is Phase 2.
// =============================================================================

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

app.use((err, _req, res, _next) => {
  console.error(err.stack);
  res.status(500).json({ error: "Internal server error" });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Grow Smart API on :${PORT}`));

module.exports = app;