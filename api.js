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
const allowedOrigins = [
  "https://vercro.com",
  "https://www.vercro.com",
  "https://app.vercro.com",
  "https://grow-smart-frontend.vercel.app",
  process.env.FRONTEND_URL,
].filter(Boolean);
app.use(cors({ origin: (origin, cb) => cb(null, !origin || allowedOrigins.includes(origin)) }));
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
  const companionKeywords = ["lavender","marigold","nasturtium","phacelia","borage","calendula","chamomile","alyssum","cosmos","sunflower","cornflower"];
  const isCompanionPlant = companionKeywords.some(c => submittedName.toLowerCase().includes(c));

  try {
    const prompt = `You are a horticultural expert for UK home growers and allotment holders.
A user has added a ${isCompanionPlant ? "companion/beneficial plant" : "crop"} to their garden with the following details:
- ${isCompanionPlant ? "Plant" : "Crop"} name: "${submittedName}"
- Variety: "${submittedVariety || "not specified"}"

Your task:
1. Determine if this is a real, growable plant in the UK. ${isCompanionPlant ? "Companion plants, flowers, and beneficial plants are all valid." : "Only vegetables, fruit, and herbs are valid."} Reject nonsense or unrecognisable names.
2. If real, correct any spelling errors.
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
    .select("*, area:area_id(name, type), crop_def:crop_def_id(name, harvest_month_start, harvest_month_end, days_to_maturity_min, sow_method), variety:variety_id(name, days_to_maturity_min)")
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
      is_companion,
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
      source:               is_companion ? "companion" : (source || "manual"),
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

// POST /crops/:id/confirm-stage — lifecycle confirmation from Quick Crop Check
app.post("/crops/:id/confirm-stage", requireAuth, async (req, res) => {
  const { stage, confirmed } = req.body;
  if (!stage) return res.status(400).json({ error: "stage required" });
  const validStages = ["seed","seedling","vegetative","flowering","fruiting","harvesting","finished"];
  if (!validStages.includes(stage)) return res.status(400).json({ error: "invalid stage" });

  if (confirmed) {
    // User confirmed — update the stage
    const { data, error } = await req.db.from("crop_instances")
      .update({
        stage,
        stage_confidence:          "confirmed",
        stage_check_snoozed_until: null, // clear any snooze
        updated_at:                new Date().toISOString(),
      })
      .eq("id", req.params.id)
      .eq("user_id", req.user.id)
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    await runRuleEngine(req.user.id);
    // Return full crop so frontend can patch local state immediately
    res.json({ confirmed: true, stage, crop: data });
  } else {
    // User said "not yet" — snooze for 7 days AND add 7 to stage_delay_days
    const snoozeUntil = new Date(Date.now() + 7 * 86400000).toISOString();
    const { data: current } = await req.db.from("crop_instances")
      .select("stage_delay_days")
      .eq("id", req.params.id)
      .eq("user_id", req.user.id)
      .single();
    const currentDelay = current?.stage_delay_days || 0;
    const { data, error } = await req.db.from("crop_instances")
      .update({
        stage_check_snoozed_until: snoozeUntil,
        stage_delay_days:          currentDelay + 7,
        updated_at:                new Date().toISOString(),
      })
      .eq("id", req.params.id)
      .eq("user_id", req.user.id)
      .select().single();
    if (error) return res.status(500).json({ error: error.message });
    // Return full crop so frontend can patch local state immediately
    res.json({ confirmed: false, snoozed_until: snoozeUntil, stage_delay_days: currentDelay + 7, crop: data });
  }
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
  const today       = completedAt.split("T")[0];

  const { data, error } = await req.db.from("tasks")
    .update({ completed_at: completedAt })
    .eq("id", req.params.id).eq("user_id", req.user.id).select().single();
  if (error) return res.status(500).json({ error: error.message });

  // Auto-update crop status + dates when lifecycle tasks are completed
  if (data.crop_instance_id) {
    const meta = data.meta ? (typeof data.meta === "string" ? JSON.parse(data.meta) : data.meta) : {};
    const transition = meta.status_transition;

    if (data.task_type === "feed") {
      await req.db.from("crop_instances")
        .update({ last_fed_at: completedAt, updated_at: completedAt })
        .eq("id", data.crop_instance_id);

    } else if (data.task_type === "sow" && transition === "sown") {
      const sowMethod = meta.sow_method || "outdoors";
      const newStatus = sowMethod === "indoors" ? "sown_indoors" : "sown_outdoors";
      await req.db.from("crop_instances")
        .update({ status: newStatus, sown_date: today, updated_at: completedAt })
        .eq("id", data.crop_instance_id);
      await runRuleEngine(req.user.id);

    } else if (data.task_type === "transplant" && transition === "transplanted") {
      await req.db.from("crop_instances")
        .update({ status: "transplanted", transplant_date: today, updated_at: completedAt })
        .eq("id", data.crop_instance_id);
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
// PLANTING SUGGESTIONS
// AI-powered suggestions for empty beds. Stored per area, cleared when a crop
// is added. One generation per empty bed per planting cycle.
// =============================================================================

app.get("/areas/:id/suggestions", requireAuth, async (req, res) => {
  // Verify ownership
  const { data: area } = await req.db.from("growing_areas")
    .select("id, name, type, location_id, locations(user_id)")
    .eq("id", req.params.id).single();
  if (!area || area.locations?.user_id !== req.user.id)
    return res.status(403).json({ error: "Not authorised" });

  const { data } = await req.db.from("planting_suggestions")
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

  // Get current crops in this area
  const { data: areaCrops } = await db.from("crop_instances")
    .select("name, variety, status")
    .eq("area_id", req.params.id).eq("active", true)
    .not("status", "eq", "harvested");
  const hasExistingCrops = (areaCrops?.length || 0) > 0;
  const currentAreaCropsStr = hasExistingCrops
    ? areaCrops.map(c => `${c.name}${c.variety ? ` (${c.variety})` : ""}`).join(", ")
    : null;

  // Always regenerate — upsert replaces existing
  // (suggestions are invalidated when crops change via clearSuggestions)

  // Get crop history for this area
  const { data: history } = await db.from("crop_instances")
    .select("name, variety, status, harvested_at")
    .eq("area_id", req.params.id)
    .eq("active", false)
    .order("created_at", { ascending: false })
    .limit(6);

  // Get what user is growing elsewhere (to avoid duplication)
  const { data: allCrops } = await db.from("crop_instances")
    .select("name").eq("user_id", req.user.id).eq("active", true);

  const month      = new Date().toLocaleString("en-GB", { month: "long" });
  const areaType   = area.type?.replace(/_/g, " ") || "growing area";
  const postcode   = area.locations?.postcode || "UK";
  const historyStr = history?.length
    ? history.map(c => `${c.name}${c.variety ? " (" + c.variety + ")" : ""}`).join(", ")
    : "nothing previously recorded";
  const currentStr = allCrops?.length
    ? [...new Set(allCrops.map(c => c.name))].join(", ")
    : "nothing currently";

  try {
    const prompt = hasExistingCrops
      ? `You are a UK horticultural expert advising a home grower or allotment holder.
Area details:
- Type: ${areaType}
- Location postcode: ${postcode}
- Currently growing in this bed: ${currentAreaCropsStr}
- Also growing elsewhere: ${currentStr}
- Current month: ${month}
This bed already has crops. Return exactly 3 suggestions to improve this bed — companion plants, interplanting ideas, and beneficial plants.
For each suggestion explain clearly WHY it benefits the existing crops.
Respond ONLY with a JSON array of exactly 3 items — no markdown, no explanation:
[
  {
    "type": "companion",
    "crop": "Plant name",
    "variety": "Specific variety if relevant, or null",
    "reason": "One clear sentence explaining exactly how this benefits the crops already in this bed",
    "sow_note": "When and how to add this in one sentence",
    "companion_note": "Which specific crop it helps most and how"
  },
  {
    "type": "interplant",
    "crop": "Crop name",
    "variety": "Specific variety name",
    "reason": "One sentence why this works well alongside the existing crops",
    "sow_note": "When and how to sow in one sentence",
    "companion_note": "How it fits with what is already growing"
  },
  {
    "type": "beneficial",
    "crop": "Plant name e.g. Lavender, Phacelia, Borage, Nasturtium, Marigold",
    "variety": "Specific variety if relevant, or null",
    "reason": "One sentence on the specific benefit — pest deterrent, pollinator attractor, or soil improver",
    "sow_note": "When and how to plant in one sentence",
    "companion_note": "Which pest it deters or which beneficial insect it attracts"
  }
]`
      : `You are a UK horticultural expert advising a home grower or allotment holder.
Area details:
- Type: ${areaType}
- Location postcode: ${postcode}
- Current month: ${month}
- Previously grown here: ${historyStr}
- Currently growing elsewhere in their garden: ${currentStr}
Return exactly 3 suggestions: 2 crop suggestions and 1 companion or beneficial plant.
Consider crop rotation, seasonality, and what the grower already grows. Suggest specific named varieties.
Respond ONLY with a JSON array of exactly 3 items — no markdown, no explanation:
[
  {
    "type": "crop",
    "crop": "Crop name",
    "variety": "Specific variety name",
    "reason": "One sentence why this is ideal right now for this grower",
    "sow_note": "When and how to sow in one sentence",
    "companion_note": "Companion benefit or null"
  },
  {
    "type": "crop",
    "crop": "Crop name",
    "variety": "Specific variety name",
    "reason": "One sentence why this is ideal right now",
    "sow_note": "When and how to sow in one sentence",
    "companion_note": "Companion benefit or null"
  },
  {
    "type": "beneficial",
    "crop": "Plant name e.g. Marigold, Nasturtium, Phacelia, Borage, Lavender",
    "variety": "Specific variety or null",
    "reason": "One sentence on why this benefits this bed",
    "sow_note": "When and how to plant in one sentence",
    "companion_note": "Specific pest it deters or pollinator it attracts"
  }
]`;

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
        messages: [{ role: "user", content: prompt }],
      }),
    });

    const raw  = await response.json();
    const text = raw.content?.[0]?.text || "";
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) throw new Error("No JSON array in response");
    const suggestions = JSON.parse(match[0]);

    // Store suggestions
    await db.from("planting_suggestions").upsert({
      area_id:      req.params.id,
      suggestions,
      generated_at: new Date().toISOString(),
    }, { onConflict: "area_id" });

    res.json({ suggestions, generated_at: new Date().toISOString() });
  } catch (e) {
    console.error("[Suggestions] Error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

// DELETE suggestions when a crop is added to the area (called internally)
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

    // User growth — auth users (everyone) vs profiles (completed onboarding)
    const { data: { users: authUsers } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
    const totalSignups   = authUsers.length;
    const newSignupsWeek = authUsers.filter(u => new Date(u.created_at) >= new Date(day7ago)).length;
    const newSignupsLastWeek = authUsers.filter(u => new Date(u.created_at) >= new Date(day28ago) && new Date(u.created_at) < new Date(day7ago)).length;

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

    ] = await Promise.all([
      db.from("profiles").select("*", { count: "exact", head: true }),

      db.from("crop_instances").select("user_id").gte("updated_at", day7ago),
      db.from("crop_instances").select("user_id").gte("updated_at", day1ago),

      db.from("locations").select("*", { count: "exact", head: true }),
      db.from("growing_areas").select("*", { count: "exact", head: true }),
      db.from("crop_instances").select("*", { count: "exact", head: true }),

      db.from("crop_instances").select("*", { count: "exact", head: true }).not("sown_date", "is", null),
      db.from("crop_instances").select("*", { count: "exact", head: true }).eq("status", "harvested"),
      db.from("harvest_log").select("*", { count: "exact", head: true }),

      db.from("tasks").select("*", { count: "exact", head: true }),
      db.from("tasks").select("*", { count: "exact", head: true }).not("completed_at", "is", null),

      db.from("user_feeds").select("*", { count: "exact", head: true }),

      db.from("crop_photos").select("*", { count: "exact", head: true }),

      db.from("varieties").select("*", { count: "exact", head: true }),
      db.from("harvest_log").select("*", { count: "exact", head: true }).not("quantity_value", "is", null),
    ]);

    // Unique active users
    const wau = new Set((wauData || []).map(r => r.user_id)).size;
    const dau = new Set((dauData || []).map(r => r.user_id)).size;

    // Retention: users who signed up 7+ days ago and were active in last 7 days
    const oldUserIds = new Set(authUsers.filter(u => new Date(u.created_at) < new Date(day7ago)).map(u => u.id));
    const { data: recentActivity } = await db.from("crop_instances").select("user_id").gte("updated_at", day7ago);
    const retainedWeek1 = (recentActivity || []).filter(r => oldUserIds.has(r.user_id));
    const week1Retention = oldUserIds.size > 0 ? Math.round((new Set(retainedWeek1.map(r => r.user_id)).size / oldUserIds.size) * 100) : null;

    // Week 4 retention
    const users28agoIds = new Set(authUsers.filter(u => new Date(u.created_at) < new Date(day28ago)).map(u => u.id));
    const retained28 = (recentActivity || []).filter(r => users28agoIds.has(r.user_id));
    const week4Retention = users28agoIds.size > 0 ? Math.round((new Set(retained28.map(r => r.user_id)).size / users28agoIds.size) * 100) : null;

    // Activation: users who completed onboarding (have a profile)
    const activationRate = totalSignups > 0 ? Math.round((totalActivated / totalSignups) * 100) : 0;

    // Average crops per activated user
    const avgCropsPerUser = totalActivated > 0 ? (totalCrops / totalActivated).toFixed(1) : 0;

    // Task completion rate
    const taskCompletionRate = tasksGenerated > 0 ? Math.round((tasksCompleted / tasksGenerated) * 100) : 0;

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
      tasksGenerated,
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
    .select("*, profiles(name, email)")
    .order("created_at", { ascending: false });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data || []);
});

// =============================================================================
// ADMIN ENDPOINTS — restricted to mark@wynyardadvisory.co.uk
// =============================================================================

const ADMIN_EMAIL = "mark@wynyardadvisory.co.uk";

async function requireAdmin(req, res, next) {
  if (!req.user) return res.status(401).json({ error: "Unauthorised" });
  if (req.user.email !== ADMIN_EMAIL) return res.status(403).json({ error: "Forbidden" });
  next();
}

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
  const { data: crops } = await req.db.from("crop_instances").select("user_id").eq("active", true);
  const cropCounts = {};
  (crops || []).forEach(c => { cropCounts[c.user_id] = (cropCounts[c.user_id] || 0) + 1; });

  // Get last task completion per user
  const { data: tasks } = await req.db.from("tasks").select("user_id, completed_at").eq("completed", true).order("completed_at", { ascending: false });
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

// POST /harvest-log — create a harvest entry + mark crop as harvested
app.post("/harvest-log", requireAuth,
  [body("crop_name").trim().notEmpty()],
  async (req, res) => {
    if (!validate(req, res)) return;
    const {
      crop_instance_id, crop_name, variety,
      harvested_at, yield_score, quality_score,
      quantity_value, quantity_unit, notes,
    } = req.body;

    const { data, error } = await req.db.from("harvest_log").insert({
      user_id:          req.user.id,
      crop_instance_id: crop_instance_id || null,
      crop_name,
      variety:          variety || null,
      harvested_at:     harvested_at || new Date().toISOString().split("T")[0],
      yield_score:      yield_score || null,
      quality_score:    quality_score || null,
      quantity_value:   quantity_value || null,
      quantity_unit:    quantity_unit || null,
      notes:            notes || null,
    }).select().single();

    if (error) return res.status(500).json({ error: error.message });

    // Mark crop instance as harvested
    if (crop_instance_id) {
      await req.db.from("crop_instances")
        .update({ status: "harvested", updated_at: new Date().toISOString() })
        .eq("id", crop_instance_id)
        .eq("user_id", req.user.id);
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
  const today   = todayISO();
  const weekEnd = weekEndISO();
  // Always run rule engine on dashboard load — ensures tasks are fresh
  // without requiring user to edit/save a crop manually
  await runRuleEngine(req.user.id);
  await expireOverdueTasks(req.user.id, req.db);


  const [tasksRes, cropsRes, profileRes, harvestRes] = await Promise.all([
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
  ]);

  const tasks   = tasksRes.data  || [];
  const crops   = cropsRes.data  || [];
  const profile = profileRes.data;
  const year    = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;

  // ── Harvest forecast ──────────────────────────────────────────────────────
  const harvestForecast = crops
    .filter(c => c.crop_def?.harvest_month_start)
    .map(c => ({
      crop:         c.name,
      variety:      c.variety || null,
      window_start: new Date(year, c.crop_def.harvest_month_start - 1, 1).toISOString().split("T")[0],
      window_end:   new Date(year, c.crop_def.harvest_month_end   - 1, 28).toISOString().split("T")[0],
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
// EMAIL AUTOMATION
// =============================================================================

const RESEND_API_KEY = process.env.RESEND_API_KEY;

async function sendEmail(to, subject, html) {
  const r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${RESEND_API_KEY}` },
    body: JSON.stringify({ from: "Mark at Vercro <hello@vercro.com>", to: [to], subject, html }),
  });
  if (!r.ok) throw new Error(`Resend error: ${await r.text()}`);
  return r.json();
}

// Admin — invite all waitlist users — responds immediately, processes in background
app.post("/admin/invite-waitlist", requireAuth, requireAdmin, async (req, res) => {
  const { data: waitlist } = await supabaseService
    .from("waitlist").select("email, name, invite_sent_at").eq("status", "waitlist");
  if (!waitlist?.length) return res.json({ sent: 0, queued: 0 });
  const pending = waitlist.filter(w => !w.invite_sent_at);
  // Respond immediately so Vercel doesn't timeout
  res.json({ ok: true, queued: pending.length });
  // Process in background
  let sent = 0;
  for (const w of pending) {
    const name = w.name ? w.name.split(" ")[0] : "there";
    try {
      await sendEmail(w.email, "Great news — your Vercro spot is ready 🌱", `
        <div style="font-family:Georgia,serif;max-width:560px;margin:0 auto;padding:40px 24px;color:#1a1a1a">
          <div style="font-size:32px;margin-bottom:16px">🌱</div>
          <h1 style="font-size:24px;font-weight:700;margin-bottom:8px;color:#2F5D50">Great news, ${name}!</h1>
          <p style="font-size:15px;color:#6E6E6E;line-height:1.6;margin-bottom:20px">
            You applied for early access to Vercro a little while ago and we put you on our reserve list. We're now opening up access to everyone — and that includes you.
          </p>
          <p style="font-size:15px;color:#6E6E6E;line-height:1.6;margin-bottom:28px">
            Your account is ready and waiting. It takes less than 2 minutes to set up your first crop.
          </p>
          <div style="background:#f0f7f4;border-radius:14px;padding:28px;text-align:center;margin-bottom:28px">
            <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin-bottom:20px">Get started at:</p>
            <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#fff;text-decoration:none;padding:18px 40px;border-radius:12px;font-weight:700;font-size:17px">Open Vercro →</a>
          </div>
          <p style="font-size:14px;color:#6E6E6E;line-height:1.6">If you have any questions, just reply to this email. We read everything.</p>
          <hr style="border:none;border-top:1px solid #e8e8e8;margin:32px 0"/>
          <p style="font-size:12px;color:#999">Vercro · hello@vercro.com · <a href="https://vercro.com" style="color:#2F5D50">vercro.com</a></p>
        </div>`
      );
      await supabaseService.from("waitlist").update({
        status: "accepted", invite_sent_at: new Date().toISOString(), accepted_at: new Date().toISOString()
      }).eq("email", w.email);
      sent++;
      console.log(`[Invite] Sent to ${w.email} (${sent}/${pending.length})`);
    } catch(e) { console.error(`[Invite] Failed ${w.email}:`, e.message); }
  }
  console.log(`[Invite] Done: ${sent}/${pending.length} sent`);
});

// Cron — nudge accepted but not onboarded users
app.post("/cron/nudge-unactivated", async (req, res) => {
  if (req.headers["x-cron-secret"] !== process.env.CRON_SECRET) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { data: accepted } = await supabaseService.from("waitlist")
      .select("email, name, nudge_count, last_nudge_at").eq("status", "accepted");
    if (!accepted?.length) return res.json({ sent: 0 });
    const { data: { users: authUsers } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
    const { data: profiles } = await supabaseService.from("profiles").select("id");
    const profileIds = new Set((profiles || []).map(p => p.id));
    const onboardedEmails = new Set((authUsers || []).filter(u => profileIds.has(u.id)).map(u => u.email));
    const now = new Date(); let sent = 0;
    const subjects = ["Your Vercro garden is waiting 🌱","Don't miss the growing season — Vercro is ready for you","Your crops won't plant themselves 🥕","Still time to get your garden on track","Last reminder — your Vercro access is ready"];
    const intros = ["You signed up for the Vercro beta — your account is ready and waiting.","A quick reminder that your Vercro garden planner is set up and ready to go.","The growing season won't wait — and neither will Vercro. Your account is all set.","You're missing out on personalised daily tasks, weather alerts and harvest forecasts for your garden.","This is our final reminder — your Vercro beta access is ready whenever you are."];
    for (const w of accepted) {
      if (onboardedEmails.has(w.email)) continue;
      if (w.last_nudge_at && (now - new Date(w.last_nudge_at)) / 3600000 < 23) continue;
      if ((w.nudge_count || 0) >= 5) continue;
      const n = (w.nudge_count || 0);
      const name = w.name ? w.name.split(" ")[0] : "there";
      try {
        await sendEmail(w.email, subjects[n], `<div style="font-family:Georgia,serif;max-width:560px;margin:0 auto;padding:40px 24px;color:#1a1a1a"><div style="font-size:32px;margin-bottom:16px">🌱</div><h1 style="font-size:22px;font-weight:700;margin-bottom:12px;color:#2F5D50">Hi ${name},</h1><p style="font-size:15px;color:#6E6E6E;line-height:1.6;margin-bottom:24px">${intros[n]}</p><div style="background:#f0f7f4;border-radius:14px;padding:28px;text-align:center;margin-bottom:28px"><p style="font-size:15px;color:#1a1a1a;font-weight:700;margin-bottom:20px">Get started at:</p><a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#fff;text-decoration:none;padding:16px 36px;border-radius:12px;font-weight:700;font-size:16px">Open Vercro →</a></div><hr style="border:none;border-top:1px solid #e8e8e8;margin:32px 0"/><p style="font-size:12px;color:#999">Vercro · hello@vercro.com</p></div>`);
        await supabaseService.from("waitlist").update({ nudge_count: n+1, last_nudge_at: now.toISOString() }).eq("email", w.email);
        sent++;
      } catch(e) { console.error(`[Nudge] Failed ${w.email}:`, e.message); }
    }
    res.json({ sent });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Cron — feedback email sequence post-onboarding
app.post("/cron/feedback-sequence", async (req, res) => {
  if (req.headers["x-cron-secret"] !== process.env.CRON_SECRET) return res.status(401).json({ error: "Unauthorised" });
  try {
    const { data: { users: authUsers } } = await supabaseService.auth.admin.listUsers({ perPage: 1000 });
    const { data: profiles } = await supabaseService.from("profiles").select("id, name, feedback_emails_sent, last_feedback_email_at, created_at");
    const profileMap = {}; (profiles || []).forEach(p => { profileMap[p.id] = p; });
    const now = new Date(); let sent = 0;
    const sequences = [
      { subject: "How are you finding Vercro so far? 🌱", question: "What's working well? And what would you change or improve?", intro: "You've had a couple of days with Vercro now — we'd love to know how it's going." },
      { subject: "A week in — what do you think? 🥕", question: "Is it helping with your garden? What's missing or frustrating you?", intro: "You've had a week with Vercro — enough time to form a real opinion." },
      { subject: "One last question from the Vercro team 🌾", question: "If you could only change ONE thing about Vercro, what would it be?", intro: "You've been using Vercro for a few weeks now — thank you for being one of our early testers." },
    ];
    for (const user of authUsers) {
      const profile = profileMap[user.id];
      if (!profile) continue;
      const sent_count = profile.feedback_emails_sent || 0;
      if (sent_count >= 3) continue;
      const hoursSince = (now - new Date(profile.created_at)) / 3600000;
      const thresholds = [48, 168, 504];
      if (hoursSince < thresholds[sent_count]) continue;
      if (profile.last_feedback_email_at && (now - new Date(profile.last_feedback_email_at)) / 3600000 < 46) continue;
      const seq = sequences[sent_count];
      const name = profile.name ? profile.name.split(" ")[0] : "there";
      try {
        await sendEmail(user.email, seq.subject, `<div style="font-family:Georgia,serif;max-width:560px;margin:0 auto;padding:40px 24px;color:#1a1a1a"><div style="font-size:32px;margin-bottom:16px">🌱</div><h1 style="font-size:22px;font-weight:700;margin-bottom:12px;color:#2F5D50">Hi ${name},</h1><p style="font-size:15px;color:#6E6E6E;line-height:1.6;margin-bottom:20px">${seq.intro}</p><p style="font-size:16px;font-weight:700;color:#1a1a1a;font-style:italic;margin-bottom:20px;padding:16px;background:#f0f7f4;border-radius:12px;border-left:3px solid #2F5D50">"${seq.question}"</p><div style="text-align:center"><a href="mailto:hello@vercro.com?subject=Vercro feedback" style="display:inline-block;background:#2F5D50;color:#fff;text-decoration:none;padding:14px 32px;border-radius:12px;font-weight:700;font-size:15px">Reply with your thoughts →</a></div><p style="font-size:13px;color:#999;margin-top:24px;text-align:center">Or just reply directly to this email — we read everything.</p><hr style="border:none;border-top:1px solid #e8e8e8;margin:32px 0"/><p style="font-size:12px;color:#999">Vercro · hello@vercro.com</p></div>`);
        await supabaseService.from("profiles").update({ feedback_emails_sent: sent_count+1, last_feedback_email_at: now.toISOString() }).eq("id", user.id);
        sent++;
      } catch(e) { console.error(`[Feedback] Failed ${user.email}:`, e.message); }
    }
    res.json({ sent });
  } catch(e) { res.status(500).json({ error: e.message }); }
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