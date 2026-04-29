"use strict";

/**
 * GROW SMART — Rule Engine v2
 * ─────────────────────────────────────────────────────────────
 * Hybrid scheduling + alerting engine.
 *
 * Architecture:
 *   1. Crop Context Builder   — normalised per-crop context object
 *   2. Scheduled Rule Engine  — deterministic future task candidates
 *   3. Dynamic Risk Engine    — short-horizon weather + pest/disease alerts
 *   4. Task Materializer      — idempotent upsert via source_key
 *   5. Expiry Handler         — cleans up stale tasks/alerts
 *
 * Key design principles:
 *   - Engine runs are idempotent — safe to run multiple times per day
 *   - source_key uniqueness enforced at DB level — no duplicate tasks
 *   - Scheduled rules think in DATE WINDOWS not boolean conditions
 *   - Weather/pest rules are SHORT HORIZON ONLY (1–3 days)
 *   - Coming Up Soon populated by visible_from field, not due_date
 */

// ── Constants ─────────────────────────────────────────────────────────────────

const LOOKAHEAD_DAYS = {
  sow:        42,  // 6 weeks — show sowing tasks well ahead
  transplant: 56,  // 8 weeks — show transplant tasks ahead of May window
  feed:       14,
  harvest:    21,
  prune:      30,
  seasonal:   30,
  check:      14,
  harden_off: 21,
  default:    21,
};

const LEAD_TIME_DAYS = {
  sow:        7,
  transplant: 3,
  feed:       2,
  harvest:    5,
  prune:      5,
  protect:    1,
  default:    2,
};

const STAGE_ORDER = [
  "seed", "seedling", "vegetative", "flowering",
  "fruiting", "harvesting", "finished"
];

const STAGE_DTM_PERCENT = {
  seed:        0,
  seedling:    0.08,
  vegetative:  0.25,
  flowering:   0.55,
  fruiting:    0.70,
  harvesting:  0.90,
  finished:    1.10,
};

const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

// ── Date helpers ──────────────────────────────────────────────────────────────

function todayISO() {
  return new Date().toISOString().split("T")[0];
}

function addDays(dateStr, n) {
  const d = new Date(dateStr);
  d.setDate(d.getDate() + n);
  return d.toISOString().split("T")[0];
}

function daysBetween(a, b) {
  return Math.round((new Date(b) - new Date(a)) / 86400000);
}

function daysSince(dateStr) {
  if (!dateStr) return null;
  return Math.floor((Date.now() - new Date(dateStr).getTime()) / 86400000);
}

function currentMonth() {
  return new Date().getMonth() + 1;
}

function monthToDate(month, year = new Date().getFullYear(), day = 1) {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function withinLookahead(dateStr, lookaheadDays) {
  if (!dateStr) return false;
  const today = todayISO();
  const limit = addDays(today, lookaheadDays);
  return dateStr >= today && dateStr <= limit;
}

function isOverdue(dateStr) {
  return dateStr < todayISO();
}

// ── Source key builder ────────────────────────────────────────────────────────
// Deterministic unique key for idempotent upserts

function sourceKey(parts) {
  return Object.entries(parts)
    .map(([k, v]) => `${k}:${v}`)
    .join("|");
}

// Snap a date to a stable window anchor so window-based tasks
// don't regenerate every day with a new key.
// - expiryDays >= 21  → snap to month (YYYY-MM)   — monthly tasks
// - expiryDays >= 7   → snap to week  (YYYY-Www)  — weekly tasks
// - expiryDays < 7    → use exact date             — urgent/daily tasks
function windowAnchor(dateStr, expiryDays) {
  if (!dateStr) return dateStr;
  const d = new Date(dateStr + "T12:00:00Z");
  if (expiryDays >= 21) {
    // Month anchor: YYYY-MM
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}`;
  }
  if (expiryDays >= 7) {
    // ISO week anchor: YYYY-Www
    const jan4 = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
    const weekNum = Math.ceil(((d - jan4) / 86400000 + jan4.getUTCDay() + 1) / 7);
    return `${d.getUTCFullYear()}-W${String(weekNum).padStart(2, "0")}`;
  }
  return dateStr; // exact date for urgent tasks
}

// ── Frost offset helper ──────────────────────────────────────────────────────
// Shifts a sowing/transplant month by a frost offset (in weeks).
// Only applied to frost-sensitive crops. Clamped to valid month range 1–12.
// offsetWeeks < 0 = location has earlier last frost than UK baseline (milder)
// offsetWeeks > 0 = location has later last frost than UK baseline (cooler/northern)
function shiftMonth(month, offsetWeeks) {
  if (!month || !offsetWeeks) return month;
  const offsetMonths = Math.round(offsetWeeks / 4);
  return Math.max(1, Math.min(12, month + offsetMonths));
}

// ── Crop Context Builder ──────────────────────────────────────────────────────

function buildCropContext(crop, weather, envMods, userFeeds, observations = [], rainMm7dayActual = null) {
  const def = crop.crop_def || {};
  const variety = crop.variety || {};

  // Resolve effective values — variety overrides crop def
  const dtm_min = variety.days_to_maturity_min ?? def.days_to_maturity_min ?? null;
  const dtm_max = variety.days_to_maturity_max ?? def.days_to_maturity_max ?? null;
  const dtm     = dtm_min || 80; // fallback for stage estimation

  const sowStart       = variety.sow_window_start       ?? def.sow_window_start       ?? null;
  const sowEnd         = variety.sow_window_end         ?? def.sow_window_end         ?? null;
  const txStart        = variety.transplant_window_start ?? def.transplant_window_start ?? null;
  const txEnd          = variety.transplant_window_end   ?? def.transplant_window_end   ?? null;
  const harvestStart   = def.harvest_month_start ?? null;
  const harvestEnd     = def.harvest_month_end   ?? null;
  const feedInterval   = variety.feed_interval_days_override ?? def.feed_interval_days ?? null;
  const feedType       = def.feed_type ?? null;
  const frostSensitive = variety.frost_sensitive_override ?? def.frost_sensitive ?? true;
  const isPerennial    = def.is_perennial ?? false;
  const sowMethod      = def.sow_method ?? "either";
  const cropGroup      = def.category ?? null;

  // ── Frost offset — international climate calibration ──────────────────────
  // Uses sensitivity_band from crop_definitions to apply differential offsets:
  //   tender       → full offset (tomatoes, peppers, courgettes, beans etc)
  //   semi_hardy   → half offset capped at 2 weeks (lettuce, beetroot, peas etc)
  //   hardy        → no offset (kale, brassicas, root veg, herbs)
  //   overwintering→ no offset (garlic, overwintering onions etc)
  //   perennial    → no offset (fruit trees, berries etc)
  // Falls back to frostSensitive boolean if sensitivity_band not set.
  const sensitivityBand    = def.sensitivity_band || (frostSensitive ? "tender" : "hardy");
  const locationLastFrost  = crop.area?.location?.last_frost_spring || null;
  let rawOffsetWeeks = 0;
  if (locationLastFrost && /^\d{4}-\d{2}-\d{2}$/.test(locationLastFrost)) {
    const baseline = new Date("2000-04-15");
    const actual   = new Date("2000-" + locationLastFrost.slice(5));
    rawOffsetWeeks = Math.round((actual - baseline) / (7 * 86400000));
    rawOffsetWeeks = Math.max(-4, Math.min(8, rawOffsetWeeks)); // cap: -4 to +8 weeks
  }

  // Apply offset based on sensitivity band
  let frostOffsetWeeks = 0;
  if (sensitivityBand === "tender") {
    frostOffsetWeeks = rawOffsetWeeks;                            // full offset
  } else if (sensitivityBand === "semi_hardy") {
    frostOffsetWeeks = Math.max(-2, Math.min(2, Math.round(rawOffsetWeeks / 2))); // half, capped ±2 weeks
  }
  // hardy, overwintering, perennial → frostOffsetWeeks stays 0

  // Indoor sowing gets a smaller offset than outdoor — plants are protected from frost indoors.
  // Starting tomatoes indoors in Helsinki in Feb is fine; it's the outdoor transplant that's frost-critical.
  // Apply half the offset to sow windows for indoor crops, full offset to transplant windows.
  // Semi-hardy already gets half offset — don't halve again for indoor.
  const isIndoorSow = sowMethod === "indoors";
  const sowOffsetWeeks = (isIndoorSow && sensitivityBand === "tender")
    ? Math.round(frostOffsetWeeks / 2)   // tender indoor: half offset
    : frostOffsetWeeks;                  // all others: full band offset

  // Adjust sowing/transplant windows — potatoes left unshifted (overridden by variety type)
  const adjSowStart = sowOffsetWeeks !== 0 ? shiftMonth(sowStart, sowOffsetWeeks) : sowStart;
  const adjSowEnd   = sowOffsetWeeks !== 0 ? shiftMonth(sowEnd,   sowOffsetWeeks) : sowEnd;
  const adjTxStart  = frostOffsetWeeks !== 0 ? shiftMonth(txStart, frostOffsetWeeks) : txStart;
  const adjTxEnd    = frostOffsetWeeks !== 0 ? shiftMonth(txEnd,   frostOffsetWeeks) : txEnd;

  // Autumn frost gating — suppress tasks if the growing season is too short
  // Extracts month from stored "2000-MM-DD" format
  const locationFirstAutumnFrost = crop.area?.location?.first_frost_autumn || null;
  const autumnFrostMonth = (locationFirstAutumnFrost && /^\d{4}-\d{2}-\d{2}$/.test(locationFirstAutumnFrost))
    ? parseInt(locationFirstAutumnFrost.slice(5, 7), 10)
    : null; // null = unknown, no gating applied

  // Climate adjustment metadata — exposed for debugging and future user-facing explainability
  const climateAdjustment = frostOffsetWeeks !== 0 ? {
    baseline_last_frost: "04-15",
    local_last_frost:    locationLastFrost ? locationLastFrost.slice(5) : null,
    offset_weeks:        frostOffsetWeeks,
    sensitivity_band:    sensitivityBand,
  } : null;

  // Date anchors
  // If the user has manually confirmed a stage, we use stage_adjusted_sow_date
  // as the effective sow anchor for harvest date and pct calculations.
  // The original sown_date is never modified — null out stage_adjusted_sow_date to revert.
  const rawSowDate      = crop.sown_date         || crop.transplanted_date || null;
  const offsetDays      = crop.timeline_offset_days || 0;
  const adjustedSowDate = rawSowDate && offsetDays !== 0
    ? (() => { const d = new Date(rawSowDate); d.setDate(d.getDate() + offsetDays); return d.toISOString().split("T")[0]; })()
    : null;
  const sowDate         = adjustedSowDate || crop.stage_adjusted_sow_date || rawSowDate;
  const transplantDate  = crop.transplanted_date  || crop.transplant_date  || null;
  const plantedOutDate  = crop.planted_out_date   || null;
  const lastFedAt       = crop.last_fed_at        || null;
  // Tiered watering: use most specific available timestamp (crop → area → location)
  const lastWateredAt   = crop.last_watered_at
    || crop.area?.last_watered_at
    || crop.area?.location?.last_watered_at
    || null;

  // Stage
  const daysSown = daysSince(sowDate);
  const pctGrown = daysSown !== null && dtm > 0 ? daysSown / dtm : null;

  // If stage_confidence is confirmed, respect the stored stage rather than re-inferring
  let inferredStage = crop.stage || "seed";
  const vegEstablishments = ["runner","tuber","crown","cane"];
  if (crop.stage_confidence !== "confirmed" && !vegEstablishments.includes(def.default_establishment) && sowDate) {
    // Infer from DTM percentage
    if      (pctGrown === null)    inferredStage = "seed";
    else if (pctGrown < 0.08)     inferredStage = "seed";
    else if (pctGrown < 0.25)     inferredStage = "seedling";
    else if (pctGrown < 0.55)     inferredStage = "vegetative";
    else if (pctGrown < 0.70)     inferredStage = "flowering";
    else if (pctGrown < 0.90)     inferredStage = "fruiting";
    else if (pctGrown < 1.10)     inferredStage = "harvesting";
    else                           inferredStage = "finished";
  }

  // Stage boundary dates — when does each stage start?
  const stageBoundaries = {};
  if (sowDate && dtm) {
    for (const [stage, pct] of Object.entries(STAGE_DTM_PERCENT)) {
      stageBoundaries[stage] = addDays(sowDate, Math.round(dtm * pct));
    }
  }

  // Estimated harvest date
  const estimatedHarvestDate = sowDate && dtm_min
    ? addDays(sowDate, dtm_min)
    : (harvestStart ? monthToDate(harvestStart) : null);

  // Feed next due
  // If the calculated due date is in the past and the task was never completed
  // (no lastFedAt), advance the anchor to today so we don't pile up overdue tasks.
  // This handles the "missed task" case — next occurrence reschedules from now.
  let feedNextDue = null;
  if (feedInterval) {
    const anchor = lastFedAt || transplantDate || sowDate;
    if (anchor) {
      const rawNextDue = addDays(anchor, feedInterval);
      const today = todayISO();
      // If overdue and never been fed (lastFedAt is null), reschedule from today
      // so the task appears as "due now" rather than stacking up past-dated tasks
      feedNextDue = (!lastFedAt && rawNextDue < today) ? today : rawNextDue;
    }
  }

  // Environment
  const areaType = crop.area?.type || null;
  const isProtected = envMods?.frost_protection?.protected || false;

  // Weather summary
  const frostRisk           = weather?.frost_risk === true;
  const frostRisk7day       = weather?.frost_risk_7day ?? null;
  const tempC               = weather?.temp_c ?? null;
  const rainMm              = weather?.rain_mm ?? null;              // next 24h forecast (mm)
  const rainMmForecast5day  = weather?.rain_mm_forecast_5day ?? null; // next 5 days forecast (mm)
  // rainMm7dayActual — passed in from _loadRainHistory (sum of hourly cache writes over 7 days)
  // null until weather_history has accumulated data (first 7 days after deploy)

  // Soil pH — valid for 365 days. Only active if user has ever logged a reading.
  // null field = never logged = no effect on scoring (falls back to normal logic).
  const PH_VALIDITY_DAYS = 365;
  const rawPh       = crop.area?.soil_ph           ?? null;
  const phLoggedAt  = crop.area?.soil_ph_logged_at ?? null;
  const phAgeDays   = phLoggedAt ? daysSince(phLoggedAt) : null;
  const ph          = (rawPh !== null && phAgeDays !== null && phAgeDays <= PH_VALIDITY_DAYS) ? rawPh : null;
  const phMin       = def.soil_ph_min ?? null;
  const phMax       = def.soil_ph_max ?? null;

  // Soil temperature — valid for 14 days. Only active if user has ever logged a reading.
  const SOIL_TEMP_VALIDITY_DAYS = 14;
  const rawSoilTemp      = crop.area?.soil_temperature_c           ?? null;
  const soilTempLoggedAt = crop.area?.soil_temperature_logged_at   ?? null;
  const soilTempAgeDays  = soilTempLoggedAt ? daysSince(soilTempLoggedAt) : null;
  const soilTemp         = (rawSoilTemp !== null && soilTempAgeDays !== null && soilTempAgeDays <= SOIL_TEMP_VALIDITY_DAYS) ? rawSoilTemp : null;
  const soilTempMin      = def.soil_temp_min_c ?? null;

  // Feed matching
  const matchedFeed = feedType ? matchFeed(feedType, userFeeds) : null;

  // Recent observation signals
  const recentPestObs      = observations.filter(o => o.observation_type === "pest" && !o.resolved_at);
  const recentDiseaseObs   = observations.filter(o => o.observation_type === "disease" && !o.resolved_at);
  const isStruggling       = observations.some(o => o.symptom_code === "plant_struggling" && !o.resolved_at);
  const hasConfirmedStage  = observations.some(o => o.symptom_code?.includes("_confirmed"));
  const lastHarvestObs     = observations.find(o => o.symptom_code === "harvest_started");

  return {
    // Identity
    cropId:     crop.id,
    userId:     crop.user_id,
    areaId:     crop.area_id,
    cropName:   crop.name,
    variety:    crop.variety_name || variety.name || null,
    cropGroup,
    cropStatus: crop.status || "growing",

    // Definition
    def, variety, dtm, dtm_min, dtm_max,
    sowMethod, feedType, feedInterval,
    frostSensitive, isPerennial, isProtected,

    // Windows — adjusted for local frost date on frost-sensitive crops
    sowStart: adjSowStart, sowEnd: adjSowEnd,
    txStart:  adjTxStart,  txEnd:  adjTxEnd,
    harvestStart, harvestEnd,
    frostOffsetWeeks, sensitivityBand, // exposed for debugging
    autumnFrostMonth, // month of first autumn frost — used to suppress late-season tasks
    climateAdjustment, // null for UK users, populated for international users

    // Date anchors
    sowDate, transplantDate, plantedOutDate, lastFedAt,
    feedNextDue, estimatedHarvestDate,

    // Stage
    stage: inferredStage, stageBoundaries,
    daysSown, pctGrown,

    // Activity timestamps from area
    lastPrunedOrMulchedAt: crop.area?.last_pruned_or_mulched_at || null,

    // Weather
    frostRisk, frostRisk7day, tempC, rainMm, lastWateredAt,
    rainMmForecast5day,   // next 5 days forecast total (mm) — null if cache miss
    rainMm7dayActual,     // last 7 days actual total (mm) — null until history accumulates
    areaType,

    // Feed
    matchedFeed,

    // Soil state (freshness-gated — null if never logged or reading has expired)
    ph, phMin, phMax,
    soilTemp, soilTempMin,

    // Potato type
    potatoType: variety.potato_type || null,

    // Lifecycle mode — seasonal | established | overwintered
    // Defaults to seasonal for all existing crops (DB column defaults to 'seasonal')
    lifecycleMode: crop.lifecycle_mode || "seasonal",

    // Observations
    observations,
    recentPestObs,
    recentDiseaseObs,
    isStruggling,
    hasConfirmedStage,
    lastHarvestObs,
  };
}

// ── Feed matcher ──────────────────────────────────────────────────────────────

function matchFeed(cropFeedType, userFeeds) {
  if (!cropFeedType || !userFeeds?.length) return null;

  // Suppress crops that explicitly need no feed
  const k = cropFeedType.toLowerCase();
  if (k.startsWith("none")) return null;

  const scored = userFeeds.map(feed => {
    let score = 0;
    const ft = (feed.feed_type || "").toLowerCase();

    // ── Exact / specialist matches (highest priority) ──────────────────────
    if (ft.includes("specialist_ericaceous") && k.includes("ericaceous"))         score += 20;
    if (ft.includes("ericaceous")            && k.includes("ericaceous"))         score += 15;
    if (ft.includes("specialist_tomato")     && k.includes("potash"))             score += 18;
    if (ft.includes("specialist_tomato")     && k.includes("tomato"))             score += 20;
    if (ft.includes("specialist_rose")       && k.includes("rose"))               score += 20;
    if (ft.includes("specialist_citrus")     && k.includes("citrus"))             score += 20;
    if (ft.includes("citrus")               && k.includes("citrus"))              score += 15;

    // ── Potash matching ────────────────────────────────────────────────────
    if (ft.includes("high_potash") && k.includes("potash"))                       score += 15;
    if (ft.includes("high_potash") && k.includes("fruit"))                        score += 10;
    if (ft.includes("specialist_tomato") && k.includes("fruit"))                  score += 8;

    // ── Nitrogen matching ──────────────────────────────────────────────────
    if (ft.includes("high_nitrogen") && (k.includes("nitrogen") || k.includes("nitrogen-rich"))) score += 15;
    if (ft.includes("organic_general") && k.includes("nitrogen"))                 score += 8;

    // ── Low nitrogen ───────────────────────────────────────────────────────
    if (ft.includes("low_nitrogen") && k.includes("low nitrogen"))                score += 20;
    if (ft.includes("balanced")     && k.includes("low nitrogen"))                score += 8;

    // ── Balanced / general matching ────────────────────────────────────────
    if (ft.includes("balanced") && (k.includes("balanced") || k.includes("general") || k.includes("general purpose"))) score += 10;
    if (ft.includes("organic_general") && (k.includes("balanced") || k.includes("general"))) score += 7;

    // ── Fruit tree / fruit specific ────────────────────────────────────────
    if (ft.includes("balanced") && k.includes("fruit"))                           score += 6;

    // ── Seaweed — broad spectrum, low priority ─────────────────────────────
    if (ft.includes("seaweed"))                                                    score += 2;

    return { feed, score };
  }).filter(s => s.score > 0).sort((a, b) => b.score - a.score);

  return scored[0]?.feed || null;
}

function formatFeedAction(cropName, matchedFeed, feedType, prefix) {
  if (matchedFeed) {
    const label = [matchedFeed.brand, matchedFeed.product_name].filter(Boolean).join(" ");
    const dosage = matchedFeed.form === "liquid" && matchedFeed.dilution_ml_per_litre
      ? ` at ${matchedFeed.dilution_ml_per_litre}ml per litre of water`
      : matchedFeed.form === "granular" ? " — follow pack instructions" : "";
    return `${prefix} ${cropName} with ${label}${dosage}`;
  }
  return `${prefix} ${cropName} — apply ${feedType || "a balanced feed"}. Add your feed to the feeds section for personalised reminders`;
}

// ── Candidate builder ─────────────────────────────────────────────────────────

function candidate(ctx, opts) {
  const {
    ruleId, taskType, title, description,
    scheduledFor, urgency = "medium",
    engineType = "scheduled", recordType = "task",
    expiryDays = 14, leadTimeDays = null,
    meta = {}, riskPayload = null,
    dedupeByName = false, // if true, all instances of same crop share one task
  } = opts;

  const lead    = leadTimeDays ?? LEAD_TIME_DAYS[taskType] ?? LEAD_TIME_DAYS.default;
  const visFrom = scheduledFor ? addDays(scheduledFor, -lead) : todayISO();
  const expiry  = scheduledFor ? addDays(scheduledFor, expiryDays) : addDays(todayISO(), expiryDays);

  // Status: upcoming if scheduled in future, due if today or past
  const today   = todayISO();
  const status  = scheduledFor && scheduledFor > today ? "upcoming" : "due";

  // For perennial care tasks, dedupe by crop name so multiple instances share one task
  const cropKey = dedupeByName
    ? ctx.cropName.toLowerCase().replace(/\s+/g, "_")
    : ctx.cropId;

  // Use a window-stable date anchor in the key so tasks that fire
  // within a broad window (mulch, prune, monitor) don't regenerate
  // every day with a new key after being completed.
  // Feed and precise-date tasks use exact date; window tasks snap to month/week.
  const keyDate = windowAnchor(scheduledFor || today, expiryDays);
  const key = sourceKey({
    u: ctx.userId,
    c: cropKey,
    r: ruleId,
    d: keyDate,
  });

  return {
    user_id:          ctx.userId,
    crop_instance_id: ctx.cropId,
    area_id:          ctx.areaId,
    action:           title,        // keep 'action' for API compatibility
    task_type:        taskType,
    urgency,
    due_date:         scheduledFor || today,
    scheduled_for:    scheduledFor || today,
    visible_from:     visFrom < today ? today : visFrom,
    expires_at:       new Date(expiry + "T23:59:59Z").toISOString(),
    status,
    engine_type:      engineType,
    record_type:      recordType,
    source:           "rule_engine",
    rule_id:          ruleId,
    source_key:       key,
    date_confidence:  scheduledFor ? "exact" : "approximate",
    meta:             JSON.stringify(meta),
    risk_payload:     riskPayload ? JSON.stringify(riskPayload) : null,
    // description stored in meta for now (tasks table has no body field yet)
    _description:     description, // stripped before DB insert
    _status:          status,
  };
}

// ── Confidence scoring ───────────────────────────────────────────────────────
// Scores each candidate 0–100 based on available signals.
// Missing optional data (soil temp, pH) is NEUTRAL — does not penalise.
// Only present data can add or subtract. This ensures data-poor users still
// get tasks while advanced users get more precise outputs.
//
// Thresholds:
//   >= 30 → task (surface_class = 'task')
//   <  30 → suppress (unless new-user fallback applies)
//
// NOTE: The 30–49 "insight" band is collapsed into task for now.
// There is no current UI surface for insights — tasks must show.
// Insight infrastructure is preserved for future use.
// Missing enrichment data should reduce precision, not remove guidance.

function scoreCandidate(ctx, candidate) {
  let score = 0;

  // ── Base signals (always available) ────────────────────────────────────────

  // Calendar window open — strongest base signal
  // Inferred from task being generated within an open window
  // We use rule_id to detect window-based rules
  const windowRules = ["sow_prompt", "transplant_prompt", "harden_off", "perennial_harvest", "perennial_spring_feed"];
  if (windowRules.includes(candidate.rule_id)) score += 20;

  // ── Raised base scores for core actionable rules ─────────────────────────
  // These rules should surface naturally without needing rich metadata.
  // Absence of enrichment data (soil temp, pH, stage) should not suppress them.
  // Base score chosen so one moderate signal (frost safe) pushes them to task level.
  const raisedBaseRules = {
    feed_scheduled:        25, // feeding is always actionable when due
    harden_off:            20, // +20 already from windowRules — nets to 40 total
    perennial_harvest:      5, // +20 already from windowRules — nets to 25, needs one signal
    perennial_spring_feed:  5, // +20 already from windowRules — nets to 25, needs one signal
    perennial_summer_feed: 25, // not in windowRules — needs base
  };
  if (raisedBaseRules[candidate.rule_id] !== undefined) {
    score += raisedBaseRules[candidate.rule_id];
  }

  // Frost safe — positive signal
  if (ctx.frostRisk7day !== null && ctx.frostRisk7day > 0) score += 20;

  // Hard frost — strong negative signal
  if (ctx.frostRisk7day !== null && ctx.frostRisk7day <= 0) score -= 30;

  // Heavy rain — mild negative signal
  if (ctx.rainMm !== null && ctx.rainMm >= 5) score -= 10;

  // ── Stage confidence (common but optional) ─────────────────────────────────
  if (ctx.stage_confidence === "confirmed") score += 20;

  // ── Observation signals (high value when present, neutral if absent) ────────
  if (ctx.recentPestObs?.length > 0) score += 15;

  // Struggling suppresses generic tasks — reduce score significantly
  if (ctx.isStruggling) score -= 20;

  // ── Soil temperature (neutral if not available) ────────────────────────────
  // ctx.soilTemp and ctx.soilTempMin populated when user has provided soil data
  // and crop definition has a minimum threshold. Currently unused — wired for future.
  if (ctx.soilTemp !== null && ctx.soilTemp !== undefined &&
      ctx.soilTempMin !== null && ctx.soilTempMin !== undefined) {
    if (ctx.soilTemp >= ctx.soilTempMin) score += 25;
    else score -= 20;
  }
  // else → no effect (neutral for users without soil temp data)

  // ── pH (neutral if not available) ─────────────────────────────────────────
  if (ctx.ph !== null && ctx.ph !== undefined &&
      ctx.phMin !== null && ctx.phMin !== undefined &&
      ctx.phMax !== null && ctx.phMax !== undefined) {
    if (ctx.ph >= ctx.phMin && ctx.ph <= ctx.phMax) score += 10;
    else score -= 10;
  }
  // else → no effect

  // ── Bypass — urgent, real-time, consequence-heavy rules always surface ───────
  // These are already gated by their own risk/weather logic — don't double-suppress.
  // Keep this list tight: only rules where suppression would cause real user harm.
  // sow_prompt and transplant_prompt are calendar-gated — if generated they're valid
  // and must surface as tasks regardless of confidence score.
  const bypassRules = [
    "pest_slugs_snails",      // dynamic pest risk — already gated by weather/season
    "pest_flea_beetle",       // dynamic pest risk
    "frost_alert",            // real-time weather alert — always urgent
    "watering_due",           // area-level, already gated by rain/dry logic
    "struggling_flag",        // user has flagged a problem — must surface
    "sow_prompt",             // calendar-gated sowing window — always actionable
    "transplant_prompt",      // calendar-gated transplant window — always actionable
    "perennial_spring_feed",  // calendar-gated March/April — always actionable in season
    "perennial_summer_feed",  // calendar-gated June/July — always actionable in season
    "feed_scheduled",         // interval-based — only fires when genuinely due
  ];
  if (bypassRules.includes(candidate.rule_id)) score = Math.max(score, 55);

  return Math.max(0, score);
}

// ── Lifecycle mode helpers ─────────────────────────────────────────────────────
// These are the single source of truth for routing engine behaviour by mode.
// Always use these helpers — do not scatter raw lifecycleMode checks everywhere.

function isSetupSuppressed(ctx) {
  // Established and overwintered crops should never receive sow, transplant,
  // harden-off, or any early-lifecycle setup tasks.
  return ctx.lifecycleMode === "established" || ctx.lifecycleMode === "overwintered";
}

function isSeasonalCrop(ctx) {
  return ctx.lifecycleMode === "seasonal" || !ctx.lifecycleMode;
}

// ── SCHEDULED RULE ENGINE ─────────────────────────────────────────────────────

class ScheduledRuleEngine {

  evaluate(ctx, rules) {
    const candidates = [];
    const status = ctx.cropStatus;

    // Finished crops — nothing to do
    if (ctx.stage === "finished") return candidates;

    // Struggling crops — suppress non-urgent scheduled tasks
    // (engine will still fire alerts and checks)
    if (ctx.isStruggling) {
      candidates.push(candidate(ctx, {
        ruleId:       "struggling_flag",
        taskType:     "check",
        title:        `Check on ${ctx.cropName} — this plant was flagged as struggling. Inspect roots, leaves and growing conditions`,
        scheduledFor: todayISO(),
        urgency:      "high",
        expiryDays:   7,
        leadTimeDays: 0,
        meta:         { lifecycle_check: true, stage: "struggling" },
      }));
      return candidates; // don't pile on more tasks while struggling
    }

    // ── PERENNIALS ───────────────────────────────────────────────────────────
    if (ctx.isPerennial) {
      candidates.push(...this._evalPerennial(ctx));
      return candidates;
    }

    // ── ESTABLISHED / OVERWINTERED ────────────────────────────────────────────
    // These crops skip all setup paths (planned, indoor, sow, transplant,
    // harden-off). Route directly to growing logic which handles feeding,
    // harvest, pest alerts, and seasonal care.
    if (isSetupSuppressed(ctx)) {
      candidates.push(...this._evalGrowing(ctx, rules));
      return candidates;
    }

    // ── PLANNED ─────────────────────────────────────────────────────────────
    if (status === "planned") {
      candidates.push(...this._evalPlanned(ctx));
      return candidates;
    }

    // ── SOWN INDOORS ────────────────────────────────────────────────────────
    if (status === "sown_indoors") {
      candidates.push(...this._evalIndoor(ctx));
      return candidates;
    }

    // ── GROWING / TRANSPLANTED / SOWN OUTDOORS ───────────────────────────────
    candidates.push(...this._evalGrowing(ctx, rules));

    return candidates;
  }

  // ── Perennial evaluation ─────────────────────────────────────────────────

  _evalPerennial(ctx) {
    const results = [];
    const today = todayISO();
    const m = currentMonth();
    const year = new Date().getFullYear();

    // Harvest window — recurring every 14 days
    const hs = ctx.harvestStart, he = ctx.harvestEnd;
    if (hs && he && m >= hs && m <= he) {
      // Next harvest check = today (it's within window)
      results.push(candidate(ctx, {
        ruleId:       "perennial_harvest",
        dedupeByName: true,
        taskType:     "harvest",
        title:        `${ctx.cropName} should be ready to harvest — check for ripeness and pick regularly to encourage more fruit`,
        description:  `You're in the harvest window for ${ctx.cropName}. Regular picking encourages further fruiting.`,
        scheduledFor: today,
        urgency:      "medium",
        expiryDays:   14,
        leadTimeDays: 0,
      }));
    }

    // Feed scheduling — interval-based when feed_interval_days is set,
    // otherwise fall back to one-shot seasonal prompts (spring + summer)
    if (ctx.feedType) {
      if (ctx.feedInterval && ctx.feedNextDue) {
        // Interval-based: behaves like _evalGrowing feed scheduling
        const feedDue = ctx.feedNextDue;
        if (withinLookahead(feedDue, LOOKAHEAD_DAYS.feed) || isOverdue(feedDue)) {
          const scheduledFor = isOverdue(feedDue) ? today : feedDue;
          const feedUrgency = isOverdue(feedDue) ? "high"
            : ctx.stage === "fruiting" ? "high"
            : "medium";
          results.push(candidate(ctx, {
            ruleId:       "perennial_feed_scheduled",
            dedupeByName: true,
            taskType:     "feed",
            title:        formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Time to feed"),
            description:  `Regular feeding is due for ${ctx.cropName}.`,
            scheduledFor,
            urgency:      feedUrgency,
            expiryDays:   7,
            leadTimeDays: LEAD_TIME_DAYS.feed,
          }));
        }
      } else {
        // Fallback: no feed_interval_days set — use one-shot seasonal prompts
        const springFeedDate = `${new Date().getFullYear()}-03-15`;
        if (withinLookahead(springFeedDate, LOOKAHEAD_DAYS.seasonal) || (m >= 3 && m <= 4)) {
          const due = m >= 3 && m <= 4 ? today : springFeedDate;
          results.push(candidate(ctx, {
            ruleId:       "perennial_spring_feed",
            dedupeByName: true,
            taskType:     "feed",
            title:        formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Feed"),
            description:  `Spring feeding supports new growth on ${ctx.cropName}.`,
            scheduledFor: due,
            urgency:      "low",
            expiryDays:   21,
            leadTimeDays: 7,
          }));
        }

        // Summer feed — June/July
        const summerFeedDate = `${new Date().getFullYear()}-06-15`;
        if (withinLookahead(summerFeedDate, LOOKAHEAD_DAYS.seasonal) || (m >= 6 && m <= 7)) {
          const due = m >= 6 && m <= 7 ? today : summerFeedDate;
          results.push(candidate(ctx, {
            ruleId:       "perennial_summer_feed",
            dedupeByName: true,
            taskType:     "feed",
            title:        formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Feed"),
            description:  `Summer feeding supports fruiting on ${ctx.cropName}.`,
            scheduledFor: due,
            urgency:      "low",
            expiryDays:   21,
            leadTimeDays: 7,
          }));
        }
      }
    }

    // ── Lifecycle check prompts — adds to QuickCropCheck via stage field ────────
    // These generate a check task that the frontend surfaces as a crop check
    const perennialLifecycle = this._perennialLifecycle(ctx, m, year, today);
    results.push(...perennialLifecycle);

    // ── Seasonal care tasks ───────────────────────────────────────────────────
    const perennialCare = this._perennialSeasonalCare(ctx, m, year, today);
    results.push(...perennialCare);

    return results;
  }

  // ── Perennial lifecycle checks ───────────────────────────────────────────
  // Generates stage-appropriate check prompts for perennial crops
  // These surface in QuickCropCheck on the dashboard

  _perennialLifecycle(ctx, m, year, today) {
    const results = [];
    const name = ctx.cropName.toLowerCase();

    // Flowering check — prompt when crop should be flowering
    // Different crops flower at different times
    const floweringWindows = {
      strawberry:   { start: 4, end: 5 },
      apple:        { start: 4, end: 5 },
      pear:         { start: 4, end: 5 },
      blueberry:    { start: 4, end: 5 },
      blackcurrant: { start: 4, end: 5 },
      redcurrant:   { start: 4, end: 5 },
      gooseberry:   { start: 4, end: 5 },
      raspberry:    { start: 5, end: 6 },
      blackberry:   { start: 6, end: 7 },
      rhubarb:      { start: 3, end: 4 },
      asparagus:    { start: 4, end: 5 },
    };

    // Fruit set check — when fruit should be forming
    const fruitSetWindows = {
      strawberry:   { start: 5, end: 6 },
      apple:        { start: 6, end: 7 },
      pear:         { start: 6, end: 7 },
      blueberry:    { start: 6, end: 7 },
      blackcurrant: { start: 6, end: 7 },
      redcurrant:   { start: 6, end: 7 },
      gooseberry:   { start: 6, end: 7 },
      raspberry:    { start: 6, end: 8 },
      blackberry:   { start: 7, end: 9 },
    };

    // Match crop to lifecycle windows
    const cropKey = Object.keys(floweringWindows).find(k => name.includes(k));
    if (!cropKey) return results;

    const fw = floweringWindows[cropKey];
    const fsw = fruitSetWindows[cropKey];

    // Flowering check
    if (fw && m >= fw.start && m <= fw.end) {
      results.push(candidate(ctx, {
        ruleId:       "perennial_flowering_check",
        dedupeByName: true,
        taskType:     "check",
        title:        `Are your ${ctx.cropName} flowering yet? Check for open blossoms and ensure pollinators can access them`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   14,
        leadTimeDays: 0,
        meta:         { lifecycle_check: true, stage: "flowering" },
      }));
    }

    // Fruit set check
    if (fsw && m >= fsw.start && m <= fsw.end) {
      results.push(candidate(ctx, {
        ruleId:       "perennial_fruit_set_check",
        dedupeByName: true,
        taskType:     "check",
        title:        `Check your ${ctx.cropName} — has fruit started to set after flowering? Look for small fruit forming`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   14,
        leadTimeDays: 0,
        meta:         { lifecycle_check: true, stage: "fruiting" },
      }));
    }



    return results;
  }

  // ── Perennial seasonal care ──────────────────────────────────────────────
  // Pruning, mulching, winter care tasks

  _perennialSeasonalCare(ctx, m, year, today) {
    const results = [];
    const name = ctx.cropName.toLowerCase();

    // Suppression — if user logged pruned/mulched recently, skip tasks whose
    // expiryDays window would not yet have elapsed.
    // last_pruned_or_mulched_at sits on the area; close enough for perennial care.
    const daysSincePrune = ctx.lastPrunedOrMulchedAt ? daysSince(ctx.lastPrunedOrMulchedAt) : null;

    // Strawberry runners — July/August
    if (name.includes("strawberry") && m >= 7 && m <= 8) {
      if (daysSincePrune === null || daysSincePrune >= 21) results.push(candidate(ctx, {
        ruleId:       "strawberry_runners",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Check ${ctx.cropName} for runners — pin down the strongest ones to propagate, remove the rest to keep the plant's energy in fruiting`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   21,
        leadTimeDays: 0,
      }));
    }

    // Strawberry renovation — August after harvest
    if (name.includes("strawberry") && m === 8) {
      if (daysSincePrune === null || daysSincePrune >= 21) results.push(candidate(ctx, {
        ruleId:       "strawberry_renovate",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Renovate ${ctx.cropName} after harvesting — cut back old foliage to 10cm, clear debris and feed to encourage new growth`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   21,
        leadTimeDays: 0,
      }));
    }

    // Raspberry cane management — August/September (summer fruiting)
    if (name.includes("raspberry") && m >= 8 && m <= 9) {
      if (daysSincePrune === null || daysSincePrune >= 28) results.push(candidate(ctx, {
        ruleId:       "raspberry_cane_prune",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Prune ${ctx.cropName} — cut out all canes that fruited this year to ground level, tie in the new green canes for next year`,
        scheduledFor: today,
        urgency:      "medium",
        expiryDays:   28,
        leadTimeDays: 0,
      }));
    }

    // Apple/pear summer prune — July/August for trained forms
    if ((name.includes("apple") || name.includes("pear")) && m >= 7 && m <= 8) {
      if (daysSincePrune === null || daysSincePrune >= 28) results.push(candidate(ctx, {
        ruleId:       "apple_pear_summer_prune",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Summer prune ${ctx.cropName} — cut back sideshoots to 3 leaves above the basal cluster to encourage fruiting spurs`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   28,
        leadTimeDays: 0,
      }));
    }

    // Apple/pear winter prune — December/January
    if ((name.includes("apple") || name.includes("pear")) && (m === 12 || m === 1)) {
      const pruneDate = m === 12 ? monthToDate(12, year, 15) : today;
      if (daysSincePrune === null || daysSincePrune >= 42) results.push(candidate(ctx, {
        ruleId:       "apple_pear_winter_prune",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Winter prune ${ctx.cropName} while dormant — remove crossing, dead, or diseased branches, open up the centre for airflow`,
        scheduledFor: pruneDate,
        urgency:      "low",
        expiryDays:   42,
        leadTimeDays: 7,
      }));
    }

    // Blueberry/currant winter prune — January/February
    if ((name.includes("blueberry") || name.includes("currant") || name.includes("gooseberry")) && (m === 1 || m === 2)) {
      if (daysSincePrune === null || daysSincePrune >= 42) results.push(candidate(ctx, {
        ruleId:       "berry_winter_prune",
        dedupeByName: true,
        taskType:     "prune",
        title:        `Prune ${ctx.cropName} while dormant — remove oldest darkest stems to ground level to encourage new productive growth`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   42,
        leadTimeDays: 0,
      }));
    }

    // Mulching — March for most perennials
    const mulchCrops = ["strawberry","raspberry","blueberry","blackcurrant","redcurrant","gooseberry","apple","pear","blackberry"];
    if (mulchCrops.some(k => name.includes(k)) && m >= 3 && m <= 4) {
      if (daysSincePrune === null || daysSincePrune >= 28) results.push(candidate(ctx, {
        ruleId:       "perennial_mulch",
        dedupeByName: true,
        taskType:     "mulch",
        title:        `Mulch around ${ctx.cropName} now — apply 5-8cm of well-rotted compost or bark to retain moisture and suppress weeds`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   28,
        leadTimeDays: 0,
      }));
    }

    // Rhubarb — forcing check February/March
    if (name.includes("rhubarb") && m >= 2 && m <= 3) {
      results.push(candidate(ctx, {
        ruleId:       "rhubarb_forcing_check",
        dedupeByName: true,
        taskType:     "check",
        title:        `Check ${ctx.cropName} — are stems emerging? You can force early growth by covering crowns with a forcing pot or bucket now`,
        scheduledFor: today,
        urgency:      "low",
        expiryDays:   21,
        leadTimeDays: 0,
        meta:         { lifecycle_check: true, stage: "vegetative" },
      }));
    }

    // Asparagus — cutting season check April/May/June
    if (name.includes("asparagus") && m >= 4 && m <= 6) {
      results.push(candidate(ctx, {
        ruleId:       "asparagus_cutting_check",
        dedupeByName: true,
        taskType:     "harvest",
        title:        `Check ${ctx.cropName} — spears ready to cut when 15-20cm tall. Stop cutting after mid-June to let plants build strength for next year`,
        scheduledFor: today,
        urgency:      "medium",
        expiryDays:   14,
        leadTimeDays: 0,
        meta:         { lifecycle_check: true, stage: "harvesting" },
      }));
    }

    return results;
  }

  // ── Planned crop evaluation ──────────────────────────────────────────────

  _evalPlanned(ctx) {
    const results = [];
    let { sowStart, sowEnd, sowMethod, potatoType, frostSensitive } = ctx;

    // Potato type offset
    if (sowMethod === "tuber" && potatoType) {
      if (potatoType === "first_early")  { sowStart = 3; sowEnd = 4; }
      if (potatoType === "second_early") { sowStart = 3; sowEnd = 5; }
      if (potatoType === "maincrop")     { sowStart = 4; sowEnd = 5; }
    }

    if (!sowStart || !sowEnd) return results;

    // Calculate sow window start date
    const year = new Date().getFullYear();
    const sowWindowStart = monthToDate(sowStart, year, 1);
    const sowWindowEnd   = monthToDate(sowEnd,   year, 28);
    const today          = todayISO();
    const m              = currentMonth();

    // Autumn frost gating — suppress sow task once we are within ~1 month of first autumn frost.
    // Uses current month (m), not window end, so early-window tasks are not wrongly suppressed.
    // Example: first frost Sept → suppress from August onwards for frost-sensitive crops.
    if (ctx.autumnFrostMonth && frostSensitive && m >= ctx.autumnFrostMonth - 1) {
      return results; // too close to first autumn frost — suppress task
    }

    // Only fire sow prompt when the window is actually open — upcoming low-urgency
    // prompts generated too much noise (2% completion rate) so removed.
    const windowOpenNow = m >= sowStart && m <= sowEnd;
    if (!windowOpenNow) return results;

    const scheduledFor = today;

    // Frost suppression for outdoor sowing
    const isOutdoor = sowMethod === "outdoors" || sowMethod === "direct_sow" ||
                      sowMethod === "either" || sowMethod === "tuber";
    const hardFrost = frostSensitive && isOutdoor && ctx.frostRisk7day !== null && ctx.frostRisk7day <= 0;
    const softFrost = frostSensitive && isOutdoor && ctx.frostRisk7day !== null && ctx.frostRisk7day > 0 && ctx.frostRisk7day <= 3;

    // Hard block — don't show task when hard frost is forecast
    if (hardFrost) return results;

    let title, urgency, meta;
    const windowStr = `${MONTHS[sowStart-1]}–${MONTHS[sowEnd-1]}`;

    if (sowMethod === "indoors") {
      title   = `Sow ${ctx.cropName} indoors now — starting indoors gives stronger plants and an earlier harvest. Sowing window: ${windowStr}`;
      urgency = "medium";
      meta    = { status_transition: "sown_indoors", sow_method: "indoors", can_prefer_outdoors: true };
    } else if (sowMethod === "tuber") {
      title   = softFrost
        ? `Almost time to plant out ${ctx.cropName} — frost risk still present, wait for a settled spell. Window: ${windowStr}`
        : `Time to plant out ${ctx.cropName} — chit them now if not already started. Window: ${windowStr}`;
      urgency = softFrost ? "low" : "medium";
      meta    = { status_transition: "sown", sow_method: "tuber" };
    } else if (sowMethod === "crown") {
      title   = `Plant out ${ctx.cropName} crowns — do this while dormant for best establishment. Window: ${windowStr}`;
      urgency = "medium";
      meta    = { status_transition: "sown", sow_method: "crown" };
    } else {
      // Direct / either
      title   = softFrost
        ? `Almost time to direct sow ${ctx.cropName} outdoors — frost risk still marginal, wait for a settled spell. Window: ${windowStr}`
        : `Time to direct sow ${ctx.cropName} outdoors. Window: ${windowStr}`;
      urgency = softFrost ? "low" : "medium";
      meta    = { status_transition: "sown", sow_method: "outdoors" };
    }

    // Append climate adjustment to meta if applicable
    if (ctx.climateAdjustment) meta = { ...meta, climate_adjustment: ctx.climateAdjustment };

    results.push(candidate(ctx, {
      ruleId:       "sow_prompt",
      taskType:     "sow",
      title,
      scheduledFor,
      urgency,
      expiryDays:   daysBetween(scheduledFor, sowWindowEnd) + 3,
      leadTimeDays: LEAD_TIME_DAYS.sow,
      meta,
    }));

    return results;
  }

  // ── Indoor seedling evaluation ────────────────────────────────────────────

  _evalIndoor(ctx) {
    const results = [];
    const { txStart, txEnd, frostSensitive, cropName } = ctx;
    if (!txStart || !txEnd) return results;

    // Greenhouse and polytunnel crops stay under cover — suppress outdoor
    // transplant and harden-off tasks entirely. They may still need
    // potting-on or spacing, but that is handled by growing rules.
    if (ctx.areaType === "greenhouse" || ctx.areaType === "polytunnel" || ctx.areaType === "indoors") return results;

    // Tuber and crown crops are planted directly — not transplanted from indoor seedlings.
    // They are handled by _evalSow where the tuber/crown path already exists.
    // Running them through _evalIndoor causes duplicate transplant prompts (e.g. Potatoes).
    if (ctx.sowMethod === "tuber" || ctx.sowMethod === "crown") return results;

    const year        = new Date().getFullYear();
    const txDate      = monthToDate(txStart, year, 1);
    const txEndDate   = monthToDate(txEnd,   year, 28);
    const today       = todayISO();
    const m           = currentMonth();

    // Only fire transplant prompt when window is actually open.
    // Upcoming low-urgency prompts had 1.4% completion — removed.
    // When frost is forecast, still show but as low urgency with advisory copy.
    const windowOpen = m >= txStart && m <= txEnd;
    if (!windowOpen) return results;

    // Suppress transplant prompt if sown fewer than 21 days ago — seedlings
    // cannot be ready to transplant within 3 weeks of sowing from seed.
    const daysSinceSown = ctx.sowDate ? daysBetween(ctx.sowDate, today) : 999;
    if (daysSinceSown < 21) return results;

    const scheduledFor = today;
    const frostRisk    = ctx.frostRisk;

    // If hard frost (below 0), suppress entirely — not safe to even consider transplanting
    const hardFrost7d = ctx.frostRisk7day !== null && ctx.frostRisk7day <= 0;
    if (hardFrost7d) return results;

    const title = frostRisk
      ? `${cropName} is ready to transplant but frost is forecast — harden off and wait for a clear spell`
      : `Time to transplant ${cropName} outdoors — harden off for a few days first if not already done`;

    const txMeta = ctx.climateAdjustment
      ? { status_transition: "transplanted", climate_adjustment: ctx.climateAdjustment }
      : { status_transition: "transplanted" };

    results.push(candidate(ctx, {
      ruleId:       "transplant_prompt",
      taskType:     "transplant",
      title,
      scheduledFor,
      urgency:      frostRisk ? "low" : "medium",
      expiryDays:   daysBetween(scheduledFor, txEndDate) + 3,
      leadTimeDays: LEAD_TIME_DAYS.transplant,
      meta:         txMeta,
    }));

    // Harden off — only show when transplant is imminent (within 5 days)
    // Previously fired 7 days before window open — 3.8% completion, too early and abstract.
    const daysUntilTxWindow = daysBetween(today, txDate);
    const hardenDate = addDays(txDate, -5);
    if (hardenDate >= today && daysUntilTxWindow <= 5) {
      results.push(candidate(ctx, {
        ruleId:       "harden_off",
        taskType:     "check",
        title:        `Start hardening off ${cropName} now — transplanting in a few days, put outside during the day and bring in at night`,
        scheduledFor: hardenDate,
        urgency:      "low",
        expiryDays:   5,
        leadTimeDays: 1,
      }));
    }

    return results;
  }

  // ── Growing crop rule evaluation ─────────────────────────────────────────

  _evalGrowing(ctx, rules) {
    const results = [];
    const today = todayISO();

    // ── Feed scheduling ──────────────────────────────────────────────────────
    if (ctx.feedNextDue && ctx.feedInterval) {
      const feedDue = ctx.feedNextDue;
      if (withinLookahead(feedDue, LOOKAHEAD_DAYS.feed) || isOverdue(feedDue)) {
        // If overdue, schedule for today — feedNextDue already accounts for missed
        // tasks by anchoring from today when never fed (see buildCropContext)
        const scheduledFor = isOverdue(feedDue) ? today : feedDue;
        // Escalate to high urgency during fruiting — skipping feeds at this stage
        // directly reduces yield, so it warrants stronger signalling.
        const feedUrgency = isOverdue(feedDue) ? "high"
          : ctx.stage === "fruiting" ? "high"
          : "medium";
        results.push(candidate(ctx, {
          ruleId:       "feed_scheduled",
          taskType:     "feed",
          title:        formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Time to feed"),
          description:  `Regular feeding is due for ${ctx.cropName}.`,
          scheduledFor,
          urgency:      feedUrgency,
          expiryDays:   7,
          leadTimeDays: LEAD_TIME_DAYS.feed,
        }));
      }
    }

    // ── Harvest scheduling ───────────────────────────────────────────────────
    if (ctx.estimatedHarvestDate) {
      // Show harvest task leading up to and during harvest
      const harvestAlertDate = addDays(ctx.estimatedHarvestDate, -LEAD_TIME_DAYS.harvest);
      const today = todayISO();

      // Sanity check: if crop has a defined harvest window, don't fire the task
      // more than 6 weeks before the window opens. This prevents DTM-based dates
      // triggering harvest tasks for crops like peppers sown indoors in Jan/Feb
      // whose DTM expires in March but real harvest isn't until July+.
      let harvestWindowSuppressed = false;
      if (ctx.harvestStart) {
        const currentMonth = new Date().getMonth() + 1;
        const weeksBeforeWindow = (ctx.harvestStart - currentMonth) * 4.3;
        if (weeksBeforeWindow > 6) harvestWindowSuppressed = true;
        // Also handle year wrap (e.g. harvest in Jan/Feb, currently Oct/Nov)
        if (ctx.harvestStart < currentMonth) {
          const weeksUntilNextYear = ((12 - currentMonth) + ctx.harvestStart) * 4.3;
          if (weeksUntilNextYear > 6) harvestWindowSuppressed = true;
          else harvestWindowSuppressed = false;
        }
      }

      // Tightened to 5-day window — previously fired up to 21 days ahead (9.2% completion).
      // Harvest prompts are only actionable when harvest is genuinely imminent.
      const harvestImminent = ctx.estimatedHarvestDate >= addDays(today, -3)
        && ctx.estimatedHarvestDate <= addDays(today, 5);

      if (!harvestWindowSuppressed && harvestImminent) {
        const scheduledFor = harvestAlertDate < today ? today : harvestAlertDate;
        results.push(candidate(ctx, {
          ruleId:       "harvest_approaching",
          taskType:     "harvest",
          title:        `${ctx.cropName} should be ready to harvest around ${ctx.estimatedHarvestDate} — check for ripeness`,
          description:  `Based on your sow date and variety, ${ctx.cropName} should be approaching harvest.`,
          scheduledFor,
          urgency:      "medium",
          expiryDays:   7,
          leadTimeDays: LEAD_TIME_DAYS.harvest,
          meta:         { estimated_harvest_date: ctx.estimatedHarvestDate },
        }));
      }
    }

    // ── Stage-based tasks from crop_rules table ──────────────────────────────
    for (const rule of rules) {
      // Crop match
      if (rule.crop_def_id && rule.crop_def_id !== ctx.def?.id) continue;
      // Area type match
      if (rule.area_type && rule.area_type !== ctx.areaType) continue;
      // Stage match
      if (rule.stage && rule.stage !== ctx.stage) continue;
      // Skip finished
      if (ctx.stage === "finished") continue;

      const scheduledFor = this._resolveRuleDate(ctx, rule, today);
      if (!scheduledFor) continue;

      // Is it within lookahead or overdue?
      const lookahead = LOOKAHEAD_DAYS[rule.task_type] || LOOKAHEAD_DAYS.default;
      if (!withinLookahead(scheduledFor, lookahead) && !isOverdue(scheduledFor)) continue;

      const due = isOverdue(scheduledFor) ? today : scheduledFor;

      // Feed personalisation
      let action = rule.action;
      if (rule.task_type === "feed") {
        action = formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Time to feed");
      }

      results.push(candidate(ctx, {
        ruleId:       rule.rule_id,
        taskType:     rule.task_type,
        title:        action,
        scheduledFor: due,
        urgency:      rule.urgency || "medium",
        expiryDays:   rule.cooldown_days || 7,
        leadTimeDays: LEAD_TIME_DAYS[rule.task_type] || LEAD_TIME_DAYS.default,
      }));
    }

    return results;
  }

  // Resolve what date a crop_rules rule should fire
  _resolveRuleDate(ctx, rule, today) {
    const { condition_type, condition_value: cv } = rule;

    switch (condition_type) {
      case "days_since_sow":
        if (!ctx.sowDate || !cv?.days) return null;
        return addDays(ctx.sowDate, cv.days);

      case "days_since_transplant":
        if (!ctx.transplantDate || !cv?.days) return null;
        return addDays(ctx.transplantDate, cv.days);

      case "days_since_feed":
        if (!ctx.feedInterval) return null;
        return ctx.feedNextDue || today;

      case "days_to_harvest":
        if (!ctx.estimatedHarvestDate || !cv?.days) return null;
        return addDays(ctx.estimatedHarvestDate, -(cv.days));

      case "stage_reached": {
        if (!cv?.stage || !ctx.sowDate) return null;
        const stagePct = STAGE_DTM_PERCENT[cv.stage];
        if (stagePct === undefined) return null;
        return addDays(ctx.sowDate, Math.round(ctx.dtm * stagePct));
      }

      case "month_window":
        if (!cv?.start || !cv?.end) return null;
        if (currentMonth() >= cv.start && currentMonth() <= cv.end) return today;
        // Upcoming?
        const windowDate = monthToDate(cv.start);
        return windowDate > today ? windowDate : null;

      default:
        return null; // unknown condition types fall through to reactive only
    }
  }
}

// ── DYNAMIC RISK ENGINE ───────────────────────────────────────────────────────

class DynamicRiskEngine {

  evaluate(ctx, pestRules) {
    const results = [];
    const today   = todayISO();
    const m       = currentMonth();

    // ── Frost alert (1–3 day horizon) ────────────────────────────────────────
    if (ctx.frostSensitive && !ctx.isProtected && ctx.areaType !== "indoors") {
      const min7 = ctx.frostRisk7day;
      if (min7 !== null && min7 <= 2) {
        const urgency = min7 <= 0 ? "high" : "medium";
        const title   = min7 <= 0
          ? `Frost forecast tonight — protect ${ctx.cropName} with fleece or bring containers inside`
          : `Frost risk this week — keep fleece handy for ${ctx.cropName}`;
        results.push(candidate(ctx, {
          ruleId:       "frost_alert",
          taskType:     "protect",
          title,
          scheduledFor: today,
          urgency,
          engineType:   "risk",
          recordType:   "alert",
          expiryDays:   2,
          leadTimeDays: 0,
        }));
      }
    }

    // ── Heat stress alert ─────────────────────────────────────────────────────
    if (ctx.tempC !== null && ctx.tempC >= 28 && ctx.areaType !== "greenhouse" && ctx.areaType !== "indoors") {
      results.push(candidate(ctx, {
        ruleId:       "heat_alert",
        taskType:     "water",
        title:        `High temperatures forecast — water ${ctx.cropName} early morning and check for wilting`,
        scheduledFor: today,
        urgency:      "medium",
        engineType:   "risk",
        recordType:   "alert",
        expiryDays:   1,
        leadTimeDays: 0,
      }));
    }

    // ── Pest and disease risk rules ──────────────────────────────────────────
    for (const rule of pestRules) {
      if (!this._pestRuleApplies(ctx, rule, m)) continue;

      const riskLevel = this._assessRiskLevel(ctx, rule);
      if (!riskLevel) continue;

      const template = rule[`urgency_${riskLevel}_template`];
      if (!template) continue;

      const title = template.replace(/\{crop\}/g, ctx.cropName);

      results.push(candidate(ctx, {
        ruleId:       `pest_${rule.pest_code}`,
        taskType:     rule.default_task_type || "inspect_pests",
        title,
        scheduledFor: today,
        urgency:      riskLevel,
        engineType:   "risk",
        recordType:   riskLevel === "high" ? "task" : "alert",
        expiryDays:   rule.alert_cooldown_days || 2,
        leadTimeDays: 0,
        riskPayload:  {
          pest_code:         rule.pest_code,
          risk_kind:         rule.risk_kind,
          risk_level:        riskLevel,
          treatment_guidance: rule.treatment_guidance,
        },
      }));
    }

    return results;
  }

  _pestRuleApplies(ctx, rule, m) {
    // Season check
    if (rule.season_start_month && m < rule.season_start_month) return false;
    if (rule.season_end_month   && m > rule.season_end_month)   return false;

    // Crop group check
    if (rule.applies_to_crop_groups?.length) {
      if (!ctx.cropGroup || !rule.applies_to_crop_groups.includes(ctx.cropGroup)) return false;
    }

    // Specific crop def check
    if (rule.applies_to_crop_def_ids?.length) {
      if (!ctx.def?.id || !rule.applies_to_crop_def_ids.includes(ctx.def.id)) return false;
    }

    // Stage check
    if (rule.stage_min) {
      if (STAGE_ORDER.indexOf(ctx.stage) < STAGE_ORDER.indexOf(rule.stage_min)) return false;
    }
    if (rule.stage_max) {
      if (STAGE_ORDER.indexOf(ctx.stage) > STAGE_ORDER.indexOf(rule.stage_max)) return false;
    }

    // Outdoor requirement — skip for greenhouse and any indoor-sown crops
    if (rule.requires_outdoor && ctx.areaType === "greenhouse") return false;
    if (rule.requires_outdoor && ctx.areaType === "indoors") return false;
    if (rule.requires_outdoor && ctx.cropStatus === "sown_indoors") return false;
    if (rule.requires_unprotected && ctx.isProtected) return false;

    return true;
  }

  _assessRiskLevel(ctx, rule) {
    const { tempC, rainMm } = ctx;

    let score = 0;

    // Temperature check
    if (rule.temp_min_c !== null && tempC !== null && tempC >= rule.temp_min_c) score++;
    if (rule.temp_max_c !== null && tempC !== null && tempC <= rule.temp_max_c) score++;

    // Rain check
    if (rule.rain_last_24h_min_mm !== null && rainMm !== null && rainMm >= rule.rain_last_24h_min_mm) score++;

    // Observation boost — if user has confirmed pest of this type, escalate
    const pestCode = rule.pest_code;
    const confirmedByObs = (ctx.recentPestObs || []).some(o =>
      o.symptom_code?.includes(pestCode) || (o.observation_type === "pest" && score > 0)
    );
    if (confirmedByObs) score += 2; // observation confirmation = strong signal

    // Season alone can give low risk
    const m = currentMonth();
    const inSeason = (!rule.season_start_month || m >= rule.season_start_month) &&
                     (!rule.season_end_month   || m <= rule.season_end_month);
    if (inSeason && score === 0) return "low"; // seasonal watch regardless

    if (score === 0) return null;
    if (score === 1) return "low";
    if (score === 2) return "medium";
    return "high";
  }
}

// ── RULE ENGINE (main class) ──────────────────────────────────────────────────

class RuleEngine {
  constructor(supabase = null, options = {}) {
    this.supabase  = supabase;
    this.dryRun    = options.dryRun || false;
    this.scheduled = new ScheduledRuleEngine();
    this.risk      = new DynamicRiskEngine();
  }

  async runForUser(userId) {
    if (this.supabase) {
      await this._cleanupOrphanedTasks(userId);
      await this._expireStaleItems(userId);
    }

    const [crops, rules, weatherByLocation, rainHistoryByLocation, envModifiers, userFeeds, pestRules, recentObservations] = await Promise.all([
      this._loadCrops(userId),
      this._loadRules(),
      this._loadWeatherByLocation(userId),
      this._loadRainHistory(userId),
      this._loadEnvModifiers(),
      this._loadUserFeeds(userId),
      this._loadPestRules(),
      this._loadRecentObservations(userId),
    ]);

    const allCandidates = [];

    // Build a canonical context map once — reused by both the candidate loop
    // and the watering section so all stage/lastWateredAt values are consistent.
    const ctxMap = new Map();

    for (const crop of crops) {
      const locId   = crop.location_id || crop.area?.location_id;
      const weather = weatherByLocation[locId] || null;
      const areaType = crop.area?.type;
      const envMods  = envModifiers[areaType] || {};

      // Build normalised context once per crop
      const cropObs = recentObservations.filter(o => o.crop_id === crop.id);
      const rainMm7dayActual = rainHistoryByLocation[locId]?.total7day ?? null;
      const ctx = buildCropContext(crop, weather, envMods, userFeeds, cropObs, rainMm7dayActual);
      ctxMap.set(crop.id, ctx);

      // Scheduled engine — future tasks
      const scheduled = this.scheduled.evaluate(ctx, rules);
      allCandidates.push(...scheduled);

      // Risk engine — short-horizon alerts
      // Skip for planned, finished, or indoor crops (pests not relevant until outside)
      const skipRisk = crop.status === "planned" ||
                       crop.status === "sown_indoors" ||
                       ctx.stage === "finished";
      if (!skipRisk) {
        const alerts = this.risk.evaluate(ctx, pestRules);
        allCandidates.push(...alerts);
      }
    }

    // ── Succession sow-label injection ───────────────────────────────────────
    // Append "(Sow N)" to task titles for crops that belong to a succession group.
    // Keeps the Today screen unambiguous when a user has Carrots Sow 1 + Sow 2 etc.
    const successionIndexMap = {};
    for (const crop of crops) {
      if (crop.succession_group_id && crop.succession_index) {
        successionIndexMap[crop.id] = crop.succession_index;
      }
    }
    if (Object.keys(successionIndexMap).length > 0) {
      for (const candidate of allCandidates) {
        if (candidate.crop_instance_id && successionIndexMap[candidate.crop_instance_id]) {
          const idx = successionIndexMap[candidate.crop_instance_id];
          if (candidate.action && !candidate.action.includes(`(Sow ${idx})`)) {
            candidate.action = `${candidate.action} (Sow ${idx})`;
          }
        }
      }
    }

    // ── Watering tasks — one per area ────────────────────────────────────────
    // Group active outdoor crops by area, check dry conditions, generate one
    // watering task per area rather than per crop.
    // Uses ctxMap for inferred stage, tiered lastWateredAt and areaType — not raw crop fields.
    const areaMap = new Map(); // areaId -> { crops, areaType, areaName, locId, weather }
    for (const crop of crops) {
      if (crop.status === "planned" || crop.status === "sown_indoors" || crop.status === "finished") continue;
      const areaId = crop.area_id;
      if (!areaId) continue;
      const locId = crop.location_id || crop.area?.location_id;
      const weather = weatherByLocation[locId] || null;
      const ctx = ctxMap.get(crop.id);
      const areaType = ctx?.areaType || crop.area?.type || "raised_bed";
      const areaName = crop.area?.name || "Garden area";
      console.log(`[Watering] crop=${crop.name} areaId=${areaId} locId=${locId} rainMm=${weather?.rain_mm ?? null} areaType=${areaType}`);
      if (!areaMap.has(areaId)) {
        areaMap.set(areaId, { crops: [], areaType, areaName, locId, weather, userId });
      }
      areaMap.get(areaId).crops.push(crop);
    }

    const today = todayISO();

    const MOISTURE_WINDOW_DAYS = 7;

    for (const [areaId, area] of areaMap.entries()) {
      const { crops: areaCrops, areaType, areaName, weather } = area;
      const rainMm = weather?.rain_mm ?? null;

      // ── Soil moisture gate (if logged within 7 days) ───────────────────────
      // Reads soil_moisture and soil_moisture_logged_at from the area record.
      // wet  → suppress watering task entirely (soil is already moist)
      // dry  → reduce DRY_DAY_THRESHOLD by 1 and force high urgency
      // ok   → no change to current behaviour
      // no reading or reading older than 7 days → fall through to normal logic
      const firstCropArea = areaCrops[0]?.area;
      const soilMoisture       = firstCropArea?.soil_moisture       || null;
      const soilMoistureLoggedAt = firstCropArea?.soil_moisture_logged_at || null;
      let moistureActive = false;
      if (soilMoisture && soilMoistureLoggedAt) {
        const daysSinceMoisture = Math.floor(
          (Date.now() - new Date(soilMoistureLoggedAt).getTime()) / 86400000
        );
        moistureActive = daysSinceMoisture <= MOISTURE_WINDOW_DAYS;
      }
      if (moistureActive && soilMoisture === "wet") continue; // soil wet — skip watering

      // Pull rain signals for this area's location
      const areaLocId          = areaCrops[0]?.area?.location_id || areaCrops[0]?.location_id || null;
      const rainHistory        = areaLocId ? (rainHistoryByLocation?.[areaLocId] ?? null) : null;
      const rainMm7dayActual   = rainHistory?.total7day ?? null;
      const rainMm24hMax       = rainHistory?.max24h    ?? null;
      const rainForecast5day   = weather?.rain_mm_forecast_5day ?? null;
      const isOutdoor          = areaType !== "indoors" && areaType !== "greenhouse";

      // Suppress if it rained more than 5mm today (next 24h forecast gate)
      if (areaType !== "indoors" && rainMm !== null && rainMm >= 5) continue;

      // Suppress if any forecast write in the last 24h predicted >= 5mm.
      // This catches overnight rain that was forecast but has now passed —
      // the next 24h forward forecast would show 0mm even though it just rained.
      if (isOutdoor && rainMm24hMax !== null && rainMm24hMax >= 5) continue;

      // Suppress if it's been a genuinely wet week for outdoor areas — ground is already moist.
      // Greenhouse and indoors unaffected — rain doesn't reach them.
      // Guard: only apply if rainMm7dayActual is populated (first 7 days after deploy it will be null)
      if (isOutdoor && rainMm7dayActual !== null && rainMm7dayActual > 20) continue;

      // Dry day thresholds per area type
      const BASE_THRESHOLD = areaType === "greenhouse" || areaType === "indoors" ? 1
        : areaType === "container" || areaType === "pot" ? 2
        : areaType === "raised_bed" ? 4
        : 6; // ground/border

      // Reduce threshold by 1 for flowering/fruiting crops — more drought sensitive
      // Uses inferred ctx.stage rather than raw c.stage so stage inference is accurate
      const hasHighRiskCrop = areaCrops.some(c => {
        const cropCtx = ctxMap.get(c.id);
        const stage = cropCtx?.stage || c.stage;
        return ["flowering", "fruiting", "harvesting"].includes(stage);
      });
      // Apply soil moisture modifier — dry reading reduces threshold (water sooner)
      const baseDryThreshold = hasHighRiskCrop && BASE_THRESHOLD > 1 ? BASE_THRESHOLD - 1 : BASE_THRESHOLD;
      const DRY_DAY_THRESHOLD = (moistureActive && soilMoisture === "dry" && baseDryThreshold > 1)
        ? baseDryThreshold - 1
        : baseDryThreshold;

      // Find the most recent watering signal across all crops in this area.
      // Uses ctx.lastWateredAt which already applies tiered inheritance:
      // crop-level beats area-level beats location-level
      let lastWateredDate = null;
      for (const crop of areaCrops) {
        const cropCtx = ctxMap.get(crop.id);
        const lw = cropCtx?.lastWateredAt ? cropCtx.lastWateredAt.split("T")[0] : null;
        if (lw && (!lastWateredDate || lw > lastWateredDate)) lastWateredDate = lw;
      }

      // Calculate days since last watered
      const daysSinceWatered = lastWateredDate
        ? Math.floor((Date.now() - new Date(lastWateredDate).getTime()) / 86400000)
        : null;

      // Only fire if overdue
      if (daysSinceWatered !== null && daysSinceWatered < DRY_DAY_THRESHOLD) continue;
      if (daysSinceWatered === null) {
        // Never watered — only fire if no rain recently (rain_mm null = unknown, skip)
        if (rainMm === null) continue;
      }

      // Prioritise crops most at risk: flowering/fruiting > seedling > vegetative
      // Uses inferred ctx.stage for accurate ordering
      const STAGE_PRIORITY = { flowering: 0, fruiting: 0, harvesting: 0, seedling: 1, vegetative: 2, seed: 3 };
      const sortedCrops = [...areaCrops].sort((a, b) => {
        const stageA = ctxMap.get(a.id)?.stage || a.stage;
        const stageB = ctxMap.get(b.id)?.stage || b.stage;
        return (STAGE_PRIORITY[stageA] ?? 3) - (STAGE_PRIORITY[stageB] ?? 3);
      });
      const atRiskCrops = sortedCrops.slice(0, 3).map(c => c.name);
      const atRiskText = atRiskCrops.length > 0 ? " — pay particular attention to: " + atRiskCrops.join(", ") : "";

      const daysText = daysSinceWatered !== null ? " (" + daysSinceWatered + " days since last watered)" : "";

      // Urgency calculation — four signals in priority order:
      // 1. Dry soil reading logged recently → always high
      // 2. Rain forecast in next 5 days (>15mm) → downgrade to low (relief coming)
      // 3. No rain forecast and overdue by threshold+2 → high (no relief coming)
      // 4. Default escalation at threshold+2 → high
      const rainComingSoon = isOutdoor && rainForecast5day !== null && rainForecast5day > 15;
      const noDryRelief    = isOutdoor && (rainForecast5day === null || rainForecast5day < 2);

      const urgency = (moistureActive && soilMoisture === "dry")
        ? "high"
        : rainComingSoon
          ? "low"
          : (noDryRelief && daysSinceWatered !== null && daysSinceWatered >= DRY_DAY_THRESHOLD)
            ? "high"
            : daysSinceWatered !== null && daysSinceWatered >= DRY_DAY_THRESHOLD + 2
              ? "high"
              : "medium";

      // Use first crop in area for context (area_id, user_id)
      const refCrop = areaCrops[0];
      const ctx = {
        userId,
        cropId:   null, // area-level task
        cropName: areaName,
        areaId,
      };

      allCandidates.push({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Water ${areaName}${daysText}${atRiskText}`,
        task_type:        "water",
        urgency,
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(addDays(today, 1) + "T23:59:59Z").toISOString(),
        status:           "due",
        engine_type:      "risk",
        record_type:      "task",
        source:           "rule_engine",
        rule_id:          "watering_due",
        source_key:       sourceKey({ u: userId, a: areaId, r: "watering_due", d: today }),
        date_confidence:  "exact",
        meta:             JSON.stringify({ dry_days: daysSinceWatered, area_type: areaType, soil_moisture: moistureActive ? soilMoisture : null }),
        risk_payload:     null,
      });
    }

    // ── Succession next-sow reminders ────────────────────────────────────────
    // For each succession group where target_sowings > active sowing count
    // and the next sow date is due, generate a reminder task.
    if (this.supabase) {
      try {
        const successionReminders = await this._successionReminders(userId);
        allCandidates.push(...successionReminders);
      } catch (err) {
        console.error("[RuleEngine] Succession reminder error:", err.message);
      }
    }

    // ── Soil moisture stale advisories ──────────────────────────────────────
    // Generates a low-urgency 'check soil moisture' task per area where the
    // user previously logged a reading that has now gone stale (> 7 days).
    // Only fires if the user has ever logged moisture for that area.
    // No extra DB query — derived from crops already in memory.
    const soilAdvisories = this._soilAdvisories(userId, crops);
    allCandidates.push(...soilAdvisories);

    const phAdvisories = this._phAdvisories(userId, crops);
    allCandidates.push(...phAdvisories);

    const soilTempAdvisories = this._soilTempAdvisories(userId, crops);
    allCandidates.push(...soilTempAdvisories);

    // ── Weeding maintenance rhythm ────────────────────────────────────────────
    const weedingTasks = this._weedingAdvisories(userId, crops, rainHistoryByLocation, weatherByLocation);
    allCandidates.push(...weedingTasks);

    // ── Confidence scoring + surface_class assignment ───────────────────────
    // Score every candidate. Assign surface_class based on score.
    // Suppress candidates below threshold, with new-user fallback.

    const scoredCandidates = allCandidates.map(c => {
      // Find the ctx for this candidate (area-level tasks have no crop_instance_id)
      const ctx = c.crop_instance_id ? ctxMap.get(c.crop_instance_id) : null;
      const score = ctx ? scoreCandidate(ctx, c) : 55; // area-level tasks (watering) default to surfaced
      return { ...c, _score: score };
    });

    // Determine if user is new (within first 3 days of any crop being added)
    const isNewUser = crops.some(crop => {
      const created = crop.created_at ? new Date(crop.created_at) : null;
      return created && (Date.now() - created.getTime()) < 3 * 86400000;
    });

    // Split into surfaced and suppressed
    // Task threshold is 30 — the 30–49 "insight" band is collapsed into task.
    // There is no current UI surface for insights, so keeping them hidden causes
    // guidance to silently disappear. When an insight surface is built, restore
    // the split by raising this threshold back to 50.
    const surfaced = scoredCandidates.filter(c => c._score >= 30).map(c => ({
      ...c, surface_class: "task",
    }));
    const suppressed = scoredCandidates.filter(c => c._score < 30);

    // No insight band for now — all scored candidates are surfaced
    let finalCandidates = [...surfaced];

    // New-user fallback — if nothing surfaced, promote highest-scoring suppressed candidate
    if (finalCandidates.length === 0 && isNewUser && suppressed.length > 0) {
      const best = suppressed.sort((a, b) => b._score - a._score)[0];
      console.log(`[RuleEngine] New-user fallback: promoting ${best.rule_id} (score=${best._score})`);
      finalCandidates = [{ ...best, surface_class: "task" }];
    }

    // ── Null-crop safety net ──────────────────────────────────────────────────
    // If a user still has zero surfaced candidates, check whether they have any
    // active crops with no crop_def_id. These crops produce no engine output
    // because the engine has no window/timing data to work with.
    // Surface one gentle insight per unlinked crop (max 3) so the user sees
    // something rather than an empty Today screen.
    // Rules: surface_class = insight, low urgency, expires today, one per crop per day.
    if (finalCandidates.length === 0) {
      const nullDefCrops = crops.filter(c => !c.crop_def && c.status !== "planned" && c.status !== "finished");
      const today = todayISO();
      const nullFallbacks = nullDefCrops.slice(0, 3).map(c => ({
        user_id:          userId,
        crop_instance_id: c.id,
        area_id:          c.area_id,
        action:           `Check on your ${c.name} and log anything you notice`,
        task_type:        "check",
        urgency:          "low",
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(today + "T23:59:59Z").toISOString(),
        status:           "due",
        engine_type:      "scheduled",
        record_type:      "task",
        source:           "rule_engine",
        rule_id:          "null_crop_fallback",
        source_key:       sourceKey({ u: userId, c: c.id, r: "null_crop_fallback", d: today }),
        date_confidence:  "approximate",
        surface_class:    "insight",
        meta:             JSON.stringify({ null_def: true }),
        risk_payload:     null,
        _score:           25,
      }));
      if (nullFallbacks.length > 0) {
        console.log(`[RuleEngine] Null-crop fallback: ${nullFallbacks.length} insight(s) for user=${userId}`);
        finalCandidates = nullFallbacks;
      }
    }

    // ── No-rules-def safety net ───────────────────────────────────────────────
    // If a user still has zero surfaced candidates after the null-def check,
    // they may have crops with a valid crop_def_id but no crop_rules rows behind it
    // (e.g. Potatoes, which has potato-type-specific logic but no generic rules).
    // The engine produces nothing and the user sees an empty Today screen.
    // Surface one gentle check task per affected active crop (max 3) so the user
    // always sees something after activation.
    if (finalCandidates.length === 0) {
      const today = todayISO();
      // Crops that have a def (so weren't caught above) but are active and non-finished
      const noRulesCrops = crops.filter(c =>
        c.crop_def &&
        c.active &&
        c.status !== "planned" &&
        c.status !== "finished"
      );
      const noRulesFallbacks = noRulesCrops.slice(0, 3).map(c => ({
        user_id:          userId,
        crop_instance_id: c.id,
        area_id:          c.area_id,
        action:           `Check on your ${c.name} — note how it's looking and log anything unusual`,
        task_type:        "check",
        urgency:          "low",
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(today + "T23:59:59Z").toISOString(),
        status:           "due",
        engine_type:      "scheduled",
        record_type:      "task",
        source:           "rule_engine",
        rule_id:          "no_rules_fallback",
        source_key:       sourceKey({ u: userId, c: c.id, r: "no_rules_fallback", d: today }),
        date_confidence:  "approximate",
        surface_class:    "task",
        meta:             JSON.stringify({ no_rules: true }),
        risk_payload:     null,
        _score:           35,
      }));
      if (noRulesFallbacks.length > 0) {
        console.log(`[RuleEngine] No-rules fallback: ${noRulesFallbacks.length} task(s) for user=${userId}`);
        finalCandidates = noRulesFallbacks;
      }
    }

    // ── Run-level logging — always emit so suppression rates are visible ────────
    const nTask       = surfaced.length;
    const nSuppressed = suppressed.length;
    const nFallback   = (finalCandidates.length === 1 && finalCandidates[0]._score < 30) ? 1 : 0;
    console.log(
      `[RuleEngine] user=${userId} ` +
      `total=${scoredCandidates.length} ` +
      `task=${nTask} suppressed=${nSuppressed} fallback=${nFallback}`
    );

    // Decision logging — strictly best-effort, never blocks task generation
    if (this.supabase) {
      this._logDecisions(userId, scoredCandidates, finalCandidates).catch(err => {
        console.error("[RuleEngine] Decision log failed (non-fatal):", err.message);
      });
    }

    // Upsert all candidates
    if (!this.dryRun && this.supabase) {
      await this._materialize(finalCandidates);
    }

    return finalCandidates;
  }

  // ── Decision logger ──────────────────────────────────────────────────────────
  // Logs decisions for score >= 30 (surfaced and borderline suppressed).
  // Suppressed low-score candidates are not logged to avoid noise.

  async _logDecisions(userId, allScored, surfacedFinal) {
    if (!this.supabase) return;
    const surfacedKeys = new Set(surfacedFinal.map(c => c.source_key));
    const toLog = allScored.filter(c => c._score >= 30);
    if (!toLog.length) return;

    const rows = toLog.map(c => ({
      user_id:           userId,
      crop_id:           c.crop_instance_id || null,
      rule_id:           c.rule_id || null,
      model_name:        "v1_max",
      surface_class:     surfacedKeys.has(c.source_key) ? "task" : "suppressed",
      score:             c._score,
      surfaced:          surfacedKeys.has(c.source_key),
      suppression_reason: !surfacedKeys.has(c.source_key) ? "score_below_threshold" : null,
    }));

    try {
      await this.supabase.from("engine_decision_log").insert(rows);
    } catch (err) {
      // Non-fatal — log silently, don't break task generation
      console.error("[RuleEngine] Decision log error:", err.message);
    }
  }

  // ── Materializer — idempotent upsert ────────────────────────────────────────

  async _materialize(candidates) {
    console.log(`[RuleEngine] Materializing ${candidates.length} candidates`);
    let inserted = 0, errors = 0;
    for (const c of candidates) {
      try {
        // Strip internal fields
        const { _description, _status, _score, ...task } = c;

        console.log(`[RuleEngine] Upserting: rule=${task.rule_id} type=${task.engine_type} status=${task.status} due=${task.due_date} key=${task.source_key?.slice(0,40)}`);

        const { data, error } = await this.supabase
          .from("tasks")
          .upsert(task, {
            onConflict:       "source_key",
            ignoreDuplicates: true,   // never overwrite existing tasks — preserves completed_at
          })
          .select("id, engine_type, status");

        if (error) {
          console.error(`[RuleEngine] Upsert error (${task.rule_id}):`, error.message, error.details);
          errors++;
        } else {
          // ignoreDuplicates returns 0 rows when the task already exists — that's fine
          const row = Array.isArray(data) ? data[0] : data;
          console.log(`[RuleEngine] Upserted OK: id=${row?.id ?? "duplicate-skipped"} engine_type=${row?.engine_type ?? task.engine_type}`);
          inserted++;
        }
      } catch (err) {
        console.error("[RuleEngine] Persist error:", err.message);
        errors++;
      }
    }
    console.log(`[RuleEngine] Materialize complete: ${inserted} inserted, ${errors} errors`);
  }

  // ── Expiry ────────────────────────────────────────────────────────────────

  async _expireStaleItems(userId) {
    try {
      await this.supabase
        .from("tasks")
        .update({ status: "expired" })
        .eq("user_id", userId)
        .in("status", ["upcoming","due"])
        .is("completed_at", null)
        .lt("expires_at", new Date().toISOString());
    } catch (err) {
      console.error("[RuleEngine] Expiry error:", err.message);
    }
  }

  // ── Cleanup orphaned tasks ────────────────────────────────────────────────

  async _cleanupOrphanedTasks(userId) {
    try {
      const { data: activeCrops, error } = await this.supabase
        .from("crop_instances").select("id")
        .eq("user_id", userId).eq("active", true).eq("deleted", false);
      if (error || !activeCrops) return;

      const activeIds = activeCrops.map(c => c.id);

      if (activeIds.length) {
        // Delete incomplete tasks linked to crop_instance_ids that are no longer active
        const { data: orphans } = await this.supabase
          .from("tasks").select("id")
          .eq("user_id", userId).is("completed_at", null)
          .not("crop_instance_id", "in", `(${activeIds.join(",")})`)
          .not("crop_instance_id", "is", null);
        if (orphans?.length) {
          const ids = orphans.map(t => t.id);
          await this.supabase.from("rule_log").delete().in("task_id", ids);
          await this.supabase.from("tasks").delete().in("id", ids);
        }
      } else {
        // No active crops — delete all incomplete crop-linked tasks
        const { data: allLinked } = await this.supabase
          .from("tasks").select("id")
          .eq("user_id", userId).is("completed_at", null)
          .not("crop_instance_id", "is", null);
        if (allLinked?.length) {
          const ids = allLinked.map(t => t.id);
          await this.supabase.from("rule_log").delete().in("task_id", ids);
          await this.supabase.from("tasks").delete().in("id", ids);
        }
      }

      // Delete area-level tasks (no crop_instance_id) whose source_key
      // references a crop UUID that is no longer active — catches sow/check
      // tasks for deleted crops that have no crop_instance_id set
      const activeIdSet = new Set(activeIds);
      const { data: areaTasks } = await this.supabase
        .from("tasks").select("id, source_key")
        .eq("user_id", userId).is("completed_at", null)
        .is("crop_instance_id", null);
      if (areaTasks?.length) {
        const stale = areaTasks.filter(t => {
          if (!t.source_key) return false;
          const uuids = t.source_key.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g) || [];
          return uuids.some(id => !activeIdSet.has(id));
        });
        if (stale.length) {
          const ids = stale.map(t => t.id);
          await this.supabase.from("rule_log").delete().in("task_id", ids);
          await this.supabase.from("tasks").delete().in("id", ids);
        }
      }
    } catch (err) {
      console.error("[RuleEngine] Cleanup error:", err.message);
    }
  }

  // ── Weeding maintenance rhythm ───────────────────────────────────────────
  // One area-level weeding check task per area on a rolling cadence.
  // Cadence:
  //   greenhouse / container / pot / indoors : 10 days year-round
  //   raised bed / open ground               : 14 days in season (Mar–Oct)
  //                                            21 days off season (Nov–Feb)
  // Suppressed if last_weeded_at is within the cadence window.
  // Urgency: medium if wet week (actual >15mm or forecast >20mm); low otherwise.

  _weedingAdvisories(userId, crops, rainHistoryByLocation, weatherByLocation) {
    const today    = todayISO();
    const m        = currentMonth();
    const inSeason = m >= 3 && m <= 10;
    const seen     = new Set();
    const tasks    = [];

    for (const crop of crops) {
      const area = crop.area;
      if (!area) continue;
      const areaId = crop.area_id;
      if (seen.has(areaId)) continue;

      const areaType = area.type || null;
      const areaName = area.name || 'your area';

      const intensiveTypes = ['greenhouse', 'container', 'pot', 'indoors'];
      const cadenceDays = intensiveTypes.includes(areaType)
        ? 10
        : inSeason ? 14 : 21;

      // Suppress if weeded recently enough
      const lastWeeded    = area.last_weeded_at || null;
      const daysSinceWeed = lastWeeded ? daysSince(lastWeeded) : null;
      if (daysSinceWeed !== null && daysSinceWeed < cadenceDays) continue;

      seen.add(areaId);

      // Weather signals for urgency
      const locId            = crop.location_id || area.location_id;
      const rainActual7day   = rainHistoryByLocation?.[locId]?.total7day ?? null;
      const weather          = weatherByLocation?.[locId]    ?? null;
      const rainForecast5day = weather?.rain_mm_forecast_5day ?? null;
      const wetRecent        = rainActual7day   !== null && rainActual7day   > 15;
      const wetForecast      = rainForecast5day !== null && rainForecast5day > 20 && inSeason;
      const urgency          = (wetRecent || wetForecast) ? 'medium' : 'low';

      const lastWeededNote = daysSinceWeed !== null ? ` — last weeded ${daysSinceWeed} days ago` : '';
      const wetNote        = wetRecent ? ' — recent rain will have encouraged weed growth' : '';
      const keyDate        = windowAnchor(today, cadenceDays);

      tasks.push({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Check for weeds in ${areaName}${lastWeededNote}${wetNote}`,
        task_type:        'check',
        urgency,
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(addDays(today, Math.floor(cadenceDays / 2)) + 'T23:59:59Z').toISOString(),
        status:           'due',
        engine_type:      'scheduled',
        record_type:      'task',
        source:           'rule_engine',
        rule_id:          'weeding_due',
        source_key:       sourceKey({ u: userId, a: areaId, r: 'weeding_due', d: keyDate }),
        date_confidence:  'approximate',
        meta:             JSON.stringify({
          cadence_days:        cadenceDays,
          days_since_weeded:   daysSinceWeed,
          rain_actual_7day_mm: rainActual7day,
          rain_forecast_5day:  rainForecast5day,
          in_season:           inSeason,
        }),
        risk_payload:     null,
        _score:           55,
      });
    }

    if (tasks.length) console.log(`[Weeding] ${tasks.length} weeding advisory(s) for user=${userId}`);
    return tasks;
  }

  // ── Soil moisture stale advisory generator ───────────────────────────────
  // Derives stale areas from the crops already loaded — no extra DB call.
  // Fires one advisory per area where:
  //   - soil_moisture was previously logged (field is not null)
  //   - the reading is older than MOISTURE_VALIDITY_DAYS (7)
  //   - no recent watering logged within 2 days (advisory would be redundant)
  //   - area has at least one active crop (guaranteed by crops list)
  // Urgency: medium for greenhouse/container/pot; low otherwise.

  _soilAdvisories(userId, crops) {
    const MOISTURE_VALIDITY_DAYS = 7;
    const RECENT_WATERING_DAYS   = 2;
    const today = todayISO();
    const seen  = new Set(); // dedupe by area_id
    const tasks = [];

    for (const crop of crops) {
      const area = crop.area;
      if (!area) continue;
      const areaId = crop.area_id;
      if (seen.has(areaId)) continue;

      // Only fire if user has ever logged moisture for this area
      const moisture          = area.soil_moisture        || null;
      const moistureLoggedAt  = area.soil_moisture_logged_at || null;
      if (!moisture || !moistureLoggedAt) continue;

      // Check the reading is stale
      const ageDays = daysSince(moistureLoggedAt);
      if (ageDays === null || ageDays <= MOISTURE_VALIDITY_DAYS) continue;

      // Suppress if user watered very recently — they're clearly active
      const lastWatered = area.last_watered_at || null;
      const daysSinceWatered = lastWatered ? daysSince(lastWatered) : null;
      if (daysSinceWatered !== null && daysSinceWatered <= RECENT_WATERING_DAYS) continue;

      seen.add(areaId);

      const areaName  = area.name || 'your area';
      const areaType  = area.type || null;
      const thirsty   = ['greenhouse', 'container', 'pot', 'indoors'].includes(areaType);
      const urgency   = thirsty ? 'medium' : 'low';

      // Key snaps to week so task doesn't regenerate daily after being dismissed
      const keyDate = windowAnchor(today, 7);

      tasks.push({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Check soil moisture in ${areaName} — your last reading was ${ageDays} days ago`,
        task_type:        'check',
        urgency,
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(addDays(today, 3) + 'T23:59:59Z').toISOString(),
        status:           'due',
        engine_type:      'scheduled',
        record_type:      'task',
        source:           'rule_engine',
        rule_id:          'soil_moisture_stale',
        source_key:       sourceKey({ u: userId, a: areaId, r: 'soil_moisture_stale', d: keyDate }),
        date_confidence:  'approximate',
        meta:             JSON.stringify({ last_moisture: moisture, age_days: ageDays, area_type: areaType }),
        risk_payload:     null,
        _score:           55,
      });
    }

    if (tasks.length) console.log(`[SoilAdvisory] ${tasks.length} stale moisture advisory(s) for user=${userId}`);
    return tasks;
  }

  // ── Soil pH stale advisory generator ─────────────────────────────────────
  // Fires one advisory per area where:
  //   - soil_ph was previously logged (field is not null)
  //   - the reading is older than PH_VALIDITY_DAYS (180)
  //   - at least one crop in the area has ph_min/ph_max defined AND the logged
  //     pH is outside that crop's acceptable range — if everything is in range
  //     the advisory is low value and is suppressed
  //   - if no crop in the area has pH thresholds defined, still fire (generic)
  // Urgency always low — pH changes slowly and this is informational.

  _phAdvisories(userId, crops) {
    const PH_VALIDITY_DAYS = 180;
    const today = todayISO();
    const seen  = new Set();
    const tasks = [];

    // Group crops by area_id for out-of-range check
    const cropsByArea = new Map();
    for (const crop of crops) {
      if (!crop.area_id) continue;
      if (!cropsByArea.has(crop.area_id)) cropsByArea.set(crop.area_id, []);
      cropsByArea.get(crop.area_id).push(crop);
    }

    for (const crop of crops) {
      const area = crop.area;
      if (!area) continue;
      const areaId = crop.area_id;
      if (seen.has(areaId)) continue;

      // Only fire if user has ever logged pH for this area
      const ph           = area.soil_ph            ?? null;
      const phLoggedAt   = area.soil_ph_logged_at  ?? null;
      if (ph === null || !phLoggedAt) continue;

      // Check the reading is stale
      const ageDays = daysSince(phLoggedAt);
      if (ageDays === null || ageDays <= PH_VALIDITY_DAYS) continue;

      // Check whether any crop in this area has pH thresholds defined
      // and whether the logged pH is outside range for at least one of them.
      // If all crops with thresholds are in range, suppress the advisory.
      const areaCrops     = cropsByArea.get(areaId) || [];
      const cropsWithPh   = areaCrops.filter(c => c.crop_def?.soil_ph_min != null && c.crop_def?.soil_ph_max != null);
      const anyOutOfRange = cropsWithPh.some(c => ph < c.crop_def.soil_ph_min || ph > c.crop_def.soil_ph_max);

      // If crops have pH thresholds and all are in range — not worth flagging
      if (cropsWithPh.length > 0 && !anyOutOfRange) continue;

      seen.add(areaId);

      const areaName = area.name || 'your area';
      const keyDate  = windowAnchor(today, 30); // snap to month — no need to nag weekly

      tasks.push({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Retest soil pH in ${areaName} — your last reading (pH ${ph}) was ${ageDays} days ago`,
        task_type:        'check',
        urgency:          'low',
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(addDays(today, 14) + 'T23:59:59Z').toISOString(),
        status:           'due',
        engine_type:      'scheduled',
        record_type:      'task',
        source:           'rule_engine',
        rule_id:          'soil_ph_stale',
        source_key:       sourceKey({ u: userId, a: areaId, r: 'soil_ph_stale', d: keyDate }),
        date_confidence:  'approximate',
        meta:             JSON.stringify({ last_ph: ph, age_days: ageDays, any_out_of_range: anyOutOfRange }),
        risk_payload:     null,
        _score:           55,
      });
    }

    if (tasks.length) console.log(`[PhAdvisory] ${tasks.length} stale pH advisory(s) for user=${userId}`);
    return tasks;
  }

  // ── Soil temperature stale advisory generator ────────────────────────────
  // Fires one advisory per area where:
  //   - soil_temperature_c was previously logged (field is not null)
  //   - the reading is older than SOIL_TEMP_VALIDITY_DAYS (14)
  //   - current month is in a season where soil temp actually matters:
  //     Feb–May (sowing/transplanting) or Sep–Oct (late season, frost-sensitive)
  //   - area has at least one crop that is frost-sensitive OR in an early stage
  //     (seed/seedling/vegetative) where soil temp is most relevant
  // Suppressed Jun–Aug when soil is reliably warm and temp is rarely a concern.
  // Urgency: medium if frost-sensitive crops present; low otherwise.

  _soilTempAdvisories(userId, crops) {
    const SOIL_TEMP_VALIDITY_DAYS = 14;
    const today = todayISO();
    const m     = currentMonth();

    // Only fire in months where soil temp materially affects decisions
    const tempMattersMonths = [2, 3, 4, 5, 9, 10];
    if (!tempMattersMonths.includes(m)) return [];

    const seen  = new Set();
    const tasks = [];

    // Group crops by area for frost-sensitive and stage checks
    const cropsByArea = new Map();
    for (const crop of crops) {
      if (!crop.area_id) continue;
      if (!cropsByArea.has(crop.area_id)) cropsByArea.set(crop.area_id, []);
      cropsByArea.get(crop.area_id).push(crop);
    }

    for (const crop of crops) {
      const area = crop.area;
      if (!area) continue;
      const areaId = crop.area_id;
      if (seen.has(areaId)) continue;

      // Only fire if user has ever logged soil temp for this area
      const soilTemp       = area.soil_temperature_c           ?? null;
      const tempLoggedAt   = area.soil_temperature_logged_at   ?? null;
      if (soilTemp === null || !tempLoggedAt) continue;

      // Check reading is stale
      const ageDays = daysSince(tempLoggedAt);
      if (ageDays === null || ageDays <= SOIL_TEMP_VALIDITY_DAYS) continue;

      // Check area has at least one frost-sensitive crop or an early-stage crop
      const areaCrops      = cropsByArea.get(areaId) || [];
      const hasFrostRisk   = areaCrops.some(c => c.crop_def?.frost_sensitive === true);
      const hasEarlyStage  = areaCrops.some(c => ['seed', 'seedling', 'vegetative'].includes(c.stage));
      if (!hasFrostRisk && !hasEarlyStage) continue;

      seen.add(areaId);

      const areaName = area.name || 'your area';
      const urgency  = hasFrostRisk ? 'medium' : 'low';
      const keyDate  = windowAnchor(today, 7); // snap to week

      tasks.push({
        user_id:          userId,
        crop_instance_id: null,
        area_id:          areaId,
        action:           `Update soil temperature for ${areaName} — your last reading (${soilTemp}°C) was ${ageDays} days ago`,
        task_type:        'check',
        urgency,
        due_date:         today,
        scheduled_for:    today,
        visible_from:     today,
        expires_at:       new Date(addDays(today, 3) + 'T23:59:59Z').toISOString(),
        status:           'due',
        engine_type:      'scheduled',
        record_type:      'task',
        source:           'rule_engine',
        rule_id:          'soil_temp_stale',
        source_key:       sourceKey({ u: userId, a: areaId, r: 'soil_temp_stale', d: keyDate }),
        date_confidence:  'approximate',
        meta:             JSON.stringify({ last_temp_c: soilTemp, age_days: ageDays, has_frost_risk: hasFrostRisk }),
        risk_payload:     null,
        _score:           55,
      });
    }

    if (tasks.length) console.log(`[SoilTempAdvisory] ${tasks.length} stale soil temp advisory(s) for user=${userId}`);
    return tasks;
  }

  // ── Succession next-sow reminder generator ────────────────────────────────
  // Loads succession groups, checks how many sowings exist vs target,
  // and generates a "Sow next X (Sow N)" reminder task when due.

  async _successionReminders(userId) {
    const reminders = [];
    const today = todayISO();

    const { data: groups } = await this.supabase
      .from("succession_groups")
      .select("*")
      .eq("user_id", userId);

    if (!groups?.length) return reminders;

    // Fetch active sowings for all groups in one query
    const groupIds = groups.map(g => g.id);
    const { data: sowings } = await this.supabase
      .from("crop_instances")
      .select("id, succession_group_id, succession_index, sown_date, status")
      .in("succession_group_id", groupIds)
      .eq("active", true).eq("deleted", false)
      .order("succession_index", { ascending: false });

    const sowingsByGroup = {};
    for (const s of (sowings || [])) {
      if (!sowingsByGroup[s.succession_group_id]) sowingsByGroup[s.succession_group_id] = [];
      sowingsByGroup[s.succession_group_id].push(s);
    }

    for (const group of groups) {
      const groupSowings = sowingsByGroup[group.id] || [];
      const activeCount  = groupSowings.length;

      // Nothing to remind about if we've hit the target
      if (activeCount >= group.target_sowings) continue;

      // Need interval_days to calculate next due date
      if (!group.interval_days) continue;

      // Find the latest sown_date among active sowings
      const latestSowing = groupSowings
        .filter(s => s.sown_date)
        .sort((a, b) => b.sown_date.localeCompare(a.sown_date))[0];

      if (!latestSowing?.sown_date) continue;

      // Next sow due date = latest sown date + interval_days
      const nextDueDate = addDays(latestSowing.sown_date, group.interval_days);

      // Only fire reminder if due today or overdue
      if (nextDueDate > today) continue;

      const nextIndex = (latestSowing.succession_index || activeCount) + 1;
      const cropName  = group.crop_name;
      const sk        = sourceKey({ u: userId, sg: group.id, r: "succession_sow", n: nextIndex });

      reminders.push({
        user_id:             userId,
        crop_instance_id:    null,
        area_id:             group.area_id,
        succession_group_id: group.id,
        action:              `Sow next ${cropName} (Sow ${nextIndex})`,
        task_type:           "sow",
        urgency:             "medium",
        due_date:            nextDueDate,
        scheduled_for:       nextDueDate,
        visible_from:        nextDueDate,
        expires_at:          new Date(addDays(nextDueDate, 7) + "T23:59:59Z").toISOString(),
        status:              nextDueDate <= today ? "due" : "upcoming",
        engine_type:         "scheduled",
        record_type:         "task",
        source:              "rule_engine",
        rule_id:             "succession_sow_due",
        source_key:          sk,
        date_confidence:     "exact",
        meta:                JSON.stringify({ succession_group_id: group.id, next_index: nextIndex }),
        risk_payload:        null,
      });

      console.log(`[Succession] Reminder: ${cropName} Sow ${nextIndex} due ${nextDueDate} for group ${group.id}`);
    }

    return reminders;
  }

  // ── Data loaders ──────────────────────────────────────────────────────────

  async _loadCrops(userId) {
    if (!this.supabase) return [];
    const { data, error } = await this.supabase
      .from("crop_instances")
      .select(`
        *,
        area:area_id ( type, location_id, name, last_watered_at, last_pruned_or_mulched_at,
          soil_moisture, soil_moisture_logged_at,
          soil_ph, soil_ph_logged_at, soil_temperature_c, soil_temperature_logged_at,
          location:location_id ( last_watered_at, last_frost_spring, first_frost_autumn )
        ),
        crop_def:crop_def_id (
          id, is_perennial, frost_sensitive, sow_method, category,
          days_to_maturity_min, days_to_maturity_max,
          feed_interval_days, pest_window_start, pest_window_end,
          sow_window_start, sow_window_end,
          transplant_window_start, transplant_window_end,
          harvest_month_start, harvest_month_end, feed_type,
          default_establishment, sensitivity_band,
          soil_ph_min, soil_ph_max, soil_temp_min_c
        ),
        variety:variety_id (
          days_to_maturity_min, days_to_maturity_max,
          frost_sensitive_override, feed_interval_days_override,
          pest_window_start_override, pest_window_end_override,
          sow_window_start, sow_window_end,
          transplant_window_start, transplant_window_end,
          potato_type
        )
      `)
      .eq("user_id", userId)
      .eq("active", true).eq("deleted", false);
    if (error) throw error;
    return data || [];
  }

  async _loadRules() {
    if (!this.supabase) return [];
    const { data, error } = await this.supabase
      .from("crop_rules").select("*")
      .eq("active", true)
      .order("priority_score", { ascending: false });
    if (error) throw error;
    return data || [];
  }

  async _loadPestRules() {
    if (!this.supabase) return [];
    const { data } = await this.supabase
      .from("pest_risk_rules").select("*").eq("is_active", true);
    return data || [];
  }

  async _loadWeatherByLocation(userId) {
    if (!this.supabase) return {};
    const { data: locations } = await this.supabase
      .from("locations").select("id, postcode").eq("user_id", userId);
    const result = {};
    for (const loc of locations || []) {
      if (!loc.postcode) continue;
      const { data: cached } = await this.supabase
        .from("weather_cache")
        .select("temp_c, frost_risk, frost_risk_7day, rain_mm, rain_mm_forecast_5day, condition")
        .eq("postcode", loc.postcode)
        .gt("expires_at", new Date().toISOString())
        .single();
      if (cached) result[loc.id] = cached;
    }
    return result;
  }

  // Returns total rainfall (mm) over the last 7 days per location_id.
  // Queries weather_history which accumulates one row per cache refresh.
  // Returns { [locId]: { total7day, max24h } } per location.
  // max24h = the highest single rain_mm forecast value written in the last 24h.
  // If any write predicted >= 5mm in the last 24h, it likely rained or was about to.
  // Returns {} if table is empty or not yet populated.
  async _loadRainHistory(userId) {
    if (!this.supabase) return {};
    const { data: locations } = await this.supabase
      .from("locations").select("id, postcode").eq("user_id", userId);
    const result = {};
    const cutoff7day = new Date(Date.now() - 7 * 86400000).toISOString();
    const cutoff24h  = new Date(Date.now() - 24 * 3600000).toISOString();
    for (const loc of locations || []) {
      if (!loc.postcode) continue;
      const { data: rows } = await this.supabase
        .from("weather_history")
        .select("rain_mm, recorded_at")
        .eq("postcode", loc.postcode)
        .gte("recorded_at", cutoff7day);
      if (rows?.length) {
        const total7day = rows.reduce((sum, r) => sum + (r.rain_mm || 0), 0);
        const max24h    = rows
          .filter(r => r.recorded_at >= cutoff24h)
          .reduce((max, r) => Math.max(max, r.rain_mm || 0), 0);
        result[loc.id] = { total7day, max24h };
      }
    }
    return result;
  }

  async _loadEnvModifiers() {
    if (!this.supabase) return {};
    const { data } = await this.supabase.from("environment_modifiers").select("*");
    return (data || []).reduce((acc, m) => {
      acc[m.area_type] = acc[m.area_type] || {};
      acc[m.area_type][m.modifier_type] = m.value;
      return acc;
    }, {});
  }

  async _loadUserFeeds(userId) {
    if (!this.supabase) return [];
    const { data } = await this.supabase
      .from("user_feeds")
      .select("id, brand, product_name, form, feed_type, npk, dilution_ml_per_litre, frequency_days, suitable_crop_types, application_method, notes, enriched")
      .eq("user_id", userId).eq("active", true).eq("enriched", true);
    return data || [];
  }

  async _loadRecentObservations(userId) {
    if (!this.supabase) return [];
    const cutoff = new Date(Date.now() - 30 * 86400000).toISOString();
    const { data } = await this.supabase
      .from("observation_logs")
      .select("id, crop_id, observation_type, symptom_code, observed_at, confirmed_stage, notes, resolved_at")
      .eq("user_id", userId)
      .gte("observed_at", cutoff)
      .order("observed_at", { ascending: false });
    return data || [];
  }

}

module.exports = { RuleEngine, buildCropContext, resolveEffectiveValues: buildCropContext, inferStage: () => {}, daysSince, matchFeed, formatFeedAction };