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

// ── Crop Context Builder ──────────────────────────────────────────────────────

function buildCropContext(crop, weather, envMods, userFeeds, observations = []) {
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

  // Date anchors
  const sowDate         = crop.sown_date         || crop.transplanted_date || null;
  const transplantDate  = crop.transplanted_date  || crop.transplant_date  || null;
  const plantedOutDate  = crop.planted_out_date   || null;
  const lastFedAt       = crop.last_fed_at        || null;

  // Stage
  const daysSown = daysSince(sowDate);
  const pctGrown = daysSown !== null && dtm > 0 ? daysSown / dtm : null;

  let inferredStage = crop.stage || "seed";
  const vegEstablishments = ["runner","tuber","crown","cane"];
  if (!vegEstablishments.includes(def.default_establishment) && sowDate) {
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
  const frostRisk     = weather?.frost_risk === true;
  const frostRisk7day = weather?.frost_risk_7day ?? null;
  const tempC         = weather?.temp_c ?? null;
  const rainMm        = weather?.rain_mm ?? null;

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

    // Windows
    sowStart, sowEnd, txStart, txEnd,
    harvestStart, harvestEnd,

    // Date anchors
    sowDate, transplantDate, plantedOutDate, lastFedAt,
    feedNextDue, estimatedHarvestDate,

    // Stage
    stage: inferredStage, stageBoundaries,
    daysSown, pctGrown,

    // Weather
    frostRisk, frostRisk7day, tempC, rainMm,
    areaType,

    // Feed
    matchedFeed,

    // Potato type
    potatoType: variety.potato_type || null,

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
  const keywords = cropFeedType.toLowerCase();
  const scored = userFeeds.map(feed => {
    let score = 0;
    const ft = (feed.feed_type || "").toLowerCase();
    if (ft.includes("high_potash")       && keywords.includes("potash"))    score += 10;
    if (ft.includes("high_nitrogen")     && keywords.includes("nitrogen"))  score += 10;
    if (ft.includes("balanced")          && keywords.includes("balanced"))  score += 10;
    if (ft.includes("specialist_tomato") && keywords.includes("potash"))    score += 15;
    if (ft.includes("seaweed"))                                              score += 2;
    if (ft.includes("organic_general"))                                      score += 3;
    if (keywords.includes("potash")  && ft.includes("potash"))              score += 5;
    if (keywords.includes("general") && ft.includes("balanced"))            score += 5;
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
    } else if (hs && m < hs) {
      // Upcoming harvest — show in Coming Up Soon
      const harvestDate = monthToDate(hs, year, 1);
      if (withinLookahead(harvestDate, LOOKAHEAD_DAYS.harvest)) {
        results.push(candidate(ctx, {
          ruleId:       "perennial_harvest_upcoming",
        dedupeByName: true,
          taskType:     "harvest",
          title:        `${ctx.cropName} harvest season starts ${MONTHS[hs-1]} — prepare to pick regularly`,
          description:  `Your ${ctx.cropName} harvest window opens in ${MONTHS[hs-1]}. Prepare to pick regularly once ripe.`,
          scheduledFor: harvestDate,
          urgency:      "low",
          leadTimeDays: 7,
          expiryDays:   7,
        }));
      }
    }

    // Spring feed — schedule for March 15 if in lookahead
    if (ctx.feedType) {
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

    // Upcoming flowering — show in Coming Up Soon
    if (fw && m < fw.start) {
      const flowerDate = monthToDate(fw.start, year, 1);
      if (withinLookahead(flowerDate, LOOKAHEAD_DAYS.seasonal)) {
        results.push(candidate(ctx, {
          ruleId:       "perennial_flowering_upcoming",
        dedupeByName: true,
          taskType:     "check",
          title:        `${ctx.cropName} should start flowering in ${MONTHS[fw.start-1]} — watch for blossom and protect from late frosts`,
          scheduledFor: flowerDate,
          urgency:      "low",
          expiryDays:   21,
          leadTimeDays: 7,
        }));
      }
    }

    return results;
  }

  // ── Perennial seasonal care ──────────────────────────────────────────────
  // Pruning, mulching, winter care tasks

  _perennialSeasonalCare(ctx, m, year, today) {
    const results = [];
    const name = ctx.cropName.toLowerCase();

    // Strawberry runners — July/August
    if (name.includes("strawberry") && m >= 7 && m <= 8) {
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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
      results.push(candidate(ctx, {
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

    // Is window open now or coming up within lookahead?
    const windowOpenNow    = m >= sowStart && m <= sowEnd;
    const windowUpcoming   = sowWindowStart > today && withinLookahead(sowWindowStart, LOOKAHEAD_DAYS.sow);

    if (!windowOpenNow && !windowUpcoming) return results;

    const scheduledFor = windowOpenNow ? today : sowWindowStart;

    // Frost suppression for outdoor sowing
    const isOutdoor = sowMethod === "outdoors" || sowMethod === "direct_sow" ||
                      sowMethod === "either" || sowMethod === "tuber";
    const hardFrost = frostSensitive && isOutdoor && ctx.frostRisk7day !== null && ctx.frostRisk7day <= 0;
    const softFrost = frostSensitive && isOutdoor && ctx.frostRisk7day !== null && ctx.frostRisk7day > 0 && ctx.frostRisk7day <= 3;

    // Hard block — don't even show task
    if (hardFrost && windowOpenNow) return results;

    let title, urgency, meta;
    const windowStr = `${MONTHS[sowStart-1]}–${MONTHS[sowEnd-1]}`;

    if (sowMethod === "indoors") {
      title   = `Sow ${ctx.cropName} indoors now — starting indoors gives stronger plants and an earlier harvest. Sowing window: ${windowStr}`;
      urgency = windowOpenNow ? "medium" : "low";
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

    const year        = new Date().getFullYear();
    const txDate      = monthToDate(txStart, year, 1);
    const txEndDate   = monthToDate(txEnd,   year, 28);
    const today       = todayISO();
    const m           = currentMonth();

    const windowOpen     = m >= txStart && m <= txEnd;
    const windowUpcoming = txDate > today && withinLookahead(txDate, LOOKAHEAD_DAYS.transplant);

    if (!windowOpen && !windowUpcoming) return results;

    const scheduledFor = windowOpen ? today : txDate;
    const frostRisk    = ctx.frostRisk;

    const title = frostRisk
      ? `${cropName} is ready to transplant but frost is forecast — harden off and wait for a clear spell`
      : `Time to transplant ${cropName} outdoors — harden off for a few days first if not already done`;

    results.push(candidate(ctx, {
      ruleId:       "transplant_prompt",
      taskType:     "transplant",
      title,
      scheduledFor,
      urgency:      frostRisk ? "low" : "medium",
      expiryDays:   daysBetween(scheduledFor, txEndDate) + 3,
      leadTimeDays: LEAD_TIME_DAYS.transplant,
      meta:         { status_transition: "transplanted" },
    }));

    // Harden off — show 1 week before transplant window
    const hardenDate = addDays(txDate, -7);
    if (hardenDate >= today && withinLookahead(hardenDate, LOOKAHEAD_DAYS.transplant)) {
      results.push(candidate(ctx, {
        ruleId:       "harden_off",
        taskType:     "check",
        title:        `Start hardening off ${cropName} — put outside during the day, bring in at night for a week before transplanting`,
        scheduledFor: hardenDate,
        urgency:      "low",
        expiryDays:   7,
        leadTimeDays: 2,
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
        results.push(candidate(ctx, {
          ruleId:       "feed_scheduled",
          taskType:     "feed",
          title:        formatFeedAction(ctx.cropName, ctx.matchedFeed, ctx.feedType, "Time to feed"),
          description:  `Regular feeding is due for ${ctx.cropName}.`,
          scheduledFor,
          urgency:      isOverdue(feedDue) ? "high" : "medium",
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

      if (!harvestWindowSuppressed && (
        withinLookahead(harvestAlertDate, LOOKAHEAD_DAYS.harvest) ||
        (ctx.estimatedHarvestDate >= addDays(today, -7) && ctx.estimatedHarvestDate <= addDays(today, 14))
      )) {
        const scheduledFor = harvestAlertDate < today ? today : harvestAlertDate;
        results.push(candidate(ctx, {
          ruleId:       "harvest_approaching",
          taskType:     "harvest",
          title:        `${ctx.cropName} should be ready to harvest around ${ctx.estimatedHarvestDate} — check for ripeness`,
          description:  `Based on your sow date and variety, ${ctx.cropName} should be approaching harvest.`,
          scheduledFor,
          urgency:      "medium",
          expiryDays:   21,
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
    if (ctx.frostSensitive && !ctx.isProtected) {
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
    if (ctx.tempC !== null && ctx.tempC >= 28 && ctx.areaType !== "greenhouse") {
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

    const [crops, rules, weatherByLocation, envModifiers, userFeeds, pestRules, recentObservations] = await Promise.all([
      this._loadCrops(userId),
      this._loadRules(),
      this._loadWeatherByLocation(userId),
      this._loadEnvModifiers(),
      this._loadUserFeeds(userId),
      this._loadPestRules(),
      this._loadRecentObservations(userId),
    ]);

    const allCandidates = [];

    for (const crop of crops) {
      const locId   = crop.location_id || crop.area?.location_id;
      const weather = weatherByLocation[locId] || null;
      const areaType = crop.area?.type;
      const envMods  = envModifiers[areaType] || {};

      // Build normalised context once per crop
      const cropObs = recentObservations.filter(o => o.crop_id === crop.id);
      const ctx = buildCropContext(crop, weather, envMods, userFeeds, cropObs);

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

    // Upsert all candidates
    if (!this.dryRun && this.supabase) {
      await this._materialize(allCandidates);
    }

    return allCandidates;
  }

  // ── Materializer — idempotent upsert ────────────────────────────────────────

  async _materialize(candidates) {
    console.log(`[RuleEngine] Materializing ${candidates.length} candidates`);
    let inserted = 0, errors = 0;
    for (const c of candidates) {
      try {
        // Strip internal fields
        const { _description, _status, ...task } = c;

        console.log(`[RuleEngine] Upserting: rule=${task.rule_id} type=${task.engine_type} status=${task.status} due=${task.due_date} key=${task.source_key?.slice(0,40)}`);

        const { data, error } = await this.supabase
          .from("tasks")
          .upsert(task, {
            onConflict:       "source_key",
            ignoreDuplicates: true,   // never overwrite existing tasks — preserves completed_at
          })
          .select("id, engine_type, status")
          .single();

        if (error) {
          console.error(`[RuleEngine] Upsert error (${task.rule_id}):`, error.message, error.details);
          errors++;
        } else {
          console.log(`[RuleEngine] Upserted OK: id=${data?.id} engine_type=${data?.engine_type}`);
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
        .eq("user_id", userId).eq("active", true);
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

  // ── Data loaders ──────────────────────────────────────────────────────────

  async _loadCrops(userId) {
    if (!this.supabase) return [];
    const { data, error } = await this.supabase
      .from("crop_instances")
      .select(`
        *,
        area:area_id ( type, location_id ),
        crop_def:crop_def_id (
          id, is_perennial, frost_sensitive, sow_method, category,
          days_to_maturity_min, days_to_maturity_max,
          feed_interval_days, pest_window_start, pest_window_end,
          sow_window_start, sow_window_end,
          transplant_window_start, transplant_window_end,
          harvest_month_start, harvest_month_end, feed_type,
          default_establishment
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
      .eq("active", true);
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
        .select("temp_c, frost_risk, frost_risk_7day, rain_mm, condition")
        .eq("postcode", loc.postcode)
        .gt("expires_at", new Date().toISOString())
        .single();
      if (cached) result[loc.id] = cached;
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
      .from("crop_observations")
      .select("id, crop_id, symptom_code, observed_at, confirmed_stage, notes")
      .eq("user_id", userId)
      .gte("observed_at", cutoff)
      .order("observed_at", { ascending: false });
    return data || [];
  }

}

module.exports = { RuleEngine, buildCropContext, resolveEffectiveValues: buildCropContext, inferStage: () => {}, daysSince };