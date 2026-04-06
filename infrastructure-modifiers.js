"use strict";

// ─────────────────────────────────────────────────────────────────────────────
// infrastructure-modifiers.js
//
// Tunable modifier constants for the ROI / infrastructure engine.
// These are product assumptions, not user data.
// Tweak freely — no DB migration required.
// Bump MODIFIER_VERSION if you change multipliers, so committed plans retain
// a record of which assumptions were in effect at time of planning.
// ─────────────────────────────────────────────────────────────────────────────

const MODIFIER_VERSION = "v1";

// ── Cost ranges (GBP) ─────────────────────────────────────────────────────────
// [low, high] per size tier
const COST_RANGES = {
  greenhouse:     { small: [150, 300], medium: [300,  600], large: [600,  1500] },
  polytunnel:     { small: [80,  200], medium: [200,  500], large: [500,  1200] },
  raised_bed:     { small: [40,   80], medium: [80,   180], large: [180,   400] },
  irrigation:     { small: [30,   80], medium: [80,   200], large: [200,   500] },
  water_butt:     { small: [30,   60], medium: [60,   120], large: [120,   250] },
  compost_system: { small: [25,   60], medium: [60,   150], large: [150,   300] },
};

// ── Yield multipliers per crop category per infrastructure type ───────────────
// Applied as: modelled_yield = baseline_yield × multiplier
// Conservative estimates for UK home growers — intended to be credible, not
// optimistic. Fruiting crops benefit most from protected structures.
const YIELD_MULTIPLIERS = {
  greenhouse: {
    fruiting: 1.55,   // tomatoes, peppers, chilli — biggest beneficiary
    brassica: 1.15,
    root:     1.10,
    legume:   1.15,
    allium:   1.10,
    salad:    1.25,
    herb:     1.20,
    default:  1.15,
  },
  polytunnel: {
    fruiting: 1.45,
    brassica: 1.15,
    root:     1.10,
    legume:   1.10,
    allium:   1.10,
    salad:    1.20,
    herb:     1.15,
    default:  1.12,
  },
  raised_bed: {
    fruiting: 1.20,
    brassica: 1.20,
    root:     1.25,   // drainage + soil warmth — roots benefit most
    legume:   1.15,
    allium:   1.15,
    salad:    1.20,
    herb:     1.15,
    default:  1.18,
  },
  irrigation: {
    fruiting: 1.15,
    brassica: 1.10,
    root:     1.08,
    legume:   1.05,
    allium:   1.08,
    salad:    1.12,
    herb:     1.05,
    default:  1.08,
  },
  water_butt: {
    default:  1.04,   // convenience benefit, not direct yield uplift
  },
  compost_system: {
    fruiting: 1.12,
    brassica: 1.15,
    root:     1.10,
    legume:   1.05,
    allium:   1.10,
    salad:    1.08,
    herb:     1.08,
    default:  1.08,
  },
};

// ── Season extension (weeks) ──────────────────────────────────────────────────
// [earlier_sow_weeks, later_harvest_weeks] vs open ground baseline
const SEASON_EXTENSION = {
  greenhouse:     { earlier_sow_weeks: 4, later_harvest_weeks: 4 },
  polytunnel:     { earlier_sow_weeks: 3, later_harvest_weeks: 3 },
  raised_bed:     { earlier_sow_weeks: 1, later_harvest_weeks: 1 },
  irrigation:     { earlier_sow_weeks: 0, later_harvest_weeks: 0 },
  water_butt:     { earlier_sow_weeks: 0, later_harvest_weeks: 0 },
  compost_system: { earlier_sow_weeks: 0, later_harvest_weeks: 0 },
};

// ── Effort change ─────────────────────────────────────────────────────────────
const EFFORT_CHANGE = {
  greenhouse:     { direction: "harder",  note: "Regular ventilation, watering and pest monitoring needed" },
  polytunnel:     { direction: "harder",  note: "Ventilation and watering more frequent than open ground" },
  raised_bed:     { direction: "easier",  note: "Better drainage and less bending — easier to manage" },
  irrigation:     { direction: "easier",  note: "Reduces manual watering — biggest time saver in dry spells" },
  water_butt:     { direction: "easier",  note: "Reduces reliance on mains water — saves time in summer" },
  compost_system: { direction: "same",    note: "Adds a composting task but reduces bought feed costs over time" },
};

// ── Crop unlocks ──────────────────────────────────────────────────────────────
// Crops that are significantly enabled or improved by this infrastructure.
// Shown as qualitative benefits in the UI — "You can now grow reliably."
const CROP_UNLOCKS = {
  greenhouse:     ["Tomato", "Pepper", "Chilli", "Aubergine", "Cucumber", "Melon", "Sweet Corn", "Basil"],
  polytunnel:     ["Tomato", "Pepper", "Chilli", "Cucumber", "Sweet Corn"],
  raised_bed:     [],
  irrigation:     [],
  water_butt:     [],
  compost_system: [],
};

// ── Things to know ────────────────────────────────────────────────────────────
// Compact downside / maintenance notes surfaced in the deep-dive sheet
const THINGS_TO_KNOW = {
  greenhouse: [
    "Needs ventilation on warm days to prevent overheating",
    "Watering is more frequent — soil dries faster under glass",
    "Spider mite and whitefly are more common in enclosed structures",
    "Heating in winter adds cost if you extend into cold months",
  ],
  polytunnel: [
    "Cover degrades over time — typical lifespan 5–8 years",
    "Ventilation doors essential to prevent overheating in summer",
    "More prone to slug damage — soil stays warm and moist",
  ],
  raised_bed: [
    "Soil top-up needed every 2–3 years as it compresses",
    "Beds can dry out faster in summer — mulching helps",
    "Initial fill cost (compost, topsoil) adds to upfront spend",
  ],
  irrigation: [
    "Drip irrigation needs occasional unblocking and winterising",
    "Overwatering risk if timer is not adjusted seasonally",
  ],
  water_butt: [
    "Refills depend on rainfall — less useful in dry summers",
    "Needs occasional cleaning to prevent algae",
  ],
  compost_system: [
    "Takes 6–12 months to produce usable compost",
    "Works best with a good mix of green and brown material",
  ],
};

// ── Card benefit copy ─────────────────────────────────────────────────────────
// One-line benefit shown on the infrastructure selector card
const CARD_BENEFIT = {
  greenhouse:     "Extend your growing season and unlock tender crops",
  polytunnel:     "Protect crops from frost and boost fruiting harvests",
  raised_bed:     "Improve drainage, soil warmth and rotation flexibility",
  irrigation:     "Reduce watering effort and improve crop consistency",
  water_butt:     "Cut your water use and save time in dry spells",
  compost_system: "Improve soil health and reduce feed costs over time",
};

// ── Compatible area types ─────────────────────────────────────────────────────
// Which area types each infrastructure can be applied to.
// "new" = user is modelling a hypothetical new area, not an existing one.
const COMPATIBLE_AREA_TYPES = {
  greenhouse:     ["greenhouse", "new"],
  polytunnel:     ["polytunnel", "new"],
  raised_bed:     ["raised_bed", "open_ground", "new"],
  irrigation:     ["raised_bed", "open_ground", "greenhouse", "polytunnel"],
  water_butt:     ["raised_bed", "open_ground", "greenhouse", "polytunnel"],
  compost_system: ["raised_bed", "open_ground"],
};

// ── Incompatibility messages ──────────────────────────────────────────────────
// Shown when selected infrastructure is a poor fit for the user's setup
const INCOMPATIBILITY_NOTES = {
  greenhouse:     "A greenhouse has less impact on container or pot-only setups",
  polytunnel:     "A polytunnel is most useful for in-ground or raised bed growing",
  raised_bed:     "This area already uses raised bed growing — improvements will be modest",
  irrigation:     "Irrigation has limited benefit on very small or container-only setups",
  water_butt:     "A water butt is most useful if you have ground beds or large containers",
  compost_system: "Composting has less impact on container-only setups with limited planting area",
};

module.exports = {
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
};
