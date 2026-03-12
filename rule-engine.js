"use strict";

/**
 * GROW SMART — Rule Engine
 * ─────────────────────────────────────────────────────────────
 * Evaluates crop rules against a user's active crop instances
 * and generates tasks where conditions are met.
 *
 * Usage:
 *   const engine = new RuleEngine(supabaseClient);
 *   const tasks  = await engine.runForUser(userId);
 *
 * Called on:
 *   - User adds or updates a crop (via API)
 *   - Daily Vercel Cron job at 06:00 UTC
 */

// ── Date helpers ──────────────────────────────────────────────────────────────

function daysSince(dateStr) {
  if (!dateStr) return null;
  return Math.floor((Date.now() - new Date(dateStr).getTime()) / 86400000);
}

function currentMonth() {
  return new Date().getMonth() + 1;
}

function todayISO() {
  return new Date().toISOString().split("T")[0];
}

// ── Effective value resolver ──────────────────────────────────────────────────
// Prefer variety overrides over crop_def defaults.
// Variety_id is always optional — never block task generation when it is NULL.

function resolveEffectiveValues(crop) {
  const v = crop.variety;
  const d = crop.crop_def;
  return {
    dtm_min:         v?.days_to_maturity_min          ?? d?.days_to_maturity_min          ?? null,
    dtm_max:         v?.days_to_maturity_max          ?? d?.days_to_maturity_max          ?? null,
    frost_sensitive: v?.frost_sensitive_override      ?? d?.frost_sensitive               ?? true,
    feed_interval:   v?.feed_interval_days_override   ?? d?.feed_interval_days            ?? null,
    pest_start:      v?.pest_window_start_override    ?? d?.pest_window_start             ?? null,
    pest_end:        v?.pest_window_end_override      ?? d?.pest_window_end               ?? null,
    is_perennial:    d?.is_perennial                  ?? false,
  };
}

// ── Timing status ─────────────────────────────────────────────────────────────
// Returns 'early', 'peak', or 'late' based on position within a month window
// adjusted by frost risk and temperature.

function timingStatus(windowStart, windowEnd, weather) {
  const m = currentMonth();
  if (!windowStart || !windowEnd) return "peak"; // no window = assume peak

  const windowLen  = windowEnd - windowStart;
  const posInWindow = m - windowStart; // 0 = first month, windowLen = last month

  // Weather adjustments — frost risk pushes timing later
  const min7      = weather?.frost_risk_7day ?? null;
  const frostAdj  = (min7 !== null && min7 <= 2) ? 1 : 0; // add 1 to position if frost risk

  const adjusted = posInWindow + frostAdj;

  if (windowLen <= 1) return adjusted === 0 ? "peak" : "late";
  if (adjusted === 0)                         return "early";
  if (adjusted >= windowLen)                  return "late";
  return "peak";
}


// Scales stage thresholds to the crop's days_to_maturity.
// Falls back to generic thresholds when DTM is unknown.

function inferStage(crop, effective) {
  const days = daysSince(crop.sown_date);
  if (days === null) return crop.stage;
  const dtm = effective.dtm_min || 80;
  if (days <= 7)           return "seed";
  if (days <= dtm * 0.25)  return "seedling";
  if (days <= dtm * 0.55)  return "vegetative";
  if (days <= dtm * 0.75)  return "flowering";
  if (days <= dtm * 0.95)  return "fruiting";
  if (days <= dtm * 1.5)   return "harvesting";
  return "finished";
}

// ── Condition evaluators ──────────────────────────────────────────────────────

const CONDITIONS = {

  days_since_sow(crop, params) {
    const days = daysSince(crop.sown_date);
    return days !== null && days >= params.days;
  },

  days_since_feed(crop, params, _ctx, effective) {
    const interval = params.days || effective.feed_interval;
    if (!interval) return false;
    if (!crop.last_fed_at) return (daysSince(crop.sown_date) || 0) >= 7;
    return daysSince(crop.last_fed_at) >= interval;
  },

  days_since_transplant(crop, params) {
    const days = daysSince(crop.transplanted_date);
    return days !== null && days >= params.days;
  },

  month_window(_crop, params) {
    const m = currentMonth();
    return m >= params.start && m <= params.end;
  },

  stage_reached(crop, params) {
    const ORDER = ["seed","seedling","vegetative","flowering","fruiting","harvesting","finished"];
    return ORDER.indexOf(crop.stage) >= ORDER.indexOf(params.stage);
  },

  days_to_harvest(crop, params, _ctx, effective) {
    if (!crop.sown_date || !effective.dtm_min) return false;
    const daysLeft = effective.dtm_min - (daysSince(crop.sown_date) || 0);
    return daysLeft >= 0 && daysLeft <= params.days;
  },

  weather_frost(_crop, _params, ctx, effective) {
    // Skip if crop is growing in a frost-protected environment
    if (ctx.envMods?.frost_protection?.protected) return false;
    // Skip if crop is not frost sensitive
    if (!effective.frost_sensitive) return false;
    return ctx.weather?.frost_risk === true;
  },

  weather_temp_below(_crop, params, ctx) {
    return ctx.weather?.temp_c !== undefined && ctx.weather.temp_c < params.temp_c;
  },
};

// ── Rule Engine ───────────────────────────────────────────────────────────────

class RuleEngine {
  constructor(supabase = null, options = {}) {
    this.supabase = supabase;
    this.dryRun   = options.dryRun || false;
  }

  // ── Main entry point ────────────────────────────────────────────────────────

  async runForUser(userId) {
    // ── Cleanup: remove tasks for inactive/deleted crops ──────────────────────
    if (this.supabase) {
      await this._cleanupOrphanedTasks(userId);
    }

    const [crops, rules, weatherByLocation, recentLog, envModifiers, userFeeds] = await Promise.all([
      this._loadCrops(userId),
      this._loadRules(),
      this._loadWeatherByLocation(userId),
      this._loadRuleLog(userId),
      this._loadEnvModifiers(),
      this._loadUserFeeds(userId),
    ]);

    const newTasks = [];

    for (const crop of crops) {
      const effective  = resolveEffectiveValues(crop);
      const cropStatus = crop.status || "growing";
      const areaType   = crop.area?.type;
      const locId      = crop.location_id || crop.area?.location_id;
      const weather    = weatherByLocation[locId] || null;
      const envMods    = envModifiers[areaType]   || {};
      const context    = { weather, envMods };
      const m          = currentMonth();
      const sowMethod  = crop.crop_def?.sow_method || "either";

      // ── PERENNIALS: fruit trees, bushes, established plants ─────────────────
      // Skip the seed cycle entirely. Generate harvest alerts and feed reminders
      // based on harvest window and feed schedule, not days since sowing.
      if (effective.is_perennial) {
        const harvestStart = crop.crop_def?.harvest_month_start;
        const harvestEnd   = crop.crop_def?.harvest_month_end;
        const feedSchedule = crop.crop_def?.feed_type;

        // Harvest alert — when in harvest window
        if (harvestStart && harvestEnd && m >= harvestStart && m <= harvestEnd) {
          const logKey  = `${crop.id}:perennial_harvest`;
          const lastRun = recentLog.get(logKey);
          if (!lastRun || (Date.now() - lastRun.getTime()) >= 14 * 86400000) {
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action:           `${crop.name} should be ready to harvest — check for ripeness and pick regularly to encourage more fruit`,
              task_type:        "harvest",
              urgency:          "medium",
              due_date:         todayISO(),
              source:           "rule_engine",
              rule_id:          "perennial_harvest",
              date_confidence:  "approximate",
              meta:             JSON.stringify({}),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "perennial_harvest" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "perennial_harvest");
            }
          }
        }

        // Spring feed reminder — March/April for most perennials
        if (m >= 3 && m <= 4 && feedSchedule) {
          const logKey  = `${crop.id}:perennial_spring_feed`;
          const lastRun = recentLog.get(logKey);
          if (!lastRun || (Date.now() - lastRun.getTime()) >= 21 * 86400000) {
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action:           `Feed ${crop.name} now growth is starting — apply ${feedSchedule} around the base and water in well`,
              task_type:        "feed",
              urgency:          "low",
              due_date:         todayISO(),
              source:           "rule_engine",
              rule_id:          "perennial_spring_feed",
              date_confidence:  "approximate",
              meta:             JSON.stringify({}),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "perennial_spring_feed" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "perennial_spring_feed");
            }
          }
        }

        // Summer feed reminder — June/July during fruiting
        if (m >= 6 && m <= 7 && feedSchedule) {
          const logKey  = `${crop.id}:perennial_summer_feed`;
          const lastRun = recentLog.get(logKey);
          if (!lastRun || (Date.now() - lastRun.getTime()) >= 21 * 86400000) {
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action:           `Feed ${crop.name} to support fruiting — apply ${feedSchedule} and keep well watered`,
              task_type:        "feed",
              urgency:          "low",
              due_date:         todayISO(),
              source:           "rule_engine",
              rule_id:          "perennial_summer_feed",
              date_confidence:  "approximate",
              meta:             JSON.stringify({}),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "perennial_summer_feed" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "perennial_summer_feed");
            }
          }
        }

        continue; // perennials skip the rest of the rule engine
      }

      // ── PLANNED CROPS: generate sow prompt when in sow window ────────────────
      if (cropStatus === "planned") {
        // Prefer variety-level sow window if set (e.g. late maincrop vs early variety)
        const sowStart       = crop.variety?.sow_window_start       ?? crop.crop_def?.sow_window_start;
        const sowEnd         = crop.variety?.sow_window_end         ?? crop.crop_def?.sow_window_end;
        const frostSensitive = effective.frost_sensitive;

        // ── Potato variety-aware plant-out offset ────────────────────────────
        // first_early: plant out Mar–Apr, second_early: Apr–May, maincrop: Apr–May (later)
        const potatoType = crop.variety?.potato_type || null;
        let effectiveSowStart = sowStart;
        let effectiveSowEnd   = sowEnd;
        if (sowMethod === "tuber" && potatoType) {
          if (potatoType === "first_early")   { effectiveSowStart = 3; effectiveSowEnd = 4; }
          if (potatoType === "second_early")  { effectiveSowStart = 3; effectiveSowEnd = 5; }
          if (potatoType === "maincrop")      { effectiveSowStart = 4; effectiveSowEnd = 5; }
        }

        // ── User sow preference override ────────────────────────────────────
        // sow_preference = 'outdoors' means user has opted out of indoors recommendation
        const userPreference = crop.sow_preference || null;

        // Determine effective sow method — user preference overrides recommendation
        let effectiveSowMethod = sowMethod;
        if (userPreference === "outdoors" && (sowMethod === "indoors" || sowMethod === "either")) {
          effectiveSowMethod = "outdoors";
          // Outdoor sow window starts later — use direct sow window if available
          const outdoorStart = crop.crop_def?.sow_direct_start ?? sowStart;
          const outdoorEnd   = crop.crop_def?.sow_direct_end   ?? sowEnd;
          effectiveSowStart  = outdoorStart;
          effectiveSowEnd    = outdoorEnd;
        }

        if (effectiveSowStart && effectiveSowEnd && m >= effectiveSowStart && m <= effectiveSowEnd) {

          // ── Frost-aware suppression for outdoor sowing ──────────────────────
          const min7        = weather?.frost_risk_7day ?? null;
          const isOutdoor   = effectiveSowMethod === "outdoors" || effectiveSowMethod === "direct_sow" || effectiveSowMethod === "either";
          const frostHigh   = frostSensitive && isOutdoor && min7 !== null && min7 <= 0;
          const frostMedium = frostSensitive && isOutdoor && min7 !== null && min7 > 0 && min7 <= 3;

          // Hard block: frost forecast and crop is frost-sensitive outdoor sow
          if (frostHigh) continue;

          const logKey  = `${crop.id}:sow_prompt`;
          const lastRun = recentLog.get(logKey);
          const cooldown = frostMedium ? 3 * 86400000 : 7 * 86400000;

          if (!lastRun || (Date.now() - lastRun.getTime()) >= cooldown) {
            let action, urgency, why, sowMeta;

            // ── Month name helpers ───────────────────────────────────────────
            const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
            const windowStr = effectiveSowStart && effectiveSowEnd
              ? `${MONTHS[effectiveSowStart-1]}–${MONTHS[effectiveSowEnd-1]}`
              : null;

            if (effectiveSowMethod === "indoors") {
              // Recommend indoors with clear reasoning
              why    = "Starting indoors now gives stronger plants, an earlier harvest, and better protection from slugs and late frosts.";
              action = `Sow ${crop.name} indoors now — ${why}${windowStr ? ` Sowing window: ${windowStr}.` : ""}`;
              urgency = "medium";
              sowMeta = { status_transition: "sown_indoors", sow_method: "indoors", can_prefer_outdoors: true };

            } else if (effectiveSowMethod === "outdoors" || effectiveSowMethod === "direct_sow") {
              if (userPreference === "outdoors") {
                // User chose outdoors — respect it, note timing
                if (frostMedium) {
                  action  = `Almost time to direct sow ${crop.name} outdoors — frost risk still present, wait for a settled spell.${windowStr ? ` Sowing window: ${windowStr}.` : ""}`;
                  urgency = "low";
                } else {
                  action  = `Time to direct sow ${crop.name} outdoors.${windowStr ? ` Sowing window: ${windowStr}.` : ""}`;
                  urgency = "medium";
                }
              } else {
                if (frostMedium) {
                  action  = `Almost time to direct sow ${crop.name} outdoors — frost risk still marginal, wait for a settled spell.`;
                  urgency = "low";
                } else {
                  action  = `Time to direct sow ${crop.name} outdoors.${windowStr ? ` Sowing window: ${windowStr}.` : ""}`;
                  urgency = "medium";
                }
              }
              sowMeta = { status_transition: "sown", sow_method: "outdoors" };

            } else if (effectiveSowMethod === "tuber") {
              // Potato plant-out
              const typeLabel = potatoType === "first_early" ? "first early"
                              : potatoType === "second_early" ? "second early"
                              : potatoType === "maincrop" ? "maincrop"
                              : null;
              const typeNote  = typeLabel ? ` (${typeLabel})` : "";
              action  = `Plant out ${crop.name}${typeNote} tubers now — chitting should be complete. Earth up as shoots emerge.${windowStr ? ` Plant-out window: ${windowStr}.` : ""}`;
              urgency = "medium";
              sowMeta = { status_transition: "planted_out", sow_method: "tuber" };

            } else {
              // "either" — recommend indoors by default unless user has opted out
              why    = "Starting indoors gives stronger plants and an earlier harvest.";
              action = `Sow ${crop.name} indoors now for best results — ${why}${windowStr ? ` Sowing window: ${windowStr}.` : ""}`;
              urgency = "medium";
              sowMeta = { status_transition: "sown_indoors", sow_method: "indoors", can_prefer_outdoors: true };
            }

            // ── Succession sowing note ───────────────────────────────────────
            // Encourage adding a second instance for crops with a long sow window
            const windowLength = (effectiveSowEnd - effectiveSowStart);
            const successionNote = windowLength >= 2
              ? ` Sowing window runs ${windowStr} — add another ${crop.name} to your garden in a few weeks for a succession harvest.`
              : "";
            if (successionNote && !action.includes("Sowing window")) {
              action = action.trimEnd() + successionNote;
            }

            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action,
              task_type:        "sow",
              urgency,
              due_date:         todayISO(),
              due_window_start: effectiveSowStart ? `${new Date().getFullYear()}-${String(effectiveSowStart).padStart(2,"0")}-01` : null,
              due_window_end:   effectiveSowEnd   ? `${new Date().getFullYear()}-${String(effectiveSowEnd).padStart(2,"0")}-28`   : null,
              source:           "rule_engine",
              rule_id:          "sow_prompt",
              date_confidence:  "exact",
              timing_status:    timingStatus(effectiveSowStart, effectiveSowEnd, weather),
              meta:             JSON.stringify({ ...(sowMeta || { status_transition: "sown", sow_method: effectiveSowMethod }), why: why || null }),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "sow_prompt" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "sow_prompt");
            }
          }
        }
        continue;
      }

      // ── SOWN INDOORS: generate transplant task when frost risk low + window right
      if (cropStatus === "sown_indoors") {
        const txStart = crop.crop_def?.transplant_window_start;
        const txEnd   = crop.crop_def?.transplant_window_end;
        const isTuber = crop.crop_def?.sow_method === "tuber"; // potatoes, etc.

        // ── Chitting tip for tubers (Jan–Feb, while still indoors) ──
        if (isTuber && m >= 1 && m <= 2) {
          const chitKey  = `${crop.id}:chitting_tip`;
          const lastChit = recentLog.get(chitKey);
          if (!lastChit || (Date.now() - lastChit.getTime()) >= 14 * 86400000) {
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action:           `${crop.name} chitting tip — stand seed potatoes rose-end up in egg boxes in a cool, light frost-free spot. Short stubby chits (2cm) are ideal before planting out`,
              task_type:        "info",
              urgency:          "low",
              due_date:         todayISO(),
              source:           "rule_engine",
              rule_id:          "chitting_tip",
              date_confidence:  "approximate",
              meta:             JSON.stringify({ tip: true }),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "chitting_tip" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "chitting_tip");
            }
          }
        }

        // ── Plant out task (transplant window, frost-aware) ──
        if (txStart && txEnd && m >= txStart && m <= txEnd) {
          const frostRisk  = weather?.frost_risk === true;
          const logKey     = `${crop.id}:transplant_prompt`;
          const lastRun    = recentLog.get(logKey);
          const cooldown   = frostRisk ? 3 * 86400000 : 7 * 86400000;
          if (!lastRun || (Date.now() - lastRun.getTime()) >= cooldown) {
            let action;
            if (isTuber) {
              action = frostRisk
                ? `${crop.name} are ready to plant out but frost is forecast — hold off a few more days to protect emerging shoots`
                : `Time to plant out your ${crop.name} — chits look good, frosts should now be clear. Plant 10–15cm deep, 30cm apart`;
            } else {
              action = frostRisk
                ? `${crop.name} is ready to transplant but frost is forecast — hold off a few more days`
                : `Time to transplant ${crop.name} outdoors — frosts should now be clear`;
            }
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action,
              task_type:        "transplant",
              urgency:          frostRisk ? "low" : "medium",
              due_date:         todayISO(),
              due_window_start: txStart ? `${new Date().getFullYear()}-${String(txStart).padStart(2,"0")}-01` : null,
              due_window_end:   txEnd   ? `${new Date().getFullYear()}-${String(txEnd).padStart(2,"0")}-28`   : null,
              timing_status:    timingStatus(txStart, txEnd, weather),
              source:           "rule_engine",
              rule_id:          "transplant_prompt",
              date_confidence:  "exact",
              meta:             JSON.stringify({ status_transition: "transplanted" }),
            };
            newTasks.push({ ...task, crop_name: crop.name, rule_id: "transplant_prompt" });
            if (!this.dryRun && this.supabase) {
              await this._persistTaskWithKey(task, crop, "transplant_prompt");
            }
          }
        }
        continue; // sown_indoors crops only get transplant prompts for now
      }

      // ── GROWING CROPS: run normal rules ──────────────────────────────────────
      // Only infer stage from sow date for seed-grown crops.
      // Perennial/vegetative establishments (runner, tuber, crown, cane) keep
      // whatever stage is stored in the database — inferring from sow_date
      // would incorrectly reset them to 'seed'.
      const vegEstablishments = ["runner", "tuber", "crown", "cane"];
      const useStoredStage = vegEstablishments.includes(crop.crop_def?.default_establishment);
      if (!useStoredStage) {
        crop.stage = inferStage(crop, effective);
      }

      for (const rule of rules) {
        // 1. Crop match — NULL means applies to all crops
        if (rule.crop_def_id && rule.crop_def_id !== crop.crop_def_id) continue;

        // 2. Area type match — NULL means applies to all area types
        if (rule.area_type && rule.area_type !== areaType) continue;

        // 3. Stage match — NULL means applies to all stages
        if (rule.stage && rule.stage !== crop.stage) continue;

        // 4. Skip finished crops
        if (crop.stage === "finished") continue;

        // 5. Cooldown — use per-rule cooldown_days
        const logKey    = `${crop.id}:${rule.rule_id}`;
        const lastRun   = recentLog.get(logKey);
        const cooldown  = (rule.cooldown_days || 3) * 86400000;
        if (lastRun && (Date.now() - lastRun.getTime()) < cooldown) continue;

        // 6. Evaluate condition
        const evaluator = CONDITIONS[rule.condition_type];
        if (!evaluator) {
          console.warn(`[RuleEngine] Unknown condition: ${rule.condition_type}`);
          continue;
        }

        if (!evaluator(crop, rule.condition_value, context, effective)) continue;

        // 7. Build and persist task
        const confidence = (crop.start_date_confidence === "unknown" ||
                            crop.stage_confidence       === "inferred")
                           ? "estimated" : "exact";

        // For feed tasks — personalise action with user's matched feed
        let action = rule.action;
        if (rule.task_type === "feed") {
          const cropFeedType = crop.crop_def?.feed_type;
          const matchedFeed  = this._matchFeed(cropFeedType, userFeeds);
          if (matchedFeed) {
            const productLabel = [matchedFeed.brand, matchedFeed.product_name].filter(Boolean).join(" ");
            let dosageNote = "";
            if (matchedFeed.form === "liquid" && matchedFeed.dilution_ml_per_litre) {
              dosageNote = ` — ${matchedFeed.dilution_ml_per_litre}ml per litre of water`;
            } else if (matchedFeed.form === "granular" || matchedFeed.form === "powder") {
              dosageNote = matchedFeed.notes ? ` — follow pack instructions` : "";
            }
            action = `Time to feed your ${crop.name} with ${productLabel}${dosageNote}`;
          }
        }

        const task = {
          user_id:          crop.user_id,
          crop_instance_id: crop.id,
          area_id:          crop.area_id,
          action,
          task_type:        rule.task_type,
          urgency:          rule.urgency,
          due_date:         todayISO(),
          timing_status:    "peak", // general rules fire when conditions are met = peak
          source:           "rule_engine",
          rule_id:          rule.rule_id,
          date_confidence:  confidence,
        };

        newTasks.push({ ...task, crop_name: crop.name, rule_id: rule.rule_id });

        if (!this.dryRun && this.supabase) {
          await this._persistTask(task, crop, rule);
        }
      }
    }

    return newTasks;
  }

  // ── Persist task + log entry ────────────────────────────────────────────────

  async _persistTask(task, crop, rule) {
    try {
      const { data: inserted, error } = await this.supabase
        .from("tasks")
        .insert(task)
        .select("id")
        .single();
      if (error) throw error;
      await this.supabase.from("rule_log").insert({
        crop_instance_id: crop.id,
        rule_id:          rule.rule_id,
        task_id:          inserted.id,
      });
    } catch (err) {
      console.error("[RuleEngine] Persist error:", err.message);
    }
  }

  async _persistTaskWithKey(task, crop, ruleKey) {
    try {
      // Dedup check — don't insert if any task with same rule_id exists for this crop
      const { data: existingTasks } = await this.supabase
        .from("tasks")
        .select("id")
        .eq("crop_instance_id", crop.id)
        .eq("rule_id", ruleKey)
        .limit(1);
      const existing = existingTasks?.[0] || null;
      if (existing) {
        console.log(`[RuleEngine] Skipping duplicate task ${ruleKey} for ${crop.name}`);
        return;
      }

      const { data: inserted, error } = await this.supabase
        .from("tasks")
        .insert(task)
        .select("id")
        .single();
      if (error) throw error;
      await this.supabase.from("rule_log").insert({
        crop_instance_id: crop.id,
        rule_id:          ruleKey,
        task_id:          inserted.id,
      });
    } catch (err) {
      console.error("[RuleEngine] Persist error:", err.message);
    }
  }

  // ── Feed matching ────────────────────────────────────────────────────────────
  // Match a crop's feed_type to the best user feed available.
  // Returns the best matching feed or null if none found.

  _matchFeed(cropFeedType, userFeeds) {
    if (!cropFeedType || !userFeeds?.length) return null;

    // Normalise crop feed type to a keyword list for fuzzy matching
    const cropKeywords = cropFeedType.toLowerCase();

    // Score each feed — higher = better match
    const scored = userFeeds.map(feed => {
      let score = 0;
      const feedType = (feed.feed_type || "").toLowerCase();
      const suitableTypes = feed.suitable_crop_types || [];

      // Exact feed_type match is best
      if (feedType.includes("high_potash") && cropKeywords.includes("potash")) score += 10;
      if (feedType.includes("high_nitrogen") && cropKeywords.includes("nitrogen")) score += 10;
      if (feedType.includes("balanced") && cropKeywords.includes("balanced")) score += 10;
      if (feedType.includes("specialist_tomato") && cropKeywords.includes("potash")) score += 15;
      if (feedType.includes("seaweed")) score += 2; // seaweed is generally beneficial
      if (feedType.includes("organic_general")) score += 3;

      // Partial keyword overlaps
      if (cropKeywords.includes("potash") && feedType.includes("potash")) score += 5;
      if (cropKeywords.includes("general") && feedType.includes("balanced")) score += 5;

      return { feed, score };
    }).filter(s => s.score > 0);

    if (!scored.length) return null;
    scored.sort((a, b) => b.score - a.score);
    return scored[0].feed;
  }

  // ── Cleanup ─────────────────────────────────────────────────────────────────

  async _cleanupOrphanedTasks(userId) {
    try {
      // Get all active crop instance IDs for this user
      const { data: activeCrops, error: cropError } = await this.supabase
        .from("crop_instances")
        .select("id")
        .eq("user_id", userId)
        .eq("active", true);

      // Safety — if the crop query failed or returned nothing, don't delete anything
      if (cropError || !activeCrops) {
        console.log("[RuleEngine] Skipping cleanup — could not load crops");
        return;
      }

      const activeIds = activeCrops.map(c => c.id);

      // If user has no crops at all, skip cleanup entirely
      if (activeIds.length === 0) {
        console.log("[RuleEngine] Skipping cleanup — no active crops found");
        return;
      }

      // Find incomplete tasks whose crop_instance_id is not in the active list
      // Never delete completed tasks — they're needed for metrics history
      const { data: orphanTasks } = await this.supabase
        .from("tasks")
        .select("id, crop_instance_id")
        .eq("user_id", userId)
        .is("completed_at", null)
        .not("crop_instance_id", "in", `(${activeIds.join(",")})`)

      if (orphanTasks?.length) {
        const orphanIds = orphanTasks.map(t => t.id);
        await this.supabase.from("rule_log").delete().in("task_id", orphanIds);
        await this.supabase.from("tasks").delete().in("id", orphanIds);
        console.log(`[RuleEngine] Cleaned up ${orphanIds.length} orphaned tasks`);
      }
    } catch (err) {
      console.error("[RuleEngine] Cleanup error:", err.message);
    }
  }

  // ── Data loaders ────────────────────────────────────────────────────────────

  async _loadCrops(userId) {
    if (!this.supabase) return [];
    const { data, error } = await this.supabase
      .from("crop_instances")
      .select(`
        *,
        area:area_id ( type, location_id ),
        crop_def:crop_def_id (
          is_perennial, frost_sensitive, sow_method,
          days_to_maturity_min, days_to_maturity_max,
          feed_interval_days, pest_window_start, pest_window_end,
          sow_window_start, sow_window_end,
          transplant_window_start, transplant_window_end,
          harvest_month_start, harvest_month_end, feed_type
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
      .from("crop_rules")
      .select("*")
      .eq("active", true)
      .order("priority_score", { ascending: false });
    if (error) throw error;
    return data || [];
  }

  async _loadWeatherByLocation(userId) {
    if (!this.supabase) return {};
    const { data: locations } = await this.supabase
      .from("locations")
      .select("id, postcode")
      .eq("user_id", userId);

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
    const { data } = await this.supabase
      .from("environment_modifiers")
      .select("*");
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
      .eq("user_id", userId)
      .eq("active", true)
      .eq("enriched", true);
    return data || [];
  }

  async _loadRuleLog(userId) {
    if (!this.supabase) return new Map();
    const cropIds = await this._getUserCropIds(userId);
    if (!cropIds.length) return new Map();
    const { data } = await this.supabase
      .from("rule_log")
      .select("crop_instance_id, rule_id, triggered_at")
      .in("crop_instance_id", cropIds);
    const log = new Map();
    for (const row of data || []) {
      const key = `${row.crop_instance_id}:${row.rule_id}`;
      const ts  = new Date(row.triggered_at);
      if (!log.has(key) || ts > log.get(key)) log.set(key, ts);
    }
    return log;
  }

  async _getUserCropIds(userId) {
    const { data } = await this.supabase
      .from("crop_instances")
      .select("id")
      .eq("user_id", userId)
      .eq("active", true);
    return (data || []).map(c => c.id);
  }
}

module.exports = { RuleEngine, resolveEffectiveValues, inferStage, daysSince };