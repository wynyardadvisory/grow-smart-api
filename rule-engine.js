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

// ── Stage inference ───────────────────────────────────────────────────────────
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

    const [crops, rules, weatherByLocation, recentLog, envModifiers] = await Promise.all([
      this._loadCrops(userId),
      this._loadRules(),
      this._loadWeatherByLocation(userId),
      this._loadRuleLog(userId),
      this._loadEnvModifiers(),
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

      // ── PLANNED CROPS: generate sow prompt when in sow window ────────────────
      if (cropStatus === "planned") {
        // Prefer variety-level sow window if set (e.g. late maincrop vs early variety)
        const sowStart       = crop.variety?.sow_window_start       ?? crop.crop_def?.sow_window_start;
        const sowEnd         = crop.variety?.sow_window_end         ?? crop.crop_def?.sow_window_end;
        const frostSensitive = effective.frost_sensitive;

        if (sowStart && sowEnd && m >= sowStart && m <= sowEnd) {

          // ── Frost-aware suppression for outdoor sowing ──────────────────────
          // frost_risk_7day is the actual minimum °C forecast over 7 days.
          // We use it to suppress or warn on outdoor sowing for frost-sensitive crops.
          // Indoors sowing is never blocked by frost.
          const min7        = weather?.frost_risk_7day ?? null;
          const isOutdoor   = sowMethod === "outdoors" || sowMethod === "either";
          const frostHigh   = frostSensitive && isOutdoor && min7 !== null && min7 <= 0;
          const frostMedium = frostSensitive && isOutdoor && min7 !== null && min7 > 0 && min7 <= 3;

          // Hard block: frost forecast and crop is frost-sensitive outdoor sow
          if (frostHigh) continue; // suppress entirely — try again on daily cron

          const logKey  = `${crop.id}:sow_prompt`;
          const lastRun = recentLog.get(logKey);
          // Re-prompt sooner when frost is marginal so user gets updated once it clears
          const cooldown = frostMedium ? 3 * 86400000 : 7 * 86400000;

          if (!lastRun || (Date.now() - lastRun.getTime()) >= cooldown) {
            let action, urgency;

            if (sowMethod === "indoors") {
              action  = `Time to sow ${crop.name} indoors — start on a windowsill or in the greenhouse`;
              urgency = "medium";
            } else if (sowMethod === "outdoors") {
              if (frostMedium) {
                action  = `Almost time to direct sow ${crop.name} outdoors — frost risk still marginal, wait for a settled spell`;
                urgency = "low";
              } else {
                action  = `Time to direct sow ${crop.name} outdoors`;
                urgency = "medium";
              }
            } else {
              // either — always offer indoors option even if outdoor is marginal
              if (frostMedium) {
                action  = `Time to sow ${crop.name} — sow indoors now or wait a little longer before direct sowing outdoors`;
                urgency = "medium";
              } else {
                action  = `Time to sow ${crop.name} — sow indoors for an earlier start or direct sow outdoors`;
                urgency = "medium";
              }
            }

            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action,
              task_type:        "sow",
              urgency,
              due_date:         todayISO(),
              source:           "rule_engine",
              rule_id:          "sow_prompt",
              date_confidence:  "exact",
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
        if (txStart && txEnd && m >= txStart && m <= txEnd) {
          const frostRisk  = weather?.frost_risk === true;
          const logKey     = `${crop.id}:transplant_prompt`;
          const lastRun    = recentLog.get(logKey);
          const cooldown   = frostRisk ? 3 * 86400000 : 7 * 86400000;
          if (!lastRun || (Date.now() - lastRun.getTime()) >= cooldown) {
            const action = frostRisk
              ? `${crop.name} is ready to transplant but frost is forecast — hold off a few more days`
              : `Time to transplant ${crop.name} outdoors — frosts should now be clear`;
            const task = {
              user_id:          crop.user_id,
              crop_instance_id: crop.id,
              area_id:          crop.area_id,
              action,
              task_type:        "transplant",
              urgency:          frostRisk ? "low" : "medium",
              due_date:         todayISO(),
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
      // Skip crops with no sow date for rules that need it
      crop.stage = inferStage(crop, effective);

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

        const task = {
          user_id:          crop.user_id,
          crop_instance_id: crop.id,
          area_id:          crop.area_id,
          action:           rule.action,
          task_type:        rule.task_type,
          urgency:          rule.urgency,
          due_date:         todayISO(),
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

  // ── Cleanup ─────────────────────────────────────────────────────────────────

  async _cleanupOrphanedTasks(userId) {
    try {
      // Get all active crop instance IDs for this user
      const { data: activeCrops } = await this.supabase
        .from("crop_instances")
        .select("id")
        .eq("user_id", userId)
        .eq("active", true);

      const activeIds = (activeCrops || []).map(c => c.id);

      // Find tasks whose crop_instance_id is not in the active list
      const { data: orphanTasks } = await this.supabase
        .from("tasks")
        .select("id, crop_instance_id")
        .eq("user_id", userId)
        .not("crop_instance_id", "in", `(${activeIds.length ? activeIds.map(id => `"${id}"`).join(",") : '"00000000-0000-0000-0000-000000000000"'})`);

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
          transplant_window_start, transplant_window_end
        ),
        variety:variety_id (
          days_to_maturity_min, days_to_maturity_max,
          frost_sensitive_override, feed_interval_days_override,
          pest_window_start_override, pest_window_end_override,
          sow_window_start, sow_window_end,
          transplant_window_start, transplant_window_end
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