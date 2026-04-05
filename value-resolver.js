"use strict";

async function resolveShopValue(supabase, cropDefId, expectedYieldKg, harvestMonthKey) {
  if (!cropDefId || !expectedYieldKg || !harvestMonthKey) return null;

  const { data: item } = await supabase
    .from("produce_items")
    .select("retail_key, unit_basis, typical_unit_weight_kg, typical_bunch_weight_kg, typical_pack_weight_kg")
    .eq("crop_def_id", cropDefId)
    .eq("active", true)
    .single();
  if (!item) return null;

  const [yr, mo] = harvestMonthKey.split("-").map(Number);
  const mkOf = (y, m) => {
    const mm = ((m-1+12)%12)+1;
    const yy = y + Math.floor((m-1)/12);
    return `${yy}-${String(mm).padStart(2,"0")}`;
  };
  const candidates = [
    harvestMonthKey,
    mkOf(yr, mo-1), mkOf(yr, mo+1),
    mkOf(yr, mo-2), mkOf(yr, mo+2),
  ];

  let agg = null;
  for (const mk of candidates) {
    const { data } = await supabase
      .from("produce_price_aggregates")
      .select("*")
      .eq("retail_key", item.retail_key)
      .eq("country_code", "GB")
      .eq("month_key", mk)
      .single();
    if (data) { agg = data; break; }
  }
  if (!agg) return null;

  const pricePerKg   = agg.trimmed_mean_price_per_kg_gbp   || agg.median_price_per_kg_gbp;
  const pricePerUnit = agg.trimmed_mean_price_per_unit_gbp  || agg.median_price_per_unit_gbp;

  let resolvedValue = null;

  if (item.unit_basis === "kg" && pricePerKg) {
    resolvedValue = expectedYieldKg * pricePerKg;
  } else if (["each","bunch","pack"].includes(item.unit_basis) && pricePerUnit) {
    const unitWeightKg =
      item.unit_basis === "each"  ? item.typical_unit_weight_kg  :
      item.unit_basis === "bunch" ? item.typical_bunch_weight_kg :
      item.unit_basis === "pack"  ? item.typical_pack_weight_kg  : null;
    if (unitWeightKg && unitWeightKg > 0) {
      resolvedValue = (expectedYieldKg / unitWeightKg) * pricePerUnit;
    } else if (pricePerKg) {
      resolvedValue = expectedYieldKg * pricePerKg;
    }
  } else if (pricePerKg) {
    resolvedValue = expectedYieldKg * pricePerKg;
  }

  if (!resolvedValue) return null;

  return {
    value_gbp:        Math.round(resolvedValue * 100) / 100,
    confidence_score: agg.confidence_score,
    month_key:        agg.month_key,
    retail_key:       item.retail_key,
    price_basis:      pricePerKg ? `£${pricePerKg.toFixed(2)}/kg` : `£${pricePerUnit?.toFixed(2)}/unit`,
  };
}

module.exports = { resolveShopValue };