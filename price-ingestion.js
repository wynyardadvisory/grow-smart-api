"use strict";

const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

const supabase        = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const PEPESTO_API_KEY = process.env.PEPESTO_API_KEY;
const PEPESTO_BASE    = "https://www.pepesto.com/api";
const RETAILERS       = ["tesco.com", "sainsburys.co.uk", "asda.com"];

async function pepestoSearch(supermarket_domain, query) {
  const res = await fetch(`${PEPESTO_BASE}/search`, {
    method:  "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${PEPESTO_API_KEY}` },
    body:    JSON.stringify({ supermarket_domain, query }),
  });
  if (!res.ok) { console.error(`[Pepesto] ${supermarket_domain} "${query}" → ${res.status}`); return []; }
  const data = await res.json();
  return data.products || data.results || Object.values(data) || [];
}

function normalizeProduct(product, item) {
  try {
    const priceGbp = product.price / 100;
    if (!priceGbp || priceGbp <= 0) return null;

    const rawName      = (product.names?.en || product.entity_name || "").toLowerCase();
    const includeTerms = JSON.parse(item.match_include_terms_json || "[]");
    const excludeTerms = JSON.parse(item.match_exclude_terms_json || "[]");

    if (includeTerms.length && !includeTerms.some(t => rawName.includes(t.toLowerCase()))) return null;
    if (excludeTerms.some(t => rawName.includes(t.toLowerCase()))) return null;

    let pricePerKg = null, pricePerUnit = null;
    const kgStr = product.price_per_meausure_unit || product.price_per_measure_unit || "";

    if      (kgStr.includes("/100g")) pricePerKg = parseFloat(kgStr) * 10;
    else if (kgStr.includes("/kg"))   pricePerKg = parseFloat(kgStr);

    if (!pricePerKg && product.quantity?.Unit?.HundredGrams) {
      pricePerKg = priceGbp / (product.quantity.Unit.HundredGrams / 10);
    }

    if (["each","bunch","pack"].includes(item.unit_basis)) pricePerUnit = priceGbp;

    if (pricePerKg   !== null && (pricePerKg   < 0.20 || pricePerKg   > 25)) return null;
    if (pricePerUnit !== null && (pricePerUnit  < 0.10 || pricePerUnit > 10)) return null;

    return {
      price_gbp:           priceGbp,
      price_per_kg_gbp:    pricePerKg,
      price_per_unit_gbp:  pricePerUnit,
      product_name:        rawName,
      pack_size_raw:       product.quantity_str || "",
      promo_flag:          !!(product.promo || product.promotion || product.was_price),
      confidence_score:    pricePerKg ? 0.9 : 0.7,
      source_payload_json: product,
    };
  } catch(e) { console.error("[Normalize]", e.message); return null; }
}

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a,b) => a-b);
  const m = Math.floor(s.length/2);
  return s.length%2 ? s[m] : (s[m-1]+s[m])/2;
}

function trimmedMean(arr) {
  if (arr.length < 4) return median(arr);
  const s    = [...arr].sort((a,b) => a-b);
  const trim = Math.max(1, Math.floor(s.length*0.1));
  const t    = s.slice(trim, s.length-trim);
  return t.reduce((a,b) => a+b, 0) / t.length;
}

async function runPriceIngestion() {
  console.log("[PriceIngestion] Starting weekly run");

  const { data: items, error } = await supabase
    .from("produce_items").select("*").eq("active", true);
  if (error || !items?.length) {
    console.error("[PriceIngestion] Failed to load produce_items:", error?.message);
    return { ok: false };
  }

  const monthKey = new Date().toISOString().slice(0,7);
  let totalSnapshots = 0, totalAggregates = 0;

  for (const item of items) {
    const searchTerms = JSON.parse(item.search_terms_json || "null") || [item.display_name];
    const allSnapshots = [];

    for (const retailer of RETAILERS) {
      for (const term of searchTerms.slice(0,2)) {
        const products = await pepestoSearch(retailer, term);
        for (const product of products.slice(0,10)) {
          const n = normalizeProduct(product, item);
          if (!n) continue;
          allSnapshots.push({
            retail_key:   item.retail_key,
            retailer,
            country_code: "GB",
            captured_at:  new Date().toISOString(),
            ...n,
          });
        }
        await new Promise(r => setTimeout(r, 300));
      }
    }

    if (!allSnapshots.length) {
      console.log(`[PriceIngestion] No data: ${item.retail_key}`);
      continue;
    }

    const { error: se } = await supabase
      .from("produce_price_snapshots").insert(allSnapshots);
    if (se) console.error(`[PriceIngestion] Snapshot error ${item.retail_key}:`, se.message);
    else totalSnapshots += allSnapshots.length;

    const nonPromoKg   = allSnapshots.filter(s => !s.promo_flag && s.price_per_kg_gbp).map(s => s.price_per_kg_gbp);
    const nonPromoUnit = allSnapshots.filter(s => !s.promo_flag && s.price_per_unit_gbp).map(s => s.price_per_unit_gbp);
    const allKg        = allSnapshots.filter(s => s.price_per_kg_gbp).map(s => s.price_per_kg_gbp);
    const allUnit      = allSnapshots.filter(s => s.price_per_unit_gbp).map(s => s.price_per_unit_gbp);

    const agg = {
      retail_key:                      item.retail_key,
      country_code:                    "GB",
      month_key:                       monthKey,
      median_price_per_kg_gbp:         median(allKg),
      median_price_per_unit_gbp:       median(allUnit),
      trimmed_mean_price_per_kg_gbp:   trimmedMean(nonPromoKg.length>=3 ? nonPromoKg : allKg),
      trimmed_mean_price_per_unit_gbp: trimmedMean(nonPromoUnit.length>=3 ? nonPromoUnit : allUnit),
      sample_count:                    allSnapshots.length,
      confidence_score:                allSnapshots.length>=5 ? 0.9 : allSnapshots.length>=2 ? 0.7 : 0.5,
      freshness_days:                  0,
      computed_at:                     new Date().toISOString(),
    };

    const { error: ae } = await supabase
      .from("produce_price_aggregates")
      .upsert(agg, { onConflict: "retail_key,country_code,month_key" });
    if (ae) console.error(`[PriceIngestion] Aggregate error ${item.retail_key}:`, ae.message);
    else {
      totalAggregates++;
      console.log(`[PriceIngestion] ${item.retail_key}: ${allSnapshots.length} snapshots, kg=£${agg.trimmed_mean_price_per_kg_gbp?.toFixed(2)}`);
    }
  }

  console.log(`[PriceIngestion] Done. ${totalSnapshots} snapshots, ${totalAggregates} aggregates`);
  return { ok: true, snapshots: totalSnapshots, aggregates: totalAggregates };
}

module.exports = { runPriceIngestion };