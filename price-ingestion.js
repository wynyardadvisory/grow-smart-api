"use strict";

const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

const supabase        = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const PEPESTO_API_KEY = process.env.PEPESTO_API_KEY;
const PEPESTO_BASE    = "https://s.pepesto.com/api";

// One catalog fetch per retailer — we filter locally. 3 requests/week total.
const RETAILERS = ["tesco.com", "sainsburys.co.uk", "asda.com"];

// Fetch the full catalog for a supermarket domain
async function fetchCatalog(supermarket_domain) {
  const res = await fetch(`${PEPESTO_BASE}/catalog`, {
    method:  "POST",
    headers: {
      "Content-Type":  "application/json",
      "Authorization": `Bearer ${PEPESTO_API_KEY}`,
    },
    body: JSON.stringify({ supermarket_domain }),
  });
  if (!res.ok) {
    console.error(`[Pepesto] catalog ${supermarket_domain} → ${res.status} ${await res.text()}`);
    return {};
  }
  return await res.json();
}

// Match a catalog product against a produce_item's include/exclude terms
function matchesItem(productName, includeTerms, excludeTerms) {
  const name = productName.toLowerCase();
  if (includeTerms.length && !includeTerms.some(t => name.includes(t.toLowerCase()))) return false;
  if (excludeTerms.some(t => name.includes(t.toLowerCase()))) return false;
  return true;
}

// Normalize a matched catalog product into a snapshot row
function normalizeProduct(product, item, retailer) {
  try {
    const priceGbp = product.price / 100; // Pepesto returns pence/cents
    if (!priceGbp || priceGbp <= 0) return null;

    const rawName = (
      (product.names && product.names.en) ||
      product.entity_name ||
      ""
    ).toLowerCase();

    let pricePerKg   = null;
    let pricePerUnit = null;

    const kgStr = product.price_per_meausure_unit || product.price_per_measure_unit || "";
    if      (kgStr.includes("/100g")) pricePerKg = parseFloat(kgStr) * 10;
    else if (kgStr.includes("/kg"))   pricePerKg = parseFloat(kgStr);

    // Fallback: derive from quantity if Pepesto gives us HundredGrams
    if (!pricePerKg && product.quantity && product.quantity.Unit && product.quantity.Unit.HundredGrams) {
      const hg = product.quantity.Unit.HundredGrams;
      if (hg > 0) pricePerKg = priceGbp / (hg / 10);
    }

    if (["each", "bunch", "pack"].includes(item.unit_basis)) {
      pricePerUnit = priceGbp;
    }

    // Sanity bounds — discard obvious noise
    if (pricePerKg   !== null && (pricePerKg   < 0.20 || pricePerKg   > 25)) return null;
    if (pricePerUnit !== null && (pricePerUnit  < 0.10 || pricePerUnit > 10)) return null;
    if (!pricePerKg && !pricePerUnit) return null;

    return {
      retail_key:          item.retail_key,
      retailer,
      country_code:        "GB",
      captured_at:         new Date().toISOString(),
      product_name:        rawName,
      pack_size_raw:       product.quantity_str || "",
      price_gbp:           priceGbp,
      price_per_kg_gbp:    pricePerKg,
      price_per_unit_gbp:  pricePerUnit,
      promo_flag:          !!(product.promo || product.promotion || product.was_price),
      confidence_score:    pricePerKg ? 0.9 : 0.7,
      source_payload_json: product,
    };
  } catch (e) {
    console.error("[Normalize]", e.message);
    return null;
  }
}

function median(arr) {
  if (!arr.length) return null;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function trimmedMean(arr) {
  if (arr.length < 4) return median(arr);
  const s    = [...arr].sort((a, b) => a - b);
  const trim = Math.max(1, Math.floor(s.length * 0.1));
  const t    = s.slice(trim, s.length - trim);
  return t.reduce((a, b) => a + b, 0) / t.length;
}

async function runPriceIngestion() {
  console.log("[PriceIngestion] Starting weekly run");

  // Load all active produce items with their match rules
  const { data: items, error } = await supabase
    .from("produce_items")
    .select("*")
    .eq("active", true);

  if (error || !items?.length) {
    console.error("[PriceIngestion] Failed to load produce_items:", error?.message);
    return { ok: false };
  }

  // Pre-parse match terms for all items
  const parsedItems = items.map(item => ({
    ...item,
    _includeTerms: JSON.parse(item.match_include_terms_json || "[]"),
    _excludeTerms: JSON.parse(item.match_exclude_terms_json || "[]"),
  }));

  const monthKey       = new Date().toISOString().slice(0, 7);
  let   totalSnapshots = 0;
  let   totalAggregates = 0;

  // Collect snapshots per retail_key across all retailers
  const snapshotsByKey = {}; // retail_key → snapshot[]

  for (const retailer of RETAILERS) {
    console.log(`[PriceIngestion] Fetching catalog: ${retailer}`);
    const catalog = await fetchCatalog(retailer);

    // Pepesto wraps products under parsed_products key
    const products = Object.values(catalog.parsed_products || catalog || {});
    console.log(`[PriceIngestion] ${retailer}: ${products.length} products in catalog`);

    for (const product of products) {
      const productName = (
        (product.names && product.names.en) ||
        product.entity_name ||
        ""
      );
      if (!productName) continue;

      for (const item of parsedItems) {
        if (!matchesItem(productName, item._includeTerms, item._excludeTerms)) continue;

        const snapshot = normalizeProduct(product, item, retailer);
        if (!snapshot) continue;

        if (!snapshotsByKey[item.retail_key]) snapshotsByKey[item.retail_key] = [];
        snapshotsByKey[item.retail_key].push(snapshot);
      }
    }

    // Small pause between retailer calls
    await new Promise(r => setTimeout(r, 500));
  }

  // Now store snapshots and compute aggregates per retail_key
  for (const item of parsedItems) {
    const snapshots = snapshotsByKey[item.retail_key] || [];

    if (!snapshots.length) {
      console.log(`[PriceIngestion] No matches: ${item.retail_key}`);
      continue;
    }

    // Insert snapshots
    const { error: se } = await supabase
      .from("produce_price_snapshots")
      .insert(snapshots);
    if (se) console.error(`[PriceIngestion] Snapshot insert error ${item.retail_key}:`, se.message);
    else totalSnapshots += snapshots.length;

    // Compute aggregates
    const nonPromoKg   = snapshots.filter(s => !s.promo_flag && s.price_per_kg_gbp).map(s => s.price_per_kg_gbp);
    const nonPromoUnit = snapshots.filter(s => !s.promo_flag && s.price_per_unit_gbp).map(s => s.price_per_unit_gbp);
    const allKg        = snapshots.filter(s => s.price_per_kg_gbp).map(s => s.price_per_kg_gbp);
    const allUnit      = snapshots.filter(s => s.price_per_unit_gbp).map(s => s.price_per_unit_gbp);

    const agg = {
      retail_key:                      item.retail_key,
      country_code:                    "GB",
      month_key:                       monthKey,
      median_price_per_kg_gbp:         median(allKg),
      median_price_per_unit_gbp:       median(allUnit),
      trimmed_mean_price_per_kg_gbp:   trimmedMean(nonPromoKg.length >= 3 ? nonPromoKg : allKg),
      trimmed_mean_price_per_unit_gbp: trimmedMean(nonPromoUnit.length >= 3 ? nonPromoUnit : allUnit),
      sample_count:                    snapshots.length,
      confidence_score:                snapshots.length >= 5 ? 0.9 : snapshots.length >= 2 ? 0.7 : 0.5,
      freshness_days:                  0,
      computed_at:                     new Date().toISOString(),
    };

    const { error: ae } = await supabase
      .from("produce_price_aggregates")
      .upsert(agg, { onConflict: "retail_key,country_code,month_key" });

    if (ae) console.error(`[PriceIngestion] Aggregate error ${item.retail_key}:`, ae.message);
    else {
      totalAggregates++;
      console.log(`[PriceIngestion] ${item.retail_key}: ${snapshots.length} matches, kg=£${agg.trimmed_mean_price_per_kg_gbp?.toFixed(2)}`);
    }
  }

  console.log(`[PriceIngestion] Done. ${totalSnapshots} snapshots, ${totalAggregates} aggregates`);
  return { ok: true, snapshots: totalSnapshots, aggregates: totalAggregates };
}

module.exports = { runPriceIngestion };