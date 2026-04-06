// Workers/hubspot-sync.js
const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, ".env") });
const { createClient } = require("@supabase/supabase-js");

const HUBSPOT_OBJECT_TYPE_ID = (process.env.HUBSPOT_OBJECT_TYPE_ID || "2-35904844").trim();
const HUBSPOT_TOKEN = (process.env.HUBSPOT_TOKEN || "").trim();
const SUPABASE_URL = (process.env.SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE_KEY = (process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();

const PROPERTIES = [
  "active_concurrent_playback", "all_tiers_enabled", "badge_redeemed", "badge_redeemed_date", 
  "campaign", "cancel_reason_category", "cancel_reason_long", "city", "come_back_reason", 
  "content_enjoyed_the_most", "continues_customer_days", "converted_trial", "country", 
  "coupon_code", "coupon_code_id", "current_plan", "customer_created_at", "customer_id", 
  "date_became_enabled", "date_last_canceled", "decision_timeline", 
  "enamel_pin_coupon_id", "event_created_at", "expiration_date", "first_name", 
  "free_tier_enabled", "free_tier_health", "free_tier_health_status", "frequency", 
  "frustrations", "has_canceled", "health_score", "health_score_status", "hs_createdate", 
  "hs_lastmodifieddate", "hs_object_source_label", "hs_object_source_detail_1", "hs_pipeline", 
  "hs_pipeline_stage", "initial_discovery", "last_name", "last_payment_date", 
  "last_video_started_date", "lifetime_value_workflow", "marketing_opt_in", 
  "master_record_association", "max_concurrent_playbacks", "offering", "paid_months", 
  "particularly_excited", "platform", "previous_vimeo_product_id", "previous_vimeo_status", 
  "product_change_date", "product_id", "product_name", "promotion_code", "referrer", 
  "standard_tier_enabled", "standard_tier_monthly_enabled", "standard_tier_yearly_enabled", 
  "state", "status", "status_change_date", "subscribe_count", "subscription_currency", 
  "subscription_price", "subscription_type", "time_since_payment", "trial_end_date", 
  "trial_started_date", "unsubscribe_count", "utm_campaign", "utm_medium", "utm_source", 
  "vimeo_customer_id", "vimeo_email", "vimeo_old_last_payment_date", 
  "vimeo_subscription_price_usd_calc", "will_you_come_back", "worth_the_price"
];

function parseNumber(value) {
  if (!value || !String(value).trim()) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseTimestamp(value) {
  if (!value || !String(value).trim()) return null;
  const trimmed = String(value).trim();
  if (/^\d{12,}$/.test(trimmed)) {
    const epochMs = Number(trimmed);
    if (Number.isFinite(epochMs)) return new Date(epochMs).toISOString();
  }
  const d = new Date(trimmed);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function getProperty(record, name) {
  const raw = record.properties?.[name];
  if (raw == null) return null;
  const cleaned = String(raw).trim();
  return cleaned.length ? cleaned : null;
}

function transform(record) {
  const g = (name) => getProperty(record, name);
  return {
    record_id: record.id,
    vimeo_customer_id: g("customer_id"),
    vimeo_email: g("vimeo_email"),
    first_name: g("first_name"),
    last_name: g("last_name"),
    current_plan: g("current_plan"),
    product_name: g("product_name"),
    product_id: g("product_id"),
    offering: g("offering"),
    frequency: g("frequency"),
    platform: g("platform"),
    subscription_type: g("subscription_type"),
    subscription_currency: g("subscription_currency"),
    subscription_price: parseNumber(g("subscription_price")),
    vimeo_subscription_price_usd: parseNumber(g("vimeo_subscription_price_usd_calc")),
    lifetime_value: parseNumber(g("lifetime_value_workflow")),
    paid_months: g("paid_months"),
    status: g("status"),
    health_score: parseNumber(g("health_score")),
    health_score_status: g("health_score_status"),
    free_tier_enabled: g("free_tier_enabled"),
    free_tier_health: g("free_tier_health"),
    free_tier_health_status: g("free_tier_health_status"),
    standard_tier_enabled: g("standard_tier_enabled"),
    standard_tier_monthly_enabled: g("standard_tier_monthly_enabled"),
    standard_tier_yearly_enabled: g("standard_tier_yearly_enabled"),
    all_tiers_enabled: g("all_tiers_enabled"),
    active_concurrent_playback: g("active_concurrent_playback"),
    max_concurrent_playbacks: g("max_concurrent_playbacks"),
    country: g("country"),
    state: g("state"),
    city: g("city"),
    customer_created_at: parseTimestamp(g("customer_created_at")),
    date_became_enabled: parseTimestamp(g("date_became_enabled")),
    date_last_canceled: parseTimestamp(g("date_last_canceled")),
    vimeo_old_last_payment_date: parseTimestamp(g("vimeo_old_last_payment_date")),
    status_change_date: parseTimestamp(g("status_change_date")),
    last_video_started_date: parseTimestamp(g("last_video_started_date")),
    last_payment_date: parseTimestamp(g("last_payment_date")),
    expiration_date: parseTimestamp(g("expiration_date")),
    event_created_at: parseTimestamp(g("event_created_at")),
    product_change_date: parseTimestamp(g("product_change_date")),
    time_since_payment: g("time_since_payment"),
    hs_createdate: parseTimestamp(g("hs_createdate")),
    hs_lastmodifieddate: parseTimestamp(g("hs_lastmodifieddate")),
    trial_started_date: parseTimestamp(g("trial_started_date")),
    trial_end_date: parseTimestamp(g("trial_end_date")),
    converted_trial: g("converted_trial"),
    has_canceled: g("has_canceled"),
    cancel_reason_category: g("cancel_reason_category"),
    cancel_reason_long: g("cancel_reason_long"),
    come_back_reason: g("come_back_reason"),
    will_you_come_back: g("will_you_come_back"),
    worth_the_price: g("worth_the_price"),
    frustrations: g("frustrations"),
    content_enjoyed_the_most: g("content_enjoyed_the_most"),
    particularly_excited: g("particularly_excited"),
    subscribe_count: g("subscribe_count"),
    unsubscribe_count: g("unsubscribe_count"),
    continues_customer_days: g("continues_customer_days"),
    utm_campaign: g("utm_campaign"),
    utm_medium: g("utm_medium"),
    utm_source: g("utm_source"),
    campaign: g("campaign"),
    referrer: g("referrer"),
    initial_discovery: g("initial_discovery"),
    hs_object_source_label: g("hs_object_source_label"),
    hs_object_source_detail_1: g("hs_object_source_detail_1"),
    marketing_opt_in: g("marketing_opt_in"),
    coupon_code: g("coupon_code"),
    coupon_code_id: g("coupon_code_id"),
    promotion_code: g("promotion_code"),
    enamel_pin_coupon_id: g("enamel_pin_coupon_id"),
    badge_redeemed: g("badge_redeemed"),
    badge_redeemed_date: parseTimestamp(g("badge_redeemed_date")),
    hs_pipeline: g("hs_pipeline"),
    hs_pipeline_stage: g("hs_pipeline_stage"),
    previous_vimeo_product_id: g("previous_vimeo_product_id"),
    previous_vimeo_status: g("previous_vimeo_status"),
    decision_timeline: g("decision_timeline"),
    master_record_association: g("master_record_association"),
    synced_at: new Date().toISOString(),
  };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function runSync() {
  console.log(`\n🚀 Starting Uncapped Data Sync at ${new Date().toISOString()}`);

  if (!HUBSPOT_TOKEN || HUBSPOT_TOKEN.length < 10) {
    console.error("❌ Fatal Error: HUBSPOT_TOKEN is missing or too short.");
    process.exit(1);
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  let endMs = Date.now();
  let startMs = Date.now() - (5 * 24 * 60 * 60 * 1000); // 5 days back default

  try {
    const { data: statusData } = await supabase
      .from("sync_status")
      .select("last_sync_timestamp")
      .eq("id", "hubspot_daily")
      .single();

    if (statusData?.last_sync_timestamp) {
      startMs = parseInt(statusData.last_sync_timestamp, 10);
      console.log(`🤖 Memory found. Starting from: ${new Date(startMs).toISOString()}`);
    }

    // CHANGED: This is now a "let" so we can overwrite it when bypassing the limit
    let filterGroups = [{
      filters: [
        { propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(startMs) },
        { propertyName: "hs_lastmodifieddate", operator: "LTE", value: String(endMs) }
      ]
    }];

    let hasMore = true;
    let totalFetched = 0;
    let totalUpserted = 0;
    let currentSearchFetched = 0; // Tracks fetches for the 10k limit
    let hubspotCursor = undefined; 
    let latestTimestamp = startMs;

    while (hasMore) {
      const body = {
        filterGroups: filterGroups,
        sorts: [{ propertyName: "hs_lastmodifieddate", direction: "ASCENDING" }],
        properties: PROPERTIES,
        limit: 100, // HubSpot API max limit per page
      };

      if (hubspotCursor) {
        body.after = parseInt(hubspotCursor, 10);
      }

      const res = await fetch(
        `https://api.hubapi.com/crm/v3/objects/${HUBSPOT_OBJECT_TYPE_ID}/search`,
        {
          method: "POST",
          headers: { 
            "Authorization": `Bearer ${HUBSPOT_TOKEN}`, 
            "Content-Type": "application/json" 
          },
          body: JSON.stringify(body),
        }
      );

      if (res.status === 429) {
        console.warn("⚠️ Rate limit hit. Sleeping 5 seconds...");
        await sleep(5000);
        continue;
      }

      if (!res.ok) throw new Error(`HubSpot Search API failed: ${res.status} ${await res.text()}`);

      const data = await res.json();
      const results = Array.isArray(data?.results) ? data.results : [];
      
      totalFetched += results.length;
      currentSearchFetched += results.length;

      if (results.length === 0) break;

      const transformedRows = results.map(transform);

      // Upserting 500 at a time to Supabase Database
      const BATCH = 500; 
      for (let i = 0; i < transformedRows.length; i += BATCH) {
        const chunk = transformedRows.slice(i, i + BATCH);
        const { error } = await supabase
          .from("vimeo_subscriptions_new")
          .upsert(chunk, { onConflict: "record_id" });
          
        if (error) throw new Error(`HubSpot upsert error: ${error.message}`);
        
        totalUpserted += chunk.length;
        console.log(`💾 Saved chunk. Total saved: ${totalUpserted}`);
        await sleep(300); 
      }

      // Track the highest timestamp we've seen so far
      if (transformedRows.length > 0) {
        const lastRecordTime = new Date(transformedRows[transformedRows.length - 1].hs_lastmodifieddate).getTime();
        latestTimestamp = Math.max(latestTimestamp, lastRecordTime);
      }

      hubspotCursor = data?.paging?.next?.after;
      
      // ==========================================
      // THE 10,000 LIMIT BYPASS (THE "TIME SLIDE")
      // ==========================================
      if (currentSearchFetched >= 9500 && hubspotCursor) {
        console.log(`\n🔄 Approaching HubSpot 10k Limit! Performing Time Slide to ${new Date(latestTimestamp).toISOString()}...`);
        
        // 1. Destroy the cursor so HubSpot doesn't crash
        hubspotCursor = undefined; 
        
        // 2. Change the filter to start exactly where we left off
        filterGroups = [{
          filters: [
            { propertyName: "hs_lastmodifieddate", operator: "GTE", value: String(latestTimestamp) },
            { propertyName: "hs_lastmodifieddate", operator: "LTE", value: String(endMs) }
          ]
        }];
        
        // 3. Reset our local 10k counter, but keep the loop running
        currentSearchFetched = 0; 
        hasMore = true; 
        
      } else {
        hasMore = !!hubspotCursor;
      }
    }

    console.log(`\n🏁 Sync complete. Saving current timestamp to memory...`);
    await supabase.from("sync_status").upsert({
      id: "hubspot_daily",
      last_sync_timestamp: endMs, 
      updated_at: new Date().toISOString()
    });

    console.log(`✅ Success! Full Sync Complete. Records upserted: ${totalUpserted}`);
    process.exit(0);

  } catch (err) {
    console.error("\n❌ Fatal Error:", err.message);
    process.exit(1);
  }
}

runSync();