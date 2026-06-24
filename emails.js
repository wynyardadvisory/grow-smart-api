"use strict";

/**
 * Vercro Email Sequences
 * ─────────────────────────────────────────────────────────────
 * Sequences:
 *
 * ACTIVATION
 *   nudge-unactivated     — confirmed email, never completed onboarding (D1, D5, D14, D28)
 *   nudge-unconfirmed     — signed up but never confirmed email (once, after 2h)
 *
 * ONBOARDING
 *   feedback-sequence     — active users at day 3 and day 7 (active/quiet variants)
 *
 * RE-ENGAGEMENT
 *   reengage_day14        — activated users who've gone quiet at 2 weeks
 *   reengage_day30        — activated users who've gone quiet at 1 month
 *   streak_recovery       — users with a 5+ day streak who've missed 2 days
 *
 * MILESTONES
 *   first_harvest         — fires once when harvest_logged_total goes from 0 → 1
 *   badge_unlock          — fires when a badge is earned (not already shown by push)
 *
 * UPGRADE
 *   upgrade_prompt        — fires within 24h of hitting Why Now / PlantCheck / Boost limit
 *
 * WEEKLY
 *   weekly_digest         — Sunday only, users without push, have due tasks, not seen in 24h
 *
 * WAITLIST
 *   waitlist_invite       — accepted waitlist members (once)
 *   waitlist_nudge        — invite sent 3+ days ago, not signed up (nudge_count 0→1)
 *   waitlist_nudge2       — invite sent 7+ days ago, not signed up (nudge_count 1→2)
 *   waitlist_nudge3       — invite sent 14+ days ago, not signed up (nudge_count 2→3, final)
 *
 * RECOVERY
 *   onboarding_recovery   — profiles with no crops (one-time batch)
 *
 * Install: npm install resend
 */

const { Resend } = require("resend");

function getResend() {
  if (!process.env.RESEND_API_KEY) {
    console.warn("[Email] RESEND_API_KEY not set — email disabled");
    return null;
  }
  return new Resend(process.env.RESEND_API_KEY);
}

const FROM    = "Vercro <hello@vercro.com>";
const APP_URL = "https://app.vercro.com";

// ── UTM helper ────────────────────────────────────────────────────────────────
// Appends UTM params to every CTA link so PostHog can attribute app sessions
// back to specific emails.
function utm(campaign, medium = "email") {
  return `?utm_source=email&utm_medium=${medium}&utm_campaign=${campaign}`;
}

// ── Unsubscribe footer helper ─────────────────────────────────────────────────
// All non-transactional emails include this. Links to Profile → Notifications.
function unsubscribeFooter() {
  return `<p style="font-size:11px;color:#aaa;text-align:center;margin:12px 0 0;">
    Don't want these emails?
    <a href="${APP_URL}${utm("unsubscribe_footer")}" style="color:#aaa;">Open the app</a>
    and go to Profile → Notifications to manage your preferences.
  </p>`;
}

// ── Plain text helper ─────────────────────────────────────────────────────────
// Strips HTML tags to produce a minimal plain-text fallback.
function toPlainText(html) {
  return html
    .replace(/<style[^>]*>.*?<\/style>/gis, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&nbsp;/g, " ")
    .replace(/&#x2019;/g, "'")
    .replace(/&#x201C;/g, '"')
    .replace(/&#x201D;/g, '"')
    .replace(/\s{2,}/g, " ")
    .trim();
}

// ── Preheader helper ──────────────────────────────────────────────────────────
// Hidden span shown in inbox preview after the subject line.
function preheader(text) {
  return `<span style="display:none;font-size:1px;color:#f4f8f2;max-height:0;overflow:hidden;opacity:0;">${text}&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;&nbsp;&zwnj;</span>`;
}

// ── Shell ─────────────────────────────────────────────────────────────────────
function shell(preheaderText, body, footerLine, includeUnsubscribe = true) {
  return `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8f2;font-family:Georgia,serif;">
  ${preheader(preheaderText)}
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 16px rgba(47,93,80,0.08);">
    <div style="background:#2F5D50;padding:32px 40px;text-align:center;">
      <div style="font-size:32px;margin-bottom:8px;">🌱</div>
      <div style="font-family:Georgia,serif;font-size:24px;font-weight:700;color:#ffffff;">Vercro</div>
    </div>
    <div style="padding:40px;">
      ${body}
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">${footerLine}</p>
      ${includeUnsubscribe ? unsubscribeFooter() : ""}
    </div>
  </div>
</body>
</html>`;
}

function firstName(name) {
  return name ? name.split(" ")[0] : "there";
}

function cta(label, href) {
  return `<div style="text-align:center;margin-bottom:32px;">
    <a href="${href}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">${label}</a>
  </div>`;
}

function infoBox(title, body, accent = "#2F5D50") {
  return `<div style="background:#f4f8f2;border-radius:12px;padding:18px 20px;margin-bottom:24px;border-left:3px solid ${accent};">
    <p style="font-size:13px;color:${accent};font-weight:700;margin:0 0 6px;">${title}</p>
    <p style="font-size:13px;color:#4a4a4a;line-height:1.7;margin:0;">${body}</p>
  </div>`;
}

// ── Paginated auth user loader ────────────────────────────────────────────────
// supabase.auth.admin.listUsers has a hard perPage cap of 1000.
// With 1,200+ users this silently drops users. Always paginate.
async function listAllAuthUsers(supabase) {
  const users = [];
  let page = 1;
  const perPage = 1000;
  while (true) {
    const { data, error } = await supabase.auth.admin.listUsers({ page, perPage });
    if (error) { console.error("[listAllAuthUsers] Error:", error.message); break; }
    const batch = data?.users || [];
    users.push(...batch);
    if (batch.length < perPage) break;
    page++;
  }
  return users;
}

// ── Send helper ───────────────────────────────────────────────────────────────
async function sendEmail(to, template, emailType) {
  const resend = getResend();
  if (!resend) return { sent: false, reason: "resend_not_configured" };
  try {
    const payload = {
      from:    FROM,
      to,
      subject: template.subject,
      html:    template.html,
      text:    template.text || toPlainText(template.html),
    };
    if (emailType) payload.tags = [{ name: "email_type", value: emailType }];
    const { data, error } = await resend.emails.send(payload);
    if (error) {
      console.error(`[Email] Failed to send to ${to}:`, error);
      return { sent: false, reason: error.message };
    }
    console.log(`[Email] Sent "${template.subject}" to ${to} (${data.id})`);
    return { sent: true, id: data.id };
  } catch (err) {
    console.error(`[Email] Error sending to ${to}:`, err.message);
    return { sent: false, reason: err.message };
  }
}

// ── Non-transactional cooldown map ────────────────────────────────────────────
// Returns the most recent sent_at (ms) for any non-transactional email per user.
// Used by weekly digest and re-engagement for 7-day cooldown enforcement.
const NON_TRANSACTIONAL_EMAIL_TYPES = [
  "weekly_digest",
  "reengage_day14",
  "reengage_day30",
  "feedback_day3",
  "feedback_day7",
  "streak_recovery",
  "first_harvest",
  "badge_unlock",
  "upgrade_prompt",
];

async function buildRecentNonTransactionalMap(supabase) {
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 3600000).toISOString();
  const { data: recentEmails } = await supabase
    .from("email_log")
    .select("user_id, email_type, sent_at")
    .in("email_type", NON_TRANSACTIONAL_EMAIL_TYPES)
    .gte("sent_at", sevenDaysAgo);

  const map = {};
  (recentEmails || []).forEach(e => {
    const ms = new Date(e.sent_at).getTime();
    if (!map[e.user_id] || ms > map[e.user_id]) map[e.user_id] = ms;
  });
  return map;
}

// ═══════════════════════════════════════════════════════════════════════════════
// TEMPLATES
// ═══════════════════════════════════════════════════════════════════════════════

// ── Nudge: unactivated D1 ────────────────────────────────────────────────────
function templateNudgeUnactivated(name, signupSource) {
  const fn = firstName(name);
  // Tailor opening line slightly based on how they found Vercro
  const isOrganic = signupSource && (signupSource.includes("seo") || signupSource.includes("organic") || signupSource.includes("learn"));
  const opener = isOrganic
    ? `You found Vercro while looking for growing advice — the app takes that further. Instead of reading about what to do, it builds you a personalised daily plan based on your exact crops, location and the weather.`
    : `You've got access to Vercro but haven't set up your garden yet. It only takes a couple of minutes — add your crops and we'll build you a personalised growing plan straight away.`;

  const html = shell(
    "Your personalised growing plan is one step away",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your garden is waiting</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">${opener}</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Daily task reminders, harvest forecasts, frost alerts — all based on exactly what you're growing. Takes about 2 minutes to set up.</p>
    ${cta("Set up my garden →", APP_URL + utm("nudge_unactivated_d1"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">No payment needed.</p>`,
    "Vercro · Growing intelligence · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "Your Vercro garden is waiting 🌱", html };
}

// ── Nudge: unactivated D5 ────────────────────────────────────────────────────
function templateNudgeUnactivatedD5(name) {
  const fn = firstName(name);
  const html = shell(
    "Your personalised plan is still waiting — takes 2 minutes",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your garden plan is one step away</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You signed up a few days ago but haven't set up your garden yet. Takes about 2 minutes — just tell us your location and what you're growing, and we'll build your personalised daily plan straight away.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Daily task reminders, harvest forecasts, frost alerts, companion planting suggestions — all based on exactly what you're growing.</p>
    ${cta("Set up my garden →", APP_URL + utm("nudge_unactivated_d5"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">No payment needed.</p>`,
    "Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "Still time to set up your Vercro garden 🌱", html };
}

// ── Nudge: unactivated D14 ───────────────────────────────────────────────────
function templateNudgeUnactivatedD14(name) {
  const fn = firstName(name);
  const html = shell(
    "The growing season is underway — it's not too late",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — it's not too late</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">Two weeks ago you signed up for Vercro but never got your garden set up. The growing season is well underway — there's still plenty to sow and plan.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Add your location and crops and we'll tell you exactly what to do this week — no guessing, no forgetting.</p>
    ${cta("Start my plan →", APP_URL + utm("nudge_unactivated_d14"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">No payment needed.</p>`,
    "Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "Your growing season is slipping by 🌿", html };
}

// ── Nudge: unactivated D28 ───────────────────────────────────────────────────
function templateNudgeUnactivatedD28(name) {
  const fn = firstName(name);
  const html = shell(
    "Last nudge from us — your account is still there whenever you're ready",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — we'll leave you to it</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You signed up for Vercro a month ago but never got started. We won't keep nudging you — but your account is still there if you change your mind.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">If you do want a personalised garden plan this season, it takes about 2 minutes to set up.</p>
    ${cta("Set up my garden →", APP_URL + utm("nudge_unactivated_d28"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">You won't hear from us again on this.</p>`,
    "Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false // no unsubscribe footer — this IS the last email
  );
  return { subject: "Last nudge from us 🌱", html };
}

// ── Nudge: unconfirmed ───────────────────────────────────────────────────────
function templateNudgeUnconfirmed(name) {
  const fn = firstName(name);
  const html = shell(
    "One click to confirm and your garden plan can start",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — just one step left</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You signed up for Vercro but haven't confirmed your email yet. Check your inbox for a confirmation email from us and click the link inside to activate your account.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Can't find it? Check your spam folder — sometimes it ends up there.</p>
    ${cta("Go to Vercro →", APP_URL + utm("nudge_unconfirmed"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">If you didn't sign up for Vercro, you can safely ignore this email.</p>`,
    "Vercro · Growing intelligence · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false // transactional — no unsubscribe
  );
  return { subject: "Please confirm your Vercro account ✉️", html };
}

// ── Feedback: day 3 active ───────────────────────────────────────────────────
function templateFeedbackDay3Active(name, tasksCompleted, cropList) {
  const fn = firstName(name);
  const cropStr = cropList && cropList.length
    ? `across your ${cropList.slice(0, 3).join(", ")}${cropList.length > 3 ? ` and ${cropList.length - 3} more` : ""}`
    : "in your garden";
  const html = shell(
    `${tasksCompleted} tasks done — you're building a real habit`,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">${tasksCompleted} tasks in 3 days — that's a great start, ${fn}</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You've completed ${tasksCompleted} tasks ${cropStr}. The growers who check in regularly are the ones who get the best harvests — you're already building that habit.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">A couple of things worth exploring if you haven't yet:</p>
    ${infoBox("Why Now? 💡", "Tap the 'Why Now?' button on any task card and Vercro explains exactly why it's recommending that task today — your crop stage, local weather, what happens if you skip it. Free users get 3 explanations; Pro gets unlimited.")}
    ${infoBox("PlantCheck 📷", "Not sure what's wrong with a plant? Take a photo and Vercro diagnoses it — identifying problems, estimating growth stage, and telling you what to do next. Free users get 3 checks.")}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">Quick question — what made you sign up? Was there a specific problem you were trying to solve? Just hit reply — I read everything personally.</p>
    ${cta("Keep growing →", APP_URL + utm("feedback_day3_active"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return {
    subject: `${tasksCompleted} tasks down, ${fn} — you're off to a great start 🌱`,
    html,
  };
}

// ── Feedback: day 3 quiet ────────────────────────────────────────────────────
function templateFeedbackDay3Quiet(name) {
  const fn = firstName(name);
  const html = shell(
    "Getting started with Vercro — one tip that helps",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — getting on OK with Vercro?</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You signed up a few days ago and I just wanted to check in. Sometimes the app takes a few minutes to click — especially once you've added your crops and seen your first daily task list.</p>
    ${infoBox("If you haven't already, try this:", "Go to the Crops tab → Add a crop you're growing → Come back to the dashboard. Your personalised task list will appear straight away.")}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">If something isn't working or feels confusing, just hit reply — I'll help you get set up.</p>
    ${cta("Open my garden →", APP_URL + utm("feedback_day3_quiet"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "Getting started with Vercro — anything I can help with?", html };
}

// ── Feedback: day 7 active ───────────────────────────────────────────────────
function templateFeedbackDay7Active(name, tasksCompleted, cropList) {
  const fn = firstName(name);
  const cropStr = cropList && cropList.length
    ? `Your ${cropList.slice(0, 3).join(", ")}${cropList.length > 3 ? ` and ${cropList.length - 3} more crops are` : cropList.length > 1 ? " are" : " is"} in good hands.`
    : "Your garden is in good hands.";
  const html = shell(
    "One week in — two quick questions",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">One week, ${tasksCompleted} tasks — your garden is in good hands, ${fn}</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You've been one of our most consistent users this week. ${cropStr} Small actions compounding over the season make a real difference at harvest time.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">Two quick questions — reply with whatever comes to mind:</p>
    <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:32px;">
      <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0 0 8px;">1. What's the single most useful thing Vercro does for you?</p>
      <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0;">2. What's the one thing you wish it did that it doesn't yet?</p>
    </div>
    ${cta("Open my garden →", APP_URL + utm("feedback_day7_active"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: `A week in and ${tasksCompleted} tasks done — what do you think?`, html };
}

// ── Feedback: day 7 quiet ────────────────────────────────────────────────────
function templateFeedbackDay7Quiet(name) {
  const fn = firstName(name);
  const html = shell(
    "7 days of tasks have been building in your garden — take a look",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your plan has been building without you</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">It's been a week since you set up your garden in Vercro. Every day since, your personalised task plan has been updating — based on your crops, your location, and what's happening in the garden right now.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">You haven't seen any of it yet.</p>
    ${infoBox("This week your plan may include:", "Sowing reminders · Frost alerts · Watering guidance · Thinning and feeding tasks · What to do next with each crop")}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">It takes 30 seconds to open. Your tasks are waiting.</p>
    ${cta("See this week's tasks →", APP_URL + utm("feedback_day7_quiet"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "7 days of tasks you haven't seen yet 🌱", html };
}

// ── Re-engagement: day 14 ────────────────────────────────────────────────────
// Personalised with actual crop names and stages where available.
function templateReengageDay14(name, cropSummary) {
  const fn = firstName(name);
  // cropSummary: array of { name, stage } or null
  let cropLine = "";
  if (cropSummary && cropSummary.length > 0) {
    const parts = cropSummary.slice(0, 3).map(c => {
      const stageLabel = {
        sown: "recently sown",
        seedling: "at seedling stage",
        vegetative: "growing strongly",
        flowering: "flowering",
        harvest: "ready to harvest",
      }[c.stage] || "growing";
      return `your ${c.name} is ${stageLabel}`;
    });
    cropLine = `<p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">Since you last opened the app: ${parts.join(", ")}. There are tasks waiting for each of them.</p>`;
  }
  const html = shell(
    "Your crops have moved on — here's what's waiting",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your garden has moved on without you</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">It's been a couple of weeks. Vercro has been running your plan in the background — tracking crop stages, updating tasks, watching the weather.</p>
    ${cropLine}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">A couple of things that might be worth trying if you haven't:</p>
    ${infoBox("PlantCheck 📷", "Photograph any plant — Vercro identifies what's wrong, estimates its growth stage, and tells you what to do. Free users get 3 checks. Tap any crop → ··· → PlantCheck.")}
    ${infoBox("Why Now? 💡", "Every task card has a 'Why Now?' button that explains exactly why Vercro is recommending it today — crop stage, local weather, what's at risk if you skip it.")}
    ${infoBox("Boost Your Bed 🌿", "Tap ··· on any growing area → Boost. Vercro suggests the best companion plants to add — reducing pests, improving soil, increasing yield.", "#7b5ea7")}
    <p style="font-size:13px;color:#888;text-align:center;margin:0 0 24px;">There's also an iOS and Android app — if you've been using the web version, the native app adds push notifications and a home screen badge.</p>
    ${cta("Back to my garden →", APP_URL + utm("reengage_day14"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "A couple of things we've added to Vercro 🌿", html };
}

// ── Re-engagement: day 30 ────────────────────────────────────────────────────
function templateReengageDay30(name, longestStreak) {
  const fn = firstName(name);
  const month = new Date().toLocaleString("en-GB", { month: "long" });
  const streakLine = longestStreak && longestStreak >= 3
    ? `<p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You had a ${longestStreak}-day streak earlier in the season. That kind of consistency is exactly what makes the difference at harvest time — and it's easy to pick up again.</p>`
    : "";
  const html = shell(
    `${month} in the garden — your plan is still there`,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — ${month} in the garden</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">It's been about a month since you joined Vercro. If you've drifted away, no worries at all — your garden is exactly as you left it. Your crops, your tasks, your growing plan — all still there.</p>
    ${streakLine}
    ${infoBox(`What active Vercro growers are doing in ${month}:`, "Completing daily tasks · Tracking sowing dates · Getting frost alerts · Logging harvests · Confirming crop stages on their timelines")}
    ${cta("Back to my garden →", APP_URL + utm("reengage_day30"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:8px 0 0;">Hit reply anytime — I read everything.</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: `What's growing this ${month}`, html };
}

// ── Streak recovery ──────────────────────────────────────────────────────────
// Fires when a user had a streak of 5+ days and has missed 2 days.
function templateStreakRecovery(name, streakDays) {
  const fn = firstName(name);
  const html = shell(
    `Your ${streakDays}-day streak is at risk — one task saves it`,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your ${streakDays}-day streak is at risk</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">You built a ${streakDays}-day streak in Vercro — that's real consistency. You haven't checked in for a couple of days, and the streak won't last much longer.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Open the app, tick one task, and your streak is back on track. Takes 30 seconds.</p>
    ${cta("Save my streak →", APP_URL + utm("streak_recovery"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">Your garden is waiting for you.</p>`,
    "Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: `Your ${streakDays}-day streak is at risk 🔥`, html };
}

// ── First harvest ─────────────────────────────────────────────────────────────
// Fires once when a user logs their very first harvest.
function templateFirstHarvest(name, cropName, quantityG) {
  const fn = firstName(name);
  const weightLine = quantityG
    ? `<p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">${quantityG >= 1000 ? `${(quantityG / 1000).toFixed(1)}kg` : `${quantityG}g`} of ${cropName} — from seed to table. That's what this is all about.</p>`
    : `<p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">${cropName} — from seed to table. That's what this is all about.</p>`;
  const html = shell(
    "Your first harvest — congratulations",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your first harvest 🌾</h1>
    ${weightLine}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">Every harvest you log builds your growing record — yield, quality, notes. By the end of the season you'll have a full picture of what worked and what to do differently next year.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">What was it like? Just hit reply — I'd genuinely love to hear.</p>
    ${cta("Log another harvest →", APP_URL + utm("first_harvest"))}`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: "Your first Vercro harvest 🌾", html };
}

// ── Badge unlock ──────────────────────────────────────────────────────────────
// Fires when a badge is earned and push didn't already cover it.
function templateBadgeUnlock(name, badgeTitle, badgeDescription, celebrationCopy) {
  const fn = firstName(name);
  const body = celebrationCopy || badgeDescription || `You earned the ${badgeTitle} badge.`;
  const html = shell(
    `You earned the ${badgeTitle} badge`,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — you earned a badge 🏅</h1>
    <div style="background:#f4f8f2;border-radius:12px;padding:24px;text-align:center;margin-bottom:24px;border:1px solid #D4E8CE;">
      <div style="font-size:36px;margin-bottom:8px;">🏅</div>
      <div style="font-size:18px;font-weight:700;color:#2F5D50;font-family:Georgia,serif;margin-bottom:8px;">${badgeTitle}</div>
      <div style="font-size:14px;color:#4a4a4a;line-height:1.6;">${body}</div>
    </div>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Open the app to see your full badge collection and what you're working toward next.</p>
    ${cta("See my badges →", APP_URL + utm("badge_unlock"))}`,
    "Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: `You earned the ${badgeTitle} badge 🏅`, html };
}

// ── Upgrade prompt ────────────────────────────────────────────────────────────
// Fires within 24h of hitting a Pro limit. limitType: "why_now" | "plantcheck" | "boost"
function templateUpgradePrompt(name, limitType) {
  const fn = firstName(name);
  const variants = {
    why_now: {
      subject: "You've used all 3 Why Now? explanations — here's what Pro gets you",
      preheaderText: "Unlimited Why Now? is one of the things Pro growers love most",
      featureName: "Why Now?",
      featureDesc: "You've used all 3 of your free Why Now? explanations. Tap Why Now? on any task and Vercro explains exactly why it's recommending it today — your crop stage, local weather, what happens if you skip it.",
      limitLine: "Free users get 3 explanations. Pro users get unlimited, for every task, every day.",
    },
    plantcheck: {
      subject: "You've used all 3 PlantChecks — here's what Pro gets you",
      preheaderText: "Unlimited plant diagnoses — that's Pro",
      featureName: "PlantCheck",
      featureDesc: "You've used all 3 of your free PlantChecks. Photograph any plant and Vercro identifies problems, estimates growth stage, and tells you exactly what to do next.",
      limitLine: "Free users get 3 lifetime checks. Pro users get unlimited.",
    },
    boost: {
      subject: "You've used all 3 free Boosts — here's what Pro gets you",
      preheaderText: "Unlimited companion suggestions — Pro growers use this every season",
      featureName: "Boost Your Bed",
      featureDesc: "You've used all 3 of your free Boost suggestions. Tap ··· on any area → Boost and Vercro recommends the best companion plants — reducing pests, improving soil, increasing yield.",
      limitLine: "Free users get 3 boosts. Pro users get unlimited across all beds.",
    },
  };
  const v = variants[limitType] || variants.why_now;
  const html = shell(
    v.preheaderText,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — you hit your free limit on ${v.featureName}</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">${v.featureDesc}</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">${v.limitLine}</p>
    <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:28px;">
      <p style="font-size:14px;font-weight:700;color:#2F5D50;margin:0 0 12px;">Vercro Pro also includes:</p>
      <p style="font-size:13px;color:#4a4a4a;line-height:1.8;margin:0;">
        ✓ Unlimited Why Now? explanations<br>
        ✓ Unlimited PlantCheck diagnoses<br>
        ✓ Unlimited Boost suggestions<br>
        ✓ Unlimited growing locations<br>
        ✓ Season planning &amp; draft plans<br>
        ✓ Priority support
      </p>
    </div>
    ${cta("Upgrade to Pro →", APP_URL + utm("upgrade_prompt_" + limitType))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">You can manage or cancel your subscription any time.</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return { subject: v.subject, html };
}

// ── Weekly digest ─────────────────────────────────────────────────────────────
// Shows the single most urgent task prominently, then remaining tasks.
function templateWeeklyDigest(name, tasks, cropNames) {
  const fn = firstName(name);
  const taskCount = tasks.length;
  // Sort by urgency: high → medium → low
  const urgencyOrder = { high: 0, medium: 1, low: 2 };
  const sorted = [...tasks].sort((a, b) => (urgencyOrder[a.urgency] ?? 1) - (urgencyOrder[b.urgency] ?? 1));
  const topTask = sorted[0];
  const remaining = sorted.slice(1, 4); // show up to 3 more

  const topTaskBlock = topTask ? `
    <div style="background:#2F5D50;border-radius:12px;padding:20px 24px;margin-bottom:20px;">
      <p style="font-size:11px;font-weight:700;color:#a8d5c2;text-transform:uppercase;letter-spacing:1px;margin:0 0 8px;">Most urgent this week</p>
      <p style="font-size:16px;font-weight:700;color:#ffffff;margin:0 0 4px;">${topTask.crop?.name || "Garden task"}</p>
      <p style="font-size:13px;color:#d4eee5;line-height:1.5;margin:0;">${topTask.action}</p>
    </div>` : "";

  const remainingBlock = remaining.length ? `
    <div style="background:#f4f8f2;border-radius:12px;padding:16px 20px;margin-bottom:20px;">
      ${remaining.map((t, i) => `
        <div style="${i < remaining.length - 1 ? "margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #D4E8CE;" : ""}">
          <div style="font-size:13px;font-weight:700;color:#1a1a1a;">${t.crop?.name || "Garden task"}</div>
          <div style="font-size:12px;color:#4a4a4a;line-height:1.5;">${t.action}</div>
        </div>`).join("")}
      ${taskCount > 4 ? `<div style="font-size:12px;color:#6E6E6E;margin-top:12px;padding-top:12px;border-top:1px solid #D4E8CE;">+ ${taskCount - 4} more task${taskCount - 4 !== 1 ? "s" : ""} in the app</div>` : ""}
    </div>` : "";

  const html = shell(
    `${taskCount} task${taskCount !== 1 ? "s" : ""} waiting in your garden this week`,
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 8px;">Good morning, ${fn}</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">You have <strong>${taskCount} task${taskCount !== 1 ? "s" : ""}</strong> waiting in your garden this week.</p>
    ${topTaskBlock}
    ${remainingBlock}
    <p style="font-size:13px;color:#6E6E6E;line-height:1.6;margin:0 0 24px;">These tasks are timed around your local weather, growing season and frost dates.</p>
    ${cta("Open my garden →", APP_URL + utm("weekly_digest"))}`,
    "Vercro · Growing intelligence · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>"
  );
  return {
    subject: `Your garden this week 🌱 (${taskCount} thing${taskCount !== 1 ? "s" : ""} to do)`,
    html,
  };
}

// ── Waitlist invite ───────────────────────────────────────────────────────────
function templateWaitlistInvite(name) {
  const fn = firstName(name);
  const html = shell(
    "Your Vercro access is ready — set up in 2 minutes",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">You're in, ${fn}!</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">Your place on the Vercro beta is ready. Create your free account and we'll build you a personalised growing plan based on your crops, your location and the time of year.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Daily tasks, harvest forecasts, frost alerts, AI crop identification — all in one place, wherever you grow.</p>
    ${cta("Create my free account →", APP_URL + utm("waitlist_invite"))}
    ${infoBox("Getting started takes 2 minutes:", "1. Create your account<br>2. Add your crops<br>3. Get your personalised plan")}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">No cost · No payment details · Free beta access</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false
  );
  return { subject: "You're in — welcome to Vercro 🌱", html };
}

// ── Waitlist nudge D3 ─────────────────────────────────────────────────────────
function templateWaitlistNudge(name) {
  const fn = firstName(name);
  const html = shell(
    "Your Vercro spot is still there — takes 2 minutes",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — your garden is waiting</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">We sent you an invite to Vercro a few days ago but haven't seen you inside yet. Your spot is still reserved — it only takes 2 minutes to get set up.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">With the growing season getting started, now's the perfect time to get your garden plan in place.</p>
    ${cta("Set up my garden →", APP_URL + utm("waitlist_nudge"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free to use · No payment needed</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false
  );
  return { subject: `Still waiting for you, ${fn} 🌱`, html };
}

// ── Waitlist nudge D7 ─────────────────────────────────────────────────────────
function templateWaitlistNudge2(name) {
  const fn = firstName(name);
  const html = shell(
    "The growing season is starting — your spot is still open",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">The growing season is getting started</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">Hey ${fn} — sowing windows are opening, frost risk is still real in many areas, and getting organised now makes the rest of the season much easier.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">Vercro tells you exactly what to do and when — based on your crops, your location and the actual weather. Your spot is still there.</p>
    ${infoBox("What you'll get on day one:", "✓ A personalised daily task list<br>✓ Sowing and harvest windows for your crops<br>✓ Frost alerts based on your location<br>✓ PlantCheck — AI crop identification and diagnosis")}
    ${cta("Start growing →", APP_URL + utm("waitlist_nudge2"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free · No card needed · 2 minutes to set up</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false
  );
  return { subject: `The growing season is starting, ${fn}`, html };
}

// ── Waitlist nudge D14 (final) ────────────────────────────────────────────────
function templateWaitlistNudge3(name) {
  const fn = firstName(name);
  const html = shell(
    "Last nudge from me — your access isn't going anywhere",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${fn} — one last nudge from me</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">I've sent you a couple of emails about Vercro and I don't want to keep pestering you — so this is the last one.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">If the timing isn't right or gardening isn't a priority right now, that's completely fine. Your access isn't going anywhere — you can sign up whenever you're ready.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">But if you do want a growing season where you always know what to do next — Vercro is ready for you.</p>
    ${cta("Set up my garden →", APP_URL + utm("waitlist_nudge3"))}
    <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free · No card needed · Always open</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false
  );
  return { subject: "One last nudge from me 🌱", html };
}

// ── Onboarding recovery ───────────────────────────────────────────────────────
function templateOnboardingRecovery(name) {
  const fn = firstName(name);
  const html = shell(
    "We found and fixed an issue with your Vercro setup",
    `<h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hi ${fn} — I wanted to email you personally</h1>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">We found an issue that may have affected your account when you first set up Vercro. In some cases, the crops you chose during onboarding weren't saved properly — which meant your personalised plan and daily tasks never generated as they should have.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">That's now been fixed.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">If you open Vercro and re-add your crops, your personalised plan will build straight away — daily tasks, harvest forecasts, frost alerts, all based on exactly what you're growing.</p>
    ${cta("Set up my garden →", APP_URL + utm("onboarding_recovery"))}
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">I'm really sorry about this — it was entirely on our side. I appreciate your patience and I'm grateful you gave Vercro a try.</p>
    <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0;">If you have any questions just reply to this email — I read everything personally.</p>`,
    "Mark · Founder of Vercro · <a href=\"https://vercro.com\" style=\"color:#2F5D50;\">vercro.com</a>",
    false
  );
  return { subject: "Sorry — we found and fixed an issue with your Vercro setup", html };
}

// ═══════════════════════════════════════════════════════════════════════════════
// SEQUENCE RUNNERS
// ═══════════════════════════════════════════════════════════════════════════════

// ── Nudge: unactivated ────────────────────────────────────────────────────────
// Confirmed email, never completed onboarding (no profile row).
// D1, D5, D14, D28 — then stops.
// signup_source on auth.users is checked (not profiles — these users have none).
async function runNudgeUnactivated(supabase) {
  const users = await listAllAuthUsers(supabase);

  const { data: profiles } = await supabase.from("profiles").select("id, email_unsubscribed");
  const profileIds      = new Set((profiles || []).map(p => p.id));
  const unsubscribedIds = new Set((profiles || []).filter(p => p.email_unsubscribed).map(p => p.id));

  const { data: alreadySent } = await supabase
    .from("email_log")
    .select("user_id, email_type")
    .in("email_type", ["nudge_unactivated", "nudge_unactivated_d5", "nudge_unactivated_d14", "nudge_unactivated_d28"]);

  const sentMap = {};
  (alreadySent || []).forEach(e => {
    if (!sentMap[e.user_id]) sentMap[e.user_id] = new Set();
    sentMap[e.user_id].add(e.email_type);
  });

  const DAYS = [
    { minDays: 1,  maxDays: 4,   type: "nudge_unactivated",      templateFn: (u) => templateNudgeUnactivated(u.user_metadata?.full_name, u.user_metadata?.signup_source) },
    { minDays: 5,  maxDays: 13,  type: "nudge_unactivated_d5",   templateFn: (u) => templateNudgeUnactivatedD5(u.user_metadata?.full_name) },
    { minDays: 14, maxDays: 27,  type: "nudge_unactivated_d14",  templateFn: (u) => templateNudgeUnactivatedD14(u.user_metadata?.full_name) },
    { minDays: 28, maxDays: 999, type: "nudge_unactivated_d28",  templateFn: (u) => templateNudgeUnactivatedD28(u.user_metadata?.full_name) },
  ];

  const now = Date.now();
  let sent = 0, skipped = 0;

  for (const user of (users || [])) {
    if (!user.email_confirmed_at)    { skipped++; continue; }
    if (profileIds.has(user.id))     { skipped++; continue; }
    if (unsubscribedIds.has(user.id)){ skipped++; continue; }

    const daysSince = (now - new Date(user.email_confirmed_at).getTime()) / 86400000;
    const userSent  = sentMap[user.id] || new Set();
    const due = DAYS.find(d =>
      daysSince >= d.minDays && daysSince < d.maxDays && !userSent.has(d.type)
    );
    if (!due) { skipped++; continue; }

    const result = await sendEmail(user.email, due.templateFn(user), due.type);
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: user.id, email: user.email, email_type: due.type,
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    }
  }

  console.log(`[NudgeUnactivated] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Nudge: unconfirmed ────────────────────────────────────────────────────────
async function runNudgeUnconfirmed(supabase) {
  const users = await listAllAuthUsers(supabase);

  const { data: profiles } = await supabase.from("profiles").select("id, email_unsubscribed");
  const unsubIds = new Set((profiles || []).filter(p => p.email_unsubscribed).map(p => p.id));

  const { data: alreadySent } = await supabase
    .from("email_log").select("user_id").eq("email_type", "nudge_unconfirmed");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const now = Date.now();
  let sent = 0, skipped = 0;

  for (const user of (users || [])) {
    if (user.email_confirmed_at)                              { skipped++; continue; }
    if (now - new Date(user.created_at).getTime() < 7200000) { skipped++; continue; } // < 2h
    if (alreadySentIds.has(user.id))                         { skipped++; continue; }
    if (unsubIds.has(user.id))                               { skipped++; continue; }

    const result = await sendEmail(user.email, templateNudgeUnconfirmed(user.user_metadata?.full_name), "nudge_unconfirmed");
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: user.id, email: user.email, email_type: "nudge_unconfirmed",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    }
  }

  console.log(`[NudgeUnconfirmed] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Feedback sequence ─────────────────────────────────────────────────────────
// Day 3 and day 7 — active/quiet variants. Personalised with crop names.
async function runFeedbackSequence(supabase) {
  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name, email_unsubscribed")
    .order("created_at");

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // Task completion counts per user
  const { data: taskCounts } = await supabase
    .from("tasks").select("user_id").not("completed_at", "is", null);
  const completedMap = {};
  (taskCounts || []).forEach(t => {
    completedMap[t.user_id] = (completedMap[t.user_id] || 0) + 1;
  });

  // Active crop names per user
  const { data: crops } = await supabase
    .from("crop_instances").select("user_id, name").eq("active", true);
  const cropsByUser = {};
  (crops || []).forEach(c => {
    if (!cropsByUser[c.user_id]) cropsByUser[c.user_id] = [];
    cropsByUser[c.user_id].push(c.name);
  });

  const { data: alreadySent } = await supabase.from("email_log").select("user_id, email_type");
  const sentMap = {};
  (alreadySent || []).forEach(e => {
    if (!sentMap[e.user_id]) sentMap[e.user_id] = new Set();
    sentMap[e.user_id].add(e.email_type);
  });

  const now = Date.now();
  let sent = 0;

  for (const profile of (profiles || [])) {
    const user = userMap[profile.id];
    if (!user?.email)            continue;
    if (profile.email_unsubscribed) continue;

    const daysSince      = (now - new Date(user.created_at).getTime()) / 86400000;
    const userSent       = sentMap[profile.id] || new Set();
    const tasksCompleted = completedMap[profile.id] || 0;
    const userCrops      = cropsByUser[profile.id] || [];
    const isActive       = tasksCompleted >= 5;

    // Day 3
    if (daysSince >= 3 && daysSince < 6 && !userSent.has("feedback_day3")) {
      const template = isActive
        ? templateFeedbackDay3Active(profile.name, tasksCompleted, userCrops)
        : templateFeedbackDay3Quiet(profile.name);
      const result = await sendEmail(user.email, template, "feedback_day3");
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email, email_type: "feedback_day3",
          sent_at: new Date().toISOString(), resend_email_id: result.id || null,
        });
        sent++;
      }
    }

    // Day 7
    if (daysSince >= 7 && daysSince < 10 && !userSent.has("feedback_day7")) {
      const template = isActive
        ? templateFeedbackDay7Active(profile.name, tasksCompleted, userCrops)
        : templateFeedbackDay7Quiet(profile.name);
      const result = await sendEmail(user.email, template, "feedback_day7");
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email, email_type: "feedback_day7",
          sent_at: new Date().toISOString(), resend_email_id: result.id || null,
        });
        sent++;
      }
    }
  }

  console.log(`[FeedbackSequence] Sent: ${sent}`);
  return { sent };
}

// ── Re-engagement ─────────────────────────────────────────────────────────────
// D14 personalised with real crop stages. D30 with longest streak.
async function runReengagement(supabase) {
  const { data: profiles } = await supabase
    .from("profiles").select("id, name, email_unsubscribed").order("created_at");
  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // Crop stages for D14 personalisation
  const { data: crops } = await supabase
    .from("crop_instances").select("user_id, name, stage").eq("active", true);
  const cropsByUser = {};
  (crops || []).forEach(c => {
    if (!cropsByUser[c.user_id]) cropsByUser[c.user_id] = [];
    cropsByUser[c.user_id].push({ name: c.name, stage: c.stage });
  });

  // Longest streak for D30 personalisation
  const { data: counters } = await supabase
    .from("user_activity_counters").select("user_id, longest_streak_days");
  const streakMap = {};
  (counters || []).forEach(c => { streakMap[c.user_id] = c.longest_streak_days || 0; });

  const { data: alreadySent } = await supabase.from("email_log").select("user_id, email_type");
  const sentMap = {};
  (alreadySent || []).forEach(e => {
    if (!sentMap[e.user_id]) sentMap[e.user_id] = new Set();
    sentMap[e.user_id].add(e.email_type);
  });

  const recentMap = await buildRecentNonTransactionalMap(supabase);
  const now = Date.now();
  let sent = 0;

  for (const profile of (profiles || [])) {
    const user = userMap[profile.id];
    if (!user?.email)               continue;
    if (profile.email_unsubscribed) continue;
    if (recentMap[profile.id] && (now - recentMap[profile.id]) < 7 * 24 * 3600000) continue;

    const daysSince = (now - new Date(user.created_at).getTime()) / 86400000;
    const userSent  = sentMap[profile.id] || new Set();

    if (daysSince >= 14 && daysSince < 17 && !userSent.has("reengage_day14")) {
      const cropSummary = cropsByUser[profile.id] || null;
      const result = await sendEmail(user.email, templateReengageDay14(profile.name, cropSummary), "reengage_day14");
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email, email_type: "reengage_day14",
          sent_at: new Date().toISOString(), resend_email_id: result.id || null,
        });
        sent++;
      }
    }

    if (daysSince >= 30 && daysSince < 33 && !userSent.has("reengage_day30")) {
      const longestStreak = streakMap[profile.id] || 0;
      const result = await sendEmail(user.email, templateReengageDay30(profile.name, longestStreak), "reengage_day30");
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email, email_type: "reengage_day30",
          sent_at: new Date().toISOString(), resend_email_id: result.id || null,
        });
        sent++;
      }
    }
  }

  console.log(`[Reengagement] Sent: ${sent}`);
  return { sent };
}

// ── Streak recovery ───────────────────────────────────────────────────────────
// Users with a streak of 5+ who haven't had a qualifying activity in 2 days.
// Runs daily. One email per streak break (checked via email_log).
async function runStreakRecovery(supabase) {
  const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toISOString().split("T")[0];

  // Users with streak >= 5, last activity 2+ days ago
  const { data: atRisk } = await supabase
    .from("user_activity_counters")
    .select("user_id, current_streak_days, last_qualifying_activity_date")
    .gte("current_streak_days", 5)
    .lte("last_qualifying_activity_date", twoDaysAgo);

  if (!atRisk?.length) return { sent: 0, skipped: 0 };

  const { data: profiles } = await supabase
    .from("profiles").select("id, name, email_unsubscribed");
  const profileMap = {};
  (profiles || []).forEach(p => { profileMap[p.id] = p; });

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // Users already sent streak recovery (don't resend for same streak break)
  // Use a 5-day window — once per streak break
  const fiveDaysAgo = new Date(Date.now() - 5 * 86400000).toISOString();
  const { data: recentStreak } = await supabase
    .from("email_log")
    .select("user_id")
    .eq("email_type", "streak_recovery")
    .gte("sent_at", fiveDaysAgo);
  const recentStreakIds = new Set((recentStreak || []).map(e => e.user_id));

  let sent = 0, skipped = 0;

  for (const counter of atRisk) {
    const profile = profileMap[counter.user_id];
    const user    = userMap[counter.user_id];
    if (!profile || !user?.email)   { skipped++; continue; }
    if (profile.email_unsubscribed) { skipped++; continue; }
    if (recentStreakIds.has(counter.user_id)) { skipped++; continue; }

    const result = await sendEmail(
      user.email,
      templateStreakRecovery(profile.name, counter.current_streak_days),
      "streak_recovery"
    );
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: counter.user_id, email: user.email, email_type: "streak_recovery",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    }
  }

  console.log(`[StreakRecovery] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── First harvest email ───────────────────────────────────────────────────────
// Fires once when harvest_logged_total goes from 0 to 1.
// Picks up the most recent harvest for context.
async function runFirstHarvest(supabase) {
  // Users who have exactly 1 total harvest logged
  const { data: counters } = await supabase
    .from("user_activity_counters")
    .select("user_id, harvest_logged_total")
    .eq("harvest_logged_total", 1);

  if (!counters?.length) return { sent: 0, skipped: 0 };

  // Exclude users already sent this email
  const { data: alreadySent } = await supabase
    .from("email_log").select("user_id").eq("email_type", "first_harvest");
  const sentIds = new Set((alreadySent || []).map(e => e.user_id));

  const { data: profiles } = await supabase
    .from("profiles").select("id, name, email_unsubscribed");
  const profileMap = {};
  (profiles || []).forEach(p => { profileMap[p.id] = p; });

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  let sent = 0, skipped = 0;

  for (const counter of counters) {
    if (sentIds.has(counter.user_id)) { skipped++; continue; }
    const profile = profileMap[counter.user_id];
    const user    = userMap[counter.user_id];
    if (!profile || !user?.email)   { skipped++; continue; }
    if (profile.email_unsubscribed) { skipped++; continue; }

    // Fetch the harvest for context
    const { data: harvest } = await supabase
      .from("harvest_log")
      .select("crop:crop_instance_id(name), quantity_g")
      .eq("user_id", counter.user_id)
      .order("created_at", { ascending: false })
      .limit(1)
      .single();

    const cropName  = harvest?.crop?.name || "your first crop";
    const quantityG = harvest?.quantity_g || null;

    const result = await sendEmail(
      user.email,
      templateFirstHarvest(profile.name, cropName, quantityG),
      "first_harvest"
    );
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: counter.user_id, email: user.email, email_type: "first_harvest",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    }
  }

  console.log(`[FirstHarvest] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Badge unlock email ────────────────────────────────────────────────────────
// Fires for badge_unlock_events not yet shown to user AND where user doesn't
// have active push tokens (push would already have shown the celebration).
async function runBadgeUnlockEmails(supabase) {
  // Users with push active — skip them, push covers it
  const { data: pushTokens } = await supabase
    .from("device_push_tokens").select("user_id").eq("is_active", true);
  const hasPushIds = new Set((pushTokens || []).map(t => t.user_id));

  // Unshown badge unlock events
  const { data: unlocks } = await supabase
    .from("badge_unlock_events")
    .select("id, user_id, unlocked_at, badge:badge_id(title, description, celebration_copy)")
    .eq("shown_to_user", false)
    .order("unlocked_at", { ascending: true });

  if (!unlocks?.length) return { sent: 0, skipped: 0 };

  // Dedupe: only send one badge email per user per run (first unlock wins)
  const seenUsers = new Set();

  const { data: profiles } = await supabase
    .from("profiles").select("id, name, email_unsubscribed");
  const profileMap = {};
  (profiles || []).forEach(p => { profileMap[p.id] = p; });

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // 7-day cooldown
  const recentMap = await buildRecentNonTransactionalMap(supabase);
  const now = Date.now();

  let sent = 0, skipped = 0;

  for (const unlock of unlocks) {
    if (hasPushIds.has(unlock.user_id))  { skipped++; continue; }
    if (seenUsers.has(unlock.user_id))   { skipped++; continue; }
    const profile = profileMap[unlock.user_id];
    const user    = userMap[unlock.user_id];
    if (!profile || !user?.email)        { skipped++; continue; }
    if (profile.email_unsubscribed)      { skipped++; continue; }
    if (recentMap[unlock.user_id] && (now - recentMap[unlock.user_id]) < 7 * 24 * 3600000) { skipped++; continue; }

    const { title, description, celebration_copy } = unlock.badge || {};
    if (!title) { skipped++; continue; }

    const result = await sendEmail(
      user.email,
      templateBadgeUnlock(profile.name, title, description, celebration_copy),
      "badge_unlock"
    );
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: unlock.user_id, email: user.email, email_type: "badge_unlock",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      // Mark shown so we don't re-send on next run
      await supabase.from("badge_unlock_events")
        .update({ shown_to_user: true }).eq("id", unlock.id);
      seenUsers.add(unlock.user_id);
      sent++;
    }
  }

  console.log(`[BadgeUnlockEmails] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Upgrade prompt ────────────────────────────────────────────────────────────
// Fires within 24h of a free user hitting their Why Now / PlantCheck / Boost limit.
// Checks: why_log count = 3, diagnosis_log count = 3, boost_uses = 3.
async function runUpgradePrompt(supabase) {
  const FREE_LIMIT = 3;

  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name, email_unsubscribed, plan, boost_uses");
  if (!profiles?.length) return { sent: 0, skipped: 0 };

  const freeProfiles = profiles.filter(p => (!p.plan || p.plan === "free") && !p.email_unsubscribed);
  if (!freeProfiles.length) return { sent: 0, skipped: 0 };

  // Already sent upgrade_prompt
  const { data: alreadySent } = await supabase
    .from("email_log").select("user_id").eq("email_type", "upgrade_prompt");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // Why Now usage — users who hit exactly 3 in last 48h
  const twoDaysAgo = new Date(Date.now() - 2 * 86400000).toISOString();
  const { data: whyNowLogs } = await supabase
    .from("why_log")
    .select("user_id")
    .gte("created_at", twoDaysAgo);
  const whyNowRecent = {};
  (whyNowLogs || []).forEach(r => {
    whyNowRecent[r.user_id] = (whyNowRecent[r.user_id] || 0) + 1;
  });
  // Full lifetime counts
  const { data: whyNowAll } = await supabase.from("why_log").select("user_id");
  const whyNowTotal = {};
  (whyNowAll || []).forEach(r => {
    whyNowTotal[r.user_id] = (whyNowTotal[r.user_id] || 0) + 1;
  });

  // PlantCheck usage
  const { data: plantCheckLogs } = await supabase
    .from("diagnosis_log")
    .select("user_id")
    .gte("created_at", twoDaysAgo);
  const plantCheckRecent = {};
  (plantCheckLogs || []).forEach(r => {
    plantCheckRecent[r.user_id] = (plantCheckRecent[r.user_id] || 0) + 1;
  });
  const { data: plantCheckAll } = await supabase.from("diagnosis_log").select("user_id");
  const plantCheckTotal = {};
  (plantCheckAll || []).forEach(r => {
    plantCheckTotal[r.user_id] = (plantCheckTotal[r.user_id] || 0) + 1;
  });

  let sent = 0, skipped = 0;

  for (const profile of freeProfiles) {
    if (alreadySentIds.has(profile.id)) { skipped++; continue; }
    const user = userMap[profile.id];
    if (!user?.email) { skipped++; continue; }

    // Determine which limit was hit most recently
    let limitType = null;
    if ((whyNowTotal[profile.id] || 0) >= FREE_LIMIT && (whyNowRecent[profile.id] || 0) > 0) {
      limitType = "why_now";
    } else if ((plantCheckTotal[profile.id] || 0) >= FREE_LIMIT && (plantCheckRecent[profile.id] || 0) > 0) {
      limitType = "plantcheck";
    } else if ((profile.boost_uses || 0) >= FREE_LIMIT) {
      // boost_uses is on profile — no recent check available, just check total
      limitType = "boost";
    }

    if (!limitType) { skipped++; continue; }

    const result = await sendEmail(
      user.email,
      templateUpgradePrompt(profile.name, limitType),
      "upgrade_prompt"
    );
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: profile.id, email: user.email, email_type: "upgrade_prompt",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    }
  }

  console.log(`[UpgradePrompt] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Weekly digest ─────────────────────────────────────────────────────────────
// Sunday only. Users without active push. Have due tasks. Not seen in 24h.
// 7-day cooldown across all non-transactional emails.
async function runWeeklyEmailDigest(supabase) {
  const today = new Date();
  if (today.getDay() !== 0) {
    console.log("[WeeklyDigest] Not Sunday — skipping.");
    return { sent: 0, skipped: 0, reason: "not_sunday" };
  }

  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name, last_seen_at, email_unsubscribed")
    .eq("is_demo", false);
  if (!profiles?.length) return { sent: 0, skipped: 0 };

  // Skip users with active push tokens and push enabled
  const { data: pushTokens } = await supabase
    .from("device_push_tokens").select("user_id").eq("is_active", true);
  const hasPush = new Set((pushTokens || []).map(t => t.user_id));

  const { data: pushPrefs } = await supabase
    .from("notification_preferences").select("user_id, push_enabled");
  const pushEnabled = {};
  (pushPrefs || []).forEach(p => { pushEnabled[p.user_id] = p.push_enabled; });

  // 7-day cooldown
  const recentMap = await buildRecentNonTransactionalMap(supabase);

  // Already sent this week
  const weekStart = new Date();
  weekStart.setHours(0, 0, 0, 0);
  weekStart.setDate(weekStart.getDate() - weekStart.getDay());
  const { data: sentThisWeek } = await supabase
    .from("email_log")
    .select("user_id")
    .eq("email_type", "weekly_digest")
    .gte("sent_at", weekStart.toISOString());
  const sentThisWeekIds = new Set((sentThisWeek || []).map(e => e.user_id));

  // Due tasks — sorted by urgency server-side
  const todayStr = today.toISOString().split("T")[0];
  const { data: dueTasks } = await supabase
    .from("tasks")
    .select("user_id, action, task_type, urgency, crop:crop_instance_id(name)")
    .is("completed_at", null)
    .lte("due_date", todayStr)
    .not("status", "eq", "expired")
    .not("surface_class", "eq", "insight");
  const tasksByUser = {};
  (dueTasks || []).forEach(t => {
    if (!tasksByUser[t.user_id]) tasksByUser[t.user_id] = [];
    tasksByUser[t.user_id].push(t);
  });

  const { data: crops } = await supabase
    .from("crop_instances").select("user_id, name").eq("active", true);
  const cropsByUser = {};
  (crops || []).forEach(c => {
    if (!cropsByUser[c.user_id]) cropsByUser[c.user_id] = [];
    cropsByUser[c.user_id].push(c.name);
  });

  const now = Date.now();
  let sent = 0, skipped = 0;

  for (const profile of profiles) {
    const user = userMap[profile.id];
    if (!user?.email)               { skipped++; continue; }
    if (profile.email_unsubscribed) { skipped++; continue; }
    if (hasPush.has(profile.id) && pushEnabled[profile.id] !== false) { skipped++; continue; }
    if (sentThisWeekIds.has(profile.id)) { skipped++; continue; }
    if (recentMap[profile.id] && (now - recentMap[profile.id]) < 7 * 24 * 3600000) { skipped++; continue; }
    if (profile.last_seen_at && (now - new Date(profile.last_seen_at).getTime()) < 86400000) { skipped++; continue; }

    const userTasks = tasksByUser[profile.id] || [];
    const userCrops = cropsByUser[profile.id]  || [];
    if (userTasks.length === 0) { skipped++; continue; }

    const result = await sendEmail(user.email, templateWeeklyDigest(profile.name, userTasks, userCrops), "weekly_digest");
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: profile.id, email: user.email, email_type: "weekly_digest",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    } else {
      skipped++;
    }
  }

  console.log(`[WeeklyEmailDigest] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Waitlist runners ──────────────────────────────────────────────────────────

async function runWaitlistInvites(supabase) {
  const { data: pending } = await supabase
    .from("waitlist").select("id, email, name")
    .eq("status", "accepted").is("invite_sent_at", null);
  if (!pending?.length) return { sent: 0, skipped: 0 };

  let sent = 0, skipped = 0;
  for (const person of pending) {
    const result = await sendEmail(person.email, templateWaitlistInvite(person.name), "waitlist_invite");
    if (result.sent) {
      await supabase.from("waitlist").update({ invite_sent_at: new Date().toISOString(), nudge_count: 0 }).eq("id", person.id);
      sent++;
    } else { skipped++; }
  }
  console.log(`[WaitlistInvites] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runWaitlistNudges(supabase) {
  const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist").select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted").not("invite_sent_at", "is", null)
    .lte("invite_sent_at", threeDaysAgo).lt("nudge_count", 1);
  if (!invited?.length) return { sent: 0, skipped: 0 };

  const users = await listAllAuthUsers(supabase);
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    if (signedUpEmails.has(person.email?.toLowerCase())) { skipped++; continue; }
    const result = await sendEmail(person.email, templateWaitlistNudge(person.name), "waitlist_nudge");
    if (result.sent) {
      await supabase.from("waitlist").update({ nudge_count: 1, last_nudge_at: new Date().toISOString() }).eq("id", person.id);
      sent++;
    } else { skipped++; }
  }
  console.log(`[WaitlistNudges] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runWaitlistNudges2(supabase) {
  const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist").select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted").not("invite_sent_at", "is", null)
    .lte("invite_sent_at", sevenDaysAgo).lt("nudge_count", 2);
  if (!invited?.length) return { sent: 0, skipped: 0 };

  const users = await listAllAuthUsers(supabase);
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    if (signedUpEmails.has(person.email?.toLowerCase())) { skipped++; continue; }
    if ((person.nudge_count || 0) !== 1) { skipped++; continue; }
    const result = await sendEmail(person.email, templateWaitlistNudge2(person.name), "waitlist_nudge2");
    if (result.sent) {
      await supabase.from("waitlist").update({ nudge_count: 2, last_nudge_at: new Date().toISOString() }).eq("id", person.id);
      sent++;
    } else { skipped++; }
  }
  console.log(`[WaitlistNudges2] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runWaitlistNudges3(supabase) {
  const fourteenDaysAgo = new Date(Date.now() - 14 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist").select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted").not("invite_sent_at", "is", null)
    .lte("invite_sent_at", fourteenDaysAgo).lt("nudge_count", 3);
  if (!invited?.length) return { sent: 0, skipped: 0 };

  const users = await listAllAuthUsers(supabase);
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    if (signedUpEmails.has(person.email?.toLowerCase())) { skipped++; continue; }
    if ((person.nudge_count || 0) !== 2) { skipped++; continue; }
    const result = await sendEmail(person.email, templateWaitlistNudge3(person.name), "waitlist_nudge3");
    if (result.sent) {
      await supabase.from("waitlist").update({ nudge_count: 3, last_nudge_at: new Date().toISOString() }).eq("id", person.id);
      sent++;
    } else { skipped++; }
  }
  console.log(`[WaitlistNudges3] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Onboarding recovery (one-time batch) ─────────────────────────────────────
async function runOnboardingRecovery(supabase) {
  const users = await listAllAuthUsers(supabase);
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: profiles } = await supabase
    .from("profiles").select("id, name").eq("is_demo", false);
  if (!profiles?.length) return { sent: 0, skipped: 0 };

  const { data: cropUsers } = await supabase.from("crop_instances").select("user_id");
  const hasCropIds = new Set((cropUsers || []).map(r => r.user_id));

  const { data: alreadySent } = await supabase
    .from("email_log").select("user_id").eq("email_type", "onboarding_recovery");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const affected = profiles.filter(p => !hasCropIds.has(p.id) && !alreadySentIds.has(p.id));

  let sent = 0, skipped = 0;
  for (const profile of affected) {
    const user = userMap[profile.id];
    if (!user?.email) { skipped++; continue; }
    const result = await sendEmail(user.email, templateOnboardingRecovery(profile.name), "onboarding_recovery");
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: profile.id, email: user.email, email_type: "onboarding_recovery",
        sent_at: new Date().toISOString(), resend_email_id: result.id || null,
      });
      sent++;
    } else { skipped++; }
  }

  console.log(`[OnboardingRecovery] Sent: ${sent}, Skipped: ${skipped}, Total affected: ${affected.length}`);
  return { sent, skipped, total: affected.length };
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXPORTS
// ═══════════════════════════════════════════════════════════════════════════════

module.exports = {
  // Activation
  runNudgeUnactivated,
  runNudgeUnconfirmed,
  // Onboarding
  runFeedbackSequence,
  // Re-engagement
  runReengagement,
  runStreakRecovery,
  // Milestones
  runFirstHarvest,
  runBadgeUnlockEmails,
  // Upgrade
  runUpgradePrompt,
  // Weekly
  runWeeklyEmailDigest,
  // Waitlist
  runWaitlistInvites,
  runWaitlistNudges,
  runWaitlistNudges2,
  runWaitlistNudges3,
  // Recovery
  runOnboardingRecovery,
};

/*
 * NEW CRON ENDPOINTS NEEDED IN api.js
 * ─────────────────────────────────────
 * Add these to api.js alongside the existing cron routes:
 *
 *   app.post("/cron/streak-recovery", async (req, res) => {
 *     try { const r = await runStreakRecovery(supabaseService); res.json(r); }
 *     catch (e) { res.status(500).json({ error: e.message }); }
 *   });
 *
 *   app.post("/cron/first-harvest", async (req, res) => {
 *     try { const r = await runFirstHarvest(supabaseService); res.json(r); }
 *     catch (e) { res.status(500).json({ error: e.message }); }
 *   });
 *
 *   app.post("/cron/badge-unlock-emails", async (req, res) => {
 *     try { const r = await runBadgeUnlockEmails(supabaseService); res.json(r); }
 *     catch (e) { res.status(500).json({ error: e.message }); }
 *   });
 *
 *   app.post("/cron/upgrade-prompt", async (req, res) => {
 *     try { const r = await runUpgradePrompt(supabaseService); res.json(r); }
 *     catch (e) { res.status(500).json({ error: e.message }); }
 *   });
 *
 * VERCEL CRON SCHEDULE (vercel.json) — add:
 *   { "path": "/cron/streak-recovery",    "schedule": "0 8 * * *"   }  daily 8am
 *   { "path": "/cron/first-harvest",      "schedule": "0 9 * * *"   }  daily 9am
 *   { "path": "/cron/badge-unlock-emails","schedule": "0 10 * * *"  }  daily 10am
 *   { "path": "/cron/upgrade-prompt",     "schedule": "0 11 * * *"  }  daily 11am
 *
 * NON_TRANSACTIONAL_EMAIL_TYPES in this file now includes:
 *   streak_recovery, first_harvest, badge_unlock, upgrade_prompt
 * These are subject to the 7-day cooldown alongside existing types.
 * upgrade_prompt is an exception — it should fire regardless of cooldown
 * since hitting a limit is high-intent. Consider removing it from the
 * cooldown list if open rates support it.
 */
