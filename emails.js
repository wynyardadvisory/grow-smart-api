"use strict";

/**
 * Vercro Email Sequences
 * ─────────────────────────────────────────────────────────────
 * Three sequences sent via Resend:
 *
 * 1. nudge-unactivated  — confirmed email but never completed onboarding
 * 2. nudge-unconfirmed  — signed up but never confirmed email
 * 3. feedback-sequence  — active users at day 3 and day 7
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

const FROM = "Vercro <hello@vercro.com>";
const APP_URL = "https://app.vercro.com";

// ── Email templates ───────────────────────────────────────────────────────────

function templateNudgeUnactivated(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "Your Vercro garden is waiting 🌱",
    html: `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8f2;font-family:Georgia,serif;">
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 16px rgba(47,93,80,0.08);">
    <div style="background:#2F5D50;padding:32px 40px;text-align:center;">
      <div style="font-size:32px;margin-bottom:8px;">🌱</div>
      <div style="font-family:Georgia,serif;font-size:24px;font-weight:700;color:#ffffff;">Vercro</div>
    </div>
    <div style="padding:40px;">
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — your garden is waiting</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You've got access to Vercro but haven't set up your garden yet. It only takes a couple of minutes — add your crops and we'll build you a personalised growing plan straight away.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        Daily task reminders, harvest forecasts, frost alerts — all based on exactly what you're growing.
      </p>
      <div style="text-align:center;margin-bottom:32px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Set up my garden →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">
        Takes about 2 minutes. No payment needed.
      </p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Vercro · Built for UK growers · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

function templateNudgeUnconfirmed(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "Please confirm your Vercro account ✉️",
    html: `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8f2;font-family:Georgia,serif;">
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 16px rgba(47,93,80,0.08);">
    <div style="background:#2F5D50;padding:32px 40px;text-align:center;">
      <div style="font-size:32px;margin-bottom:8px;">🌱</div>
      <div style="font-family:Georgia,serif;font-size:24px;font-weight:700;color:#ffffff;">Vercro</div>
    </div>
    <div style="padding:40px;">
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — just one step left</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You signed up for Vercro but haven't confirmed your email yet. Check your inbox for a confirmation email from us and click the link inside to activate your account.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        Can't find it? Check your spam folder — sometimes it ends up there.
      </p>
      <div style="text-align:center;margin-bottom:32px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Go to Vercro →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">
        If you didn't sign up for Vercro, you can safely ignore this email.
      </p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Vercro · Built for UK growers · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

function templateFeedbackDay3(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "How's Vercro working for you so far?",
    html: `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8f2;font-family:Georgia,serif;">
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 16px rgba(47,93,80,0.08);">
    <div style="background:#2F5D50;padding:32px 40px;text-align:center;">
      <div style="font-size:32px;margin-bottom:8px;">🌱</div>
      <div style="font-family:Georgia,serif;font-size:24px;font-weight:700;color:#ffffff;">Vercro</div>
    </div>
    <div style="padding:40px;">
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — how's it going?</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You've been using Vercro for a few days now. I'd love to know what you think — what's working well, what's confusing, what's missing.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        Just hit reply and tell me — I read every response personally.
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:32px;border-left:3px solid #2F5D50;">
        <p style="font-size:14px;color:#2F5D50;font-weight:700;margin:0 0 4px;">A couple of things worth knowing:</p>
        <ul style="font-size:14px;color:#4a4a4a;line-height:1.7;margin:8px 0 0;padding-left:20px;">
          <li>You can add more crops from the Crops tab</li>
          <li>Your daily tasks update every morning based on your garden</li>
          <li>Tap any task to complete it and track your progress</li>
        </ul>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Open my garden →
        </a>
      </div>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

function templateFeedbackDay7(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "A week in — what do you think of Vercro?",
    html: `
<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f4f8f2;font-family:Georgia,serif;">
  <div style="max-width:560px;margin:40px auto;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 16px rgba(47,93,80,0.08);">
    <div style="background:#2F5D50;padding:32px 40px;text-align:center;">
      <div style="font-size:32px;margin-bottom:8px;">🌱</div>
      <div style="font-family:Georgia,serif;font-size:24px;font-weight:700;color:#ffffff;">Vercro</div>
    </div>
    <div style="padding:40px;">
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">One week with Vercro, ${firstName}</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        It's been a week since you joined. I hope the daily tasks and reminders have been useful — we're still in early beta and your feedback directly shapes what we build next.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        Two quick questions — just hit reply:
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:32px;">
        <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0 0 8px;">1. What's the one thing Vercro does that you find most useful?</p>
        <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0;">2. What's the one thing that's missing or frustrating?</p>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Open my garden →
        </a>
      </div>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Send helper ───────────────────────────────────────────────────────────────

async function sendEmail(to, template) {
  const resend = getResend();
  if (!resend) return { sent: false, reason: "resend_not_configured" };
  try {
    const { data, error } = await resend.emails.send({
      from:    FROM,
      to,
      subject: template.subject,
      html:    template.html,
    });
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

// ── Sequence runners ──────────────────────────────────────────────────────────

async function runNudgeUnactivated(supabase) {
  // Find users who confirmed email but never completed onboarding (no profile row)
  // Only nudge once — check email_logs table, or use a simple time window
  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });

  // Get all profile IDs (users who completed onboarding)
  const { data: profiles } = await supabase.from("profiles").select("id");
  const profileIds = new Set((profiles || []).map(p => p.id));

  // Get already-nudged users from email_log
  const { data: alreadySent } = await supabase
    .from("email_log")
    .select("user_id")
    .eq("email_type", "nudge_unactivated");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const now = Date.now();
  let sent = 0, skipped = 0;

  for (const user of (users || [])) {
    // Must have confirmed email
    if (!user.email_confirmed_at) { skipped++; continue; }
    // Must NOT have a profile (never onboarded)
    if (profileIds.has(user.id)) { skipped++; continue; }
    // Must have been confirmed for at least 24 hours
    const confirmedAt = new Date(user.email_confirmed_at).getTime();
    if (now - confirmedAt < 24 * 3600000) { skipped++; continue; }
    // Must not have been nudged before
    if (alreadySentIds.has(user.id)) { skipped++; continue; }

    const result = await sendEmail(user.email, templateNudgeUnactivated(user.user_metadata?.full_name || null));
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id:    user.id,
        email:      user.email,
        email_type: "nudge_unactivated",
        sent_at:    new Date().toISOString(),
      });
      sent++;
    }
  }

  console.log(`[NudgeUnactivated] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runNudgeUnconfirmed(supabase) {
  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });

  const { data: alreadySent } = await supabase
    .from("email_log")
    .select("user_id")
    .eq("email_type", "nudge_unconfirmed");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const now = Date.now();
  let sent = 0, skipped = 0;

  for (const user of (users || [])) {
    // Must NOT have confirmed email
    if (user.email_confirmed_at) { skipped++; continue; }
    // Must have signed up at least 2 hours ago
    const createdAt = new Date(user.created_at).getTime();
    if (now - createdAt < 2 * 3600000) { skipped++; continue; }
    // Must not have been nudged before
    if (alreadySentIds.has(user.id)) { skipped++; continue; }

    const result = await sendEmail(user.email, templateNudgeUnconfirmed(user.user_metadata?.full_name || null));
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id:    user.id,
        email:      user.email,
        email_type: "nudge_unconfirmed",
        sent_at:    new Date().toISOString(),
      });
      sent++;
    }
  }

  console.log(`[NudgeUnconfirmed] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runFeedbackSequence(supabase) {
  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name")
    .order("created_at");

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: alreadySent } = await supabase
    .from("email_log")
    .select("user_id, email_type");
  const sentMap = {};
  (alreadySent || []).forEach(e => {
    if (!sentMap[e.user_id]) sentMap[e.user_id] = new Set();
    sentMap[e.user_id].add(e.email_type);
  });

  const now = Date.now();
  let sent = 0;

  for (const profile of (profiles || [])) {
    const user = userMap[profile.id];
    if (!user?.email) continue;

    const createdAt  = new Date(user.created_at).getTime();
    const daysSince  = (now - createdAt) / 86400000;
    const userSent   = sentMap[profile.id] || new Set();

    // Day 3 email
    if (daysSince >= 3 && daysSince < 4 && !userSent.has("feedback_day3")) {
      const result = await sendEmail(user.email, templateFeedbackDay3(profile.name));
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id:    profile.id,
          email:      user.email,
          email_type: "feedback_day3",
          sent_at:    new Date().toISOString(),
        });
        sent++;
      }
    }

    // Day 7 email
    if (daysSince >= 7 && daysSince < 8 && !userSent.has("feedback_day7")) {
      const result = await sendEmail(user.email, templateFeedbackDay7(profile.name));
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id:    profile.id,
          email:      user.email,
          email_type: "feedback_day7",
          sent_at:    new Date().toISOString(),
        });
        sent++;
      }
    }
  }

  console.log(`[FeedbackSequence] Sent: ${sent}`);
  return { sent };
}

module.exports = { runNudgeUnactivated, runNudgeUnconfirmed, runFeedbackSequence };
