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

function templateFeedbackDay3Active(name, tasksCompleted) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: `${tasksCompleted} tasks down, ${firstName} — you're off to a great start 🌱`,
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">${tasksCompleted} tasks in 3 days — that's a great start, ${firstName}</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You're one of our most active early users and that means a lot — the people who tick off tasks regularly are the ones who get the best harvests at the end of the season.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        Quick question — what made you sign up? Was it a specific problem you were trying to solve? I'm building Vercro to fix real gardening frustrations and hearing from engaged users like you directly shapes what we build next.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">Just hit reply — I read everything personally.</p>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Keep growing →</a>
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

function templateFeedbackDay3Quiet(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "Getting started with Vercro — anything I can help with?",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — getting on OK with Vercro?</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You signed up a few days ago and I just wanted to check in. Sometimes the app takes a few minutes to click — especially once you've added your crops and seen your first daily task list.
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:16px 20px;margin-bottom:24px;border-left:3px solid #2F5D50;">
        <p style="font-size:13px;color:#2F5D50;font-weight:700;margin:0 0 8px;">If you haven't already, try this:</p>
        <p style="font-size:13px;color:#4a4a4a;line-height:1.7;margin:0;">Go to the Crops tab → Add a crop you're growing → Come back to the dashboard. Your personalised task list will appear straight away.</p>
      </div>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">If something isn't working or feels confusing, just hit reply — I'll help you get set up.</p>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Open my garden →</a>
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

function templateFeedbackDay7Active(name, tasksCompleted) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: `A week in and ${tasksCompleted} tasks done — what do you think?`,
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">One week, ${tasksCompleted} tasks — your garden is in good hands, ${firstName}</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        You've been one of our most consistent users this week. That's exactly the habit that makes the difference — small actions compounding over the season.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        Two quick questions — reply with whatever comes to mind:
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:32px;">
        <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0 0 8px;">1. What's the single most useful thing Vercro does for you?</p>
        <p style="font-size:15px;color:#1a1a1a;font-weight:700;margin:0;">2. What's the one thing you wish it did that it doesn't yet?</p>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Open my garden →</a>
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

function templateFeedbackDay7Quiet(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "A week in — is Vercro working for you?",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — honest question</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        It's been a week since you joined Vercro. I noticed you haven't used it much yet — and I'd genuinely like to know why, because it helps me make it better.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        Was it confusing to get started? Not relevant to what you're growing? Or just bad timing? No wrong answers — hit reply and tell me.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        If you'd like another go, your garden is still there. Add a couple of crops and see your personalised plan — it usually clicks at that point.
      </p>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Give it another go →</a>
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
  const { data: profiles } = await supabase.from("profiles").select("id, email_unsubscribed");
  const profileIds = new Set((profiles || []).map(p => p.id));
  const unsubscribedIds = new Set((profiles || []).filter(p => p.email_unsubscribed).map(p => p.id));

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
    // Skip unsubscribed users
    if (unsubscribedIds.has(user.id)) { skipped++; continue; }

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
  // Fetch unsubscribed profile IDs
  const { data: _unsubProfiles } = await supabase.from("profiles").select("id, email_unsubscribed");
  const _unsubIds = new Set((_unsubProfiles || []).filter(p => p.email_unsubscribed).map(p => p.id));
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

    if (_unsubIds.has(user.id)) { skipped++; continue; }
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
    .select("id, name, email_unsubscribed")
    .order("created_at");

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  // Get task completion counts per user
  const { data: taskCounts } = await supabase
    .from("tasks")
    .select("user_id")
    .not("completed_at", "is", null);
  const completedMap = {};
  (taskCounts || []).forEach(t => {
    completedMap[t.user_id] = (completedMap[t.user_id] || 0) + 1;
  });

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
    if (profile.email_unsubscribed) continue;

    const createdAt  = new Date(user.created_at).getTime();
    const daysSince  = (now - createdAt) / 86400000;
    const userSent   = sentMap[profile.id] || new Set();
    const tasksCompleted = completedMap[profile.id] || 0;
    const isActive = tasksCompleted >= 3; // completed 3+ tasks = engaged user

    // Day 3 email — different tone for active vs quiet users
    if (daysSince >= 3 && daysSince < 4 && !userSent.has("feedback_day3")) {
      const template = isActive
        ? templateFeedbackDay3Active(profile.name, tasksCompleted)
        : templateFeedbackDay3Quiet(profile.name);
      const result = await sendEmail(user.email, template);
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email,
          email_type: "feedback_day3", sent_at: new Date().toISOString(),
        });
        sent++;
      }
    }

    // Day 7 email — different tone for active vs quiet users
    if (daysSince >= 7 && daysSince < 8 && !userSent.has("feedback_day7")) {
      const template = isActive
        ? templateFeedbackDay7Active(profile.name, tasksCompleted)
        : templateFeedbackDay7Quiet(profile.name);
      const result = await sendEmail(user.email, template);
      if (result.sent) {
        await supabase.from("email_log").insert({
          user_id: profile.id, email: user.email,
          email_type: "feedback_day7", sent_at: new Date().toISOString(),
        });
        sent++;
      }
    }
  }

  console.log(`[FeedbackSequence] Sent: ${sent}`);
  return { sent };
}

function templateWaitlistInvite(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "You're in — welcome to Vercro 🌱",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">You're in, ${firstName}!</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        Your place on the Vercro beta is ready. Create your free account and we'll build you a personalised growing plan based on your crops, your postcode and the time of year.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        Daily tasks, harvest forecasts, frost alerts, AI crop identification — all in one place, built for UK growers.
      </p>
      <div style="text-align:center;margin-bottom:24px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Create my free account →
        </a>
      </div>
      <div style="background:#f4f8f2;border-radius:12px;padding:16px 20px;margin-bottom:24px;">
        <p style="font-size:13px;color:#2F5D50;font-weight:700;margin:0 0 8px;">Getting started takes 2 minutes:</p>
        <p style="font-size:13px;color:#4a4a4a;line-height:1.6;margin:0;">1. Create your account<br>2. Add your crops<br>3. Get your personalised plan</p>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">No cost · No payment details · Free beta access</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

function templateWaitlistNudge(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: `Still waiting for you, ${firstName} 🌱`,
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
        We sent you an invite to Vercro a few days ago but haven't seen you inside yet. Your spot is still reserved — it only takes 2 minutes to get set up.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        With the growing season getting started, now's the perfect time to get your garden plan in place.
      </p>
      <div style="text-align:center;margin-bottom:24px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Set up my garden →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free to use · No payment needed</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

async function runWaitlistInvites(supabase) {
  // Send invite to accepted waitlist members who haven't had one yet
  const { data: pending } = await supabase
    .from("waitlist")
    .select("id, email, name")
    .eq("status", "accepted")
    .is("invite_sent_at", null);

  if (!pending?.length) return { sent: 0, skipped: 0 };

  let sent = 0, skipped = 0;
  for (const person of pending) {
    const result = await sendEmail(person.email, templateWaitlistInvite(person.name));
    if (result.sent) {
      await supabase.from("waitlist")
        .update({
          invite_sent_at: new Date().toISOString(),
          nudge_count:    0,
        })
        .eq("id", person.id);
      sent++;
    } else {
      skipped++;
    }
  }

  console.log(`[WaitlistInvites] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runWaitlistNudges(supabase) {
  // Nudge people who got an invite 3+ days ago but never signed up (not in auth.users)
  const threeDaysAgo = new Date(Date.now() - 3 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist")
    .select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted")
    .not("invite_sent_at", "is", null)
    .lte("invite_sent_at", threeDaysAgo)
    .lt("nudge_count", 1); // only nudge once

  if (!invited?.length) return { sent: 0, skipped: 0 };

  // Get all auth user emails so we can skip people who already signed up
  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    // Skip if they already created an account
    if (signedUpEmails.has(person.email?.toLowerCase())) {
      skipped++;
      continue;
    }

    const result = await sendEmail(person.email, templateWaitlistNudge(person.name));
    if (result.sent) {
      await supabase.from("waitlist")
        .update({
          nudge_count:   (person.nudge_count || 0) + 1,
          last_nudge_at: new Date().toISOString(),
        })
        .eq("id", person.id);
      sent++;
    } else {
      skipped++;
    }
  }

  console.log(`[WaitlistNudges] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Waitlist day 7 nudge ──────────────────────────────────────────────────────
function templateWaitlistNudge2(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: `The growing season is starting, ${firstName}`,
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">The growing season is getting started</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        Hey ${firstName} — March and April are the busiest months in the UK growing calendar. Sowing windows are opening, frost risk is still real, and getting organised now makes the rest of the season much easier.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        Vercro tells you exactly what to do and when — based on your crops, your postcode and the actual weather. Your spot is still there.
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:16px 20px;margin-bottom:28px;">
        <p style="font-size:13px;color:#2F5D50;font-weight:700;margin:0 0 8px;">What you'll get on day one:</p>
        <p style="font-size:13px;color:#4a4a4a;line-height:1.7;margin:0;">✓ A personalised daily task list<br>✓ Sowing and harvest windows for your crops<br>✓ Frost alerts based on your postcode<br>✓ AI crop identification if you're not sure what you have</p>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Start growing →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free · No card needed · 2 minutes to set up</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Waitlist day 14 final nudge ───────────────────────────────────────────────
function templateWaitlistNudge3(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "One last nudge from me 🌱",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — one last nudge from me</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        I've sent you a couple of emails about Vercro and I don't want to keep pestering you — so this is the last one.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        If the timing isn't right or gardening isn't a priority right now, that's completely fine. Your access isn't going anywhere — you can sign up whenever you're ready.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        But if you do want a growing season where you always know what to do next — Vercro is ready for you.
      </p>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Set up my garden →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:0;">Free · No card needed · Always open</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Re-engagement day 14 ──────────────────────────────────────────────────────
function templateReengageDay14(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "A couple of things we've added to Vercro 🌿",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — a couple of things worth knowing</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        It's been a couple of weeks since you joined. We've been building quickly based on what early users are telling us — here are a few things that might be useful if you haven't tried them yet.
      </p>
      <div style="display:flex;flex-direction:column;gap:0;">
        <div style="border:1px solid #D4E8CE;border-radius:12px;padding:18px 20px;margin-bottom:12px;">
          <p style="font-size:14px;font-weight:700;color:#2F5D50;margin:0 0 6px;">📈 Crop timelines</p>
          <p style="font-size:13px;color:#4a4a4a;line-height:1.6;margin:0;">Tap any crop and hit Timeline to see exactly where it is in its lifecycle — sown, seedling, vegetative, flowering, harvest — with predicted dates for what's coming next.</p>
        </div>
        <div style="border:1px solid #D4E8CE;border-radius:12px;padding:18px 20px;margin-bottom:12px;">
          <p style="font-size:14px;font-weight:700;color:#2F5D50;margin:0 0 6px;">🌿 Companion suggestions</p>
          <p style="font-size:13px;color:#4a4a4a;line-height:1.6;margin:0;">On any bed in your Garden tab, tap the suggestions button — for empty beds it recommends what to plant, for beds with crops it suggests companion plants to improve yield and deter pests.</p>
        </div>
        <div style="border:1px solid #D4E8CE;border-radius:12px;padding:18px 20px;margin-bottom:24px;">
          <p style="font-size:14px;font-weight:700;color:#2F5D50;margin:0 0 6px;">📤 Share your garden</p>
          <p style="font-size:13px;color:#4a4a4a;line-height:1.6;margin:0;">Generate a shareable card showing your recent tasks, crop count and harvest stats — perfect for Instagram or WhatsApp. Find it on the dashboard.</p>
        </div>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Open my garden →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:8px 0 0;">As always — just hit reply if you have questions or feedback.</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Re-engagement day 30 ──────────────────────────────────────────────────────
function templateReengageDay30(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  const month = new Date().toLocaleString("en-GB", { month: "long" });
  return {
    subject: `What's happening in UK gardens this ${month}`,
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hey ${firstName} — ${month} in the garden</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        It's been about a month since you joined Vercro. We've had a great first group of beta users helping us shape the app — and we'd love to have you more involved.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        If you've drifted away, no worries at all — but if you'd like to give it another go, your garden is exactly as you left it. Your crops, your tasks, your growing plan — all still there.
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:18px 20px;margin-bottom:28px;border-left:3px solid #2F5D50;">
        <p style="font-size:14px;color:#2F5D50;font-weight:700;margin:0 0 6px;">What active Vercro growers are doing in ${month}:</p>
        <p style="font-size:13px;color:#4a4a4a;line-height:1.7;margin:0;">Completing daily tasks · Tracking sowing dates · Getting frost alerts · Logging harvests · Confirming crop stages on their timelines</p>
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="https://app.vercro.com" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">
          Back to my garden →
        </a>
      </div>
      <p style="font-size:13px;color:#888;text-align:center;margin:8px 0 0;">Hit reply anytime — I read everything.</p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Waitlist day 7 + day 14 nudge runners ─────────────────────────────────────

async function runWaitlistNudges2(supabase) {
  // Day 7 nudge — invite sent 7+ days ago, nudge_count < 2, not signed up
  const sevenDaysAgo = new Date(Date.now() - 7 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist")
    .select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted")
    .not("invite_sent_at", "is", null)
    .lte("invite_sent_at", sevenDaysAgo)
    .lt("nudge_count", 2);

  if (!invited?.length) return { sent: 0, skipped: 0 };

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    if (signedUpEmails.has(person.email?.toLowerCase())) { skipped++; continue; }
    // nudge_count 1 = already had day-3 nudge, now send day-7
    if ((person.nudge_count || 0) !== 1) { skipped++; continue; }
    const result = await sendEmail(person.email, templateWaitlistNudge2(person.name));
    if (result.sent) {
      await supabase.from("waitlist")
        .update({ nudge_count: 2, last_nudge_at: new Date().toISOString() })
        .eq("id", person.id);
      sent++;
    } else { skipped++; }
  }

  console.log(`[WaitlistNudges2] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

async function runWaitlistNudges3(supabase) {
  // Day 14 final nudge — nudge_count is 2, not signed up
  const fourteenDaysAgo = new Date(Date.now() - 14 * 86400000).toISOString();
  const { data: invited } = await supabase
    .from("waitlist")
    .select("id, email, name, invite_sent_at, nudge_count")
    .eq("status", "accepted")
    .not("invite_sent_at", "is", null)
    .lte("invite_sent_at", fourteenDaysAgo)
    .lt("nudge_count", 3);

  if (!invited?.length) return { sent: 0, skipped: 0 };

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const signedUpEmails = new Set((users || []).map(u => u.email?.toLowerCase()));

  let sent = 0, skipped = 0;
  for (const person of invited) {
    if (signedUpEmails.has(person.email?.toLowerCase())) { skipped++; continue; }
    if ((person.nudge_count || 0) !== 2) { skipped++; continue; }
    const result = await sendEmail(person.email, templateWaitlistNudge3(person.name));
    if (result.sent) {
      await supabase.from("waitlist")
        .update({ nudge_count: 3, last_nudge_at: new Date().toISOString() })
        .eq("id", person.id);
      sent++;
    } else { skipped++; }
  }

  console.log(`[WaitlistNudges3] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

// ── Re-engagement runners ─────────────────────────────────────────────────────

async function runReengagement(supabase) {
  const { data: profiles } = await supabase.from("profiles").select("id, name, email_unsubscribed").order("created_at");
  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: alreadySent } = await supabase.from("email_log").select("user_id, email_type");
  const sentMap = {};
  (alreadySent || []).forEach(e => {
    if (!sentMap[e.user_id]) sentMap[e.user_id] = new Set();
    sentMap[e.user_id].add(e.email_type);
  });

  // 7-day cooldown — don't send re-engagement if a non-transactional email went recently
  const recentMap = await buildRecentNonTransactionalMap(supabase);

  const now = Date.now();
  let sent = 0;

  for (const profile of (profiles || [])) {
    const user = userMap[profile.id];
    if (!user?.email) continue;
    if (profile.email_unsubscribed) continue;

    // 7-day cooldown across all non-transactional emails
    if (recentMap[profile.id] && (now - recentMap[profile.id]) < 7 * 24 * 3600000) continue;

    const daysSince = (now - new Date(user.created_at).getTime()) / 86400000;
    const userSent  = sentMap[profile.id] || new Set();

    // Day 14
    if (daysSince >= 14 && daysSince < 15 && !userSent.has("reengage_day14")) {
      const result = await sendEmail(user.email, templateReengageDay14(profile.name));
      if (result.sent) {
        await supabase.from("email_log").insert({ user_id: profile.id, email: user.email, email_type: "reengage_day14", sent_at: new Date().toISOString() });
        sent++;
      }
    }

    // Day 30
    if (daysSince >= 30 && daysSince < 31 && !userSent.has("reengage_day30")) {
      const result = await sendEmail(user.email, templateReengageDay30(profile.name));
      if (result.sent) {
        await supabase.from("email_log").insert({ user_id: profile.id, email: user.email, email_type: "reengage_day30", sent_at: new Date().toISOString() });
        sent++;
      }
    }
  }

  console.log(`[Reengagement] Sent: ${sent}`);
  return { sent };
}

// ── Daily email fallback templates ───────────────────────────────────────────

function templateGardenToday(name, tasks, cropNames) {
  const firstName = name ? name.split(" ")[0] : "there";
  const taskCount = tasks.length;
  const topTasks  = tasks.slice(0, 3);
  return {
    subject: `Your garden today 🌱 (${taskCount} thing${taskCount !== 1 ? "s" : ""} to do)`,
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 8px;">Good morning, ${firstName}</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 24px;">
        You have <strong>${taskCount} task${taskCount !== 1 ? "s" : ""}</strong> in your garden today.
      </p>
      <div style="background:#f4f8f2;border-radius:12px;padding:20px 24px;margin-bottom:28px;">
        ${topTasks.map(t => `
        <div style="display:flex;align-items:flex-start;gap:12px;margin-bottom:${topTasks.indexOf(t) < topTasks.length - 1 ? "14px" : "0"};">
          <div style="width:20px;height:20px;border-radius:50%;background:#2F5D50;flex-shrink:0;margin-top:2px;"></div>
          <div>
            <div style="font-size:14px;font-weight:700;color:#1a1a1a;">${t.crop?.name || "Garden task"}</div>
            <div style="font-size:13px;color:#4a4a4a;line-height:1.5;">${t.action}</div>
          </div>
        </div>`).join("")}
        ${taskCount > 3 ? `<div style="font-size:13px;color:#6E6E6E;margin-top:14px;padding-top:14px;border-top:1px solid #D4E8CE;">+ ${taskCount - 3} more task${taskCount - 3 !== 1 ? "s" : ""} in the app</div>` : ""}
      </div>
      <div style="text-align:center;margin-bottom:16px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Open my garden →</a>
      </div>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Vercro · Built for UK growers · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

// ── Onboarding recovery email ─────────────────────────────────────────────────

function templateOnboardingRecovery(name) {
  const firstName = name ? name.split(" ")[0] : "there";
  return {
    subject: "Sorry — we found and fixed an issue with your Vercro setup",
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
      <h1 style="font-family:Georgia,serif;font-size:22px;color:#1a1a1a;margin:0 0 16px;">Hi ${firstName} — I wanted to email you personally</h1>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        We found an issue that may have affected your account when you first set up Vercro. In some cases, the crops you chose during onboarding weren't saved properly — which meant your personalised plan and daily tasks never generated as they should have.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        That's now been fixed.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 32px;">
        If you open Vercro and re-add your crops, your personalised plan will build straight away — daily tasks, harvest forecasts, frost alerts, all based on exactly what you're growing.
      </p>
      <div style="text-align:center;margin-bottom:32px;">
        <a href="${APP_URL}" style="display:inline-block;background:#2F5D50;color:#ffffff;text-decoration:none;border-radius:12px;padding:16px 36px;font-family:Georgia,serif;font-size:16px;font-weight:700;">Set up my garden →</a>
      </div>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0 0 16px;">
        I'm really sorry about this — it was entirely on our side. I appreciate your patience and I'm grateful you gave Vercro a try.
      </p>
      <p style="font-size:15px;color:#4a4a4a;line-height:1.7;margin:0;">
        If you have any questions just reply to this email — I read everything personally.
      </p>
    </div>
    <div style="background:#f4f8f2;padding:20px 40px;text-align:center;border-top:1px solid #D4E8CE;">
      <p style="font-size:12px;color:#888;margin:0;">Mark · Founder of Vercro · <a href="https://vercro.com" style="color:#2F5D50;">vercro.com</a></p>
    </div>
  </div>
</body>
</html>`,
  };
}

async function runOnboardingRecovery(supabase) {
  const resend = getResend();
  if (!resend) return { sent: 0, skipped: 0, reason: "resend_not_configured" };

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name")
    .eq("is_demo", false);

  if (!profiles?.length) return { sent: 0, skipped: 0 };

  // Users with no crops at all
  const { data: cropUsers } = await supabase.from("crop_instances").select("user_id");
  const hasCropIds = new Set((cropUsers || []).map(r => r.user_id));

  // Skip anyone already sent this email
  const { data: alreadySent } = await supabase
    .from("email_log").select("user_id").eq("email_type", "onboarding_recovery");
  const alreadySentIds = new Set((alreadySent || []).map(e => e.user_id));

  const affected = profiles.filter(p => !hasCropIds.has(p.id) && !alreadySentIds.has(p.id));

  let sent = 0, skipped = 0;
  for (const profile of affected) {
    const user = userMap[profile.id];
    if (!user?.email) { skipped++; continue; }
    const template = templateOnboardingRecovery(profile.name);
    const result = await sendEmail(user.email, template);
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id: profile.id, email: user.email,
        email_type: "onboarding_recovery", sent_at: new Date().toISOString(),
      });
      sent++;
    } else { skipped++; }
  }

  console.log(`[OnboardingRecovery] Sent: ${sent}, Skipped: ${skipped}, Total affected: ${affected.length}`);
  return { sent, skipped, total: affected.length };
}

// ── Weekly email digest runner ────────────────────────────────────────────────
// Replaces the old daily fallback. Only sends on Sundays. Only sends if the
// user has tasks due. Respects a 7-day cooldown across all non-transactional
// emails so users never feel clustered.
//
// Triggered from POST /cron/weekly-digest (called by Vercel Cron on Sundays).
// No longer called from /cron/push-morning.
//
// Non-transactional email types (subject to cooldown):
//   weekly_digest, reengage_day14, reengage_day30, feedback_day3, feedback_day7
//
// Transactional (NOT subject to cooldown — always send):
//   nudge_unactivated, nudge_unconfirmed, onboarding_recovery

const NON_TRANSACTIONAL_EMAIL_TYPES = [
  "weekly_digest",
  "reengage_day14",
  "reengage_day30",
  "feedback_day3",
  "feedback_day7",
];

// Returns the most recent sent_at (ms) for any non-transactional email per user.
// Used by both runWeeklyEmailDigest and runReengagement for cooldown enforcement.
async function buildRecentNonTransactionalMap(supabase) {
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 3600000).toISOString();
  const { data: recentEmails } = await supabase
    .from("email_log")
    .select("user_id, email_type, sent_at")
    .in("email_type", NON_TRANSACTIONAL_EMAIL_TYPES)
    .gte("sent_at", sevenDaysAgo);

  // Map: user_id → most recent sent_at ms
  const map = {};
  (recentEmails || []).forEach(e => {
    const ms = new Date(e.sent_at).getTime();
    if (!map[e.user_id] || ms > map[e.user_id]) map[e.user_id] = ms;
  });
  return map;
}

async function runWeeklyEmailDigest(supabase) {
  // Only run on Sundays (day 0). Cron is scheduled for Sunday but this guard
  // makes it safe to call manually without spamming on wrong days.
  const today = new Date();
  if (today.getDay() !== 0) {
    console.log("[WeeklyDigest] Not Sunday — skipping.");
    return { sent: 0, skipped: 0, reason: "not_sunday" };
  }

  const { data: { users } } = await supabase.auth.admin.listUsers({ perPage: 1000 });
  const userMap = {};
  (users || []).forEach(u => { userMap[u.id] = u; });

  const { data: profiles } = await supabase
    .from("profiles")
    .select("id, name, last_seen_at, email_unsubscribed")
    .eq("is_demo", false);
  if (!profiles?.length) return { sent: 0, skipped: 0 };

  // Skip users with active push tokens and push enabled — push covers them
  const { data: pushTokens } = await supabase
    .from("device_push_tokens").select("user_id").eq("is_active", true);
  const hasPush = new Set((pushTokens || []).map(t => t.user_id));

  const { data: pushPrefs } = await supabase
    .from("notification_preferences").select("user_id, push_enabled");
  const pushEnabled = {};
  (pushPrefs || []).forEach(p => { pushEnabled[p.user_id] = p.push_enabled; });

  // 7-day cooldown — skip anyone who got a non-transactional email in last 7 days
  const recentMap = await buildRecentNonTransactionalMap(supabase);

  // Already sent this week's digest
  const weekStart = new Date();
  weekStart.setHours(0, 0, 0, 0);
  weekStart.setDate(weekStart.getDate() - weekStart.getDay()); // last Sunday 00:00
  const { data: sentThisWeek } = await supabase
    .from("email_log")
    .select("user_id")
    .eq("email_type", "weekly_digest")
    .gte("sent_at", weekStart.toISOString());
  const sentThisWeekIds = new Set((sentThisWeek || []).map(e => e.user_id));

  // Tasks due this week per user
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

  // Crops per user for digest copy
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
    if (!user?.email) { skipped++; continue; }
    if (profile.email_unsubscribed) { skipped++; continue; }

    // Push covers this user — skip
    if (hasPush.has(profile.id) && pushEnabled[profile.id] !== false) { skipped++; continue; }

    // Already got a digest this week
    if (sentThisWeekIds.has(profile.id)) { skipped++; continue; }

    // 7-day cooldown — another non-transactional email sent recently
    if (recentMap[profile.id] && (now - recentMap[profile.id]) < 7 * 24 * 3600000) { skipped++; continue; }

    // Opened the app in the last 24 hours — no need to email
    if (profile.last_seen_at) {
      if (now - new Date(profile.last_seen_at).getTime() < 24 * 3600000) { skipped++; continue; }
    }

    const userTasks = tasksByUser[profile.id] || [];
    const userCrops = cropsByUser[profile.id]  || [];

    // Only send if there are actual due tasks — no nagging when garden is quiet
    if (userTasks.length === 0) { skipped++; continue; }

    const template = templateGardenToday(profile.name, userTasks, userCrops);
    const result   = await sendEmail(user.email, template);
    if (result.sent) {
      await supabase.from("email_log").insert({
        user_id:    profile.id,
        email:      user.email,
        email_type: "weekly_digest",
        sent_at:    new Date().toISOString(),
      });
      sent++;
    } else {
      skipped++;
    }
  }

  console.log(`[WeeklyEmailDigest] Sent: ${sent}, Skipped: ${skipped}`);
  return { sent, skipped };
}

module.exports = { runNudgeUnactivated, runNudgeUnconfirmed, runFeedbackSequence, runWaitlistInvites, runWaitlistNudges, runWaitlistNudges2, runWaitlistNudges3, runReengagement, runWeeklyEmailDigest, runOnboardingRecovery };