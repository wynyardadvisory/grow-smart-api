/**
 * GROW SMART — Main App
 * React PWA. Connects to the Grow Smart API.
 *
 * Set NEXT_PUBLIC_API_URL in your .env.local:
 *   NEXT_PUBLIC_API_URL=https://your-api.vercel.app
 *
 * Auth is handled by Supabase JS client.
 * All API calls send the Supabase JWT as Bearer token.
 */

import { useState, useEffect, useCallback } from "react";
import { createClient } from "@supabase/supabase-js";

// ── Supabase client (frontend) ────────────────────────────────────────────────
const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
);

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

// ── Design tokens ─────────────────────────────────────────────────────────────
const C = {
  forest:    "#2F5D50",
  sage:      "#A8C1B5",
  offwhite:  "#F7F6F2",
  stone:     "#6E6E6E",
  leaf:      "#6FAF63",
  amber:     "#D9A441",
  red:       "#C65A5A",
  border:    "#E2E0DA",
  cardBg:    "#FFFFFF",
};

// ── API helper ────────────────────────────────────────────────────────────────
async function apiFetch(path, options = {}) {
  const { data: { session } } = await supabase.auth.getSession();
  const token = session?.access_token;
  const res = await fetch(`${API}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...options.headers,
    },
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `API error ${res.status}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

// ── Helpers ───────────────────────────────────────────────────────────────────
// Supabase joins can return variety as a nested object {name, days_to_maturity_min}
// or as a plain string depending on the query. This always returns a safe string.
function varietyName(v) {
  if (!v) return null;
  if (typeof v === "string") return v;
  if (typeof v === "object") return v.name || null;
  return null;
}
const inputStyle = {
  width: "100%", padding: "12px 14px",
  border: `1px solid ${C.border}`, borderRadius: 10,
  fontSize: 14, background: C.cardBg, color: "#222",
  outline: "none", boxSizing: "border-box",
  appearance: "none", fontFamily: "inherit",
};

const labelStyle = {
  fontSize: 12, fontWeight: 700, color: C.stone,
  letterSpacing: 1, textTransform: "uppercase",
  marginBottom: 6, display: "block",
};

function SectionLabel({ children }) {
  return (
    <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: 2, color: C.stone, textTransform: "uppercase", marginBottom: 12, marginTop: 8, paddingBottom: 6, borderBottom: `1px solid ${C.border}` }}>
      {children}
    </div>
  );
}

function Spinner() {
  return <div style={{ textAlign: "center", padding: 40, color: C.stone, fontSize: 14 }}>Loading…</div>;
}

function ErrorMsg({ msg }) {
  return <div style={{ background: "#fdf0f0", border: `1px solid ${C.red}`, borderRadius: 10, padding: "12px 16px", color: C.red, fontSize: 13, marginBottom: 16 }}>{msg}</div>;
}

// ── Auth screen ───────────────────────────────────────────────────────────────
function AuthScreen({ onAuth }) {
  const [email, setEmail]       = useState("");
  const [password, setPassword] = useState("");
  const [isSignUp, setIsSignUp] = useState(false);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState(null);
  const [sent, setSent]         = useState(false);

  const handle = async () => {
    setLoading(true); setError(null);
    try {
      if (isSignUp) {
        const { error } = await supabase.auth.signUp({ email, password });
        if (error) throw error;
        setSent(true);
      } else {
        const { data, error } = await supabase.auth.signInWithPassword({ email, password });
        if (error) throw error;
        onAuth(data.session);
      }
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  if (sent) return (
    <div style={{ padding: 32, textAlign: "center" }}>
      <div style={{ fontSize: 32, marginBottom: 16 }}>🌱</div>
      <div style={{ fontFamily: "serif", fontSize: 20, fontWeight: 700 }}>Check your email</div>
      <div style={{ color: C.stone, marginTop: 8, fontSize: 14 }}>We sent a confirmation link to {email}</div>
    </div>
  );

  return (
    <div style={{ padding: "40px 24px", maxWidth: 400, margin: "0 auto" }}>
      <div style={{ textAlign: "center", marginBottom: 32 }}>
        <div style={{ fontSize: 36 }}>🌱</div>
        <div style={{ fontFamily: "serif", fontSize: 26, fontWeight: 700, color: C.forest, marginTop: 8 }}>Vercro</div>
        <div style={{ color: C.stone, fontSize: 13, marginTop: 4 }}>{isSignUp ? "Create your account" : "Sign in to your garden"}</div>
      </div>
      {error && <ErrorMsg msg={error} />}
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        <div><label style={labelStyle}>Email</label><input type="email" value={email} onChange={e => setEmail(e.target.value)} style={inputStyle} placeholder="you@example.com" /></div>
        <div><label style={labelStyle}>Password</label><input type="password" value={password} onChange={e => setPassword(e.target.value)} style={inputStyle} placeholder="••••••••" /></div>
        <button onClick={handle} disabled={loading || !email || !password} style={{ background: (!email || !password) ? C.border : C.forest, color: (!email || !password) ? C.stone : "#fff", border: "none", borderRadius: 12, padding: 14, fontSize: 15, fontWeight: 700, cursor: "pointer", fontFamily: "serif" }}>
          {loading ? "…" : isSignUp ? "Create account" : "Sign in"}
        </button>
        <button onClick={() => setIsSignUp(!isSignUp)} style={{ background: "none", border: "none", color: C.forest, fontSize: 13, cursor: "pointer", textDecoration: "underline" }}>
          {isSignUp ? "Already have an account? Sign in" : "No account? Sign up"}
        </button>
      </div>
    </div>
  );
}

// ── Dashboard ─────────────────────────────────────────────────────────────────
function Dashboard() {
  const [data,         setData]        = useState(null);
  const [loading,      setLoading]     = useState(true);
  const [error,        setError]       = useState(null);
  const [completed,    setCompleted]   = useState(new Set());  // completed this session
  const [undoQueue,    setUndoQueue]   = useState({});         // taskId -> timeout
  const [recentlyDone, setRecentlyDone] = useState([]);        // task objects completed this session
  const [undone,       setUndone]      = useState([]);         // task objects un-completed this session

  const load = useCallback(async () => {
    try {
      const d = await apiFetch("/dashboard");
      setData(d);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const completeTask = async (task) => {
    setCompleted(prev => new Set([...prev, task.id]));
    setRecentlyDone(prev => [task, ...prev.filter(t => t.id !== task.id)]);
    setUndone(prev => prev.filter(t => t.id !== task.id));

    try {
      await apiFetch(`/tasks/${task.id}/complete`, { method: "POST" });
    } catch {
      setCompleted(prev => { const s = new Set(prev); s.delete(task.id); return s; });
      setRecentlyDone(prev => prev.filter(t => t.id !== task.id));
      return;
    }

    // Undo window — 10 seconds
    const timeout = setTimeout(() => {
      setUndoQueue(prev => { const q = { ...prev }; delete q[task.id]; return q; });
    }, 10000);
    setUndoQueue(prev => ({ ...prev, [task.id]: timeout }));
  };

  const undoComplete = async (task) => {
    clearTimeout(undoQueue[task.id]);
    setUndoQueue(prev => { const q = { ...prev }; delete q[task.id]; return q; });

    // Remove from completed — this makes it active again
    setCompleted(prev => { const s = new Set(prev); s.delete(task.id); return s; });
    setRecentlyDone(prev => prev.filter(t => t.id !== task.id));
    // Add to undone so it appears in active list even if not in data.tasks
    setUndone(prev => [task, ...prev.filter(t => t.id !== task.id)]);

    try {
      await apiFetch(`/tasks/${task.id}/uncomplete`, { method: "POST" });
    } catch {
      // Revert — mark as completed again
      setCompleted(prev => new Set([...prev, task.id]));
      setUndone(prev => prev.filter(t => t.id !== task.id));
      setRecentlyDone(prev => [task, ...prev.filter(t => t.id !== task.id)]);
    }
  };

  if (loading) return <Spinner />;
  if (error)   return <ErrorMsg msg={error} />;
  if (!data)   return null;

  const today   = todayISO();
  const weekEnd = weekEndISO();

  // Merge server tasks with any tasks un-completed this session
  const serverTasks = [
    ...(data.tasks.today     || []),
    ...(data.tasks.this_week || []),
    ...(data.tasks.coming_up || []),
  ];
  const allTaskIds = new Set(serverTasks.map(t => t.id));
  const extraTasks = undone.filter(t => !allTaskIds.has(t.id)); // undone tasks not yet in server data
  const allTasks   = [...serverTasks, ...extraTasks];

  // Re-group with undone tasks included
  const grouped = {
    today:     allTasks.filter(t => t.due_date === today),
    this_week: allTasks.filter(t => t.due_date > today && t.due_date <= weekEnd),
    coming_up: allTasks.filter(t => t.due_date > weekEnd),
  };

  const activeTodayCount = grouped.today.filter(t => !completed.has(t.id)).length;
  const totalToday       = grouped.today.length;
  const doneToday        = totalToday - activeTodayCount;
  const h = new Date().getHours();
  const greeting = h < 12 ? "Good morning" : h < 17 ? "Good afternoon" : "Good evening";

  return (
    <div>
      {/* Header */}
      <div style={{ background: C.forest, color: "#fff", borderRadius: 14, padding: "16px 20px", marginBottom: 12 }}>
        <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 2, letterSpacing: 1 }}>{greeting}{data.user ? `, ${data.user}` : ""}</div>
        <div style={{ fontSize: 12, opacity: 0.7, marginTop: 4 }}>{new Date().toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long" })}</div>
      </div>

      {/* Weather + traffic lights strip */}
      <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "12px 16px", marginBottom: 16, display: "flex", alignItems: "center", gap: 12 }}>
        {/* Weather */}
        <div style={{ display: "flex", alignItems: "center", gap: 8, flex: 1 }}>
          {data.weather?.icon_code && (
            <img
              src={`https://openweathermap.org/img/wn/${data.weather.icon_code}.png`}
              alt={data.weather.condition}
              style={{ width: 36, height: 36 }}
            />
          )}
          {data.weather ? (
            <div>
              <div style={{ fontSize: 20, fontWeight: 700, color: C.forest, fontFamily: "serif", lineHeight: 1 }}>{data.weather.temp_c}°C</div>
              <div style={{ fontSize: 11, color: C.stone, textTransform: "capitalize", marginTop: 1 }}>{data.weather.condition}</div>
            </div>
          ) : (
            <div style={{ fontSize: 12, color: C.stone }}>No weather data — set postcode in profile</div>
          )}
        </div>

        {/* Traffic lights */}
        <div style={{ display: "flex", gap: 10 }}>
          {/* Frost risk */}
          {(() => {
            const risk   = data.frost_risk || "low";
            const colour = risk === "high" ? "#e74c3c" : risk === "medium" ? "#f39c12" : "#27ae60";
            const label  = risk === "high" ? "Frost risk" : risk === "medium" ? "Near frost" : "No frost";
            return (
              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 3 }}>
                <div style={{ width: 14, height: 14, borderRadius: "50%", background: colour, boxShadow: `0 0 6px ${colour}88` }} />
                <div style={{ fontSize: 9, color: C.stone, textAlign: "center", lineHeight: 1.2 }}>❄️ {label}</div>
              </div>
            );
          })()}

          {/* Pest risk */}
          {(() => {
            const risk   = data.pest_risk || "low";
            const colour = risk === "high" ? "#e74c3c" : risk === "medium" ? "#f39c12" : "#27ae60";
            const label  = risk === "high" ? "High pest" : risk === "medium" ? "Pest alert" : "Low pest";
            const tip    = data.pest_crops?.length > 0 ? data.pest_crops.slice(0, 2).join(", ") : null;
            return (
              <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 3 }}>
                <div style={{ width: 14, height: 14, borderRadius: "50%", background: colour, boxShadow: `0 0 6px ${colour}88` }} />
                <div style={{ fontSize: 9, color: C.stone, textAlign: "center", lineHeight: 1.2 }}>🐛 {tip || label}</div>
              </div>
            );
          })()}
        </div>
      </div>

      {/* Missing data prompt */}
      {data.missing_data?.length > 0 && (
        <div style={{ background: "#fffbf0", border: `1px solid ${C.amber}`, borderLeft: `3px solid ${C.amber}`, borderRadius: 10, padding: "12px 16px", marginBottom: 16 }}>
          <div style={{ fontWeight: 700, fontSize: 13, color: C.amber, marginBottom: 4 }}>Needs your input</div>
          {data.missing_data.slice(0, 3).map(c => (
            <div key={c.id} style={{ fontSize: 12, color: C.stone }}>{c.name} — missing: {c.missing.join(", ")}</div>
          ))}
          <div style={{ fontSize: 11, color: C.stone, marginTop: 6, fontStyle: "italic" }}>Better data = more accurate tasks</div>
        </div>
      )}

      {/* Progress */}
      {totalToday > 0 && (
        <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 18px", marginBottom: 20, display: "flex", alignItems: "center", gap: 16 }}>
          <div style={{ flex: 1 }}>
            <div style={{ fontSize: 13, color: C.stone, marginBottom: 6 }}>Today&apos;s tasks</div>
            <div style={{ height: 6, background: C.border, borderRadius: 10, overflow: "hidden" }}>
              <div style={{ height: "100%", width: `${totalToday > 0 ? (doneToday / totalToday) * 100 : 0}%`, background: C.leaf, borderRadius: 10, transition: "width 0.4s ease" }} />
            </div>
          </div>
          <div style={{ fontSize: 22, fontWeight: 700, color: C.forest, fontFamily: "serif" }}>{doneToday}/{totalToday}</div>
        </div>
      )}

      {/* Active tasks */}
      {[
        { label: "Today",     items: grouped.today.filter(t     => !completed.has(t.id)) },
        { label: "This Week", items: grouped.this_week.filter(t => !completed.has(t.id)) },
        { label: "Coming Up", items: grouped.coming_up.filter(t => !completed.has(t.id)) },
      ].map(({ label, items }) => items?.length > 0 && (
        <div key={label}>
          <SectionLabel>{label}</SectionLabel>
          {items.map(t => (
            <TaskCard key={t.id} task={t} completed={false}
              onComplete={() => completeTask(t)}
              showUndo={false}
              onUndo={null}
            />
          ))}
        </div>
      ))}

      {/* Recently completed — stays visible with undo option */}
      {recentlyDone.length > 0 && (
        <div>
          <SectionLabel>Done today</SectionLabel>
          {recentlyDone.map(t => (
            <TaskCard key={t.id} task={t} completed={true}
              onComplete={() => {}}
              showUndo={!!undoQueue[t.id]}
              onUndo={() => undoComplete(t)}
            />
          ))}
        </div>
      )}

      {/* Harvest forecast */}
      {data.harvest_forecast?.length > 0 && (
        <>
          <SectionLabel>Harvest Forecast</SectionLabel>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 16 }}>
            {data.harvest_forecast.map((h, i) => (
              <div key={i} style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 10, padding: "12px 14px" }}>
                <div style={{ fontWeight: 700, fontSize: 13, fontFamily: "serif", color: "#1a1a1a" }}>{h.crop}</div>
                {h.variety && <div style={{ fontSize: 11, color: C.stone }}>{varietyName(h.variety)}</div>}
                <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>
                  {new Date(h.window_start).toLocaleDateString("en-GB", { month: "short" })} — {new Date(h.window_end).toLocaleDateString("en-GB", { month: "short" })}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {allTasks.filter(t => !completed.has(t.id)).length === 0 && recentlyDone.length === 0 && (
        <div style={{ textAlign: "center", padding: "40px 20px", color: C.stone }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>🌿</div>
          <div style={{ fontSize: 14 }}>No tasks right now. Add crops to get started.</div>
        </div>
      )}
    </div>
  );
}

function TaskCard({ task, completed, onComplete, showUndo, onUndo }) {
  const urgencyColor = task.urgency === "high" ? C.red : task.urgency === "medium" ? C.amber : C.leaf;
  const isEstimated  = task.date_confidence === "estimated";
  return (
    <div onClick={() => !completed && onComplete(task)} style={{ background: completed ? "#f0f4f2" : C.cardBg, border: `1px solid ${completed ? C.border : urgencyColor + "44"}`, borderLeft: `3px solid ${completed ? C.sage : urgencyColor}`, borderRadius: 10, padding: "14px 16px", marginBottom: 10, display: "flex", alignItems: "center", gap: 14, cursor: completed ? "default" : "pointer", opacity: completed ? 0.55 : 1, transition: "opacity 0.2s" }}>
      <div style={{ flex: 1 }}>
        <div style={{ fontWeight: 700, fontSize: 14, color: completed ? C.stone : "#222", textDecoration: completed ? "line-through" : "none", fontFamily: "serif" }}>
          {task.crop?.name || "General"}
        </div>
        <div style={{ fontSize: 13, color: C.stone, marginTop: 2 }}>{task.action}</div>
        <div style={{ display: "flex", gap: 6, marginTop: 6, flexWrap: "wrap", alignItems: "center" }}>
          {task.area?.name && <span style={{ background: C.offwhite, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: C.forest }}>{task.area.name}</span>}
          {isEstimated     && <span style={{ background: "#fff8ed", border: `1px solid ${C.amber}`, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: C.amber }}>~estimated</span>}
          {showUndo && onUndo && (
            <button onClick={e => { e.stopPropagation(); onUndo(task); }}
              style={{ background: C.offwhite, border: `1px solid ${C.border}`, borderRadius: 20, fontSize: 11, padding: "2px 10px", color: C.forest, cursor: "pointer", fontWeight: 600 }}>
              Undo
            </button>
          )}
        </div>
      </div>
      <div style={{ width: 22, height: 22, borderRadius: "50%", border: `2px solid ${completed ? C.leaf : C.border}`, background: completed ? C.leaf : "transparent", display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, transition: "all 0.2s" }}>
        {completed && <span style={{ color: "#fff", fontSize: 12 }}>✓</span>}
      </div>
    </div>
  );
}

// ── Garden view ───────────────────────────────────────────────────────────────
function GardenView() {
  const [locations, setLocations] = useState([]);
  const [crops,     setCrops]     = useState([]);
  const [loading,   setLoading]   = useState(true);
  const [error,     setError]     = useState(null);

  // Add area form state
  const [showAddArea,     setShowAddArea]     = useState(false);
  const [showAddLocation, setShowAddLocation] = useState(false);
  const [newArea,         setNewArea]         = useState({ name: "", type: "raised_bed", location_id: "" });
  const [newLocation,     setNewLocation]     = useState({ name: "", postcode: "" });
  const [saving,          setSaving]          = useState(false);

  const load = useCallback(async () => {
    try {
      const [locs, cropsData] = await Promise.all([
        apiFetch("/locations"),
        apiFetch("/crops"),
      ]);
      setLocations(locs);
      setCrops(cropsData);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const saveArea = async () => {
    if (!newArea.name || !newArea.location_id) return;
    setSaving(true);
    try {
      await apiFetch("/areas", { method: "POST", body: JSON.stringify(newArea) });
      setNewArea({ name: "", type: "raised_bed", location_id: "" });
      setShowAddArea(false);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const saveLocation = async () => {
    if (!newLocation.name) return;
    setSaving(true);
    try {
      await apiFetch("/locations", { method: "POST", body: JSON.stringify(newLocation) });
      setNewLocation({ name: "", postcode: "" });
      setShowAddLocation(false);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const [editingArea,  setEditingArea]  = useState(null);
  const [editAreaForm, setEditAreaForm] = useState({ name: "", type: "" });
  const [confirmArea,  setConfirmArea]  = useState(null);

  const saveEditArea = async (areaId) => {
    setSaving(true);
    try {
      await apiFetch(`/areas/${areaId}`, {
        method: "PUT",
        body: JSON.stringify(editAreaForm),
      });
      setEditingArea(null);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const deleteArea = async (areaId) => {
    setSaving(true);
    try {
      await apiFetch(`/areas/${areaId}`, { method: "DELETE" });
      setConfirmArea(null);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };
  const cropsByArea = crops.reduce((acc, c) => {
    if (!acc[c.area_id]) acc[c.area_id] = [];
    acc[c.area_id].push(c);
    return acc;
  }, {});

  if (loading) return <Spinner />;
  if (error)   return <ErrorMsg msg={error} />;

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 20 }}>
        <div>
          <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a" }}>My Garden</div>
          <div style={{ fontSize: 13, color: C.stone, marginTop: 2 }}>{locations.length} location{locations.length !== 1 ? "s" : ""}</div>
        </div>
        <button onClick={() => { setShowAddLocation(!showAddLocation); setShowAddArea(false); }}
          style={{ background: C.forest, color: "#fff", border: "none", borderRadius: 8, padding: "7px 14px", fontSize: 12, fontWeight: 700, cursor: "pointer" }}>
          + Location
        </button>
      </div>

      {/* Add location form */}
      {showAddLocation && (
        <div style={{ background: C.cardBg, border: `1px solid ${C.forest}`, borderRadius: 12, padding: "16px", marginBottom: 16 }}>
          <div style={{ fontWeight: 700, fontSize: 14, fontFamily: "serif", marginBottom: 12 }}>New location</div>
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            <div><label style={labelStyle}>Name</label>
              <input value={newLocation.name} onChange={e => setNewLocation(l => ({ ...l, name: e.target.value }))} style={inputStyle} placeholder="e.g. Allotment plot 7" /></div>
            <div><label style={labelStyle}>Postcode</label>
              <input value={newLocation.postcode} onChange={e => setNewLocation(l => ({ ...l, postcode: e.target.value.toUpperCase() }))} style={inputStyle} placeholder="e.g. TS22" />
              <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>First part only — e.g. <strong>TS22</strong>, not TS22 5BQ</div>
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={saveLocation} disabled={saving || !newLocation.name}
                style={{ flex: 1, background: !newLocation.name ? C.border : C.forest, color: !newLocation.name ? C.stone : "#fff", border: "none", borderRadius: 8, padding: 12, fontWeight: 700, cursor: "pointer", fontFamily: "serif" }}>
                {saving ? "Saving…" : "Save location"}
              </button>
              <button onClick={() => setShowAddLocation(false)}
                style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "0 14px", color: C.stone, cursor: "pointer" }}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {locations.length === 0 && (
        <div style={{ textAlign: "center", padding: "32px 20px", color: C.stone, fontSize: 14 }}>
          No locations yet. Use the + Location button above.
        </div>
      )}

      {locations.map(loc => (
        <div key={loc.id} style={{ marginBottom: 28 }}>
          {/* Location header */}
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
            <div style={{ fontWeight: 700, fontSize: 16, fontFamily: "serif", color: C.forest }}>{loc.name}</div>
            <button onClick={() => { setShowAddArea(loc.id); setShowAddLocation(false); setNewArea(a => ({ ...a, location_id: loc.id })); }}
              style={{ background: C.offwhite, border: `1px solid ${C.border}`, color: C.forest, borderRadius: 8, padding: "5px 12px", fontSize: 11, fontWeight: 700, cursor: "pointer" }}>
              + Add area
            </button>
          </div>

          {/* Add area form for this location */}
          {showAddArea === loc.id && (
            <div style={{ background: C.cardBg, border: `1px solid ${C.forest}`, borderRadius: 12, padding: "14px", marginBottom: 10 }}>
              <div style={{ fontWeight: 700, fontSize: 13, fontFamily: "serif", marginBottom: 10 }}>New growing area</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                <div><label style={labelStyle}>Name</label>
                  <input value={newArea.name} onChange={e => setNewArea(a => ({ ...a, name: e.target.value }))} style={inputStyle} placeholder="e.g. Raised bed 2, Greenhouse" /></div>
                <div><label style={labelStyle}>Type</label>
                  <select value={newArea.type} onChange={e => setNewArea(a => ({ ...a, type: e.target.value }))} style={inputStyle}>
                    <option value="raised_bed">Raised bed</option>
                    <option value="open_ground">Open ground</option>
                    <option value="greenhouse">Greenhouse</option>
                    <option value="polytunnel">Polytunnel</option>
                    <option value="container">Container / pots</option>
                  </select></div>
                <div style={{ display: "flex", gap: 8 }}>
                  <button onClick={saveArea} disabled={saving || !newArea.name}
                    style={{ flex: 1, background: !newArea.name ? C.border : C.forest, color: !newArea.name ? C.stone : "#fff", border: "none", borderRadius: 8, padding: 12, fontWeight: 700, cursor: "pointer", fontFamily: "serif" }}>
                    {saving ? "Saving…" : "Save area"}
                  </button>
                  <button onClick={() => setShowAddArea(false)}
                    style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "0 14px", color: C.stone, cursor: "pointer" }}>
                    Cancel
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Areas */}
          {(loc.growing_areas || []).map(area => {
            const areaCrops = cropsByArea[area.id] || [];
            return (
              <div key={area.id} style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 16px", marginBottom: 10 }}>

                {/* Confirm delete */}
                {confirmArea === area.id && (
                  <div style={{ background: "#fff5f5", border: `1px solid ${C.red}`, borderRadius: 8, padding: "10px 12px", marginBottom: 10 }}>
                    <div style={{ fontSize: 13, fontWeight: 600, color: C.red, marginBottom: 8 }}>Remove {area.name}? {areaCrops.length > 0 ? `This will also remove ${areaCrops.length} crop${areaCrops.length > 1 ? "s" : ""} in it.` : ""}</div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <button onClick={() => deleteArea(area.id)} disabled={saving}
                        style={{ flex: 1, background: C.red, color: "#fff", border: "none", borderRadius: 8, padding: "8px 0", fontWeight: 700, fontSize: 12, cursor: "pointer" }}>
                        {saving ? "Removing…" : "Yes, remove"}
                      </button>
                      <button onClick={() => setConfirmArea(null)}
                        style={{ flex: 1, background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "8px 0", color: C.stone, cursor: "pointer", fontSize: 12 }}>
                        Cancel
                      </button>
                    </div>
                  </div>
                )}

                {editingArea === area.id ? (
                  /* Edit form */
                  <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                    <div style={{ fontWeight: 700, fontSize: 13, fontFamily: "serif", color: "#1a1a1a" }}>Edit area</div>
                    <div>
                      <label style={labelStyle}>Name</label>
                      <input value={editAreaForm.name} onChange={e => setEditAreaForm(f => ({ ...f, name: e.target.value }))} style={inputStyle} />
                    </div>
                    <div>
                      <label style={labelStyle}>Type</label>
                      <select value={editAreaForm.type} onChange={e => setEditAreaForm(f => ({ ...f, type: e.target.value }))} style={inputStyle}>
                        <option value="raised_bed">Raised bed</option>
                        <option value="open_ground">Open ground</option>
                        <option value="greenhouse">Greenhouse</option>
                        <option value="polytunnel">Polytunnel</option>
                        <option value="container">Container / pots</option>
                      </select>
                    </div>
                    <div style={{ display: "flex", gap: 8 }}>
                      <button onClick={() => saveEditArea(area.id)} disabled={saving || !editAreaForm.name}
                        style={{ flex: 1, background: C.forest, color: "#fff", border: "none", borderRadius: 8, padding: 10, fontWeight: 700, fontSize: 13, cursor: "pointer", fontFamily: "serif" }}>
                        {saving ? "Saving…" : "Save"}
                      </button>
                      <button onClick={() => setEditingArea(null)}
                        style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "0 14px", color: C.stone, cursor: "pointer" }}>
                        Cancel
                      </button>
                    </div>
                  </div>
                ) : (
                  <>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: areaCrops.length > 0 ? 10 : 0 }}>
                      <div>
                        <div style={{ fontWeight: 600, fontSize: 14, color: "#1a1a1a" }}>{area.name}</div>
                        <div style={{ fontSize: 11, color: C.stone, marginTop: 2 }}>{area.type.replace(/_/g, " ")}</div>
                      </div>
                      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                        <span style={{ background: C.offwhite, borderRadius: 8, padding: "3px 10px", fontSize: 11, color: C.forest, fontWeight: 600 }}>
                          {areaCrops.length} crop{areaCrops.length !== 1 ? "s" : ""}
                        </span>
                        <button onClick={() => { setEditingArea(area.id); setEditAreaForm({ name: area.name, type: area.type }); }}
                          style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "3px 10px", fontSize: 11, color: C.stone, cursor: "pointer" }}>
                          Edit
                        </button>
                        <button onClick={() => setConfirmArea(area.id)}
                          style={{ background: "none", border: `1px solid ${C.red}22`, borderRadius: 8, padding: "3px 8px", fontSize: 11, color: C.red, cursor: "pointer" }}>
                          ✕
                        </button>
                      </div>
                    </div>
                    {areaCrops.length > 0 && (
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                        {areaCrops.map(c => {
                          const isPlanned  = c.status === "planned";
                          const isIndoors  = c.status === "sown_indoors";
                          const chipBg     = isPlanned ? "#fff8ed" : isIndoors ? "#f0f4ff" : C.offwhite;
                          const chipBorder = isPlanned ? C.amber : isIndoors ? "#7b9ef7" : C.border;
                          const chipColor  = isPlanned ? C.amber  : isIndoors ? "#2d4fc0" : "#1a1a1a";
                          const statusIcon = isPlanned ? "🗓 " : isIndoors ? "🪟 " : "";
                          return (
                            <span key={c.id} style={{ background: chipBg, border: `1px solid ${chipBorder}`, borderRadius: 8, padding: "4px 10px", fontSize: 12, fontWeight: 500, color: chipColor }}>
                              {statusIcon}{c.name}{varietyName(c.variety) ? ` · ${varietyName(c.variety)}` : ""}
                            </span>
                          );
                        })}
                      </div>
                    )}
                    {areaCrops.length === 0 && (
                      <div style={{ fontSize: 12, color: C.stone, fontStyle: "italic", marginTop: 4 }}>Empty — add crops via the Add tab</div>
                    )}
                  </>
                )}
              </div>
            );
          })}
        </div>
      ))}
    </div>
  );
}

// ── Crops list ────────────────────────────────────────────────────────────────
function CropList() {
  const [crops,    setCrops]   = useState([]);
  const [loading,  setLoading] = useState(true);
  const [error,    setError]   = useState(null);
  const [editing,       setEditing]      = useState(null);
  const [editForm,      setEditForm]      = useState({});
  const [editVarieties, setEditVarieties] = useState([]);
  const [areas,         setAreas]         = useState([]);
  const [saving,        setSaving]        = useState(false);
  const [confirm,       setConfirm]       = useState(null);

  const load = useCallback(async () => {
    try {
      const [cropsData, areasData] = await Promise.all([
        apiFetch("/crops"),
        apiFetch("/areas"),
      ]);
      setCrops(cropsData);
      setAreas(areasData);
    } catch (e) { setError(e.message); }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const startEdit = async (crop) => {
    setEditing(crop.id);
    setEditForm({
      variety_id:  crop.variety_id || "",
      variety:     varietyName(crop.variety) || "",
      sown_date:   crop.sown_date || "",
      area_id:     crop.area_id || "",
      notes:       crop.notes || "",
      status:      crop.status || "growing",
    });
    if (crop.crop_def_id) {
      try {
        const vars = await apiFetch(`/varieties?crop_def_id=${crop.crop_def_id}`);
        setEditVarieties(vars);
      } catch { setEditVarieties([]); }
    } else {
      setEditVarieties([]);
    }
  };

  const saveEdit = async (cropId) => {
    setSaving(true);
    try {
      const isOther = editForm.variety_id === "__other__";
      await apiFetch(`/crops/${cropId}`, {
        method: "PUT",
        body: JSON.stringify({
          ...editForm,
          sown_date:  editForm.sown_date || null,   // never send empty string
          variety_id: isOther ? null : (editForm.variety_id || null),
          variety:    isOther ? (editForm.variety || null) : (editVarieties.find(v => v.id === editForm.variety_id)?.name || editForm.variety || null),
          status:     editForm.status || "growing",
        }),
      });
      setEditing(null);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const deleteCrop = async (cropId) => {
    setSaving(true);
    try {
      await apiFetch(`/crops/${cropId}`, { method: "DELETE" });
      setConfirm(null);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const STAGE_COLOR = { seed: C.stone, seedling: C.leaf, vegetative: C.forest, flowering: C.amber, fruiting: C.amber, harvesting: "#e08020", finished: C.stone };

  if (loading) return <Spinner />;
  if (error)   return <ErrorMsg msg={error} />;

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a" }}>My Crops</div>
        <div style={{ fontSize: 13, color: C.stone, marginTop: 2 }}>{crops.length} crop{crops.length !== 1 ? "s" : ""} growing</div>
      </div>
      {crops.length === 0 && (
        <div style={{ textAlign: "center", padding: "32px 20px", color: C.stone, fontSize: 14 }}>No crops yet. Add your first crop.</div>
      )}
      {crops.map(crop => (
        <div key={crop.id} style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px 18px", marginBottom: 12 }}>

          {/* Confirm delete overlay */}
          {confirm === crop.id && (
            <div style={{ background: "#fff5f5", border: `1px solid ${C.red}`, borderRadius: 10, padding: "12px 14px", marginBottom: 12 }}>
              <div style={{ fontSize: 13, fontWeight: 600, color: C.red, marginBottom: 10 }}>Remove {crop.name}? This cannot be undone.</div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => deleteCrop(crop.id)} disabled={saving}
                  style={{ flex: 1, background: C.red, color: "#fff", border: "none", borderRadius: 8, padding: "9px 0", fontWeight: 700, fontSize: 13, cursor: "pointer" }}>
                  {saving ? "Removing…" : "Yes, remove"}
                </button>
                <button onClick={() => setConfirm(null)}
                  style={{ flex: 1, background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "9px 0", color: C.stone, cursor: "pointer", fontSize: 13 }}>
                  Cancel
                </button>
              </div>
            </div>
          )}

          {editing === crop.id ? (
            /* Edit form */
            <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
              <div style={{ fontWeight: 700, fontSize: 14, fontFamily: "serif", color: "#1a1a1a", marginBottom: 4 }}>Edit {crop.name}</div>
              <div>
                <label style={labelStyle}>Variety</label>
                <select
                  value={editForm.variety_id === "__other__" ? "__other__" : (editForm.variety_id || "")}
                  onChange={e => {
                    if (e.target.value === "__other__") {
                      setEditForm(f => ({ ...f, variety_id: "__other__", variety: "" }));
                    } else {
                      const matched = editVarieties.find(v => v.id === e.target.value);
                      setEditForm(f => ({ ...f, variety_id: e.target.value, variety: matched?.name || "" }));
                    }
                  }}
                  style={inputStyle}
                >
                  <option value="">Unknown / not sure</option>
                  {editVarieties.map(v => <option key={v.id} value={v.id}>{v.name}{v.classification ? ` (${v.classification})` : ""}</option>)}
                  <option value="__other__">Other — type my own</option>
                </select>
                {(editForm.variety_id === "__other__" || (!editForm.variety_id && editForm.variety)) && (
                  <input
                    value={editForm.variety}
                    onChange={e => setEditForm(f => ({ ...f, variety: e.target.value }))}
                    style={{ ...inputStyle, marginTop: 8 }}
                    placeholder="Type your variety name"
                    autoFocus
                  />
                )}
              </div>
              <div>
                <label style={labelStyle}>Status</label>
                <select value={editForm.status} onChange={e => setEditForm(f => ({ ...f, status: e.target.value }))} style={inputStyle}>
                  <option value="planned">🗓 Planned</option>
                  <option value="sown_indoors">🪟 Sowing indoors</option>
                  <option value="sown_outdoors">🌱 Sowing outdoors</option>
                  <option value="transplanted">🪴 Transplanted</option>
                  <option value="growing">✅ Growing</option>
                  <option value="harvested">🧺 Harvested</option>
                </select>
              </div>
              <div>
                <label style={labelStyle}>Sow date</label>
                <input type="date" value={editForm.sown_date} onChange={e => setEditForm(f => ({ ...f, sown_date: e.target.value }))} style={inputStyle} />
              </div>
              <div>
                <label style={labelStyle}>Growing area</label>
                <select value={editForm.area_id} onChange={e => setEditForm(f => ({ ...f, area_id: e.target.value }))} style={inputStyle}>
                  <option value="">Select area</option>
                  {areas.map(a => <option key={a.id} value={a.id}>{a.name}</option>)}
                </select>
              </div>
              <div>
                <label style={labelStyle}>Notes</label>
                <input value={editForm.notes} onChange={e => setEditForm(f => ({ ...f, notes: e.target.value }))} style={inputStyle} placeholder="Optional notes" />
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button onClick={() => saveEdit(crop.id)} disabled={saving}
                  style={{ flex: 1, background: C.forest, color: "#fff", border: "none", borderRadius: 8, padding: 12, fontWeight: 700, cursor: "pointer", fontFamily: "serif" }}>
                  {saving ? "Saving…" : "Save changes"}
                </button>
                <button onClick={() => setEditing(null)}
                  style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "0 16px", color: C.stone, cursor: "pointer" }}>
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            /* Normal view */
            <>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                <div style={{ flex: 1 }}>
                  <div style={{ fontWeight: 700, fontSize: 15, fontFamily: "serif", color: "#1a1a1a" }}>{crop.name}</div>
                  <div style={{ fontSize: 12, color: C.stone, marginTop: 2 }}>{varietyName(crop.variety) || "No variety set"}</div>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ background: (STAGE_COLOR[crop.stage] || C.stone) + "22", borderRadius: 8, padding: "3px 10px", fontSize: 11, color: STAGE_COLOR[crop.stage] || C.stone, fontWeight: 600, textTransform: "capitalize", border: `1px solid ${(STAGE_COLOR[crop.stage] || C.stone) + "55"}` }}>
                    {crop.stage || "seed"}
                  </span>
                  <button onClick={() => startEdit(crop)}
                    style={{ background: "none", border: `1px solid ${C.border}`, borderRadius: 8, padding: "4px 10px", fontSize: 11, color: C.stone, cursor: "pointer" }}>
                    Edit
                  </button>
                  <button onClick={() => setConfirm(crop.id)}
                    style={{ background: "none", border: `1px solid ${C.red}22`, borderRadius: 8, padding: "4px 10px", fontSize: 11, color: C.red, cursor: "pointer" }}>
                    ✕
                  </button>
                </div>
              </div>
              <div style={{ display: "flex", gap: 6, marginTop: 10, flexWrap: "wrap" }}>
                <span style={{ background: C.offwhite, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: C.forest }}>{crop.area?.name}</span>
                {crop.sown_date && <span style={{ background: C.offwhite, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: C.stone }}>Sown {new Date(crop.sown_date).toLocaleDateString("en-GB", { day: "numeric", month: "short" })}</span>}
                {crop.status === "planned"      && <span style={{ background: "#fff8ed", border: `1px solid ${C.amber}`, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: C.amber }}>🗓 Planned</span>}
                {crop.status === "sown_indoors" && <span style={{ background: "#f0f4ff", border: `1px solid #7b9ef7`, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: "#2d4fc0" }}>🪟 Indoors</span>}
                {!crop.crop_def_id && <span style={{ background: "#f0f4ff", border: `1px solid #7b9ef7`, borderRadius: 20, fontSize: 11, padding: "2px 8px", color: "#2d4fc0" }}>🔍 Being identified…</span>}
              </div>
            </>
          )}
        </div>
      ))}
    </div>
  );
}

// ── Add crop ──────────────────────────────────────────────────────────────────
function AddCrop() {
  const [cropDefs,  setCropDefs]  = useState([]);
  const [varieties, setVarieties] = useState([]);
  const [areas,     setAreas]     = useState([]);
  const [form, setForm] = useState({
    crop_def_id: "", variety_id: "", variety: "", crop_other: "", area_id: "",
    status: "", sown_date: "", transplant_date: "", notes: "",
  });
  const [saving,    setSaving]    = useState(false);
  const [saved,     setSaved]     = useState(false);
  const [enriching, setEnriching] = useState(false);
  const [error,     setError]     = useState(null);

  useEffect(() => {
    Promise.all([apiFetch("/crop-definitions"), apiFetch("/areas")])
      .then(([defs, areasData]) => { setCropDefs(defs); setAreas(areasData); })
      .catch(e => setError(e.message));
  }, []);

  useEffect(() => {
    if (!form.crop_def_id || form.crop_def_id === "__other__") { setVarieties([]); return; }
    apiFetch(`/varieties?crop_def_id=${form.crop_def_id}`)
      .then(setVarieties).catch(() => setVarieties([]));
    setForm(f => ({ ...f, variety_id: "", variety: "" }));
  }, [form.crop_def_id]);

  const set = (field, value) => setForm(f => ({ ...f, [field]: value }));
  const isOtherCrop    = form.crop_def_id === "__other__";
  const isOtherVariety = form.variety_id  === "__other__";
  const selectedCrop   = cropDefs.find(d => d.id === form.crop_def_id);

  // Status options with descriptions
  const STATUS_OPTIONS = [
    { value: "planned",       label: "🗓 Planned",             hint: "I plan to grow this — not started yet" },
    { value: "sown_indoors",  label: "🪟 Sowing indoors",      hint: "Starting on windowsill, greenhouse or cold frame" },
    { value: "sown_outdoors", label: "🌱 Sowing outdoors",     hint: "Direct sowing outside in final position" },
    { value: "transplanted",  label: "🪴 Transplanted",        hint: "Moved outside from indoors / greenhouse" },
    { value: "growing",       label: "✅ Already growing",     hint: "Established and growing — add sow date below" },
  ];

  // What date fields to show based on status
  const showSowDate        = ["sown_indoors","sown_outdoors","growing","transplanted"].includes(form.status);
  const showTransplantDate = form.status === "transplanted";
  const sowDateLabel       = form.status === "sown_indoors" ? "Date sown indoors"
                           : form.status === "sown_outdoors" ? "Date sown outdoors"
                           : "Sow date";

  const handleSave = async () => {
    const cropName = isOtherCrop ? form.crop_other : selectedCrop?.name;
    if ((!form.crop_def_id && !isOtherCrop) || !form.area_id || !cropName || !form.status) return;
    setSaving(true); setError(null);
    try {
      const realVarietyId = isOtherVariety ? null : (form.variety_id || null);
      const realVariety   = isOtherVariety
        ? (form.variety || null)
        : (varieties.find(v => v.id === form.variety_id)?.name || null);

      const result = await apiFetch("/crops", {
        method: "POST",
        body: JSON.stringify({
          name:             cropName,
          crop_def_id:      isOtherCrop ? null : (form.crop_def_id || null),
          variety_id:       realVarietyId,
          variety:          realVariety,
          area_id:          form.area_id,
          status:           form.status,
          sown_date:        showSowDate ? (form.sown_date || null) : null,
          transplant_date:  showTransplantDate ? (form.transplant_date || null) : null,
          start_date_confidence: form.sown_date ? "exact" : "unknown",
          notes:            form.notes || null,
          is_other_crop:    isOtherCrop,
          is_other_variety: isOtherVariety,
        }),
      });

      if (result.enriching) setEnriching(true);
      setSaved(true);
      setForm({ crop_def_id: "", variety_id: "", variety: "", crop_other: "", area_id: "", status: "", sown_date: "", transplant_date: "", notes: "" });
      setTimeout(() => { setSaved(false); setEnriching(false); }, 8000);
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const canSave = (form.crop_def_id || (isOtherCrop && form.crop_other)) && form.area_id && form.status;

  return (
    <div>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "serif", marginBottom: 24, color: "#1a1a1a" }}>Add Crop</div>
      {saved && !enriching && <div style={{ background: "#edf7ec", border: `1px solid ${C.leaf}`, borderRadius: 10, padding: "12px 16px", marginBottom: 16, color: "#2d7a28", fontWeight: 600, fontSize: 13 }}>✓ Crop added — tasks will be generated</div>}
      {saved && enriching  && <div style={{ background: "#f0f4ff", border: `1px solid #7b9ef7`, borderRadius: 10, padding: "12px 16px", marginBottom: 16, color: "#2d4fc0", fontWeight: 600, fontSize: 13 }}>✓ Crop added — identifying and enriching data 🔍</div>}
      {error && <ErrorMsg msg={error} />}

      <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>

        {/* Crop */}
        <div>
          <label style={labelStyle}>Crop</label>
          <select value={form.crop_def_id} onChange={e => set("crop_def_id", e.target.value)} style={inputStyle}>
            <option value="">Select crop…</option>
            {cropDefs.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
            <option value="__other__">Other — type my own</option>
          </select>
          {isOtherCrop && (
            <input type="text" value={form.crop_other} onChange={e => set("crop_other", e.target.value)}
              style={{ ...inputStyle, marginTop: 8 }} placeholder="e.g. Tomatillo, Okra, Pak Choi…" autoFocus />
          )}
        </div>

        {/* Variety */}
        <div>
          <label style={labelStyle}>Variety</label>
          <select
            value={form.variety_id === "__other__" ? "__other__" : (form.variety_id || "")}
            onChange={e => {
              if (e.target.value === "__other__") { set("variety_id", "__other__"); set("variety", ""); }
              else { set("variety_id", e.target.value); set("variety", ""); }
            }}
            style={inputStyle} disabled={!form.crop_def_id}>
            <option value="">Unknown / not sure</option>
            {varieties.map(v => <option key={v.id} value={v.id}>{v.name}{v.classification ? ` (${v.classification})` : ""}</option>)}
            <option value="__other__">Other — type my own</option>
          </select>
          {(isOtherVariety || isOtherCrop) && (
            <input type="text" value={form.variety} onChange={e => set("variety", e.target.value)}
              style={{ ...inputStyle, marginTop: 8 }} placeholder="Type your variety name" />
          )}
          <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>Optional — tasks work without a variety.</div>
        </div>

        {/* Area */}
        <div>
          <label style={labelStyle}>Growing Area</label>
          <select value={form.area_id} onChange={e => set("area_id", e.target.value)} style={inputStyle}>
            <option value="">Select area…</option>
            {areas.map(a => <option key={a.id} value={a.id}>{a.name} ({a.type?.replace("_"," ")})</option>)}
          </select>
        </div>

        {/* Status — the key new field */}
        <div>
          <label style={labelStyle}>What stage is this crop at?</label>
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {STATUS_OPTIONS.map(opt => (
              <div key={opt.value}
                onClick={() => set("status", opt.value)}
                style={{
                  border: `2px solid ${form.status === opt.value ? C.forest : C.border}`,
                  borderRadius: 10, padding: "10px 14px", cursor: "pointer",
                  background: form.status === opt.value ? "#f0f5f3" : C.cardBg,
                  transition: "all 0.15s",
                }}>
                <div style={{ fontWeight: 700, fontSize: 13, color: form.status === opt.value ? C.forest : "#1a1a1a" }}>{opt.label}</div>
                <div style={{ fontSize: 11, color: C.stone, marginTop: 2 }}>{opt.hint}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Sow date — only shown when relevant */}
        {showSowDate && (
          <div>
            <label style={labelStyle}>{sowDateLabel} <span style={{ color: C.stone, fontWeight: 400 }}>(optional)</span></label>
            <input type="date" value={form.sown_date} onChange={e => set("sown_date", e.target.value)} style={inputStyle} />
            <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>Helps generate accurate feeding and harvest tasks</div>
          </div>
        )}

        {/* Transplant date — only if transplanted */}
        {showTransplantDate && (
          <div>
            <label style={labelStyle}>Date transplanted outdoors <span style={{ color: C.stone, fontWeight: 400 }}>(optional)</span></label>
            <input type="date" value={form.transplant_date} onChange={e => set("transplant_date", e.target.value)} style={inputStyle} />
          </div>
        )}

        {/* Notes */}
        <div>
          <label style={labelStyle}>Notes (optional)</label>
          <textarea value={form.notes} onChange={e => set("notes", e.target.value)}
            style={{ ...inputStyle, height: 80, resize: "vertical" }} placeholder="Any notes about this plant…" />
        </div>

        <button onClick={handleSave} disabled={saving || !canSave}
          style={{ background: !canSave ? C.border : C.forest, color: !canSave ? C.stone : "#fff", border: "none", borderRadius: 12, padding: 14, fontSize: 15, fontWeight: 700, cursor: !canSave ? "not-allowed" : "pointer", fontFamily: "serif", transition: "background 0.2s" }}>
          {saving ? "Saving…" : "Save Crop"}
        </button>
      </div>
    </div>
  );
}

// ── Helper date functions ─────────────────────────────────────────────────────
function todayISO() { return new Date().toISOString().split("T")[0]; }
function weekEndISO() { return new Date(Date.now() + 7 * 86400000).toISOString().split("T")[0]; }

// ── Profile Screen ────────────────────────────────────────────────────────────
function ProfileScreen({ session }) {
  const [form,    setForm]    = useState({ name: "", postcode: "" });
  const [pwForm,  setPwForm]  = useState({ current: "", next: "", confirm: "" });
  const [loading, setLoading] = useState(true);
  const [saving,  setSaving]  = useState(false);
  const [pwSaving, setPwSaving] = useState(false);
  const [saved,   setSaved]   = useState(false);
  const [pwSaved, setPwSaved] = useState(false);
  const [error,   setError]   = useState(null);
  const [pwError, setPwError] = useState(null);

  useEffect(() => {
    apiFetch("/auth/profile")
      .then(p => { setForm({ name: p.name || "", postcode: p.postcode || "" }); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const saveProfile = async () => {
    if (!form.name.trim() || !form.postcode.trim()) return;
    setSaving(true); setError(null);
    try {
      await apiFetch("/auth/profile", { method: "POST", body: JSON.stringify(form) });
      setSaved(true);
      setTimeout(() => setSaved(false), 3000);
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const changePassword = async () => {
    setPwError(null);
    if (!pwForm.next.trim()) return;
    if (pwForm.next !== pwForm.confirm) { setPwError("Passwords don't match"); return; }
    if (pwForm.next.length < 8) { setPwError("Password must be at least 8 characters"); return; }
    setPwSaving(true);
    try {
      const { error } = await supabase.auth.updateUser({ password: pwForm.next });
      if (error) throw new Error(error.message);
      setPwSaved(true);
      setPwForm({ current: "", next: "", confirm: "" });
      setTimeout(() => setPwSaved(false), 3000);
    } catch (e) { setPwError(e.message); }
    setPwSaving(false);
  };

  if (loading) return <Spinner />;

  return (
    <div>
      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "serif", marginBottom: 24, color: "#1a1a1a" }}>Profile</div>

      {/* Account info */}
      <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 16px", marginBottom: 8 }}>
        <div style={{ fontSize: 11, color: C.stone, marginBottom: 2 }}>Signed in as</div>
        <div style={{ fontSize: 13, fontWeight: 600, color: "#1a1a1a" }}>{session?.user?.email}</div>
      </div>

      {/* Edit name + postcode */}
      <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px", marginBottom: 16, marginTop: 16 }}>
        <div style={{ fontSize: 15, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a", marginBottom: 16 }}>Your Details</div>
        {saved  && <div style={{ background: "#edf7ec", border: `1px solid ${C.leaf}`, borderRadius: 8, padding: "10px 14px", marginBottom: 12, color: "#2d7a28", fontWeight: 600, fontSize: 13 }}>✓ Saved</div>}
        {error  && <ErrorMsg msg={error} />}
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <label style={labelStyle}>Your name</label>
            <input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} style={inputStyle} placeholder="e.g. Mark" />
          </div>
          <div>
            <label style={labelStyle}>Postcode</label>
            <input value={form.postcode} onChange={e => setForm(f => ({ ...f, postcode: e.target.value.toUpperCase() }))} style={inputStyle} placeholder="e.g. TS22" />
            <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>Enter the first part only — e.g. <strong>TS22</strong>, not TS22 5BQ</div>
          </div>
          <button onClick={saveProfile} disabled={saving} style={{ background: C.forest, color: "#fff", border: "none", borderRadius: 10, padding: "12px", fontWeight: 700, fontSize: 14, cursor: "pointer", opacity: saving ? 0.6 : 1 }}>
            {saving ? "Saving…" : "Save Changes"}
          </button>
        </div>
      </div>

      {/* Change password */}
      <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px", marginBottom: 16 }}>
        <div style={{ fontSize: 15, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a", marginBottom: 16 }}>Change Password</div>
        {pwSaved  && <div style={{ background: "#edf7ec", border: `1px solid ${C.leaf}`, borderRadius: 8, padding: "10px 14px", marginBottom: 12, color: "#2d7a28", fontWeight: 600, fontSize: 13 }}>✓ Password updated</div>}
        {pwError  && <ErrorMsg msg={pwError} />}
        <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
          <div>
            <label style={labelStyle}>New password</label>
            <input type="password" value={pwForm.next} onChange={e => setPwForm(f => ({ ...f, next: e.target.value }))} style={inputStyle} placeholder="At least 8 characters" />
          </div>
          <div>
            <label style={labelStyle}>Confirm new password</label>
            <input type="password" value={pwForm.confirm} onChange={e => setPwForm(f => ({ ...f, confirm: e.target.value }))} style={inputStyle} placeholder="Repeat new password" />
          </div>
          <button onClick={changePassword} disabled={pwSaving} style={{ background: C.forest, color: "#fff", border: "none", borderRadius: 10, padding: "12px", fontWeight: 700, fontSize: 14, cursor: "pointer", opacity: pwSaving ? 0.6 : 1 }}>
            {pwSaving ? "Updating…" : "Update Password"}
          </button>
        </div>
      </div>

      {/* Sign out */}
      <button
        onClick={() => supabase.auth.signOut()}
        style={{ width: "100%", background: "none", border: `1px solid ${C.border}`, borderRadius: 10, padding: "12px", fontWeight: 600, fontSize: 14, cursor: "pointer", color: C.stone, marginBottom: 8 }}>
        Sign Out
      </button>

      <div style={{ fontSize: 11, color: C.stone, textAlign: "center", marginTop: 8 }}>Vercro — version 1.0</div>
    </div>
  );
}


// ── My Feeds ──────────────────────────────────────────────────────────────────

function FeedsScreen() {
  const [feeds,   setFeeds]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving,  setSaving]  = useState(false);
  const [error,   setError]   = useState(null);
  const [form,    setForm]    = useState({ brand: "", product_name: "" });
  const [added,   setAdded]   = useState(false);

  const load = async () => {
    try {
      const data = await apiFetch("/feeds");
      setFeeds(data);
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  useEffect(() => { load(); }, []);

  const addFeed = async () => {
    if (!form.product_name.trim()) return;
    setSaving(true);
    try {
      await apiFetch("/feeds", {
        method: "POST",
        body: JSON.stringify({ brand: form.brand.trim() || null, product_name: form.product_name.trim() }),
      });
      setForm({ brand: "", product_name: "" });
      setAdded(true);
      setTimeout(() => setAdded(false), 4000);
      await load();
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const deleteFeed = async (id) => {
    try {
      await apiFetch(`/feeds/${id}`, { method: "DELETE" });
      setFeeds(f => f.filter(x => x.id !== id));
    } catch (e) { setError(e.message); }
  };

  const feedTypeLabel = (ft) => {
    const map = {
      high_potash: "High Potash", balanced: "Balanced", high_nitrogen: "High Nitrogen",
      low_nitrogen: "Low Nitrogen", specialist_tomato: "Tomato Feed", specialist_rose: "Rose Feed",
      seaweed: "Seaweed", organic_general: "Organic General",
    };
    return map[ft] || ft || "Unknown";
  };

  if (loading) return <Spinner />;

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a" }}>My Feeds</div>
        <div style={{ fontSize: 13, color: C.stone, marginTop: 2 }}>Register the feeds you own — we'll personalise your feeding tasks</div>
      </div>

      {error && <ErrorMsg msg={error} />}

      {/* Add feed form */}
      <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px", marginBottom: 20 }}>
        <div style={{ fontSize: 15, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a", marginBottom: 14 }}>Add a Feed</div>
        {added && <div style={{ background: "#edf7ec", border: `1px solid ${C.leaf}`, borderRadius: 8, padding: "10px 14px", marginBottom: 12, color: "#2d7a28", fontWeight: 600, fontSize: 13 }}>✓ Feed added — identifying and enriching data 🔍</div>}
        <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
          <div>
            <label style={labelStyle}>Brand (optional)</label>
            <input value={form.brand} onChange={e => setForm(f => ({ ...f, brand: e.target.value }))}
              style={inputStyle} placeholder="e.g. Chempak, Growmore, Miracle-Gro" />
          </div>
          <div>
            <label style={labelStyle}>Product name</label>
            <input value={form.product_name} onChange={e => setForm(f => ({ ...f, product_name: e.target.value }))}
              style={inputStyle} placeholder="e.g. Tomorite, Fish Blood & Bone" />
          </div>
          <button onClick={addFeed} disabled={saving || !form.product_name.trim()}
            style={{ background: C.forest, color: "#fff", border: "none", borderRadius: 10, padding: "12px", fontWeight: 700, fontSize: 14, cursor: "pointer", opacity: (saving || !form.product_name.trim()) ? 0.6 : 1, fontFamily: "serif" }}>
            {saving ? "Adding…" : "Add Feed"}
          </button>
        </div>
      </div>

      {/* Feed list */}
      {feeds.length === 0 ? (
        <div style={{ textAlign: "center", padding: "32px 20px", color: C.stone, fontSize: 14 }}>
          No feeds added yet. Add your first feed above.
        </div>
      ) : (
        feeds.map(feed => (
          <div key={feed.id} style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 16px", marginBottom: 10 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
              <div style={{ flex: 1 }}>
                <div style={{ fontSize: 15, fontWeight: 700, color: "#1a1a1a", fontFamily: "serif" }}>
                  {feed.brand ? `${feed.brand} ` : ""}{feed.product_name}
                </div>
                {feed.enriched ? (
                  <div style={{ marginTop: 6, display: "flex", flexWrap: "wrap", gap: 6 }}>
                    <span style={{ fontSize: 11, background: "#e8f0fe", color: "#3a5fc8", borderRadius: 6, padding: "2px 8px", fontWeight: 600 }}>{feedTypeLabel(feed.feed_type)}</span>
                    {feed.form && <span style={{ fontSize: 11, background: C.offWhite, color: C.stone, borderRadius: 6, padding: "2px 8px" }}>{feed.form}</span>}
                    {feed.npk && <span style={{ fontSize: 11, background: C.offWhite, color: C.stone, borderRadius: 6, padding: "2px 8px" }}>NPK {feed.npk}</span>}
                    {feed.dilution_ml_per_litre && <span style={{ fontSize: 11, background: C.offWhite, color: C.stone, borderRadius: 6, padding: "2px 8px" }}>{feed.dilution_ml_per_litre}ml/L</span>}
                    {feed.frequency_days && <span style={{ fontSize: 11, background: C.offWhite, color: C.stone, borderRadius: 6, padding: "2px 8px" }}>Every {feed.frequency_days} days</span>}
                  </div>
                ) : (
                  <div style={{ fontSize: 12, color: C.stone, marginTop: 4, fontStyle: "italic" }}>Identifying feed data… 🔍</div>
                )}
                {feed.notes && <div style={{ fontSize: 12, color: C.stone, marginTop: 6 }}>{feed.notes}</div>}
              </div>
              <button onClick={() => deleteFeed(feed.id)}
                style={{ background: "none", border: "none", cursor: "pointer", color: C.stone, fontSize: 18, padding: "0 0 0 12px", lineHeight: 1 }}>×</button>
            </div>
          </div>
        ))
      )}
    </div>
  );
}

// ── Navigation tabs ───────────────────────────────────────────────────────────
const TABS = [
  { id: "dashboard", label: "Today",  icon: "◈" },
  { id: "garden",    label: "Garden",  icon: "⬡" },
  { id: "crops",     label: "Crops",   icon: "◉" },
  { id: "add",       label: "Add",     icon: "+" },
  { id: "feeds",     label: "Feeds",   icon: "🧪" },
  { id: "profile",   label: "Profile", icon: "👤" },
];

// ── Onboarding ────────────────────────────────────────────────────────────────
// Runs once after sign-up. Three steps: profile → location → area.
// Skipped entirely if the user already has at least one location.

const ONBOARDING_STEPS = [
  { id: "profile",  label: "About you",       hint: "So we can personalise your experience" },
  { id: "location", label: "Your garden",      hint: "Where are you growing?" },
  { id: "area",     label: "First growing area", hint: "A bed, greenhouse, or container" },
];

function OnboardingScreen({ onComplete }) {
  const [step, setStep]     = useState(0);
  const [saving, setSaving] = useState(false);
  const [error, setError]   = useState(null);

  // Form state per step
  const [profile,  setProfile]  = useState({ name: "", postcode: "" });
  const [location, setLocation] = useState({ name: "", postcode: "" });
  const [area,     setArea]     = useState({ name: "", type: "raised_bed" });

  // Saved IDs — needed to link area → location
  const [locationId, setLocationId] = useState(null);

  const current = ONBOARDING_STEPS[step];
  const isLast  = step === ONBOARDING_STEPS.length - 1;

  const canAdvance = () => {
    if (step === 0) return profile.name.trim() && profile.postcode.trim();
    if (step === 1) return location.name.trim() && location.postcode.trim();
    if (step === 2) return area.name.trim();
    return false;
  };

  const handleNext = async () => {
    setSaving(true); setError(null);
    try {
      if (step === 0) {
        await apiFetch("/auth/profile", { method: "POST", body: JSON.stringify(profile) });
        setStep(1);
      } else if (step === 1) {
        const loc = await apiFetch("/locations", { method: "POST", body: JSON.stringify({ name: location.name, postcode: location.postcode }) });
        setLocationId(loc.id);
        setStep(2);
      } else if (step === 2) {
        await apiFetch("/areas", { method: "POST", body: JSON.stringify({ location_id: locationId, name: area.name, type: area.type }) });
        onComplete();
      }
    } catch (e) { setError(e.message); }
    setSaving(false);
  };

  const setP = (f, v) => setProfile(p  => ({ ...p, [f]: v }));
  const setL = (f, v) => setLocation(p => ({ ...p, [f]: v }));
  const setA = (f, v) => setArea(p     => ({ ...p, [f]: v }));

  return (
    <div style={{ background: C.offwhite, minHeight: "100vh", fontFamily: "Georgia, serif", maxWidth: 440, margin: "0 auto", padding: "0 0 40px" }}>

      {/* Progress bar */}
      <div style={{ height: 3, background: C.border }}>
        <div style={{ height: "100%", width: `${((step + 1) / ONBOARDING_STEPS.length) * 100}%`, background: C.forest, transition: "width 0.4s ease" }} />
      </div>

      {/* Header */}
      <div style={{ padding: "28px 24px 0" }}>
        <div style={{ fontSize: 11, color: C.stone, letterSpacing: 2, textTransform: "uppercase", marginBottom: 8 }}>
          {step + 1} of {ONBOARDING_STEPS.length}
        </div>
        <div style={{ fontSize: 26, fontWeight: 700, fontFamily: "serif", color: C.forest }}>{current.label}</div>
        <div style={{ fontSize: 13, color: C.stone, marginTop: 4 }}>{current.hint}</div>
      </div>

      {/* Fields */}
      <div style={{ padding: "28px 24px 0", display: "flex", flexDirection: "column", gap: 18 }}>

        {error && <ErrorMsg msg={error} />}

        {step === 0 && <>
          <div>
            <label style={labelStyle}>Your name</label>
            <input value={profile.name} onChange={e => setP("name", e.target.value)} style={inputStyle} placeholder="e.g. Sarah" autoFocus />
          </div>
          <div>
            <label style={labelStyle}>Your postcode</label>
            <input value={profile.postcode} onChange={e => setP("postcode", e.target.value.toUpperCase())} style={inputStyle} placeholder="e.g. TS22" />
            <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>First part only — e.g. <strong>TS22</strong>, not TS22 5BQ</div>
            <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>Used for weather and frost alerts</div>
          </div>
        </>}

        {step === 1 && <>
          <div>
            <label style={labelStyle}>Location name</label>
            <input value={location.name} onChange={e => setL("name", e.target.value)} style={inputStyle} placeholder="e.g. Back garden, Allotment plot 14" autoFocus />
          </div>
          <div>
            <label style={labelStyle}>Postcode</label>
            <input value={location.postcode} onChange={e => setL("postcode", e.target.value.toUpperCase())} style={inputStyle} placeholder="e.g. TS22" />
            <div style={{ fontSize: 11, color: C.stone, marginTop: 4 }}>First part only — e.g. <strong>TS22</strong>, not TS22 5BQ</div>
          </div>

          {/* What counts as a location */}
          <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 10, padding: "14px 16px" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.stone, marginBottom: 8, letterSpacing: 1, textTransform: "uppercase" }}>What is a location?</div>
            {["Your home garden", "An allotment plot", "A community garden", "A friend's garden you help with"].map(ex => (
              <div key={ex} style={{ fontSize: 13, color: C.stone, paddingBottom: 4 }}>· {ex}</div>
            ))}
            <div style={{ fontSize: 11, color: C.stone, marginTop: 6, fontStyle: "italic" }}>You can add more locations later.</div>
          </div>
        </>}

        {step === 2 && <>
          <div>
            <label style={labelStyle}>Area name</label>
            <input value={area.name} onChange={e => setA("name", e.target.value)} style={inputStyle} placeholder="e.g. Main raised bed, Greenhouse" autoFocus />
          </div>
          <div>
            <label style={labelStyle}>Type</label>
            <select value={area.type} onChange={e => setA("type", e.target.value)} style={inputStyle}>
              <option value="raised_bed">Raised bed</option>
              <option value="open_ground">Open ground</option>
              <option value="greenhouse">Greenhouse</option>
              <option value="polytunnel">Polytunnel</option>
              <option value="container">Container / pots</option>
            </select>
          </div>

          {/* Area type context */}
          <div style={{ background: C.cardBg, border: `1px solid ${C.border}`, borderRadius: 10, padding: "14px 16px" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: C.stone, marginBottom: 8, letterSpacing: 1, textTransform: "uppercase" }}>Why this matters</div>
            <div style={{ fontSize: 13, color: C.stone, lineHeight: 1.6 }}>
              A greenhouse or polytunnel gets earlier planting dates and no frost alerts. Containers get more frequent watering reminders. You can add more areas after setup.
            </div>
          </div>
        </>}

        <button
          onClick={handleNext}
          disabled={saving || !canAdvance()}
          style={{ background: !canAdvance() ? C.border : C.forest, color: !canAdvance() ? C.stone : "#fff", border: "none", borderRadius: 12, padding: 16, fontSize: 16, fontWeight: 700, cursor: !canAdvance() ? "not-allowed" : "pointer", fontFamily: "serif", marginTop: 8, transition: "background 0.2s" }}
        >
          {saving ? "Saving…" : isLast ? "Take me to my garden 🌱" : "Continue →"}
        </button>

        {step > 0 && (
          <button onClick={() => setStep(s => s - 1)} style={{ background: "none", border: "none", color: C.stone, fontSize: 13, cursor: "pointer", textDecoration: "underline" }}>
            ← Back
          </button>
        )}
      </div>
    </div>
  );
}

// ── Root app ──────────────────────────────────────────────────────────────────
export default function GrowSmart() {
  const [session,     setSession]     = useState(undefined); // undefined = loading
  const [onboarding,  setOnboarding]  = useState(null);      // null = checking, true/false = resolved
  const [tab,         setTab]         = useState("dashboard");

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => setSession(session));
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_e, s) => setSession(s));
    return () => subscription.unsubscribe();
  }, []);

  // Once we have a session, check whether onboarding is needed
  useEffect(() => {
    if (!session) { setOnboarding(null); return; }
    apiFetch("/locations")
      .then(locs => setOnboarding(locs.length === 0))
      .catch(() => setOnboarding(false)); // if check fails, don't block the app
  }, [session]);

  if (session === undefined || (session && onboarding === null)) {
    return <div style={{ display: "flex", alignItems: "center", justifyContent: "center", height: "100vh", color: C.stone, fontSize: 14 }}>Loading…</div>;
  }
  if (!session)   return <AuthScreen onAuth={setSession} />;
  if (onboarding) return <OnboardingScreen onComplete={() => setOnboarding(false)} />;

  return (
    <div style={{ background: C.offwhite, minHeight: "100vh", fontFamily: "Georgia, serif", maxWidth: 440, margin: "0 auto" }}>
      {/* Header */}
      <div style={{ background: C.offwhite, borderBottom: `1px solid ${C.border}`, padding: "16px 20px 12px", position: "sticky", top: 0, zIndex: 10 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ fontSize: 20, fontWeight: 700, fontFamily: "serif", color: "#1a1a1a" }}>Vercro 🌱</div>
        </div>
      </div>

      {/* Content */}
      <div style={{ padding: "20px 20px 100px" }}>
        {tab === "dashboard" && <Dashboard />}
        {tab === "garden"    && <GardenView />}
        {tab === "crops"     && <CropList />}
        {tab === "add"       && <AddCrop />}
        {tab === "feeds"     && <FeedsScreen />}
        {tab === "profile"   && <ProfileScreen session={session} />}
      </div>

      {/* Bottom nav */}
      <div style={{ position: "fixed", bottom: 0, left: "50%", transform: "translateX(-50%)", width: "100%", maxWidth: 440, background: "rgba(247,246,242,0.96)", borderTop: `1px solid ${C.border}`, display: "flex", zIndex: 20 }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)} style={{ flex: 1, border: "none", background: "transparent", padding: "10px 4px 14px", cursor: "pointer", display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
            <div style={{ width: 36, height: 36, borderRadius: 10, background: tab === t.id ? C.forest : "transparent", display: "flex", alignItems: "center", justifyContent: "center", fontSize: t.id === "add" ? 22 : 16, color: tab === t.id ? "#fff" : C.stone, transition: "all 0.2s" }}>{t.icon}</div>
            <div style={{ fontSize: 10, color: tab === t.id ? C.forest : C.stone, fontFamily: "sans-serif", fontWeight: tab === t.id ? 700 : 400 }}>{t.label}</div>
          </button>
        ))}
      </div>
    </div>
  );
}