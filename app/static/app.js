// app/static/app.js

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text ?? "";
}

function hide(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = "none";
}

function show(id) {
  const el = document.getElementById(id);
  if (!el) return;
  el.style.display = "block";
}

function renderTable(columns, rows) {
  const table = document.getElementById("resultTable");
  if (!table) return;

  table.innerHTML = "";

  if (!Array.isArray(columns) || columns.length === 0) {
    hide("resultBox");
    return;
  }

  // header
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  for (const c of columns) {
    const th = document.createElement("th");
    th.innerHTML = escapeHtml(c);
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  table.appendChild(thead);

  // body
  const tbody = document.createElement("tbody");
  const safeRows = Array.isArray(rows) ? rows : [];

  for (const r of safeRows) {
    const tr = document.createElement("tr");
    const arr = Array.isArray(r) ? r : [r];

    for (const v of arr) {
      const td = document.createElement("td");
      td.innerHTML =
        v === null || v === undefined ? "" : escapeHtml(String(v));
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }

  table.appendChild(tbody);
  show("resultBox");
}

async function ask() {
  const qEl = document.getElementById("question");
  const agentEl = document.getElementById("agent");

  const q = qEl ? qEl.value : "";
  const agent = agentEl ? agentEl.value : "auto";

  setText("answer", "⏳ асууж байна...");
  setText("sql", "");
  setText("notes", "");
  hide("resultBox");
  renderTable([], []);

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: q,
        force_agent: agent === "auto" ? null : agent
      })
    });

    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`HTTP ${res.status}: ${txt}`);
    }

    const data = await res.json();

    // 1) summary/answer
    setText("answer", data.answer || "");

    // 2) meta
    const meta = data.meta || {};
    const sql = meta.sql || meta.query || "";   // аль нэг нь ирж магадгүй
    const notes = meta.notes || "";

    setText("sql", sql);
    setText("notes", notes);

    // 3) result table
    const columns = meta.columns || [];
    const rows = meta.rows || [];
    renderTable(columns, rows);

  } catch (e) {
    setText("answer", "❌ Алдаа: " + (e?.message || e));
  }
}

// Optional: Enter дархад асуух
document.addEventListener("DOMContentLoaded", () => {
  const qEl = document.getElementById("question");
  if (!qEl) return;

  qEl.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter" && (ev.ctrlKey || ev.metaKey)) {
      ev.preventDefault();
      ask();
    }
  });
});
