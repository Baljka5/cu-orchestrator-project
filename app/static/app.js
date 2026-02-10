function esc(s) {
  return String(s ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setActiveTab(name) {
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.add("hidden"));
  document.querySelector(`.tab[data-tab="${name}"]`)?.classList.add("active");
  document.getElementById(`panel-${name}`)?.classList.remove("hidden");
}

function renderTable(columns, rows) {
  const wrap = document.getElementById("resultTable");
  if (!columns || !columns.length) {
    wrap.innerHTML = `<div class="muted">Result хоосон байна.</div>`;
    return;
  }

  const head = columns.map(c => `<th>${esc(c)}</th>`).join("");
  const body = (rows || []).map(r => {
    const tds = columns.map((_, i) => `<td>${esc(r?.[i])}</td>`).join("");
    return `<tr>${tds}</tr>`;
  }).join("");

  wrap.innerHTML = `
    <div class="tableWrap">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body || `<tr><td colspan="${columns.length}" class="muted">No rows</td></tr>`}</tbody>
      </table>
    </div>
  `;
}

async function ask() {
  const q = document.getElementById("question").value;
  const agent = document.getElementById("agent").value;

  const pretty = document.getElementById("pretty");
  const raw = document.getElementById("raw");
  const api = document.getElementById("api");
  const sqlBox = document.getElementById("sqlBox");

  pretty.textContent = "⏳ асууж байна...";
  raw.textContent = "";
  api.textContent = "";
  sqlBox.textContent = "";
  document.getElementById("resultTable").innerHTML = "";

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: q,
        force_agent: agent === "auto" ? null : agent
      })
    });

    const data = await res.json();

    api.textContent = JSON.stringify(data, null, 2);
    raw.textContent = data.answer || "";

    if (data.sql) sqlBox.textContent = data.sql;

    const agentName = data?.meta?.agent || "unknown";
    const mode = data?.meta?.mode || "";
    const rowCount = (data.rows || []).length;

    pretty.textContent =
      `Agent: ${agentName}\n` +
      (mode ? `Mode: ${mode}\n` : "") +
      (data.sql ? `SQL: OK\n` : `SQL: (none)\n`) +
      `Rows: ${rowCount}`;

    if (data.columns && data.columns.length) {
      renderTable(data.columns, data.rows || []);
    }

    setActiveTab("pretty");
  } catch (e) {
    pretty.textContent = " Алдаа: " + e;
    setActiveTab("pretty");
  }
}

document.addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    ask();
  }
});

document.addEventListener("click", (e) => {
  const t = e.target.closest(".tab");
  if (t) setActiveTab(t.dataset.tab);
});

function copyCurrent() {
  const active = document.querySelector(".tab.active")?.dataset?.tab || "pretty";
  const panel = document.getElementById(`panel-${active}`);
  if (!panel) return;
  const text = panel.innerText || panel.textContent || "";
  navigator.clipboard.writeText(text);
}

window.ask = ask;
window.copyCurrent = copyCurrent;
window.setActiveTab = setActiveTab;
