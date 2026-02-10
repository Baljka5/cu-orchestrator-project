function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function copyToClipboard(text) {
  navigator.clipboard.writeText(text).then(() => {
    toast("✅ Copied");
  }).catch(() => toast("❌ Copy failed"));
}

let toastTimer = null;
function toast(msg) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 1300);
}

function setLoading(isLoading) {
  const btn = document.getElementById("askBtn");
  const spinner = document.getElementById("spinner");
  btn.disabled = isLoading;
  spinner.style.display = isLoading ? "inline-block" : "none";
  btn.querySelector("span").textContent = isLoading ? "Асууж байна..." : "Асуух";
}

function parseLegacyAnswer(text) {
  // Supports:
  // Text2SQL ... \nSQL:\n<sql>\n\nDATA (top N):\ncol1 | col2\nv1 | v2
  const out = { answerText: text, sql: "", columns: [], rows: [] };
  if (!text) return out;

  const sqlIdx = text.indexOf("SQL:");
  if (sqlIdx === -1) return out;

  // find data section
  const dataIdx = text.indexOf("\n\nDATA");
  if (dataIdx === -1) {
    // only SQL present
    out.sql = text.slice(sqlIdx + 4).trim();
    out.answerText = text.slice(0, sqlIdx).trim();
    return out;
  }

  out.answerText = text.slice(0, sqlIdx).trim();
  out.sql = text.slice(sqlIdx + 4, dataIdx).trim();

  const dataBlock = text.slice(dataIdx).trim();
  // locate header line after first newline
  const lines = dataBlock.split("\n").map(l => l.trim()).filter(Boolean);

  // find the first line that contains " | " -> header
  const headerLine = lines.find(l => l.includes(" | "));
  if (!headerLine) return out;

  const headerPos = lines.indexOf(headerLine);
  const cols = headerLine.split(" | ").map(x => x.trim());
  out.columns = cols;

  const rowLines = lines.slice(headerPos + 1).filter(l => l.includes(" | "));
  out.rows = rowLines.map(l => l.split(" | ").map(x => x.trim()));
  return out;
}

function renderSql(sql) {
  const sqlWrap = document.getElementById("sqlWrap");
  const sqlPre = document.getElementById("sqlPre");
  const sqlCopy = document.getElementById("sqlCopy");

  if (!sql) {
    sqlWrap.style.display = "none";
    sqlPre.textContent = "";
    return;
  }
  sqlWrap.style.display = "block";
  sqlPre.textContent = sql;

  sqlCopy.onclick = () => copyToClipboard(sql);
}

function renderTable(columns, rows) {
  const tableWrap = document.getElementById("tableWrap");
  const table = document.getElementById("resultTable");
  const meta = document.getElementById("resultMeta");

  if (!columns || columns.length === 0) {
    tableWrap.style.display = "none";
    table.innerHTML = "";
    meta.textContent = "";
    return;
  }

  tableWrap.style.display = "block";
  meta.textContent = `Rows: ${rows ? rows.length : 0}  •  Columns: ${columns.length}`;

  // build table
  let thead = "<thead><tr>";
  for (const c of columns) thead += `<th>${escapeHtml(c)}</th>`;
  thead += "</tr></thead>";

  let tbody = "<tbody>";
  const safeRows = rows || [];
  const maxShow = Math.min(safeRows.length, 200); // UI cap
  for (let i = 0; i < maxShow; i++) {
    tbody += "<tr>";
    const r = safeRows[i];
    for (let j = 0; j < columns.length; j++) {
      const v = (r && r[j] !== undefined) ? r[j] : "";
      tbody += `<td>${escapeHtml(v)}</td>`;
    }
    tbody += "</tr>";
  }
  tbody += "</tbody>";

  table.innerHTML = thead + tbody;

  // show "more rows" hint if truncated
  const hint = document.getElementById("moreHint");
  hint.style.display = safeRows.length > maxShow ? "block" : "none";
  if (safeRows.length > maxShow) {
    hint.textContent = `UI дээр ${maxShow} мөр хүртэл харууллаа (нийт ${safeRows.length}).`;
  }
}

function renderAnswerText(text) {
  const ans = document.getElementById("answerText");
  ans.textContent = text || "";
}

function setActiveTab(name) {
  const tabs = ["tabPretty", "tabRaw"];
  for (const t of tabs) document.getElementById(t).classList.remove("active");

  const panels = ["panelPretty", "panelRaw"];
  for (const p of panels) document.getElementById(p).style.display = "none";

  if (name === "pretty") {
    document.getElementById("tabPretty").classList.add("active");
    document.getElementById("panelPretty").style.display = "block";
  } else {
    document.getElementById("tabRaw").classList.add("active");
    document.getElementById("panelRaw").style.display = "block";
  }
}

async function ask() {
  const q = document.getElementById("question").value.trim();
  const agent = document.getElementById("agent").value;
  const rawPre = document.getElementById("rawPre");

  if (!q) return toast("Асуултаа бичнэ үү");

  setLoading(true);
  renderSql("");
  renderTable([], []);
  renderAnswerText("");
  rawPre.textContent = "";

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

    // 1) Raw panel always shows JSON
    rawPre.textContent = JSON.stringify(data, null, 2);

    // 2) Preferred structured format:
    // {
    //   answer: "...",
    //   sql: "SELECT ...",
    //   columns: ["a","b"],
    //   rows: [[...],[...]]
    // }
    let sql = data.sql || "";
    let columns = data.columns || (data.data && data.data.columns) || [];
    let rows = data.rows || (data.data && data.data.rows) || [];

    // 3) Legacy: answer string containing SQL/DATA
    const answerStr = data.answer || "";
    if (!sql && answerStr) {
      const parsed = parseLegacyAnswer(answerStr);
      sql = parsed.sql || "";
      columns = parsed.columns || [];
      rows = parsed.rows || [];
      renderAnswerText(parsed.answerText || "");
    } else {
      renderAnswerText(answerStr || "");
    }

    renderSql(sql);
    renderTable(columns, rows);

    // choose tab: if table exists -> pretty, else raw
    setActiveTab(columns && columns.length ? "pretty" : "raw");

  } catch (e) {
    toast("❌ Алдаа");
    document.getElementById("rawPre").textContent = String(e);
    setActiveTab("raw");
  } finally {
    setLoading(false);
  }
}

// hotkeys
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("askBtn").addEventListener("click", ask);

  document.getElementById("question").addEventListener("keydown", (ev) => {
    // Ctrl+Enter -> ask
    if ((ev.ctrlKey || ev.metaKey) && ev.key === "Enter") {
      ev.preventDefault();
      ask();
    }
  });

  document.getElementById("tabPretty").addEventListener("click", () => setActiveTab("pretty"));
  document.getElementById("tabRaw").addEventListener("click", () => setActiveTab("raw"));

  setActiveTab("pretty");
});
