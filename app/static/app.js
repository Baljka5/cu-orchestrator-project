// app/static/app.js
function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, (m) => ({
        "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[m]));
}

function renderTable(columns, rows) {
    if (!columns || columns.length === 0) return "";
    const head = columns.map(c => `<th>${esc(c)}</th>`).join("");
    const body = (rows || []).map(r =>
        `<tr>${r.map(v => `<td>${esc(v)}</td>`).join("")}</tr>`
    ).join("");
    return `
    <div class="tbl-wrap">
      <table class="tbl">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

async function ask() {
    const q = document.getElementById("question").value;
    const agent = document.getElementById("agent").value;
    const out = document.getElementById("answer");

    out.innerHTML = `<div class="muted">Боловсруулж байна...</div>`;

    try {
        const res = await fetch("/api/chat", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({
                message: q,
                force_agent: agent === "auto" ? null : agent
            })
        });

        const data = await res.json();
        const answer = data.answer || "";
        const meta = data.meta || {};

        const sql = meta.sql || "";
        const notes = meta.notes || "";
        const table = meta.data ? renderTable(meta.data.columns, meta.data.rows) : "";

        out.innerHTML = `
      <div class="card">
        <div class="section">
          <div class="label">Хариу</div>
          <div class="answer">${esc(answer).replace(/\n/g, "<br>")}</div>
        </div>

        ${notes ? `
          <div class="section">
            <div class="label">Notes</div>
            <div class="muted">${esc(notes)}</div>
          </div>
        ` : ""}

        ${sql ? `
          <div class="section">
            <div class="label-row">
              <div class="label">SQL</div>
              <button class="btn" id="copySql">Copy</button>
            </div>
            <pre class="code"><code>${esc(sql)}</code></pre>
          </div>
        ` : ""}

        ${table ? `
          <div class="section">
            <div class="label">DATA</div>
            ${table}
          </div>
        ` : ""}
      </div>
    `;

        const btn = document.getElementById("copySql");
        if (btn && sql) {
            btn.onclick = async () => {
                try {
                    await navigator.clipboard.writeText(sql);
                    btn.textContent = "Copied";
                } catch {
                    btn.textContent = "Copy failed";
                }
                setTimeout(() => btn.textContent = "Copy", 1200);
            };
        }

    } catch (e) {
        out.innerHTML = `<div class="err">❌ Алдаа: ${esc(e)}</div>`;
    }
}
