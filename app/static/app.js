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
        `<tr>${(r || []).map(v => `<td>${esc(v)}</td>`).join("")}</tr>`
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

        // ✅ SQL-г аль ч тохиолдолд барьж авна:
        // - зарим агент meta.sql өгнө
        // - text2sql одоо answer дээр SQL буцааж байгаа
        const sql = (meta.sql || "").trim() || (answer || "").trim();

        const notes = meta.notes || "";

        // ✅ Table data бол meta.data дээр ирнэ
        const hasData = !!(meta.data && Array.isArray(meta.data.columns));
        const table = hasData ? renderTable(meta.data.columns, meta.data.rows) : "";

        // ✅ Error бол meta.error дээр харуулна
        const err = meta.error || (meta.data && meta.data.error) || "";

        out.innerHTML = `
      <div class="card">

        ${notes ? `
          <div class="section">
            <div class="label">Notes</div>
            <div class="muted">${esc(notes)}</div>
          </div>
        ` : ""}

        ${sql ? `
          <div class="section">
            <div class="label-row">
              <div class="label">QUERY (SQL)</div>
              <button class="btn" id="copySql">Copy</button>
            </div>
            <pre class="code"><code>${esc(sql)}</code></pre>

            ${err ? `
              <div class="err" style="margin-top:10px;">❌ Query error: ${esc(err)}</div>
            ` : ""}

            ${table ? `
              <div style="margin-top:12px;">
                <div class="label" style="margin-bottom:6px;">RESULT</div>
                ${table}
              </div>
            ` : ""}
          </div>
        ` : `
          <div class="section">
            <div class="label">Хариу</div>
            <div class="answer">${esc(answer).replace(/\n/g, "<br>")}</div>
          </div>
        `}
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