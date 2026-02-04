async function ask() {
  const q = document.getElementById("question").value;
  const agent = document.getElementById("agent").value;
  const out = document.getElementById("answer");

  out.textContent = "⏳ асууж байна...";

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
    out.textContent = data.answer || JSON.stringify(data, null, 2);
  } catch (e) {
    out.textContent = "❌ Алдаа: " + e;
  }
}
