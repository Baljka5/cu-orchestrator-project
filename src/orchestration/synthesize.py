def synthesize(label: str, conf: float, route_name: str, agent_answer: str) -> str:
    return (
        f"📌 Ангилал: {label} (conf={conf:.2f})\n"
        f"🧭 Route: {route_name}\n\n"
        f"{agent_answer}"
    )
