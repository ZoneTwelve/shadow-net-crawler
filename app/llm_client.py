# app/llm_client.py
import requests
import os

def summarize_html_to_markdown(html_text: str, url: str, model: str | None = None, system_prompt: str | None = None):
    """
    Convert HTML to clean, structured Markdown suitable for LLM processing.
    """
    base = os.environ.get("OPENAI_BASE_URL")
    api_key = os.environ.get("OPENAI_API_KEY")
    if base is None or api_key is None:
        raise RuntimeError("Set OPENAI_BASE_URL and OPENAI_API_KEY environment variables")

    model = model or os.environ.get("MODEL", "gpt-4o")

    system_prompt = system_prompt or (
        "You are a content extraction assistant. "
        "Your job is to take the raw HTML of a webpage and produce a clean, complete Markdown document "
        "that preserves the logical structure and important content of the page so it is friendly for further LLM processing.\n\n"
        "Rules:\n"
        "- Keep the original content order where possible.\n"
        "- Preserve all meaningful headings (#, ##, ###), bullet/numbered lists, code blocks, tables, and key inline formatting.\n"
        "- Remove navigation menus, headers/footers, ads, cookie notices, popups, and repeated boilerplate.\n"
        "- Keep the main article or body content intact.\n"
        "- Do NOT overly summarize; only shorten where text is clearly redundant or irrelevant.\n"
        "- At the very top, include:\n"
        "  1. The page title as a first-level heading.\n"
        "  2. A concise 2–4 sentence overview of the page’s purpose and main content.\n"
        "- Ensure the Markdown is syntactically correct and readable by both humans and LLMs.\n"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": f"URL: {url}\n\nHTML CONTENT:\n{html_text}\n\n"
                       f"Please convert the HTML into LLM-friendly Markdown according to the rules."
        }
    ]

    payload = {"model": model, "messages": messages, "temperature": 0.0}
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(f"{base}/v1/chat/completions", json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data.get("choices"), list) and len(data["choices"]) > 0:
        return data["choices"][0]["message"]["content"]
    else:
        raise RuntimeError(f"Unexpected response from LLM: {data}")

