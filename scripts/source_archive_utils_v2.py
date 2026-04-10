from __future__ import annotations

import html
import re
import sys
from pathlib import Path

from bs4 import BeautifulSoup, Comment, NavigableString, Tag
from bs4 import FeatureNotFound


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_ROOT = REPO_ROOT / "reports"


ALLOWED_ATTRS = {
    "a": {"href"},
    "img": {"src", "alt", "loading"},
    "td": {"colspan", "rowspan"},
    "th": {"colspan", "rowspan"},
}


def configure_utf8_stdio() -> None:
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")


def make_soup(markup: str) -> BeautifulSoup:
    for parser in ("lxml", "html.parser"):
        try:
            return BeautifulSoup(markup, parser)
        except FeatureNotFound:
            continue
    raise RuntimeError("No HTML parser available for BeautifulSoup")


def repair_text(text: str | None) -> str:
    if not text:
        return ""
    value = html.unescape(text)
    value = value.replace("\u00a0", " ").replace("\u00c2 ", " ").replace("\u00c2", "")
    if any(marker in value for marker in ("\u00c3", "\u00c2", "\u00e2", "\u00f0")):
        try:
            repaired = value.encode("latin1").decode("utf-8")
            if repaired.count("\u00c3") < value.count("\u00c3"):
                value = repaired
        except UnicodeError:
            pass
    return re.sub(r"\s+", " ", value).strip()


def sanitize_filename(value: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", value)


def slugify(value: str) -> str:
    value = repair_text(value).lower().strip()
    value = re.sub(r"[^\w\s-]", "", value)
    value = re.sub(r"[\s_-]+", "-", value)
    return value[:80].strip("-")


def humanize_slug(value: str) -> str:
    cleaned = repair_text(value.replace("-", " ").replace("_", " "))
    return cleaned[:140].strip()


def clean_node_text(root: Tag) -> None:
    for node in list(root.descendants):
        if isinstance(node, Comment):
            node.extract()
            continue
        if isinstance(node, NavigableString):
            fixed = repair_text(str(node))
            if fixed:
                node.replace_with(fixed)
            else:
                node.extract()


def strip_attrs(root: Tag) -> None:
    for tag in root.find_all(True):
        allowed = ALLOWED_ATTRS.get(tag.name, set())
        tag.attrs = {key: value for key, value in tag.attrs.items() if key in allowed}


def remove_empty_tags(root: Tag) -> None:
    for tag in list(root.find_all(True)):
        if tag.name in {"img", "br", "hr"}:
            continue
        if tag.find(["img", "table", "hr", "br"]) is not None:
            continue
        if tag.name == "a" and tag.get("href"):
            if repair_text(tag.get_text(" ", strip=True)):
                continue
        if not repair_text(tag.get_text(" ", strip=True)):
            tag.decompose()


def unwrap_redundant_containers(root: Tag) -> None:
    changed = True
    while changed:
        changed = False
        for tag in list(root.find_all(["div", "section", "span"])):
            if tag.parent is None:
                continue
            if tag.name == "section":
                continue
            if tag.attrs:
                continue
            if any(child.name in {"table", "img", "a"} for child in tag.find_all(recursive=False)):
                continue
            if len([child for child in tag.children if isinstance(child, Tag)]) <= 1:
                tag.unwrap()
                changed = True


def standard_archive_html(
    *,
    title: str,
    archive_source: str,
    source_url: str,
    published_date: str,
    category: str,
    body_html: str,
    source_label: str,
    tags: list[str] | None = None,
    extra_html: str = "",
    accent_color: str = "#1a6b3c",
) -> str:
    title = repair_text(title) or "Untitled Source Document"
    source_label = repair_text(source_label)
    category = repair_text(category)
    published_date = repair_text(published_date)
    source_url = repair_text(source_url)
    tags = [repair_text(tag) for tag in (tags or []) if repair_text(tag)]
    tag_html = ""
    if tags:
        tag_html = f'<p class="tags">Tags: {" | ".join(tags)}</p>'

    css = f"""
*, *::before, *::after {{ box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Georgia, serif;
    font-size: 15px;
    line-height: 1.75;
    color: #1a1a1a;
    max-width: 920px;
    margin: 0 auto;
    padding: 30px 22px;
    background: #fff;
}}
h1 {{ font-size: 1.65em; color: #1a1a1a; margin: 0 0 8px; line-height: 1.3; }}
h2 {{ font-size: 1.2em; color: #222; margin: 22px 0 8px; }}
h3 {{ font-size: 1.05em; color: #222; margin: 16px 0 6px; }}
p  {{ margin: 0 0 12px; }}
a  {{ color: {accent_color}; }}
img {{ max-width: 100%; height: auto; margin: 14px 0; border-radius: 4px; display: block; }}
.meta {{
    font-size: 0.82em;
    color: #555;
    margin: 6px 0 18px;
    padding: 8px 12px;
    background: #f4f6f8;
    border-left: 3px solid {accent_color};
    border-radius: 2px;
}}
.source-tag {{ font-weight: 600; color: {accent_color}; }}
.tags {{ font-size: 0.82em; color: #777; margin-top: 22px; }}
.archive-note {{
    background: #f8f8f8;
    border: 1px solid #e3e3e3;
    border-radius: 4px;
    padding: 10px 12px;
    margin: 14px 0;
    font-size: 0.88em;
    color: #555;
}}
hr {{ border: none; border-top: 1px solid #e0e0e0; margin: 18px 0; }}
blockquote {{ border-left: 3px solid #ccc; margin: 14px 0; padding: 4px 14px; color: #444; }}
table {{ width: 100%; border-collapse: collapse; margin: 14px 0; font-size: 0.9em; }}
th {{ background: {accent_color}; color: #fff; padding: 7px 10px; text-align: left; }}
td {{ padding: 6px 10px; border-bottom: 1px solid #dde3ea; }}
tr:nth-child(even) td {{ background: #f5f8fb; }}
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <meta name="archive-source" content="{html.escape(archive_source)}">
  <meta name="archive-category" content="{html.escape(category)}">
  <meta name="archive-url" content="{html.escape(source_url)}">
  <meta name="archive-date" content="{html.escape(published_date)}">
  <style>{css}</style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class="meta">
    <span class="source-tag">{html.escape(source_label or archive_source)}</span>
    &nbsp;|&nbsp; {html.escape(published_date or "unknown")}
    &nbsp;|&nbsp; <a href="{html.escape(source_url)}">{html.escape(source_url)}</a>
  </div>
  <hr>
  {body_html}
  {extra_html}
  {tag_html}
</body>
</html>"""
