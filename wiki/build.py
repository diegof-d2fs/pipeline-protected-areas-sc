"""Gera o site estático da wiki (GitHub Pages) a partir de wiki/index.md.

O markdown segue o padrão Wiki.js usado nas wikis do projeto: avisos com `{.is-info}`,
`{.is-warning}`, `{.is-success}` ou `{.is-danger}` na linha seguinte ao bloco de citação,
diagramas em blocos ```mermaid e remissões internas por âncora. O build falha se uma âncora
interna não existir, para que um título renomeado não deixe link quebrado no ar.

Uso: python wiki/build.py [pasta_de_saída]   (padrão: wiki/_site)
"""
from __future__ import annotations

import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import markdown
from bs4 import BeautifulSoup
from markdown.extensions.toc import slugify_unicode

ROOT = Path(__file__).resolve().parent
OUT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "_site"
CALLOUTS = ("is-info", "is-warning", "is-success", "is-danger")


CALLOUT_BLOCK = re.compile(r"((?:^>.*\n)+)\{\.(" + "|".join(CALLOUTS) + r")\}[ \t]*$", re.M)


def explicit_callouts(text: str) -> str:
    """Transforma cada aviso num blockquote próprio.

    No python-markdown, citações separadas só por linha em branco viram um único bloco, e dois
    avisos seguidos (como info e warning) se fundiriam. O Wiki.js os trata como blocos distintos.
    """
    def repl(m: re.Match) -> str:
        inner = re.sub(r"^> ?", "", m.group(1), flags=re.M)
        return f'<blockquote class="callout {m.group(2)}" markdown="1">\n\n{inner}\n</blockquote>'

    return CALLOUT_BLOCK.sub(repl, text)


def render_markdown(text: str) -> tuple[str, list[dict]]:
    text = explicit_callouts(text)
    md = markdown.Markdown(
        extensions=["tables", "fenced_code", "attr_list", "sane_lists", "toc", "md_in_html"],
        extension_configs={"toc": {"slugify": slugify_unicode, "toc_depth": "2-3"}},
    )
    return md.convert(text), md.toc_tokens


def postprocess(body: str) -> tuple[str, str]:
    soup = BeautifulSoup(body, "html.parser")

    title_tag = soup.find("h1")
    title = title_tag.get_text(" ", strip=True) if title_tag else "Wiki"

    for code in soup.select("pre > code.language-mermaid"):
        div = soup.new_tag("div", attrs={"class": "mermaid"})
        div.string = code.get_text()
        code.parent.replace_with(div)

    for table in soup.find_all("table"):
        wrap = soup.new_tag("div", attrs={"class": "table-wrap"})
        table.wrap(wrap)

    for img in soup.find_all("img"):
        img["loading"] = "lazy"
        if not img.get("alt"):
            caption = img.find_parent("p").find_next_sibling("p") if img.find_parent("p") else None
            img["alt"] = caption.get_text(strip=True) if caption and caption.find("em") else "Captura de tela"

    ids = {tag["id"] for tag in soup.find_all(id=True)}
    broken = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#"):
            if href[1:] not in ids:
                broken.append(href)
        elif href.startswith(("http://", "https://")):
            a["target"] = "_blank"
            a["rel"] = "noopener noreferrer"
    if broken:
        raise SystemExit("Âncoras internas inexistentes: " + ", ".join(sorted(set(broken))))

    return title, str(soup)


def render_toc(tokens: list[dict]) -> str:
    items = []
    for token in tokens:
        if token["level"] == 1:
            items.append(render_toc(token["children"]))
            continue
        children = token.get("children") or []
        sub = ""
        if children:
            sub = "<ul>" + "".join(
                f'<li><a href="#{c["id"]}">{c["name"]}</a></li>' for c in children
            ) + "</ul>"
        items.append(f'<li><a href="#{token["id"]}">{token["name"]}</a>{sub}</li>')
    return "".join(items)


def main() -> None:
    body, toc_tokens = render_markdown((ROOT / "index.md").read_text(encoding="utf-8"))
    title, body = postprocess(body)
    template = (ROOT / "template.html").read_text(encoding="utf-8")
    page = (
        template.replace("{{title}}", title)
        .replace("{{toc}}", "<ul>" + render_toc(toc_tokens) + "</ul>")
        .replace("{{content}}", body)
        .replace("{{built}}", datetime.now(timezone.utc).strftime("%d/%m/%Y"))
    )
    if OUT.exists():
        shutil.rmtree(OUT)
    (OUT / "assets").mkdir(parents=True)
    (OUT / "index.html").write_text(page, encoding="utf-8")
    shutil.copytree(ROOT / "img", OUT / "img")
    for asset in (ROOT / "assets").iterdir():
        shutil.copy2(asset, OUT / "assets" / asset.name)
    (OUT / ".nojekyll").touch()
    print(f"wiki gerada em {OUT}")


if __name__ == "__main__":
    main()
