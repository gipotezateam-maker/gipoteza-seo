"""
M6 — Competitor watcher.

Что делает:
  1. Краулит главные страницы и /blog/ конкурентов из docs/seo/agent-system/competitors.
  2. Извлекает структуру: title, h1, h2, объём, количество ссылок, наличие FAQ/JSON-LD.
  3. Сравнивает с нашим сайтом (M2-данные).
  4. Если у конкурента появилась новая статья в /blog/ или /news/ за последнюю неделю —
     алертит. Это сигнал, что конкурент ловит горячую тему.
  5. Опционально (если ANTHROPIC_API_KEY и есть баланс): через Claude извлекает
     ключевые ideas из структуры — что нового они показывают.

Запуск:
    python3 -m modules.m6_competitors             # production
    python3 -m modules.m6_competitors --dry-run   # без Telegram

Это не SERP-парсер (нет Я.XML / Topvisor) — это **структурный анализ** сайтов конкурентов.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR.parent) not in sys.path:
    sys.path.insert(0, str(THIS_DIR.parent))

from modules.crawler import crawl, fetch_sitemap_urls  # noqa: E402

log = logging.getLogger(__name__)

REPO_ROOT = THIS_DIR.parent.parent
DATA_DIR = THIS_DIR.parent / "data" / "competitors"
COMPETITORS_FILE = DATA_DIR / "competitors.json"

# Наш домен + агрегаторы/площадки, которые НЕ считаем прямыми конкурентами при автопоиске.
OWN_DOMAIN = "gipoteza-agency.ru"
IGNORE_DOMAINS = {
    "gipoteza-agency.ru", "yandex.ru", "ya.ru", "google.com", "youtube.com",
    "vk.com", "dzen.ru", "habr.com", "vc.ru", "rbc.ru", "wikipedia.org",
    "skillbox.ru", "geekbrains.ru", "netology.ru", "sberbank.ru", "t.me",
    "ozon.ru", "wildberries.ru", "avito.ru", "2gis.ru", "pikabu.ru",
}


def load_competitors() -> list[dict]:
    """Список конкурентов из data/competitors/competitors.json (seed + discovered).
    Падать не должен: при отсутствии файла возвращает пустой список."""
    if not COMPETITORS_FILE.exists():
        log.warning("Нет %s — список конкурентов пуст", COMPETITORS_FILE)
        return []
    try:
        data = json.loads(COMPETITORS_FILE.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        log.warning("Не удалось прочитать %s: %s", COMPETITORS_FILE, e)
        return []
    seed = data.get("seed", [])
    discovered = data.get("discovered", [])
    # дедуп по домену homepage
    out, seen = [], set()
    for c in [*seed, *discovered]:
        dom = urlparse(c.get("homepage", "")).netloc.lower().lstrip("www.")
        if dom and dom not in seen:
            seen.add(dom)
            out.append(c)
    return out


def analyse_page(url: str) -> dict:
    """Структурный анализ одной страницы."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "bs4 not installed"}

    result = crawl(url)
    if result.error or result.status >= 400 or not result.html:
        return {"error": result.error or f"HTTP {result.status}"}

    soup = BeautifulSoup(result.html, "html.parser")

    title_tag = soup.find("title")
    h1_tag = soup.find("h1")
    h2_tags = soup.find_all("h2")
    img_tags = soup.find_all("img")
    a_tags = soup.find_all("a", href=True)

    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    json_ld_types: list[str] = []
    for s in json_ld_scripts:
        try:
            data = json.loads((s.string or s.text or "").strip())
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and "@type" in item:
                    t = item["@type"]
                    if isinstance(t, list):
                        json_ld_types.extend(t)
                    else:
                        json_ld_types.append(t)
        except Exception:
            pass

    body_text = soup.body.get_text(" ", strip=True) if soup.body else ""
    word_count = len(re.findall(r"\b[\w-]+\b", body_text))

    return {
        "url": url,
        "status": result.status,
        "title": (title_tag.text.strip() if title_tag else "").strip()[:200],
        "h1": (h1_tag.text.strip() if h1_tag else "").strip()[:200],
        "h2_count": len(h2_tags),
        "h2_examples": [h.text.strip()[:80] for h in h2_tags[:5]],
        "img_count": len(img_tags),
        "links_count": len(a_tags),
        "json_ld_types": sorted(set(json_ld_types)),
        "word_count": word_count,
    }


def find_recent_blog_urls(sitemap_url: str, days: int = 30, limit: int = 100) -> list[str]:
    """Найти URLs из sitemap, по которым lastmod моложе N дней (не все sitemap это даёт)."""
    try:
        from bs4 import BeautifulSoup
        import requests
        r = requests.get(sitemap_url, timeout=20,
                         headers={"User-Agent": "seo-agent/1.0"})
        if r.status_code >= 400:
            return []
        soup = BeautifulSoup(r.content, "xml")
        cutoff = (dt.date.today() - dt.timedelta(days=days)).isoformat()
        out: list[str] = []
        for url in soup.find_all("url"):
            loc = url.find("loc")
            if not loc:
                continue
            url_text = loc.text.strip()
            lastmod = url.find("lastmod")
            mod_date = lastmod.text.strip()[:10] if lastmod else ""
            # Берём только URL, похожие на блог/статью
            path = urlparse(url_text).path.lower()
            if any(seg in path for seg in ("/blog/", "/news/", "/journal/", "/articles/")):
                if not mod_date or mod_date >= cutoff:
                    out.append(url_text)
            if len(out) >= limit:
                break
        return out
    except Exception as e:
        log.warning("Sitemap %s: %s", sitemap_url, e)
        return []


def _discovery_queries(limit: int = 15) -> list[str]:
    """Запросы для автопоиска конкурентов — из tracked-keywords.json."""
    kw_file = THIS_DIR.parent / "data" / "rankings" / "tracked-keywords.json"
    if not kw_file.exists():
        return []
    try:
        kws = json.loads(kw_file.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    return [k for k in kws if isinstance(k, str) and k.strip()][:limit]


def discover_competitors() -> list[str]:
    """Автопоиск конкурентов: по нашим запросам смотрит выдачу Яндекса (Yandex XML API)
    и добавляет домены, которые ранжируются по ≥2 запросам, в competitors.json → discovered.
    Требует YANDEX_XML_USER + YANDEX_XML_KEY (yandex.ru/dev/xml). Без ключей — no-op.
    Возвращает список новых доменов. Никогда не бросает."""
    user = os.environ.get("YANDEX_XML_USER", "").strip()
    key = os.environ.get("YANDEX_XML_KEY", "").strip()
    if not (user and key):
        log.info("Автопоиск конкурентов пропущен: задай YANDEX_XML_USER + YANDEX_XML_KEY "
                 "(yandex.ru/dev/xml), чтобы агент сам находил конкурентов по выдаче.")
        return []
    queries = _discovery_queries()
    if not queries:
        log.info("Нет запросов для автопоиска (data/rankings/tracked-keywords.json)")
        return []

    import xml.etree.ElementTree as ET
    import requests

    domain_hits: "Counter[str]" = Counter()
    for q in queries:
        try:
            r = requests.get(
                "https://yandex.ru/search/xml",
                params={"user": user, "key": key, "query": q, "l10n": "ru",
                        "sortby": "rlv", "groupby": "attr=d.mode=deep.groups-on-page=10.docs-in-group=1"},
                timeout=30,
            )
            r.raise_for_status()
            root = ET.fromstring(r.text)
            seen_in_q = set()
            for url_el in root.iter("url"):
                dom = urlparse((url_el.text or "").strip()).netloc.lower()
                dom = dom[4:] if dom.startswith("www.") else dom
                if dom and dom not in IGNORE_DOMAINS and dom not in seen_in_q:
                    seen_in_q.add(dom)
                    domain_hits[dom] += 1
        except Exception as e:  # noqa: BLE001
            log.warning("Yandex XML по запросу '%s': %s", q, e)

    # существующие домены
    data = json.loads(COMPETITORS_FILE.read_text(encoding="utf-8")) if COMPETITORS_FILE.exists() else {"seed": [], "discovered": []}
    known = {urlparse(c.get("homepage", "")).netloc.lower().lstrip("www.")
             for c in [*data.get("seed", []), *data.get("discovered", [])]}
    new_domains = [d for d, n in domain_hits.most_common() if n >= 2 and d not in known]
    for dom in new_domains:
        data.setdefault("discovered", []).append({
            "name": dom,
            "homepage": f"https://{dom}/",
            "blog_url": f"https://{dom}/blog",
            "sitemap": f"https://{dom}/sitemap.xml",
        })
    if new_domains:
        COMPETITORS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Автопоиск: добавлено %d новых конкурентов: %s", len(new_domains), ", ".join(new_domains))
    else:
        log.info("Автопоиск: новых конкурентов не найдено")
    return new_domains


def run_competitors(dry_run: bool = False) -> Path:
    today = dt.date.today().isoformat()
    log.info("=== M6 competitor watcher · %s ===", today)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # автопоиск новых конкурентов по нашим запросам (если настроен Yandex XML)
    discover_competitors()

    report: dict = {"date": today, "competitors": []}
    for comp in load_competitors():
        log.info("Анализирую %s — %s", comp["name"], comp["homepage"])
        homepage_data = analyse_page(comp["homepage"])
        recent = find_recent_blog_urls(comp["sitemap"], days=14)
        log.info("  Свежих URL за 14 дней (sitemap): %d", len(recent))
        report["competitors"].append({
            "name": comp["name"],
            "homepage_analysis": homepage_data,
            "recent_urls": recent[:30],
            "recent_count": len(recent),
        })

    path = DATA_DIR / f"{today}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Снимок: %s", path)

    # Telegram сводка
    if not dry_run and os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        from notifiers.telegram import send_telegram
        lines = [f"👁 Что у конкурентов · {today}"]
        lines.append("\nСмотрим, как часто конкуренты выпускают статьи и насколько "
                     "наполнены их главные страницы — чтобы держать темп.")
        for c in report["competitors"]:
            recent = c.get("recent_count", 0)
            home = c.get("homepage_analysis", {})
            error = home.get("error")
            if error:
                lines.append(f"\n{c['name']}: не удалось проверить ({error})")
                continue
            lines.append(f"\n{c['name']}")
            lines.append(f"  Главная страница: {home.get('word_count', 0)} слов, "
                         f"{home.get('h2_count', 0)} подзаголовков, {home.get('img_count', 0)} картинок")
            lines.append(f"  Новых статей за 2 недели: {recent}")
        send_telegram("\n".join(lines))

    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="M6 competitor watcher")
    parser.add_argument("--dry-run", action="store_true", help="Не слать в Telegram")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run_competitors(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
