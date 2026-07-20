"""
Telegram-нотификатор для seo-agent.

Единственный канал доставки алертов (решение SA-002, см. ТЗ Агент_SEO_система_2026_05_19.md).

Использование:
    from seo_agent.notifiers import send_telegram, send_telegram_markdown

    send_telegram("Простое сообщение")
    send_telegram_markdown("*Жирный* и `код` через Markdown V2")

ENV:
    TELEGRAM_BOT_TOKEN — токен бота от BotFather
    TELEGRAM_CHAT_ID   — id канала/чата (отрицательное для каналов)
"""

from __future__ import annotations

import os
import logging
import requests
from typing import Optional

log = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
MAX_MESSAGE_LENGTH = 4096  # лимит Telegram


def _credentials() -> tuple[str, list[str]]:
    """Токен + список chat_id. TELEGRAM_CHAT_ID может содержать несколько
    получателей через запятую (напр. "-1001234,683646991")."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    raw = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    chat_ids = [c.strip() for c in raw.split(",") if c.strip()]
    if not token or not chat_ids:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID не заданы. "
            "См. docs/seo/agent-system/access-checklist.md, пункт 1."
        )
    return token, chat_ids


def _send(text: str, parse_mode: Optional[str] = None) -> dict:
    """Шлёт всем получателям. Сбой доставки одному не мешает остальным.
    Возвращает ответ по последнему успешному получателю."""
    token, chat_ids = _credentials()
    url = TELEGRAM_API.format(token=token)
    last: dict = {}
    for chat_id in chat_ids:
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            last = response.json()
        except Exception as e:  # noqa: BLE001 — один получатель не должен ронять доставку
            log.warning("Telegram: не доставлено получателю %s: %s", chat_id, e)
    return last


def send_telegram(text: str) -> dict:
    """Отправить простое текстовое сообщение в Telegram-канал/чат.

    Длинные сообщения автоматически разбиваются на куски по 4000 символов.
    Возвращает ответ Telegram API для последнего отправленного куска.
    """
    if len(text) <= MAX_MESSAGE_LENGTH:
        return _send(text)

    chunks = _split_message(text, limit=4000)
    last_response = {}
    for chunk in chunks:
        last_response = _send(chunk)
    return last_response


def send_telegram_markdown(text: str) -> dict:
    """Отправить сообщение с разметкой Telegram MarkdownV2."""
    if len(text) <= MAX_MESSAGE_LENGTH:
        return _send(text, parse_mode="MarkdownV2")

    chunks = _split_message(text, limit=4000)
    last_response = {}
    for chunk in chunks:
        last_response = _send(chunk, parse_mode="MarkdownV2")
    return last_response


def _split_message(text: str, limit: int = 4000) -> list[str]:
    """Бьём длинное сообщение по строкам, не разрывая середину строки."""
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in text.splitlines(keepends=True):
        if current_len + len(line) > limit and current:
            chunks.append("".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


if __name__ == "__main__":
    # Smoke-test: запустить локально с заданными ENV — проверит, что бот доставит сообщение.
    logging.basicConfig(level=logging.INFO)
    result = send_telegram(
        "✅ seo-agent: smoke-test Telegram-нотификатора. "
        "Если ты это видишь — токен и chat_id настроены правильно."
    )
    log.info("Сообщение отправлено, response: %s", result)
