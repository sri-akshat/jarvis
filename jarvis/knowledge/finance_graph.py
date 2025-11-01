"""Utilities for aggregating payment data from the Neo4j knowledge graph."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import requests

from jarvis.knowledge.neo4j_exporter import Neo4jConnectionConfig

logger = logging.getLogger(__name__)


FLOAT_PATTERN = re.compile(r"[0-9]+(?:[.,][0-9]+)?")


@dataclass
class PaymentMention:
    amount_text: str
    amount_value: Optional[float]
    currency: Optional[str]
    subject: Optional[str]
    filename: Optional[str]
    content_type: Optional[str]
    message_id: Optional[str]
    attachment_id: Optional[str]


def _normalise_currency(text: str) -> Optional[str]:
    lowered = text.lower()
    if "usd" in lowered or "$" in lowered:
        return "USD"
    if any(token in lowered for token in ("inr", "rs", "₹")):
        return "INR"
    if "eur" in lowered or "€" in lowered:
        return "EUR"
    if "gbp" in lowered or "£" in lowered:
        return "GBP"
    return None


def _parse_amount(text: str) -> Optional[float]:
    cleaned = text.replace(",", "")
    match = FLOAT_PATTERN.search(cleaned)
    if not match:
        return None
    try:
        value = float(match.group(0))
    except ValueError:
        return None
    lowered = cleaned.lower()
    if value < 1000:
        if "crore" in lowered:
            value *= 10_000_000
        elif "lakh" in lowered or "lakhs" in lowered or "lac" in lowered:
            value *= 100_000
        elif "thousand" in lowered:
            value *= 1_000
    return value


def _extract_amount(canonical: str, aliases: Iterable[str] | None) -> Tuple[Optional[float], Optional[str]]:
    candidates = [canonical] + list(aliases or [])
    for candidate in candidates:
        value = _parse_amount(candidate)
        if value is not None:
            return value, candidate
    return None, canonical


def collect_payments_from_graph(
    connection: Neo4jConnectionConfig,
    canonical_name: str,
    limit: Optional[int] = None,
) -> List[PaymentMention]:
    """Fetch payment-related money mentions for the given counterparty."""
    query = """
    MATCH (counter {canonical: $canonical})-[:MENTIONED_IN]->(content:Content)
    MATCH (money:MONEY)-[:MENTIONED_IN]->(content)
    RETURN DISTINCT
        money.canonical AS canonical,
        money.aliases AS aliases,
        content.subject AS subject,
        content.filename AS filename,
        content.content_type AS content_type,
        content.message_id AS message_id,
        content.attachment_id AS attachment_id
    """
    if limit:
        query += " LIMIT $limit"

    params: Dict[str, object] = {"canonical": canonical_name.lower()}
    if limit:
        params["limit"] = limit

    from neo4j import GraphDatabase  # Imported lazily to avoid hard dependency outside runtime

    with GraphDatabase.driver(connection.uri, auth=(connection.user, connection.password)) as driver:
        with driver.session(database=connection.database) as session:
            records = session.run(query, params).data()

    mentions: List[PaymentMention] = []
    for record in records:
        canonical = record.get("canonical") or ""
        aliases = record.get("aliases") or []
        amount_value, amount_text = _extract_amount(canonical, aliases)
        currency = _normalise_currency((amount_text or "") + " " + canonical)
        mentions.append(
            PaymentMention(
                amount_text=amount_text or canonical,
                amount_value=amount_value,
                currency=currency,
                subject=record.get("subject"),
                filename=record.get("filename"),
                content_type=record.get("content_type"),
                message_id=record.get("message_id"),
                attachment_id=record.get("attachment_id"),
            )
        )
    logger.info(
        "[finance_graph] collected %s money mention(s) for %s",
        len(mentions),
        canonical_name,
    )
    return mentions


def aggregate_totals(mentions: Iterable[PaymentMention]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for mention in mentions:
        if mention.amount_value is None:
            continue
        currency = mention.currency or "UNKNOWN"
        totals.setdefault(currency, 0.0)
        totals[currency] += mention.amount_value
    return totals


class MistralLLMClient:
    def __init__(self, model: str = "mistral", endpoint: str = "http://localhost:11434/api/generate", timeout: int = 60):
        self.model = model
        self.endpoint = endpoint
        self.timeout = timeout

    def chat(self, prompt: str) -> str:
        payload = {"model": self.model, "prompt": prompt, "stream": False}
        response = requests.post(self.endpoint, json=payload, timeout=self.timeout)
        response.raise_for_status()
        body = response.json()
        if "response" in body:
            return body["response"]
        if "output" in body:
            return body["output"]
        choices = body.get("choices") or []
        if choices:
            return choices[0].get("message", {}).get("content", "")
        return ""


def build_llm_prompt(counterparty: str, mentions: List[PaymentMention], totals: Dict[str, float]) -> str:
    payload = [
        {
            "amount_text": mention.amount_text,
            "amount_value": mention.amount_value,
            "currency": mention.currency,
            "subject": mention.subject,
            "filename": mention.filename,
            "content_type": mention.content_type,
        }
        for mention in mentions
    ]
    data_json = json.dumps(payload, ensure_ascii=False, indent=2)
    totals_json = json.dumps(totals, ensure_ascii=False)
    return (
        f"You are helping analyse payments made to '{counterparty}'.\n"
        "The following finance mentions were extracted from a knowledge graph. "
        "Amounts may require interpretation from free-form text. "
        "Use the provided numeric extractions as hints, but double-check against the text if needed.\n\n"
        f"Mentions:\n{data_json}\n\n"
        f"Precomputed totals (per currency): {totals_json}\n\n"
        "Please produce a concise answer stating the total paid to this counterparty per currency, "
        "and briefly list the supporting payments. If any amounts look ambiguous, call that out."
    )


def ask_finance_question(
    connection: Neo4jConnectionConfig,
    counterparty: str,
    llm_client: MistralLLMClient,
    limit: Optional[int] = None,
) -> Tuple[List[PaymentMention], Dict[str, float], str]:
    mentions = collect_payments_from_graph(connection, counterparty, limit=limit)
    totals = aggregate_totals(mentions)
    prompt = build_llm_prompt(counterparty, mentions, totals)
    answer = llm_client.chat(prompt)
    return mentions, totals, answer


__all__ = [
    "PaymentMention",
    "collect_payments_from_graph",
    "aggregate_totals",
    "MistralLLMClient",
    "ask_finance_question",
]
