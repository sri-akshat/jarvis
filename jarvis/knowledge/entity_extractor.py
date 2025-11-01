"""Entity extraction pipeline populating the knowledge graph."""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Iterable, List, Optional

import requests
import spacy
from spacy.language import Language
from spacy.tokens import Span

from jarvis.knowledge.semantic_indexer import SemanticIndexer

logger = logging.getLogger(__name__)


@dataclass
class TextSegment:
    content_id: str
    chunk_index: int
    text: str
    page: Optional[int]
    content_type: str
    message_id: Optional[str]
    attachment_id: Optional[str]
    filename: Optional[str]
    subject: Optional[str]


@dataclass
class EntityMention:
    text: str
    label: str
    start_char: int
    end_char: int
    confidence: Optional[float] = None
    metadata: Optional[dict] = None


class SpacyEntityExtractor:
    def __init__(self, model: str = "en_core_web_sm") -> None:
        try:
            self.nlp: Language = spacy.load(model)
        except OSError as exc:  # pragma: no cover
            raise RuntimeError(
                f"spaCy model '{model}' is not installed. Install via `python -m spacy download {model}`"
            ) from exc

    def extract(self, text: str) -> Iterable[EntityMention]:
        doc = self.nlp(text)
        for span in doc.ents:
            yield EntityMention(
                text=span.text.strip(),
                label=span.label_,
                start_char=span.start_char,
                end_char=span.end_char,
                confidence=self._confidence(span),
            )

    @staticmethod
    def _confidence(span: Span) -> Optional[float]:  # pragma: no cover
        return None


class LLMEntityExtractor:
    DEFAULT_PROMPT = """You are an expert information extraction assistant. From the text below, identify entities relevant to personal health, finances, and communications. Focus on:
- people (patients, relatives, clinicians)
- organizations (hospitals, labs, insurers, employers, vendors)
- lab tests, diagnostic procedures, and clinical findings
- medications, prescriptions, dosages, treatment plans
- invoices, bills, purchase orders, payment receipts, monetary amounts
- banking or transaction details (accounts, invoice numbers, references)
- important dates or appointments
- other critical entities useful for downstream reasoning

Return a JSON array. Each element must be an object with:
- "text": exact substring from the input
- "label": concise type such as PATIENT, CLINICIAN, LAB_TEST, MEASUREMENT, REFERENCE_RANGE, MEDICATION, DOSAGE, DIAGNOSIS, PROCEDURE, PRESCRIPTION, ORGANIZATION, INVOICE, PAYMENT, MONEY, CURRENCY, INVOICE_REFERENCE, BANK_ACCOUNT, DATE, EVENT, OTHER
- optional "attributes": dictionary capturing structured detail (e.g., {"value": "13", "units": "IU/L"}, {"currency": "INR", "amount": "1500"}, {"dosage": "500 mg", "frequency": "daily"})
- optional "start": integer start offset (0-based) in the original text
- optional "end": integer end offset (exclusive)

Output only valid JSON (no prose, no markdown). If no entities exist, output [].

Text:
{text}
JSON:"""

    def __init__(
        self,
        model: str,
        endpoint: str = "http://localhost:11434/api/generate",
        timeout: int = 60,
        prompt_template: str = DEFAULT_PROMPT,
    ) -> None:
        self.model = model
        self.endpoint = endpoint
        self.timeout = timeout
        self.prompt_template = prompt_template

    def extract(self, text: str) -> Iterable[EntityMention]:
        payload = {
            "model": self.model,
            "prompt": self._render_prompt(text),
            "stream": False,
        }
        try:
            response = requests.post(self.endpoint, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:  # pragma: no cover
            raise RuntimeError(f"Entity extraction request failed: {exc}") from exc
        output = self._extract_response_text(response.json())
        for entity in self._parse_entities(output):
            mention = self._to_mention(text, entity)
            if mention:
                yield mention

    def _render_prompt(self, text: str) -> str:
        stripped = text.strip()
        placeholder = "{text}"
        template = self.prompt_template
        if placeholder in template:
            return template.replace(placeholder, stripped)
        return f"{template.rstrip()}\n\nText:\n{stripped}"

    @staticmethod
    def _extract_response_text(payload: dict) -> str:
        if "response" in payload:
            return payload["response"]
        if "output" in payload:
            return payload["output"]
        choices = payload.get("choices") or []
        if choices:
            return choices[0].get("message", {}).get("content", "")
        return ""

    @staticmethod
    def _strip_json_fences(text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = text[3:].lstrip()
            if text.lower().startswith("json"):
                text = text[4:].lstrip()
            if text.endswith("```"):
                text = text[:-3]
        return text.strip()

    def _parse_entities(self, output: str) -> List[dict]:
        cleaned = self._strip_json_fences(output)
        if not cleaned:
            return []
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning("LLM extractor returned non-JSON payload: %s", output)
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            if "entities" in parsed and isinstance(parsed["entities"], list):
                return parsed["entities"]
            return [parsed]
        return []

    def _to_mention(self, text: str, entity: dict) -> Optional[EntityMention]:
        raw = entity.get("text") or entity.get("span")
        if not isinstance(raw, str) or not raw.strip():
            return None
        raw_text = raw.strip()
        label = str(entity.get("label") or entity.get("type") or "ENTITY").strip()
        start = entity.get("start")
        end = entity.get("end")
        if isinstance(start, int) and isinstance(end, int):
            if 0 <= start < end <= len(text):
                actual = text[start:end]
                if raw_text.lower() in actual.lower():
                    return EntityMention(
                        text=raw_text,
                        label=label,
                        start_char=start,
                        end_char=end,
                        confidence=entity.get("confidence"),
                        metadata=entity.get("attributes"),
                    )
        idx = text.lower().find(raw_text.lower())
        if idx == -1:
            return None
        return EntityMention(
            text=raw_text,
            label=label,
            start_char=idx,
            end_char=idx + len(raw_text),
            confidence=entity.get("confidence"),
            metadata=entity.get("attributes"),
        )


class KnowledgeGraphBuilder:
    def __init__(
        self,
        database_path: str,
        extractor,
        extractor_name: str,
    ) -> None:
        self.database_path = database_path
        self.extractor = extractor
        self.extractor_name = extractor_name
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            self._ensure_tables(conn)

    def _ensure_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_entities (
                entity_id TEXT PRIMARY KEY,
                label TEXT,
                properties TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_relations (
                relation_id TEXT PRIMARY KEY,
                source_id TEXT,
                target_id TEXT,
                relation_type TEXT,
                properties TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_mentions (
                mention_id TEXT PRIMARY KEY,
                extractor TEXT,
                content_id TEXT,
                chunk_index INTEGER,
                entity_id TEXT,
                label TEXT,
                text TEXT,
                start_char INTEGER,
                end_char INTEGER,
                confidence REAL,
                metadata TEXT,
                created_at TEXT
            )
            """
        )

    def run(self, limit: Optional[int] = None, content_id: Optional[str] = None) -> int:
        segments = list(self._fetch_segments(limit=limit, content_id=content_id))
        if not segments:
            return 0
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            processed = 0
            for segment in segments:
                if self._process_segment(conn, segment):
                    processed += 1
            return processed

    def process_content(self, content_id: str) -> int:
        return self.run(content_id=content_id)

    def _fetch_segments(
        self, limit: Optional[int], content_id: Optional[str]
    ) -> Iterable[TextSegment]:
        sql = [
            "SELECT t.content_id, t.chunk_index, t.text, t.page,",
            "       cr.content_type, cr.message_id, cr.attachment_id,",
            "       a.filename, cr.metadata, m.subject",
            "FROM attachment_texts t",
            "JOIN content_registry cr ON cr.content_id = t.content_id",
            "LEFT JOIN attachments a",
            "  ON a.message_id = cr.message_id AND a.id = cr.attachment_id",
            "LEFT JOIN messages m ON m.id = cr.message_id",
            "WHERE NOT EXISTS (",
            "  SELECT 1 FROM entity_mentions em",
            "  WHERE em.content_id = t.content_id AND em.chunk_index = t.chunk_index",
            "    AND em.extractor = ?",
            ")",
        ]
        params: List = [self.extractor_name]
        if content_id:
            sql.append("  AND t.content_id = ?")
            params.append(content_id)
        sql.append("ORDER BY cr.created_at ASC, t.chunk_index ASC")
        if limit:
            sql.append("LIMIT ?")
            params.append(limit)
        query = "\n".join(sql)
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            rows = conn.execute(query, tuple(params)).fetchall()
        for row in rows:
            metadata = _loads(row[8])
            filename = row[7] or metadata.get("filename")
            if not filename and metadata.get("path"):
                filename = Path(metadata["path"]).name
            yield TextSegment(
                content_id=row[0],
                chunk_index=row[1],
                text=row[2] or "",
                page=row[3],
                content_type=row[4] or "content",
                message_id=row[5],
                attachment_id=row[6],
                filename=filename,
                subject=row[9],
            )

    def _process_segment(self, conn: sqlite3.Connection, segment: TextSegment) -> bool:
        text = segment.text.strip()
        if not text:
            return False
        mentions = list(self.extractor.extract(text))
        if not mentions:
            return False
        conn.execute(
            "DELETE FROM entity_mentions WHERE content_id = ? AND chunk_index = ? AND extractor = ?",
            (segment.content_id, segment.chunk_index, self.extractor_name),
        )
        content_entity_id = self._ensure_content_entity(conn, segment)
        created_at = datetime.now(timezone.utc).isoformat()
        inserted = False
        for mention in mentions:
            entity_id = self._ensure_entity(conn, mention, created_at)
            mention_id = self._build_mention_id(segment, mention, entity_id)
            metadata = {
                "page": segment.page,
                "filename": segment.filename,
                "subject": segment.subject,
                "message_id": segment.message_id,
                "attachment_id": segment.attachment_id,
                "extractor": self.extractor_name,
            }
            if isinstance(mention.metadata, dict):
                attributes = mention.metadata.get("attributes")
                if isinstance(attributes, dict):
                    metadata["attributes"] = attributes
                else:
                    metadata["source_metadata"] = mention.metadata
            conn.execute(
                """
                INSERT OR REPLACE INTO entity_mentions (
                    mention_id, extractor, content_id, chunk_index, entity_id,
                    label, text, start_char, end_char, confidence, metadata, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mention_id,
                    self.extractor_name,
                    segment.content_id,
                    segment.chunk_index,
                    entity_id,
                    mention.label,
                    mention.text,
                    mention.start_char,
                    mention.end_char,
                    mention.confidence if mention.confidence is not None else 1.0,
                    json.dumps(metadata, sort_keys=True),
                    created_at,
                ),
            )
            relation_id = f"mention:{mention_id}"
            relation_properties = {
                "content_id": segment.content_id,
                "chunk_index": segment.chunk_index,
                "start_char": mention.start_char,
                "end_char": mention.end_char,
                "created_at": created_at,
                "extractor": self.extractor_name,
            }
            conn.execute(
                """
                INSERT OR REPLACE INTO graph_relations (
                    relation_id, source_id, target_id, relation_type, properties
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    relation_id,
                    entity_id,
                    content_entity_id,
                    "MENTIONED_IN",
                    json.dumps(relation_properties, sort_keys=True),
                ),
            )
            inserted = True
        if inserted:
            conn.commit()
        return inserted

    def _ensure_entity(
        self, conn: sqlite3.Connection, mention: EntityMention, timestamp: str
    ) -> str:
        normalized = mention.text.lower().strip() or mention.text.strip()
        base = f"{mention.label}:{normalized}"
        entity_id = f"{mention.label}:{sha1(base.encode('utf-8')).hexdigest()[:16]}"
        row = conn.execute(
            "SELECT properties FROM graph_entities WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if row:
            properties = _loads(row[0])
            aliases = set(properties.get("aliases", []))
            aliases.add(mention.text)
            properties["aliases"] = sorted(aliases)
            properties["updated_at"] = timestamp
        else:
            properties = {
                "canonical": normalized,
                "aliases": [mention.text],
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        conn.execute(
            """
            INSERT OR REPLACE INTO graph_entities (entity_id, label, properties)
            VALUES (?, ?, ?)
            """,
            (entity_id, mention.label, json.dumps(properties, sort_keys=True)),
        )
        return entity_id

    def _ensure_content_entity(
        self, conn: sqlite3.Connection, segment: TextSegment
    ) -> str:
        entity_id = segment.content_id
        row = conn.execute(
            "SELECT 1 FROM graph_entities WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        if row:
            return entity_id
        properties = {
            "content_type": segment.content_type,
            "message_id": segment.message_id,
            "attachment_id": segment.attachment_id,
            "filename": segment.filename,
            "subject": segment.subject,
        }
        conn.execute(
            """
            INSERT INTO graph_entities (entity_id, label, properties)
            VALUES (?, ?, ?)
            """,
            (entity_id, "Content", json.dumps(properties, sort_keys=True)),
        )
        return entity_id

    def _build_mention_id(
        self, segment: TextSegment, mention: EntityMention, entity_id: str
    ) -> str:
        raw = (
            f"{segment.content_id}:{segment.chunk_index}:{mention.start_char}:{mention.end_char}:{entity_id}:{self.extractor_name}"
        )
        return sha1(raw.encode("utf-8")).hexdigest()


def _loads(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return {}
