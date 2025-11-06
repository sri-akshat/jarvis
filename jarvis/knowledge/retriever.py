"""Conversation-aware semantic retrieval utilities."""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np

from jarvis.knowledge.semantic_indexer import SimpleEmbeddingGenerator

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "last",
    "latest",
    "conversation",
    "email",
    "message",
    "talk",
    "about",
    "tell",
    "show",
    "give",
    "what",
    "which",
    "who",
    "whom",
    "where",
    "when",
    "how",
    "details",
    "status",
    "update",
    "recent",
}


@dataclass
class SearchResult:
    score: float
    raw_score: float
    content_id: str
    chunk_index: int
    citation_id: str
    text: str
    page: int | None
    attachment_filename: str | None
    subject: str | None
    message_id: str | None
    attachment_id: str | None
    source: str | None
    metadata: dict
    keyword_hits: int
    strong_hits: int


class SemanticRetriever:
    def __init__(self, database_path: str, embedding_generator: SimpleEmbeddingGenerator | None = None) -> None:
        self.database_path = database_path
        self.embedding_generator = embedding_generator or SimpleEmbeddingGenerator()

    def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        query_vec = self.embedding_generator.embed(query)
        tokens = self._extract_query_tokens(query)
        strong_tokens = [tok for tok in tokens if tok not in STOPWORDS]

        with sqlite3.connect(self.database_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT e.content_id, e.chunk_index, e.vector, e.metadata, t.text
                FROM embeddings e
                JOIN attachment_texts t
                  ON t.content_id = e.content_id AND t.chunk_index = e.chunk_index
                """
            ).fetchall()
            fts_scores, candidate_ids = self._run_fts(conn, strong_tokens or tokens, top_k)

        if not rows:
            return []

        vectors = []
        metadata_rows = []
        texts = []
        for row in rows:
            vector = np.frombuffer(row[2], dtype=np.float32)
            vectors.append(vector)
            metadata_rows.append((row[0], row[1], row[3]))
            texts.append(row[4])
        matrix = np.vstack(vectors)
        cosine_scores = matrix @ query_vec

        results: List[SearchResult] = []
        for idx, (content_id, chunk_index, meta_json) in enumerate(metadata_rows):
            raw_score = float(cosine_scores[idx])
            text = texts[idx]
            meta = self._parse_metadata(meta_json)
            message_id = self._extract_message_id(meta)
            participants = self._collect_participants(meta)
            result = SearchResult(
                score=raw_score,
                raw_score=raw_score,
                content_id=content_id,
                chunk_index=int(chunk_index),
                citation_id=f"{content_id}:{chunk_index}",
                text=text,
                page=meta.get("page"),
                attachment_filename=meta.get("filename"),
                subject=meta.get("subject"),
                message_id=message_id,
                attachment_id=meta.get("attachment_id"),
                source=meta.get("source"),
                metadata=meta,
                keyword_hits=0,
                strong_hits=0,
            )

            keyword_hits, strong_hits = self._keyword_hits(tokens, strong_tokens, result)
            result.keyword_hits = keyword_hits
            result.strong_hits = strong_hits

            result.score = result.raw_score
            if fts_scores:
                boost = fts_scores.get(message_id)
                if boost:
                    result.score += 0.25 * boost
                elif candidate_ids:
                    result.score -= 0.6

            participant_match = False
            if strong_tokens:
                participant_match = any(token in participants for token in strong_tokens)
                if participant_match:
                    result.score += 0.2
                else:
                    result.score -= 0.8

            if strong_tokens:
                if result.strong_hits > 0:
                    result.score += 0.15 * result.strong_hits + 0.05 * result.keyword_hits
                else:
                    result.score -= 0.5
            elif tokens:
                if result.keyword_hits > 0:
                    result.score += 0.1 * result.keyword_hits
                else:
                    result.score -= 0.5

            results.append(result)

        results.sort(key=lambda r: (r.score, r.raw_score), reverse=True)
        return results[:top_k]

    def _run_fts(
        self,
        conn: sqlite3.Connection,
        tokens: List[str],
        top_k: int,
    ) -> Tuple[Dict[str, float], set[str]]:
        if not tokens:
            return {}, set()
        fts_query = self._build_fts_query(tokens)
        if not fts_query:
            return {}, set()
        scores: Dict[str, float] = {}
        candidates: set[str] = set()
        try:
            rows = conn.execute(
                """
                SELECT message_id, bm25(message_search) AS rank
                FROM message_search
                WHERE message_search MATCH ?
                ORDER BY rank ASC
                LIMIT ?
                """,
                (fts_query, max(top_k * 4, 20)),
            ).fetchall()
            for message_id, rank in rows:
                if not message_id:
                    continue
                boost = 1.0 / (float(rank) + 1e-3)
                prev = scores.get(message_id)
                if prev is None or boost > prev:
                    scores[message_id] = boost
                candidates.add(message_id)
        except sqlite3.OperationalError:
            pass
        return scores, candidates

    @staticmethod
    def _parse_metadata(raw: str | bytes | None) -> dict:
        if not raw:
            return {}
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            meta = json.loads(raw)
            if isinstance(meta, dict):
                return meta
        except json.JSONDecodeError:
            pass
        return {}

    @staticmethod
    def _extract_message_id(meta: Dict[str, Any]) -> str | None:
        message_id = meta.get("message_id")
        if message_id:
            return message_id
        content_meta = meta.get("content_metadata")
        if isinstance(content_meta, dict):
            return content_meta.get("message_id")
        return None

    @staticmethod
    def _collect_participants(meta: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in ("sender", "recipients"):
            value = meta.get(key)
            if isinstance(value, str):
                parts.append(value.lower())
            elif isinstance(value, (list, tuple, set)):
                parts.extend(item.lower() for item in value if isinstance(item, str))
        content_meta = meta.get("content_metadata")
        if isinstance(content_meta, dict):
            for key in ("sender", "recipients"):
                value = content_meta.get(key)
                if isinstance(value, str):
                    parts.append(value.lower())
                elif isinstance(value, (list, tuple, set)):
                    parts.extend(item.lower() for item in value if isinstance(item, str))
        return " ".join(parts)

    @staticmethod
    def _extract_query_tokens(query: str) -> List[str]:
        raw_tokens = re.findall(r"[\w@\.]+", query.lower())
        tokens: List[str] = []
        for token in raw_tokens:
            token = token.strip("._")
            if len(token) <= 2:
                continue
            tokens.append(token)
        return tokens

    @staticmethod
    def _build_fts_query(tokens: List[str]) -> str:
        clauses: List[str] = []
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            clauses.append(f"{token}*")
        return " AND ".join(clauses)

    def _keyword_hits(
        self,
        tokens: List[str],
        strong_tokens: List[str],
        result: SearchResult,
    ) -> Tuple[int, int]:
        if not tokens:
            return 0, 0
        haystack_parts: List[str] = []
        if result.text:
            haystack_parts.append(result.text.lower())
        if result.subject:
            haystack_parts.append(result.subject.lower())
        metadata_strings = self._flatten_metadata(result.metadata)
        if metadata_strings:
            haystack_parts.extend(metadata_strings)
        haystack = " \n".join(haystack_parts)
        hits = sum(1 for token in tokens if token and token in haystack)
        strong_hits = sum(1 for token in strong_tokens if token and token in haystack)
        return hits, strong_hits

    def _flatten_metadata(self, metadata: Dict[str, Any] | None) -> List[str]:
        strings: List[str] = []
        if not metadata:
            return strings

        def _walk(value: Any) -> None:
            if value is None:
                return
            if isinstance(value, str):
                if value:
                    strings.append(value.lower())
            elif isinstance(value, (list, tuple, set)):
                for item in value:
                    _walk(item)
            elif isinstance(value, dict):
                for item in value.values():
                    _walk(item)
            else:
                strings.append(str(value).lower())

        _walk(metadata)
        return strings
