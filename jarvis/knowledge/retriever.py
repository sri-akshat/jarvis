"""Simple semantic search over the embeddings table."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List

import numpy as np

from jarvis.knowledge.semantic_indexer import SimpleEmbeddingGenerator


@dataclass
class SearchResult:
    score: float
    content_id: str
    text: str
    page: int | None
    attachment_filename: str | None
    subject: str | None


class SemanticRetriever:
    def __init__(self, database_path: str, embedding_generator: SimpleEmbeddingGenerator | None = None) -> None:
        self.database_path = database_path
        self.embedding_generator = embedding_generator or SimpleEmbeddingGenerator()

    def search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        query_vec = self.embedding_generator.embed(query)
        with sqlite3.connect(self.database_path) as conn:
            rows = conn.execute(
                """
                SELECT e.content_id, e.chunk_index, e.vector, e.metadata, t.text
                FROM embeddings e
                JOIN attachment_texts t
                  ON t.content_id = e.content_id AND t.chunk_index = e.chunk_index
                """
            ).fetchall()
        if not rows:
            return []

        vectors = []
        metadata = []
        texts = []
        for content_id, chunk_index, vector_blob, meta_json, text in rows:
            vector = np.frombuffer(vector_blob, dtype=np.float32)
            vectors.append(vector)
            metadata.append((content_id, chunk_index, meta_json))
            texts.append(text)
        matrix = np.vstack(vectors)
        scores = matrix @ query_vec
        top_indices = np.argsort(scores)[::-1][:top_k]
        results: List[SearchResult] = []
        for idx in top_indices:
            score = float(scores[idx])
            content_id, chunk_index, meta_json = metadata[idx]
            text = texts[idx]
            meta = self._parse_metadata(meta_json)
            results.append(
                SearchResult(
                    score=score,
                    content_id=f"{content_id}:{chunk_index}",
                    text=text,
                    page=meta.get("page"),
                    attachment_filename=meta.get("filename"),
                    subject=meta.get("subject"),
                )
            )
        return results

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
