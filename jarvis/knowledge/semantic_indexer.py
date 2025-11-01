"""Semantic extraction pipeline for attachments and messages."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from hashlib import sha1, sha256
from io import BytesIO
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence

import numpy as np

try:
    from pypdf import PdfReader
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "pypdf is required for semantic indexing. Install it via `pip install pypdf`."
    ) from exc

CHUNK_SIZE = 1000


@dataclass
class ContentRecord:
    content_id: str
    message_id: Optional[str]
    attachment_id: Optional[str]
    filename: str
    mime_type: str
    data: Optional[bytes] = None
    path: Optional[str] = None
    text: Optional[str] = None
    source: str = "attachment"
    metadata: dict = field(default_factory=dict)


class SimpleEmbeddingGenerator:
    def __init__(self, dimensions: int = 128, model_name: str = "hashed-bow-128") -> None:
        self.dimensions = dimensions
        self.model_name = model_name

    def embed(self, text: str) -> np.ndarray:
        vec = np.zeros(self.dimensions, dtype=np.float32)
        for token in text.lower().split():
            h = int(sha1(token.encode("utf-8")).hexdigest(), 16)
            vec[h % self.dimensions] += 1.0
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec /= norm
        return vec


class SemanticIndexer:
    def __init__(
        self,
        database_path: str,
        embedding_generator: Optional[SimpleEmbeddingGenerator] = None,
    ) -> None:
        self.database_path = database_path
        self.embedding_generator = embedding_generator or SimpleEmbeddingGenerator()

    def run(self, limit: Optional[int] = None) -> int:
        records = self._collect_pending_records(limit)
        if not records:
            return 0
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            for record in records:
                texts = self._extract_text(record)
                if not texts:
                    continue
                self._clear_content(conn, record.content_id)
                self._store_texts(conn, record.content_id, texts)
                self._store_embeddings(conn, record.content_id, texts, record)
        return len(records)

    def process_content_id(self, content_id: str) -> bool:
        record = self._fetch_record_by_id(content_id)
        if not record:
            return False
        texts = self._extract_text(record)
        if not texts:
            return False
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            self._clear_content(conn, content_id)
            self._store_texts(conn, content_id, texts)
            self._store_embeddings(conn, content_id, texts, record)
        return True

    def _collect_pending_records(self, limit: Optional[int]) -> List[ContentRecord]:
        records: List[ContentRecord] = []
        remaining = limit

        def extend(batch: List[ContentRecord]) -> None:
            nonlocal remaining
            if not batch:
                return
            records.extend(batch)
            if remaining is not None:
                remaining = max(remaining - len(batch), 0)

        extend(self._fetch_pending_messages(remaining))
        if remaining == 0:
            return records
        extend(self._fetch_pending_attachments(remaining))
        if remaining == 0:
            return records
        extend(self._fetch_pending_local_files(remaining))
        return records

    def _fetch_pending_messages(self, limit: Optional[int]) -> List[ContentRecord]:
        sql = """
            SELECT cr.content_id, m.id, m.body, COALESCE(cr.mime_type, 'text/plain'), cr.metadata
            FROM content_registry cr
            JOIN messages m ON m.id = cr.message_id
            LEFT JOIN attachment_texts t ON t.content_id = cr.content_id
            WHERE cr.content_type = 'message' AND t.content_id IS NULL
            ORDER BY cr.created_at ASC
        """
        params: Sequence[object]
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        else:
            params = ()
        items: List[ContentRecord] = []
        with sqlite3.connect(self.database_path) as conn:
            rows = conn.execute(sql, params).fetchall()
        for row in rows:
            metadata = _loads(row[4])
            text = row[2] or ""
            filename = metadata.get("subject") or f"message-{row[1]}"
            items.append(
                ContentRecord(
                    content_id=row[0],
                    message_id=row[1],
                    attachment_id=None,
                    filename=filename,
                    mime_type=row[3] or "text/plain",
                    text=text,
                    source="message",
                    metadata=metadata,
                )
            )
        return items

    def _fetch_pending_attachments(self, limit: Optional[int]) -> List[ContentRecord]:
        sql = """
            SELECT cr.content_id, cr.message_id, cr.attachment_id, a.filename,
                   COALESCE(a.mime_type, cr.mime_type), a.data, cr.metadata
            FROM content_registry cr
            JOIN attachments a
              ON a.message_id = cr.message_id AND a.id = cr.attachment_id
            LEFT JOIN attachment_texts t ON t.content_id = cr.content_id
            WHERE cr.content_type = 'attachment' AND t.content_id IS NULL
            ORDER BY cr.created_at ASC
        """
        params: Sequence[object]
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        else:
            params = ()
        items: List[ContentRecord] = []
        with sqlite3.connect(self.database_path) as conn:
            rows = conn.execute(sql, params).fetchall()
        for row in rows:
            metadata = _loads(row[6])
            items.append(
                ContentRecord(
                    content_id=row[0],
                    message_id=row[1],
                    attachment_id=row[2],
                    filename=row[3] or "",
                    mime_type=row[4] or "application/octet-stream",
                    data=row[5],
                    source="attachment",
                    metadata=metadata,
                )
            )
        return items

    def _fetch_pending_local_files(self, limit: Optional[int]) -> List[ContentRecord]:
        sql = """
            SELECT cr.content_id, lf.path, COALESCE(cr.mime_type, 'application/octet-stream'), cr.metadata
            FROM content_registry cr
            JOIN local_files lf ON lf.content_id = cr.content_id
            LEFT JOIN attachment_texts t ON t.content_id = cr.content_id
            WHERE cr.content_type = 'local_file' AND t.content_id IS NULL
            ORDER BY cr.created_at ASC
        """
        params: Sequence[object]
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        else:
            params = ()
        items: List[ContentRecord] = []
        with sqlite3.connect(self.database_path) as conn:
            rows = conn.execute(sql, params).fetchall()
        for row in rows:
            metadata = _loads(row[3])
            filename = metadata.get("filename") or Path(row[1]).name
            items.append(
                ContentRecord(
                    content_id=row[0],
                    message_id=None,
                    attachment_id=None,
                    filename=filename,
                    mime_type=row[2] or "application/octet-stream",
                    path=row[1],
                    source="local_file",
                    metadata=metadata,
                )
            )
        return items

    def _fetch_record_by_id(self, content_id: str) -> Optional[ContentRecord]:
        with sqlite3.connect(self.database_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            row = conn.execute(
                """
                SELECT cr.content_id, cr.message_id, cr.attachment_id, a.filename,
                       COALESCE(a.mime_type, cr.mime_type), a.data, cr.metadata
                FROM content_registry cr
                JOIN attachments a
                  ON a.message_id = cr.message_id AND a.id = cr.attachment_id
                WHERE cr.content_id = ?
                """,
                (content_id,),
            ).fetchone()
            if row:
                metadata = _loads(row[6])
                return ContentRecord(
                    content_id=row[0],
                    message_id=row[1],
                    attachment_id=row[2],
                    filename=row[3] or "",
                    mime_type=row[4] or "application/octet-stream",
                    data=row[5],
                    source="attachment",
                    metadata=metadata,
                )
            row = conn.execute(
                """
                SELECT cr.content_id, m.id, m.body, COALESCE(cr.mime_type, 'text/plain'), cr.metadata
                FROM content_registry cr
                JOIN messages m ON m.id = cr.message_id
                WHERE cr.content_id = ?
                """,
                (content_id,),
            ).fetchone()
            if row:
                metadata = _loads(row[4])
                filename = metadata.get("subject") or f"message-{row[1]}"
                return ContentRecord(
                    content_id=row[0],
                    message_id=row[1],
                    attachment_id=None,
                    filename=filename,
                    mime_type=row[3] or "text/plain",
                    text=row[2] or "",
                    source="message",
                    metadata=metadata,
                )
            row = conn.execute(
                """
                SELECT cr.content_id, lf.path, COALESCE(cr.mime_type, 'application/octet-stream'), cr.metadata
                FROM content_registry cr
                JOIN local_files lf ON lf.content_id = cr.content_id
                WHERE cr.content_id = ?
                """,
                (content_id,),
            ).fetchone()
            if row:
                metadata = _loads(row[3])
                filename = metadata.get("filename") or Path(row[1]).name
                return ContentRecord(
                    content_id=row[0],
                    message_id=None,
                    attachment_id=None,
                    filename=filename,
                    mime_type=row[2] or "application/octet-stream",
                    path=row[1],
                    source="local_file",
                    metadata=metadata,
                )
        return None

    def _extract_text(self, record: ContentRecord) -> List[str]:
        if record.source == "message":
            return [record.text or ""] if record.text is not None else []
        if record.source == "local_file":
            return self._extract_local_file(record)
        data = record.data or b""
        if record.mime_type == "application/pdf" or record.filename.lower().endswith(".pdf"):
            return self._extract_pdf_text(data)
        if record.mime_type.startswith("text/"):
            try:
                return [data.decode("utf-8")]
            except UnicodeDecodeError:
                return [data.decode("latin-1")]
        return []

    def _extract_local_file(self, record: ContentRecord) -> List[str]:
        if not record.path:
            return []
        path = Path(record.path)
        if not path.exists():
            return []
        if record.mime_type == "application/pdf" or path.suffix.lower() == ".pdf":
            try:
                return self._extract_pdf_text(path.read_bytes())
            except OSError:  # pragma: no cover
                return []
        if record.mime_type.startswith("text/") or path.suffix.lower() in {".txt", ".md"}:
            for encoding in ("utf-8", "latin-1"):
                try:
                    return [path.read_text(encoding=encoding)]
                except UnicodeDecodeError:
                    continue
                except OSError:  # pragma: no cover
                    return []
        return []

    @staticmethod
    def _extract_pdf_text(binary: bytes) -> List[str]:
        reader = PdfReader(BytesIO(binary))
        texts: List[str] = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            texts.append(page_text)
        return texts

    @staticmethod
    def _clear_content(conn: sqlite3.Connection, content_id: str) -> None:
        conn.execute("DELETE FROM attachment_texts WHERE content_id = ?", (content_id,))
        conn.execute("DELETE FROM embeddings WHERE content_id = ?", (content_id,))

    def _store_texts(
        self,
        conn: sqlite3.Connection,
        content_id: str,
        page_texts: Sequence[str],
    ) -> None:
        cursor = conn.cursor()
        chunk_index = 0
        for page_number, page_text in enumerate(page_texts):
            for chunk in self._chunk_text(page_text):
                token_count = len(chunk.split())
                if token_count == 0:
                    continue
                checksum = sha256(chunk.encode("utf-8")).hexdigest()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO attachment_texts (
                        content_id, page, chunk_index, text, token_count, sha256
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (content_id, page_number, chunk_index, chunk, token_count, checksum),
                )
                chunk_index += 1
        conn.commit()

    def _store_embeddings(
        self,
        conn: sqlite3.Connection,
        content_id: str,
        page_texts: Sequence[str],
        record: ContentRecord,
    ) -> None:
        cursor = conn.cursor()
        chunk_index = 0
        created_at = datetime.now(timezone.utc).isoformat()
        for page_number, page_text in enumerate(page_texts):
            for chunk in self._chunk_text(page_text):
                if not chunk.strip():
                    continue
                vector = self.embedding_generator.embed(chunk)
                metadata = {
                    "page": page_number,
                    "filename": record.filename,
                    "mime_type": record.mime_type,
                    "source": record.source,
                }
                if record.path:
                    metadata["path"] = record.path
                if record.message_id:
                    metadata["message_id"] = record.message_id
                if record.attachment_id:
                    metadata["attachment_id"] = record.attachment_id
                if record.metadata:
                    metadata.setdefault("content_metadata", record.metadata)
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO embeddings (
                        embedding_id, content_id, chunk_index, model,
                        dimensions, vector, created_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"{content_id}:{chunk_index}:{self.embedding_generator.model_name}",
                        content_id,
                        chunk_index,
                        self.embedding_generator.model_name,
                        self.embedding_generator.dimensions,
                        vector.tobytes(),
                        created_at,
                        json.dumps(metadata, sort_keys=True),
                    ),
                )
                chunk_index += 1
        conn.commit()

    @staticmethod
    def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE) -> Iterable[str]:
        normalized = " ".join(text.split())
        for start in range(0, len(normalized), chunk_size):
            yield normalized[start : start + chunk_size]


def _loads(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        loaded = json.loads(raw)
        if isinstance(loaded, dict):
            return loaded
    except json.JSONDecodeError:
        pass
    return {}
