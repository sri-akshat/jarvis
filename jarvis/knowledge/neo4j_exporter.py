"""Utility for pushing the local knowledge graph into Neo4j."""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional

try:  # pragma: no cover - dependency optional in some environments
    from neo4j import GraphDatabase
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "neo4j driver is not installed. Install it via `pip install neo4j`."
    ) from exc

logger = logging.getLogger(__name__)


def _load_json(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        logger.debug("Failed to decode JSON payload: %s", raw)
    return {}


_LABEL_PATTERN = re.compile(r"[^A-Za-z0-9_]")


def _sanitize_label(label: str | None) -> str:
    value = (label or "Entity").strip()
    value = _LABEL_PATTERN.sub("_", value)
    value = value.strip("_") or "Entity"
    if value[0].isdigit():
        value = f"_{value}"
    return value


def _sanitize_rel_type(relation_type: str | None) -> str:
    value = (relation_type or "RELATED_TO").strip()
    value = _LABEL_PATTERN.sub("_", value)
    value = value.strip("_") or "RELATED_TO"
    if value[0].isdigit():
        value = f"_{value}"
    return value


@dataclass
class Neo4jConnectionConfig:
    uri: str
    user: str
    password: str
    database: Optional[str] = None


class Neo4jGraphExporter:
    """Export nodes/edges from the SQLite knowledge graph into Neo4j."""

    def __init__(
        self,
        database_path: str,
        connection: Neo4jConnectionConfig,
        *,
        clear_existing: bool = False,
        driver_builder: Callable[..., Any] | None = None,
    ) -> None:
        self.database_path = database_path
        self.connection = connection
        self.clear_existing = clear_existing
        self._driver_builder = driver_builder or GraphDatabase.driver

    def run(self) -> tuple[int, int]:
        """Push the current graph state into Neo4j.

        Returns:
            Tuple of (nodes_exported, relations_exported).
        """
        entities = list(self._iter_entities())
        relations = list(self._iter_relations())
        logger.info(
            "Preparing to export %s entities and %s relations to Neo4j.",
            len(entities),
            len(relations),
        )

        driver = self._driver_builder(
            self.connection.uri,
            auth=(self.connection.user, self.connection.password),
        )
        try:
            with driver.session(database=self.connection.database) as session:
                if self.clear_existing:
                    session.execute_write(self._truncate_graph)
                for entity in entities:
                    session.execute_write(
                        self._merge_entity,
                        _sanitize_label(entity["label"]),
                        entity["entity_id"],
                        entity["properties"],
                        entity["label"],
                    )
                for relation in relations:
                    session.execute_write(
                        self._merge_relation,
                        relation["source_id"],
                        relation["target_id"],
                        _sanitize_rel_type(relation["relation_type"]),
                        relation["relation_id"],
                        relation["properties"],
                    )
        finally:
            driver.close()
        logger.info(
            "Neo4j export complete: %s entities, %s relations.",
            len(entities),
            len(relations),
        )
        return len(entities), len(relations)

    def _iter_entities(self) -> Iterable[Dict[str, Any]]:
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute(
                "SELECT entity_id, label, properties FROM graph_entities"
            )
            for entity_id, label, properties in cursor.fetchall():
                props = _load_json(properties)
                props.setdefault("entity_id", entity_id)
                yield {
                    "entity_id": entity_id,
                    "label": label or "Entity",
                    "properties": props,
                }

    def _iter_relations(self) -> Iterable[Dict[str, Any]]:
        with sqlite3.connect(self.database_path) as conn:
            cursor = conn.execute(
                "SELECT relation_id, source_id, target_id, relation_type, properties "
                "FROM graph_relations"
            )
            for relation in cursor.fetchall():
                relation_id, source_id, target_id, relation_type, properties = relation
                props = _load_json(properties)
                props.setdefault("relation_id", relation_id)
                yield {
                    "relation_id": relation_id,
                    "source_id": source_id,
                    "target_id": target_id,
                    "relation_type": relation_type or "RELATED_TO",
                    "properties": props,
                }

    @staticmethod
    def _truncate_graph(tx) -> None:
        tx.run("MATCH (n) DETACH DELETE n")

    @staticmethod
    def _merge_entity(
        tx,
        label: str,
        entity_id: str,
        properties: Dict[str, Any],
        original_label: str,
    ) -> None:
        tx.run(
            f"MERGE (n:{label} {{entity_id: $entity_id}}) "
            "SET n += $properties, n.original_label = $original_label",
            entity_id=entity_id,
            properties=properties,
            original_label=original_label,
        )

    @staticmethod
    def _merge_relation(
        tx,
        source_id: str,
        target_id: str,
        relation_type: str,
        relation_id: str,
        properties: Dict[str, Any],
    ) -> None:
        tx.run(
            f"""
            MATCH (src {{entity_id: $source_id}}), (dst {{entity_id: $target_id}})
            MERGE (src)-[r:{relation_type} {{relation_id: $relation_id}}]->(dst)
            SET r += $properties
            """,
            source_id=source_id,
            target_id=target_id,
            relation_id=relation_id,
            properties=properties,
        )


__all__ = ["Neo4jGraphExporter", "Neo4jConnectionConfig"]
