"""
Graph models for knowledge graph nodes.
"""
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class KnowledgeNode:
    """Represents a content chunk node in the knowledge graph."""

    id: str
    content: str
    source_file: str
    chunk_type: str  # "large" | "small"
    parent_id: Optional[str]
    page: int
    category: str
    tags: List[str]

    def to_dict(self) -> dict:
        """Convert to dict for GUtils.add_node and GraphBuilder."""
        return {
            "id": self.id,
            "type": "CONTENT",
            "content": self.content,
            "source_file": self.source_file,
            "chunk_type": self.chunk_type,
            "parent_id": self.parent_id,
            "page": self.page,
            "category": self.category,
            "tags": self.tags,
        }
