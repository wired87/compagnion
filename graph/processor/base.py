from typing import List, Dict, Any, Optional
from rich.console import Console
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

from qbrain.graph.models import KnowledgeNode

class BaseProcessor:
    def __init__(self):
        self.console = Console()
        self.text_splitter_small = RecursiveCharacterTextSplitter(chunk_size=200, chunk_overlap=50)
        self.text_splitter_large = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0)

    def load_from_path(self, file_path: str) -> List[Document]:
        """Override this to load documents from a file path."""
        raise NotImplementedError

    def load_from_bytes(self, filename: str, content: bytes) -> List[Document]:
        """Override this to load documents from bytes."""
        raise NotImplementedError

    def process_path(self, file_path: str, category: str = "Document") -> List[Dict[str, Any]]:
        self.console.print(f"[blue]ℹ️  Processing path: {file_path} (Category: {category})[/blue]")
        docs = self.load_from_path(file_path)
        if not docs:
            self.console.print(f"[yellow]⚠️  No documents loaded from {file_path}[/yellow]")
            return []
        
        # Default filename extraction
        import os
        filename = os.path.basename(file_path)
        
        docs = self.resolve_edges(docs) # Post-process for structural edges
        
        result = self._structure_docs(docs, filename, category)
        self.console.print(f"[green]✅ Processed {len(result)} knowledge nodes from {file_path}[/green]")
        return result

    def resolve_edges(self, docs: List[Document]) -> List[Document]:
        """
        Post-processing hook to identify and link internal edges based on document structure,
        hierarchy, or content matching (e.g., table connections).
        Should update 'edge_ids' in document metadata.
        """
        return docs

            

    def _structure_docs(self, docs: List[Document], filename: str, category: str) -> List[Dict[str, Any]]:
        """
        Common logic to split documents into large/small chunks and wrap in KnowledgeNodes.
        """
        self.console.print(f"[blue]ℹ️  Structuring {len(docs)} documents for {filename}[/blue]")
        rows = []
        
        # 1. Create Large Chunks (Parents)
        large_chunks = self.text_splitter_large.split_documents(docs)
        self.console.print(f"[blue]ℹ️  Created {len(large_chunks)} large chunks[/blue]")
        
        for i, parent_doc in enumerate(large_chunks):
            parent_id = f"{filename}_p{i}"
            
            # Create Parent Node
            parent_node = KnowledgeNode(
                id=parent_id,
                content=parent_doc.page_content,
                source_file=filename,
                chunk_type="large",
                parent_id=None,
                page=parent_doc.metadata.get("page", 0),
                category=category,
                tags=[filename.split('.')[-1]] if '.' in filename else []
            )
            rows.append(parent_node.to_dict())
            
            # 2. Create Small Chunks (Children) from this Parent
            child_docs = self.text_splitter_small.split_text(parent_doc.page_content)
            
            for j, child_text in enumerate(child_docs):
                child_id = f"{parent_id}_c{j}"
                
                child_node = KnowledgeNode(
                    id=child_id,
                    content=child_text,
                    source_file=filename,
                    chunk_type="small",
                    parent_id=parent_id,
                    page=parent_doc.metadata.get("page", 0),
                    category=category,
                    tags=["child"]
                )
                rows.append(child_node.to_dict())
        
        self.console.print(f"[green]✅ Structured {len(rows)} total nodes (large + small)[/green]")
        return rows
