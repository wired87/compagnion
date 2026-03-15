from typing import List
from langchain_community.document_loaders import UnstructuredImageLoader
from langchain_core.documents import Document

from .base import BaseProcessor

class ImageProcessor(BaseProcessor):
    def load_from_path(self, file_path: str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Loading Image from path: {file_path}[/blue]")
        try:
            docs = UnstructuredImageLoader(file_path).load()
            self.console.print(f"[green]✅ Successfully loaded {len(docs)} documents from {file_path}[/green]")
            return docs
        except ImportError:
            self.console.print("[yellow]⚠️  Image processing requires 'unstructured' and 'opencv-python'. Skipping.[/yellow]")
            return []
        except Exception as e:
            self.console.print(f"[red]❌ Error loading Image {file_path}: {e}[/red]")
            return []

    def process_bytes(self, filename: str, content: bytes, category=None) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Processing Image bytes for: {filename}[/blue]")
        # Unstructured often needs a file on disk or specific handling. 
        # For simple byte processing without a file, it's complex.
        # We'll skip byte processing for images for now or treat as placeholder.
        self.console.print("[yellow]⚠️  In-memory image processing not fully supported yet.[/yellow]")
        return []
