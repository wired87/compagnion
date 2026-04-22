from typing import List, Dict, Any, TYPE_CHECKING

from langchain_core.documents import Document
from rich.console import Console
import os

from .pdf_processor import PdfProcessor
from .table_processor import TableProcessor
from .text_processor import TextProcessor
from .image_processor import ImageProcessor
from .graph_builder import build_graph

if TYPE_CHECKING:
    from qbrain.graph.local_graph_utils import GUtils


class FileProcessorFacade:
    def __init__(self):
        self.console = Console()
        self.pdf_processor = PdfProcessor()
        self.table_processor = TableProcessor()
        self.image_processor = ImageProcessor()
        self.text_processor = TextProcessor()
    def process_file(self, file_path: str) -> List[Dict[str, Any]]:
        self.console.print(f"[dim]📄 Processing {os.path.basename(file_path)}...[/dim]")
        
        processor = self._get_processor(file_path)
        category = self._get_category(file_path)
        
        try:
            rows = processor.process_path(file_path, category=category)
            if rows:
                self.console.print(f"[green]✓ Generated {len(rows)} structured chunks from {os.path.basename(file_path)}[/green]")
            return rows
        except Exception as e:
            self.console.print(f"[red]❌ Processing failed for {file_path}: {e}[/red]")
            return []

    def process_bytes(self, filename: str, content: bytes) -> List[Document]:
        self.console.print(f"[dim]📄 Processing in-memory file: {filename}...[/dim]")
        
        processor = self._get_processor(filename)
        #category = self._get_category(filename)
        
        try:
            rows = processor.process_bytes(filename, content)
            if rows:
                self.console.print(f"[green]✓ Generated {len(rows)} structured chunks from {filename}[/green]")
            return rows #self.pdf_processor._structure_docs(rows, filename, category)
        except Exception as e:
            self.console.print(f"[red]❌ Processing failed for {filename}: {e}[/red]")
            return []

    def process_to_graph(
        self,
        file_path: str,
        g: "GUtils",
        add_file_nodes: bool = False,
    ) -> int:
        """
        Process file and add CONTENT nodes + edges to GUtils.

        Returns:
            Number of CONTENT nodes added.
        """
        rows = self.process_file(file_path)
        return build_graph(rows, g, add_file_nodes=add_file_nodes)

    def _get_processor(self, filename: str):
        if filename.lower().endswith(".pdf"):
            self.console.print(f"[blue]ℹ️  Selected PdfProcessor for {filename}[/blue]")
            return self.pdf_processor
        elif filename.lower().endswith(".csv"):
            self.console.print(f"[blue]ℹ️  Selected TableProcessor for {filename}[/blue]")
            return self.table_processor
        elif filename.lower().endswith((".jpg", ".png", ".jpeg")):
            self.console.print(f"[blue]ℹ️  Selected ImageProcessor for {filename}[/blue]")
            return self.image_processor
        else:
            self.console.print(f"[blue]ℹ️  Selected TextProcessor for {filename}[/blue]")
            return self.text_processor

    def _get_category(self, filename: str) -> str:
        if filename.endswith(".csv"): return "Data"
        if filename.endswith((".py", ".json", ".sql")): return "Code"
        return "Document"
