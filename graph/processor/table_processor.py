from typing import List
from langchain_community.document_loaders import CSVLoader
from langchain_core.documents import Document
import io

from .base import BaseProcessor

class TableProcessor(BaseProcessor):
    def load_from_path(self, file_path: str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Loading CSV from path: {file_path}[/blue]")
        try:
            docs = CSVLoader(file_path).load()
            self.console.print(f"[green]✅ Successfully loaded {len(docs)} documents from {file_path}[/green]")
            return docs
        except Exception as e:
            self.console.print(f"[red]❌ Error loading CSV {file_path}: {e}[/red]")
            return []

    def process_bytes(self, filename: str, content: bytes, category:str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Processing CSV bytes for: {filename}[/blue]")
        try:
            import pandas as pd
            df = pd.read_csv(io.BytesIO(content))
            text_content = df.to_string() # Simple representation
            docs = [Document(page_content=text_content, metadata={"source": filename, "category": category})]
            self.console.print(f"[green]✅ Successfully processed {len(docs)} documents from bytes for {filename}[/green]")
            return docs
        except ImportError:
            # Fallback to plain text if pandas not available
            self.console.print("[yellow]⚠️  Pandas not available, falling back to plain text[/yellow]")
            docs = [Document(page_content=content.decode('utf-8', errors='ignore'), metadata={"source": filename, "category": category})]
            self.console.print(f"[green]✅ Successfully processed {len(docs)} documents from bytes for {filename} (fallback)[/green]")
            return docs
        except Exception as e:
            self.console.print(f"[red]❌ Error loading CSV bytes for {filename}: {e}[/red]")
            return []
