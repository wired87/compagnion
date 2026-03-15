from typing import List
from langchain_community.document_loaders import TextLoader
from langchain_core.documents import Document

from .base import BaseProcessor

class TextProcessor(BaseProcessor):
    def load_from_path(self, file_path: str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Loading Text from path: {file_path}[/blue]")
        try:
            docs = TextLoader(file_path, autodetect_encoding=True).load()
            self.console.print(f"[green]✅ Successfully loaded {len(docs)} documents from {file_path}[/green]")
            return docs
        except Exception as e:
            self.console.print(f"[red]❌ Error loading Text {file_path}: {e}[/red]")
            return []

    def process_bytes(self, filename: str, content: bytes, category:str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Processing Text bytes for: {filename}[/blue]")
        try:
            text = content.decode("utf-8", errors="ignore")
            docs = [Document(page_content=text, metadata={"source": filename,"category":category})]
            self.console.print(f"[green]✅ Successfully processed {len(docs)} documents from bytes for {filename}[/green]")
            return docs
        except Exception as e:
            self.console.print(f"[red]❌ Error loading Text bytes for {filename}: {e}[/red]")
            return []
