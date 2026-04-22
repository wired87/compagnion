
import io
import uuid
import json
import logging
import pandas as pd
from typing import List, Optional

import pymupdf
from bs4 import BeautifulSoup, Tag
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pdfminer.high_level import extract_text_to_fp
from pdfminer.layout import LAParams
from langchain_core.documents import Document

from .base import BaseProcessor

logger = logging.getLogger(__name__)

class PdfProcessor(BaseProcessor):

    def __init__(self, g):
        super().__init__()
        self.g=g


    def load_from_path(self, file_path: str) -> List[Document]:
        # This keeps compatibility with BaseProcessor.process_path
        # But for the full DataFrame requirement, use extract_to_dataframe
        self.console.print(f"[blue]ℹ️  Loading PDF from path: {file_path}[/blue]")
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            docs = self._process_pdf_html_to_docs(content, file_path)
            self.console.print(f"[green]✅ Successfully loaded {len(docs)} documents from {file_path}[/green]")
            return docs
        except Exception as e:
            self.console.print(f"[red]❌ Error loading PDF {file_path}: {e}[/red]")
            return []

    def process_bytes(self, filename: str, content: bytes, category:str) -> List[Document]:
        self.console.print(f"[blue]ℹ️  Processing PDF bytes for: {filename}[/blue]")
        docs = self._process_pdf_html_to_docs(content, filename, category)
        self.console.print(f"[green]✅ Successfully processed {len(docs)} documents from bytes for {filename}[/green]")
        return docs

    def extract_to_dataframe(self, file_path: str, category=None) -> pd.DataFrame:
        """
        Extracts content from PDF and returns a pandas DataFrame with SOA structure.
        """
        self.console.print(f"[blue]ℹ️  Extracting PDF to DataFrame: {file_path}[/blue]")
        try:
            with open(file_path, "rb") as f:
                content = f.read()
            
            # 1. Get Documents (Nodes)
            docs = self._process_pdf_html_to_docs(content, file_path)
            
            # 2. Convert to SOA Struct / DataFrame List
            rows = []
            for d in docs:
                row = {
                    "id": d.metadata.get("id", str(uuid.uuid4())),
                    "content": d.page_content,
                    "file_name": file_path.split("/")[-1].split("\\")[-1], # Simple basename
                    "field_type": d.metadata.get("html_tag", "unknown"), # gfiel_type
                    "content_type": "text" if not d.metadata.get("is_table_row") else "table_row",
                    "part_type": "chunk", # or logic to determine if it's a whole or part
                    "category": category, # or logic to determine if it's a whole or part
                    "parent_id": d.metadata.get("parent_ref"),
                    "page_number": d.metadata.get("page_number", 1),
                    "metadata": json.dumps(d.metadata)
                }
                rows.append(row)
            
            df = pd.DataFrame(rows)
            self.console.print(f"[green]✅ Successfully extracted {len(df)} rows to DataFrame from {file_path}[/green]")
            return df
            
        except Exception as e:
            self.console.print(f"[red]❌ Error extracting PDF to DataFrame {file_path}: {e}[/red]")
            return pd.DataFrame()

    def _process_pdf_html_to_docs(self, content: bytes, filename: str, category=None) -> List[Document]:
        """
        Extracts HTML from PDF, cleans tags, and chunks content using LangChain.
        """
        try:
            output_string = io.BytesIO()
            with io.BytesIO(content) as input_stream:
                extract_text_to_fp(input_stream, output_string, output_type='html', laparams=LAParams())
            html_content = output_string.getvalue().decode("utf-8")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove scripts and styles
            for script_or_style in soup(["script", "style"]):
                script_or_style.decompose()

            docs = []

            # Define block-level tags that typically define structure
            BLOCK_TAGS = {'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', 'blockquote', 'pre', 'table', 'div', 'section', 'article'}

            def has_block_children(element: Tag) -> bool:
                for child in element.children:
                    if isinstance(child, Tag) and child.name in BLOCK_TAGS:
                        return True
                return False

            def traverse(tag: Tag, parent_id: Optional[str] = None):
                if not isinstance(tag, Tag): return
                
                # Special handling for table elements
                if tag.name == 'table':
                    table_docs = self._process_table_element(tag, filename, parent_id)
                    docs.extend(table_docs)
                    return  # Don't traverse children of table

                # Check if this tag is a "Leaf Block"
                is_content_unit = (tag.name in {'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'}) or \
                                  (tag.name in BLOCK_TAGS and not has_block_children(tag))

                if is_content_unit:
                    # Get clean text content
                    text_content = tag.get_text(" ", strip=True)
                    
                    if text_content and len(text_content) > 5:
                        node_id = str(uuid.uuid4())
                        
                        # Split logic using inherited text_splitter_small (LangChain)
                        if len(text_content) > 200:
                            splits = self.text_splitter_small.split_text(text_content)
                        else:
                            splits = [text_content]
                            
                        for i, split in enumerate(splits):
                            docs.append(Document(
                                page_content=split,
                                metadata={
                                    "id": f"{node_id}_{i}",
                                    "file_name": filename,
                                    "html_tag": tag.name,
                                    "parent_ref": parent_id,
                                    "page_number": 1
                                }
                            ))
                    return # Stop recursion

                # Recurse for structural tags
                for child in tag.children:
                    if isinstance(child, Tag):
                        # Pass the current parent_id down (skip structural nodes as parents since they aren't saved)
                        traverse(child, parent_id)

            root = soup.body if soup.body else soup
            traverse(root)
            return docs

        except Exception as e:
            print(f"PDF HTML Extraction Error: {e}")
            self.console.print(f"[red]Error parsing PDF HTML: {e}[/red]")
            return []

    def extract_pdf_content(self, pdf_path, output_folder="extracted_data"):
        doc = pymupdf.open("a.pdf")  # open a document
        for page in doc:  # iterate the document pages
            text = page.get_text().encode("utf8")  # get plain text (is in UTF-8)

            # 2. Splitter konfigurieren
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=200,
                chunk_overlap=60,
                length_function=len,
                separators=["\n\n", "\n", " ", ""]
            )

            # 3. Splitten
            chunks = text_splitter.split_text(text)

            for chunk in chunks:
                self.g.add_node()



    def process_image(self, doc):
        for page_index in range(len(doc)):  # iterate over pdf pages
            page = doc[page_index]  # get the page
            image_list = page.get_images()

            # print the number of images found on the page
            if image_list:
                print(f"Found {len(image_list)} images on page {page_index}")
            else:
                print("No images found on page", page_index)

            for image_index, img in enumerate(image_list, start=1):  # enumerate the image list
                xref = img[0]  # get the XREF of the image
                pix = pymupdf.Pixmap(doc, xref)  # create a Pixmap

                if pix.n - pix.alpha > 3:  # CMYK: convert to RGB first
                    pix = pymupdf.Pixmap(pymupdf.csRGB, pix)

                pix.save(f"page_{page_index}-image_{image_index}.png")  # save the image as png
                pix = None


    def process_vector_graphics(self, doc):
        for page in doc:
            paths = page.get_drawings()




    def _process_table_element(self, table_tag: Tag, filename: str, parent_id: Optional[str]) -> List[Document]:
        docs = []
        table_id = str(uuid.uuid4())
        
        try:
            headers = []
            thead = table_tag.find('thead')
            if thead:
                header_row = thead.find('tr')
                if header_row:
                    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
            
            if not headers:
                first_row = table_tag.find('tr')
                if first_row:
                    ths = first_row.find_all('th')
                    if ths:
                        headers = [th.get_text(strip=True) for th in ths]
                    else:
                        first_cells = first_row.find_all(['td', 'th'])
                        headers = [f"col_{i+1}" for i in range(len(first_cells))]
            
            tbody = table_tag.find('tbody')
            rows = tbody.find_all('tr') if tbody else table_tag.find_all('tr')
            
            skip_first = False
            if rows and not thead:
                first_row_ths = rows[0].find_all('th')
                if first_row_ths:
                    skip_first = True
            
            row_start_idx = 1 if skip_first else 0
            
            for row_idx, tr in enumerate(rows[row_start_idx:], start=1):
                cells = tr.find_all(['td', 'th'])
                cell_contents = [cell.get_text(strip=True) for cell in cells]
                
                if not any(cell_contents):
                    continue
                
                columns_dict = {}
                for i, content in enumerate(cell_contents):
                    col_name = headers[i] if i < len(headers) else f"col_{i+1}"
                    columns_dict[col_name] = content
                
                # Concise content for embedding
                row_content = " | ".join([f"{k}: {v}" for k, v in columns_dict.items() if v])
                
                doc = Document(
                    page_content=row_content,
                    metadata={
                        "id": f"{table_id}_row_{row_idx}",
                        "file_name": filename,
                        "html_tag": "tr",
                        "parent_ref": parent_id,
                        "is_table_row": True,
                        "table_id": table_id,
                        "row_number": row_idx,
                        "columns": columns_dict,
                        "column_headers": headers
                    }
                )
                docs.append(doc)
            
        except Exception as e:
            print(f"Error processing table element: {e}")
        
        return docs

    def resolve_edges(self, docs: List[Document]) -> List[Document]:
        """
        Implements hierarchy linking and table-content connectivity.
        """
        self.console.print(f"[blue]ℹ️  Resolving edges for {len(docs)} documents[/blue]")
        # 1. Initialize edge_ids
        for d in docs:
            if "edge_ids" not in d.metadata:
                d.metadata["edge_ids"] = []
                
        node_map = {d.metadata.get("id"): d for d in docs if d.metadata.get("id")}
        
        # 2. Hierarchy Linking (Child -> Parent)
        # Note: In our PDF extraction, parents (structural tags) might usually be virtual or
        # created as their own nodes. If parent exists in docs, link to it.
        for d in docs:
            parent_id = d.metadata.get("parent_ref")
            if parent_id and parent_id in node_map:
                # Add parent to edge_ids
                if parent_id not in d.metadata["edge_ids"]:
                    d.metadata["edge_ids"].append(parent_id)
                # Add self to parent's edge_ids (Bidirectional)
                parent_doc = node_map[parent_id]
                my_id = d.metadata.get("id")
                if my_id and my_id not in parent_doc.metadata["edge_ids"]:
                    parent_doc.metadata["edge_ids"].append(my_id)

        # 3. Table Content Linking (Value-based)
        # Group rows by table
        table_rows = [d for d in docs if d.metadata.get("is_table_row")]
        if len(table_rows) > 1:
            # Build Value Index: value -> [doc_ids]
            # We focus on "columns" dictionary in metadata
            value_index = {}
            for row in table_rows:
                my_id = row.metadata.get("id")
                cols = row.metadata.get("columns", {})
                if not cols: continue
                
                for col_name, val in cols.items():
                    val_str = str(val).strip()
                    # Filter short/noise values
                    if len(val_str) < 3 or val_str.lower() in ["nan", "none", "null", "total"]:
                        continue
                    
                    if val_str not in value_index:
                        value_index[val_str] = []
                    value_index[val_str].append(my_id)
            
            # create edges for shared values
            for val, ids in value_index.items():
                if len(ids) > 1:
                    # Link all these IDs to each other (Clique)
                    # Limit clique size to avoid explosion? User said "collect all ids... within a edges list".
                    # Let's link them.
                    unique_ids = list(set(ids))
                    for i in range(len(unique_ids)):
                        for j in range(i + 1, len(unique_ids)):
                            id_a = unique_ids[i]
                            id_b = unique_ids[j]
                            
                            if id_a in node_map and id_b in node_map:
                                doc_a = node_map[id_a]
                                doc_b = node_map[id_b]
                                
                                if id_b not in doc_a.metadata["edge_ids"]:
                                    doc_a.metadata["edge_ids"].append(id_b)
                                if id_a not in doc_b.metadata["edge_ids"]:
                                    doc_b.metadata["edge_ids"].append(id_a)
        
        self.console.print(f"[green]✅ Edges resolved for {len(docs)} documents[/green]")
        return docs
