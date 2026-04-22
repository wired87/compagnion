# PdfProcessor

The `PdfProcessor` is a specialized processor for handling PDF files with advanced content extraction capabilities. It goes beyond simple text extraction by parsing the PDF structure to identify and preserve tables, images, and HTML hierarchical elements.

## Features

- **HTML-based Extraction**: Uses `pdfminer.high_level` to convert PDF to HTML, preserving layout structure.
- **Table Parsing**: Identifies HTML tables (`<table>`) and extracts them as structured data, preserving row and column relationships.
- **Hierarchical Content**: Extracts content based on HTML tags (`div`, `p`, `span`, etc.), maintaining the document structure.
- **Chunking**: Uses `LangChain`'s `RecursiveCharacterTextSplitter` to chunk extracted text content into manageable pieces.
- **DataFrame Output**: Returns a `pandas.DataFrame` containing the processed data, ready for upsertion into BigQuery.

## Output Schema (SOA)

The processor generates a DataFrame with the following columns (Structure of Arrays):

- `id`: Unique identifier for the chunk/row.
- `content`: The text content of the chunk.
- `embedding`: Vector embedding of the content (initially empty/placeholder).
- `file_name`: Name of the source file.
- `field_type`: The HTML tag or structural type (e.g., `table`, `p`, `div`).
- `content_type`: General content classification (e.g., `text`, `table_row`).
- `part_type`: Granularity of the part (e.g., `chunk`, `row`).
- `metadata`: JSON string containing additional metadata (page number, parent reference, table columns, etc.).
- `parent_id`: ID of the parent node/chunk.
- `page_number`: Page number in the original PDF.

## Usage

```python
from client_package.processor.pdf_processor import PdfProcessor

processor = PdfProcessor()
df = processor.extract_to_dataframe("path/to/document.pdf")
print(df.head())
```
