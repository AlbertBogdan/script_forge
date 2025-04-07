# KH (Knowledge Hub) Utilities

This module provides utilities for working with Knowledge Hub services for document processing and management.

## Files

### kh2_records_deserializer.py
Deserializes KH2 records from a directory structure. Features:
- Extraction of KH2 annotations from file system
- Conversion to KH2Record objects with image providers
- Support for indexing and page management

### kh2_upload_example_misc.py
Example script demonstrating the upload process to KH2 services. Includes:
- Sample code for initiating uploads
- Miscellaneous utility functions for KH2 integration

### kh_documents_uploader_service.py
Service for uploading documents to Knowledge Hub. Features:
- Document upload automation
- Integration with KH API
- Document processing workflow

### kh_link.py
Utility for generating and managing links to KH resources. Features:
- Link generation for KH documents
- URL management for Knowledge Hub resources

### kh2-generate-zip.ipynb
Jupyter notebook with example workflows for generating ZIP archives for KH2 system.
- Document preparation
- ZIP file generation
- KH2 compatible archive structures

## Usage Example

```python
from utils.kh.kh2_records_deserializer import Kh2RecordsDeserializer

# Initialize deserializer
deserializer = Kh2RecordsDeserializer()

# Deserialize KH2 records from directory
records = deserializer.deserialize_from_dir("path/to/kh2/data")

# Process records
for record in records:
    # Access record properties
    file_path = record.file_path
    page_number = record.page_number
    annotation = record.annotation
    
    # Get image from provider
    image = record.image_provider.get_image()
    
    # Process the image and annotation as needed
    # ...
``` 