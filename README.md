# Script Forge

A Python utility library for various data processing and automation tasks.

## Description

Script Forge is a collection of utilities for handling various data processing tasks, including OCR (Optical Character Recognition), S3 storage operations, and data management tools.

## Features

- OCR processing capabilities
- S3 storage integration
- Data compression and management tools
- CVAT integration tools
- Various utility functions for data processing

## Requirements

- Python >= 3.12.8
- See `requirements.txt` for full list of dependencies

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
```

2. Install dependencies using uv:
```bash
uv pip install -r requirements.txt
```

## Project Structure

```
.
├── utils/               # Utility modules
│   ├── kh/             # Knowledge Hub utilities
│   │   ├── kh2_records_deserializer.py    # KH2 records deserialization
│   │   ├── kh2_upload_example_misc.py     # KH2 upload examples
│   │   ├── kh_documents_uploader_service.py # Document upload service
│   │   ├── kh_link.py                     # KH resource links
│   │   └── kh2-generate-zip.ipynb         # ZIP generation examples
│   ├── ocr/            # OCR processing utilities
│   │   └── gradio_clients.py              # OCR API clients
│   ├── s3/             # S3 storage utilities
│   │   ├── manager.py                     # S3 management
│   │   └── gui.py                         # S3 GUI interface
│   ├── zipmanager.py   # ZIP file management
│   └── cvat_tools.py   # CVAT integration tools
├── pyproject.toml      # Project configuration
├── requirements.txt    # Python dependencies
└── README.md          # This file
```

## Module Descriptions

### S3 Utilities (utils/s3/)
Provides comprehensive tools for Amazon S3 storage operations:
- Efficient file uploads/downloads with progress tracking
- Parallel/concurrent operations
- Asynchronous operations via aioboto3
- Directory structure preservation
- File listing and browsing
- Error handling and retries

### OCR Utilities (utils/ocr/)
Tools for Optical Character Recognition:
- Text detection in images
- Text recognition from cropped regions
- Document OCR processing
- Symbol detection
- Batch processing with parallel execution
- Automatic retry mechanisms

### Knowledge Hub Utilities (utils/kh/)
Tools for Knowledge Hub integration:
- KH2 records deserialization
- Document upload automation
- Resource link management
- ZIP archive generation
- Document processing workflows

### Additional Utilities
- `zipmanager.py`: ZIP file management and compression
- `cvat_tools.py`: CVAT (Computer Vision Annotation Tool) integration

## Dependencies

Major dependencies include:
- aioboto3: For async S3 operations
- doc-toolbox: For document processing
- opencv-python: For image processing
- numpy: For numerical operations
- requests: For HTTP requests
- and more...

## License

This project is licensed under the terms specified in the LICENSE file.

## Contributing

Please read the contributing guidelines before submitting pull requests.

## Support

For support, please contact the project maintainers.

# Перемещаем всё содержимое папки script_forge в текущую директорию
Move-Item -Path "script_forge\*" -Destination . -Force
# Удаляем теперь пустую папку script_forge
Remove-Item -Path "script_forge" -Recurse -Force

# Перемещаем содержимое и удаляем папку
mv script_forge/* . && rm -rf script_forge