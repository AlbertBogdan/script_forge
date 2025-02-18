# Utility Scripts Collection

## Overview
This repository contains a collection of utility scripts for various tasks including file management, cloud storage, OCR, and object detection.

## Modules

### File Management
- `zipmanager.py`: Provides utilities for creating, managing, and manipulating ZIP archives
  - Create ZIP files
  - Add files and folders to ZIP archives
  - Extract ZIP contents
  - List ZIP contents

### Cloud Storage
- `s3_tools.py`: AWS S3 interaction utilities
  - List files in S3 buckets
  - Download files from S3
  - Upload files to S3
  - Print file structures

### Schema and Image Handling
- `schemas_loader.py`: Manages loading and saving of various schema types
  - Load and save image schemas
  - Manage annotations
  - Handle image and category metadata

### OCR (Optical Character Recognition)
- `ocr/gradio_clients.py`: OCR-related utilities
  - Text detection
  - Image conversion
  - Text recognition from images and PDFs

### Object Detection
- `obj_detect/yolo.py`: YOLO object detection utilities
  - Load object detection models
  - Perform predictions with model slicing

## Subfolders
- `kh/`: Additional utility scripts (contents not fully explored)
- `ocr/`: OCR-related tools
- `obj_detect/`: Object detection utilities

## Requirements
- Python 3.8+
- Dependencies: See `requirements.txt`

## Usage
Refer to individual script docstrings for specific usage instructions.

## License
See `LICENSE` file for licensing information.