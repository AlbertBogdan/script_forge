# OCR Utilities

This module provides utilities for Optical Character Recognition (OCR) processing using Gradio-based API clients.

## Files

### gradio_clients.py
Contains client classes for interacting with OCR services through Gradio interfaces. Features include:

- Text detection in images
- Text recognition from cropped regions
- Document OCR processing
- Symbol detection
- Support for batch processing with parallel execution
- Automatic retries for failed requests

Key classes:
- `TextDetectorApiClientGradio`: Detects text regions in images
- `ImageTextRecognizerApiClientParseq`: Recognizes text from image regions
- `DocumentOcrApiClient`: Process entire documents for OCR
- `OCRProcessor`: Processes and converts OCR data formats

## Utility Functions

- `convert2image`: Converts PDF pages to images
- `crop_text_box_new`: Crops text boxes from images with rotation support
- `pil_image_to_base64`: Converts PIL images to base64 encoding

## Usage Example

```python
from PIL import Image
from utils.ocr.gradio_clients import TextDetectorApiClientGradio, ImageTextRecognizerApiClientParseq

# Initialize text detector
detector = TextDetectorApiClientGradio(api_url="http://your-text-detector-url")

# Load image
image = Image.open("document.png")

# Detect text regions
textboxes = detector.detect_text(image)

# Initialize text recognizer
recognizer = ImageTextRecognizerApiClientParseq(api_url="http://your-text-recognizer-url")

# Recognize text in detected regions
recognized_boxes = recognizer.recognize_many(textboxes)

# Print recognized text
for box in recognized_boxes:
    print(box.text)
``` 