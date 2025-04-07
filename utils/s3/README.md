# S3 Utilities

This module provides utilities for working with Amazon S3 storage services.

## Files

### manager.py
The main S3 management utility that provides a comprehensive interface for interacting with AWS S3 services. Features include:

- Efficient file uploads and downloads with progress bars
- Parallel/concurrent operations for improved performance
- Asynchronous operations via aioboto3
- Directory structure preservation
- File listing and browsing capabilities
- Error handling and retry mechanisms

Key classes:
- `S3Manager`: Main class for S3 operations

### gui.py
Provides a graphical user interface for S3 operations, built on top of the S3Manager functionality.

## Usage Examples

```python
# Initialize S3 manager
s3_manager = S3Manager(aws_access_key_id="YOUR_KEY", aws_secret_access_key="YOUR_SECRET")

# Download files
s3_manager.download_files(
    bucket_name="my-bucket",
    list_files=["path/to/file1.txt", "path/to/file2.txt"],
    local_base_path="./downloads"
)

# Upload files
s3_manager.upload_files(
    bucket_name="my-bucket",
    base_object_key="destination/folder",
    list_files=["local/file1.txt", "local/file2.txt"]
)

# List files in bucket
files = s3_manager.list_files(
    bucket_name="my-bucket",
    prefix="path/to/folder"
)

# Use async operations for improved performance
await s3_manager.async_download_files(
    bucket_name="my-bucket",
    list_files=["path/to/file1.txt", "path/to/file2.txt"],
    local_base_path="./downloads"
)
``` 