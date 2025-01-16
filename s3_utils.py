from utils import tqdm

import boto3
import logging
from pathlib import Path
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def list_objects(self, bucket_name: str, prefix: str, file_extension: str = None) -> list[Path]:
        """
        Lists objects in an S3 bucket that match the specified prefix and optional file extension.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix to filter objects in the bucket.
            file_extension (str, optional): The file extension to filter objects. If None, all objects with the specified prefix are listed. Defaults to None.

        Example:
            >>> objects = s3_manager.list_objects("my-bucket", "my-prefix", ".txt")
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix)
            if "Contents" not in response:
                logger.info(f"No objects found in folder: {prefix}")
                return []

            objects = [
                Path(obj["Key"]) for obj in response["Contents"] if not file_extension or obj["Key"].endswith(file_extension)
            ]

            if not objects:
                logger.info(
                    f"No {file_extension if file_extension else 'files'} found in folder: {prefix}")
            return objects

        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def print_file_structure(self, files: list[Path]):
        if not files:
            logger.info("No files to display.")
            return

        folder_structure = {}

        for file in files:
            parts = str(file).split("/")
            current_level = folder_structure

            for part in parts:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]

        def print_hierarchy(level, indent=0):
            for folder, subfolders in level.items():
                print(f"|{'-' * indent}-{folder}")
                if isinstance(subfolders, dict):
                    print_hierarchy(subfolders, indent + 2)

        print("Listing file structure:")
        print_hierarchy(folder_structure)

    def download_file(self, bucket_name: str, object_key: Path, local_path: Path):
        """
        Downloads a file from an S3 bucket to a local path.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key of the object in the S3 bucket.
            local_path (str): The local path where the file will be downloaded.

        Example:
            >>> s3_manager.download_file('my-bucket', 'path/to/my-file.txt', '/local/path/to/my-file.txt')
            File path/to/my-file.txt downloaded to /local/path/to/my-file.txt
        """
        try:
            self.s3_client.download_file(
                bucket_name, str(object_key), str(local_path))
            logger.info(f"File {object_key} downloaded to {local_path}")
        except ClientError as e:
            logger.error(f"Error downloading file {object_key}: {e}")

    def parallel_download(self, list_files: list[Path], bucket_name: str, local_base_path: Path, max_workers: int = 10):
        with ThreadPoolExecutor(max_workers) as executor:
            futures = []
            for file in list_files:
                file_path = Path(file)
                local_path = local_base_path / file_path.name
                futures.append(executor.submit(
                    self.download_file, bucket_name, file, local_path))

            for future in tqdm(as_completed(futures), total=len(futures)):
                future.result()

    def upload_file(self, bucket_name: str, object_key: Path, local_path: Path):
        """
        Uploads a file from a local path to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key of the object in the S3 bucket.
            local_path (str): The local path of the file to be uploaded.

        Example:
            >>> s3_manager.upload_file('my-bucket', 'path/to/my-file.txt', '/local/path/to/my-file.txt')
            File /local/path/to/my-file.txt uploaded to my-bucket/path/to/my-file.txt
        """
        try:
            self.s3_client.upload_file(
                str(local_path), bucket_name, str(object_key))
            logger.info(f"File {local_path} uploaded to {bucket_name}/{object_key}")
        except ClientError as e:
            logger.error(f"Error uploading file {local_path}: {e}")
