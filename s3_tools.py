import os
import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from tqdm.auto import tqdm
from rich.tree import Tree
from rich.console import Console

from pathlib import Path, PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        max_pool_connections = os.cpu_count() or 1
        config = Config(max_pool_connections=max_pool_connections)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config
        )

    def list_files(self, bucket_name: str, prefix: str, file_extension: str = None) -> list[Path]:
        """
        Lists objects in an S3 bucket that match the specified prefix and optional file extension.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix to filter objects in the bucket.
            file_extension (str, optional): The file extension to filter objects. If None, all objects with the specified prefix are listed. Defaults to None.

        Returns:
            list[Path]: A list of object keys as Path objects.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix)
            if "Contents" not in response:
                logger.info(f"No objects found in folder: {prefix}")
                return []

            objects = [
                # Use PurePosixPath
                PurePosixPath(obj["Key"]) for obj in response["Contents"]
                if not file_extension or obj["Key"].endswith(file_extension)
            ]

            if not objects:
                logger.info(
                    f"No {file_extension if file_extension else 'files'} found in folder: {prefix}")

            return objects

        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def print_file_structure(self, files: list[Path]):
        """
        Prints a tree-like structure of the files and directories.

        Args:
            files (list[Path]): List of file paths to display.
        """
        if not files:
            logger.info("No files to display.")
            return

        folder_structure = {}

        for file in files:
            parts = file.parts
            current_level = folder_structure

            for part in parts:
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]

        def build_tree(level: dict, parent: Tree):
            for folder, subfolders in level.items():
                branch = parent.add(folder)
                if isinstance(subfolders, dict):
                    build_tree(subfolders, branch)

        console = Console()
        root = Tree("[bold blue]S3 File Structure[/bold blue]")
        build_tree(folder_structure, root)
        console.print(root)

    def _download_file_single(self, bucket_name: str, object_key: Path, local_path: Path, keep_structure: bool = True, verbose: bool = True) -> bool:
        """
        Downloads a file from an S3 bucket to a local path.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path): The key of the object in the S3 bucket.
            local_path (Path): The base local directory for downloads.
            keep_structure (bool): Whether to preserve the S3 directory structure locally.
                                If False, adds unique identifiers to avoid name conflicts.
                                Defaults to True.
        """
        try:
            if keep_structure:
                full_local_path = local_path / \
                    object_key.relative_to(object_key.anchor)
            else:
                full_local_path = local_path / f"{object_key.name}"

            full_local_path.parent.mkdir(parents=True, exist_ok=True)

            self.s3_client.download_file(
                bucket_name, str(object_key), str(full_local_path))
            if verbose:
                tqdm.write(f"Downloaded: {object_key}")
            return True
        except ClientError as e:
            logger.error(f"Error downloading file {object_key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading file {object_key}: {e}")
            return False

    def download_files(self, list_files: list[Path | str], bucket_name: str, local_base_path: Path, max_workers: int = os.cpu_count() or 1, keep_structure=True):
        """
        Downloads multiple files in parallel from an S3 bucket.

        Args:
            list_files (list[Path]): List of files to download.
            bucket_name (str): The name of the S3 bucket.
            local_base_path (Path): The base directory where files will be downloaded.
            max_workers (int): Maximum number of threads to use for parallel downloads. Defaults to 10.
            keep_structure (bool): Whether to preserve the S3 directory structure locally.
                    If False, adds unique identifiers to avoid name conflicts. Defaults to True.
        """
        list_files = [Path(item) if isinstance(item, str)
                      else item for item in list_files]
        success_count = 0
        failure_count = 0

        with ThreadPoolExecutor(max_workers) as executor:
            futures = [
                executor.submit(
                    self._download_file_single,
                    bucket_name,
                    file,
                    local_base_path,
                    keep_structure,
                    False
                )
                for file in list_files
            ]

            with tqdm(
                total=len(futures),
                desc=f"Downloading {len(futures)} files",
                unit="file",
                dynamic_ncols=True
            ) as pbar:
                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        failure_count += 1
                    pbar.update(1)

        tqdm.write(f"\nAll files processed. Downloaded to: {local_base_path}")
        tqdm.write(f"Successfully downloaded: {success_count} files")
        tqdm.write(f"Failed downloads: {failure_count} files")

        return success_count, failure_count

    def upload_file(self, bucket_name: str, object_key: Path, local_path: Path):
        """
        Uploads a file from a local path to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path): The key of the object in the S3 bucket.
            local_path (Path): The local path of the file to be uploaded.

        Example:
            >>> s3_manager.upload_file('my-bucket', 'path/to/my-file.txt', '/local/path/to/my-file.txt')
        """
        try:
            self.s3_client.upload_file(
                str(local_path), bucket_name, str(object_key))
            logger.info(f"File {local_path} uploaded to {bucket_name}/{object_key}")
        except ClientError as e:
            logger.error(f"Error uploading file {local_path}: {e}")
