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
        self.max_threads = os.cpu_count()
        config = Config(max_pool_connections=self.max_threads * 2)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config
        )

    def _process_operations(self, operations, description, success_message, error_message_prefix):
        success_count = 0
        failure_count = 0

        with ThreadPoolExecutor(self.max_threads) as executor:
            futures = [executor.submit(op) for op in operations]

            with tqdm(
                total=len(futures),
                desc=description,
                unit="file",
                dynamic_ncols=True
            ) as pbar:
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            failure_count += 1
                    except Exception as e:
                        logger.error(f"{error_message_prefix}: {e}")
                        failure_count += 1
                    finally:
                        pbar.update(1)

        tqdm.write(f"\n{success_message}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count

    def _normalize_paths(self, list_files):
        return [
            Path(item) if isinstance(item, str) else item
            for item in list_files
        ]

    def download_files(self, bucket_name: str, list_files: list[Path | str], local_base_path: Path | str, keep_structure=True):
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
        if isinstance(local_base_path, str):
            local_base_path = Path(local_base_path)

        list_files = self._normalize_paths(list_files)
        operations = [
            lambda f=file: self._download_file_single(
                bucket_name=bucket_name,
                object_key=f,
                local_path=local_base_path,
                keep_structure=keep_structure,
                verbose=False
            )
            for file in list_files
        ]

        return self._process_operations(
            operations=operations,
            description=f"Downloading {len(list_files)} files",
            success_message=f"All files processed. Downloaded to: {local_base_path}",
            error_message_prefix="Error during download"
        )

    def _download_file_single(self, bucket_name: str, object_key: Path | str, local_path: Path | str, keep_structure: bool = True, verbose: bool = True):
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
        if isinstance(local_path, str):
            local_path = Path(local_path)
        try:
            if keep_structure:
                full_local_path = local_path / \
                    object_key.relative_to(object_key.anchor)
            else:
                full_local_path = local_path / object_key.name

            full_local_path.parent.mkdir(parents=True, exist_ok=True)
            self.s3_client.download_file(
                bucket_name, str(object_key), str(full_local_path))

            if verbose:
                tqdm.write(f"Downloaded: {object_key}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {object_key}: {e}")
            return False

    def upload_files(self, bucket_name: str, base_object_key: Path | str, list_files: list[Path | str]):
        """
        Uploads multiple files in parallel to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            base_object_key (Path | str): The base directory in the S3 bucket where files will be uploaded.
            list_files (list[Path | str]): List of files to upload.

        Example:
            >>> s3_manager.upload_file('my-bucket', 'path/to/my-file.txt', '/local/path/to/my-file.txt')
        """
        if isinstance(base_object_key, str):
            base_object_key = Path(base_object_key)

        list_files = self._normalize_paths(list_files)
        operations = [
            lambda f=file: self._upload_file_single(
                bucket_name=bucket_name,
                object_key=base_object_key / f.name,
                local_path=f
            )
            for file in list_files
        ]

        return self._process_operations(
            operations=operations,
            description=f"Uploading {len(list_files)} files",
            success_message=f"All files processed. Uploaded to: {bucket_name}/{base_object_key}",
            error_message_prefix="Error during upload"
        )

    def _upload_file_single(self, bucket_name: str, object_key: Path | str, local_path: Path | str):
        """
        Логика выгрузки одного файла с прогресс-баром.
        """

        if isinstance(local_path, str):
            local_path = Path(local_path)

        try:
            file_size = os.path.getsize(local_path)
            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                desc=f"Uploading {local_path.name}",
                leave=False,
                dynamic_ncols=True
            ) as pbar:
                with open(local_path, "rb") as f:
                    self.s3_client.upload_fileobj(
                        Fileobj=f,
                        Bucket=bucket_name,
                        Key=str(object_key),
                        Callback=lambda bytes_: pbar.update(bytes_)
                    )
            return True
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False

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
