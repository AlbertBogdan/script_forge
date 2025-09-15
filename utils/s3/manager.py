import base64
import fnmatch
import logging
import os
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from rich.console import Console
from rich.tree import Tree
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.max_threads = os.cpu_count()
        config = Config(max_pool_connections=self.max_threads)

        self.s3_client = boto3.client(
            "s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, config=config
        )

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def _is_unique(self, list_files: list) -> bool:
        return len(list_files) == len(set(list_files))

    def _with_progress_bar(self, total: int, desc: str, file_operation: Callable) -> None:
        """
        Wrap a file operation with a tqdm progress bar.

        Args:
            total: Total size of the file operation, in bytes.
            desc: Description to display above the progress bar.
            file_operation: Callable that takes a single argument, a callable to report bytes written.
        """
        with tqdm(total=total, unit="B", unit_scale=True, desc=desc, leave=False, dynamic_ncols=True) as pbar:
            file_operation(lambda bytes_: pbar.update(bytes_))

    def _process_operations(
        self, operations: list[Callable[..., bool]], description: str, success_message: str, error_message_prefix: str
    ) -> tuple[int, int]:
        """
        Run file operations in parallel with a progress bar, supporting cancellation in Jupyter.

        Args:
            operations: List of callables, each taking a `threading.Event` for cancellation and returning a boolean.
            description: Progress bar description.
            success_message: Message to log on successful completion.
            error_message_prefix: Prefix for error messages.

        Returns:
            tuple: (success_count, failure_count) for successful (`True`) and failed (`False`, exception, or interrupted
                )operations.
        """
        success_count = 0
        failure_count = 0

        try:
            with ThreadPoolExecutor(self.max_threads) as executor:
                futures = [executor.submit(op) for op in operations]

                with tqdm(total=len(futures), desc=description, unit="file", dynamic_ncols=True) as pbar:
                    for future in as_completed(futures):
                        try:
                            result = future.result()

                            if result is True:
                                success_count += 1
                            else:
                                failure_count += 1

                        except Exception as e:
                            logger.error(f"{error_message_prefix}: {e}")
                            failure_count += 1
                        finally:
                            pbar.update(1)
        except KeyboardInterrupt:
            logger.info("Operation interrupted by user. Shutting down...")
            executor._threads.clear()
            raise

        tqdm.write(f"\n{success_message}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count

    def download_files(
        self,
        list_files: list[tuple[str, Path | str]] | tuple[str, Path | str],
        local_base_path: Path | str,
        keep_structure=True,
        progress_bar=True,
    ):
        """
        Download multiple files from one or more S3 buckets in parallel.

        Args:
            list_files (list[tuple[str, Path | str]] | tuple[str, Path | str]):
                A list of tuples (bucket_name, object_key) or a single tuple specifying
                the S3 files to download.
            local_base_path (Path | str):
                The local base directory where files will be saved.
            keep_structure (bool, optional):
                If True, preserves the original S3 key directory structure under
                the local_base_path. Defaults to True.
            progress_bar (bool, optional):
                If True, displays a progress bar for each file download.
                Defaults to True.

        Returns:
            tuple[int, int]: A tuple (success_count, failure_count) indicating
            the number of files downloaded successfully and the number of failures.

        Example:
            s3_manager.download_files(
                list_files=[("my-bucket", "path/to/file1.txt"),
                            ("my-bucket", "path/to/file2.txt")],
                local_base_path="./downloads"
            )
        """

        is_unique = self._is_unique(list_files)

        if isinstance(local_base_path, str):
            local_base_path = Path(local_base_path)

        if isinstance(list_files, tuple):
            list_files = [list_files]

        operations = [
            lambda f=file: self._download_file_single(
                bucket_name=bucket_name,
                object_key=f,
                local_path=local_base_path,
                is_unique=is_unique,
                keep_structure=keep_structure,
                progress_bar=progress_bar,
            )
            for bucket_name, file in list_files
        ]

        return self._process_operations(
            operations=operations,
            description=f"Downloading {len(list_files)} files",
            success_message=f"All files processed. Downloaded to: {local_base_path}",
            error_message_prefix="Error during download",
        )

    def _download_file_single(
        self,
        bucket_name: str,
        object_key: Path | str,
        local_path: Path | str,
        is_unique: bool,
        keep_structure: bool = True,
        progress_bar: bool = True,
    ):
        """
        Downloads a single file from an S3 bucket to a local path.

        Args:
            bucket_name (str): Name of the S3 bucket.
            object_key (Path | str): S3 object key (path to the file in the bucket).
            local_path (Path | str): Local destination path for the downloaded file.
            is_unique (bool): If True, preserves the original file name; if False, generates
                a unique name using SHA-256 hash to avoid conflicts.
            keep_structure (bool, optional): If True, maintains the S3 object's directory
                structure in the local path; if False, saves the file directly under
                local_path. Defaults to True.
            progress_bar (bool, optional): If True, displays a progress bar during
                download. Defaults to True.

        Returns:
            bool: True if the download succeeds, False otherwise.

        Example:
            s3_manager._download_file_single('my-bucket', 'path/to/my-file.txt', '/local/path/to/download')
        """
        local_path = Path(local_path)
        object_key = Path(object_key)

        if object_key.is_dir():
            logger.error(f"It's dir {object_key}")
            return False

        try:
            if keep_structure:
                full_local_path = local_path / object_key.relative_to(object_key.anchor)
            elif not is_unique:
                encode_filename = base64.b64encode(str(object_key.with_suffix(""))).decode()
                full_local_path = local_path / f"{encode_filename}{object_key.suffix}"
            else:
                full_local_path = local_path / object_key.name

            full_local_path.parent.mkdir(parents=True, exist_ok=True)

            if progress_bar:
                object_info = self.s3_client.head_object(Bucket=bucket_name, Key=str(object_key))
                file_size = object_info["ContentLength"]

                with full_local_path.open("wb") as f:
                    self._with_progress_bar(
                        total=file_size,
                        desc=f"Downloading {object_key}",
                        file_operation=lambda callback: self.s3_client.download_fileobj(
                            Bucket=bucket_name, Key=str(object_key), Fileobj=f, Callback=callback
                        ),
                    )
            return True

        except Exception as e:
            logger.error(f"Error downloading {object_key}: {e}")
            return False

    def upload_files(
        self,
        bucket_name: str,
        base_object_key: Path | str,
        list_files: list[Path | str],
        progress_bar: bool = True,
    ):
        """
        Uploads multiple files to an S3 bucket in parallel.

        Args:
            bucket_name (str): The name of the S3 bucket.
            base_object_key (Path | str): The base key prefix for the objects in the S3 bucket.
            list_files (list[Path | str]): List of local file paths to upload.

        Returns:
            tuple: A tuple containing the count of successful and failed uploads.

        Example:
            s3_manager.upload_files('my-bucket', 'path/to/', ['/local/path/to/file1.txt',
                '/local/path/to/file2.txt'])
        """
        list_files: list[Path] = [Path(item) if isinstance(item, str) else item for item in list_files]
        base_object_key = Path(base_object_key)

        operations = [
            lambda f=file: self._upload_file_single(
                bucket_name=bucket_name,
                object_key=base_object_key / f.name,
                local_path=f,
                progress_bar=progress_bar,
            )
            for file in list_files
        ]

        return self._process_operations(
            operations=operations,
            description=f"Uploading {len(list_files)} files",
            success_message=f"All files processed. Uploaded to: {bucket_name}/{base_object_key}",
            error_message_prefix="Error during upload",
        )

    def _upload_file_single(
        self,
        bucket_name: str,
        object_key: Path | str,
        local_path: Path | str,
        progress_bar: bool = True,
    ) -> bool:
        """
        Uploads a single file to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path | str): The key of the object in the S3 bucket.
            local_path (Path | str): The path to the local file to upload.

        Returns:
            bool: True if the upload was successful, False otherwise.

        Example:
            >>> s3_manager._upload_file_single('my-bucket', 'path/to/my-file.txt', '/local/path/to/my-file.txt')
        """
        if isinstance(local_path, str):
            local_path = Path(local_path)
        try:
            if progress_bar:
                file_size = local_path.stat().st_size
                with local_path.open("rb") as f:
                    self._with_progress_bar(
                        total=file_size,
                        desc=f"Uploading {local_path.name}",
                        file_operation=lambda callback: self.s3_client.upload_fileobj(
                            Fileobj=f, Bucket=bucket_name, Key=str(object_key), Callback=callback
                        ),
                    )
            return True

        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False

    def list_files(
        self,
        bucket_name: str,
        prefix: str,
        file_pattern: str | list[str] | None = None,
    ) -> list[tuple[str, Path]]:
        """
        List files in an S3 bucket under a given prefix, with optional filtering.

        Args:
            bucket_name (str):
                The name of the S3 bucket.
            prefix (str):
                The prefix (folder-like path) to search within the bucket.
            file_pattern (str | list[str] | None, optional):
                File extension(s) or glob-style patterns to filter results.
                Examples: "txt", [".csv", "*.json"], or ["data_*.parquet"].
                If None, all files are returned. Defaults to None.

        Returns:
            list[tuple[str, Path]]:
                A list of (bucket_name, Path(object_key)) tuples for matching files.

        Notes:
            - Directories (keys ending with "/") are ignored.
            - Patterns are case-insensitive.
            - Supports glob wildcards (*, ?, []).

        Example:
            files = s3_manager.list_files(
                bucket_name="my-bucket",
                prefix="data/",
                file_pattern=["*.csv", "*.json"]
            )
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if "Contents" not in response:
                logger.info(f"No objects found in folder: {prefix}")
                return []

            raw_patterns = []
            if isinstance(file_pattern, str):
                raw_patterns = [file_pattern]
            elif isinstance(file_pattern, list):
                raw_patterns = file_pattern

            regex_patterns = [re.compile(p, re.IGNORECASE) for p in raw_patterns]

            objects = []
            for obj in response["Contents"]:
                key: str = obj["Key"]
                if key.endswith("/"):
                    continue

                file_path = (bucket_name, Path(key))
                filename = file_path[1].name

                if not regex_patterns or any(p.search(filename) for p in regex_patterns):
                    objects.append(file_path)

            if not objects and regex_patterns:
                logger.info(f"No files matching regex patterns {raw_patterns} found in folder: {prefix}")

            return objects

        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            return []

    def print_file_structure(self, files: list[Path | str]):
        """
        Prints a tree-like structure of the files and directories.

        Args:
            files (list[Path | str]): List of file paths to display.
        """
        if not files:
            logger.info("No files to display.")
            return

        folder_structure = {}

        for file in files:
            if isinstance(file, Path):
                file = Path(file)

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
