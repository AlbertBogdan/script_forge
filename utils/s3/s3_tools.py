import os
import logging

import asyncio
import aioboto3

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from tqdm.auto import tqdm
from rich.tree import Tree
from rich.console import Console

from pathlib import Path, PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Awaitable

# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str):
        """
        Initialize S3Manager with AWS access key pair.

        Args:
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.

        Notes:
            The maximum number of threads used by boto3 is set to the number of CPUs available,
            multiplied by 2. This should be sufficient for most file operations, but it can be
            increased if needed.
        """
        self.max_threads = os.cpu_count()
        config = Config(max_pool_connections=self.max_threads * 2)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=config
        )

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def _with_progress_bar(self, total: int, desc: str, file_operation: callable) -> None:
        """
        Wrap a file operation with a tqdm progress bar.

        Args:
            total: Total size of the file operation, in bytes.
            desc: Description to display above the progress bar.
            file_operation: Callable that takes a single argument, a callable to report bytes written.
        """
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=desc,
            leave=False,
            dynamic_ncols=True
        ) as pbar:
            file_operation(lambda bytes_: pbar.update(bytes_))

    async def _async_with_progress_bar(self, total: int, desc: str, file_operation: Callable[[Callable[[int], None]], Awaitable[None]]) -> None:
        """
        Asynchronously wrap a file operation with a tqdm progress bar.

        Args:
            total: Total size of the file operation, in bytes.
            desc: Description to display above the progress bar.
            file_operation: Async callable that takes a callback to report bytes written.
        """
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=desc,
            leave=False,
            dynamic_ncols=True
        ) as pbar:
            await file_operation(lambda bytes_: pbar.update(bytes_))

    def _process_operations(self, operations: list[Callable[..., bool]], description: str, success_message: str, error_message_prefix: str):
        """
        Execute a list of file operations in parallel with a progress bar.

        Args:
            operations: List of callables that each take a single argument, a callable to report bytes written.
            description: Description to display above the progress bar.
            success_message: Message to display when all operations are complete.
            error_message_prefix: Prefix to use for error messages.

        Returns:
            tuple: (success_count, failure_count) where success_count is the number of operations that completed
                successfully and failure_count is the number of operations that failed.
        """
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
        Downloads multiple files from an S3 bucket in parallel.

        Args:
            bucket_name (str): The name of the S3 bucket.
            list_files (list[Path | str]): List of S3 object keys to download.
            local_base_path (Path | str): The local directory to download files into.
            keep_structure (bool, optional): Whether to preserve the S3 directory structure locally.
                Defaults to True.

        Returns:
            tuple: A tuple containing the count of successful and failed downloads.

        Example:
            >>> s3_manager.download_files('my-bucket', ['path/to/file1.txt', 'path/to/file2.txt'], './downloads')
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
        Downloads a single file from an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path | str): The key of the object in the S3 bucket.
            local_path (Path | str): The local path to download the file to.
            keep_structure (bool): Whether to preserve the S3 directory structure locally.
                    If False, adds unique identifiers to avoid name conflicts. Defaults to True.
            verbose (bool): Whether to print the name of each downloaded file. Defaults to True.

        Returns:
            bool: True if the download was successful, False otherwise.

        Example:
            >>> s3_manager._download_file_single('my-bucket', 'path/to/my-file.txt', '/local/path/to/download')
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

            object_info = self.s3_client.head_object(
                Bucket=bucket_name, Key=str(object_key))
            file_size = object_info['ContentLength']

            with open(full_local_path, "wb") as f:
                self._with_progress_bar(
                    total=file_size,
                    desc=f"Downloading {object_key}",
                    file_operation=lambda callback: self.s3_client.download_fileobj(
                        Bucket=bucket_name,
                        Key=str(object_key),
                        Fileobj=f,
                        Callback=callback
                    )
                )

            if verbose:
                tqdm.write(f"Downloaded: {object_key}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {object_key}: {e}")
            return False

    def upload_files(self, bucket_name: str, base_object_key: Path | str, list_files: list[Path | str]):
        """
        Uploads multiple files to an S3 bucket in parallel.

        Args:
            bucket_name (str): The name of the S3 bucket.
            base_object_key (Path | str): The base key prefix for the objects in the S3 bucket.
            list_files (list[Path | str]): List of local file paths to upload.

        Returns:
            tuple: A tuple containing the count of successful and failed uploads.

        Example:
            >>> s3_manager.upload_files('my-bucket', 'path/to/', ['/local/path/to/file1.txt', '/local/path/to/file2.txt'])
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

    def _upload_file_single(self, bucket_name: str, object_key: Path | str, local_path: Path | str) -> bool:
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
            file_size = os.path.getsize(local_path)
            with open(local_path, "rb") as f:
                self._with_progress_bar(
                    total=file_size,
                    desc=f"Uploading {local_path.name}",
                    file_operation=lambda callback: self.s3_client.upload_fileobj(
                        Fileobj=f,
                        Bucket=bucket_name,
                        Key=str(object_key),
                        Callback=callback
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False

    def list_files(self, bucket_name: str, prefix: str, file_extension: str = None) -> list[Path]:
        """
        Lists the files in an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix to filter the objects by.
            file_extension (str, optional): The file extension to filter the objects by.
                If not provided, all objects are returned.

        Returns:
            list[Path]: List of paths to files in the bucket.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix)
            if "Contents" not in response:
                logger.info(f"No objects found in folder: {prefix}")
                return []

            objects = [
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

    async def async_download_files(self, bucket_name: str, list_files: list[Path | str], local_base_path: Path | str, keep_structure=True):
        """
        Asynchronously downloads multiple files from an S3 bucket in parallel.

        Args:
            bucket_name (str): The name of the S3 bucket.
            list_files (list[Path | str]): List of S3 object keys to download.
            local_base_path (Path | str): The local directory to download files into.
            keep_structure (bool, optional): Whether to preserve the S3 directory structure locally.
                Defaults to True.

        Returns:
            tuple: A tuple containing the count of successful and failed downloads.
        """
        if isinstance(local_base_path, str):
            local_base_path = Path(local_base_path)

        list_files = self._normalize_paths(list_files)
        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        tasks = []
        async with session.client("s3") as s3_client:
            for file in list_files:
                tasks.append(
                    self._async_download_file_single(
                        s3_client, bucket_name, file, local_base_path, keep_structure, False)
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        failure_count = 0
        for result in results:
            if isinstance(result, Exception) or not result:
                failure_count += 1
            else:
                success_count += 1

        tqdm.write(
            f"\nAll files processed asynchronously. Downloaded to: {local_base_path}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count

    async def _async_download_file_single(self, s3_client, bucket_name: str, object_key: Path | str, local_path: Path | str, keep_structure: bool = True, verbose: bool = True) -> bool:
        """
        Asynchronously downloads a single file from an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path | str): The key of the object in the S3 bucket.
            local_path (Path | str): The local path to download the file to.
            keep_structure (bool): Whether to preserve the S3 directory structure locally.
                    If False, adds unique identifiers to avoid name conflicts. Defaults to True.
            verbose (bool): Whether to print the name of each downloaded file. Defaults to True.

        Returns:
            bool: True if the download was successful, False otherwise.
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

            object_info = await s3_client.head_object(Bucket=bucket_name, Key=str(object_key))
            file_size = object_info['ContentLength']

            with open(full_local_path, "wb") as f:
                await self._async_with_progress_bar(
                    total=file_size,
                    desc=f"Downloading {object_key}",
                    file_operation=lambda callback: s3_client.download_fileobj(
                        Bucket=bucket_name,
                        Key=str(object_key),
                        Fileobj=f,
                        Callback=callback
                    )
                )

            if verbose:
                tqdm.write(f"Downloaded: {object_key}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {object_key}: {e}")
            return False

    async def async_upload_files(self, bucket_name: str, base_object_key: Path | str, list_files: list[Path | str]):
        """
        Asynchronously uploads multiple files to an S3 bucket in parallel.

        Args:
            bucket_name (str): The name of the S3 bucket.
            base_object_key (Path | str): The base key prefix for the objects in the S3 bucket.
            list_files (list[Path | str]): List of local file paths to upload.

        Returns:
            tuple: A tuple containing the count of successful and failed uploads.
        """
        if isinstance(base_object_key, str):
            base_object_key = Path(base_object_key)

        list_files = self._normalize_paths(list_files)
        session = aioboto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        tasks = []
        async with session.client("s3") as s3_client:
            for file in list_files:
                tasks.append(
                    self._async_upload_file_single(
                        s3_client, bucket_name, base_object_key / file.name, file)
                )
            results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        failure_count = 0
        for result in results:
            if isinstance(result, Exception) or not result:
                failure_count += 1
            else:
                success_count += 1

        tqdm.write(
            f"\nAll files processed asynchronously. Uploaded to: {bucket_name}/{base_object_key}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count

    async def _async_upload_file_single(self, s3_client, bucket_name: str, object_key: Path | str, local_path: Path | str, verbose: bool = True) -> bool:
        """
        Asynchronously uploads a single file to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (Path | str): The key of the object in the S3 bucket.
            local_path (Path | str): The path to the local file to upload.
            verbose (bool): Whether to print the name of each uploaded file. Defaults to True.

        Returns:
            bool: True if the upload was successful, False otherwise.
        """
        if isinstance(local_path, str):
            local_path = Path(local_path)
        try:
            file_size = os.path.getsize(local_path)
            with open(local_path, "rb") as f:
                await self._async_with_progress_bar(
                    total=file_size,
                    desc=f"Uploading {local_path.name}",
                    file_operation=lambda callback: s3_client.upload_fileobj(
                        Fileobj=f,
                        Bucket=bucket_name,
                        Key=str(object_key),
                        Callback=callback
                    )
                )
            return True
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False

    async def hybrid_download_files(
        self,
        bucket_name: str,
        list_files: list[Path | str],
        local_base_path: Path | str,
        keep_structure: bool = True
    ):
        """
        Downloads multiple files from an S3 bucket in parallel using a mix of sync and async.

        Args:
            bucket_name (str): The name of the S3 bucket.
            list_files (list[Path | str]): List of S3 object keys to download.
            local_base_path (Path | str): The local directory to download files into.
            keep_structure (bool): Whether to preserve the S3 directory structure locally.
                If False, adds unique identifiers to avoid name conflicts. Defaults to True.

        Returns:
            tuple: A tuple containing the count of successful and failed downloads.
        """
        if isinstance(local_base_path, str):
            local_base_path = Path(local_base_path)

        list_files = self._normalize_paths(list_files)
        loop = asyncio.get_event_loop()
        tasks = []
        for file in list_files:
            tasks.append(loop.run_in_executor(
                None,
                self._download_file_single,
                bucket_name,
                file,
                local_base_path,
                keep_structure,
                False
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = 0
        failure_count = 0
        for result in results:
            if isinstance(result, Exception) or not result:
                failure_count += 1
            else:
                success_count += 1

        tqdm.write(
            f"\nHybrid download completed. Downloaded to: {local_base_path}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count

    async def hybrid_upload_files(
        self,
        bucket_name: str,
        base_object_key: Path | str,
        list_files: list[Path | str]
    ):
        """
        Uploads multiple files to an S3 bucket in parallel using a mix of sync and async.

        Args:
            bucket_name (str): The name of the S3 bucket.
            base_object_key (Path | str): The base key prefix for the objects in the S3 bucket.
            list_files (list[Path | str]): List of local file paths to upload.

        Returns:
            tuple: A tuple containing the count of successful and failed uploads.
        """
        if isinstance(base_object_key, str):
            base_object_key = Path(base_object_key)

        list_files = self._normalize_paths(list_files)
        loop = asyncio.get_event_loop()
        tasks = []
        for file in list_files:
            object_key = base_object_key / file.name
            tasks.append(loop.run_in_executor(
                None,
                self._upload_file_single,
                bucket_name,
                object_key,
                file
            ))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = 0
        failure_count = 0
        for result in results:
            if isinstance(result, Exception) or not result:
                failure_count += 1
            else:
                success_count += 1

        tqdm.write(
            f"\nHybrid upload completed. Uploaded to: {bucket_name}/{base_object_key}")
        tqdm.write(f"Successfully processed: {success_count} files")
        tqdm.write(f"Failed: {failure_count} files")

        return success_count, failure_count
