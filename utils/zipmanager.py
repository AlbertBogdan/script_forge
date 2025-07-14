import asyncio
import concurrent.futures
import logging
import os
import threading
import zipfile
from pathlib import Path

from tqdm.auto import tqdm

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class ZipManager:
    def __init__(self, zip_path):
        self.zip_path = Path(zip_path)
        self._lock = threading.Lock()
        self.archived_count = 0

    def create_zip(self):
        with self._lock:
            with zipfile.ZipFile(self.zip_path, "w"):
                pass

    def add_file(self, file_path: str | Path, arcname=None):
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")
        with self._lock:
            with zipfile.ZipFile(self.zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.write(file_path, arcname=arcname or file_path.name)
            self.archived_count += 1

    def add_folder(self, folder_path: str | Path, base_arcname: str | Path = None):
        folder_path = Path(folder_path)
        if not folder_path.is_dir():
            raise NotADirectoryError(f"Directory not found: {folder_path}")

        base_arcname = Path(base_arcname or folder_path.name)

        with self._lock:
            with zipfile.ZipFile(self.zip_path, "a", compression=zipfile.ZIP_DEFLATED) as zip_file:
                for file_path in folder_path.rglob("*"):
                    if file_path.is_file():
                        arcname = base_arcname / file_path.relative_to(folder_path)
                        zip_file.write(file_path, arcname)
                        self.archived_count += 1

    def add_items(self, items: list[Path | str]):
        tasks = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for item in items:
                path_item = Path(item)
                if path_item.is_file():
                    tasks.append(executor.submit(self.add_file, path_item))
                elif path_item.is_dir():
                    tasks.append(executor.submit(self.add_folder, path_item))
                else:
                    logger.warning(f"Path not found or not a file/folder: {path_item}")
            for future in tqdm(
                concurrent.futures.as_completed(tasks), total=len(tasks), desc="Adding items", unit="item"
            ):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing item: {e}")

    def extract_all(self, extract_path: str | Path):
        extract_path = Path(extract_path)
        with zipfile.ZipFile(self.zip_path, "r") as zip_file:
            members = zip_file.namelist()
            for member in tqdm(members, desc="Extracting files", unit="file"):
                zip_file.extract(member, extract_path)

    def list_contents(self):
        with zipfile.ZipFile(self.zip_path, "r") as zip_file:
            return zip_file.namelist()

    async def add_items_async(self, items):
        await asyncio.to_thread(self.add_items, items)

    async def extract_all_async(self, extract_path):
        await asyncio.to_thread(self.extract_all, extract_path)

    def get_archived_count(self):
        return self.archived_count


# Example usage:
# zip_manager = ZipManager("example.zip")
# zip_manager.add_file("test.txt")
# zip_manager.add_folder("my_folder")
# print(zip_manager.list_contents())
# zip_manager.extract_all("output_folder")
