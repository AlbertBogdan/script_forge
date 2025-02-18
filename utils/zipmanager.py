import zipfile
import logging
import os
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZipManager:
    def __init__(self, zip_path):
        self.zip_path = Path(zip_path)

    def create_zip(self):
        with zipfile.ZipFile(self.zip_path, 'w') as zip_file:
            pass

    def add_file(self, file_path, arcname=None):
        """
        Adds a file to the ZIP archive.

        :param file_path: Path to the file to add.
        :param arcname: Name to use inside the archive (optional).
        """
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"File not found: {file_path}")

        with zipfile.ZipFile(self.zip_path, 'a', compression=zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(file_path, arcname=arcname or file_path.name)

    def add_folder(self, folder_path, base_arcname=None):
        """
        Recursively adds a folder and its contents to the ZIP archive.

        :param folder_path: Path to the folder to add.
        :param base_arcname: Base name to use inside the archive (optional).
        """
        folder_path = Path(folder_path)
        if not folder_path.is_dir():
            raise NotADirectoryError(f"Directory not found: {folder_path}")

        with zipfile.ZipFile(self.zip_path, 'a', compression=zipfile.ZIP_DEFLATED) as zip_file:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = Path(root) / file
                    arcname = (Path(base_arcname) / file_path.relative_to(folder_path)) if base_arcname else file_path.relative_to(folder_path)
                    zip_file.write(file_path, arcname)

    def extract_all(self, extract_path):
        """
        Extracts all contents of the ZIP archive to a specified directory.

        :param extract_path: Path to the directory where files will be extracted.
        """
        extract_path = Path(extract_path)
        with zipfile.ZipFile(self.zip_path, 'r') as zip_file:
            zip_file.extractall(extract_path)
            logger.info(f"Files was extract to {extract_path}")

    def list_contents(self):
        with zipfile.ZipFile(self.zip_path, 'r') as zip_file:
            return zip_file.namelist()

# Example usage:
# zip_manager = ZipManager("example.zip")
# zip_manager.create_zip()
# zip_manager.add_file("test.txt")
# zip_manager.add_folder("my_folder")
# print(zip_manager.list_contents())
# zip_manager.extract_all("output_folder")
