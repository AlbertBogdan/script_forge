import sys
import traceback
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pymupdf
from PIL import Image
from toolbox.pdf import get_pdf_images


@dataclass
class PageComponent:
    image_path: Path
    page_no: int
    ocr_path: Path | None = None
    symbols_path: Path | None = None


@dataclass
class FileComponent:
    local_path: Path
    blob_path: Path = None
    pages: tuple[PageComponent, ...] | None = None


class ProcessFileSystem:
    def __init__(self, images_dir: Path, ocr_dir: Path, symbols_dir: Path):
        self.images_dir = images_dir
        self.ocr_dir = ocr_dir
        self.symbols_dir = symbols_dir

    def can_handle(self, file_comp: FileComponent) -> bool:
        raise NotImplementedError

    def process(self, file_comp: FileComponent) -> None:
        raise NotImplementedError

    def extract_image(self, file_comp: FileComponent) -> None:
        raise NotImplementedError


class PdfSystem(ProcessFileSystem):
    SUPPORTED_EXTS = {".pdf"}

    def can_handle(self, file_comp: FileComponent) -> bool:
        return file_comp.local_path.suffix.lower() in self.SUPPORTED_EXTS

    def process(self, file_comp: FileComponent) -> None:
        if not file_comp.local_path.exists():
            print(f"MISSING PDF: {file_comp.local_path}", file=sys.stderr)
            return

        try:
            doc = pymupdf.open(file_comp.local_path)
            page_count = len(doc)
            doc.close()
        except (pymupdf.EmptyFileError, pymupdf.FileDataError) as e:
            print(f"PDF ERROR: {type(e).__name__} - {file_comp.local_path}", file=sys.stderr)
            return

        stem = file_comp.local_path.stem
        pages = []

        for page_no in range(page_count):
            img_path = self.images_dir / f"{stem}___page_{page_no}.png"
            if not img_path.exists():
                print(f"Missing image for page {page_no}: {img_path}", file=sys.stderr)
                return

            pages.append(
                PageComponent(
                    image_path=img_path,
                    page_no=page_no,
                    ocr_path=self.ocr_dir / f"{img_path.stem}_ocr.json",
                    symbols_path=self.symbols_dir / f"{img_path.stem}_symbols.json",
                )
            )

        file_comp.pages = tuple(pages)

    def extract_image(self, file_comp: FileComponent) -> None:
        pages_to_generate = []

        page_paths = {}
        local_path = file_comp.local_path
        images_dir = self.images_dir
        with pymupdf.open(local_path) as doc:
            total_pages = len(doc)
            for page_num in range(total_pages):
                img_path = images_dir / f"{local_path.stem}___page_{page_num}.png"
                page_paths[page_num] = str(img_path)

                if img_path.exists():
                    try:
                        Image.open(img_path)
                    except Exception as e:
                        print(f"Corrupted image found, will regenerate. Error: {type(e)}: {e}, Path: {img_path}")
                        img_path.unlink()
                        pages_to_generate.append(page_num)

                if not img_path.exists():
                    pages_to_generate.append(page_num)

        if pages_to_generate:
            try:
                generated_images = get_pdf_images(str(local_path), pages_to_generate)

                for page_num, img in zip(pages_to_generate, generated_images):
                    img_path_to_save = Path(page_paths[page_num])

                    img.save(img_path_to_save, format="PNG")

            except Exception as e:
                log_path = Path(f"./{type(e).__name__}.log")
                with log_path.open("a") as file:
                    file.write(traceback.format_exc())
                    file.write(f"Document: {local_path}, Failed Pages: {pages_to_generate}\n")
                print(f"An error occurred during batch generation. See {log_path.resolve()} for details.")
                raise e


class ImageSystem(ProcessFileSystem):
    SUPPORTED_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".tiff"}

    def can_handle(self, file_comp: FileComponent) -> bool:
        return file_comp.local_path.suffix.lower() in self.SUPPORTED_EXTS

    def process(self, file_comp: FileComponent) -> None:
        img_path = self.images_dir / f"{file_comp.local_path.stem}___page_0.png"

        if not img_path.exists():
            print(f"Missing image file: {img_path}", file=sys.stderr)
            return

        file_comp.pages = (
            PageComponent(
                image_path=img_path,
                page_no=0,
                ocr_path=self.ocr_dir / f"{img_path.stem}_ocr.json",
                symbols_path=self.symbols_dir / f"{img_path.stem}_symbols.json",
            ),
        )

    def extract_image(self, file_comp: FileComponent) -> None:
        img_path = self.images_dir / f"{file_comp.local_path.stem}___page_0.png"
        file_comp.pages = (
            PageComponent(
                image_path=img_path,
                page_no=0,
                ocr_path=self.ocr_dir / f"{img_path.stem}_ocr.json",
                symbols_path=self.symbols_dir / f"{img_path.stem}_symbols.json",
            ),
        )

        if img_path.exists():
            try:
                Image.open(img_path)
                return
            except Exception as e:
                print(
                    f"Corrupted PNG found, will regenerate. Error: {type(e).__name__}: {e}, Path: {img_path}",
                    file=sys.stderr,
                )
                img_path.unlink()

        try:
            with Image.open(file_comp.local_path) as img:
                img.save(img_path, format="PNG")
        except Exception as e:
            print(f"Failed to convert {file_comp.local_path} to PNG. Error: {type(e).__name__}: {e}", file=sys.stderr)


class IgnoreSystem(ProcessFileSystem):
    def can_handle(self, file_comp: FileComponent) -> bool:
        return False

    def process(self, file_comp: FileComponent) -> None:
        print(f"Ignoring unsupported file type: {file_comp.local_path}", file=sys.stderr)

    def extract_image(self, file_comp: FileComponent) -> None:
        print(f"Ignoring unsupported file type: {file_comp.local_path}", file=sys.stderr)


class ExcelSystem(ProcessFileSystem):
    SUPPORTED_EXTS = {".xlsx", ".xls"}

    def can_handle(self, file_comp: FileComponent) -> bool:
        return False

    def process(self, file_comp: FileComponent) -> None:
        pass

    def extract_image(self, file_comp: FileComponent) -> None:
        pass


class ProjectReport:
    def __init__(self, root_path: Path | str, files_dict: dict[str, Path] = {}, name_try: str = "") -> None:
        self.files_dict = files_dict
        self.root_path = Path(root_path).resolve()
        self.name_suffix = f"_{name_try}" if name_try else ""
        self.projects_cache = Path("data")

        self._setup_directories()
        self.file_paths = list(self._collect_files())
        self.systems = self._register_systems()

    def _setup_directories(self) -> None:
        self.images_dir = self.projects_cache / "images_compressed"
        self.ocr_dir = self.projects_cache / f"text_recognition_json{self.name_suffix}"
        self.symbols_dir = self.projects_cache / f"symbols_detection_json{self.name_suffix}"

        self.images_dir.mkdir(parents=True, exist_ok=True)
        self.ocr_dir.mkdir(parents=True, exist_ok=True)
        self.symbols_dir.mkdir(parents=True, exist_ok=True)

    def _collect_files(self) -> Iterator[Path]:
        for path in self.root_path.rglob("*"):
            if path.is_file():
                yield path

    def _register_systems(self) -> list[ProcessFileSystem]:
        return [
            PdfSystem(self.images_dir, self.ocr_dir, self.symbols_dir),
            ImageSystem(self.images_dir, self.ocr_dir, self.symbols_dir),
            ExcelSystem(self.images_dir, self.ocr_dir, self.symbols_dir),
            IgnoreSystem(self.images_dir, self.ocr_dir, self.symbols_dir),
        ]

    def __len__(self) -> int:
        return len(self.file_paths)

    def __getitem__(self, index: int) -> FileComponent:
        file_path = self.file_paths[index]
        blob_path = self.files_dict.get(f"{file_path.stem}.pdf", None)
        file_comp = FileComponent(local_path=file_path, blob_path=blob_path)
        self._process_entity(file_comp)
        return file_comp

    def __iter__(self) -> Iterator[FileComponent]:
        for i in range(len(self)):
            yield self[i]

    def _process_entity(self, entity: FileComponent) -> None:
        for system in self.systems:
            if system.can_handle(entity):
                system.process(entity)
                return

    def _extract_image(self, entity: FileComponent) -> None:
        for system in self.systems:
            if system.can_handle(entity):
                system.extract_image(entity)
                return
