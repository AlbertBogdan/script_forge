import json
from collections.abc import Generator
from pathlib import Path

from joblib import Parallel, delayed
from PIL import Image
from toolbox.api_clients.base.client_worker_pool import ClientWorkerPool
from toolbox.ocr.textbox import Textbox
from tqdm.auto import tqdm

from utils.ocr.project_report import ProjectReport


class DocumentsProcessor:
    def __init__(
        self,
        path2doc: str | Path,
        doc_ocr_client: ClientWorkerPool | None = None,
        symb_ocr_client: ClientWorkerPool | None = None,
    ):
        self.project = ProjectReport(path2doc)
        self.doc_ocr_client = doc_ocr_client
        self.doc_symb_client = symb_ocr_client

    def extract_images(self):
        for file_info in tqdm(self.project, desc="Collecting docs"):
            if file_info.pages is not None:
                continue

            if not file_info.local_path.exists():
                print(f"FILE NOT FOUND!!! {file_info.local_path} !")
                continue

        Parallel(n_jobs=-1, backend="loky", batch_size=1)(
            delayed(self.project._extract_image)(file_component)
            for file_component in tqdm(self.project, desc="Converting to PNGs")
        )

    def ensure_all_images(self):
        def generate_png_paths():
            for file_info in self.project:
                if file_info.pages is None:
                    continue
                for page_info in file_info.pages:
                    yield page_info.image_path

        def ensure_image(png_path: str | Path):
            png_path = Path(png_path)

            try:
                Image.open(png_path).load()
            except Exception as e:
                print(f"!!! {type(e)}: {e}, {str(png_path)}")
                png_path.unlink()

        all_png_paths = list(generate_png_paths())
        Parallel(n_jobs=-1, backend="threading", batch_size=10)(
            delayed(ensure_image)(path) for path in tqdm(all_png_paths, desc="Ensure images")
        )

    def _is_valid_json(self, file_path: Path):
        try:
            with file_path.open("r") as file:
                json.load(file)
            return True
        except Exception as e:
            print(f"{type(e)}: {e} {file_path}")
            file_path.unlink()
            return False

    def run_ocr(self):
        files_data: list[tuple[Path, Path]] = [
            (page_info.image_path, page_info.ocr_path)
            for file_info in self.project
            if file_info.pages is not None
            for page_info in file_info.pages
            if not (page_info.ocr_path.exists() and self._is_valid_json(page_info.ocr_path))
        ]

        files2ocr, out_paths = zip(*files_data) if files_data else ([], [])

        if not files2ocr:
            return
        if self.doc_ocr_client is None:
            print("SKIPPING OCR!!!")
            return

        gen: Generator[list[Textbox], None, None] = self.doc_ocr_client.call_batch(files2ocr)
        for ocr_result_path, res in tqdm(zip(out_paths, gen), total=len(out_paths), desc="Ocring"):
            ocr_result = [tb.model_dump(exclude="source_image") for tb in res]
            with ocr_result_path.open("w+") as f:
                json.dump(ocr_result, f)

    def run_symbols(self):
        files_data: list[tuple[Path, Path]] = [
            (page_info.image_path, page_info.symbols_path)
            for file_info in self.project
            if file_info.pages is not None
            for page_info in file_info.pages
            if not (page_info.symbols_path.exists() and self._is_valid_json(page_info.symbols_path))
        ]

        files2symbol, out_paths = zip(*files_data) if files_data else ([], [])

        if not files2symbol:
            return
        if self.doc_symb_client is None:
            print("SKIPPING OBJECT DETECTION!!!")
            return

        gen: Generator[list, None, None] = self.doc_symb_client.call_batch(files2symbol)
        for symb_result_path, res in tqdm(zip(out_paths, gen), total=len(out_paths), desc="Object detection"):
            symb_result = [en.model_dump(exclude="source_image") for en in res]
            with symb_result_path.open("w+") as f:
                json.dump(symb_result, f)

    def run_all(self):
        self.extract_images()
        self.ensure_all_images()
        self.run_ocr()
        self.run_symbols()
