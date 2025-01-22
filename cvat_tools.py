from toolbox.cvat.datumaro import make_datumaro_ocr_dataset
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from pathlib import Path
from typing import Tuple
from cvat_sdk import make_client
from toolbox.cvat.cvat import make_cvat_dataset, upload_dataset

class CVATUtilities:
    def __init__(self, host: str, port: str, credentials: Tuple[str, str]):
        self.client = make_client(
            host=host, port=port, credentials=credentials)
        self.tmp_dir = TemporaryDirectory()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tmp_dir.cleanup()
        self.client.close()

    def create_dataset(
        self,
        dataset,
        output_dir: str,
        text_attribute: str = "text"
    ) -> Path:
        datumaro_dataset = make_datumaro_ocr_dataset(
            dataset=dataset,
            text_attribute_name=text_attribute
        )

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        ds_name = f"cvat_dataset_{timestamp}"
        output_path = Path(output_dir) / ds_name

        make_cvat_dataset(
            dataset=datumaro_dataset,
            ds_name=ds_name,
            out_folder=str(output_path)
        )

        return output_path

    def upload_dataset(
        self,
        dataset_path: Path,
        project_id: int,
        task_name: str,
        dataset_format: str = "CVAT 1.1"
    ) -> None:
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")

        upload_dataset(
            task_name=task_name,
            client=self.client,
            dataset_path=str(dataset_path),
            project_id=project_id,
            dataset_format=dataset_format
        )
