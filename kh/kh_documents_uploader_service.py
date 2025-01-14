from dataclasses import dataclass
from datetime import datetime
import tempfile
import os

from bson import ObjectId
from dexlib.tools.kh2_uploader.kh2_uploader import Kh2Uploader
from dexlib.tools.kh2_exporter.kh2_exporter import Kh2Exporter
from dexlib.tools.kh2_exporter.kh2_exporter import (
    Kh2Record,
)


@dataclass
class KhUploaderConfig:
    kh2_url: str
    kh2_user_email: str
    kh2_user_password: str
    kh2_project_id: str


class KhUploaderService:
    def __init__(self, config: KhUploaderConfig):
        self.config = config

    def upload(self, kh2_records: list[Kh2Record]) -> ObjectId:
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = os.path.join(tmp_dir, "kh2_export-tmp.zip")
            Kh2Exporter().export(kh2_records=kh2_records, output_zip_path=zip_path)

            kh2_uploader = Kh2Uploader(
                api_url=self.config.kh2_url + '/api/v1',
                user_email=self.config.kh2_user_email,
                user_password=self.config.kh2_user_password,
                project_id=self.config.kh2_project_id,
            )
            run_name = f'run-{datetime.now().strftime("%d/%m/%Y %H%M%S")}'
            uploaded_run_id = kh2_uploader.upload_run(zip_path, run_name)
            kh2_link = (
                f"{self.config.kh2_url}/user_view/explore_results/{uploaded_run_id}"
            )
            print(kh2_link)

            return ObjectId(uploaded_run_id)
