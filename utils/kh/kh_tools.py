from typing import Optional, Any
from pathlib import Path
from datetime import datetime
from dexlib.tools.kh2_exporter.kh2_exporter import (
    Kh2Record,
    Kh2Annotation,
    Kh2Attribute,
    Kh2Object,
    Kh2Bbox,
    Kh2Exporter,
    Kh2ObjectMetadata,
    FileImageProvider,
)
from dexlib.tools.kh2_uploader.kh2_uploader import Kh2Uploader


class KH2Config:
    def __init__(
        self,
        api_url: str,
        project_id: str,
        email: str,
        password: str,
        default_fields: Optional[list[Kh2Attribute]] = None,
    ):
        self.api_url = api_url
        self.project_id = project_id
        self.email = email
        self.password = password
        self.default_fields = default_fields or []


class KH2Manager:
    def __init__(self, config: KH2Config):
        self.config = config
        self.records: list[Kh2Record] = []
        self.uploader = self._init_uploader()

    def _init_uploader(self) -> Kh2Uploader:
        return Kh2Uploader(
            api_url=self.config.api_url,
            user_email=self.config.email,
            user_password=self.config.password,
            project_id=self.config.project_id,
        )

    def create_record(
        self,
        image_path: str,
        source_file: str,
        page_number: int = 0,
        custom_fields: Optional[list[Kh2Attribute]] = None,
    ) -> Kh2Record:
        record = Kh2Record(
            image_provider=FileImageProvider(image_path),
            annotation=Kh2Annotation(fields=self.config.default_fields + (custom_fields or []), objects=[]),
            file_path=source_file,
            page_number=page_number,
        )
        self.records.append(record)
        return record

    def add_flange_annotation(
        self, record: Kh2Record, bbox: list[float], metadata: Optional[dict[str, Any]] = None
    ) -> Kh2Object:
        obj = Kh2Object(
            text="flange-joint", label="flanges", bbox=Kh2Bbox(*bbox), metadata=metadata or Kh2ObjectMetadata()
        )
        record.annotation.objects.append(obj)
        return obj

    def add_text_annotation(
        self, record: Kh2Record, text: str, bbox: list[float], metadata: Optional[dict[str, Any]] = None
    ) -> Kh2Object:
        obj = Kh2Object(text=text, label="text", bbox=Kh2Bbox(*bbox), metadata=metadata or Kh2ObjectMetadata())
        record.annotation.objects.append(obj)
        return obj

    def export_to_zip(self, output_dir: str = "exports") -> str:
        Path(output_dir).mkdir(exist_ok=True)
        zip_path = str(Path(output_dir) / f"kh2_export_{datetime.now().strftime('%Y%m%d_%H%M')}.zip")
        Kh2Exporter().export(self.records, zip_path)
        return zip_path

    def upload_to_kh2(self, zip_path: str, run_name: Optional[str] = None) -> str:
        if not run_name:
            run_name = f"Run {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        return self.uploader.upload_run(zip_path, run_name)


class KH2Utilities:
    @staticmethod
    def create_bbox_from_points(points: list[list[float]]) -> list[float]:
        x_coords = [p[0] for p in points]
        y_coords = [p[1] for p in points]
        return [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]

    @staticmethod
    def convert_yolo_to_kh2(yolo_results: list[dict], image_size: tuple) -> list[Kh2Object]:
        objects = []
        for result in yolo_results:
            bbox = KH2Utilities.create_bbox_from_points(result["box"])
            obj = Kh2Object(
                text=result.get("text", ""),
                label=result.get("label", "object"),
                bbox=Kh2Bbox(*bbox),
                metadata=Kh2ObjectMetadata(),
            )
            objects.append(obj)
        return objects
