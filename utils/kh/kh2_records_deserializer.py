import os
from dexlib.tools.kh2_exporter.kh2_exporter import (
    Kh2Annotation,
    ImageProvider,
    Kh2Record,
    Kh2Exporter,
    Kh2PageData,
    Kh2IndexData,
    Kh2DocumentData,
    FileImageProvider,
)


class Kh2RecordsDeserializer:
    def deserialize_from_dir(self, input_dir_path: str) -> list[Kh2Record]:
        with open(os.path.join(input_dir_path, "index.json")) as f:
            kh2_index = Kh2IndexData.parse_raw(f.read())
        records = []
        for document in kh2_index.documents:
            for page in document.pages:
                cur_dir_path = os.path.join(input_dir_path, "pages", page.dir)
                with open(os.path.join(cur_dir_path, "annotation.json")) as f:
                    annotation = Kh2Annotation.parse_raw(f.read())
                image_provider = FileImageProvider(
                    image_file_path=os.path.join(cur_dir_path, "image.png")
                )
                record = Kh2Record(
                    file_path=document.file_name,
                    page_number=page.page_number,
                    annotation=annotation,
                    image_provider=image_provider,
                    upload_image_key=None,
                )
                records.append(record)
        return records
