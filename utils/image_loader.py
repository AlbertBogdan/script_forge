from pathlib import Path
from toolbox.pdf import read_pages

class ImageLoader:
    def __init__(self, image_dir: str):
        self.image_dir = Path(image_dir)
        self.image_files = sorted(self.image_dir.glob("*.jpg")) + sorted(self.image_dir.glob("*.png"))

    def get_image_path(self, idx: int) -> str:
        return str(self.image_files[idx])

    def __len__(self) -> int:
        return len(self.image_files)


class ImageLoader_Pdf(ImageLoader):
    def __init__(self, image_dir):
        self.image_dir = Path(image_dir)
        self.image_files = sorted(self.image_dir.glob("*.pdf"))

    def read_pdf(self, idx: int):
        return read_pages(str(self.image_files[idx]))
