import json
from pathlib import Path


class JSONLoader:
    def __init__(self, file_path=None):
        self.file_path = file_path
        self.data = None

    def load(self):
        if not self.file_path:
            raise ValueError("File path is not specified.")
        with Path.open(self.file_path, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        return self.data

    def save(self, data, file_path=None):
        path = file_path if file_path else self.file_path
        if not path:
            raise ValueError("File path is not specified.")
        with Path.open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)


class COCOHandler(JSONLoader):
    def __init__(self, file_path=None):
        super().__init__(file_path)

    def get_images(self):
        if self.data is None:
            raise ValueError("Data is not loaded.")
        return self.data.get("images", [])

    def get_annotations(self):
        if self.data is None:
            raise ValueError("Data is not loaded.")
        return self.data.get("annotations", [])

    def get_categories(self):
        if self.data is None:
            raise ValueError("Data is not loaded.")
        return self.data.get("categories", [])

    def add_image(self, image):
        if "images" not in self.data:
            self.data["images"] = []
        self.data["images"].append(image)

    def add_annotation(self, annotation):
        if "annotations" not in self.data:
            self.data["annotations"] = []
        self.data["annotations"].append(annotation)

    def add_category(self, category):
        if "categories" not in self.data:
            self.data["categories"] = []
        self.data["categories"].append(category)


class YOLOHandler:
    def __init__(self, file_path=None):
        self.file_path = file_path
        self.data = []

    def load(self):
        if not self.file_path:
            raise ValueError("File path is not specified.")
        with Path.open(self.file_path, "r", encoding="utf-8") as f:
            self.data = [line.strip() for line in f.readlines()]
        return self.data

    def save(self, file_path=None):
        path = file_path if file_path else self.file_path
        if not path:
            raise ValueError("File path is not specified.")
        with Path.open(path, "w", encoding="utf-8") as f:
            f.writelines([line + "\n" for line in self.data])

    def parse_line(self, line):
        parts = line.split()
        return {"class_id": int(parts[0]), "bbox": [float(v) for v in parts[1:]]}

    def format_line(self, class_id, bbox):
        return f"{class_id} " + " ".join(f"{v:.6f}" for v in bbox)

    def add_annotation(self, class_id, bbox):
        line = self.format_line(class_id, bbox)
        self.data.append(line)

    def get_annotations(self):
        return [self.parse_line(line) for line in self.data]
