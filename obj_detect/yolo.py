from sahi import AutoDetectionModel
from sahi.predict import get_sliced_prediction
from PIL import Image as pil_image
from IPython.display import Image, display
from pathlib import Path
import subprocess


class SAHIYoloHandler:
    def __init__(self, model_path: str, device: str = "cuda:0", confidence_threshold: float = 0.5):
        self.model_path = model_path
        self.device = device
        self.confidence_threshold = confidence_threshold
        self.detection_model = None

    def load_model(self):
        self.detection_model = AutoDetectionModel.from_pretrained(
            model_type="yolov8",
            model_path=self.model_path,
            confidence_threshold=self.confidence_threshold,
            device=self.device,
        )

    def predict_with_slicing(
        self, image_path: str, slice_height: int = 1024, slice_width: int = 1024, overlap_ratio: float = 0.2
    ) -> int:
        if self.detection_model is None:
            raise ValueError("Model is not loaded. Call 'load_model()' first.")

        img = pil_image.open(image_path).convert("RGB")
        result = get_sliced_prediction(
            img,
            self.detection_model,
            slice_height=slice_height,
            slice_width=slice_width,
            overlap_height_ratio=overlap_ratio,
            overlap_width_ratio=overlap_ratio,
        )

        num_objects = len(result.object_prediction_list)
        return num_objects

    def visualize_predictions(self, image_path: str, save_path: str):
        if self.detection_model is None:
            raise ValueError("Model is not loaded. Call 'load_model()' first.")

        img = pil_image.open(image_path).convert("RGB")
        result = get_sliced_prediction(
            img,
            self.detection_model,
            slice_height=1024,
            slice_width=1024,
            overlap_height_ratio=0.2,
            overlap_width_ratio=0.2,
        )

        result.export_visuals(export_dir=Path(save_path).parent)
        display(Image(filename=save_path))


class YOLOTrainer:
    def __init__(self, model_path: str, data_yaml: str, device: str = "cuda:0"):
        self.model_path = model_path
        self.data_yaml = data_yaml
        self.device = device

    def train(
        self,
        epochs: int = 100,
        batch_size: int = 16,
        img_size: int = 640,
        optimizer: str = "SGD",
        save_dir: str = "./runs/train",
    ):
        command = [
            "yolo",
            "task=detect",
            "mode=train",
            f"model={self.model_path}",
            f"data={self.data_yaml}",
            f"epochs={epochs}",
            f"batch={batch_size}",
            f"imgsz={img_size}",
            f"optimizer={optimizer}",
            f"device={self.device}",
            f"save_dir={save_dir}",
        ]

        print(f"Running training command: {' '.join(command)}")
        subprocess.run(command, check=True)

    def validate(self, save_dir: str):
        command = [
            "yolo",
            "task=detect",
            "mode=val",
            f"model={self.model_path}",
            f"data={self.data_yaml}",
            f"device={self.device}",
            f"save_dir={save_dir}",
        ]

        print(f"Running validation command: {' '.join(command)}")
        subprocess.run(command, check=True)

    def predict(self, image_path: str, conf: float = 0.25, save_dir: str = "./runs/predict"):
        command = [
            "yolo",
            "task=detect",
            "mode=predict",
            f"model={self.model_path}",
            f"source={image_path}",
            f"conf={conf}",
            f"device={self.device}",
            f"save_dir={save_dir}",
        ]

        print(f"Running prediction command: {' '.join(command)}")
        subprocess.run(command, check=True)
