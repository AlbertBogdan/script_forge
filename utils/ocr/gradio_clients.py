import numpy as np

from PIL import Image

import base64
from io import BytesIO

from toolbox.image import encode_pil_to_base64

from toolbox.pdf import read_pages
from pathlib import Path


from joblib import Parallel, delayed
from tqdm.auto import tqdm

from gradio_client import Client
from more_itertools import chunked
from toolbox.ocr.textbox import Textbox
import math
from shapely.affinity import rotate
from shapely import Polygon
from toolbox.image import properly_convert_to_rgb
import requests
from dataclasses import dataclass
from typing import Generator, List, Dict, Any


def convert2image(pdf_path, page, project_name):
    image = next(read_pages(filepath=pdf_path, pages_ids=[page]))
    Path(f"data/converted/{project_name}").mkdir(exist_ok=True)
    image.save(f"data/converted/{project_name}/{Path(pdf_path).name.split('.')[0]}_{page}.png")

    return image


def crop_text_box_new(image, rect) -> Image.Image:
    # Calculate the minimum rotated rectangle
    min_rect = rect

    # Calculate the angle of rotation and the center of the box
    angle = math.degrees(
        math.atan2(
            min_rect.exterior.coords[1][1] - min_rect.exterior.coords[0][1],
            min_rect.exterior.coords[1][0] - min_rect.exterior.coords[0][0],
        )
    )

    angle = (angle + 45) % 90 - 45

    center = min_rect.centroid.coords[0]

    box_rotated = rotate(min_rect, -angle, origin=center)
    roi_bbox = min_rect.union(box_rotated)
    roi = image.crop(roi_bbox.bounds)
    # account for roi offset
    roi_center = (center[0] - roi_bbox.bounds[0], center[1] - roi_bbox.bounds[1])
    roi_rotated = roi.rotate(
        angle, center=roi_center, resample=Image.BICUBIC, fillcolor=(255, 255, 255)
    )

    minx, miny, maxx, maxy = box_rotated.bounds

    # min point index by x - y
    min_point_idx = np.argmin(
        np.array(box_rotated.exterior.coords)[:4, 0] - np.array(box_rotated.exterior.coords)[:4, 1]
    )

    # subtract roi offset
    minx -= roi_bbox.bounds[0]
    miny -= roi_bbox.bounds[1]
    maxx -= roi_bbox.bounds[0]
    maxy -= roi_bbox.bounds[1]

    # Crop the box from the rotated image
    image_cropped = roi_rotated.crop((minx, miny, maxx, maxy))

    return image_cropped


def pil_image_to_base64(image: Image) -> str:
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue())
    return img_str.decode("utf-8")


class TextDetectorApiClientGradio:
    def __init__(self, api_url: str):
        self.client = Client(api_url, ssl_verify=False)

    def detect_text(self, image: Image.Image) -> list[Textbox]:
        image_encoded = encode_pil_to_base64(image)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                api_result = self.client.predict(img={"url": image_encoded}, api_name="/predict")
                textboxes = [
                    Textbox(box=cont, source_image=image) for cont in api_result["contours"]
                ]
                return textboxes

            except Exception as e:
                if attempt < max_retries - 1:  # i.e. if it's not the last attempt
                    print(f"Failed to recognize text due to {str(e)}. Retrying...")
                    continue
                else:
                    raise

        raise Exception("Failed to recognize text after multiple retries")


class ImageTextRecognizerApiClientParseq:
    def __init__(self, api_url: str, max_concurrent_requests: int = 64):
        self.max_concurrent_requests = max_concurrent_requests
        self.client = Client(api_url, max_workers=max_concurrent_requests + 10, ssl_verify=False)

    def recognize(self, images: list[Image.Image]) -> list[Textbox]:
        encoded_imgs = [encode_pil_to_base64(img) for img in images]
        results = []

        with tqdm(total=len(encoded_imgs), desc="Recognizing images") as tqdm_bar:
            for chunk in chunked(encoded_imgs, self.max_concurrent_requests):
                jobs = [
                    self.client.submit(images={"url": e_img}, api_name="/predict")
                    for e_img in chunk
                ]
                for j in jobs:
                    results.append(Textbox.model_validate(j.result()))
                    tqdm_bar.update(1)

        for tb, img in zip(results, images):
            tb.source_image = img

        return results

    def recognize_one(self, image: Image.Image) -> Textbox:
        image_encoded = encode_pil_to_base64(image)
        # print(image_encoded)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = self.client.predict(images={"url": image_encoded}, api_name="/predict")
                return Textbox(**result)

            except Exception as e:
                if attempt < max_retries - 1:  # i.e. if it's not the last attempt
                    print(f"Failed to recognize text due to {str(e)}. Retrying...")
                    continue
                else:
                    raise

        raise Exception("Failed to recognize text after multiple retries")

    def recognize_many(self, textboxes: list[Textbox]) -> list[Textbox]:
        img_boxes = [crop_text_box_new(t.source_image, Polygon(t.box)) for t in textboxes]
        # for box in img_boxes:
        #     display(box)
        encoded_imgs = [encode_pil_to_base64(img) for img in img_boxes]
        results = []

        with tqdm(total=len(encoded_imgs), desc="Recognizing text boxes") as tqdm_bar:
            for chunk in chunked(zip(encoded_imgs, textboxes), self.max_concurrent_requests):
                jobs = [
                    self.client.submit(images={"url": e_img}, api_name="/predict")
                    for e_img, _ in chunk
                ]
                for j, (_, tb) in zip(jobs, chunk):
                    res_box = Textbox(**j.result(), source_image=tb.source_image)
                    out_cnt = res_box.box
                    init_cnt = tb.box
                    while not np.allclose(out_cnt[0], [0, 0]):
                        out_cnt = np.roll(out_cnt, -1, axis=0)
                        init_cnt = np.roll(init_cnt, 1, axis=0)
                    res_box.box = init_cnt
                    results.append(res_box)
                    tqdm_bar.update(1)

        return results


class DocumentOcrApiClient:
    def __init__(self, api_url: str, max_concurrent_requests: int = 4):
        self.max_concurrent_requests = max_concurrent_requests
        self.client = Client(api_url, max_workers=max_concurrent_requests + 10, ssl_verify=False)

    def recognize_one(self, img_or_path: Image.Image | str):
        # FOR DEBUGGING!
        if isinstance(img_or_path, Image.Image):
            img = img_or_path
        elif isinstance(img_or_path, str):
            img = Image.open(img_or_path)
            print(np.array(img).shape)
            img = properly_convert_to_rgb(img)
            img.load()
        else:
            raise AttributeError(f"Unsupported type: {type(img_or_path)}")
        
        print(np.array(img).shape)
        encoded = encode_pil_to_base64(img)
        result = self.client.predict(images={"url": encoded}, api_name="/predict")
        return [Textbox.model_validate(tb) for tb in result]

    def recognize(self, images: list[Image.Image | str]) -> Generator[list[Textbox], None, None]:
        def recognize_one(img_or_path: Image.Image | str):
            if isinstance(img_or_path, Image.Image):
                img = img_or_path
            elif isinstance(img_or_path, str):
                img = Image.open(img_or_path)
                img = properly_convert_to_rgb(img)
                img.load()
            else:
                raise AttributeError(f"Unsupported type: {type(img_or_path)}")
            encoded = encode_pil_to_base64(img)
            img = properly_convert_to_rgb(img)
            result = self.client.predict(images={"url": encoded}, api_name="/predict")
            return [Textbox.model_validate(tb) for tb in result]

        return Parallel(
            n_jobs=self.max_concurrent_requests,
            backend="threading",
            return_as="generator",
            batch_size=1,
        )(delayed(recognize_one)(img) for img in tqdm(images, desc="Recognizing docs"))




def detect_symbols(image: Image, API_URL: str = "http://10.1.0.6:7778"):
    image_encoded = encode_pil_to_base64(image)
    client = Client(API_URL, ssl_verify=False, verbose=False)
    api_result = client.predict(img={'url': image_encoded}, api_name="/predict")

    return api_result


@dataclass
class OCRBox:
    x1: float
    y1: float
    x2: float
    y2: float
    text: str


class OCRProcessor:
    @staticmethod
    def convert_ocr_data(ocr_data: List[Dict[str, Any]]) -> List[OCRBox]:
        return [
            OCRBox(
                min(
                    min(data["box"][0][0], data["box"][1][0]),
                    min(data["box"][2][0], data["box"][3][0]),
                ),
                min(
                    min(data["box"][0][1], data["box"][1][1]),
                    min(data["box"][2][1], data["box"][3][1]),
                ),
                max(
                    max(data["box"][0][0], data["box"][1][0]),
                    max(data["box"][2][0], data["box"][3][0]),
                ),
                max(
                    max(data["box"][0][1], data["box"][1][1]),
                    max(data["box"][2][1], data["box"][3][1]),
                ),
                data["text"],
            )
            for data in ocr_data
        ]

    @staticmethod
    def convert_symbols_data(symbols_data: List[Dict[str, Any]]) -> List[OCRBox]:
        return [
            OCRBox(
                data["bbox"]["x1"],
                data["bbox"]["y1"],
                data["bbox"]["x2"],
                data["bbox"]["y2"],
                data["label"],
            )
            for data in symbols_data["entities"]
        ]

    @staticmethod
    def json2ocr(json_data: List[Dict[str, Any]]) -> List[OCRBox]:
        return [
            OCRBox(data["box"][0], data["box"][1], data["box"][2], data["box"][3], data["text"])
            for data in json_data
        ]
