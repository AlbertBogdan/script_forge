from gradio_client import Client
from more_itertools import chunked
from PIL import Image
from toolbox.image import encode_pil_to_base64
from toolbox.ocr.textbox import Textbox
from tqdm.auto import tqdm

class TextlineOcrApiClient:
    def __init__(self, api_url: str, max_concurrent_requests: int = 64):
        self.max_concurrent_requests = max_concurrent_requests
        self.client = Client(
            api_url, max_workers=max_concurrent_requests + 10, ssl_verify=False
        )

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
                    results.append(Textbox.validate(j.result()))
                    tqdm_bar.update(1)

        for tb, img in zip(results, images):
            tb.source_image = img

        return results


class DocumentOcrApiClient:
    def __init__(self, api_url: str, max_concurrent_requests: int = 4):
        self.max_concurrent_requests = max_concurrent_requests
        self.client = Client(
            api_url
        )

    def recognize(self, images: list[Image.Image]) -> list[Textbox]:
        encoded_imgs = [encode_pil_to_base64(img) for img in images]
        results = []

        # with tqdm(total=len(encoded_imgs), desc="Recognizing documents") as tqdm_bar:
        print("OK")
        for chunk in chunked(encoded_imgs, self.max_concurrent_requests):
            jobs = [
                self.client.submit(images={"url": e_img}, api_name="/predict")
                for e_img in chunk
            ]
            for j in jobs:
                # results.append([Textbox.validate(tb) for tb in j.result()])
                results.append(j.result())
                # tqdm_bar.update(1)

        # for res, img in zip(results, images):
        #     for tb in res:
        #         tb.source_image = img
        return results
