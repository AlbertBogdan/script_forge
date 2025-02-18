import base64


class LinkGenerator:
    def __init__(self, instance_url: str, project_id: str):
        self.instance_url = instance_url
        self.project_id = project_id

    def generate_link(
        self, file_name: str, page_number: int, mode: str = "actual"
    ) -> str:
        encoded_file_name = base64.b64encode(file_name.encode("utf-8")).decode("utf-8")
        encoded_page_number = base64.b64encode(
            str(page_number - 1).encode("utf-8")
        ).decode("utf-8")
        base64_page_uid = f"{encoded_file_name},{encoded_page_number}"
        link = f"{self.instance_url}/project/{self.project_id}/general_link?page_uid={base64_page_uid}&mode={mode}"
        return link
