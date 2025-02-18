from IPython import get_ipython
from ipyfilechooser import FileChooser
from ipywidgets import (
    Button, HBox, VBox, Text, Output, Dropdown, Layout, SelectMultiple, HTML
)
from IPython.display import display
from pathlib import Path, PurePosixPath
from s3_tools import S3Manager


class S3ManagerGUI:
    def __init__(self, s3_manager: S3Manager, show_files: bool = False):
        if not self._is_jupyter():
            raise RuntimeError("This GUI works only in Jupyter Notebook!")
        self.s3_manager = s3_manager
        self.show_files = show_files
        self._create_widgets()
        self._setup_ui()

    def _is_jupyter(self):
        try:
            shell = get_ipython().__class__.__name__
            return shell == 'ZMQInteractiveShell'
        except NameError:
            return False

    def _create_widgets(self):
        self.bucket_name = Text(
            description="Bucket Name:", layout=Layout(width='500px'))
        self.s3_prefix = Text(description="S3 Prefix:",
                              value='', layout=Layout(width='500px'))
        self.operation = Dropdown(options=[
                                  'Upload', 'Download'], description='Operation:', layout=Layout(width='300px'))
        self.folder_chooser = FileChooser()
        self.folder_chooser.show_only_dirs = True
        self.folder_chooser.title = '<b>Select Local Folder</b>'
        self.s3_folder_select = SelectMultiple(
            options=[], description='Select Folders:', disabled=True, layout=Layout(width='800px', height='200px'))
        self.process_btn = Button(
            description="Start Process", button_style='success')
        self.confirm_btn = Button(
            description="Confirm Selection", button_style='primary')
        self.back_btn = Button(description="Back", button_style='warning')
        self.output = Output()
        self.main_box = VBox()
        self.folder_selection_box = VBox()

    def _setup_ui(self):
        credentials_box = VBox([self.bucket_name, self.s3_prefix])
        main_controls = HBox(
            [self.operation, self.folder_chooser, self.process_btn])
        self.main_box.children = [credentials_box, main_controls, self.output]
        self.folder_selection_box.children = [HTML(
            "<b>Select folders to download:</b>"), self.s3_folder_select, HBox([self.confirm_btn, self.back_btn])]
        self.process_btn.on_click(self._on_process_click)
        self.confirm_btn.on_click(self._on_confirm_click)
        self.back_btn.on_click(lambda _: display(self.main_box))

    def _validate_inputs(self):
        required_fields = [
            (self.bucket_name.value, "Bucket name is required"),
            (self.folder_chooser.selected, "Local folder is required")
        ]
        for value, error in required_fields:
            if not value:
                self._show_error(error)
                return False
        return True

    def _show_error(self, message):
        with self.output:
            print(f"Error: {message}")

    def _on_process_click(self, _):
        self.output.clear_output()
        if not self._validate_inputs():
            return
        operation = self.operation.value
        local_path = Path(self.folder_chooser.selected)
        bucket = self.bucket_name.value
        prefix = self.s3_prefix.value
        try:
            if operation == 'Download':
                self._handle_upload(local_path, bucket, prefix)
            else:
                self._handle_download(local_path, bucket, prefix)
        except Exception as e:
            self._show_error(f"Critical error: {str(e)}")

    def _handle_upload(self, local_path: Path, bucket: str, prefix: str):
        files = [f for f in local_path.glob('**/*') if f.is_file()]
        if self.show_files:
            with self.output:
                print(f"Found {len(files)} files for upload")
        self.s3_manager.upload_files(bucket, prefix, files)

    def _handle_download(self, local_path, bucket: str, prefix: str):
        with self.output:
            print("Retrieving S3 structure...")
        s3_files = self.s3_manager.list_files(bucket, prefix)
        folders = self._extract_s3_folders(s3_files)
        if not folders:
            self._show_error("No folders found for download")
            return
        self.s3_folder_select.options = sorted(folders)
        self.s3_folder_select.disabled = False
        display(self.folder_selection_box)
        self.main_box.layout.display = 'none'

    def _extract_s3_folders(self, s3_files: list[Path]):
        folders: set = set()
        for path in s3_files:
            parts = path.parts
            if '.' in parts[-1]:
                parts = parts[:-1]
            for i in range(1, len(parts) + 1):
                folder = PurePosixPath(*parts[:i])
                folders.add(str(folder))
        return list(folders)

    def _on_confirm_click(self, _):
        selected_folders = self.s3_folder_select.value
        if not selected_folders:
            self._show_error("Select at least one folder")
            return

        bucket = self.bucket_name.value
        local_path = Path(self.folder_chooser.selected)

        with self.output:
            print("Starting download...")

        all_files = []
        for folder in selected_folders:
            files = self.s3_manager.list_files(bucket, folder)
            files = [f for f in files if '.' in f.name]
            all_files.extend(files)

        self.s3_manager.download_files(
            bucket_name=bucket,
            list_files=all_files,
            local_base_path=local_path,
            keep_structure=True
        )

        self.main_box.layout.display = 'flex'
        display(self.main_box)

    def show(self):
        display(self.main_box)
