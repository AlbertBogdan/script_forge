import re
import os
import json

from PIL import Image
from utils.ocr.gradio_clients import DocumentOcrApiClient
from .find_box import find_box_below
from toolbox.ocr.textbox import Textbox
cache_path = "utils/ocr/cache"

patterns_line = [r'[A-Z]{1,2}(-\d+)?-\d{1,2}(\/\d{1,2})?(.{1,2})?-[A-Z\d]{3}-\d{2}-\d+(-[A-Z]+)?',
                 r'[A-Z]{1,2}(-\d+)?-\d{1,2}(\/\d{1,2})?(.{1,2})?.{5}-\d{5}(-[A-Z]{1,2})?(-\d+)?']

patterns_line_small = r'^[A-Z]{1,2}(\.|-)\d{1,2}(\/\d{1,2})?(.{1,2})?(-|\.)?\d+$'
pattern_size =r"^}SIZE$|^.?IZE$|^S.?ZE$|^S.?ZE$|^SI.?E$|^SIZ.?$"
pattern_drawing = r"DRAWING|.?RAWING|D.?AWING|DR.?WING|DRA.?ING|DRAW.?NG|DRAWI.?G|DRAWIN.?"
pattern_specification = r"^.?SPECIFICATION$|^.?.?PECIFICATION$|^.?S.?ECIFICATION$|^.?SP.?CIFICATION$|^.?SPE.?IFICATION$|^.?SPEC.?FICATION$|^.?SPECI.?ICATION$|^.?SPECIF.?CATION$|^.?SPECIFI.?ATION$|^.?SPECIFIC.?TION$|^.?SPECIFICA.?ION$|^.?SPECIFICAT.?ON$|^.?SPECIFICATI.?N$|^.?SPECIFICATIO.?$"
pattern_spec = r"^SPEC.?$|^.?PEC.?$|^S.?EC.?$|^SP.?C.?$|^SPE.?.?$|^SPEC.?$"

def Extract_line_number_table(data):

    no_line_small = []
    size_arr = []
    spec_arr = []
    drawing_arr = []

    size = None
    spec = None
    drawing = None

    data.sort(key=lambda box: min(point[1] for point in box['box']))

    for label in data:
        text= label["text"].replace(" ", "")
        if re.match(patterns_line_small, text): no_line_small.append(text)
        elif re.match(pattern_size, text): size_arr.append(label)
        elif re.match(pattern_drawing, text): drawing_arr.append(label)
        elif re.match(pattern_specification, text) or re.match(pattern_spec, text):
            spec_arr.append(label)

    size_arr.reverse()
    for sz in size_arr:
        label_down = find_box_below(sz, data)
        if label_down == None: continue
        text = label_down["text"].replace(" ", "")
        if re.match(r'^\d{1,2}(.)?$', text):
            size = text
            break

    drawing_arr.reverse()
    for dr in drawing_arr:

        label_down = find_box_below(dr, data)
        if label_down == None: continue
        text = re.sub(r"[^A-Z0-9]", "", label_down["text"])
        drawing = text
        break

    conf = -1
    for sp in spec_arr:
        label_down = find_box_below(sp, data)
        if label_down == None: continue
        text = re.sub(r"[^A-Z0-9]", "", label_down["text"])
        if len(text) == 5 and label_down["text_confidence"] > conf:
            spec = text
            conf = label_down["text_confidence"]

    line_size = 'XX"'
    if size != None:
        if size[-1].isdigit() == False:
            if size[-1] != '"':
                line_size = f'{size[:-1]}"'
            else: line_size = size
        else: line_size = f'{size}"'

    draw_1 = "X"
    draw_2 = "XXXXX"

    if drawing != None:
        match = re.search(r"([A-Z]{1,2})(\d+)", drawing)
        if match:
            draw_1 = match.group(1)

        match = re.search(r"(\d+)", drawing)
        if match:
            digits = match.group(1)
            if len(digits) <= 7:
                draw_2 = digits[:5]
            if len(digits) >= 8:
                draw_2 = digits[1:6]

    spec_1 = "XXXXX"
    if spec != None:
        spec_1 = spec

    return f"{draw_1}-{line_size}-{draw_2}-{spec_1}"


def Extract_data(image, ocr):

    img_name = image.filename.split("/")[-1]

    if not os.path.exists(cache_path):
        os.makedirs(cache_path)

    cache_names = os.listdir(cache_path)

    if f"{img_name}.json" not in cache_names:
        data = ocr.recognize([image])
        with open(os.path.join(cache_path, f"{img_name}.json"), "w") as fp:
            json.dump(data, fp)
    else:
         with open(os.path.join(cache_path, f"{img_name}.json"), "r") as fp:
            data = json.load(fp)

    return data[0]

def Extract_line_number(image, ocr):

    data = Extract_data(image, ocr)

    no_line = []
    for pattern_line in patterns_line:
        for label in data:
            mm = re.search(pattern_line, label["text"])
            if mm: no_line.append(mm.group())

    if len(no_line) == 1:
        return no_line[0]

    if len(no_line) > 1:
        return max(no_line, key=len)

    if len(no_line) == 0:
         return Extract_line_number_table(data)

def Get_Line_No(image, ocr = None):

    if ocr is None:
        ocr = DocumentOcrApiClient("http://192.168.92.128:7772/")

    return f"FMT-MTBE-{Extract_doc_number(image, ocr)}-XX"

def Extract_doc_number(image, ocr):
    data = ocr.recognize([image])[0]
    textboxes =  [Textbox.model_validate(tb) for tb in data]
    pattern_doc = r"^[A-Z]-\d{2}-\d{2}$"

    for tb in textboxes:
        if re.match(pattern_doc, tb.text):
            return tb.text

