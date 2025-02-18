from PIL import ImageDraw, ImageFont, Image
from dexlib.tools.kh2_exporter.kh2_exporter import (
    Kh2Record,
    FileImageProvider,
    Kh2Annotation,
    Kh2Attribute,
    Kh2Object,
    Kh2ObjectMetadata,
    Kh2Bbox,
    PilImageProvider,
)

def generate_kh2_sample() -> Kh2Record:
    blank_image_pil = Image.new('RGB', (480, 320), (255, 255, 255))

    draw = ImageDraw.Draw(blank_image_pil)
    font = ImageFont.load_default()
    draw.text((100, 200), "P-1234A", font=font, fill=(0, 0, 0))
    bbox_coords1 = draw.textbbox((100, 200), "P-1234A", font=font)
    draw.text((350, 100), "62-PI-064321", font=font, fill=(0, 0, 0))
    bbox_coords2 = draw.textbbox((350, 100), "62-PI-064321", font=font)

    bbox_center_coords_rel = (
        (bbox_coords2[0] + bbox_coords2[2]) / 2 / 480,
        (bbox_coords2[1] + bbox_coords2[3]) / 2 / 320
    )


    kh2_record1 = Kh2Record(
        image_provider=PilImageProvider(blank_image_pil),
        annotation=Kh2Annotation(
            fields=[
                Kh2Attribute(key='File name', value='file1.pdf'),
            ],
            objects=[
                Kh2Object(
                    text='P-1234A',
                    label='Tag',
                    bbox=Kh2Bbox(x1=bbox_coords1[0], y1=bbox_coords1[1], x2=bbox_coords1[2], y2=bbox_coords1[3]),
                    metadata=Kh2ObjectMetadata()
                ),
                Kh2Object(
                    text='62-PI-064321',
                    label='Tag',
                    bbox=Kh2Bbox(x1=bbox_coords2[0], y1=bbox_coords2[1], x2=bbox_coords2[2], y2=bbox_coords2[3]),
                    metadata=Kh2ObjectMetadata()
                )
            ]
        ),
        file_path='file1.pdf',
        page_number=0,
    )

    return kh2_record1
