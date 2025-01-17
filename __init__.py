try:
    from IPython import get_ipython
    ipython = get_ipython()
    if ipython is None or 'IPKernelApp' not in ipython.config:
        raise ImportError("console")
    from tqdm.notebook import tqdm
    import matplotlib.pyplot as plt
    plt.ion()
    from PIL import Image
    from IPython.display import display

    def imshow(img):
        display(img)

except ImportError:
    from tqdm import tqdm
    import matplotlib.pyplot as plt
    from PIL import Image
