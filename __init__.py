try:
    from IPython import get_ipython
    if 'IPKernelApp' not in get_ipython().config:
        raise ImportError("console")
    from tqdm.notebook import tqdm
except ImportError:
    from tqdm import tqdm