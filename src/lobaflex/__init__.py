""""""
from pathlib import Path

from lobaflex import __path__ as src_root


def get_parallel_dir(path, key):
    """Get directories parallel to src level

    Parameters
    ----------
    path : Poxispath
    key : Name of dir

    Returns
    -------
    key_dir
    """
    if not isinstance(path, Path):
        path = Path(path)
    parent_dir = Path(path).absolute().parent
    parallel_dir = parent_dir / key
    return parallel_dir


def get_parent_dir(key):
    """Gets the absolute path of one parent dir with the key as name if exists.

    Parameters
    ----------
    key : Name of directory

    Returns
    -------
    path:
        absolute path of parent dir

    """
    i = [i for i, dir in enumerate(Path("./").absolute().parts) if key in dir]
    path = Path(*Path("./").absolute().parts[: i + 1])
    return path


content_root = Path(src_root[0]).parent
logs_dir = get_parallel_dir(path=content_root, key="logs")
data_dir = get_parallel_dir(path=content_root, key="data")
config_dir = get_parallel_dir(path=content_root, key="config")
results_dir = get_parallel_dir(path=content_root, key="results")
