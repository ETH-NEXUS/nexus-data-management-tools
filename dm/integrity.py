from os.path import isfile
import shutil
from typing import Optional, Tuple

# Package-safe imports with fallback for script execution
try:
    from .helpers import Hasher, Message as M
except ImportError:  # pragma: no cover
    from helpers import Hasher, Message as M  # type: ignore


def read_md5_sidecar(source_path: str) -> Optional[str]:
    """Return expected MD5 from a `.md5` sidecar if present, else None.
    Reads the first token on the first line.
    """
    md5_filename = f"{source_path}.md5"
    if not isfile(md5_filename):
        return None
    try:
        with open(md5_filename, "r") as f:
            first_line = f.readline().split(None, 1)
            return first_line[0] if first_line else None
    except Exception as ex:  # Robust against funky sidecar files
        M.warn(f"Could not read md5 sidecar for {source_path}: {ex}")
        return None


def write_blake3_sidecar(source_path: str) -> bool:
    """Compute BLAKE3 for source and write `<source>.blake3`. Returns success bool."""
    try:
        digest = Hasher.blake3(source_path)
        with open(f"{source_path}.blake3", "w") as bf:
            bf.write(f"{digest}\n")
        return True
    except Exception as ex:
        M.warn(f"Could not write blake3 sidecar for {source_path}: {ex}")
        return False


def copy_matching_sidecar(source_path: str, target_path: str) -> Tuple[str, bool]:
    """Copy a matching sidecar (`.md5` or `.blake3`) alongside the target.
    Returns (sidecar_type, copy_ok). sidecar_type is "md5", "blake3", or "" if none.
    """
    src_md5 = f"{source_path}.md5"
    if isfile(src_md5):
        dst_md5 = f"{target_path}.md5"
        try:
            shutil.copyfile(src_md5, dst_md5)
            return ("md5", isfile(dst_md5))
        except Exception as ex:
            M.warn(f"Could not copy md5 sidecar for {source_path}: {ex}")
            return ("md5", False)

    src_b3 = f"{source_path}.blake3"
    if isfile(src_b3):
        dst_b3 = f"{target_path}.blake3"
        try:
            shutil.copyfile(src_b3, dst_b3)
            return ("blake3", isfile(dst_b3))
        except Exception as ex:
            M.warn(f"Could not copy blake3 sidecar for {source_path}: {ex}")
            return ("blake3", False)

    return ("", False)
