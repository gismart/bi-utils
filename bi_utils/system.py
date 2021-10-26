import os
import shutil
import psutil
from typing import Dict, Optional


UNITS_MAPPING = {
    "PB": 1 << 50,
    "TB": 1 << 40,
    "GB": 1 << 30,
    "MB": 1 << 20,
    "KB": 1 << 10,
    "B": 1,
}


def fill_message(message: str, /, char: str = "=", *, prefix_width: int = 0) -> str:
    """Center message for terminal and fill it by `char`"""
    width = max(shutil.get_terminal_size().columns - prefix_width, 0)
    filled_message = f" {message} ".center(width, char)
    return filled_message


def ram_usage() -> str:
    """Get current process physical memory usage"""
    process = psutil.Process(os.getpid())
    ram = verbose_size(process.memory_info().rss)
    return ram


def verbose_size(size_bytes: int, /, units: Optional[Dict[str, int]] = None) -> str:
    """Convert size in bytes to readable units"""
    if units is None:
        units = UNITS_MAPPING
    suffix, factor = "", 1
    for suffix, factor in units.items():
        if size_bytes >= factor:
            break
    if factor == 1:
        amount = int(size_bytes / factor)
    else:
        amount = round(size_bytes / factor, 2)
    return f"{amount}{suffix}"
