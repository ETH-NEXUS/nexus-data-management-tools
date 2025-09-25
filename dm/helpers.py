"""
Helpers
====================================
Core helper classes and functions used by the CLI.
"""

import os
import binascii
import operator
import hashlib
import blake3
from yachalk import chalk
from rich.console import Console
from rich.table import Table

class Message:
    """
    Class to output colored messages to the console
    """

    @classmethod
    def info(cls, message):
        print(chalk.green_bright.bold(message))

    @classmethod
    def warn(cls, message):
        print(chalk.yellow_bright.bold(message))

    @classmethod
    def error(cls, message):
        print(chalk.red_bright.bold(message))

    @classmethod
    def debug(cls, message):
        print(chalk.blue_bright.bold(message))


class TableOutput:
    """
    Class to output text in table format
    """

    console = Console()

    @classmethod
    def out(
        cls,
        data: str | list | dict,
        sep: str = "#",
        headers: tuple[str] = None,
        show_lines=False,
        column_options={},
        sort_by=None,
        row_style=lambda row: None,
    ):
        table = Table(
            show_header=(
                headers is not None
                or (isinstance(data, list) and isinstance(data[0], dict))
            ),
            show_lines=show_lines,
            show_edge=False,
        )
        if headers:
            for header in headers:
                table.add_column(header, **column_options)
        elif isinstance(data, list) and isinstance(data[0], dict):
            for header in data[0].keys():
                table.add_column(header, **column_options)

        if isinstance(data, str):
            data = data.split("\n")

        if sort_by is not None and isinstance(data, list) and isinstance(data[0], dict):
            if sort_by in data[0].keys():
                data = sorted(data, key=operator.itemgetter(sort_by))

        for line in data:
            if isinstance(line, str):
                row = line.split(sep)
                table.add_row(*row, style=row_style(row))
            elif isinstance(line, list):
                table.add_row(*line, style=row_style(line))
            elif isinstance(line, dict):
                if headers:
                    table.add_row(
                        *[str(line[h]) for h in headers], style=row_style(line)
                    )
                else:
                    table.add_row(
                        *[str(v) for v in line.values()], style=row_style(line)
                    )

        cls.console.print(table)


class Hasher:
    @classmethod
    def crc32(cls, filename, block_size: int = 8 * 1024 * 1024):
        crc = 0
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                crc = binascii.crc32(chunk, crc)
        return "%08X" % (crc & 0xFFFFFFFF)

    @classmethod
    def md5(cls, filename):
        h = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @classmethod
    def blake3(cls, filename, block_size: int = 8 * 1024 * 1024) -> str:
        h = blake3.blake3()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(block_size), b""):
                h.update(chunk)
        return h.hexdigest()

    @classmethod
    def equals(cls, src: str, dst: str, block_size: int = 8 * 1024 * 1024) -> bool:
        """
        Compare two files efficiently by reading both in fixed-size blocks.
        Returns True if files are byte-for-byte identical, False otherwise.
        """
        if os.path.getsize(src) != os.path.getsize(dst):
            return False
        with open(src, "rb") as f1, open(dst, "rb") as f2:
            while True:
                b1 = f1.read(block_size)
                b2 = f2.read(block_size)
                if not b1 and not b2:
                    return True
                if b1 != b2:
                    return False
