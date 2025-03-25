"""
Helpers
====================================
Helper classes and functions
"""

import os
import shlex
import binascii
import operator
import hashlib
from subprocess import run
from enum import Enum
from yachalk import chalk
from rich.console import Console
from rich.table import Table


class ReturnCode(Enum):
    OK = 0


class Executor:
    """
    Class to execute shell commands
    """

    @classmethod
    def success(cls, command: str, env: dict = None) -> bool:
        ret, _ = cls.__run(command, env=env)
        return ret == ReturnCode.OK.value

    @classmethod
    def run(cls, command: str, env: dict = None) -> str:
        _, output = cls.__run(command, env=env)
        return cls.__handle_output(output)

    @classmethod
    def __run(cls, command: str, env: dict = None) -> tuple[int, str]:
        """
        Runs a shell command with the default environment.
        If env is given it is _UPDATED_ to the default environment.
        """
        _env = os.environ.copy()
        if env:
            _env.update(env)
        p = run(shlex.split(command), capture_output=True, env=_env)
        return p.returncode, cls.__handle_output(p.stdout.decode())

    @classmethod
    def __handle_output(cls, output: str) -> str:
        # We remove the last \n
        return output.rstrip("\n")


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
    def crc32(cls, filename):
        buf = open(filename, "rb").read()
        hash = binascii.crc32(buf) & 0xFFFFFFFF
        return "%08X" % hash

    @classmethod
    def md5(cls, filename):
        return hashlib.md5(open(filename, "rb").read()).hexdigest()
