from __future__ import annotations

import atexit
import logging
import re
import sys
import threading
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Optional, TextIO

import yaml

from .dataclass import AutoDataClass, autodataclass


class ANSIColors(Enum):
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    DEFAULT = "\033[39m"

    BLACK_BRIGHT = "\033[90m"
    RED_BRIGHT = "\033[91m"
    GREEN_BRIGHT = "\033[92m"
    YELLOW_BRIGHT = "\033[93m"
    BLUE_BRIGHT = "\033[94m"
    MAGENTA_BRIGHT = "\033[95m"
    CYAN_BRIGHT = "\033[96m"
    WHITE_BRIGHT = "\033[97m"
    DEFAULT_BRIGHT = "\033[99m"

    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"
    BG_DEFAULT = "\033[49m"

    BG_BLACK_BRIGHT = "\033[100m"
    BG_RED_BRIGHT = "\033[101m"
    BG_GREEN_BRIGHT = "\033[102m"
    BG_YELLOW_BRIGHT = "\033[103m"
    BG_BLUE_BRIGHT = "\033[104m"
    BG_MAGENTA_BRIGHT = "\033[105m"
    BG_CYAN_BRIGHT = "\033[106m"
    BG_WHITE_BRIGHT = "\033[107m"
    BG_DEFAULT_BRIGHT = "\033[109m"

    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    REVERSE = "\033[7m"
    HIDE = "\033[8m"

    BOLD_OFF = "\033[21m"
    DIM_OFF = "\033[22m"
    UNDERLINE_OFF = "\033[24m"
    BLINK_OFF = "\033[25m"
    REVERSE_OFF = "\033[27m"
    HIDE_OFF = "\033[28m"

    END = "\033[0m"


def color_fmt(msg: Any, color: ANSIColors) -> str:
    return f"{color.value}{str(msg)}{ANSIColors.END.value}"


@autodataclass
class LoggingManagerConfig(AutoDataClass):
    app_name: str = "app"
    log_dir: Path = Path("logs")
    level: int = logging.INFO
    console_level: Optional[int] = None  # if None, uses `level`
    utc_timestamps: bool = False

    # Decorations
    log_format: Optional[str] = None
    date_format: Optional[str] = None

    # Create file immediately and append continuously
    filename_prefix: Optional[str] = None  # e.g. "run" -> run_2025-...log
    filename_time_format: str = "%Y-%m-%d_%H-%M-%S"
    encoding: str = "utf-8-sig"
    overwrite: bool = False  # if True and name collides (rare), overwrite

    # Optional: also capture print() and raw stderr/stdout into logging
    also_capture_stdout_stderr: bool = False


_std_fmt = "%(asctime)s.%(msecs)03d    [ %(levelname)s ] [ %(name)s ] %(message)s"
_std_date_fmt = "%Y-%m-%d %H:%M:%S"


class _ColoredConsoleFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: ANSIColors.BLUE,
        logging.INFO: ANSIColors.GREEN,
        logging.WARNING: ANSIColors.YELLOW,
        logging.ERROR: ANSIColors.RED,
        logging.CRITICAL: ANSIColors.RED_BRIGHT,
    }

    def __init__(
        self,
        fmt: Optional[str] = None,
        *,
        datefmt: Optional[str] = None,
        utc: bool = False,
    ) -> None:
        fmt = fmt or _std_fmt
        datefmt = datefmt or _std_date_fmt
        super().__init__(fmt=fmt, datefmt=datefmt)

        if utc:
            self.converter = lambda *args: datetime.now(timezone.utc).timetuple()

    def format(self, record: logging.LogRecord) -> str:
        log_color = self.COLORS.get(record.levelno, ANSIColors.DEFAULT)
        formatted = super().format(record)

        pattern_number = r"(^| )(\d+\.?\d*)( |$)"
        pattern_color_fmt = r"color_fmt\(([^,]+),\s*(?:<)?ANSIColors\.(\w+)(?::[^>]+>)?\)"
        pattern_logger_name = r"(    \[[^\]]+\] )(\[[^\]]+\])"

        def replace_newline(match):
            date_length = formatted.find("[")
            return f"\n{date_length * ' '}"

        def replace_number(match):
            prefix = match.group(1)
            number = match.group(2)
            suffix = match.group(3)
            return f"{prefix}{ANSIColors.DEFAULT.value}{number}{log_color.value}{suffix}"

        def replace_color_fmt(match):
            text = match.group(1)
            color_name = match.group(2)
            try:
                color = ANSIColors[color_name]
                return f"{color.value}{text}{log_color.value}"
            except KeyError:
                return match.group(0)  # Return original if color not found

        def replace_logger_name(match):
            prefix = match.group(1)
            name = match.group(2)
            return f"{prefix}{ANSIColors.WHITE.value}{name}{log_color.value}"

        context = {}
        keys = list(record.__dict__.keys())
        extra_keys = keys[21:-2] if len(keys) > 23 else []
        if extra_keys:
            for key in extra_keys:
                if hasattr(record, key):
                    context[key] = getattr(record, key)
            if context:
                try:
                    formatted += "\n" + yaml.dump(
                        context, default_flow_style=False, allow_unicode=True, sort_keys=False
                    )
                except Exception:
                    formatted += f"\n{context}"

        formatted = re.sub(r"\n", replace_newline, formatted)
        formatted = re.sub(pattern_number, replace_number, formatted)
        formatted = re.sub(pattern_color_fmt, replace_color_fmt, formatted)
        formatted = re.sub(pattern_logger_name, replace_logger_name, formatted)

        return f"{log_color.value}{formatted}{ANSIColors.END.value}"


class _SimpleConsoleFormatter(logging.Formatter):
    def __init__(
        self,
        fmt: Optional[str] = None,
        *,
        datefmt: Optional[str] = None,
        utc: bool = False,
    ) -> None:
        fmt = fmt or _std_fmt
        datefmt = datefmt or _std_date_fmt
        super().__init__(fmt=fmt, datefmt=datefmt)

        if utc:
            self.converter = lambda *args: datetime.now(timezone.utc).timetuple()

    def format(self, record: logging.LogRecord) -> str:
        formatted = super().format(record)

        def replace_newline(match):
            date_length = formatted.find("[")
            return f"{date_length * ' '}\n"

        formatted = re.sub(r"\n", replace_newline, formatted)
        return formatted


class LoggingManager:
    """
    Configures logging to:
      1) Console (real-time)
      2) Timestamped log file (real-time, appended on each record)
    """

    _instance: Optional[LoggingManager] = None
    _lock = threading.Lock()
    _initialized = False
    _configured = False

    def __new__(cls) -> LoggingManager:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            self.config = LoggingManagerConfig()

            self._run_time: Optional[datetime] = None
            self._run_timestamp: Optional[str] = None
            self._log_file_path: Optional[Path] = None
            self._logger: Optional[logging.Logger] = None
            self._console_handler: Optional[logging.StreamHandler] = None
            self._file_handler: Optional[logging.FileHandler] = None

            self._orig_stdout: Optional[TextIO] = None
            self._orig_stderr: Optional[TextIO] = None
            self._stdout_proxy: Optional[_StreamToLogger] = None
            self._stderr_proxy: Optional[_StreamToLogger] = None

            atexit.register(self._cleanup)
            self._initialized = True

    @property
    def run_time(self) -> Optional[datetime]:
        return self._run_time

    @property
    def run_timestamp(self) -> Optional[str]:
        return self._run_timestamp

    @property
    def log_file_path(self) -> Optional[Path]:
        return self._log_file_path

    def configure(self, config: Optional[LoggingManagerConfig] = None) -> None:
        if self._configured:
            return
        self.config = config or self.config

        self._run_time = (
            datetime.now(timezone.utc) if self.config.utc_timestamps else datetime.now()
        )
        self._run_timestamp = self._run_time.strftime(self.config.filename_time_format)

        self._logger = logging.getLogger()
        self._logger.setLevel(self.config.level)

        # Remove existing handlers to avoid duplicates in re-runs.
        for h in list(self._logger.handlers):
            self._logger.removeHandler(h)

        # Console handler
        self._console_handler = logging.StreamHandler(stream=sys.stdout)
        self._console_handler.setLevel(self.config.console_level or self.config.level)

        console_formatter = _ColoredConsoleFormatter(
            self.config.log_format,
            datefmt=self.config.date_format,
            utc=self.config.utc_timestamps,
        )
        self._console_handler.setFormatter(console_formatter)

        # File handler (writes continuously)
        self._log_file_path = self._make_log_path()
        self._log_file_path.parent.mkdir(parents=True, exist_ok=True)

        file_mode = "w" if self.config.overwrite else "a"
        self._file_handler = logging.FileHandler(
            self._log_file_path,
            mode=file_mode,
            encoding=self.config.encoding,
            delay=False,
        )
        self._file_handler.setLevel(self.config.level)

        file_formatter = _SimpleConsoleFormatter(
            self.config.log_format,
            datefmt=self.config.date_format,
            utc=self.config.utc_timestamps,
        )
        self._file_handler.setFormatter(file_formatter)

        self._logger.addHandler(self._console_handler)
        self._logger.addHandler(self._file_handler)

        # Optional capture of print() / raw writes
        if self.config.also_capture_stdout_stderr:
            self._redirect_stdout_stderr(self._logger)

        self._configured = True

    def _make_log_path(self) -> Path:
        prefix = self.config.filename_prefix or self.config.app_name
        filename = f"{prefix}_{self._run_timestamp}.log"
        path = self.config.log_dir / filename

        # Very rare collision; resolve unless overwrite=True.
        if not self.config.overwrite and path.exists():
            suffix = 1
            while True:
                candidate = self.config.log_dir / f"{prefix}_{self._run_timestamp}_{suffix}.log"
                if not candidate.exists():
                    return candidate
                suffix += 1

        return path

    def _redirect_stdout_stderr(self, root_logger: logging.Logger) -> None:
        """
        Redirect print() and unhandled writes to logging, so they show up
        in the console/file too.

        Note: this changes behavior slightly (line-buffered).
        """
        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr

        self._stdout_proxy = _StreamToLogger(root_logger, logging.INFO)
        self._stderr_proxy = _StreamToLogger(root_logger, logging.ERROR)

        sys.stdout = self._stdout_proxy  # type: ignore[assignment]
        sys.stderr = self._stderr_proxy  # type: ignore[assignment]

    def _cleanup(self) -> None:
        if self._console_handler:
            self._console_handler.close()
        if self._file_handler:
            self._file_handler.close()
        if self._logger:
            for handler in self._logger.handlers[:]:
                handler.close()
                self._logger.removeHandler(handler)


class _StreamToLogger:
    """
    File-like object that redirects writes to a logger.
    Useful for capturing print() output into logs.
    """

    def __init__(self, logger: logging.Logger, level: int) -> None:
        self.logger = logger
        self.level = level
        self._buf = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self.logger.log(self.level, line)
        return len(s)

    def flush(self) -> None:
        if self._buf.strip():
            self.logger.log(self.level, self._buf.rstrip("\n"))
        self._buf = ""


def setup_logger(config: Optional[LoggingManagerConfig] = None) -> LoggingManager:
    manager = LoggingManager()
    manager.configure(config)
    return manager
