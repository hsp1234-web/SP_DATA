import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import sys
from typing import Optional, Any

LOG_DIR_NAME = "api_test_logs"
LOG_FILE_PATH: Optional[str] = None
_global_logger_initialized_flag = False

_bootstrap_logger = logging.getLogger("BootstrapLogger")
if not _bootstrap_logger.handlers and not logging.getLogger().hasHandlers():
    _ch_bootstrap = logging.StreamHandler(sys.stdout)
    _ch_bootstrap.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s (bootstrap)'))
    _bootstrap_logger.addHandler(_ch_bootstrap)
    _bootstrap_logger.setLevel(logging.INFO)
    _bootstrap_logger.propagate = False

def get_taipei_time() -> datetime:
    """Returns the current time in Taipei timezone (UTC+8)."""
    return datetime.now(timezone.utc) + timedelta(hours=8)

class TaipeiTimeFormatter(logging.Formatter):
    """Custom formatter to add Taipei time to log records."""
    def format(self, record: logging.LogRecord) -> str:
        record.taipei_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return super().format(record)

def initialize_log_file(
    log_dir_override: Optional[str] = None,
    force_reinit: bool = False,
    project_root_path: Optional[Path] = None
) -> Optional[str]:
    global LOG_FILE_PATH, _global_logger_initialized_flag

    current_project_root: Path
    if project_root_path:
        current_project_root = project_root_path
    else:
        try:
            current_project_root = Path(__file__).resolve().parent.parent
        except NameError:
            current_project_root = Path(".").resolve()
            _bootstrap_logger.warning(f"__file__ not defined, using CWD '{current_project_root}' as project root for log path determination.")

    current_log_dir_path: Path
    if log_dir_override:
        current_log_dir_path = Path(log_dir_override)
    else:
        current_log_dir_path = current_project_root / LOG_DIR_NAME

    if _global_logger_initialized_flag and not force_reinit and LOG_FILE_PATH:
        if Path(LOG_FILE_PATH).parent == current_log_dir_path.resolve():
            _bootstrap_logger.debug(f"Global logger already initialized. Log file: {LOG_FILE_PATH}")
            return LOG_FILE_PATH
        else:
            _bootstrap_logger.warning(
                f"Log directory has changed or re-initialization is forced. "
                f"Old log dir: {Path(LOG_FILE_PATH).parent}, New log dir: {current_log_dir_path.resolve()}. Forcing re-init."
            )
            force_reinit = True

    try:
        current_log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to create log directory '{current_log_dir_path}': {e}", exc_info=True)
        return None

    utc_now = datetime.now(timezone.utc)
    timestamp_filename_str = utc_now.strftime("%Y-%m-%dT%H%M%SZ")
    log_filename = f"{timestamp_filename_str}_application_log.txt"
    current_log_file_full_path = current_log_dir_path / log_filename

    try:
        file_log_format_str = '%(asctime)s (Taipei: %(taipei_time_str)s) [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        file_formatter = TaipeiTimeFormatter(file_log_format_str)
        file_handler = logging.FileHandler(current_log_file_full_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        console_log_format_str = '[%(levelname)s] %(name)s: %(message)s'
        console_formatter = logging.Formatter(console_log_format_str)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()

        if force_reinit and root_logger.hasHandlers():
            _bootstrap_logger.info("Forcing re-initialization of root logger handlers.")
            for handler_to_remove in root_logger.handlers[:]:
                root_logger.removeHandler(handler_to_remove)
                handler_to_remove.close()

        if not root_logger.handlers or force_reinit:
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.DEBUG)

            _global_logger_initialized_flag = True
            LOG_FILE_PATH = str(current_log_file_full_path)
            logging.getLogger("GlobalLogSetup").info(f"Global logger initialized. Log file: {LOG_FILE_PATH}")
        else:
            _bootstrap_logger.info("Root logger already has handlers and not forcing re-init. Current setup maintained.")
            if LOG_FILE_PATH is None:
                 LOG_FILE_PATH = str(current_log_file_full_path)
                 _bootstrap_logger.warning(f"LOG_FILE_PATH was None but logger seemed initialized. Set to: {LOG_FILE_PATH}")

    except Exception as e:
        _bootstrap_logger.error(f"Failed to configure logging to file '{current_log_file_full_path}': {e}", exc_info=True)
        LOG_FILE_PATH = None
        _global_logger_initialized_flag = False
        return None

    return LOG_FILE_PATH

def log_message(
    message: str,
    level: str = "INFO",
    logger_name: Optional[str] = None,
    exc_info: bool = False,
    **kwargs: Any
):
    effective_logger: logging.Logger
    if not _global_logger_initialized_flag or LOG_FILE_PATH is None:
        effective_logger = _bootstrap_logger
        if not hasattr(log_message, "_bootstrap_warning_issued_for_general_use"):
            effective_logger.warning(
                f"Global logger not fully initialized (Log file path: {LOG_FILE_PATH}). "
                f"Logging message ('{message[:50]}...') with bootstrap logger as fallback."
            )
            setattr(log_message, "_bootstrap_warning_issued_for_general_use", True)
    else:
        effective_logger = logging.getLogger(logger_name if logger_name else "project_logger.general")

    level_upper = level.upper()
    log_level_int = logging.getLevelName(level_upper)
    log_method = getattr(effective_logger, level_upper.lower(), effective_logger.info)
    should_pass_exc_info = exc_info and (isinstance(log_level_int, int) and log_level_int >= logging.ERROR)

    try:
        log_method(message, exc_info=should_pass_exc_info, extra=kwargs if kwargs else None)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to log message with '{effective_logger.name}'. Original message: '{message}'. Error: {e}", exc_info=True)

if __name__ == "__main__":
    main_script_project_root_path = Path(__file__).resolve().parent.parent
    log_file_path_main = initialize_log_file(force_reinit=True, project_root_path=main_script_project_root_path)

    if log_file_path_main:
        log_message("Info message from __main__ of initialize_global_log (Historical).", "INFO", logger_name="TestInitializeGlobalLogHist")
        log_message(f"Global log file for this direct run is confirmed at: {LOG_FILE_PATH}", "CRITICAL", logger_name="TestInitializeGlobalLogHist.CriticalSub")
        print(f"Script execution finished. Log file should be at: {LOG_FILE_PATH}")
    else:
        print("Failed to initialize the log file in __main__ of initialize_global_log (Historical). Check console for bootstrap logger errors.")
