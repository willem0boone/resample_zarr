import time
import psutil
import logging
import threading
import functools
import multiprocessing
from typing import Any
from typing import Callable
from typing import Optional


def setup_logger(log_file: Optional[str] = "log_events.log") -> logging.Logger:
    """
    Sets up a logger for monitoring with a file handler.

    :param log_file: Path to the log file where logs will be written.
    """
    # Create a logger instance
    logger = logging.getLogger('EventLogger')
    logger.setLevel(logging.INFO)

    # Remove existing handlers if any
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create a file handler for the logger
    thread_monitor_handler = logging.FileHandler(log_file)

    # Create a formatter and set it for the handler
    thread_monitor_formatter = logging.Formatter('%(asctime)s - %(message)s')
    thread_monitor_handler.setFormatter(thread_monitor_formatter)

    # Add the handler to the logger
    logger.addHandler(thread_monitor_handler)

    return logger


class ResourceMonitor:
    def __init__(self, log_file: Optional[str] = 'log_resources.log'):
        """
        Initialize the ThreadMonitor class with a logging configuration.

        :param log_file: Optional log file name. Default is 'default_log.log'.
        """
        # Configure the _logger
        self._logger = logging.getLogger('MonitorLogger')
        self._logger.setLevel(logging.INFO)

        # Remove existing handlers if any
        if self._logger.hasHandlers():
            self._logger.handlers.clear()

        thread_monitor_handler = logging.FileHandler(log_file)
        thread_monitor_formatter = logging.Formatter(
            '%(asctime)s - %(message)s')
        thread_monitor_handler.setFormatter(thread_monitor_formatter)
        self._logger.addHandler(thread_monitor_handler)

        self._logger.info(f"Starting new run")

    def _log_function(self, func: Callable) -> Callable:
        """
        Decorator to log the execution time and errors of a function.

        :param func: Function to be decorated.
        :return: Wrapped function with logging.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            self._logger.info(f"Started {func.__name__}")
            try:
                result = func(*args, **kwargs)
                self._logger.info(f"Finished {func.__name__}")
                return result
            except Exception as e:
                self._logger.error(f"Error in {func.__name__}")
                self._logger.error(f"Error: {e}")
                raise
        return wrapper

    def _count_resources(self, interval: int = 60) -> None:
        """
        Continuously monitors and logs the number of active threads and memory
        usage.

        :param interval: Interval in seconds between each check. Default is 60.
        :return: None
        """
        while True:
            active_threads = threading.active_count()
            memory_info = psutil.Process().memory_info()
            # Convert to GB
            memory_usage = memory_info.rss / (1024 * 1024 * 1024)
            cpu = multiprocessing.cpu_count()

            self._logger.info(
                f"Active threads: {active_threads}, "
                f"Memory usage: {memory_usage:.2f} GB, "
                f"CPU: {cpu}"
            )
            time.sleep(interval)

    def start_monitor_resources(self, interval: int = 60) -> threading.Thread:
        """
        Start a monitoring thread for counting active threads and monitoring
        memory usage.

        :param interval: Interval in seconds between each check. Default is 60.
        :return: Thread object for the monitoring thread.
        """
        monitor_thread = threading.Thread(
            target=lambda: self._count_resources(interval=interval),
            daemon=True
        )
        monitor_thread.start()
        return monitor_thread
