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
    Set up a logger with a file handler for monitoring events.

    This function creates and configures a logger that writes log messages
    to a specified file. The logger's level is set to INFO, and a formatter
    is applied to ensure that log messages include timestamps.

    :param log_file: The path to the log file where logs will be written.
        If not provided, defaults to "log_events.log".
    :type log_file: Optional[str]

    :return: A configured `logging.Logger` instance.
    :rtype: logging.Logger

    :raises ValueError: If the log file path is invalid or cannot be created.

    .. note::
        If the logger already has handlers, they will be cleared before
        adding the new file handler. Ensure that the log file path is writable.
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
        Initialize the ResourceMonitor with logging configuration.

        This class sets up a logger to track resource usage, including active
        threads and memory usage. The log file where logs will be written
        can be specified; if not provided, defaults to 'log_resources.log'.

        :param log_file: Optional path to the log file. Default is
        'log_resources.log'.
        :type log_file: Optional[str]
        """
        # Configure the logger
        self._logger = logging.getLogger('MonitorLogger')
        self._logger.setLevel(logging.INFO)

        # Remove existing handlers if any
        if self._logger.hasHandlers():
            self._logger.handlers.clear()

        # Create and configure file handler
        thread_monitor_handler = logging.FileHandler(log_file)
        thread_monitor_formatter = (
            logging.Formatter('%(asctime)s - %(message)s'))
        thread_monitor_handler.setFormatter(thread_monitor_formatter)
        self._logger.addHandler(thread_monitor_handler)

        self._logger.info("Starting new run")

    def _log_function(self, func: Callable) -> Callable:
        """
        Decorator to log the execution time and errors of a function.

        This method wraps a function to log its start and end times, as well
        as any errors encountered during execution.

        :param func: The function to be decorated.
        :type func: Callable
        :return: The wrapped function with logging.
        :rtype: Callable
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

        This method runs in a loop, checking the number of active threads and
        the memory usage of the current process at regular intervals. The
        interval between checks is specified by the `interval` parameter.

        :param interval: Interval in seconds between each check. Default is 60
        seconds.
        :type interval: int
        :return: None
        :rtype: None
        """
        while True:
            active_threads = threading.active_count()
            memory_info = psutil.Process().memory_info()
            memory_usage = memory_info.rss / (1024 * 1024 * 1024)  # Convert to GB
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

        This method creates and starts a separate thread to continuously
        monitor and log resource usage based on the specified interval.

        :param interval: Interval in seconds between each check. Default is 60
        seconds.

        :type interval: int
        :return: Thread object for the monitoring thread.
        :rtype: threading.Thread
        """
        monitor_thread = threading.Thread(
            target=lambda: self._count_resources(interval=interval),
            daemon=True
        )
        monitor_thread.start()
        return monitor_thread

