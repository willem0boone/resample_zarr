import re
import pandas as pd
from typing import Optional
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def _parse_resource_log(logfile: Optional[str] = 'log_resources.log'
                        ) -> pd.DataFrame:
    timestamps = []
    active_threads = []
    memory_usage = []

    # Define regex patterns to match log lines
    pattern_thread_memory = re.compile(
        r'(?P<timestamp>[\d-]+\s[\d:,]+) - Active threads: (?P<threads>\d+), '
        r'Memory usage: (?P<memory>[0-9.]+) GB')

    # Read and parse the log file
    with open(logfile, 'r') as f:
        for line in f:
            match_thread_memory = pattern_thread_memory.match(line)
            if match_thread_memory:
                data = match_thread_memory.groupdict()
                timestamps.append(datetime.strptime(
                    data['timestamp'],
                    '%Y-%m-%d %H:%M:%S,%f'))
                active_threads.append(int(data['threads']))
                memory_usage.append(float(data['memory']))

    # Convert lists to pandas DataFrame for easier plotting
    df = pd.DataFrame({
        'timestamp': timestamps,
        'active_threads': active_threads,
        'memory_usage': memory_usage,
    }).sort_values(by='timestamp').reset_index(drop=True)

    return df


def _parse_event_log(logfile: Optional[str] = 'log_events.log'
                     ) -> pd.DataFrame:

    timestamps = []
    datasets = []
    vars = []
    seen_vars = set()  # To keep track of already seen variables

    # Define regex patterns to match log lines
    pattern_downscaling = re.compile(
        r'(?P<timestamp>[\d-]+\s[\d:,]+) - Downscaling dataset: '
        r'(?P<dataset>[\w_]+)'
    )
    pattern_var = re.compile(
        r'(?P<timestamp>[\d-]+\s[\d:,]+) - >> Working on VAR (?P<var>[\w_]+) '
        r'- batch \d+/\d+:windows \[\d+-\d+\]/\d+'
    )

    # Read and parse the log file
    with open(logfile, 'r') as f:
        for line in f:
            # Check for downscaling dataset updates
            match_downscaling = pattern_downscaling.match(line)
            if match_downscaling:
                data = match_downscaling.groupdict()
                timestamps.append(datetime.strptime(
                    data['timestamp'],
                    '%Y-%m-%d %H:%M:%S,%f')
                )
                datasets.append(data['dataset'])
                vars.append(None)  # No variable for this line

            # Check for VAR processing lines
            match_var = pattern_var.match(line)
            if match_var:
                data = match_var.groupdict()
                var_name = data['var']
                if var_name not in seen_vars:
                    timestamps.append(datetime.strptime(
                        data['timestamp'],
                        '%Y-%m-%d %H:%M:%S,%f')
                    )
                    datasets.append(None)  # No dataset for this line
                    vars.append(var_name)
                    seen_vars.add(var_name)  # Mark this variable as seen

    # Convert lists to pandas DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'dataset': datasets,
        'var': vars
    }).sort_values(by='timestamp').reset_index(drop=True)

    return df


def _plot_resource_log(
        logfile: Optional[str] = 'log_resources.log'
) -> None:
    """
    Plots resource usage metrics from a log file, showing memory usage and
    active threads over time.

    This function reads resource log data from a specified file and generates
    a plot with two y-axes: one for memory usage and another for active
    threads. The x-axis represents timestamps.

    :param logfile: Path to the resource log file. Default is
        'log_resources.log'.

    :type logfile: Optional[str]

    :return: None
    :rtype: None

    :raises ValueError: If the DataFrame does not contain a 'timestamp' column.
    """
    df = _parse_resource_log(logfile)

    # Ensure 'timestamp' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Plot the data
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot memory usage on the primary y-axis
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Memory Usage (GB)', color='tab:blue')
    ax1.plot(df['timestamp'],
             df['memory_usage'],
             color='tab:blue',
             label='Memory Usage')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    # Format the x-axis with readable date and time labels
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    fig.autofmt_xdate()  # Auto-format date labels for better readability

    # Create a second y-axis for active threads
    ax2 = ax1.twinx()
    ax2.set_ylabel('Active Threads', color='tab:red')
    ax2.plot(df['timestamp'],
             df['active_threads'],
             color='tab:red',
             linestyle='--',
             label='Active Threads')
    ax2.tick_params(axis='y', labelcolor='tab:red')

    # Title and grid
    plt.title('Memory Usage and Active Threads Over Time')
    fig.tight_layout()  # Adjust layout to fit labels
    plt.grid(True)

    # Save the plot to a file
    plt.savefig("monitor_resources.png")
    plt.close()


def _plot_event_log(
        logfile: Optional[str] = 'log_events.log'
) -> None:
    """
    Plots events from a log file, showing vertical lines for each event.

    This function reads event log data from a specified file and generates
    a plot with vertical lines marking the timestamps of events. The event
    names are annotated along the lines.

    :param logfile: Path to the event log file. Default is 'log_events.log'.
    :type logfile: Optional[str]

    :return: None
    :rtype: None

    :raises ValueError: If the DataFrame does not contain 'timestamp' and 'var'
     columns.
    """
    df = _parse_event_log(logfile)

    # Ensure the DataFrame has the necessary columns
    if 'timestamp' not in df or 'var' not in df:
        raise ValueError("DataFrame must contain 'timestamp' and 'var' "
                         "columns.")

    # Filter the DataFrame to only include rows with VAR values
    var_df = df[df['var'].notna()]

    # Set up the plot
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot vertical lines for each VAR
    for _, row in var_df.iterrows():
        timestamp = row['timestamp']
        var_name = row['var']
        ax1.axvline(x=timestamp, color='black', linestyle='--')
        ax1.text(timestamp, 0.5, var_name, rotation=90,
                 verticalalignment='center',
                 horizontalalignment='left', color='black', fontsize=8)

    # Formatting the plot
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('VAR Names', color='r')
    ax1.tick_params(axis='y', labelcolor='r')

    # Format the x-axis to show datetime
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    fig.autofmt_xdate()  # Auto-format date labels for better readability

    # Create a second y-axis for dataset if needed (for illustration)
    ax2 = ax1.twinx()
    ax2.set_ylabel('Dataset', color='b')

    # Optionally, you can add dataset information on the secondary y-axis
    # Here we're just adding an empty plot for demonstration
    ax2.plot(df['timestamp'], [1] * len(df), color='b', linestyle='none',
             marker='o')  # Dummy data
    ax2.tick_params(axis='y', labelcolor='b')

    plt.title('VAR Timestamps with Vertical Lines')
    plt.grid(True)
    plt.tight_layout()

    plt.savefig("monitor_events.png")
    plt.close()


def plot_logs(
        resource_log: Optional[str] = 'log_resources.log',
        event_log: Optional[str] = 'log_events.log',
        show: Optional[bool] = False
) -> None:
    """
    Plots resource usage and event data from log files.

    This function reads resource and event logs from specified files and
    generates a plot that shows memory usage and active threads over time,
    as well as event markers for variable (VAR) events.

    :param resource_log: Path to the resource log file. Default is
        'log_resources.log'.

    :type resource_log: Optional[str]

    :param event_log: Path to the event log file. Default is 'log_events.log'.
    :type event_log: Optional[str]

    :param show: If True, display the plot. If False, save the plot to a file.
        Default is False.

    :type show: Optional[bool]

    :return: None
    :rtype: None
    """
    df_resources = _parse_resource_log(resource_log)
    df_events = _parse_event_log(event_log)

    # Ensure 'timestamp' column is in datetime format
    if not pd.api.types.is_datetime64_any_dtype(df_resources['timestamp']):
        df_resources['timestamp'] = pd.to_datetime(df_resources['timestamp'])

    if not pd.api.types.is_datetime64_any_dtype(df_events['timestamp']):
        df_events['timestamp'] = pd.to_datetime(df_events['timestamp'])

    # Create a single plot_logs
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot Memory Usage on primary y-axis
    ax1.set_xlabel('Timestamp')
    ax1.set_ylabel('Memory Usage (GB)', color='tab:blue')
    ax1.plot(df_resources['timestamp'], df_resources['memory_usage'],
                  color='tab:blue', label='Memory Usage')
    ax1.tick_params(axis='y', labelcolor='tab:blue')

    # Create a second y-axis for active threads
    ax1_twin = ax1.twinx()
    ax1_twin.set_ylabel('Active Threads', color='tab:red')
    ax1_twin.plot(df_resources['timestamp'], df_resources['active_threads'],
                       color='tab:red', linestyle='--', label='Active Threads')
    ax1_twin.tick_params(axis='y', labelcolor='tab:red')

    # Plot VAR events
    if df_events is not None:
        if 'timestamp' not in df_events or 'var' not in df_events:
            raise ValueError(
                "df_events must contain 'timestamp' and 'var' columns.")

        # Filter the DataFrame to only include rows with VAR values
        var_df = df_events[df_events['var'].notna()]

        # Plot vertical lines for each VAR on the primary axes
        for _, row in var_df.iterrows():
            timestamp = row['timestamp']
            var_name = row['var']
            ax1.axvline(x=timestamp, color='black', linestyle='--')
            ax1.text(timestamp, ax1.get_ylim()[1], var_name, rotation=90,
                     verticalalignment='top', horizontalalignment='left',
                     color='black', fontsize=8)

    # Formatting the x-axis with readable date and time labels
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    fig.autofmt_xdate()  # Auto-format date labels for better readability

    # Title and grid
    plt.title('Resource monitoring & events')
    plt.grid(True)
    plt.tight_layout()

    # Show the plot_logs
    plt.savefig("monitoring.png")
    if show:
        plt.show()
    else:
        plt.close()

