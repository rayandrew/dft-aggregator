#!/usr/bin/env python3
"""
Generate summary table from aggregated DFTracer traces.

Computes per-epoch metrics and displays them with mean ± std.
"""

import json
import gzip
import numpy as np
from collections import defaultdict


def load_aggregated_traces(filepath):
    """Load and parse aggregated trace data from gzipped JSON.

    Returns:
        tuple: (traces, metadata) where metadata contains trace_duration and boundary_durations
    """
    with gzip.open(filepath, 'rt') as f:
        content = f.read()
        traces = []
        metadata = None

        for line in content.strip()[1:-1].split('\n'):
            line = line.strip().rstrip(',')
            if line:
                event = json.loads(line)

                # Extract metadata event
                if event.get('ph') == 'M' and event.get('name') == 'trace_metadata':
                    metadata = event.get('args', {})
                else:
                    traces.append(event)

    return traces, metadata


def get_epoch(event):
    """Extract epoch number from event args."""
    args = event.get('args', {})
    epoch = args.get('epoch', None)
    return int(epoch) if epoch is not None else None


def extract_durations_from_metadata(metadata):
    """
    Extract trace duration and epoch durations from metadata.

    Args:
        metadata: Metadata dict from trace_metadata event

    Returns:
        tuple: (trace_duration_seconds, epoch_durations_dict)
    """
    if not metadata:
        return None, {}

    # Get trace duration (in microseconds, convert to seconds)
    trace_duration_us = metadata.get('trace_duration', 0)
    trace_duration_s = trace_duration_us / 1_000_000

    # Get boundary durations (epoch durations)
    boundary_durations = metadata.get('boundary_durations', {})
    epoch_durations_us = boundary_durations.get('epoch', {})

    # Convert epoch durations from microseconds to seconds
    epoch_durations = {
        int(epoch): duration / 1_000_000
        for epoch, duration in epoch_durations_us.items()
    }

    return trace_duration_s, epoch_durations


def compute_aggregated_time(traces, category, name):
    """
    Compute total aggregated time for events matching category and name.

    Uses dur.total to sum actual event durations (not wall-clock elapsed time).

    Returns:
        dict: {epoch: total_seconds}
    """
    epoch_times = defaultdict(float)

    for event in traces:
        if event.get('ph') != 'C':
            continue

        if event.get('cat') != category or event.get('name') != name:
            continue

        epoch = get_epoch(event)
        if epoch is None:
            continue

        args = event.get('args', {})
        dur_info = args.get('dur', {})

        # Use total duration (sum of all event durations in this bucket)
        total_dur = dur_info.get('total', 0)
        epoch_times[epoch] += total_dur / 1_000_000  # Convert to seconds

    return dict(epoch_times)


def compute_wall_clock_time(traces, category, name):
    """
    Compute wall-clock elapsed time for events matching category and name.

    This measures the actual elapsed time from first event start to last event end,
    accounting for overlapping parallel operations.

    Returns:
        dict: {epoch: elapsed_seconds}
    """
    # Track min start and max end for each epoch
    epoch_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

    for event in traces:
        if event.get('ph') != 'C':
            continue

        if event.get('cat') != category or event.get('name') != name:
            continue

        epoch = get_epoch(event)
        if epoch is None:
            continue

        args = event.get('args', {})

        # Get timestamp range and duration from this bucket
        ts_range = args.get('ts_range', [])
        dur = args.get('dur', {})

        if len(ts_range) >= 2 and 'max' in dur:
            first_ts = ts_range[0]
            last_ts = ts_range[1]
            max_dur = dur['max']

            # Track global min/max across all buckets
            epoch_ranges[epoch]['min_ts'] = min(epoch_ranges[epoch]['min_ts'], first_ts)
            epoch_ranges[epoch]['max_te'] = max(epoch_ranges[epoch]['max_te'], last_ts + max_dur)

    # Convert ranges to durations
    epoch_times = {}
    for epoch, range_data in epoch_ranges.items():
        duration_us = range_data['max_te'] - range_data['min_ts']
        epoch_times[epoch] = duration_us / 1_000_000  # Convert to seconds

    return epoch_times


def count_workers(traces):
    """
    Count number of workers from fork/spawn events.

    Returns:
        dict: {epoch: worker_count}
    """
    epoch_workers = {}

    for event in traces:
        if event.get('ph') != 'C':
            continue

        if event.get('cat') != 'POSIX':
            continue

        name = event.get('name', '')
        if name not in ['fork', 'spawn']:
            continue

        epoch = get_epoch(event)
        if epoch is None:
            continue

        args = event.get('args', {})
        count = args.get('count', 0)

        if epoch in epoch_workers:
            epoch_workers[epoch] += count
        else:
            epoch_workers[epoch] = count

    return epoch_workers


def compute_io_metrics(traces):
    """
    Compute I/O size and aggregate bandwidth for POSIX operations.

    Aggregate bandwidth = Total I/O Size / Sum(all I/O operation durations)
    This measures the sum of individual operation bandwidths (can exceed physical limits with parallelism).

    Returns:
        tuple: (epoch_sizes_dict, epoch_bandwidths_dict)
    """
    epoch_sizes = defaultdict(float)
    epoch_durations = defaultdict(float)

    for event in traces:
        if event.get('ph') != 'C':
            continue

        if event.get('cat') != 'POSIX':
            continue

        epoch = get_epoch(event)
        if epoch is None:
            continue

        args = event.get('args', {})

        # Accumulate size
        size_info = args.get('size', {})
        total_size = size_info.get('total', 0)
        epoch_sizes[epoch] += total_size

        # Use total duration (sum of all I/O operation durations)
        dur_info = args.get('dur', {})
        total_dur = dur_info.get('total', 0)
        epoch_durations[epoch] += total_dur

    # Calculate aggregate bandwidth (MB/s)
    epoch_bandwidths = {}
    for epoch in epoch_sizes:
        size_mb = epoch_sizes[epoch] / (1024 * 1024)
        dur_sec = epoch_durations[epoch] / 1_000_000

        if dur_sec > 0:
            epoch_bandwidths[epoch] = size_mb / dur_sec
        else:
            epoch_bandwidths[epoch] = 0.0

    # Convert sizes to GB
    epoch_sizes_gb = {epoch: size / (1024**3) for epoch, size in epoch_sizes.items()}

    return dict(epoch_sizes_gb), dict(epoch_bandwidths)


def compute_effective_io_bandwidth(traces):
    """
    Compute effective I/O bandwidth using wall-clock elapsed time.

    Effective bandwidth = Total I/O Size / Wall-clock elapsed time
    This measures actual throughput and cannot exceed physical LFS bandwidth limits.
    Use this to validate against peak cluster LFS bandwidth.

    Returns:
        dict: {epoch: effective_bandwidth_mbps}
    """
    epoch_sizes = defaultdict(float)
    epoch_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

    for event in traces:
        if event.get('ph') != 'C':
            continue

        if event.get('cat') != 'POSIX':
            continue

        epoch = get_epoch(event)
        if epoch is None:
            continue

        args = event.get('args', {})

        # Accumulate size
        size_info = args.get('size', {})
        total_size = size_info.get('total', 0)
        epoch_sizes[epoch] += total_size

        # Track timestamp range for wall-clock calculation
        ts_range = args.get('ts_range', [])
        dur = args.get('dur', {})

        if len(ts_range) >= 2 and 'max' in dur:
            first_ts = ts_range[0]
            last_ts = ts_range[1]
            max_dur = dur['max']

            epoch_ranges[epoch]['min_ts'] = min(epoch_ranges[epoch]['min_ts'], first_ts)
            epoch_ranges[epoch]['max_te'] = max(epoch_ranges[epoch]['max_te'], last_ts + max_dur)

    # Calculate effective bandwidth (MB/s)
    epoch_bandwidths = {}
    for epoch in epoch_sizes:
        if epoch in epoch_ranges:
            elapsed_us = epoch_ranges[epoch]['max_te'] - epoch_ranges[epoch]['min_ts']
            elapsed_sec = elapsed_us / 1_000_000

            if elapsed_sec > 0:
                size_mb = epoch_sizes[epoch] / (1024 * 1024)
                epoch_bandwidths[epoch] = size_mb / elapsed_sec
            else:
                epoch_bandwidths[epoch] = 0.0
        else:
            epoch_bandwidths[epoch] = 0.0

    return dict(epoch_bandwidths)


def format_metric(values):
    """
    Format metric values as mean ± std or single value.

    Args:
        values: dict or list of values

    Returns:
        str: Formatted string
    """
    if isinstance(values, dict):
        values = list(values.values())

    if not values:
        return "N/A"

    if len(values) == 1:
        return f"{values[0]:.2f}"
    else:
        mean = np.mean(values)
        std = np.std(values)
        return f"{mean:.2f} ± {std:.2f}"


def print_summary_table(metrics):
    """Print summary table with all metrics."""
    print("\n" + "="*80)
    print("TRACE SUMMARY")
    print("="*80)
    print()

    # Determine number of epochs
    all_epochs = set()
    for metric_dict in metrics.values():
        if isinstance(metric_dict, dict):
            all_epochs.update(metric_dict.keys())

    num_epochs = len(all_epochs)
    print(f"Number of Epochs: {num_epochs}")
    print()

    # Print table
    print(f"{'Metric':<35} | {'Value':<25}")
    print("-" * 80)

    # Trace duration (global)
    if 'trace_duration' in metrics and metrics['trace_duration'] is not None:
        print(f"{'Trace Duration (s)':<35} | {metrics['trace_duration']:.2f}")
    else:
        print(f"{'Trace Duration (s)':<35} | N/A")

    # Epoch duration (per-epoch, with stats)
    if 'epoch_duration' in metrics and metrics['epoch_duration']:
        print(f"{'Epoch Duration (s)':<35} | {format_metric(metrics['epoch_duration']):<25}")
    else:
        print(f"{'Epoch Duration (s)':<35} | N/A")

    # App level - Total work
    print(f"{'Compute Time (s)':<35} | {format_metric(metrics['compute_time']):<25}")
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {format_metric(metrics['compute_time_wall_clock']):<25}")
    print(f"{'Fetch Iter Time (s)':<35} | {format_metric(metrics['fetch_iter_time']):<25}")
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {format_metric(metrics['fetch_iter_time_wall_clock']):<25}")

    # Data loader level
    workers = metrics['num_workers']
    if isinstance(workers, dict):
        workers_list = list(workers.values())
        if len(set(workers_list)) == 1:
            # All epochs have same number of workers
            print(f"{'Num Workers':<35} | {workers_list[0]:<25}")
        else:
            print(f"{'Num Workers':<35} | {format_metric(workers):<25}")
    else:
        print(f"{'Num Workers':<35} | {workers:<25}")

    print(f"{'Preprocess Time (s)':<35} | {format_metric(metrics['preprocess_time']):<25}")
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {format_metric(metrics['preprocess_time_wall_clock']):<25}")
    print(f"{'Get Item Time (s)':<35} | {format_metric(metrics['getitem_time']):<25}")
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {format_metric(metrics['getitem_time_wall_clock']):<25}")

    # System call level
    print(f"{'Total I/O Size (GB)':<35} | {format_metric(metrics['io_size_gb']):<25}")
    print(f"{'POSIX Bandwidth (MB/s)':<35} | {format_metric(metrics['posix_bandwidth']):<25}")
    print(f"{'  Effective Bandwidth (MB/s)':<35} | {format_metric(metrics['effective_bandwidth']):<25}")

    print()


def main():
    """Main analysis pipeline."""
    print("\n=== Loading Aggregated Traces ===")
    traces, metadata = load_aggregated_traces('aggregated.pfw.gz')
    print(f"Loaded {len(traces)} trace events")

    # Extract metadata
    if metadata:
        print(f"Found trace metadata: trace_duration={metadata.get('trace_duration', 0)} us")

    # Compute all metrics
    print("\n=== Computing Metrics ===")

    metrics = {}

    # Extract trace duration and epoch durations from metadata
    print("  - Extracting durations from metadata...")
    trace_duration, epoch_durations = extract_durations_from_metadata(metadata)
    metrics['trace_duration'] = trace_duration
    metrics['epoch_duration'] = epoch_durations

    # Compute time (fetch.block) - total work
    print("  - Compute time (fetch.block)...")
    metrics['compute_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.block')

    # Compute time - wall-clock elapsed
    print("  - Compute time wall-clock (fetch.block)...")
    metrics['compute_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.block')

    # Fetch iter time - total work
    print("  - Fetch iter time...")
    metrics['fetch_iter_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.iter')

    # Fetch iter time - wall-clock elapsed
    print("  - Fetch iter time wall-clock...")
    metrics['fetch_iter_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.iter')

    # Num workers
    print("  - Num workers...")
    metrics['num_workers'] = count_workers(traces)

    # Preprocess time - total work
    print("  - Preprocess time...")
    metrics['preprocess_time'] = compute_aggregated_time(traces, 'data', 'preprocess')

    # Preprocess time - wall-clock elapsed
    print("  - Preprocess time wall-clock...")
    metrics['preprocess_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'preprocess')

    # Get item time - total work
    print("  - Get item time...")
    metrics['getitem_time'] = compute_aggregated_time(traces, 'data', 'item')

    # Get item time - wall-clock elapsed
    print("  - Get item time wall-clock...")
    metrics['getitem_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'item')

    # I/O metrics
    print("  - I/O size and aggregate bandwidth...")
    metrics['io_size_gb'], metrics['posix_bandwidth'] = compute_io_metrics(traces)

    print("  - Effective I/O bandwidth...")
    metrics['effective_bandwidth'] = compute_effective_io_bandwidth(traces)

    # Print summary table
    print_summary_table(metrics)


if __name__ == '__main__':
    main()
