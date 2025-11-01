#!/usr/bin/env python3
"""
Analyze raw DFTracer traces without aggregation.

Uses exact event-level analysis to compute ground-truth metrics.
"""

import json
import gzip
import numpy as np
from collections import defaultdict
from pathlib import Path


def load_raw_traces(traces_dir):
    """
    Load all raw trace files from a directory.

    Args:
        traces_dir: Path to directory containing .pfw or .pfw.gz files

    Returns:
        list: All trace events from all files
    """
    traces_path = Path(traces_dir)
    events = []

    # Find all trace files
    trace_files = list(traces_path.glob("*.pfw")) + list(traces_path.glob("*.pfw.gz"))

    print(f"Found {len(trace_files)} trace file(s)")

    for trace_file in sorted(trace_files):
        print(f"  Loading {trace_file.name}...")

        # Parse newline-delimited JSON (same format as aggregated traces)
        if trace_file.suffix == '.gz':
            with gzip.open(trace_file, 'rt') as f:
                content = f.read()
        else:
            with open(trace_file, 'r') as f:
                content = f.read()

        # Parse line by line
        file_events = []
        for line in content.strip()[1:-1].split('\n'):
            line = line.strip().rstrip(',')
            if line:
                try:
                    event = json.loads(line)
                    file_events.append(event)
                except json.JSONDecodeError as e:
                    # Skip malformed lines
                    pass

        print(f"    Loaded {len(file_events)} events")
        events.extend(file_events)

    print(f"Loaded {len(events)} total events")
    return events


def extract_epoch_boundaries(events):
    """
    Extract epoch boundaries from epoch.block events.

    Returns:
        dict: {epoch: (min_ts, max_ts)} for each epoch
    """
    epoch_boundaries = {}

    for event in events:
        if event.get('name') != 'epoch.block':
            continue

        args = event.get('args', {})
        iter_count_str = args.get('iter_count')

        if iter_count_str is None:
            continue

        try:
            epoch = int(iter_count_str)
        except (ValueError, TypeError):
            continue

        ts = event.get('ts', 0)
        dur = event.get('dur', 0)
        te = ts + dur

        if epoch not in epoch_boundaries:
            epoch_boundaries[epoch] = (ts, te)
        else:
            # Extend boundary to include this event
            min_ts, max_te = epoch_boundaries[epoch]
            epoch_boundaries[epoch] = (min(min_ts, ts), max(max_te, te))

    return epoch_boundaries


def get_epoch_for_timestamp(ts, epoch_boundaries):
    """
    Determine which epoch a timestamp belongs to.

    Args:
        ts: Timestamp to check
        epoch_boundaries: dict from extract_epoch_boundaries()

    Returns:
        int or None: Epoch number if timestamp falls within boundaries
    """
    for epoch, (min_ts, max_ts) in epoch_boundaries.items():
        if min_ts <= ts <= max_ts:
            return epoch
    return None


def compute_trace_duration_raw(events):
    """
    Compute trace duration: max(ts + dur) - min(ts) for all events.

    Returns:
        float: Trace duration in seconds
    """
    if not events:
        return None

    min_ts = float('inf')
    max_ts = 0

    for event in events:
        ph = event.get('ph')
        if ph not in ['X', 'B', 'E']:  # Duration, Begin, End events
            continue

        ts = event.get('ts', 0)
        dur = event.get('dur', 0)

        min_ts = min(min_ts, ts)
        max_ts = max(max_ts, ts + dur)

    if min_ts == float('inf') or max_ts == 0:
        return None

    duration_us = max_ts - min_ts
    return duration_us / 1_000_000  # Convert to seconds


def compute_epoch_durations_raw(epoch_boundaries):
    """
    Compute per-epoch durations from boundaries.

    Args:
        epoch_boundaries: dict from extract_epoch_boundaries()

    Returns:
        dict: {epoch: duration_seconds}
    """
    epoch_durations = {}

    for epoch, (min_ts, max_ts) in epoch_boundaries.items():
        duration_us = max_ts - min_ts
        epoch_durations[epoch] = duration_us / 1_000_000

    return epoch_durations


def compute_time_by_category_name(events, epoch_boundaries, category, name):
    """
    Compute total time for events matching category and name.

    Sums all durations for matching events, grouped by epoch.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries()
        category: Event category to match
        name: Event name to match

    Returns:
        dict: {epoch: total_seconds}
    """
    epoch_times = defaultdict(float)

    for event in events:
        if event.get('ph') != 'X':  # Only duration events
            continue

        if event.get('cat') != category or event.get('name') != name:
            continue

        ts = event.get('ts', 0)
        epoch = get_epoch_for_timestamp(ts, epoch_boundaries)
        if epoch is None:
            continue

        dur = event.get('dur', 0)
        epoch_times[epoch] += dur / 1_000_000  # Convert to seconds

    return dict(epoch_times)


def compute_wall_clock_time_by_category_name(events, epoch_boundaries, category, name):
    """
    Compute wall-clock elapsed time for events matching category and name.

    Measures elapsed time from first event start to last event end per epoch,
    accounting for overlapping parallel operations.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries()
        category: Event category to match
        name: Event name to match

    Returns:
        dict: {epoch: elapsed_seconds}
    """
    # Track min start and max end for each epoch
    epoch_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

    for event in events:
        if event.get('ph') != 'X':  # Only duration events
            continue

        if event.get('cat') != category or event.get('name') != name:
            continue

        ts = event.get('ts', 0)
        epoch = get_epoch_for_timestamp(ts, epoch_boundaries)
        if epoch is None:
            continue

        dur = event.get('dur', 0)
        te = ts + dur

        epoch_ranges[epoch]['min_ts'] = min(epoch_ranges[epoch]['min_ts'], ts)
        epoch_ranges[epoch]['max_te'] = max(epoch_ranges[epoch]['max_te'], te)

    # Convert ranges to durations
    epoch_times = {}
    for epoch, range_data in epoch_ranges.items():
        duration_us = range_data['max_te'] - range_data['min_ts']
        epoch_times[epoch] = duration_us / 1_000_000  # Convert to seconds

    return epoch_times


def count_workers_raw(events, epoch_boundaries):
    """
    Count workers from fork/spawn events.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries()

    Returns:
        dict: {epoch: worker_count}
    """
    epoch_workers = defaultdict(int)

    for event in events:
        if event.get('ph') != 'X':
            continue

        if event.get('cat') != 'POSIX':
            continue

        event_name = event.get('name', '')
        if event_name not in ['fork', 'spawn']:
            continue

        ts = event.get('ts', 0)
        epoch = get_epoch_for_timestamp(ts, epoch_boundaries)
        if epoch is None:
            continue

        # Each event represents one worker
        epoch_workers[epoch] += 1

    return dict(epoch_workers)


def compute_io_metrics_raw(events, epoch_boundaries):
    """
    Compute I/O size and aggregate bandwidth from POSIX events.

    Aggregate bandwidth = Total I/O Size / Sum(all I/O operation durations)
    This measures the sum of individual operation bandwidths (can exceed physical limits with parallelism).

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries()

    Returns:
        tuple: (epoch_sizes_gb, epoch_bandwidths_mbps)
    """
    epoch_sizes = defaultdict(float)
    epoch_durations = defaultdict(float)

    # I/O operation names to track
    io_operations = {'read', 'write', 'pread', 'pwrite', 'pread64', 'pwrite64',
                     'readv', 'writev', 'preadv', 'pwritev', 'preadv2', 'pwritev2'}

    for event in events:
        if event.get('ph') != 'X':
            continue

        if event.get('cat') != 'POSIX':
            continue

        # Only count actual I/O operations
        event_name = event.get('name', '')
        if event_name not in io_operations:
            continue

        ts = event.get('ts', 0)
        epoch = get_epoch_for_timestamp(ts, epoch_boundaries)
        if epoch is None:
            continue

        args = event.get('args', {})
        # Get size from ret (return value = bytes read/written)
        ret_val = args.get('ret', 0)
        dur = event.get('dur', 0)

        # Only count successful I/O operations (ret > 0)
        if ret_val > 0:
            epoch_sizes[epoch] += ret_val
            epoch_durations[epoch] += dur

    # Convert sizes to GB
    epoch_sizes_gb = {
        epoch: size / (1024**3)
        for epoch, size in epoch_sizes.items()
    }

    # Compute aggregate bandwidth (MB/s)
    epoch_bandwidths = {}
    for epoch in epoch_sizes:
        size_mb = epoch_sizes[epoch] / (1024 * 1024)
        dur_sec = epoch_durations[epoch] / 1_000_000

        if dur_sec > 0:
            epoch_bandwidths[epoch] = size_mb / dur_sec
        else:
            epoch_bandwidths[epoch] = 0.0

    return epoch_sizes_gb, epoch_bandwidths


def compute_effective_io_bandwidth_raw(events, epoch_boundaries):
    """
    Compute effective I/O bandwidth using wall-clock elapsed time.

    Effective bandwidth = Total I/O Size / Wall-clock elapsed time
    This measures actual throughput and cannot exceed physical LFS bandwidth limits.
    Use this to validate against peak cluster LFS bandwidth.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries()

    Returns:
        dict: {epoch: effective_bandwidth_mbps}
    """
    epoch_sizes = defaultdict(float)
    epoch_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

    # I/O operation names to track
    io_operations = {'read', 'write', 'pread', 'pwrite', 'pread64', 'pwrite64',
                     'readv', 'writev', 'preadv', 'pwritev', 'preadv2', 'pwritev2'}

    for event in events:
        if event.get('ph') != 'X':
            continue

        if event.get('cat') != 'POSIX':
            continue

        # Only count actual I/O operations
        event_name = event.get('name', '')
        if event_name not in io_operations:
            continue

        ts = event.get('ts', 0)
        epoch = get_epoch_for_timestamp(ts, epoch_boundaries)
        if epoch is None:
            continue

        args = event.get('args', {})
        # Get size from ret (return value = bytes read/written)
        ret_val = args.get('ret', 0)
        dur = event.get('dur', 0)

        # Only count successful I/O operations (ret > 0)
        if ret_val > 0:
            epoch_sizes[epoch] += ret_val

            # Track timestamp range for wall-clock calculation
            te = ts + dur
            epoch_ranges[epoch]['min_ts'] = min(epoch_ranges[epoch]['min_ts'], ts)
            epoch_ranges[epoch]['max_te'] = max(epoch_ranges[epoch]['max_te'], te)

    # Compute effective bandwidth (MB/s)
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


def compute_metrics_raw(events):
    """Compute all metrics from raw events."""
    metrics = {}

    print("\n=== Computing Raw Metrics ===")

    # Extract epoch boundaries first
    print("  - Extracting epoch boundaries...")
    epoch_boundaries = extract_epoch_boundaries(events)
    print(f"    Found {len(epoch_boundaries)} epoch(s): {sorted(epoch_boundaries.keys())}")

    # Trace duration
    print("  - Trace duration...")
    metrics['trace_duration'] = compute_trace_duration_raw(events)

    # Epoch durations
    print("  - Epoch durations...")
    metrics['epoch_duration'] = compute_epoch_durations_raw(epoch_boundaries)

    # Compute time (fetch.block) - total work
    print("  - Compute time (fetch.block)...")
    metrics['compute_time'] = compute_time_by_category_name(events, epoch_boundaries, 'dataloader', 'fetch.block')

    # Compute time - wall-clock elapsed
    print("  - Compute time wall-clock (fetch.block)...")
    metrics['compute_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, 'dataloader', 'fetch.block')

    # Fetch iter time - total work
    print("  - Fetch iter time...")
    metrics['fetch_iter_time'] = compute_time_by_category_name(events, epoch_boundaries, 'dataloader', 'fetch.iter')

    # Fetch iter time - wall-clock elapsed
    print("  - Fetch iter time wall-clock...")
    metrics['fetch_iter_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, 'dataloader', 'fetch.iter')

    # Num workers
    print("  - Num workers...")
    metrics['num_workers'] = count_workers_raw(events, epoch_boundaries)

    # Preprocess time - total work
    print("  - Preprocess time...")
    metrics['preprocess_time'] = compute_time_by_category_name(events, epoch_boundaries, 'data', 'preprocess')

    # Preprocess time - wall-clock elapsed
    print("  - Preprocess time wall-clock...")
    metrics['preprocess_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, 'data', 'preprocess')

    # Get item time - total work
    print("  - Get item time...")
    metrics['getitem_time'] = compute_time_by_category_name(events, epoch_boundaries, 'data', 'item')

    # Get item time - wall-clock elapsed
    print("  - Get item time wall-clock...")
    metrics['getitem_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, 'data', 'item')

    # I/O metrics
    print("  - I/O size and aggregate bandwidth...")
    metrics['io_size_gb'], metrics['posix_bandwidth'] = compute_io_metrics_raw(events, epoch_boundaries)

    print("  - Effective I/O bandwidth...")
    metrics['effective_bandwidth'] = compute_effective_io_bandwidth_raw(events, epoch_boundaries)

    return metrics


def format_metric(values):
    """Format metric as mean ± std or single value."""
    if values is None:
        return "N/A"

    if isinstance(values, dict):
        values = list(values.values())

    if not values:
        return "N/A"

    if len(values) == 1:
        return f"{values[0]:.2f}"

    mean = np.mean(values)
    std = np.std(values)
    return f"{mean:.2f} ± {std:.2f}"


def print_summary_table(metrics):
    """Print summary table with all metrics."""
    print("\n" + "="*80)
    print("RAW TRACE SUMMARY")
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
    import sys

    # Get traces directory
    if len(sys.argv) > 1:
        traces_dir = sys.argv[1]
    else:
        traces_dir = "/Users/rayandrew/Projects/dftracer/dftracer-utils/split"

    print(f"\n=== Loading Raw Traces from: {traces_dir} ===")
    events = load_raw_traces(traces_dir)

    # Compute metrics
    metrics = compute_metrics_raw(events)

    # Print summary
    print_summary_table(metrics)


if __name__ == '__main__':
    main()
