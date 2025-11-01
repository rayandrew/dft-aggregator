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
    Extract per-rank epoch boundaries from epoch.block events.

    Returns:
        dict: {(epoch, pid): (min_ts, max_ts)} for each (epoch, pid) pair
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

        pid = event.get('pid')
        if pid is None:
            continue

        ts = event.get('ts', 0)
        dur = event.get('dur', 0)
        te = ts + dur

        key = (epoch, pid)
        if key not in epoch_boundaries:
            epoch_boundaries[key] = (ts, te)
        else:
            # Extend boundary to include this event
            min_ts, max_te = epoch_boundaries[key]
            epoch_boundaries[key] = (min(min_ts, ts), max(max_te, te))

    return epoch_boundaries


def get_epoch_for_event(event, epoch_boundaries, process_tree):
    """
    Determine which epoch an event belongs to using per-rank boundaries.

    Args:
        event: Event to check
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict mapping child_pid -> parent_pid

    Returns:
        int or None: Epoch number if event falls within a rank's epoch boundaries
    """
    ts = event.get('ts', 0)
    pid = event.get('pid')

    if pid is None:
        return None

    # Determine which rank's boundaries to check
    # For worker processes, use parent's boundaries
    grouping_pid = process_tree.get(pid, pid)

    # Check all epochs for this rank
    for (epoch, boundary_pid), (min_ts, max_ts) in epoch_boundaries.items():
        if boundary_pid == grouping_pid and min_ts <= ts <= max_ts:
            return epoch

    return None


def build_process_tree(events):
    """
    Build process tree from fork/spawn events.

    Returns:
        dict: child_pid -> parent_pid mapping
    """
    process_tree = {}

    for event in events:
        if event.get('name') not in ['fork', 'spawn']:
            continue

        args = event.get('args', {})
        child_pid = args.get('ret')
        parent_pid = event.get('pid')

        if child_pid and parent_pid:
            process_tree[child_pid] = parent_pid

    return process_tree


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
    Compute per-epoch durations from per-rank boundaries.

    Combines all ranks' epoch boundaries to get the overall epoch duration.

    Args:
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}

    Returns:
        dict: {epoch: duration_seconds}
    """
    # Group by epoch and find min/max across all ranks
    epoch_ranges = {}

    for (epoch, pid), (min_ts, max_ts) in epoch_boundaries.items():
        if epoch not in epoch_ranges:
            epoch_ranges[epoch] = (min_ts, max_ts)
        else:
            current_min, current_max = epoch_ranges[epoch]
            epoch_ranges[epoch] = (min(current_min, min_ts), max(current_max, max_ts))

    # Convert to durations
    epoch_durations = {}
    for epoch, (min_ts, max_ts) in epoch_ranges.items():
        duration_us = max_ts - min_ts
        epoch_durations[epoch] = duration_us / 1_000_000

    return epoch_durations


def compute_time_by_category_name(events, epoch_boundaries, process_tree, category, name):
    """
    Compute total time for events matching category and name.

    Sums all durations for matching events, grouped by epoch.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict from build_process_tree()
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

        epoch = get_epoch_for_event(event, epoch_boundaries, process_tree)
        if epoch is None:
            continue

        dur = event.get('dur', 0)
        epoch_times[epoch] += dur / 1_000_000  # Convert to seconds

    return dict(epoch_times)


def compute_wall_clock_time_by_category_name(events, epoch_boundaries, process_tree, category, name):
    """
    Compute wall-clock elapsed time for events matching category and name.

    Measures elapsed time from first event start to last event end per (epoch, rank),
    then sums across ranks per epoch. This prevents cross-epoch bleeding.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict from build_process_tree()
        category: Event category to match
        name: Event name to match

    Returns:
        dict: {epoch: elapsed_seconds}
    """
    # Track min start and max end for each (epoch, grouping_pid) pair
    epoch_pid_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

    for event in events:
        if event.get('ph') != 'X':  # Only duration events
            continue

        if event.get('cat') != category or event.get('name') != name:
            continue

        epoch = get_epoch_for_event(event, epoch_boundaries, process_tree)
        if epoch is None:
            continue

        # Get grouping PID (use parent for workers, own PID for main process)
        pid = event.get('pid')
        grouping_pid = process_tree.get(pid, pid)

        ts = event.get('ts', 0)
        dur = event.get('dur', 0)
        te = ts + dur

        key = (epoch, grouping_pid)
        epoch_pid_ranges[key]['min_ts'] = min(epoch_pid_ranges[key]['min_ts'], ts)
        epoch_pid_ranges[key]['max_te'] = max(epoch_pid_ranges[key]['max_te'], te)

    # Compute per-rank wall-clock time, then sum across ranks per epoch
    epoch_times = defaultdict(float)
    for (epoch, grouping_pid), range_data in epoch_pid_ranges.items():
        duration_us = range_data['max_te'] - range_data['min_ts']
        duration_s = duration_us / 1_000_000
        epoch_times[epoch] += duration_s

    return dict(epoch_times)


def count_workers_raw(events, epoch_boundaries, process_tree):
    """
    Count workers from fork/spawn events.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict from build_process_tree()

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

        epoch = get_epoch_for_event(event, epoch_boundaries, process_tree)
        if epoch is None:
            continue

        # Each event represents one worker
        epoch_workers[epoch] += 1

    return dict(epoch_workers)


def compute_io_metrics_raw(events, epoch_boundaries, process_tree):
    """
    Compute I/O size and aggregate bandwidth from POSIX events.

    Aggregate bandwidth = Total I/O Size / Sum(all I/O operation durations)
    This measures the sum of individual operation bandwidths (can exceed physical limits with parallelism).

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict from build_process_tree()

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

        epoch = get_epoch_for_event(event, epoch_boundaries, process_tree)
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


def compute_effective_io_bandwidth_raw(events, epoch_boundaries, process_tree):
    """
    Compute effective I/O bandwidth using wall-clock elapsed time per rank.

    Effective bandwidth = Total I/O Size / Wall-clock elapsed time
    This measures actual throughput and cannot exceed physical LFS bandwidth limits.
    Use this to validate against peak cluster LFS bandwidth.

    Computes per-rank bandwidth then sums across ranks to prevent cross-epoch bleeding.

    Args:
        events: List of trace events
        epoch_boundaries: dict from extract_epoch_boundaries() - {(epoch, pid): (min_ts, max_ts)}
        process_tree: dict from build_process_tree()

    Returns:
        dict: {epoch: effective_bandwidth_mbps}
    """
    # Track per (epoch, grouping_pid)
    epoch_pid_sizes = defaultdict(float)
    epoch_pid_ranges = defaultdict(lambda: {'min_ts': float('inf'), 'max_te': 0})

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

        epoch = get_epoch_for_event(event, epoch_boundaries, process_tree)
        if epoch is None:
            continue

        # Get grouping PID (use parent for workers, own PID for main process)
        pid = event.get('pid')
        grouping_pid = process_tree.get(pid, pid)

        args = event.get('args', {})
        # Get size from ret (return value = bytes read/written)
        ret_val = args.get('ret', 0)
        ts = event.get('ts', 0)
        dur = event.get('dur', 0)

        # Only count successful I/O operations (ret > 0)
        if ret_val > 0:
            key = (epoch, grouping_pid)
            epoch_pid_sizes[key] += ret_val

            # Track timestamp range for wall-clock calculation
            te = ts + dur
            epoch_pid_ranges[key]['min_ts'] = min(epoch_pid_ranges[key]['min_ts'], ts)
            epoch_pid_ranges[key]['max_te'] = max(epoch_pid_ranges[key]['max_te'], te)

    # Calculate per-rank bandwidth, then sum across ranks per epoch
    epoch_bandwidths = defaultdict(float)
    for (epoch, grouping_pid), size in epoch_pid_sizes.items():
        key = (epoch, grouping_pid)
        if key in epoch_pid_ranges:
            elapsed_us = epoch_pid_ranges[key]['max_te'] - epoch_pid_ranges[key]['min_ts']
            elapsed_sec = elapsed_us / 1_000_000

            if elapsed_sec > 0:
                size_mb = size / (1024 * 1024)
                bandwidth = size_mb / elapsed_sec
                epoch_bandwidths[epoch] += bandwidth

    return dict(epoch_bandwidths)


def compute_metrics_raw(events):
    """Compute all metrics from raw events."""
    metrics = {}

    print("\n=== Computing Raw Metrics ===")

    # Build process tree first
    print("  - Building process tree...")
    process_tree = build_process_tree(events)
    print(f"    Found {len(process_tree)} child processes")

    # Extract epoch boundaries per rank
    print("  - Extracting per-rank epoch boundaries...")
    epoch_boundaries = extract_epoch_boundaries(events)
    # Count unique epochs
    unique_epochs = {epoch for epoch, _ in epoch_boundaries.keys()}
    print(f"    Found {len(unique_epochs)} epoch(s): {sorted(unique_epochs)}")

    # Trace duration
    print("  - Trace duration...")
    metrics['trace_duration'] = compute_trace_duration_raw(events)

    # Epoch durations
    print("  - Epoch durations...")
    metrics['epoch_duration'] = compute_epoch_durations_raw(epoch_boundaries)

    # Compute time (fetch.block) - total work
    print("  - Compute time (fetch.block)...")
    metrics['compute_time'] = compute_time_by_category_name(events, epoch_boundaries, process_tree, 'dataloader', 'fetch.block')

    # Compute time - wall-clock elapsed
    print("  - Compute time wall-clock (fetch.block)...")
    metrics['compute_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, process_tree, 'dataloader', 'fetch.block')

    # Fetch iter time - total work
    print("  - Fetch iter time...")
    metrics['fetch_iter_time'] = compute_time_by_category_name(events, epoch_boundaries, process_tree, 'dataloader', 'fetch.iter')

    # Fetch iter time - wall-clock elapsed
    print("  - Fetch iter time wall-clock...")
    metrics['fetch_iter_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, process_tree, 'dataloader', 'fetch.iter')

    # Num workers
    print("  - Num workers...")
    metrics['num_workers'] = count_workers_raw(events, epoch_boundaries, process_tree)

    # Preprocess time - total work
    print("  - Preprocess time...")
    metrics['preprocess_time'] = compute_time_by_category_name(events, epoch_boundaries, process_tree, 'data', 'preprocess')

    # Preprocess time - wall-clock elapsed
    print("  - Preprocess time wall-clock...")
    metrics['preprocess_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, process_tree, 'data', 'preprocess')

    # Get item time - total work
    print("  - Get item time...")
    metrics['getitem_time'] = compute_time_by_category_name(events, epoch_boundaries, process_tree, 'data', 'item')

    # Get item time - wall-clock elapsed
    print("  - Get item time wall-clock...")
    metrics['getitem_time_wall_clock'] = compute_wall_clock_time_by_category_name(events, epoch_boundaries, process_tree, 'data', 'item')

    # I/O metrics
    print("  - I/O size and aggregate bandwidth...")
    metrics['io_size_gb'], metrics['posix_bandwidth'] = compute_io_metrics_raw(events, epoch_boundaries, process_tree)

    print("  - Effective I/O bandwidth...")
    metrics['effective_bandwidth'] = compute_effective_io_bandwidth_raw(events, epoch_boundaries, process_tree)

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
