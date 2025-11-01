#!/usr/bin/env python3
"""
Compare two DFTracer traces (raw or aggregated).

Shows percentage differences between metrics from two trace files.
Supports both raw traces (directory) and aggregated traces (.pfw.gz file).
"""

import json
import gzip
import numpy as np
import os
from collections import defaultdict
from pathlib import Path


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
    Compute I/O size and bandwidth for POSIX operations.

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

    # Calculate bandwidth (MB/s)
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


def compute_metrics(traces, metadata):
    """Compute all metrics from traces and metadata."""
    metrics = {}

    # Extract trace duration and epoch durations from metadata
    trace_duration, epoch_durations = extract_durations_from_metadata(metadata)
    metrics['trace_duration'] = trace_duration
    metrics['epoch_duration'] = epoch_durations

    # Compute time (fetch.block) - total work
    metrics['compute_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.block')
    # Compute time - wall-clock elapsed
    metrics['compute_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.block')

    # Fetch iter time - total work
    metrics['fetch_iter_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.iter')
    # Fetch iter time - wall-clock elapsed
    metrics['fetch_iter_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.iter')

    # Num workers
    metrics['num_workers'] = count_workers(traces)

    # Preprocess time - total work
    metrics['preprocess_time'] = compute_aggregated_time(traces, 'data', 'preprocess')
    # Preprocess time - wall-clock elapsed
    metrics['preprocess_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'preprocess')

    # Get item time - total work
    metrics['getitem_time'] = compute_aggregated_time(traces, 'data', 'item')
    # Get item time - wall-clock elapsed
    metrics['getitem_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'item')

    # I/O metrics
    metrics['io_size_gb'], metrics['posix_bandwidth'] = compute_io_metrics(traces)

    return metrics


def aggregate_metric(metric_value):
    """Aggregate a metric value (scalar, dict, or list) to a single number."""
    if metric_value is None:
        return None

    if isinstance(metric_value, dict):
        values = list(metric_value.values())
        if not values:
            return None
        return np.mean(values)

    if isinstance(metric_value, (list, np.ndarray)):
        if not metric_value:
            return None
        return np.mean(metric_value)

    return float(metric_value)


def compute_percentage_diff(baseline, comparison):
    """
    Compute percentage difference: ((comparison - baseline) / baseline) * 100

    Returns:
        float: Percentage difference (positive = increase, negative = decrease)
    """
    if baseline is None or comparison is None:
        return None

    if baseline == 0:
        if comparison == 0:
            return 0.0
        return float('inf')

    return ((comparison - baseline) / baseline) * 100


def format_diff(diff):
    """Format percentage difference with appropriate sign and color indicator."""
    if diff is None:
        return "N/A"

    if diff == float('inf'):
        return "+∞%"

    if diff == 0.0:
        return "0.00% (=)"

    sign = "+" if diff > 0 else ""

    # Add indicator
    if abs(diff) < 0.01:
        indicator = "≈"  # Nearly equal
    elif diff > 0:
        indicator = "↑"  # Increase
    else:
        indicator = "↓"  # Decrease

    return f"{sign}{diff:.2f}% ({indicator})"


def format_metric_with_stats(metric_value):
    """Format metric value with mean ± std if it's a dict/list, otherwise just the value."""
    if metric_value is None:
        return "N/A"

    if isinstance(metric_value, dict):
        values = list(metric_value.values())
        if not values:
            return "N/A"
        if len(values) == 1:
            return f"{values[0]:.2f}"
        mean = np.mean(values)
        std = np.std(values)
        return f"{mean:.2f} ± {std:.2f}"

    if isinstance(metric_value, (list, np.ndarray)):
        if not metric_value:
            return "N/A"
        if len(metric_value) == 1:
            return f"{metric_value[0]:.2f}"
        mean = np.mean(metric_value)
        std = np.std(metric_value)
        return f"{mean:.2f} ± {std:.2f}"

    return f"{metric_value:.2f}"


def print_comparison_table(baseline_metrics, comparison_metrics, baseline_name, comparison_name):
    """Print comparison table showing percentage differences."""
    print("\n" + "="*120)
    print(f"TRACE COMPARISON: {baseline_name} vs {comparison_name}")
    print("="*120)
    print()

    print(f"{'Metric':<35} | {'Baseline':<30} | {'Comparison':<30} | {'Difference':<20}")
    print("-" * 120)

    # Trace duration (global, single value)
    baseline_str = format_metric_with_stats(baseline_metrics['trace_duration'])
    comparison_str = format_metric_with_stats(comparison_metrics['trace_duration'])
    baseline_val = aggregate_metric(baseline_metrics['trace_duration'])
    comparison_val = aggregate_metric(comparison_metrics['trace_duration'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Trace Duration (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Epoch duration (with stats)
    baseline_str = format_metric_with_stats(baseline_metrics['epoch_duration'])
    comparison_str = format_metric_with_stats(comparison_metrics['epoch_duration'])
    baseline_val = aggregate_metric(baseline_metrics['epoch_duration'])
    comparison_val = aggregate_metric(comparison_metrics['epoch_duration'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Epoch Duration (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Compute time - total work
    baseline_str = format_metric_with_stats(baseline_metrics['compute_time'])
    comparison_str = format_metric_with_stats(comparison_metrics['compute_time'])
    baseline_val = aggregate_metric(baseline_metrics['compute_time'])
    comparison_val = aggregate_metric(comparison_metrics['compute_time'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Compute Time (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Compute time - wall-clock elapsed
    baseline_str = format_metric_with_stats(baseline_metrics['compute_time_wall_clock'])
    comparison_str = format_metric_with_stats(comparison_metrics['compute_time_wall_clock'])
    baseline_val = aggregate_metric(baseline_metrics['compute_time_wall_clock'])
    comparison_val = aggregate_metric(comparison_metrics['compute_time_wall_clock'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Fetch iter time - total work
    baseline_str = format_metric_with_stats(baseline_metrics['fetch_iter_time'])
    comparison_str = format_metric_with_stats(comparison_metrics['fetch_iter_time'])
    baseline_val = aggregate_metric(baseline_metrics['fetch_iter_time'])
    comparison_val = aggregate_metric(comparison_metrics['fetch_iter_time'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Fetch Iter Time (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Fetch iter time - wall-clock elapsed
    baseline_str = format_metric_with_stats(baseline_metrics['fetch_iter_time_wall_clock'])
    comparison_str = format_metric_with_stats(comparison_metrics['fetch_iter_time_wall_clock'])
    baseline_val = aggregate_metric(baseline_metrics['fetch_iter_time_wall_clock'])
    comparison_val = aggregate_metric(comparison_metrics['fetch_iter_time_wall_clock'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Num workers (special case - integer, no stats needed)
    baseline_val = aggregate_metric(baseline_metrics['num_workers'])
    comparison_val = aggregate_metric(comparison_metrics['num_workers'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    baseline_str = f"{int(baseline_val)}" if baseline_val is not None else "N/A"
    comparison_str = f"{int(comparison_val)}" if comparison_val is not None else "N/A"
    print(f"{'Num Workers':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Preprocess time - total work
    baseline_str = format_metric_with_stats(baseline_metrics['preprocess_time'])
    comparison_str = format_metric_with_stats(comparison_metrics['preprocess_time'])
    baseline_val = aggregate_metric(baseline_metrics['preprocess_time'])
    comparison_val = aggregate_metric(comparison_metrics['preprocess_time'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Preprocess Time (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Preprocess time - wall-clock elapsed
    baseline_str = format_metric_with_stats(baseline_metrics['preprocess_time_wall_clock'])
    comparison_str = format_metric_with_stats(comparison_metrics['preprocess_time_wall_clock'])
    baseline_val = aggregate_metric(baseline_metrics['preprocess_time_wall_clock'])
    comparison_val = aggregate_metric(comparison_metrics['preprocess_time_wall_clock'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Get item time - total work
    baseline_str = format_metric_with_stats(baseline_metrics['getitem_time'])
    comparison_str = format_metric_with_stats(comparison_metrics['getitem_time'])
    baseline_val = aggregate_metric(baseline_metrics['getitem_time'])
    comparison_val = aggregate_metric(comparison_metrics['getitem_time'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Get Item Time (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # Get item time - wall-clock elapsed
    baseline_str = format_metric_with_stats(baseline_metrics['getitem_time_wall_clock'])
    comparison_str = format_metric_with_stats(comparison_metrics['getitem_time_wall_clock'])
    baseline_val = aggregate_metric(baseline_metrics['getitem_time_wall_clock'])
    comparison_val = aggregate_metric(comparison_metrics['getitem_time_wall_clock'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'  Wall-Clock Elapsed (s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # I/O size
    baseline_str = format_metric_with_stats(baseline_metrics['io_size_gb'])
    comparison_str = format_metric_with_stats(comparison_metrics['io_size_gb'])
    baseline_val = aggregate_metric(baseline_metrics['io_size_gb'])
    comparison_val = aggregate_metric(comparison_metrics['io_size_gb'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'Total I/O Size (GB)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    # POSIX bandwidth
    baseline_str = format_metric_with_stats(baseline_metrics['posix_bandwidth'])
    comparison_str = format_metric_with_stats(comparison_metrics['posix_bandwidth'])
    baseline_val = aggregate_metric(baseline_metrics['posix_bandwidth'])
    comparison_val = aggregate_metric(comparison_metrics['posix_bandwidth'])
    diff = compute_percentage_diff(baseline_val, comparison_val)
    print(f"{'POSIX Bandwidth (MB/s)':<35} | {baseline_str:<30} | {comparison_str:<30} | {format_diff(diff):<20}")

    print()


def main():
    """Main comparison pipeline."""
    import sys

    # Parse arguments
    if len(sys.argv) != 3:
        print("Usage: python comparison.py <baseline.pfw.gz> <comparison.pfw.gz>")
        print("\nExample:")
        print("  python comparison.py aggregated.pfw.gz aggregated.pfw.gz")
        sys.exit(1)

    baseline_file = sys.argv[1]
    comparison_file = sys.argv[2]

    # Load baseline
    print(f"\n=== Loading Baseline: {baseline_file} ===")
    baseline_traces, baseline_metadata = load_aggregated_traces(baseline_file)
    print(f"Loaded {len(baseline_traces)} trace events")

    # Load comparison
    print(f"\n=== Loading Comparison: {comparison_file} ===")
    comparison_traces, comparison_metadata = load_aggregated_traces(comparison_file)
    print(f"Loaded {len(comparison_traces)} trace events")

    # Compute metrics
    print("\n=== Computing Metrics ===")
    print("  - Baseline metrics...")
    baseline_metrics = compute_metrics(baseline_traces, baseline_metadata)

    print("  - Comparison metrics...")
    comparison_metrics = compute_metrics(comparison_traces, comparison_metadata)

    # Print comparison
    print_comparison_table(baseline_metrics, comparison_metrics, baseline_file, comparison_file)


if __name__ == '__main__':
    main()
