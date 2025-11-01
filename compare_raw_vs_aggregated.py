#!/usr/bin/env python3
"""
Compare raw traces vs aggregated traces to validate aggregation accuracy.

Usage:
    python compare_raw_vs_aggregated.py <raw_traces_dir> <aggregated_file>

Example:
    python compare_raw_vs_aggregated.py /path/to/raw/traces aggregated.pfw.gz
"""

import sys
import numpy as np
from raw_analysis import load_raw_traces, compute_metrics_raw
from summary_table import load_aggregated_traces, extract_durations_from_metadata
from summary_table import (
    compute_aggregated_time,
    compute_wall_clock_time,
    count_workers,
    compute_io_metrics,
    compute_effective_io_bandwidth
)


def compute_aggregated_metrics(traces, metadata):
    """Compute all metrics from aggregated traces."""
    metrics = {}

    # Extract trace duration and epoch durations from metadata
    trace_duration, epoch_durations = extract_durations_from_metadata(metadata)
    metrics['trace_duration'] = trace_duration
    metrics['epoch_duration'] = epoch_durations

    # Compute time (fetch.block)
    metrics['compute_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.block')
    metrics['compute_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.block')

    # Fetch iter time
    metrics['fetch_iter_time'] = compute_aggregated_time(traces, 'dataloader', 'fetch.iter')
    metrics['fetch_iter_time_wall_clock'] = compute_wall_clock_time(traces, 'dataloader', 'fetch.iter')

    # Num workers
    metrics['num_workers'] = count_workers(traces)

    # Preprocess time
    metrics['preprocess_time'] = compute_aggregated_time(traces, 'data', 'preprocess')
    metrics['preprocess_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'preprocess')

    # Get item time
    metrics['getitem_time'] = compute_aggregated_time(traces, 'data', 'item')
    metrics['getitem_time_wall_clock'] = compute_wall_clock_time(traces, 'data', 'item')

    # I/O metrics
    metrics['io_size_gb'], metrics['posix_bandwidth'] = compute_io_metrics(traces)
    metrics['effective_bandwidth'] = compute_effective_io_bandwidth(traces)

    return metrics


def aggregate_metric(metric_value):
    """Convert metric to single scalar value."""
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
    """Compute percentage difference."""
    if baseline is None or comparison is None:
        return None
    if baseline == 0:
        return None
    return ((comparison - baseline) / baseline) * 100


def format_metric_with_std(metric_value):
    """Format metric with mean ± std if applicable."""
    if metric_value is None:
        return "N/A"

    if isinstance(metric_value, dict):
        values = list(metric_value.values())
        if not values:
            return "N/A"
        if len(values) == 1:
            return f"{values[0]:.2f}"
        return f"{np.mean(values):.2f} ± {np.std(values):.2f}"

    return f"{metric_value:.2f}"


def print_comparison(raw_metrics, agg_metrics):
    """Print comparison table."""
    print("\n" + "="*100)
    print("RAW vs AGGREGATED COMPARISON")
    print("="*100)
    print()
    print(f"{'Metric':<40} | {'Raw':<20} | {'Aggregated':<20} | {'Diff %':<10}")
    print("-" * 100)

    metrics_to_compare = [
        ('Trace Duration (s)', 'trace_duration'),
        ('Epoch Duration (s)', 'epoch_duration'),
        ('Compute Time (s)', 'compute_time'),
        ('  Wall-Clock (s)', 'compute_time_wall_clock'),
        ('Fetch Iter Time (s)', 'fetch_iter_time'),
        ('  Wall-Clock (s)', 'fetch_iter_time_wall_clock'),
        ('Num Workers', 'num_workers'),
        ('Preprocess Time (s)', 'preprocess_time'),
        ('  Wall-Clock (s)', 'preprocess_time_wall_clock'),
        ('Get Item Time (s)', 'getitem_time'),
        ('  Wall-Clock (s)', 'getitem_time_wall_clock'),
        ('I/O Size (GB)', 'io_size_gb'),
        ('POSIX Bandwidth (MB/s)', 'posix_bandwidth'),
        ('  Effective Bandwidth (MB/s)', 'effective_bandwidth'),
    ]

    all_accurate = True
    threshold = 5.0  # 5% threshold

    for label, key in metrics_to_compare:
        raw_val = raw_metrics.get(key)
        agg_val = agg_metrics.get(key)

        raw_str = format_metric_with_std(raw_val)
        agg_str = format_metric_with_std(agg_val)

        # Compute difference on mean values
        raw_mean = aggregate_metric(raw_val)
        agg_mean = aggregate_metric(agg_val)
        diff = compute_percentage_diff(raw_mean, agg_mean)

        if diff is not None:
            abs_diff = abs(diff)
            if abs_diff > threshold:
                status = "✗"
                all_accurate = False
            else:
                status = "✓"

            diff_str = f"{diff:+.2f}% {status}"
        else:
            diff_str = "N/A"

        print(f"{label:<40} | {raw_str:<20} | {agg_str:<20} | {diff_str:<10}")

    print()
    print("="*100)

    if all_accurate:
        print("✓ All metrics within 5% threshold - Aggregation is accurate!")
    else:
        print("✗ Some metrics exceed 5% threshold - Review differences above")

    print("="*100)
    print()


def main():
    """Main comparison pipeline."""
    if len(sys.argv) != 3:
        print("Usage: python compare_raw_vs_aggregated.py <raw_traces_dir> <aggregated_file>")
        print("\nExample:")
        print("  python compare_raw_vs_aggregated.py /path/to/raw/traces aggregated.pfw.gz")
        sys.exit(1)

    raw_dir = sys.argv[1]
    agg_file = sys.argv[2]

    # Load and compute raw metrics
    print(f"\n=== Loading Raw Traces from: {raw_dir} ===")
    raw_events = load_raw_traces(raw_dir)
    raw_metrics = compute_metrics_raw(raw_events)

    # Load and compute aggregated metrics
    print(f"\n=== Loading Aggregated Traces from: {agg_file} ===")
    agg_traces, agg_metadata = load_aggregated_traces(agg_file)
    print(f"Loaded {len(agg_traces)} aggregated events")

    print("\n=== Computing Aggregated Metrics ===")
    agg_metrics = compute_aggregated_metrics(agg_traces, agg_metadata)

    # Print comparison
    print_comparison(raw_metrics, agg_metrics)


if __name__ == '__main__':
    main()
