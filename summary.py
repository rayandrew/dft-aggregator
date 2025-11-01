import json
import gzip

# Load aggregated traces
with gzip.open('/Users/rayandrew/Projects/dftracer/dft-aggregator/aggregated.pfw.gz', 'rt') as f:
    content = f.read()
    traces = []
    for line in content.strip()[1:-1].split('\n'):
        line = line.strip().rstrip(',')
        if line:
            traces.append(json.loads(line))

# Aggregate by category
category_stats = {}

for event in traces:
    if event.get('ph') != 'C':  # Skip non-counter events
        continue
    
    cat = event.get('cat', 'unknown')
    args = event.get('args', {})
    
    if cat not in category_stats:
        category_stats[cat] = {
            'total_size_bytes': 0,
            'total_wall_clock_us': 0,
            'event_count': 0,
            'bucket_count': 0
        }
    
    # Calculate wall-clock for this bucket using ts_range + max_duration
    if 'ts_range' in args and 'dur' in args:
        first_ts = args['ts_range'][0]
        last_ts = args['ts_range'][1]
        max_dur = args['dur'].get('max', 0)
        
        wall_clock = (last_ts - first_ts) + max_dur
        category_stats[cat]['total_wall_clock_us'] += wall_clock
    
    # Sum size (if available)
    if 'size' in args and 'total' in args['size']:
        category_stats[cat]['total_size_bytes'] += args['size']['total']
    
    # Count events and buckets
    if 'count' in args:
        category_stats[cat]['event_count'] += args['count']
    category_stats[cat]['bucket_count'] += 1

# Print results
print("\n=== Category-wise Bandwidth (Using ts_range + max_duration) ===\n")
print(f"{'Category':<20} {'Wall-Clock Time':<20} {'Total Size':<20} {'Events':<10} {'Buckets':<10} {'Bandwidth':<15}")
print("-" * 120)

for cat in sorted(category_stats.keys()):
    stats = category_stats[cat]
    
    # Convert to readable units
    wall_clock_sec = stats['total_wall_clock_us'] / 1_000_000
    size_mb = stats['total_size_bytes'] / (1024 * 1024)
    
    # Calculate bandwidth
    if stats['total_wall_clock_us'] > 0 and stats['total_size_bytes'] > 0:
        bandwidth_mbps = size_mb / wall_clock_sec
        bandwidth_str = f"{bandwidth_mbps:.2f} MB/s"
    else:
        bandwidth_str = "N/A"
    
    print(f"{cat:<20} {wall_clock_sec:>15.3f} sec  {size_mb:>15.3f} MB  {stats['event_count']:>8}  {stats['bucket_count']:>8}  {bandwidth_str:<15}")

print("\n")
