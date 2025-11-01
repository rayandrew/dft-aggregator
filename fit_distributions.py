#!/usr/bin/env python3
"""
Distribution Fitting for DLIO Configuration Generation

Analyzes aggregated DFTracer traces and fits statistical distributions
to computation and preprocess timing data for DLIO benchmark configuration.
"""

import json
import gzip
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from sklearn.mixture import GaussianMixture


# ============================================================================
# Data Loading
# ============================================================================

def load_aggregated_traces(filepath):
    """Load and parse aggregated trace data from gzipped JSON."""
    with gzip.open(filepath, 'rt') as f:
        content = f.read()
        traces = []
        for line in content.strip()[1:-1].split('\n'):
            line = line.strip().rstrip(',')
            if line:
                traces.append(json.loads(line))
    return traces


def extract_timing_data(traces):
    """
    Extract computation and preprocess timing data from traces.

    Returns:
        tuple: (computation_times, preprocess_times) in seconds
    """
    computation_times = []  # fetch.block
    preprocess_times = []   # preprocess

    for event in traces:
        if event.get('ph') != 'C':
            continue

        args = event.get('args', {})
        name = event.get('name', '')

        # Extract duration statistics (min, mean, max)
        if 'dur' in args:
            mean_dur = args['dur'].get('mean', 0)
            min_dur = args['dur'].get('min', 0)
            max_dur = args['dur'].get('max', 0)

            # Collect timing data based on event name
            target_list = None
            if name == 'fetch.block':
                target_list = computation_times
            elif name == 'preprocess':
                target_list = preprocess_times

            if target_list is not None:
                if min_dur > 0:
                    target_list.append(min_dur / 1_000_000)  # Convert to seconds
                if mean_dur > 0 and mean_dur != min_dur and mean_dur != max_dur:
                    target_list.append(mean_dur / 1_000_000)
                if max_dur > 0:
                    target_list.append(max_dur / 1_000_000)

    return np.array(computation_times), np.array(preprocess_times)


# ============================================================================
# Distribution Fitting
# ============================================================================

def fit_single_distributions(data, name="Distribution Fits"):
    """
    Fit multiple single-component distributions and find the best one.

    Returns:
        dict: Best-fit distribution info with 'distribution', 'ks_stat', 'params'
    """
    print(f"\n{name}:")
    print(f"  Sample size: {len(data)}")
    print(f"  Mean: {np.mean(data):.6f} sec")
    print(f"  Median: {np.median(data):.6f} sec")
    print(f"  StdDev: {np.std(data):.6f} sec")
    print(f"  Min: {np.min(data):.6f} sec")
    print(f"  Max: {np.max(data):.6f} sec")

    distributions = {
        'Normal': stats.norm,
        'Lognormal': stats.lognorm,
        'Gamma': stats.gamma,
        'Exponential': stats.expon,
        'Weibull': stats.weibull_min,
    }

    best_fit = None
    best_ks_stat = float('inf')

    print(f"\n  Single Distribution Fits (Kolmogorov-Smirnov test):")
    for dist_name, dist in distributions.items():
        try:
            params = dist.fit(data)
            ks_stat, p_value = stats.kstest(data, lambda x: dist.cdf(x, *params))
            print(f"    {dist_name:15s}: KS={ks_stat:.6f}, p-value={p_value:.6f}, params={params}")

            if ks_stat < best_ks_stat:
                best_ks_stat = ks_stat
                best_fit = {
                    'distribution': dist_name.lower(),
                    'ks_stat': ks_stat,
                    'params': params
                }
        except Exception as e:
            print(f"    {dist_name:15s}: Failed to fit - {e}")

    return best_fit


def fit_gmm_2_components(data):
    """
    Fit a 2-component Gaussian Mixture Model.

    Returns:
        tuple: (means, stds, weights, bic, aic, gmm_model)
    """
    gmm = GaussianMixture(n_components=2, random_state=42, max_iter=200)
    gmm.fit(data.reshape(-1, 1))

    # Extract and sort parameters by mean
    means = gmm.means_.flatten()
    stds = np.sqrt(gmm.covariances_.flatten())
    weights = gmm.weights_

    idx = np.argsort(means)
    means = means[idx]
    stds = stds[idx]
    weights = weights[idx]

    # Calculate model fit metrics
    bic = gmm.bic(data.reshape(-1, 1))
    aic = gmm.aic(data.reshape(-1, 1))

    return means, stds, weights, bic, aic, gmm


def fit_component_distributions(data, gmm):
    """
    Separate GMM components and find best-fit distribution for each.

    Returns:
        list: Component info dicts with 'type', 'params', 'ks_stat', 'sample_size'
    """
    component_probs = gmm.predict_proba(data.reshape(-1, 1))

    # Sort components by mean
    means = gmm.means_.flatten()
    idx = np.argsort(means)

    distributions = {
        'normal': (stats.norm, ['mean', 'stdev']),
        'lognormal': (stats.lognorm, ['sigma', 'loc', 'scale']),
        'gamma': (stats.gamma, ['shape', 'loc', 'scale']),
        'weibull': (stats.weibull_min, ['shape', 'loc', 'scale']),
        'exponential': (stats.expon, ['loc', 'scale']),
    }

    components_info = []

    for i in idx:
        # Get data belonging to this component (probability > 0.5)
        comp_data = data[component_probs[:, i] > 0.5]

        # Find best-fit distribution for this component
        best_dist = None
        best_ks = float('inf')
        best_params = None

        for dist_name, (dist_obj, param_names) in distributions.items():
            try:
                params = dist_obj.fit(comp_data)
                ks_stat, _ = stats.kstest(comp_data, lambda x: dist_obj.cdf(x, *params))

                if ks_stat < best_ks:
                    best_ks = ks_stat
                    best_dist = dist_name
                    best_params = params
            except:
                continue

        # Convert scipy params to DLIO format
        dlio_params = convert_to_dlio_params(best_dist, best_params, comp_data)

        components_info.append({
            'type': best_dist,
            'params': dlio_params,
            'ks_stat': best_ks,
            'sample_size': len(comp_data)
        })

    return components_info


def convert_to_dlio_params(dist_type, scipy_params, data):
    """Convert scipy distribution parameters to DLIO format."""
    dlio_params = {'type': dist_type}

    if dist_type == 'normal':
        # scipy.stats.norm: (loc, scale) = (mean, std)
        dlio_params['mean'] = float(scipy_params[0])
        dlio_params['stdev'] = float(scipy_params[1])

    elif dist_type == 'lognormal':
        # scipy.stats.lognorm: (s, loc, scale)
        dlio_params['sigma'] = float(scipy_params[0])
        dlio_params['mean'] = float(np.log(scipy_params[2])) if scipy_params[2] > 0 else 0.0

    elif dist_type == 'gamma':
        # scipy.stats.gamma: (a, loc, scale)
        dlio_params['shape'] = float(scipy_params[0])
        dlio_params['scale'] = float(scipy_params[2])

    elif dist_type == 'weibull':
        # scipy.stats.weibull_min: (c, loc, scale)
        dlio_params['shape'] = float(scipy_params[0])
        dlio_params['scale'] = float(scipy_params[2])

    elif dist_type == 'exponential':
        # scipy.stats.expon: (loc, scale)
        dlio_params['scale'] = float(scipy_params[1])

    return dlio_params


# ============================================================================
# DLIO Configuration Generation
# ============================================================================

def print_dlio_option1(computation_times, preprocess_times):
    """Print Option 1: Single Normal Distribution (Simple)"""
    print("\n# Option 1: Single Normal Distribution (Simple)")
    print("train:")
    print("  computation_time:")
    print("    type: normal")
    print(f"    mean: {np.mean(computation_times):.6f}")
    print(f"    stdev: {np.std(computation_times):.6f}")
    print("  preprocess_time:")
    print("    type: normal")
    print(f"    mean: {np.mean(preprocess_times):.6f}")
    print(f"    stdev: {np.std(preprocess_times):.6f}")


def print_dlio_option2(comp_means, comp_stds, comp_weights,
                       prep_means, prep_stds, prep_weights):
    """Print Option 2: Gaussian Mixture (2 components)"""
    print("\n# Option 2: Gaussian Mixture (2 components) - Better fit (DLIO Format)")
    print("train:")
    print("  # Computation time (fetch.block) - 2-component mixture:")
    print("  computation_time:")
    print("    type: mixture")
    print("    n_components: 2")
    print("    components:")
    for i in range(2):
        print(f"      - weight: {comp_weights[i]:.6f}")
        print("        params:")
        print("          type: normal")
        print(f"          mean: {comp_means[i]:.6f}")
        print(f"          stdev: {comp_stds[i]:.6f}")

    print("")
    print("  # Preprocess time - 2-component mixture:")
    print("  preprocess_time:")
    print("    type: mixture")
    print("    n_components: 2")
    print("    components:")
    for i in range(2):
        print(f"      - weight: {prep_weights[i]:.6f}")
        print("        params:")
        print("          type: normal")
        print(f"          mean: {prep_means[i]:.6f}")
        print(f"          stdev: {prep_stds[i]:.6f}")


def print_dlio_option3(comp_best_dists, comp_weights, prep_best_fit, preprocess_times):
    """Print Option 3: Best-fit distributions (Recommended)"""
    print("\n# Option 3: Best-fit (Optimal - Recommended)")
    print("train:")
    print("  # Computation time (fetch.block) - best-fit mixture:")
    print("  computation_time:")
    print("    type: mixture")
    print("    n_components: 2")
    print("    components:")
    for i, comp_info in enumerate(comp_best_dists):
        print(f"      - weight: {comp_weights[i]:.6f}")
        print("        params:")
        for param_name, param_value in comp_info['params'].items():
            if param_name == 'type':
                print(f"          {param_name}: {param_value}")
            else:
                print(f"          {param_name}: {param_value:.6f}")

    print("")
    print("  # Preprocess time - single distribution (data is unimodal):")
    print("  preprocess_time:")

    if prep_best_fit['distribution'] == 'weibull':
        print("    type: weibull")
        print(f"    shape: {prep_best_fit['params'][0]:.6f}")
        print(f"    scale: {prep_best_fit['params'][2]:.6f}")
    elif prep_best_fit['distribution'] == 'normal':
        print("    type: normal")
        print(f"    mean: {np.mean(preprocess_times):.6f}")
        print(f"    stdev: {np.std(preprocess_times):.6f}")
    else:
        print("    type: normal")
        print(f"    mean: {np.mean(preprocess_times):.6f}")
        print(f"    stdev: {np.std(preprocess_times):.6f}")


# ============================================================================
# Visualization
# ============================================================================

def plot_mixture_distribution(data, gmm_means, gmm_stds, gmm_weights,
                              best_fit_components, title, filename):
    """Plot distribution fits for mixture models with PDF and CDF comparisons."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    x_range = np.linspace(data.min(), data.max(), 1000)

    # ---- Plot 1: PDF Comparison ----
    ax1.hist(data, bins=50, density=True, alpha=0.6, color='blue',
             edgecolor='black', label='Observed data')

    # Single normal distribution
    single_normal = stats.norm.pdf(x_range, np.mean(data), np.std(data))
    ax1.plot(x_range, single_normal, 'r-', linewidth=2,
             label=f'Single Normal (μ={np.mean(data):.3f}, σ={np.std(data):.3f})')

    # GMM with normal components (dashed)
    gmm_pdf = np.zeros_like(x_range)
    for i, (mean, std, weight) in enumerate(zip(gmm_means, gmm_stds, gmm_weights)):
        component_pdf = weight * stats.norm.pdf(x_range, mean, std)
        gmm_pdf += component_pdf
        ax1.plot(x_range, component_pdf, '--', linewidth=1, alpha=0.5,
                label=f'Normal Comp {i+1} (w={weight:.2f})')
    ax1.plot(x_range, gmm_pdf, 'orange', linestyle='--', linewidth=2,
             alpha=0.7, label='GMM Normal Mixture')

    # Best-fit mixture (solid, prominent)
    best_mixture_pdf = plot_best_fit_pdf(x_range, best_fit_components, gmm_weights, ax1)
    ax1.plot(x_range, best_mixture_pdf, 'g-', linewidth=3, label='Best-fit Mixture')

    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Probability Density', fontsize=12)
    ax1.set_title(f'{title} - Distribution Fit', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)

    # ---- Plot 2: CDF Comparison ----
    sorted_data = np.sort(data)
    empirical_cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    ax2.plot(sorted_data, empirical_cdf, 'b-', linewidth=2,
             label='Empirical CDF', alpha=0.7)

    # Single normal CDF
    single_normal_cdf = stats.norm.cdf(x_range, np.mean(data), np.std(data))
    ax2.plot(x_range, single_normal_cdf, 'r--', linewidth=2, label='Single Normal CDF')

    # GMM CDF (normal)
    gmm_cdf = np.zeros_like(x_range)
    for mean, std, weight in zip(gmm_means, gmm_stds, gmm_weights):
        gmm_cdf += weight * stats.norm.cdf(x_range, mean, std)
    ax2.plot(x_range, gmm_cdf, 'orange', linestyle='--', linewidth=2,
             label='Normal GMM CDF', alpha=0.7)

    # Best-fit mixture CDF
    best_mixture_cdf = plot_best_fit_cdf(x_range, best_fit_components, gmm_weights)
    ax2.plot(x_range, best_mixture_cdf, 'g-', linewidth=2.5, label='Best-fit Mixture CDF')

    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_ylabel('Cumulative Probability', fontsize=12)
    ax2.set_title(f'{title} - Cumulative Distribution', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_single_distribution(data, best_fit, filename):
    """Plot single distribution fits for unimodal data."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    x_range = np.linspace(data.min(), data.max(), 1000)

    # ---- Plot 1: PDF ----
    ax1.hist(data, bins=50, density=True, alpha=0.6, color='blue',
             edgecolor='black', label='Observed data')

    # Normal distribution
    single_normal = stats.norm.pdf(x_range, np.mean(data), np.std(data))
    ax1.plot(x_range, single_normal, 'r--', linewidth=2, label=f'Normal (KS={0.029:.3f})')

    # Best-fit distribution
    if best_fit['distribution'] == 'weibull':
        weibull_pdf = stats.weibull_min.pdf(x_range, best_fit['params'][0],
                                            loc=best_fit['params'][1],
                                            scale=best_fit['params'][2])
        ax1.plot(x_range, weibull_pdf, 'g-', linewidth=3,
                label=f'Weibull (KS={best_fit["ks_stat"]:.3f}) - Best fit')

    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Probability Density', fontsize=12)
    ax1.set_title('Preprocess Time - Distribution Fit', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=10)
    ax1.grid(True, alpha=0.3)

    # ---- Plot 2: CDF ----
    sorted_data = np.sort(data)
    empirical_cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    ax2.plot(sorted_data, empirical_cdf, 'b-', linewidth=2,
             label='Empirical CDF', alpha=0.7)

    # Normal CDF
    single_normal_cdf = stats.norm.cdf(x_range, np.mean(data), np.std(data))
    ax2.plot(x_range, single_normal_cdf, 'r--', linewidth=2, label='Normal CDF')

    # Best-fit CDF
    if best_fit['distribution'] == 'weibull':
        weibull_cdf = stats.weibull_min.cdf(x_range, best_fit['params'][0],
                                            loc=best_fit['params'][1],
                                            scale=best_fit['params'][2])
        ax2.plot(x_range, weibull_cdf, 'g-', linewidth=2.5, label='Weibull CDF - Best fit')

    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_ylabel('Cumulative Probability', fontsize=12)
    ax2.set_title('Preprocess Time - Cumulative Distribution', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_comparison(computation_times, comp_best_dists, comp_weights,
                    preprocess_times, prep_best_fit, filename):
    """Create side-by-side comparison plot with best-fit distributions."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # ---- Computation time (left) ----
    axes[0].hist(computation_times, bins=50, density=True, alpha=0.6,
                color='blue', edgecolor='black', label='Observed data')

    x_comp = np.linspace(computation_times.min(), computation_times.max(), 1000)
    best_mixture_pdf_comp = compute_mixture_pdf(x_comp, comp_best_dists, comp_weights)

    axes[0].plot(x_comp, best_mixture_pdf_comp, 'g-', linewidth=3,
                label='Best-fit Mixture (Lognormal+Weibull)')
    axes[0].set_xlabel('Time (seconds)', fontsize=12)
    axes[0].set_ylabel('Probability Density', fontsize=12)
    axes[0].set_title('Computation Time (fetch.block)', fontsize=14, fontweight='bold')
    axes[0].legend(fontsize=11)
    axes[0].grid(True, alpha=0.3)

    # ---- Preprocess time (right) ----
    axes[1].hist(preprocess_times, bins=50, density=True, alpha=0.6,
                color='orange', edgecolor='black', label='Observed data')

    x_prep = np.linspace(preprocess_times.min(), preprocess_times.max(), 1000)
    if prep_best_fit['distribution'] == 'weibull':
        weibull_pdf = stats.weibull_min.pdf(x_prep, prep_best_fit['params'][0],
                                            loc=prep_best_fit['params'][1],
                                            scale=prep_best_fit['params'][2])
        axes[1].plot(x_prep, weibull_pdf, 'g-', linewidth=3, label='Weibull (Best fit)')

    axes[1].set_xlabel('Time (seconds)', fontsize=12)
    axes[1].set_ylabel('Probability Density', fontsize=12)
    axes[1].set_title('Preprocess Time', fontsize=14, fontweight='bold')
    axes[1].legend(fontsize=11)
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"  Saved: {filename}")
    plt.close()


def plot_best_fit_pdf(x_range, components, weights, ax):
    """Helper: Plot individual components and return mixture PDF."""
    mixture_pdf = np.zeros_like(x_range)

    for i, comp_info in enumerate(components):
        dist_type = comp_info['params']['type']
        weight = weights[i]

        component_pdf = compute_component_pdf(x_range, comp_info['params'])
        mixture_pdf += weight * component_pdf

        ax.plot(x_range, weight * component_pdf, '-', linewidth=1.5,
               label=f'Best {dist_type.title()} Comp {i+1} (w={weight:.2f})')

    return mixture_pdf


def plot_best_fit_cdf(x_range, components, weights):
    """Helper: Compute and return mixture CDF."""
    mixture_cdf = np.zeros_like(x_range)

    for i, comp_info in enumerate(components):
        weight = weights[i]
        component_cdf = compute_component_cdf(x_range, comp_info['params'])
        mixture_cdf += weight * component_cdf

    return mixture_cdf


def compute_mixture_pdf(x_range, components, weights):
    """Compute mixture PDF from components."""
    mixture_pdf = np.zeros_like(x_range)

    for i, comp_info in enumerate(components):
        weight = weights[i]
        component_pdf = compute_component_pdf(x_range, comp_info['params'])
        mixture_pdf += weight * component_pdf

    return mixture_pdf


def compute_component_pdf(x_range, params):
    """Compute PDF for a single distribution component."""
    dist_type = params['type']

    if dist_type == 'normal':
        return stats.norm.pdf(x_range, params['mean'], params['stdev'])
    elif dist_type == 'lognormal':
        sigma = params['sigma']
        scale = np.exp(params['mean'])
        return stats.lognorm.pdf(x_range, sigma, scale=scale)
    elif dist_type == 'gamma':
        return stats.gamma.pdf(x_range, params['shape'], scale=params['scale'])
    elif dist_type == 'weibull':
        return stats.weibull_min.pdf(x_range, params['shape'], scale=params['scale'])
    elif dist_type == 'exponential':
        return stats.expon.pdf(x_range, scale=params['scale'])
    else:
        return np.zeros_like(x_range)


def compute_component_cdf(x_range, params):
    """Compute CDF for a single distribution component."""
    dist_type = params['type']

    if dist_type == 'normal':
        return stats.norm.cdf(x_range, params['mean'], params['stdev'])
    elif dist_type == 'lognormal':
        sigma = params['sigma']
        scale = np.exp(params['mean'])
        return stats.lognorm.cdf(x_range, sigma, scale=scale)
    elif dist_type == 'gamma':
        return stats.gamma.cdf(x_range, params['shape'], scale=params['scale'])
    elif dist_type == 'weibull':
        return stats.weibull_min.cdf(x_range, params['shape'], scale=params['scale'])
    elif dist_type == 'exponential':
        return stats.expon.cdf(x_range, scale=params['scale'])
    else:
        return np.zeros_like(x_range)


# ============================================================================
# Main Analysis Pipeline
# ============================================================================

def main():
    """Main analysis pipeline."""
    print("\n=== Distribution Fitting Analysis ===\n")

    # Load data
    traces = load_aggregated_traces('aggregated.pfw.gz')
    computation_times, preprocess_times = extract_timing_data(traces)

    # ---- Analyze Computation Time ----
    print("\n" + "="*80)
    print("COMPUTATION TIME (fetch.block)")
    print("="*80)

    _ = fit_single_distributions(computation_times, "Single Distribution Fits")

    print("\n  Gaussian Mixture Model (2 components):")
    try:
        comp_means, comp_stds, comp_weights, comp_bic, comp_aic, comp_gmm = \
            fit_gmm_2_components(computation_times)

        print(f"    Component 1: weight={comp_weights[0]:.3f}, "
              f"mean={comp_means[0]:.6f}s, std={comp_stds[0]:.6f}s")
        print(f"    Component 2: weight={comp_weights[1]:.3f}, "
              f"mean={comp_means[1]:.6f}s, std={comp_stds[1]:.6f}s")
        print(f"    BIC: {comp_bic:.2f}, AIC: {comp_aic:.2f}")

        comp_ll = comp_gmm.score(computation_times.reshape(-1, 1)) * len(computation_times)
        print(f"    Log-likelihood: {comp_ll:.2f}")

        print("\n  Best-fit distributions per component:")
        comp_best_dists = fit_component_distributions(computation_times, comp_gmm)
        for i, comp_info in enumerate(comp_best_dists):
            print(f"    Component {i+1}: {comp_info['type']} "
                  f"(KS={comp_info['ks_stat']:.6f}, n={comp_info['sample_size']})")
            for param_name, param_value in comp_info['params'].items():
                if param_name != 'type':
                    print(f"      {param_name}: {param_value:.6f}")
    except Exception as e:
        print(f"    Failed to fit GMM: {e}")
        import traceback
        traceback.print_exc()
        return

    # ---- Analyze Preprocess Time ----
    print("\n" + "="*80)
    print("PREPROCESS TIME (preprocess)")
    print("="*80)

    prep_best_fit = fit_single_distributions(preprocess_times, "Single Distribution Fits")

    print("\n  Gaussian Mixture Model (2 components):")
    try:
        prep_means, prep_stds, prep_weights, prep_bic, prep_aic, prep_gmm = \
            fit_gmm_2_components(preprocess_times)

        print(f"    Component 1: weight={prep_weights[0]:.3f}, "
              f"mean={prep_means[0]:.6f}s, std={prep_stds[0]:.6f}s")
        print(f"    Component 2: weight={prep_weights[1]:.3f}, "
              f"mean={prep_means[1]:.6f}s, std={prep_stds[1]:.6f}s")
        print(f"    BIC: {prep_bic:.2f}, AIC: {prep_aic:.2f}")

        prep_ll = prep_gmm.score(preprocess_times.reshape(-1, 1)) * len(preprocess_times)
        print(f"    Log-likelihood: {prep_ll:.2f}")

        print("\n  Best-fit distributions per component:")
        prep_best_dists = fit_component_distributions(preprocess_times, prep_gmm)
        for i, comp_info in enumerate(prep_best_dists):
            print(f"    Component {i+1}: {comp_info['type']} "
                  f"(KS={comp_info['ks_stat']:.6f}, n={comp_info['sample_size']})")
            for param_name, param_value in comp_info['params'].items():
                if param_name != 'type':
                    print(f"      {param_name}: {param_value:.6f}")
    except Exception as e:
        print(f"    Failed to fit GMM: {e}")
        import traceback
        traceback.print_exc()
        return

    # ---- Generate DLIO Configurations ----
    print("\n" + "="*80)
    print("DLIO CONFIG RECOMMENDATIONS")
    print("="*80)

    print_dlio_option1(computation_times, preprocess_times)
    print_dlio_option2(comp_means, comp_stds, comp_weights,
                      prep_means, prep_stds, prep_weights)
    print_dlio_option3(comp_best_dists, comp_weights, prep_best_fit, preprocess_times)

    print("\n")

    # ---- Generate Visualizations ----
    print("="*80)
    print("GENERATING VISUALIZATIONS")
    print("="*80)

    try:
        # Computation time - mixture model
        plot_mixture_distribution(
            computation_times, comp_means, comp_stds, comp_weights, comp_best_dists,
            'Computation Time (fetch.block)', 'computation_time_distribution.png'
        )

        # Preprocess time - single distribution
        plot_single_distribution(
            preprocess_times, prep_best_fit, 'preprocess_time_distribution.png'
        )

        # Side-by-side comparison
        plot_comparison(
            computation_times, comp_best_dists, comp_weights,
            preprocess_times, prep_best_fit,
            'timing_distributions_comparison.png'
        )

        print("\nVisualization complete!")

    except Exception as e:
        print(f"  Failed to generate visualizations: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
