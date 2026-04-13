"""
OPERATIONAL SKILL 4 — Competitive Context Benchmarking

Every metric is benchmarked against the category average.
Computes relative_strength and flags above_category_average products.
"""
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def calculate_benchmark(
    product_value: float,
    category_values: list[float],
    metric_name: str = "",
) -> dict:
    """
    Benchmark a product's metric against its category.

    Args:
        product_value: This product's metric value.
        category_values: List of the same metric from other products in the category
                        (last 30 days of data).
        metric_name: Name of the metric for logging.

    Returns:
        {
            "raw_value": float,
            "category_average": float,
            "relative_strength": float (1.0 = average),
            "above_category_average": bool (true if > 1.5x average),
            "percentile": float (0-100)
        }
    """
    if not category_values:
        return {
            "raw_value": product_value,
            "category_average": product_value,
            "relative_strength": 1.0,
            "above_category_average": False,
            "percentile": 50.0,
        }

    category_avg = sum(category_values) / len(category_values)

    if category_avg == 0:
        relative_strength = float(product_value) if product_value > 0 else 1.0
    else:
        relative_strength = product_value / category_avg

    # Calculate percentile
    below_count = sum(1 for v in category_values if v < product_value)
    percentile = (below_count / len(category_values)) * 100

    above_avg = relative_strength > 1.5

    if above_avg:
        logger.info(
            "Product metric '%s' is %.1fx category average (%.1f vs %.1f)",
            metric_name, relative_strength, product_value, category_avg,
        )

    return {
        "raw_value": round(product_value, 4),
        "category_average": round(category_avg, 4),
        "relative_strength": round(relative_strength, 4),
        "above_category_average": above_avg,
        "percentile": round(percentile, 1),
    }


async def get_category_averages(supabase, table: str, category: str, metric_columns: list[str]) -> dict:
    """
    Fetch category averages for specified metrics from the last 30 days.

    Args:
        supabase: Supabase client.
        table: Signal table name.
        category: Product category to benchmark against.
        metric_columns: List of column names to average.

    Returns:
        dict mapping column_name -> list of values for benchmarking.
    """
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        # Get all product IDs in this category
        products_resp = supabase.table("products") \
            .select("id") \
            .eq("category", category) \
            .eq("active", True) \
            .execute()

        product_ids = [p["id"] for p in products_resp.data]

        if not product_ids:
            return {col: [] for col in metric_columns}

        # Fetch signal data for these products
        cols = ",".join(["product_id"] + metric_columns)
        signals_resp = supabase.table(table) \
            .select(cols) \
            .in_("product_id", product_ids) \
            .gte("scraped_date", thirty_days_ago) \
            .execute()

        # Group values by metric column
        result = {col: [] for col in metric_columns}
        for row in signals_resp.data:
            for col in metric_columns:
                val = row.get(col)
                if val is not None:
                    result[col].append(float(val))

        return result

    except Exception as e:
        logger.error("Failed to fetch category averages: %s", e)
        return {col: [] for col in metric_columns}


def benchmark_signal_row(
    signal_data: dict,
    category_data: dict,
    metrics_to_benchmark: list[str],
) -> dict:
    """
    Benchmark all metrics in a signal row against category averages.

    Args:
        signal_data: The signal row data.
        category_data: Dict of {metric_name: [category_values]}.
        metrics_to_benchmark: Which metrics to benchmark.

    Returns:
        {
            "relative_strength": float (average across all metrics),
            "above_category_average": bool,
            "benchmarks": dict of {metric: benchmark_result}
        }
    """
    benchmarks = {}
    strengths = []

    for metric in metrics_to_benchmark:
        value = signal_data.get(metric)
        if value is None:
            continue

        cat_values = category_data.get(metric, [])
        result = calculate_benchmark(float(value), cat_values, metric)
        benchmarks[metric] = result
        strengths.append(result["relative_strength"])

    avg_strength = sum(strengths) / len(strengths) if strengths else 1.0

    return {
        "relative_strength": round(avg_strength, 4),
        "above_category_average": avg_strength > 1.5,
        "benchmarks": benchmarks,
    }
