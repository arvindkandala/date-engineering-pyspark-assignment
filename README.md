# date-engineering-pyspark-assignment
# PySpark Wine Quality Pipeline (Databricks)

## Project Overview

This project implements a PySpark data processing pipeline on Databricks using the red and white wine quality datasets. The goal is to simulate large-scale processing (~1GB via controlled duplication), apply transformations and SQL analytics, and demonstrate key Spark concepts: lazy evaluation, optimizations, and distributed execution.

## Dataset

- Source: Databricks sample data
  - `/databricks-datasets/wine-quality/winequality-white.csv`
  - `/databricks-datasets/wine-quality/winequality-red.csv`
- The two CSVs are read with schema inference and tagged with a wine_type column.
- The base data is scaled up using a cross join with a range() DataFrame to create a large combined dataset while preserving the original distributions.

## Pipeline Steps

1. **Load**
   - Read both CSVs with header + inferred schema.
   - Add wine_type column to distinguish red vs white.
   - Union into a single wine dataset.

2. **Transform**
   - Filter out rows with invalid or missing values in key columns (e.g. quality, alcohol, residual sugar).
   - Create derived columns:
     - is_high_quality (binary label for quality >= 7)
     - alcohol_sugar_ratio (alcohol vs residual sugar).

3. **Aggregations & Join**
   - quality_summary: group by wine_type and quality with:
     - counts
     - average alcohol
     - average residual sugar
     - average alcohol_sugar_ratio
     - % high-quality wines
   - quality_compare: aggregate red and white separately by quality and join on quality to compare
     - white_avg_alcohol, red_avg_alcohol
     - white_count, red_count.

4. **SQL Queries**
   - sql_summary_by_type: summary by wine_type using spark.sql on the temp view wine_large
   - sql_high_segment`: high-quality, high-alcohol segment filtered and grouped via SQL.

5. **Outputs**
   - Results written as managed Delta tables:
     - student_db.wine_quality_summary
     - student_db.wine_quality_compare
   - These can be queried with `spark.table or via SQL.

## Performance Analysis (Summary)

Spark applies filters and column pruning early in the pipeline: invalid / missing records are removed before groupBy operations, and only relevant columns flow into the aggregations. The physical plan for quality_summary shows a single scan of the large wine table followed by partial aggregations and a final reduce step, which minimizes data movement. The SQL queries on wine_large reuse this optimized layout, and the managed table writes are executed as efficient, distributed Parquet/Delta writes

I used the Databricks Query Details view to inspect shuffles and stages. The main cost appears in the large aggregations and in the join used for quality_compare, but that join operates on pre-aggregated statistics (white_stats, red_stats), so the shuffled data is small. I also experimented with manual repartitionon the serverless cluster; in this workload it consistently increased runtime due to the extra shuffle, so I kept the default partitioning. Overall, the pipeline is optimized by filtering early, avoiding unnecessary repartitions, limiting columns in downstream steps, and writing compact aggregated outputs

## Key Findings

- white and red wines show different quality distributions; high quality scores are relatively rare but easier to inspect at scale after aggregation
- higher quality wines in this dataset tend to be associated with higher average alcohol content and specific residual sugar ranges
- comparing white_avg_alcohol vs red_avg_alcohol by quality level highlights how style and composition differ even at the same quality score

## Screenshots
- screenshots are 