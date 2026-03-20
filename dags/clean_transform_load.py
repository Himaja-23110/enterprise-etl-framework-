import sys
import os
sys.path.insert(0, '/root/airflow')

import pandas as pd
from cleaners.rule_engine import RuleEngine, CleaningRule
from cleaners.standard_rules import (
    strip_whitespace,
    handle_nulls,
    validate_range,
    normalize_text_columns,
    drop_complete_duplicates,
    remove_outliers
)
from transformers.pandas_transformer import PandasTransformer
from transformers.spark_transformer import SparkTransformer
from utils.pipeline_logger import PipelineLogger
import uuid


def clean_and_transform(file_path: str):
    """Data Cleaning & Transformation — Milestone 2."""

    batch_id = str(uuid.uuid4())
    logger = PipelineLogger(
        dag_id='clean_transform_load',
        task_id='clean_transform',
        run_id=batch_id
    )

    logger.info('Starting cleaning & transformation', batch_id=batch_id)

    # ── LOAD RAW DATA ─────────────────────────────
    df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
    logger.info('Raw data loaded', records=len(df))

    # ── CLEANING RULES ────────────────────────────
    engine = RuleEngine()
    engine.register(CleaningRule(
        name='strip_whitespace',
        columns=['all'],
        func=strip_whitespace,
        severity='WARNING'
    ))
    engine.register(CleaningRule(
        name='handle_nulls',
        columns=['Country', 'Literacy Rate'],
        func=lambda df: handle_nulls(df, ['Country', 'Literacy Rate', 'Year']),
        severity='CRITICAL'
    ))
    engine.register(CleaningRule(
        name='validate_literacy_range',
        columns=['Literacy Rate'],
        func=lambda df: validate_range(df, 'Literacy Rate', 0, 1),
        severity='ERROR'
    ))
    engine.register(CleaningRule(
        name='normalize_text',
        columns=['Country'],
        func=lambda df: normalize_text_columns(df, ['Country']),
        severity='WARNING'
    ))
    engine.register(CleaningRule(
        name='drop_duplicates',
        columns=['all'],
        func=drop_complete_duplicates,
        severity='WARNING'
    ))
    engine.register(CleaningRule(
        name='remove_outliers',
        columns=['Literacy Rate'],
        func=lambda df: remove_outliers(df, 'Literacy Rate'),
        severity='WARNING'
    ))

    clean_df, quarantine_df = engine.run(df)
    logger.info('Cleaning complete',
                clean_records=len(clean_df),
                quarantined=len(quarantine_df))

    # ── TRANSFORMATIONS ───────────────────────────
    spark = SparkTransformer()
    clean_df = spark.clean_literacy_data(clean_df)
    clean_df = spark.add_literacy_category(clean_df)
    region_df = spark.aggregate_by_region(clean_df)

    transformer = PandasTransformer()
    clean_df = transformer.add_audit_columns(clean_df, batch_id)
    clean_df = transformer.compute_dq_score(
        clean_df,
        required_cols=['Country', 'Literacy Rate', 'Year']
    )

    # ── SAVE OUTPUTS ──────────────────────────────
    os.makedirs('data/silver', exist_ok=True)
    os.makedirs('data/gold', exist_ok=True)
    os.makedirs('data/quarantine', exist_ok=True)

    clean_df.to_csv('data/silver/literacy_clean.csv', index=False)
    region_df.to_csv('data/gold/literacy_by_region.csv', index=False)
    if len(quarantine_df) > 0:
        quarantine_df.to_csv(
            'data/quarantine/literacy_quarantine.csv', index=False
        )

    logger.info('Pipeline complete', batch_id=batch_id)

    print(f'\n✅ Clean & Transform Complete!')
    print(f'   Clean records     : {len(clean_df)}')
    print(f'   Quarantined       : {len(quarantine_df)}')
    print(f'   Regions aggregated: {len(region_df)}')
    print(f'   DQ Score Avg      : {clean_df["data_quality_score"].mean():.2f}')
    return clean_df


if __name__ == '__main__':
    clean_and_transform('data/Literacy Rate.csv')