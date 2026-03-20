import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from extractors.csv_extractor import CSVExtractor
from cleaners.rule_engine import RuleEngine, CleaningRule
from cleaners.standard_rules import (
    strip_whitespace,
    standardize_email,
    standardize_phone,
    handle_nulls
)
from transformers.pandas_transformer import PandasTransformer
from utils.pipeline_logger import PipelineLogger
import uuid


def run_pipeline(file_path: str):
    """Master ETL Pipeline — Extract, Clean, Transform, Load."""

    batch_id = str(uuid.uuid4())
    logger = PipelineLogger(
        dag_id='master_etl_pipeline',
        task_id='run_pipeline',
        run_id=batch_id
    )

    logger.info('Pipeline started', batch_id=batch_id)

    # ── EXTRACT ──────────────────────────────────
    logger.info('Starting extraction')
    extractor = CSVExtractor(file_path=file_path)
    all_chunks = []
    for chunk in extractor.extract():
        all_chunks.append(chunk)
    df = pd.concat(all_chunks, ignore_index=True)
    logger.info('Extraction complete', records=len(df))

    # ── CLEAN ────────────────────────────────────
    logger.info('Starting cleaning')
    engine = RuleEngine()
    engine.register(CleaningRule(
        name='strip_whitespace',
        columns=['all'],
        func=strip_whitespace,
        severity='WARNING'
    ))
    engine.register(CleaningRule(
        name='standardize_email',
        columns=['email'],
        func=standardize_email,
        severity='ERROR'
    ))
    engine.register(CleaningRule(
        name='standardize_phone',
        columns=['phone'],
        func=standardize_phone,
        severity='WARNING'
    ))
    engine.register(CleaningRule(
        name='handle_nulls',
        columns=['customer_id', 'full_name', 'email'],
        func=lambda df: handle_nulls(
            df, ['customer_id', 'full_name', 'email']
        ),
        severity='CRITICAL'
    ))

    clean_df, quarantine_df = engine.run(df)
    logger.info('Cleaning complete',
                clean_records=len(clean_df),
                quarantined=len(quarantine_df))

    # ── TRANSFORM ────────────────────────────────
    logger.info('Starting transformation')
    transformer = PandasTransformer()
    clean_df = transformer.normalize_customer(clean_df)
    clean_df = transformer.add_audit_columns(clean_df, batch_id)
    clean_df = transformer.compute_dq_score(
        clean_df,
        required_cols=['customer_id', 'full_name', 'email', 'phone']
    )
    logger.info('Transformation complete')

    # ── LOAD ─────────────────────────────────────
    os.makedirs('data/silver', exist_ok=True)
    os.makedirs('data/quarantine', exist_ok=True)

    clean_df.to_csv('data/silver/customers_clean.csv', index=False)
    if len(quarantine_df) > 0:
        quarantine_df.to_csv(
            'data/quarantine/customers_quarantine.csv', index=False
        )

    logger.info('Pipeline complete',
                output='data/silver/customers_clean.csv',
                batch_id=batch_id)

    print(f'\n✅ Pipeline Complete!')
    print(f'   Clean records    : {len(clean_df)}')
    print(f'   Quarantined      : {len(quarantine_df)}')
    print(f'   Batch ID         : {batch_id}')
    print(f'   DQ Score Avg     : {clean_df["data_quality_score"].mean():.2f}')
    return clean_df


if __name__ == '__main__':
    run_pipeline('/root/airflow/data/sample_customerrs.csv')