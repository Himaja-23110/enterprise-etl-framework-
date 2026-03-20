import pandas as pd
import logging


class PandasTransformer:
    """In-memory transformer using Pandas for small-medium datasets."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def normalize_customer(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize customer data — names, emails, country codes."""
        df = df.copy()
        df['full_name'] = df['full_name'].str.title().str.strip()
        df['email'] = df['email'].str.lower().str.strip()
        df['country_code'] = df['country_code'].fillna('IN').str.upper()
        self.logger.info(f'Normalized {len(df)} customer records')
        return df

    def normalize_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize transaction data — amounts, dates, status."""
        df = df.copy()
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').round(2)
        df['status'] = df['status'].str.upper().str.strip()
        df['transaction_date'] = pd.to_datetime(
            df['transaction_date'], errors='coerce'
        )
        self.logger.info(f'Normalized {len(df)} transaction records')
        return df

    def add_audit_columns(self, df: pd.DataFrame,
                          batch_id: str) -> pd.DataFrame:
        """Add pipeline audit columns to every record."""
        df = df.copy()
        df['ingested_at'] = pd.Timestamp.utcnow()
        df['batch_id'] = batch_id
        df['pipeline_version'] = '1.0.0'
        return df

    def compute_dq_score(self, df: pd.DataFrame,
                         required_cols: list) -> pd.DataFrame:
        """Compute a simple data quality score per row (0-100)."""
        df = df.copy()
        total = len(required_cols)

        def score_row(row):
            filled = sum(
                1 for col in required_cols
                if col in row and pd.notna(row[col]) and row[col] != ''
            )
            return round((filled / total) * 100, 2)

        df['data_quality_score'] = df.apply(score_row, axis=1)
        self.logger.info(
            f'Avg DQ score: {df["data_quality_score"].mean():.2f}'
        )
        return df