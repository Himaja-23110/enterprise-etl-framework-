import re
import pandas as pd
from datetime import datetime


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all string columns."""
    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
    return df


def standardize_email(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase emails and flag invalid formats."""
    EMAIL_RE = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df['email'] = df['email'].str.lower().str.strip()
    invalid = ~df['email'].str.match(EMAIL_RE, na=False)
    df.loc[invalid, 'email'] = None
    df.loc[invalid, '_dq_flag_email'] = 'INVALID_FORMAT'
    return df


def standardize_phone(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize phone numbers to E.164 format for India (+91)."""
    def to_e164(phone):
        if pd.isna(phone):
            return None
        digits = re.sub(r'\D', '', str(phone))
        if len(digits) == 10:
            return f'+91{digits}'
        if len(digits) == 12 and digits.startswith('91'):
            return f'+{digits}'
        return None
    df['phone'] = df['phone'].apply(to_e164)
    return df


def cast_numeric_amounts(df: pd.DataFrame) -> pd.DataFrame:
    """Safe cast amount columns; negative or zero values flagged."""
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
    invalid_amount = df['amount'] <= 0
    df.loc[invalid_amount, '_dq_flag_amount'] = 'NON_POSITIVE'
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse transaction_date with multiple format support."""
    FORMATS = ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%Y%m%d']
    def safe_parse(val):
        for fmt in FORMATS:
            try:
                return datetime.strptime(str(val), fmt)
            except:
                pass
        return pd.NaT
    df['transaction_date'] = df['transaction_date'].apply(safe_parse)
    return df


def deduplicate(df: pd.DataFrame,
                key_cols: list = ['transaction_id']) -> pd.DataFrame:
    """Remove duplicates based on business keys."""
    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep='last')
    print(f'Deduplication removed {before - len(df)} rows')
    return df


def handle_nulls(df: pd.DataFrame,
                 required_cols: list) -> pd.DataFrame:
    """Flag null values in required columns."""
    for col in required_cols:
        if col in df.columns:
            null_mask = df[col].isna() | (df[col] == '')
            df.loc[null_mask, f'_dq_flag_{col}'] = 'NULL_REQUIRED'
    return df


# ── Milestone 2 Rules ─────────────────────────────────────────

def remove_outliers(df: pd.DataFrame,
                    col: str,
                    lower_pct: float = 0.01,
                    upper_pct: float = 0.99) -> pd.DataFrame:
    """Remove statistical outliers using percentile method."""
    df[col] = pd.to_numeric(df[col], errors='coerce')
    lower = df[col].quantile(lower_pct)
    upper = df[col].quantile(upper_pct)
    outlier_mask = (df[col] < lower) | (df[col] > upper)
    df.loc[outlier_mask, f'_dq_flag_{col}'] = 'OUTLIER'
    print(f'Outliers flagged in {col}: {outlier_mask.sum()} rows')
    return df


def validate_range(df: pd.DataFrame,
                   col: str,
                   min_val: float,
                   max_val: float) -> pd.DataFrame:
    """Validate numeric column is within expected range."""
    df[col] = pd.to_numeric(df[col], errors='coerce')
    invalid = (df[col] < min_val) | (df[col] > max_val)
    df.loc[invalid, f'_dq_flag_{col}'] = f'OUT_OF_RANGE_{min_val}_{max_val}'
    print(f'Range violations in {col}: {invalid.sum()} rows')
    return df


def normalize_text_columns(df: pd.DataFrame,
                            cols: list) -> pd.DataFrame:
    """Title case and strip text columns."""
    for col in cols:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
    return df


def drop_complete_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drop completely duplicate rows."""
    before = len(df)
    df = df.drop_duplicates(keep='first').reset_index(drop=True)
    print(f'Dropped {before - len(df)} duplicate rows')
    return df