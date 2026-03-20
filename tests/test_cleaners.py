import pytest
import pandas as pd
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cleaners.standard_rules import (
    strip_whitespace,
    handle_nulls,
    remove_outliers,
    validate_range,
    normalize_text_columns,
    drop_complete_duplicates
)


# ── Test Data ─────────────────────────────────────────────────

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'country_id': ['C001', 'C002', 'C003', 'C004', 'C004'],
        'country_name': ['  india  ', 'CHINA', 'usa', 'Brazil', 'Brazil'],
        'region': ['South Asia', 'East Asia', 'North America', 'South America', 'South America'],
        'literacy_rate': [74.37, 96.84, 99.00, 93.23, 93.23],
        'year': [2020, 2020, 2020, 2020, 2020]
    })


# ── Tests ─────────────────────────────────────────────────────

def test_strip_whitespace(sample_df):
    result = strip_whitespace(sample_df)
    assert result['country_name'].iloc[0] == 'india'
    print('✅ test_strip_whitespace PASSED')


def test_handle_nulls():
    df = pd.DataFrame({
        'country_id': ['C001', None, 'C003'],
        'country_name': ['India', 'China', ''],
        'literacy_rate': [74.37, 96.84, 99.00]
    })
    result = handle_nulls(df, ['country_id', 'country_name'])
    assert '_dq_flag_country_id' in result.columns
    assert '_dq_flag_country_name' in result.columns
    print('✅ test_handle_nulls PASSED')


def test_validate_range(sample_df):
    result = validate_range(sample_df, 'literacy_rate', 0, 100)
    assert '_dq_flag_literacy_rate' not in result.columns or \
           result['_dq_flag_literacy_rate'].isna().all()
    print('✅ test_validate_range PASSED')


def test_validate_range_invalid():
    df = pd.DataFrame({
        'literacy_rate': [74.37, -5.00, 102.00]
    })
    result = validate_range(df, 'literacy_rate', 0, 100)
    assert '_dq_flag_literacy_rate' in result.columns
    print('✅ test_validate_range_invalid PASSED')


def test_normalize_text_columns(sample_df):
    result = normalize_text_columns(sample_df, ['country_name', 'region'])
    assert result['country_name'].iloc[1] == 'China'
    assert result['country_name'].iloc[2] == 'Usa'
    print('✅ test_normalize_text_columns PASSED')


def test_drop_complete_duplicates(sample_df):
    result = drop_complete_duplicates(sample_df)
    assert len(result) == 4
    print('✅ test_drop_complete_duplicates PASSED')


def test_remove_outliers():
    df = pd.DataFrame({
        'literacy_rate': [74.37, 96.84, 99.00, 93.23, 1.00, 0.50]
    })
    result = remove_outliers(df, 'literacy_rate')
    assert '_dq_flag_literacy_rate' in result.columns
    print('✅ test_remove_outliers PASSED')