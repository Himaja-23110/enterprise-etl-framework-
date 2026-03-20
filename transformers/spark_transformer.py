import pandas as pd
import logging


class SparkTransformer:
    """
    PySpark-style transformer using Pandas for local execution.
    In production, replace with actual PySpark DataFrame operations.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def clean_literacy_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize world literacy rate data."""
        df = df.copy()

        # Normalize country names
        df['country_name'] = df['country_name'].str.strip().str.title()

        # Normalize region
        df['region'] = df['region'].str.strip().str.title()

        # Cast numeric columns
        numeric_cols = ['literacy_rate', 'male_literacy',
                        'female_literacy', 'population',
                        'gdp_per_capita', 'education_expenditure_pct']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Validate literacy rate 0-100
        invalid_literacy = (df['literacy_rate'] < 0) | (df['literacy_rate'] > 100)
        df.loc[invalid_literacy, '_dq_flag_literacy'] = 'INVALID_RATE'

        # Validate year
        invalid_year = (df['year'] < 1900) | (df['year'] > 2030)
        df.loc[invalid_year, '_dq_flag_year'] = 'INVALID_YEAR'

        self.logger.info(f'Cleaned {len(df)} literacy records')
        return df

    def aggregate_by_region(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate literacy statistics by region — Gold layer."""
        df['literacy_rate'] = pd.to_numeric(df['literacy_rate'], errors='coerce')
        df['population'] = pd.to_numeric(df['population'], errors='coerce')

        agg_df = df.groupby('region').agg(
            total_countries=('country_name', 'count'),
            avg_literacy_rate=('literacy_rate', 'mean'),
            min_literacy_rate=('literacy_rate', 'min'),
            max_literacy_rate=('literacy_rate', 'max'),
            total_population=('population', 'sum')
        ).reset_index()

        agg_df['avg_literacy_rate'] = agg_df['avg_literacy_rate'].round(2)
        self.logger.info(f'Aggregated {len(agg_df)} regions')
        return agg_df

    def add_literacy_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize countries by literacy level."""
        df['literacy_rate'] = pd.to_numeric(df['literacy_rate'], errors='coerce')

        def categorize(rate):
            if pd.isna(rate):
                return 'Unknown'
            elif rate >= 95:
                return 'Very High'
            elif rate >= 80:
                return 'High'
            elif rate >= 60:
                return 'Medium'
            else:
                return 'Low'

        df['literacy_category'] = df['literacy_rate'].apply(categorize)
        self.logger.info('Literacy categories assigned')
        return df