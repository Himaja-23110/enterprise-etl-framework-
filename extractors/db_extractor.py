from extractors.base_extractor import BaseExtractor
import pandas as pd
from typing import Iterator
from sqlalchemy import create_engine, text

class DatabaseExtractor(BaseExtractor):

    def __init__(self, connection_string: str, query: str, batch_size: int = 10_000):
        super().__init__(source_id='database', batch_size=batch_size)
        self.connection_string = connection_string
        self.query = query
        self._engine = None

    def connect(self):
        self._engine = create_engine(self.connection_string)
        self.logger.info('Database connection established')

    def extract(self, **kwargs) -> Iterator[pd.DataFrame]:
        self.connect()
        for chunk in pd.read_sql(
            self.query,
            self._engine,
            chunksize=self.batch_size
        ):
            self._records_extracted += len(chunk)
            yield chunk

    def validate_schema(self, df: pd.DataFrame) -> bool:
        required = {'customer_id', 'full_name', 'email'}
        return required.issubset(df.columns)

    def disconnect(self):
        if self._engine:
            self._engine.dispose()
            self.logger.info('Database connection closed')