from extractors.base_extractor import BaseExtractor
import pandas as pd
from typing import Iterator

class CSVExtractor(BaseExtractor):
    
    def __init__(self, file_path: str, batch_size: int = 10_000):
        super().__init__(source_id='csv', batch_size=batch_size)
        self.file_path = file_path

    def connect(self):
        pass  # No connection needed for CSV

    def extract(self, **kwargs) -> Iterator[pd.DataFrame]:
        self.logger.info(f'Extracting from {self.file_path}')
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.batch_size,
            dtype=str,
            keep_default_na=False
        ):
            if self.validate_schema(chunk):
                self._records_extracted += len(chunk)
                yield chunk

    def validate_schema(self, df: pd.DataFrame) -> bool:
        required = {'customer_id', 'full_name', 'email'}
        return required.issubset(df.columns)