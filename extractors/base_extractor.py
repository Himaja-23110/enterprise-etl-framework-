from abc import ABC, abstractmethod
from typing import Iterator
import pandas as pd
import logging

class BaseExtractor(ABC):
    """Abstract base for all data source extractors."""

    def __init__(self, source_id: str, batch_size: int = 10_000):
        self.source_id = source_id
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)
        self._records_extracted = 0

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the source system."""

    @abstractmethod
    def extract(self, **kwargs) -> Iterator[pd.DataFrame]:
        """Yield DataFrames in batches for memory efficiency."""

    @abstractmethod
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """Validate extracted schema against expected contract."""

    def disconnect(self) -> None:
        """Cleanup connections — override if needed."""
        pass

    @property
    def records_extracted(self) -> int:
        return self._records_extracted