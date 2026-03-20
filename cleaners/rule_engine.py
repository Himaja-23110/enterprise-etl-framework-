from dataclasses import dataclass, field
from typing import Callable, List
import pandas as pd
import logging

@dataclass
class CleaningRule:
    name: str
    columns: List[str]
    func: Callable[[pd.DataFrame], pd.DataFrame]
    severity: str = 'WARNING'  # WARNING | ERROR | CRITICAL
    enabled: bool = True


class RuleEngine:
    
    def __init__(self):
        self.rules: List[CleaningRule] = []
        self.report: dict = {}
        self.logger = logging.getLogger(__name__)

    def register(self, rule: CleaningRule):
        self.rules.append(rule)
        return self  # allows chaining

    def run(self, df: pd.DataFrame):
        """
        Runs all registered rules on the dataframe.
        Returns (clean_df, quarantine_df)
        """
        quarantine_mask = pd.Series(False, index=df.index)

        for rule in self.rules:
            if not rule.enabled:
                continue
            try:
                df = rule.func(df)
                self.report[rule.name] = 'PASS'
                self.logger.info(f'Rule PASSED: {rule.name}')
            except Exception as e:
                self.report[rule.name] = f'FAIL: {e}'
                self.logger.error(f'Rule FAILED: {rule.name} -> {e}')
                if rule.severity == 'CRITICAL':
                    raise

        clean_df = df[~quarantine_mask].reset_index(drop=True)
        quarantine_df = df[quarantine_mask].reset_index(drop=True)
        return clean_df, quarantine_df

    def get_report(self) -> dict:
        return self.report