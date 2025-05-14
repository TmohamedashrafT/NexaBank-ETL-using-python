import pandas as pd
import numpy as np
from datetime import datetime
from typing import Callable, Optional
from crypto_services.Encryptor import CaesarEncryptor
import random

class Transformer:
    """A class for transforming customer-related datasets: profiles, credit billing, support tickets, transactions, and loans."""

    DAILY_FINE_RATE: float = 5.15
    COST_BASE: float = 0.5
    COST_RATE: float = 0.1

    def __init__(self) -> None:
       self.encryptor = CaesarEncryptor()

    # --- Shared Utility Functions ---

    def _calc_diff_from_today(
        self,
        df: pd.DataFrame,
        date_column: str,
        new_column: str,
        divisor: int,
        date_format: str = '%Y-%m-%d'
    ) -> pd.DataFrame:
        df[date_column] = pd.to_datetime(df[date_column], format=date_format)
        today: datetime.date = datetime.now().date()
        df[new_column] = df[date_column].apply(lambda d: (today - d.date()).days // divisor)
        return df

    def _calc_diff_in_years(
        self,
        df: pd.DataFrame,
        date_column: str,
        new_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> pd.DataFrame:
        return self._calc_diff_from_today(df, date_column, new_column, 365, date_format)

    def _calc_diff_in_days(
        self,
        df: pd.DataFrame,
        date_column: str,
        new_column: str,
        date_format: str = '%Y-%m-%d'
    ) -> pd.DataFrame:
        return self._calc_diff_from_today(df, date_column, new_column, 1, date_format)

    # --- Customer Profile Transformation ---

    def add_tenure_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self._calc_diff_in_years(df, 'account_open_date', 'tenure')
        df['tenure'] = df['tenure'].astype(int)
        return df

    def add_customer_segment_column(self, df: pd.DataFrame) -> pd.DataFrame:
        conditions = [df['tenure'] > 5, df['tenure'] < 1]
        choices = ['loyal', 'Newcomer']
        df['customer_segment'] = np.select(conditions, choices, default='Normal')
        return df

    def transform_customer_profiles(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.add_tenure_column(df)
        return self.add_customer_segment_column(df)

    # --- Credit Card Billing Transformation ---

    def add_fully_paid_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df['fully_paid'] = df['amount_due'] == df['amount_paid']
        return df

    def add_debt_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df['debt'] = df['amount_due'].astype(float) - df['amount_paid'].astype(float)
        return df

    def add_late_days_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df['payment_date'] = pd.to_datetime(df['payment_date'], format='%Y-%m-%d')
        df['due_date'] = pd.to_datetime(df['month'] + '-01', format='%Y-%m-%d')
        df['late_days'] = (df['payment_date'] - df['due_date']).dt.days.clip(lower=0)
        df.drop(columns=['due_date'], inplace=True)
        return df

    def add_fine_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df['Fine'] = df['late_days'] * self.DAILY_FINE_RATE
        return df

    def add_total_amount_column_credit(self, df: pd.DataFrame) -> pd.DataFrame:
        df['total_amount'] = df['amount_due'].astype(float) + df['Fine'].astype(float)
        return df

    def transform_credit_cards_billing(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.add_fully_paid_column(df)
        df = self.add_debt_column(df)
        df = self.add_late_days_column(df)
        df = self.add_fine_column(df)
        return self.add_total_amount_column_credit(df)

    # --- Support Tickets Transformation ---

    def add_age_column_to_support_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._calc_diff_in_days(df, 'complaint_date', 'age')

    def transform_support_tickets(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.add_age_column_to_support_tickets(df)

    # --- Transactions Transformation ---

    def add_cost_column(self, df: pd.DataFrame) -> pd.DataFrame:
        df['cost'] = self.COST_BASE + self.COST_RATE * df['transaction_amount']
        return df

    def add_total_amount_column_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        df['total_amount'] = df['cost'] + df['transaction_amount']
        return df

    def transform_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.add_cost_column(df)
        return self.add_total_amount_column_transactions(df)

    # --- Loans Transformation ---

    def add_age_column_to_loans(self, df: pd.DataFrame) -> pd.DataFrame:
        return self._calc_diff_in_days(df, 'utilization_date', 'age')

    def add_total_cost_to_loans(self, df: pd.DataFrame) -> pd.DataFrame:
        df["total_cost"] = df["amount_utilized"] * 0.20 + 1000
        return df

    def encrypt_loans_reason(self, df: pd.DataFrame):
        random_key = random.randint(1, 25)
        df['loan_reason'] = df['loan_reason'].apply(lambda x: self.encryptor.encrypt(x, random_key))
        return df

    def transform_loans(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.add_age_column_to_loans(df)
        df = self.add_total_cost_to_loans(df)
        df = self.encrypt_loans_reason(df)
        return df

    # --- Main Dispatcher ---

    def run_transform(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Dispatch transformation based on table name.
        Returns original dataframe if transform function not found.
        """
        method_name: str = f"transform_{table_name}"
        transform_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = getattr(self, method_name, None)
        if callable(transform_func):
            return transform_func(df)
        print(f"[INFO] No transform method '{method_name}' found. Returning original dataframe.")
        return df
