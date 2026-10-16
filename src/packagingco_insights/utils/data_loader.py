"""
Data loading utilities for the BI portfolio.
"""

import pandas as pd
import duckdb
import sqlite3
from typing import Optional, Dict, Any, Tuple
import os
from pathlib import Path
import numpy as np


def connect_to_database(db_path: str = "data/processed/portfolio.duckdb", create_if_missing: bool = False) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Connect to the DuckDB database.
    
    Args:
        db_path: Path to the DuckDB database file
        create_if_missing: If True, creates the database directory and file if it doesn't exist
    
    Returns:
        DuckDB connection object or None if database doesn't exist and create_if_missing is False
    """
    try:
        # Check if database file exists
        if not os.path.exists(db_path):
            if create_if_missing:
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
            else:
                return None
            
        # Connect to DuckDB
        conn = duckdb.connect(db_path)
        return conn
    except Exception:
        return None


def check_dbt_tables_exist(db_path: str = "data/processed/portfolio.duckdb") -> bool:
    """
    Check if the required dbt tables exist in the database.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        True if all required tables exist, False otherwise
    """
    conn = connect_to_database(db_path)
    if conn is None:
        return False
    
    try:
        # Check for required tables
        required_tables = ['fact_esg_monthly', 'fact_financial_monthly', 'stg_sales_data']
        
        # Get list of existing tables
        tables_query = "SHOW TABLES"
        tables = conn.execute(tables_query).fetchdf()
        existing_tables = tables['name'].tolist()
        
        # Check if all required tables exist
        for table in required_tables:
            if table not in existing_tables:
                return False
        
        # Check if tables have data
        for table in required_tables:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count = conn.execute(count_query).fetchdf()
            if count['count'].iloc[0] == 0:
                return False
                
        return True
    except Exception:
        return False
    finally:
        conn.close()


def load_data(query: str, 
              db_path: str = "data/processed/portfolio.duckdb",
              **kwargs: Any) -> pd.DataFrame:
    """
    Load data from the database using a SQL query.
    
    Args:
        query: SQL query to execute
        db_path: Path to the database file
        **kwargs: Additional arguments to pass to pandas.read_sql
    
    Returns:
        DataFrame with the query results
    """
    conn = connect_to_database(db_path)
    if conn is None:
        raise FileNotFoundError(f"Database not found: {db_path}")
    
    try:
        # Use DuckDB's execute method instead of pandas read_sql
        df = conn.execute(query).fetchdf()
        return df
    finally:
        conn.close()


def load_csv_data(file_path: str, **kwargs: Any) -> pd.DataFrame:
    """
    Load data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        **kwargs: Additional arguments to pass to pandas.read_csv
    
    Returns:
        DataFrame with the CSV data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    return pd.read_csv(file_path, **kwargs)


def create_sample_esg_data() -> pd.DataFrame:
    """
    Create sample ESG data from raw CSV files to mimic dbt models.
    
    Returns:
        DataFrame with processed ESG data
    """
    try:
        # Load raw ESG data
        esg_data = load_csv_data("data/raw/sample_esg_data.csv")
        
        # Process to mimic fact_esg_monthly structure
        esg_data['date'] = pd.to_datetime(esg_data['date'])
        esg_data['month'] = esg_data['date'].dt.to_period('M')
        
        # Group by month and product line, then aggregate
        monthly_esg = esg_data.groupby(['month', 'product_line']).agg({
            'total_emissions_kg_co2': 'sum',
            'recycled_material_pct': 'mean',
            'energy_efficiency_rating': 'mean',
            'quality_score': 'mean',
            'waste_reduction_pct': 'mean'
        }).reset_index()
        
        # Rename columns to match dbt model
        monthly_esg.rename(columns={
            'recycled_material_pct': 'avg_recycled_material_pct',
            'energy_efficiency_rating': 'avg_efficiency_rating',
            'quality_score': 'avg_quality_score',
            'waste_reduction_pct': 'avg_waste_reduction_pct'
        }, inplace=True)
        
        # Convert month back to datetime
        monthly_esg['date'] = monthly_esg['month'].dt.to_timestamp()
        
        return monthly_esg
    except Exception as e:
        # Return minimal sample data if processing fails
        return pd.DataFrame({
            'month': pd.date_range('2023-01-01', periods=12, freq='ME'),
            'product_line': ['Product A'] * 12,
            'total_emissions_kg_co2': [1000 + i*100 for i in range(12)],
            'avg_recycled_material_pct': [75 + i*2 for i in range(12)],
            'avg_efficiency_rating': [8.0 + i*0.1 for i in range(12)],
            'avg_quality_score': [9.0 + i*0.05 for i in range(12)],
            'avg_waste_reduction_pct': [20 + i*1 for i in range(12)],
            'date': pd.date_range('2023-01-01', periods=12, freq='ME')
        })


def create_sample_finance_data() -> pd.DataFrame:
    """
    Create sample financial data from raw CSV files to mimic dbt models.
    
    Returns:
        DataFrame with processed financial data
    """
    try:
        # Load raw sales data
        sales_data = load_csv_data("data/raw/sample_sales_data.csv")
        
        # Process to mimic fact_financial_monthly structure
        sales_data['date'] = pd.to_datetime(sales_data['date'])
        sales_data['month'] = sales_data['date'].dt.to_period('M')
        
        # Calculate financial metrics using correct column names
        sales_data['total_revenue'] = sales_data['revenue']
        sales_data['total_cost'] = sales_data['cost_of_goods'] + sales_data['operating_cost']
        sales_data['total_profit'] = sales_data['total_revenue'] - sales_data['total_cost']
        sales_data['total_profit_margin'] = sales_data['profit_margin'] * 100
        
        # Group by month and product line, then aggregate
        monthly_finance = sales_data.groupby(['month', 'product_line']).agg({
            'total_revenue': 'sum',
            'total_cost': 'sum',
            'total_profit': 'sum',
            'total_profit_margin': 'mean',
            'units_sold': 'sum'
        }).reset_index()
        
        # Rename column to match dbt model
        monthly_finance.rename(columns={'units_sold': 'total_quantity'}, inplace=True)
        
        # Convert month back to datetime
        monthly_finance['date'] = monthly_finance['month'].dt.to_timestamp()
        
        return monthly_finance
    except Exception as e:
        # Return minimal sample data if processing fails
        return pd.DataFrame({
            'month': pd.date_range('2023-01-01', periods=12, freq='ME'),
            'product_line': ['Product A'] * 12,
            'total_revenue': [100000 + i*10000 for i in range(12)],
            'total_cost': [70000 + i*7000 for i in range(12)],
            'total_profit': [30000 + i*3000 for i in range(12)],
            'total_profit_margin': [30 + i*0.5 for i in range(12)],
            'total_quantity': [1000 + i*100 for i in range(12)],
            'date': pd.date_range('2023-01-01', periods=12, freq='ME')
        })


def load_esg_data(db_path: str = "data/processed/portfolio.duckdb") -> Tuple[pd.DataFrame, str]:
    """
    Load ESG data from the database or fall back to sample data.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        Tuple of (DataFrame with ESG data, data source indicator)
    """
    # First try to load from dbt models
    if check_dbt_tables_exist(db_path):
        try:
            query = """
            SELECT * FROM fact_esg_monthly
            ORDER BY month
            """
            df = load_data(query, db_path)
            return df, "dbt_models"
        except Exception:
            pass
    
    # Fall back to sample data
    df = create_sample_esg_data()
    return df, "sample_csv"


def load_finance_data(db_path: str = "data/processed/portfolio.duckdb") -> Tuple[pd.DataFrame, str]:
    """
    Load financial data from the database or fall back to sample data.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        Tuple of (DataFrame with financial data, data source indicator)
    """
    # First try to load from dbt models
    if check_dbt_tables_exist(db_path):
        try:
            query = """
            SELECT * FROM fact_financial_monthly
            ORDER BY month
            """
            df = load_data(query, db_path)
            return df, "dbt_models"
        except Exception:
            pass
    
    # Fall back to sample data
    df = create_sample_finance_data()
    return df, "sample_csv"


def load_sales_data(db_path: str = "data/processed/portfolio.duckdb") -> Tuple[pd.DataFrame, str]:
    """
    Load sales data from the database or fall back to sample data.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        Tuple of (DataFrame with sales data, data source indicator)
    """
    # First try to load from dbt models
    if check_dbt_tables_exist(db_path):
        try:
            query = """
            SELECT * FROM stg_sales_data
            ORDER BY date
            """
            df = load_data(query, db_path)
            return df, "dbt_models"
        except Exception:
            pass
    
    # Fall back to sample data
    try:
        df = load_csv_data("data/raw/sample_sales_data.csv")
        return df, "sample_csv"
    except Exception:
        # Return minimal sample data if file not found
        df = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=365, freq='D'),
            'product_id': [f'PROD_{i:03d}' for i in range(1, 101)],
            'quantity': [10 + i for i in range(365)],
            'unit_price': [100 + i*0.5 for i in range(365)],
            'unit_cost': [70 + i*0.3 for i in range(365)]
        })
        return df, "generated_sample"


def get_database_info(db_path: str = "data/processed/portfolio.duckdb") -> Dict[str, Any]:
    """
    Get information about the database and its tables.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        Dictionary with database information
    """
    conn = connect_to_database(db_path)
    if conn is None:
        return {
            'tables': [],
            'schemas': {},
            'row_counts': {},
            'status': 'database_not_found'
        }
    
    try:
        # Get list of tables
        tables_query = "SHOW TABLES"
        tables = conn.execute(tables_query).fetchdf()
        
        # Get table schemas
        schemas = {}
        for table in tables['name']:
            schema_query = f"DESCRIBE {table}"
            schema = conn.execute(schema_query).fetchdf()
            schemas[table] = schema
        
        # Get row counts
        row_counts = {}
        for table in tables['name']:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count = conn.execute(count_query).fetchdf()
            row_counts[table] = count['count'].iloc[0]
        
        return {
            'tables': tables['name'].tolist(),
            'schemas': schemas,
            'row_counts': row_counts,
            'status': 'connected'
        }
    finally:
        conn.close()


def check_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform basic data quality checks on a DataFrame.
    
    Args:
        df: DataFrame to check
    
    Returns:
        Dictionary with data quality metrics
    """
    quality_report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'data_types': df.dtypes.to_dict()
    }
    
    # Check for numeric columns and their ranges
    # Include boolean columns as they are numeric in pandas
    numeric_columns = df.select_dtypes(include=[np.number, 'bool']).columns
    numeric_stats = {}
    for col in numeric_columns:
        numeric_stats[col] = {
            'min': df[col].min(),
            'max': df[col].max(),
            'mean': df[col].mean(),
            'std': df[col].std()
        }
    
    quality_report['numeric_stats'] = numeric_stats
    
    return quality_report 