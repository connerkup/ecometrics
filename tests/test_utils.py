"""
Unit tests for utility functions.
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import os
import time
from unittest.mock import patch, MagicMock
from pathlib import Path
from src.packagingco_insights.utils.data_loader import (
    connect_to_database,
    load_data,
    load_csv_data,
    load_esg_data,
    load_finance_data,
    load_sales_data,
    get_database_info,
    check_data_quality
)


class TestDataLoader:
    """Test cases for data loading utilities."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path for testing."""
        # Use a unique temporary file path
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, f'test_db_{int(time.time())}.duckdb')
        yield temp_path
        # Cleanup after test
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            os.rmdir(temp_dir)
        except (OSError, PermissionError):
            # Ignore cleanup errors on Windows
            pass
    
    @pytest.fixture
    def sample_csv_data(self):
        """Create sample CSV data for testing."""
        data = {
            'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'value': [100, 200, 300],
            'category': ['A', 'B', 'A']
        }
        return pd.DataFrame(data)
    
    @pytest.fixture
    def temp_csv_path(self, sample_csv_data):
        """Create a temporary CSV file for testing."""
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, f'test_csv_{int(time.time())}.csv')
        sample_csv_data.to_csv(temp_path, index=False)
        yield temp_path
        # Cleanup after test
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            os.rmdir(temp_dir)
        except (OSError, PermissionError):
            # Ignore cleanup errors on Windows
            pass
    
    def test_connect_to_database(self, temp_db_path):
        """Test database connection."""
        conn = None
        try:
            conn = connect_to_database(temp_db_path, create_if_missing=True)
            assert conn is not None
            
            # Test that we can execute a simple query
            result = conn.execute("SELECT 1 as test").fetchdf()
            assert len(result) == 1
            assert result['test'].iloc[0] == 1
        finally:
            if conn:
                conn.close()
    
    def test_connect_to_database_creates_directory(self):
        """Test that database connection creates directory if it doesn't exist."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, 'nonexistent', 'test.duckdb')
        conn = None
        
        try:
            conn = connect_to_database(db_path, create_if_missing=True)
            assert conn is not None
            assert os.path.exists(os.path.dirname(db_path))
        finally:
            if conn:
                conn.close()
            # Cleanup
            try:
                if os.path.exists(db_path):
                    os.remove(db_path)
                if os.path.exists(os.path.dirname(db_path)):
                    os.rmdir(os.path.dirname(db_path))
                os.rmdir(temp_dir)
            except (OSError, PermissionError):
                pass
    
    def test_load_data(self, temp_db_path):
        """Test loading data from database."""
        conn = None
        try:
            # Create a test table
            conn = connect_to_database(temp_db_path, create_if_missing=True)
            conn.execute("""
                CREATE TABLE test_table (
                    id INTEGER,
                    name VARCHAR,
                    value DOUBLE
                )
            """)
            conn.execute("""
                INSERT INTO test_table VALUES 
                (1, 'Test1', 100.0),
                (2, 'Test2', 200.0)
            """)
            conn.close()
            conn = None
            
            # Test loading data
            query = "SELECT * FROM test_table ORDER BY id"
            # create_if_missing defaults to False for load_data, so we need to ensure the DB file exists
            # which it does, because we just created and closed it above
            df = load_data(query, temp_db_path)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert list(df.columns) == ['id', 'name', 'value']
            assert df['id'].iloc[0] == 1
            assert df['name'].iloc[0] == 'Test1'
            assert df['value'].iloc[0] == 100.0
        finally:
            if conn:
                conn.close()
    
    def test_load_csv_data(self, temp_csv_path, sample_csv_data):
        """Test loading CSV data."""
        df = load_csv_data(temp_csv_path)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ['date', 'value', 'category']
        pd.testing.assert_frame_equal(df, sample_csv_data)
    
    def test_load_csv_data_file_not_found(self):
        """Test loading CSV data with non-existent file."""
        with pytest.raises(FileNotFoundError, match="File not found"):
            load_csv_data("nonexistent_file.csv")
    
    def test_load_csv_data_with_kwargs(self, temp_csv_path):
        """Test loading CSV data with additional kwargs."""
        # Test with different separator
        df = load_csv_data(temp_csv_path, sep=',')
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
    
    def test_check_data_quality(self):
        """Test data quality checking."""
        # Create test data with some issues
        data = {
            'numeric_col': [1, 2, np.nan, 4, 5],
            'text_col': ['A', 'B', 'A', None, 'C'],
            'duplicate_col': [1, 1, 2, 2, 3]
        }
        df = pd.DataFrame(data)
        
        quality_report = check_data_quality(df)
        
        assert isinstance(quality_report, dict)
        assert quality_report['total_rows'] == 5
        assert quality_report['total_columns'] == 3
        assert quality_report['duplicate_rows'] == 0  # No completely duplicate rows
        
        # Check missing values
        assert quality_report['missing_values']['numeric_col'] == 1
        assert quality_report['missing_values']['text_col'] == 1
        assert quality_report['missing_values']['duplicate_col'] == 0
        
        # Check numeric stats
        assert 'numeric_col' in quality_report['numeric_stats']
        assert 'duplicate_col' in quality_report['numeric_stats']
        assert 'text_col' not in quality_report['numeric_stats']
        
        # Check numeric column stats
        numeric_stats = quality_report['numeric_stats']['numeric_col']
        assert 'min' in numeric_stats
        assert 'max' in numeric_stats
        assert 'mean' in numeric_stats
        assert 'std' in numeric_stats
    
    def test_check_data_quality_empty_dataframe(self):
        """Test data quality checking with empty DataFrame."""
        df = pd.DataFrame()
        quality_report = check_data_quality(df)
        
        assert quality_report['total_rows'] == 0
        assert quality_report['total_columns'] == 0
        assert quality_report['duplicate_rows'] == 0
        assert len(quality_report['missing_values']) == 0
        assert len(quality_report['numeric_stats']) == 0
    
    def test_get_database_info(self, temp_db_path):
        """Test getting database information."""
        conn = None
        try:
            # Create test tables
            conn = connect_to_database(temp_db_path, create_if_missing=True)
            conn.execute("""
                CREATE TABLE test_table1 (
                    id INTEGER,
                    name VARCHAR
                )
            """)
            conn.execute("""
                CREATE TABLE test_table2 (
                    id INTEGER,
                    value DOUBLE
                )
            """)
            conn.execute("INSERT INTO test_table1 VALUES (1, 'Test1'), (2, 'Test2')")
            conn.execute("INSERT INTO test_table2 VALUES (1, 100.0)")
            conn.close()
            conn = None
            
            # Get database info
            info = get_database_info(temp_db_path)
            
            assert isinstance(info, dict)
            assert 'tables' in info
            assert 'schemas' in info
            assert 'row_counts' in info
            
            # Check tables
            assert 'test_table1' in info['tables']
            assert 'test_table2' in info['tables']
            
            # Check row counts
            assert info['row_counts']['test_table1'] == 2
            assert info['row_counts']['test_table2'] == 1
        finally:
            if conn:
                conn.close()
    
    def test_load_esg_data_mock(self, temp_db_path):
        """Test loading ESG data with mocked database."""
        with patch('src.packagingco_insights.utils.data_loader.connect_to_database') as mock_connect:
            # Mock the connection and query results
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            # Mock the query result
            mock_df = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'facility': ['Facility A', 'Facility B'],
                'emissions': [100.0, 150.0],
                'energy_consumption': [500.0, 600.0],
                'waste_generated': [50.0, 75.0]
            })
            mock_conn.execute.return_value.fetchdf.return_value = mock_df
            
            # Test the function
            with patch('src.packagingco_insights.utils.data_loader.check_dbt_tables_exist', return_value=True):
                result, source = load_esg_data(temp_db_path)

                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert list(result.columns) == ['date', 'facility', 'emissions', 'energy_consumption', 'waste_generated']
                assert source == "dbt_models"
    
    def test_load_finance_data_mock(self, temp_db_path):
        """Test loading finance data with mocked database."""
        with patch('src.packagingco_insights.utils.data_loader.connect_to_database') as mock_connect:
            # Mock the connection and query results
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            # Mock the query result
            mock_df = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'region': ['North', 'South'],
                'revenue': [10000.0, 15000.0],
                'costs': [8000.0, 12000.0],
                'profit': [2000.0, 3000.0]
            })
            mock_conn.execute.return_value.fetchdf.return_value = mock_df
            
            # Test the function
            with patch('src.packagingco_insights.utils.data_loader.check_dbt_tables_exist', return_value=True):
                result, source = load_finance_data(temp_db_path)

                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert list(result.columns) == ['date', 'region', 'revenue', 'costs', 'profit']
                assert source == "dbt_models"
    
    def test_load_sales_data_mock(self, temp_db_path):
        """Test loading sales data with mocked database."""
        with patch('src.packagingco_insights.utils.data_loader.connect_to_database') as mock_connect:
            # Mock the connection and query results
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            # Mock the query result
            mock_df = pd.DataFrame({
                'date': ['2023-01-01', '2023-01-02'],
                'product': ['Product A', 'Product B'],
                'facility': ['Facility A', 'Facility B'],
                'quantity': [100, 150],
                'revenue': [5000.0, 7500.0]
            })
            mock_conn.execute.return_value.fetchdf.return_value = mock_df
            
            # Test the function
            with patch('src.packagingco_insights.utils.data_loader.check_dbt_tables_exist', return_value=True):
                result, source = load_sales_data(temp_db_path)

                assert isinstance(result, pd.DataFrame)
                assert len(result) == 2
                assert list(result.columns) == ['date', 'product', 'facility', 'quantity', 'revenue']
                assert source == "dbt_models"
    
    def test_data_quality_with_mixed_types(self):
        """Test data quality checking with mixed data types."""
        data = {
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'string_col': ['A', 'B', 'C', 'D', 'E'],
            'bool_col': [True, False, True, False, True],
            'mixed_col': [1, 'A', 2.5, True, None]
        }
        df = pd.DataFrame(data)
        
        quality_report = check_data_quality(df)
        
        assert quality_report['total_rows'] == 5
        assert quality_report['total_columns'] == 5
        
        # Check that numeric columns are identified correctly
        assert 'int_col' in quality_report['numeric_stats']
        assert 'float_col' in quality_report['numeric_stats']
        assert 'bool_col' in quality_report['numeric_stats']
        assert 'string_col' not in quality_report['numeric_stats']
        assert 'mixed_col' not in quality_report['numeric_stats']
    
    def test_data_quality_with_duplicates(self):
        """Test data quality checking with duplicate rows."""
        data = {
            'col1': [1, 2, 1, 2, 3],
            'col2': ['A', 'B', 'A', 'B', 'C']
        }
        df = pd.DataFrame(data)
        
        quality_report = check_data_quality(df)
        
        assert quality_report['total_rows'] == 5
        assert quality_report['duplicate_rows'] == 2  # Two duplicate rows 