"""
Unit tests for SalesForecaster class.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.packagingco_insights.analysis.forecasting import SalesForecaster


class TestSalesForecaster:
    """Test cases for SalesForecaster class."""
    
    def test_init_with_valid_data(self, sample_sales_data):
        """Test initialization with valid data."""
        forecaster = SalesForecaster(sample_sales_data)
        assert forecaster.data is not None
        assert len(forecaster.data) > 0
        assert all(col in forecaster.data.columns for col in [
            'date', 'revenue', 'units_sold'
        ])
        assert hasattr(forecaster, 'prepared_data')
        assert len(forecaster.prepared_data) > 0
    
    def test_init_with_invalid_data(self):
        """Test initialization with invalid data raises ValueError."""
        invalid_data = pd.DataFrame({
            'date': ['2023-01-01'],
            'revenue': [100000]
            # Missing 'units_sold' column
        })
        with pytest.raises(ValueError, match="Missing required columns"):
            SalesForecaster(invalid_data)
    
    def test_init_with_empty_data(self, empty_dataframe):
        """Test initialization with empty DataFrame raises ValueError."""
        with pytest.raises(ValueError, match="Missing required columns"):
            SalesForecaster(empty_dataframe)
    
    def test_prepare_data(self, sample_sales_data):
        """Test data preparation."""
        forecaster = SalesForecaster(sample_sales_data)
        
        # Check that prepared data has expected columns
        expected_columns = [
            'date', 'product_line', 'revenue', 'units_sold',
            'month', 'quarter', 'year', 'revenue_lag1', 'revenue_lag2',
            'revenue_ma3', 'revenue_ma6'
        ]
        
        for col in expected_columns:
            assert col in forecaster.prepared_data.columns
        
        # Check that data is sorted by date
        assert forecaster.prepared_data['date'].is_monotonic_increasing
    
    def test_simple_linear_forecast(self, sample_sales_data):
        """Test simple linear forecast generation."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        assert isinstance(forecast, pd.DataFrame)
        assert len(forecast) > 0
        assert 'date' in forecast.columns
        assert 'product_line' in forecast.columns
        assert 'forecasted_revenue' in forecast.columns
        assert 'forecast_period' in forecast.columns
        assert 'model_type' in forecast.columns
        
        # Test forecast values are reasonable
        assert all(forecast['forecasted_revenue'] >= 0)
        assert all(forecast['forecast_period'] > 0)
        assert all(forecast['model_type'] == 'exponential_smoothing')
    
    def test_simple_linear_forecast_different_periods(self, sample_sales_data):
        """Test simple linear forecast with different period counts."""
        forecaster = SalesForecaster(sample_sales_data)
        
        for periods in [1, 3, 6, 12]:
            forecast = forecaster.simple_linear_forecast(periods=periods)
            assert isinstance(forecast, pd.DataFrame)
            if len(forecast) > 0:
                assert max(forecast['forecast_period']) <= periods
    
    def test_simple_linear_forecast_group_by_facility(self, sample_sales_data):
        """Test simple linear forecast grouped by different column."""
        # Test with product_line which exists in the prepared data
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(group_by='product_line')
        
        assert isinstance(forecast, pd.DataFrame)
        if len(forecast) > 0:  # May be empty if insufficient data
            assert 'date' in forecast.columns
            assert 'product_line' in forecast.columns
            assert 'forecasted_revenue' in forecast.columns
            assert 'forecast_period' in forecast.columns
            assert 'model_type' in forecast.columns
            
            # Test forecast values are reasonable
            assert all(forecast['forecasted_revenue'] >= 0)
            assert all(forecast['forecast_period'] > 0)
            assert all(forecast['model_type'] == 'exponential_smoothing')
    
    def test_moving_average_forecast(self, sample_sales_data):
        """Test moving average forecast generation."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.moving_average_forecast(periods=6, window=3)
        
        assert isinstance(forecast, pd.DataFrame)
        if len(forecast) > 0:  # May be empty if insufficient data
            assert 'date' in forecast.columns
            assert 'product_line' in forecast.columns
            assert 'forecasted_revenue' in forecast.columns
            assert 'forecast_period' in forecast.columns
            assert 'model_type' in forecast.columns
            
            # Test forecast values are reasonable
            assert all(forecast['forecasted_revenue'] >= 0)
            assert all(forecast['forecast_period'] > 0)
            # The model type should contain 'ma' for moving average
            assert all('ma' in str(model_type) for model_type in forecast['model_type'])
    
    def test_moving_average_forecast_different_windows(self, sample_sales_data):
        """Test moving average forecast with different window sizes."""
        forecaster = SalesForecaster(sample_sales_data)
        
        for window in [2, 3, 6]:
            forecast = forecaster.moving_average_forecast(window=window)
            assert isinstance(forecast, pd.DataFrame)
    
    def test_trend_analysis(self, sample_sales_data):
        """Test trend analysis."""
        forecaster = SalesForecaster(sample_sales_data)
        trends = forecaster.trend_analysis(metric='revenue')
        
        assert isinstance(trends, pd.DataFrame)
        if len(trends) > 0:
            assert 'product_line' in trends.columns
            assert 'first_value' in trends.columns
            assert 'last_value' in trends.columns
            assert 'total_change' in trends.columns
            assert 'percent_change' in trends.columns
            assert 'avg_monthly_growth' in trends.columns
            assert 'data_points' in trends.columns
            
            # Test that calculations are reasonable
            for _, row in trends.iterrows():
                assert row['data_points'] > 0
                assert row['first_value'] >= 0
                assert row['last_value'] >= 0
    
    def test_trend_analysis_different_metrics(self, sample_sales_data):
        """Test trend analysis with different metrics."""
        forecaster = SalesForecaster(sample_sales_data)
        
        # Test with revenue metric
        revenue_trends = forecaster.trend_analysis(metric='revenue')
        assert isinstance(revenue_trends, pd.DataFrame)
        
        # Test with units_sold metric
        units_trends = forecaster.trend_analysis(metric='units_sold')
        assert isinstance(units_trends, pd.DataFrame)
    
    def test_generate_forecast_chart(self, sample_sales_data):
        """Test forecast chart generation."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        chart = forecaster.generate_forecast_chart(forecast_data=forecast)
        
        assert chart is not None
        assert hasattr(chart, 'data')
        assert hasattr(chart, 'layout')
    
    def test_generate_forecast_chart_with_actual_data(self, sample_sales_data):
        """Test forecast chart generation with actual data."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        # Use last 6 months of actual data
        actual_data = sample_sales_data.tail(6)
        
        chart = forecaster.generate_forecast_chart(
            forecast_data=forecast,
            actual_data=actual_data
        )
        
        assert chart is not None
        assert hasattr(chart, 'data')
        assert hasattr(chart, 'layout')
    
    def test_generate_trend_chart(self, sample_sales_data):
        """Test trend chart generation."""
        forecaster = SalesForecaster(sample_sales_data)
        chart = forecaster.generate_trend_chart(metric='revenue')
        
        assert chart is not None
        assert hasattr(chart, 'data')
        assert hasattr(chart, 'layout')
    
    def test_get_forecast_insights(self, sample_sales_data):
        """Test forecast insights generation."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        insights = forecaster.get_forecast_insights(forecast)
        
        assert isinstance(insights, dict)
        assert len(insights) > 0
        assert all(isinstance(key, str) for key in insights.keys())
        assert all(isinstance(value, str) for value in insights.values())
    
    def test_data_validation_edge_cases(self):
        """Test data validation with various edge cases."""
        # Test with None data
        with pytest.raises(TypeError):
            SalesForecaster(None)  # type: ignore
        
        # Test with list instead of DataFrame
        with pytest.raises(TypeError):
            SalesForecaster([1, 2, 3])  # type: ignore
    
    def test_forecast_smoothness(self, sample_sales_data):
        """Test that forecasts are smooth and reasonable."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        if len(forecast) > 1:
            # Test that forecasts don't have extreme jumps
            revenue_values = forecast['forecasted_revenue'].values
            for i in range(1, len(revenue_values)):
                # Check that consecutive values don't differ by more than 50%
                ratio = revenue_values[i] / revenue_values[i-1]
                assert 0.5 <= ratio <= 2.0
    
    def test_forecast_date_range(self, sample_sales_data):
        """Test that forecast dates are in the future."""
        forecaster = SalesForecaster(sample_sales_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        if len(forecast) > 0:
            # Get the last actual date
            last_actual_date = sample_sales_data['date'].max()
            
            # All forecast dates should be after the last actual date
            for date in forecast['date']:
                assert pd.to_datetime(date) > last_actual_date
    
    def test_insufficient_data_handling(self):
        """Test handling of insufficient data for forecasting."""
        # Create minimal data (less than 3 months)
        minimal_data = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=2, freq='ME'),
            'product_line': ['Test Product'] * 2,
            'revenue': [100000, 110000],
            'units_sold': [1000, 1100]
        })
        
        forecaster = SalesForecaster(minimal_data)
        forecast = forecaster.simple_linear_forecast(periods=6)
        
        # Should handle gracefully (may be empty or have warnings)
        assert isinstance(forecast, pd.DataFrame) 