import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import io
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataUploader:
    """Handles data uploads for multiple companies while maintaining dbt compatibility."""
    
    def __init__(self, company_manager):
        self.company_manager = company_manager
    
    def upload_file(self, uploaded_file, company_id: str, data_type: str) -> Tuple[bool, str]:
        """Upload and validate a data file for a specific company."""
        try:
            # Read file based on type
            df = self._read_file(uploaded_file)
            if df is None:
                return False, "Failed to read file. Please check the file format."
            
            # Validate data
            validation_result = self.validate_data(df, company_id, data_type)
            if not validation_result['valid']:
                return False, f"Validation failed: {validation_result['errors']}"
            
            # Transform data to match dbt schema
            transformed_df = self.company_manager.transform_company_data(df, company_id, data_type)
            
            # Save to database
            success = self.save_data(transformed_df, company_id, data_type)
            
            if success:
                return True, f"Successfully uploaded {len(df)} rows for {company_id}"
            else:
                return False, "Failed to save data to database"
                
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False, f"Upload error: {str(e)}"
    
    def _read_file(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Read uploaded file based on its type."""
        try:
            if uploaded_file.name.endswith('.csv'):
                return pd.read_csv(uploaded_file)
            elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                return pd.read_excel(uploaded_file)
            elif uploaded_file.name.endswith('.json'):
                return pd.read_json(uploaded_file)
            else:
                st.error("Unsupported file format. Please upload CSV, Excel, or JSON files.")
                return None
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            return None
    
    def validate_data(self, df: pd.DataFrame, company_id: str, data_type: str) -> Dict:
        """Validate uploaded data against company schema."""
        errors = []
        
        # Get company-specific schema
        schema_config = self.company_manager.get_company_config(company_id, 'schema')
        if not schema_config or 'mappings' not in schema_config:
            # Use default schema
            schema_config = self.get_default_schema(data_type)
        else:
            schema_config = schema_config['mappings'].get(data_type, {})
        
        # Get required columns from schema mapping
        required_columns = self._get_required_columns(data_type)
        
        # Check required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        # Check data types
        type_errors = self._validate_data_types(df, data_type)
        errors.extend(type_errors)
        
        # Check validation rules
        rule_errors = self._validate_business_rules(df, data_type)
        errors.extend(rule_errors)
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _get_required_columns(self, data_type: str) -> List[str]:
        """Get required columns for different data types."""
        required_columns = {
            'sales': ['date', 'product_line', 'units_sold', 'revenue'],
            'esg': ['date', 'facility', 'emissions_kg_co2', 'energy_consumption_kwh'],
            'supply_chain': ['date', 'supplier', 'order_quantity', 'order_value']
        }
        return required_columns.get(data_type, [])
    
    def _validate_data_types(self, df: pd.DataFrame, data_type: str) -> List[str]:
        """Validate data types for different columns."""
        errors = []
        
        type_validations = {
            'sales': {
                'date': 'datetime',
                'units_sold': 'numeric',
                'revenue': 'numeric'
            },
            'esg': {
                'date': 'datetime',
                'emissions_kg_co2': 'numeric',
                'energy_consumption_kwh': 'numeric'
            },
            'supply_chain': {
                'date': 'datetime',
                'order_quantity': 'numeric',
                'order_value': 'numeric'
            }
        }
        
        validations = type_validations.get(data_type, {})
        
        for col, expected_type in validations.items():
            if col in df.columns:
                if expected_type == 'datetime':
                    try:
                        pd.to_datetime(df[col])
                    except:
                        errors.append(f"Column {col} should be a valid date")
                elif expected_type == 'numeric':
                    if not pd.api.types.is_numeric_dtype(df[col]):
                        errors.append(f"Column {col} should be numeric")
        
        return errors
    
    def _validate_business_rules(self, df: pd.DataFrame, data_type: str) -> List[str]:
        """Validate business rules for different data types."""
        errors = []
        
        # Positive value checks
        positive_columns = {
            'sales': ['units_sold', 'revenue', 'cost_of_goods'],
            'esg': ['emissions_kg_co2', 'energy_consumption_kwh', 'water_usage_liters'],
            'supply_chain': ['order_quantity', 'order_value']
        }
        
        cols_to_check = positive_columns.get(data_type, [])
        for col in cols_to_check:
            if col in df.columns and (df[col] < 0).any():
                errors.append(f"Column {col} contains negative values")
        
        # Date range checks
        if 'date' in df.columns:
            try:
                dates = pd.to_datetime(df['date'])
                min_date = pd.to_datetime('2020-01-01')
                max_date = pd.to_datetime('2030-12-31')
                if (dates < min_date).any() or (dates > max_date).any():
                    errors.append("Dates are outside allowed range (2020-2030)")
            except:
                errors.append("Invalid date format in date column")
        
        # Percentage range checks
        percentage_columns = {
            'esg': ['recycled_material_pct', 'renewable_energy_pct']
        }
        
        cols_to_check = percentage_columns.get(data_type, [])
        for col in cols_to_check:
            if col in df.columns:
                if (df[col] < 0).any() or (df[col] > 100).any():
                    errors.append(f"Column {col} contains values outside 0-100 range")
        
        return errors
    
    def save_data(self, df: pd.DataFrame, company_id: str, data_type: str) -> bool:
        """Save transformed data to database."""
        try:
            # Create staging table name
            staging_table = f"stg_{data_type}_data_{company_id}"
            
            # Save to database using proper DuckDB syntax
            cursor = self.company_manager.connector.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {staging_table} AS SELECT * FROM df")
            
            # Also save to the main staging table for dbt compatibility
            main_staging_table = f"stg_{data_type}_data"
            
            # Check if main table exists, if not create it
            try:
                cursor.execute(f"SELECT 1 FROM {main_staging_table} LIMIT 1")
            except:
                # Table doesn't exist, create it
                cursor.execute(f"CREATE TABLE {main_staging_table} AS SELECT * FROM df")
            else:
                # Table exists, append data
                cursor.execute(f"INSERT INTO {main_staging_table} SELECT * FROM df")
            
            return True
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return False
    
    def get_default_schema(self, data_type: str) -> Dict:
        """Get default schema for different data types."""
        schemas = {
            'sales': {
                'product_column': 'product_line',
                'region_column': 'region',
                'customer_column': 'customer_segment',
                'date_column': 'date',
                'quantity_column': 'units_sold',
                'revenue_column': 'revenue',
                'cost_column': 'cost_of_goods'
            },
            'esg': {
                'product_column': 'product_line',
                'facility_column': 'facility',
                'date_column': 'date',
                'emissions_column': 'emissions_kg_co2',
                'energy_column': 'energy_consumption_kwh',
                'water_column': 'water_usage_liters'
            },
            'supply_chain': {
                'supplier_column': 'supplier',
                'date_column': 'date',
                'quantity_column': 'order_quantity',
                'value_column': 'order_value'
            }
        }
        return schemas.get(data_type, {})
    
    def generate_sample_data(self, company_id: str, data_type: str, 
                           rows: int = 100) -> pd.DataFrame:
        """Generate sample data for a company and data type."""
        if data_type == 'sales':
            return self._generate_sample_sales_data(company_id, rows)
        elif data_type == 'esg':
            return self._generate_sample_esg_data(company_id, rows)
        elif data_type == 'supply_chain':
            return self._generate_sample_supply_chain_data(company_id, rows)
        else:
            return pd.DataFrame()
    
    def _generate_sample_sales_data(self, company_id: str, rows: int) -> pd.DataFrame:
        """Generate sample sales data."""
        # Get company product lines
        product_config = self.company_manager.get_company_config(company_id, 'products')
        product_lines = product_config.get('product_lines', ['Product A', 'Product B']) if product_config else ['Product A', 'Product B']
        
        # Generate sample data
        dates = pd.date_range('2023-01-01', periods=rows, freq='D')
        regions = ['North America', 'Europe', 'Asia Pacific']
        customer_segments = ['Retail', 'Wholesale', 'Food & Beverage']
        
        data = {
            'date': np.random.choice(dates, rows),
            'product_line': np.random.choice(product_lines, rows),
            'region': np.random.choice(regions, rows),
            'customer_segment': np.random.choice(customer_segments, rows),
            'units_sold': np.random.randint(10, 1000, rows),
            'revenue': np.random.uniform(100, 10000, rows),
            'cost_of_goods': np.random.uniform(50, 5000, rows),
            'company_id': company_id
        }
        
        return pd.DataFrame(data)
    
    def _generate_sample_esg_data(self, company_id: str, rows: int) -> pd.DataFrame:
        """Generate sample ESG data."""
        # Get company product lines
        product_config = self.company_manager.get_company_config(company_id, 'products')
        product_lines = product_config.get('product_lines', ['Product A', 'Product B']) if product_config else ['Product A', 'Product B']
        
        # Generate sample data
        dates = pd.date_range('2023-01-01', periods=rows, freq='D')
        facilities = ['Plant A', 'Plant B', 'Plant C']
        
        data = {
            'date': np.random.choice(dates, rows),
            'product_line': np.random.choice(product_lines, rows),
            'facility': np.random.choice(facilities, rows),
            'emissions_kg_co2': np.random.uniform(10, 500, rows),
            'energy_consumption_kwh': np.random.uniform(100, 2000, rows),
            'water_usage_liters': np.random.uniform(50, 1000, rows),
            'recycled_material_pct': np.random.uniform(20, 80, rows),
            'company_id': company_id
        }
        
        return pd.DataFrame(data)
    
    def _generate_sample_supply_chain_data(self, company_id: str, rows: int) -> pd.DataFrame:
        """Generate sample supply chain data."""
        # Generate sample data
        dates = pd.date_range('2023-01-01', periods=rows, freq='D')
        suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D']
        
        data = {
            'date': np.random.choice(dates, rows),
            'supplier': np.random.choice(suppliers, rows),
            'order_quantity': np.random.randint(100, 5000, rows),
            'order_value': np.random.uniform(1000, 50000, rows),
            'on_time_delivery': np.random.choice([True, False], rows, p=[0.8, 0.2]),
            'company_id': company_id
        }
        
        return pd.DataFrame(data) 