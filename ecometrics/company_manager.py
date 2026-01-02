import streamlit as st
import pandas as pd
import duckdb
from typing import Dict, List, Optional, Any
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CompanyManager:
    """Manages multi-company support while maintaining dbt compatibility."""
    
    def __init__(self, db_connector):
        self.connector = db_connector
        self.initialize_company_tables()
    
    def initialize_company_tables(self):
        """Create company management tables if they don't exist."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS companies (
                company_id VARCHAR PRIMARY KEY,
                company_name VARCHAR NOT NULL,
                industry VARCHAR,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                settings JSON,
                is_active BOOLEAN DEFAULT TRUE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS company_configs (
                company_id VARCHAR,
                config_type VARCHAR, -- 'products', 'metrics', 'schema', 'mappings'
                config_data JSON,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (company_id, config_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS company_data_sources (
                company_id VARCHAR,
                data_type VARCHAR, -- 'sales', 'esg', 'supply_chain'
                source_name VARCHAR,
                table_name VARCHAR,
                schema_mapping JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (company_id, data_type)
            )
            """
        ]
        
        for query in queries:
            try:
                self.connector.query(query)
            except Exception as e:
                logger.warning(f"Error creating table: {e}")
    
    def create_company(self, company_id: str, company_name: str, 
                      industry: str = None, description: str = None,
                      settings: Dict = None) -> bool:
        """Create a new company with default configuration."""
        try:
            # Create company record
            query = """
            INSERT INTO companies (company_id, company_name, industry, description, settings)
            VALUES (?, ?, ?, ?, ?)
            """
            self.connector.query(query, params=[company_id, company_name, industry or "", 
                                               description or "", json.dumps(settings or {})])
            
            # Initialize default configurations
            self._initialize_default_configs(company_id, industry)
            
            return True
        except Exception as e:
            logger.error(f"Error creating company: {e}")
            return False
    
    def _initialize_default_configs(self, company_id: str, industry: str = None):
        """Initialize default configurations for a new company."""
        # Default product lines based on industry
        default_products = self._get_default_products(industry)
        
        # Default schema mappings (maps company-specific columns to standard dbt schema)
        default_mappings = self._get_default_schema_mappings(industry)
        
        # Save configurations
        configs = [
            ('products', {'product_lines': default_products}),
            ('schema', {'mappings': default_mappings}),
            ('metrics', self._get_default_metrics(industry))
        ]
        
        for config_type, config_data in configs:
            self.update_company_config(company_id, config_type, config_data)
    
    def _get_default_products(self, industry: str = None) -> List[str]:
        """Get default product lines based on industry."""
        industry_products = {
            "Packaging": ["Plastic Containers", "Paper Packaging", "Glass Bottles", 
                         "Aluminum Cans", "Biodegradable Packaging"],
            "Manufacturing": ["Electronics", "Automotive", "Machinery", "Textiles"],
            "Retail": ["Apparel", "Electronics", "Home Goods", "Food"],
            "Food & Beverage": ["Beverages", "Snacks", "Dairy", "Frozen Foods"],
            "Pharmaceutical": ["Prescription Drugs", "Over-the-Counter", "Medical Devices"],
            "Technology": ["Software", "Hardware", "Services", "Cloud Solutions"]
        }
        
        return industry_products.get(industry, ["Product A", "Product B", "Product C"])
    
    def _get_default_schema_mappings(self, industry: str = None) -> Dict:
        """Get default schema mappings for different industries."""
        # These mappings allow companies to use different column names
        # while still working with the existing dbt models
        base_mappings = {
            "sales": {
                "product_column": "product_line",
                "region_column": "region", 
                "customer_column": "customer_segment",
                "date_column": "date",
                "quantity_column": "units_sold",
                "revenue_column": "revenue",
                "cost_column": "cost_of_goods"
            },
            "esg": {
                "product_column": "product_line",
                "facility_column": "facility",
                "date_column": "date",
                "emissions_column": "emissions_kg_co2",
                "energy_column": "energy_consumption_kwh",
                "water_column": "water_usage_liters"
            }
        }
        
        # Industry-specific overrides
        industry_mappings = {
            "Manufacturing": {
                "sales": {
                    "product_column": "product_category",
                    "region_column": "location",
                    "customer_column": "client_type"
                }
            },
            "Retail": {
                "sales": {
                    "product_column": "category",
                    "region_column": "store_location",
                    "customer_column": "customer_type"
                }
            }
        }
        
        # Merge base mappings with industry-specific ones
        mappings = base_mappings.copy()
        if industry in industry_mappings:
            for data_type, type_mappings in industry_mappings[industry].items():
                if data_type in mappings:
                    mappings[data_type].update(type_mappings)
        
        return mappings
    
    def _get_default_metrics(self, industry: str = None) -> Dict:
        """Get default metrics configuration based on industry."""
        base_metrics = {
            "financial": ["revenue", "profit_margin", "cost_of_goods"],
            "esg": ["emissions", "energy_consumption", "recycled_content"],
            "operational": ["units_produced", "efficiency", "quality_score"]
        }
        
        # Industry-specific metrics
        industry_metrics = {
            "Manufacturing": {
                "financial": ["revenue", "profit_margin", "production_cost"],
                "esg": ["emissions", "energy_consumption", "waste_generation"],
                "operational": ["units_produced", "defect_rate", "efficiency"]
            },
            "Retail": {
                "financial": ["sales", "profit_margin", "inventory_cost"],
                "esg": ["emissions", "energy_consumption", "packaging_waste"],
                "operational": ["units_sold", "inventory_turnover", "customer_satisfaction"]
            }
        }
        
        return industry_metrics.get(industry, base_metrics)
    
    def get_companies(self) -> List[Dict]:
        """Get all active companies."""
        try:
            query = "SELECT * FROM companies WHERE is_active = TRUE ORDER BY company_name"
            result = self.connector.query(query)
            return result.to_dict('records')
        except Exception as e:
            logger.error(f"Error getting companies: {e}")
            return []
    
    def get_company_config(self, company_id: str, config_type: str) -> Optional[Dict]:
        """Get company-specific configuration."""
        try:
            query = """
            SELECT config_data FROM company_configs 
            WHERE company_id = ? AND config_type = ?
            """
            result = self.connector.query(query, params=[company_id, config_type])
            if not result.empty:
                return json.loads(result.iloc[0]['config_data'])
            return None
        except Exception as e:
            logger.error(f"Error getting company config: {e}")
            return None
    
    def update_company_config(self, company_id: str, config_type: str, 
                            config_data: Dict) -> bool:
        """Update company configuration."""
        try:
            query = """
            INSERT OR REPLACE INTO company_configs (company_id, config_type, config_data)
            VALUES (?, ?, ?)
            """
            self.connector.query(query, params=[company_id, config_type, 
                                               json.dumps(config_data)])
            return True
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            return False
    
    def get_company_schema_mapping(self, company_id: str, data_type: str) -> Dict:
        """Get schema mapping for a specific company and data type."""
        schema_config = self.get_company_config(company_id, 'schema')
        if schema_config and 'mappings' in schema_config:
            return schema_config['mappings'].get(data_type, {})
        return {}
    
    def transform_company_data(self, df: pd.DataFrame, company_id: str, 
                             data_type: str) -> pd.DataFrame:
        """Transform company-specific data to match dbt schema."""
        mapping = self.get_company_schema_mapping(company_id, data_type)
        
        if not mapping:
            # No mapping needed, return as-is
            return df
        
        # Apply column mappings
        column_mapping = {}
        for standard_col, company_col in mapping.items():
            if company_col in df.columns and standard_col != company_col:
                column_mapping[company_col] = standard_col
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        # Add company_id if not present
        if 'company_id' not in df.columns:
            df['company_id'] = company_id
        
        return df
    
    def get_company_data_tables(self, company_id: str) -> List[str]:
        """Get all data tables for a specific company."""
        try:
            # Get all tables
            all_tables = self.connector.get_available_tables()
            
            # Filter for company-specific tables
            company_tables = []
            for table in all_tables:
                # Check if table contains data for this company
                try:
                    query = f"SELECT COUNT(*) as count FROM {table} WHERE company_id = ?"
                    result = self.connector.query(query, params=[company_id])
                    if result.iloc[0]['count'] > 0:
                        company_tables.append(table)
                except:
                    # Table might not have company_id column (legacy tables)
                    # or might be a different type of table
                    pass
            
            return company_tables
        except Exception as e:
            logger.error(f"Error getting company tables: {e}")
            return []
    
    def create_sample_company(self, company_id: str = "sample_manufacturing") -> bool:
        """Create a sample manufacturing company for demonstration."""
        return self.create_company(
            company_id=company_id,
            company_name="Sample Manufacturing Co",
            industry="Manufacturing",
            description="A sample manufacturing company for demonstration purposes",
            settings={
                "demo": True,
                "created_by": "system"
            }
        ) 