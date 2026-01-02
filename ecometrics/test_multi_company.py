#!/usr/bin/env python3
"""
Test script for multi-company functionality.
This script tests the company management and data upload features.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from company_manager import CompanyManager
from data_uploader import DataUploader
from data_connector import get_data_connector, get_company_summary_stats
import pandas as pd

def test_company_management():
    """Test company management functionality."""
    print("🧪 Testing Company Management...")
    
    # Initialize
    connector = get_data_connector()
    company_manager = CompanyManager(connector)
    
    # Test 1: Create a test company
    print("  📝 Creating test company...")
    success = company_manager.create_company(
        company_id="test_manufacturing",
        company_name="Test Manufacturing Co",
        industry="Manufacturing",
        description="A test manufacturing company for validation"
    )
    
    if success:
        print("  ✅ Test company created successfully")
    else:
        print("  ❌ Failed to create test company")
        return False
    
    # Test 2: Get companies
    print("  📋 Getting companies...")
    companies = company_manager.get_companies()
    if companies:
        print(f"  ✅ Found {len(companies)} companies")
        for company in companies:
            print(f"    - {company['company_name']} ({company['company_id']})")
    else:
        print("  ❌ No companies found")
        return False
    
    # Test 3: Get company configuration
    print("  ⚙️ Testing company configuration...")
    product_config = company_manager.get_company_config("test_manufacturing", "products")
    if product_config and 'product_lines' in product_config:
        print(f"  ✅ Product config found: {product_config['product_lines']}")
    else:
        print("  ❌ Product config not found")
        return False
    
    return True

def test_data_upload():
    """Test data upload functionality."""
    print("\n🧪 Testing Data Upload...")
    
    # Initialize
    connector = get_data_connector()
    company_manager = CompanyManager(connector)
    data_uploader = DataUploader(company_manager)
    
    # Test 1: Generate sample data
    print("  🎲 Generating sample data...")
    sample_data = data_uploader.generate_sample_data("test_manufacturing", "sales", 50)
    
    if not sample_data.empty:
        print(f"  ✅ Generated {len(sample_data)} sample records")
        print(f"    Columns: {list(sample_data.columns)}")
    else:
        print("  ❌ Failed to generate sample data")
        return False
    
    # Test 2: Save data
    print("  💾 Saving sample data...")
    success = data_uploader.save_data(sample_data, "test_manufacturing", "sales")
    
    if success:
        print("  ✅ Sample data saved successfully")
    else:
        print("  ❌ Failed to save sample data")
        return False
    
    return True

def test_data_loading():
    """Test company-specific data loading."""
    print("\n🧪 Testing Data Loading...")
    
    # Test 1: Get company summary stats
    print("  📊 Getting company summary stats...")
    stats = get_company_summary_stats("test_manufacturing")
    
    if 'error' not in stats:
        print(f"  ✅ Company stats: {stats}")
    else:
        print(f"  ❌ Error getting stats: {stats['error']}")
        return False
    
    # Test 2: Load company data
    print("  📈 Loading company data...")
    from data_connector import load_company_finance_data
    finance_data, status = load_company_finance_data("test_manufacturing")
    
    if not finance_data.empty:
        print(f"  ✅ Loaded {len(finance_data)} finance records")
    else:
        print(f"  ⚠️ No finance data found: {status}")
    
    return True

def test_schema_mapping():
    """Test schema mapping functionality."""
    print("\n🧪 Testing Schema Mapping...")
    
    # Initialize
    connector = get_data_connector()
    company_manager = CompanyManager(connector)
    
    # Test 1: Get schema mapping
    print("  🔄 Testing schema mapping...")
    mapping = company_manager.get_company_schema_mapping("test_manufacturing", "sales")
    
    if mapping:
        print(f"  ✅ Schema mapping found: {mapping}")
    else:
        print("  ℹ️ No custom schema mapping (using defaults)")
    
    # Test 2: Transform data
    print("  🔄 Testing data transformation...")
    test_df = pd.DataFrame({
        'product_category': ['Electronics', 'Automotive'],
        'location': ['North America', 'Europe'],
        'client_type': ['Wholesale', 'Retail'],
        'date': ['2023-01-01', '2023-01-02'],
        'units_sold': [100, 200],
        'revenue': [1000, 2000]
    })
    
    transformed_df = company_manager.transform_company_data(test_df, "test_manufacturing", "sales")
    
    if 'company_id' in transformed_df.columns:
        print("  ✅ Data transformation successful")
        print(f"    Original columns: {list(test_df.columns)}")
        print(f"    Transformed columns: {list(transformed_df.columns)}")
    else:
        print("  ❌ Data transformation failed")
        return False
    
    return True

def cleanup():
    """Clean up test data."""
    print("\n🧹 Cleaning up test data...")
    
    # Note: In a real implementation, you would add cleanup functionality
    # For now, we'll just note that cleanup is needed
    print("  ℹ️ Test data cleanup would be implemented here")
    print("  ℹ️ Test company 'test_manufacturing' remains in database")

def main():
    """Run all tests."""
    print("🚀 Starting Multi-Company Functionality Tests")
    print("=" * 50)
    
    tests = [
        ("Company Management", test_company_management),
        ("Data Upload", test_data_upload),
        ("Data Loading", test_data_loading),
        ("Schema Mapping", test_schema_mapping)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} test PASSED")
            else:
                print(f"❌ {test_name} test FAILED")
        except Exception as e:
            print(f"❌ {test_name} test ERROR: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Multi-company functionality is working correctly.")
    else:
        print("⚠️ Some tests failed. Please check the implementation.")
    
    cleanup()
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 