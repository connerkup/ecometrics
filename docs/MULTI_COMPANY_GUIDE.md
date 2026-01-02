# Multi-Company & Data Upload Guide

## Overview

The EcoMetrics platform now supports multiple companies with different data schemas, product lines, and business requirements. This guide explains how to set up and use the multi-company functionality while maintaining full compatibility with existing dbt models.

## 🏗️ Architecture

### **Backward Compatibility**

✅ **Fully Compatible with Existing dbt Models**

The multi-company system is designed to work seamlessly with your existing dbt models:

- **No dbt changes required** - All existing models continue to work
- **PackagingCo data preserved** - Original data remains accessible
- **Gradual migration** - Add companies incrementally
- **Schema flexibility** - Companies can use different column names

### **Database Structure**

```
Database Tables:
├── companies                    # Company metadata
├── company_configs             # Company-specific configurations
├── company_data_sources        # Data source mappings
├── fact_esg_monthly           # Existing dbt models (unchanged)
├── fact_financial_monthly     # Existing dbt models (unchanged)
├── stg_sales_data             # Existing dbt models (unchanged)
└── stg_esg_data               # Existing dbt models (unchanged)
```

### **Company Isolation**

Each company's data is isolated through:
- **company_id column** - Added to all data tables
- **Schema mapping** - Maps company-specific columns to standard schema
- **Configuration management** - Company-specific settings and product lines

## 🚀 Getting Started

### **1. Access Company Management**

Navigate to the **Company Management** page in the sidebar:
- Go to `🏢 Company Management` page
- This is where you'll manage all companies

### **2. Add Your First Company**

1. **Click "➕ Add New Company"**
2. **Fill in company details:**
   - **Company ID**: Unique identifier (e.g., `manufacturing_co`, `retail_chain`)
   - **Company Name**: Display name
   - **Industry**: Select from predefined options
   - **Description**: Optional company description

3. **Configure advanced settings:**
   - **Demo Mode**: For testing purposes
   - **Auto-generate Sample Data**: Creates sample data automatically

4. **Click "Add Company"**

### **3. Upload Your Data**

#### **Supported File Formats**
- **CSV** - Comma-separated values
- **Excel** - .xlsx and .xls files
- **JSON** - JavaScript Object Notation

#### **Data Types**
- **Sales Data** - Revenue, units sold, customer information
- **ESG Data** - Emissions, energy consumption, sustainability metrics
- **Supply Chain Data** - Supplier performance, inventory, logistics

#### **Upload Process**
1. Select your company
2. Choose data type
3. Upload file
4. System validates and transforms data
5. Data is saved to database

## 🔄 Schema Mapping

### **How It Works**

Companies can use different column names while maintaining compatibility:

```python
# Example: Manufacturing company uses different column names
Manufacturing Company Data:
├── product_category    → maps to → product_line
├── location           → maps to → region
├── client_type        → maps to → customer_segment
└── sales_amount       → maps to → revenue

# System automatically transforms to standard schema
Standard Schema:
├── product_line
├── region
├── customer_segment
└── revenue
```

### **Default Mappings by Industry**

#### **Packaging (Default)**
```python
{
    "product_line": "product_line",
    "region": "region",
    "customer_segment": "customer_segment",
    "date": "date",
    "units_sold": "units_sold",
    "revenue": "revenue"
}
```

#### **Manufacturing**
```python
{
    "product_category": "product_line",
    "location": "region",
    "client_type": "customer_segment",
    "date": "date",
    "units_sold": "units_sold",
    "revenue": "revenue"
}
```

#### **Retail**
```python
{
    "category": "product_line",
    "store_location": "region",
    "customer_type": "customer_segment",
    "date": "date",
    "units_sold": "units_sold",
    "revenue": "revenue"
}
```

## 📊 Data Validation

### **Automatic Validation**

The system validates uploaded data against:

#### **Required Columns**
- **Sales**: `date`, `product_line`, `units_sold`, `revenue`
- **ESG**: `date`, `facility`, `emissions_kg_co2`, `energy_consumption_kwh`
- **Supply Chain**: `date`, `supplier`, `order_quantity`, `order_value`

#### **Data Type Validation**
- **Dates**: Must be valid date format
- **Numeric**: Must be numeric values
- **Percentages**: Must be 0-100 range
- **Positive Values**: No negative numbers where inappropriate

#### **Business Rule Validation**
- **Date Range**: 2020-2030 (configurable)
- **Material Percentages**: Recycled + Virgin = 100%
- **Revenue Consistency**: Revenue = Price × Quantity

### **Validation Errors**

If validation fails, you'll see specific error messages:
```
❌ Validation failed:
- Missing required columns: ['date', 'revenue']
- Column 'units_sold' contains negative values
- Dates are outside allowed range (2020-2030)
```

## 🎯 Company Configuration

### **Product Lines**

Configure product lines specific to your company:

1. Go to **Configuration** tab
2. Select your company
3. Edit **Product Lines** section
4. Enter one product line per line
5. Save configuration

**Example Product Lines:**
```
Electronics
Automotive
Machinery
Textiles
```

### **Metrics Configuration**

Define which metrics are most important for your company:

- **Financial**: Revenue, profit margin, cost of goods
- **ESG**: Emissions, energy consumption, recycled content
- **Operational**: Units produced, efficiency, quality score

### **Schema Mappings**

Customize column mappings if needed:

1. Go to **Configuration** tab
2. Expand **Schema Mapping** section
3. View current mappings
4. Contact support for custom mappings

## 📈 Using the Dashboard

### **Company Selection**

1. **In the sidebar**, you'll see **Company Selection**
2. **Choose your company** from the dropdown
3. **Dashboard updates** to show your company's data
4. **Filters work** with your company's product lines

### **Data Sources**

The dashboard automatically loads data from:
1. **Company-specific tables** (if available)
2. **Main dbt tables** (fallback for backward compatibility)
3. **Mixed data** (if company has partial data)

### **Cross-Company Analysis**

For comparing multiple companies:
1. Use the **Data Browser** page
2. Select different companies
3. Export data for external analysis

## 🔧 Advanced Features

### **Sample Data Generation**

Generate realistic sample data for testing:

1. Go to **Data Upload** tab
2. Select your company and data type
3. Click **"Generate Sample Data"**
4. Choose number of rows
5. Data is automatically created and saved

### **Data Quality Monitoring**

Monitor data quality across companies:

1. Go to **Data Browser** page
2. View data quality metrics
3. Check for missing values, duplicates
4. Validate business rules

### **Bulk Operations**

For multiple companies:

1. **CSV Import**: Prepare company list in CSV format
2. **API Access**: Use programmatic access for bulk operations
3. **Data Migration**: Tools for migrating existing data

## 🛠️ Technical Implementation

### **Database Schema**

```sql
-- Company management tables
CREATE TABLE companies (
    company_id VARCHAR PRIMARY KEY,
    company_name VARCHAR NOT NULL,
    industry VARCHAR,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    settings JSON,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE company_configs (
    company_id VARCHAR,
    config_type VARCHAR,
    config_data JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (company_id, config_type)
);
```

### **Data Flow**

```
1. User Uploads Data
   ↓
2. Validation & Transformation
   ↓
3. Schema Mapping Applied
   ↓
4. Data Saved to Database
   ↓
5. dbt Models Process Data
   ↓
6. Dashboard Displays Results
```

### **API Endpoints**

For programmatic access:

```python
# Create company
POST /api/companies
{
    "company_id": "manufacturing_co",
    "company_name": "Manufacturing Co",
    "industry": "Manufacturing"
}

# Upload data
POST /api/companies/{company_id}/data
{
    "data_type": "sales",
    "file": "data.csv"
}

# Get company data
GET /api/companies/{company_id}/data/{data_type}
```

## 🚨 Troubleshooting

### **Common Issues**

#### **"Company not found"**
- Check company ID spelling
- Verify company exists in database
- Use Company Management page to view all companies

#### **"Validation failed"**
- Check required columns are present
- Verify data types are correct
- Ensure business rules are followed
- Review error messages for specific issues

#### **"No data displayed"**
- Verify data was uploaded successfully
- Check company selection in sidebar
- Use Data Browser to verify data exists
- Generate sample data for testing

#### **"Schema mapping errors"**
- Contact support for custom mappings
- Use default column names if possible
- Check column name spelling and case

### **Debug Mode**

Enable debug mode for troubleshooting:

1. Go to **Company Management**
2. Select your company
3. Enable **Demo Mode**
4. Check logs for detailed error messages

## 📋 Best Practices

### **Data Preparation**

1. **Use consistent date formats** (YYYY-MM-DD)
2. **Include all required columns**
3. **Validate data before upload**
4. **Use descriptive column names**
5. **Keep file sizes reasonable** (< 100MB)

### **Company Setup**

1. **Use descriptive company IDs**
2. **Configure product lines accurately**
3. **Set up schema mappings early**
4. **Test with sample data first**
5. **Document company-specific requirements**

### **Data Management**

1. **Regular data uploads**
2. **Monitor data quality**
3. **Backup important data**
4. **Version control configurations**
5. **Document data sources**

## 🔮 Future Enhancements

### **Planned Features**

- **Multi-tenant database** support
- **Advanced analytics** per company
- **Custom dashboards** per company
- **Data lineage** tracking
- **Advanced security** features
- **API rate limiting**
- **Bulk data operations**
- **Data versioning**

### **Integration Options**

- **ERP systems** (SAP, Oracle, etc.)
- **CRM systems** (Salesforce, HubSpot)
- **Accounting software** (QuickBooks, Xero)
- **IoT devices** for real-time data
- **External APIs** for market data

## 📞 Support

### **Getting Help**

1. **Check this documentation** first
2. **Use the test script**: `python test_multi_company.py`
3. **Review error messages** carefully
4. **Contact support** with specific error details

### **Contact Information**

- **Documentation**: Check this guide and other docs
- **Issues**: Use GitHub issues for bugs
- **Questions**: Use GitHub discussions
- **Custom Development**: Contact for enterprise features

---

**🎉 Congratulations!** You now have a robust multi-company system that maintains full compatibility with your existing dbt models while providing flexibility for different business requirements. 