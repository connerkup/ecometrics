import streamlit as st
import pandas as pd
from company_manager import CompanyManager
from data_uploader import DataUploader
from data_connector import get_data_connector, get_company_summary_stats
import json

st.set_page_config(
    page_title="Company Management - EcoMetrics",
    page_icon="🏢",
    layout="wide"
)

st.title("🏢 Company Management")
st.markdown("---")

# Initialize
connector = get_data_connector()
company_manager = CompanyManager(connector)
data_uploader = DataUploader(company_manager)

# Tabs for different management functions
tab1, tab2, tab3, tab4 = st.tabs(["Companies", "Data Upload", "Configuration", "Analytics"])

with tab1:
    st.subheader("Manage Companies")
    
    # Add new company
    with st.expander("➕ Add New Company", expanded=False):
        with st.form("add_company"):
            col1, col2 = st.columns(2)
            
            with col1:
                company_id = st.text_input("Company ID (unique identifier)", 
                                         help="Use lowercase, no spaces (e.g., 'packagingco', 'manufacturing_co')")
                company_name = st.text_input("Company Name")
                industry = st.selectbox("Industry", [
                    "Packaging", "Manufacturing", "Retail", "Food & Beverage",
                    "Pharmaceutical", "Technology", "Other"
                ])
            
            with col2:
                description = st.text_area("Description", height=100)
                
                # Advanced settings
                with st.expander("Advanced Settings"):
                    demo_mode = st.checkbox("Demo Mode", value=False, 
                                          help="Enable demo mode for testing")
                    auto_generate_data = st.checkbox("Auto-generate Sample Data", value=True,
                                                   help="Generate sample data for new company")
            
            if st.form_submit_button("➕ Add Company"):
                if company_id and company_name:
                    # Validate company ID format
                    if not company_id.replace('_', '').replace('-', '').isalnum():
                        st.error("Company ID must contain only letters, numbers, underscores, and hyphens")
                    else:
                        settings = {
                            "demo": demo_mode,
                            "auto_generate_data": auto_generate_data,
                            "created_by": "user"
                        }
                        
                        success = company_manager.create_company(
                            company_id, company_name, industry, description, settings
                        )
                        
                        if success:
                            st.success(f"✅ Company '{company_name}' added successfully!")
                            
                            # Auto-generate sample data if requested
                            if auto_generate_data:
                                with st.spinner("Generating sample data..."):
                                    for data_type in ['sales', 'esg', 'supply_chain']:
                                        sample_data = data_uploader.generate_sample_data(
                                            company_id, data_type, rows=50
                                        )
                                        if not sample_data.empty:
                                            data_uploader.save_data(sample_data, company_id, data_type)
                            
                            st.rerun()
                        else:
                            st.error("❌ Failed to create company. Please check the company ID.")
                else:
                    st.error("Company ID and Name are required")
    
    # List companies
    companies = company_manager.get_companies()
    if companies:
        st.subheader("📋 Active Companies")
        
        # Create a summary table
        summary_data = []
        for company in companies:
            stats = get_company_summary_stats(company['company_id'])
            summary_data.append({
                'Company': f"{company['company_name']} ({company['company_id']})",
                'Industry': company.get('industry', 'N/A'),
                'Total Records': stats.get('total_records', 0),
                'ESG Records': stats.get('esg_records', 0),
                'Finance Records': stats.get('finance_records', 0),
                'Supply Chain Records': stats.get('supply_chain_records', 0),
                'Created': company['created_at'][:10] if company['created_at'] else 'N/A'
            })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            st.dataframe(summary_df, use_container_width=True)
        
        # Detailed company view
        st.subheader("🔍 Company Details")
        for company in companies:
            with st.expander(f"🏢 {company['company_name']} ({company['company_id']})"):
                col1, col2, col3 = st.columns([2, 2, 1])
                
                with col1:
                    st.write(f"**Industry:** {company.get('industry', 'N/A')}")
                    st.write(f"**Created:** {company['created_at']}")
                    if company.get('description'):
                        st.write(f"**Description:** {company['description']}")
                
                with col2:
                    # Show company stats
                    stats = get_company_summary_stats(company['company_id'])
                    if 'error' not in stats:
                        st.write("**Data Summary:**")
                        st.write(f"• Total Records: {stats.get('total_records', 0):,}")
                        st.write(f"• ESG: {stats.get('esg_records', 0):,}")
                        st.write(f"• Finance: {stats.get('finance_records', 0):,}")
                        st.write(f"• Supply Chain: {stats.get('supply_chain_records', 0):,}")
                    else:
                        st.write("**Data:** No data available")
                
                with col3:
                    if st.button(f"🗑️ Delete", key=f"del_{company['company_id']}"):
                        st.warning("Delete functionality to be implemented")
                    
                    if st.button(f"📊 View Data", key=f"view_{company['company_id']}"):
                        st.session_state['selected_company'] = company['company_id']
                        st.rerun()
    else:
        st.info("📝 No companies configured yet. Add your first company above!")

with tab2:
    st.subheader("📤 Data Upload")
    
    if companies:
        # Company selection
        selected_company = st.selectbox(
            "Select Company",
            options=companies,
            format_func=lambda x: f"{x['company_name']} ({x['company_id']})",
            key="upload_company_select"
        )
        
        if selected_company:
            company_id = selected_company['company_id']
            
            # Data type selection
            data_type = st.selectbox(
                "Data Type",
                ["sales", "esg", "supply_chain"],
                format_func=lambda x: x.replace('_', ' ').title()
            )
            
            # Upload section
            st.markdown("### Upload Data File")
            
            uploaded_file = st.file_uploader(
                f"Upload {data_type.replace('_', ' ')} data for {selected_company['company_name']}",
                type=['csv', 'xlsx', 'json'],
                help=f"Upload {data_type} data. Supported formats: CSV, Excel, JSON"
            )
            
            if uploaded_file:
                # Show file info
                file_info = {
                    "Name": uploaded_file.name,
                    "Size": f"{uploaded_file.size / 1024:.1f} KB",
                    "Type": uploaded_file.type
                }
                st.json(file_info)
                
                # Upload button
                if st.button("📤 Upload Data"):
                    with st.spinner("Uploading and validating data..."):
                        success, message = data_uploader.upload_file(
                            uploaded_file, company_id, data_type
                        )
                        
                        if success:
                            st.success(f"✅ {message}")
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
            
            # Sample data generation
            st.markdown("### Generate Sample Data")
            col1, col2 = st.columns(2)
            
            with col1:
                sample_rows = st.number_input("Number of rows", min_value=10, max_value=1000, value=100)
            
            with col2:
                if st.button("🎲 Generate Sample Data"):
                    with st.spinner("Generating sample data..."):
                        sample_data = data_uploader.generate_sample_data(
                            company_id, data_type, sample_rows
                        )
                        
                        if not sample_data.empty:
                            success = data_uploader.save_data(sample_data, company_id, data_type)
                            if success:
                                st.success(f"✅ Generated {len(sample_data)} sample records")
                                st.rerun()
                            else:
                                st.error("❌ Failed to save sample data")
                        else:
                            st.error("❌ Failed to generate sample data")
            
            # Show existing data
            st.markdown("### Existing Data")
            company_tables = company_manager.get_company_data_tables(company_id)
            data_tables = [t for t in company_tables if data_type in t]
            
            if data_tables:
                for table in data_tables:
                    with st.expander(f"📊 {table}"):
                        try:
                            table_info = connector.get_table_info(table)
                            st.write(f"**Rows:** {table_info['row_count']:,}")
                            st.write(f"**Columns:** {len(table_info['columns'])}")
                            
                            if st.button(f"View Sample", key=f"sample_{table}"):
                                sample_data = connector.query(f"SELECT * FROM {table} LIMIT 10")
                                st.dataframe(sample_data, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error loading table info: {e}")
            else:
                st.info(f"No {data_type} data found for this company yet.")
    else:
        st.info("📝 Please add a company first to upload data.")

with tab3:
    st.subheader("⚙️ Company Configuration")
    
    if companies:
        selected_company = st.selectbox(
            "Select Company to Configure",
            options=companies,
            format_func=lambda x: f"{x['company_name']} ({x['company_id']})",
            key="config_company_select"
        )
        
        if selected_company:
            company_id = selected_company['company_id']
            
            # Product configuration
            st.markdown("### 🏷️ Product Configuration")
            product_config = company_manager.get_company_config(company_id, 'products') or {}
            
            with st.form("product_config"):
                st.write("Configure product lines for this company:")
                
                # Dynamic product configuration
                products_text = st.text_area(
                    "Product Lines (one per line)",
                    value="\n".join(product_config.get('product_lines', [])),
                    help="Enter product lines, one per line. These will be used for data validation and filtering."
                )
                
                if st.form_submit_button("💾 Save Product Configuration"):
                    product_lines = [p.strip() for p in products_text.split('\n') if p.strip()]
                    config_data = {'product_lines': product_lines}
                    
                    success = company_manager.update_company_config(
                        company_id, 'products', config_data
                    )
                    
                    if success:
                        st.success("✅ Product configuration saved!")
                    else:
                        st.error("❌ Failed to save configuration")
            
            # Schema mapping configuration
            st.markdown("### 🔄 Schema Mapping")
            schema_config = company_manager.get_company_config(company_id, 'schema') or {}
            mappings = schema_config.get('mappings', {})
            
            with st.expander("Configure Schema Mappings"):
                st.write("""
                Schema mappings allow companies to use different column names while still working with the system.
                For example, a manufacturing company might use 'product_category' instead of 'product_line'.
                """)
                
                for data_type in ['sales', 'esg', 'supply_chain']:
                    st.markdown(f"**{data_type.replace('_', ' ').title()} Mappings:**")
                    
                    type_mappings = mappings.get(data_type, {})
                    
                    # Show current mappings
                    for standard_col, company_col in type_mappings.items():
                        st.write(f"• {standard_col} → {company_col}")
                    
                    if not type_mappings:
                        st.write("Using default column names")
            
            # Metrics configuration
            st.markdown("### 📊 Metrics Configuration")
            metrics_config = company_manager.get_company_config(company_id, 'metrics') or {}
            
            with st.expander("Configure Key Metrics"):
                st.write("Configure which metrics are most important for this company:")
                
                for metric_type, metrics in metrics_config.items():
                    st.write(f"**{metric_type.title()}:**")
                    for metric in metrics:
                        st.write(f"• {metric}")
    else:
        st.info("📝 Please add a company first to configure settings.")

with tab4:
    st.subheader("📈 Company Analytics")
    
    if companies:
        selected_company = st.selectbox(
            "Select Company for Analytics",
            options=companies,
            format_func=lambda x: f"{x['company_name']} ({x['company_id']})",
            key="analytics_company_select"
        )
        
        if selected_company:
            company_id = selected_company['company_id']
            
            # Get company stats
            stats = get_company_summary_stats(company_id)
            
            if 'error' not in stats:
                # Display key metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Records", f"{stats['total_records']:,}")
                
                with col2:
                    st.metric("ESG Records", f"{stats['esg_records']:,}")
                
                with col3:
                    st.metric("Finance Records", f"{stats['finance_records']:,}")
                
                with col4:
                    st.metric("Supply Chain Records", f"{stats['supply_chain_records']:,}")
                
                # Date ranges
                if stats['date_range']:
                    st.markdown("### 📅 Data Date Ranges")
                    date_df = pd.DataFrame([
                        {
                            'Data Type': data_type.replace('_', ' ').title(),
                            'Start Date': range_info['start'],
                            'End Date': range_info['end'],
                            'Days': range_info['days']
                        }
                        for data_type, range_info in stats['date_range'].items()
                    ])
                    st.dataframe(date_df, use_container_width=True)
                
                # Key metrics
                if stats['key_metrics']:
                    st.markdown("### 💰 Key Financial Metrics")
                    for metric, value in stats['key_metrics'].items():
                        if 'revenue' in metric.lower():
                            st.metric(metric.replace('_', ' ').title(), f"${value:,.0f}")
                        elif 'emissions' in metric.lower():
                            st.metric(metric.replace('_', ' ').title(), f"{value:,.0f} kg CO2")
                        else:
                            st.metric(metric.replace('_', ' ').title(), f"{value:,.0f}")
            else:
                st.warning("No data available for this company yet.")
    else:
        st.info("📝 Please add a company first to view analytics.")

# Footer
st.markdown("---")
st.markdown("""
**💡 Tips:**
- Company IDs should be unique and contain only letters, numbers, underscores, and hyphens
- Use the sample data generation feature to quickly populate your company with test data
- Configure product lines to match your company's actual product categories
- Schema mappings allow you to use your existing column names while maintaining system compatibility
""") 