import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from streamlit import navigation
from data_connector import (
    check_dbt_availability, load_esg_data, load_finance_data, load_supply_chain_data,
    load_company_esg_data, load_company_finance_data, load_company_supply_chain_data
)
from company_manager import CompanyManager
import numpy as np
from datetime import datetime, timedelta
from color_config import (
    CSS_COLORS, get_comparison_colors, get_financial_color, 
    get_sustainability_color, get_monochrome_colors
)

# Configure the page
st.set_page_config(
    page_title="EcoMetrics - Business Intelligence Portfolio",
    page_icon="🌱",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling with standardized monochrome pastel colors
st.markdown(f"""
<style>
    .main-header {{
        font-size: 3rem;
        font-weight: bold;
        color: {CSS_COLORS['primary']};
        text-align: center;
        margin-bottom: 1rem;
    }}
    .sub-header {{
        font-size: 1.2rem;
        color: {CSS_COLORS['neutral-dark']};
        text-align: center;
        margin-bottom: 2rem;
    }}
    .metric-card {{
        background: linear-gradient(135deg, {CSS_COLORS['primary-light']}20, {CSS_COLORS['secondary-light']}20);
        padding: 1rem;
        border-radius: 10px;
        border: 1px solid {CSS_COLORS['primary']};
        margin-bottom: 1rem;
        text-align: center;
    }}
    .status-card {{
        background-color: {CSS_COLORS['primary-light']}25;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid {CSS_COLORS['primary']};
        margin-bottom: 1rem;
    }}
    .insight-card {{
        background: linear-gradient(135deg, {CSS_COLORS['primary-light']}10, {CSS_COLORS['secondary-light']}10);
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid {CSS_COLORS['primary']};
        margin-bottom: 1rem;
    }}
</style>
""", unsafe_allow_html=True)

# Check dbt availability
availability = check_dbt_availability()

# Initialize company manager
from data_connector import get_data_connector
connector = get_data_connector()
company_manager = CompanyManager(connector)

# Company selection in sidebar
with st.sidebar:
    st.markdown("### 🏢 Company Selection")
    
    # Get available companies
    companies = company_manager.get_companies()
    
    if companies:
        # Add default option for backward compatibility
        company_options = [{'company_id': 'packagingco', 'company_name': 'PackagingCo (Default)'}] + companies
        
        selected_company_display = st.selectbox(
            "Select Company",
            options=company_options,
            format_func=lambda x: f"{x['company_name']} ({x['company_id']})",
            index=0
        )
        
        selected_company_id = selected_company_display['company_id']
        
        # Show company info
        if selected_company_id != 'packagingco':
            st.info(f"**Industry:** {selected_company_display.get('industry', 'N/A')}")
            
            # Show quick stats
            from data_connector import get_company_summary_stats
            stats = get_company_summary_stats(selected_company_id)
            if 'error' not in stats:
                st.write(f"**Records:** {stats.get('total_records', 0):,}")
    else:
        st.info("No companies configured. Using default PackagingCo data.")
        selected_company_id = 'packagingco'

# Main content
st.markdown('<h1 class="main-header">🌱 EcoMetrics Dashboard</h1>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Integrated Business Intelligence: Financial Performance vs Sustainability Impact</p>', unsafe_allow_html=True)

# Display connection status
if availability['available']:
    st.markdown(f"""
    <div class="status-card">
        <h4>✅ Data Pipeline Connected</h4>
        <p>{availability['message']}</p>
        <small>Database: {availability['db_path']}</small>
        <br><small><strong>Company:</strong> {selected_company_id}</small>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="status-card">
        <h4>⚠️ Data Pipeline Not Available</h4>
        <p>{availability['message']}</p>
        <small>Database: {availability['db_path']}</small>
        <br><small><strong>Note:</strong> {availability.get('deployment_note', '')}</small>
    </div>
    """, unsafe_allow_html=True)

# Load data based on selected company
@st.cache_data(ttl=3600)
def load_company_dashboard_data(company_id: str):
    """Load data for a specific company"""
    if company_id == 'packagingco':
        # Use original loading functions for backward compatibility
        esg_data, esg_status = load_esg_data()
        finance_data, finance_status = load_finance_data()
        supply_data, supply_status = load_supply_chain_data()
    else:
        # Use company-specific loading functions
        esg_data, esg_status = load_company_esg_data(company_id)
        finance_data, finance_status = load_company_finance_data(company_id)
        supply_data, supply_status = load_company_supply_chain_data(company_id)
    
    return {
        'esg': {'data': esg_data, 'status': esg_status},
        'finance': {'data': finance_data, 'status': finance_status},
        'supply': {'data': supply_data, 'status': supply_status}
    }

with st.spinner(f"Loading data for {selected_company_id}..."):
    all_data = load_company_dashboard_data(selected_company_id)

# Sidebar filters for cross-functional analysis
with st.sidebar:
    st.markdown("### 🔍 Dashboard Filters")
    
    # Date range filter (using the most restrictive date range across all datasets)
    all_dates = []
    for dataset in all_data.values():
        if not dataset['data'].empty and 'date' in dataset['data'].columns:
            all_dates.extend(dataset['data']['date'].tolist())
    
    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
        date_range = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    else:
        date_range = None
    
    # Product line filter (common across datasets)
    product_lines = set()
    for dataset in all_data.values():
        if not dataset['data'].empty and 'product_line' in dataset['data'].columns:
            product_lines.update(dataset['data']['product_line'].unique())
    
    product_lines = ['All'] + sorted(list(product_lines))
    selected_product = st.selectbox("Product Line", product_lines)

# Executive Summary Section
st.markdown("## 📊 Executive Summary")

# Create three columns for high-level KPIs
col1, col2, col3, col4 = st.columns(4)

# Calculate cross-functional KPIs
esg_data = all_data['esg']['data']
finance_data = all_data['finance']['data']
supply_data = all_data['supply']['data']

# Filter data if date range is selected
if date_range and len(date_range) == 2:
    start_date = pd.to_datetime(date_range[0])
    end_date = pd.to_datetime(date_range[1])
    
    if not esg_data.empty:
        esg_data = esg_data[(esg_data['date'] >= start_date) & (esg_data['date'] <= end_date)]
    if not finance_data.empty:
        finance_data = finance_data[(finance_data['date'] >= start_date) & (finance_data['date'] <= end_date)]
    if not supply_data.empty:
        supply_data = supply_data[(supply_data['date'] >= start_date) & (supply_data['date'] <= end_date)]

# Apply product line filter
if selected_product != 'All':
    if not esg_data.empty and 'product_line' in esg_data.columns:
        esg_data = esg_data[esg_data['product_line'] == selected_product]
    if not finance_data.empty and 'product_line' in finance_data.columns:
        finance_data = finance_data[finance_data['product_line'] == selected_product]

# Calculate KPIs
total_revenue = finance_data['total_revenue'].sum() if not finance_data.empty and 'total_revenue' in finance_data.columns else 0
total_emissions = esg_data['total_emissions_kg_co2'].sum() if not esg_data.empty and 'total_emissions_kg_co2' in esg_data.columns else 0
avg_profit_margin = finance_data['avg_profit_margin_pct'].mean() if not finance_data.empty and 'avg_profit_margin_pct' in finance_data.columns else 0
avg_sustainability = esg_data['avg_recycled_material_pct'].mean() if not esg_data.empty and 'avg_recycled_material_pct' in esg_data.columns else 0

# Helper function to format large numbers
def format_large_number(value):
    if value >= 1_000_000_000:
        return f"${value/1_000_000_000:.1f}B"
    elif value >= 1_000_000:
        return f"${value/1_000_000:.1f}M"
    elif value >= 1_000:
        return f"${value/1_000:.0f}K"
    else:
        return f"${value:.0f}"

def format_emissions(value):
    if value >= 1_000_000:
        return f"{value/1_000_000:.1f}M kg"
    elif value >= 1_000:
        return f"{value/1_000:.0f}K kg"
    else:
        return f"{value:.0f} kg"

with col1:
    st.metric(
        label="💰 Revenue",
        value=format_large_number(total_revenue) if total_revenue > 0 else "No data",
        help="Total revenue across all operations"
    )

with col2:
    st.metric(
        label="🌱 Emissions",
        value=format_emissions(total_emissions) if total_emissions > 0 else "No data",
        help="Total carbon emissions across operations"
    )

with col3:
    st.metric(
        label="📈 Profit Margin",
        value=f"{avg_profit_margin:.1f}%" if avg_profit_margin > 0 else "No data",
        help="Average profit margin percentage"
    )

with col4:
    st.metric(
        label="♻️ Recycled %",
        value=f"{avg_sustainability:.1f}%" if avg_sustainability > 0 else "No data",
        help="Average percentage of recycled materials used"
    )

# Enhanced Monthly Metrics Section
st.markdown("---")
st.markdown("## 📈 Monthly Performance Trends")

if not finance_data.empty:
    # Calculate monthly metrics and growth
    monthly_revenue = finance_data.groupby('date').agg({
        'total_revenue': 'sum',
        'avg_profit_margin_pct': 'mean',
        'total_transactions': 'sum'
    }).reset_index().sort_values('date')
    
    if len(monthly_revenue) >= 2:
        # Calculate month-over-month growth
        monthly_revenue['revenue_growth'] = monthly_revenue['total_revenue'].pct_change() * 100
        monthly_revenue['margin_change'] = monthly_revenue['avg_profit_margin_pct'].diff()
        
        # Get latest vs previous month metrics
        latest_month = monthly_revenue.iloc[-1]
        prev_month = monthly_revenue.iloc[-2] if len(monthly_revenue) >= 2 else None
        
        # Enhanced KPIs row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            current_revenue = latest_month['total_revenue']
            revenue_delta = latest_month['revenue_growth'] if not pd.isna(latest_month['revenue_growth']) else 0
            st.metric(
                label="📊 Current Month Revenue",
                value=format_large_number(current_revenue),
                delta=f"{revenue_delta:+.1f}%" if revenue_delta != 0 else None,
                help="Latest month revenue with growth rate"
            )
        
        with col2:
            current_margin = latest_month['avg_profit_margin_pct']
            margin_delta = latest_month['margin_change'] if not pd.isna(latest_month['margin_change']) else 0
            st.metric(
                label="💹 Current Margin",
                value=f"{current_margin:.1f}%",
                delta=f"{margin_delta:+.1f}pp" if margin_delta != 0 else None,
                help="Latest month profit margin with change in percentage points"
            )
        
        with col3:
            avg_monthly = monthly_revenue['total_revenue'].mean()
            vs_avg = (current_revenue - avg_monthly) / avg_monthly * 100
            st.metric(
                label="📈 vs Monthly Avg",
                value=f"{vs_avg:+.1f}%",
                help="Current month performance vs historical average"
            )
        
        with col4:
            ytd_revenue = monthly_revenue['total_revenue'].sum()
            months_elapsed = len(monthly_revenue)
            run_rate = (ytd_revenue / months_elapsed) * 12
            st.metric(
                label="🎯 Annual Run Rate",
                value=format_large_number(run_rate),
                help="Projected annual revenue based on current pace"
            )

    # Monthly Revenue Trend Chart
    st.markdown("### 💰 Monthly Revenue Performance")

    # Create comprehensive monthly revenue chart
    monthly_detailed = finance_data.groupby(['date', 'product_line'])['total_revenue'].sum().reset_index()
    
    # Overall monthly trend
    monthly_total = monthly_detailed.groupby('date')['total_revenue'].sum().reset_index()
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Main revenue trend chart using Plotly for smooth lines [following user preferences]
        fig_revenue = px.line(
            monthly_total,
            x='date',
            y='total_revenue',
            title='Monthly Revenue Trends',
            labels={
                'date': 'Date',
                'total_revenue': 'Monthly Revenue ($)',
            },
            line_shape='spline'
        )
        
        # Apply smooth line styling with standardized colors
        fig_revenue.update_traces(
            line=dict(width=4, color=get_monochrome_colors(1)[0]),  # Primary blue from monochrome palette
            mode='lines+markers',
            marker=dict(size=8, opacity=0.8, color=CSS_COLORS['primary-dark'])
        )
        
        fig_revenue.update_layout(
            height=400,
            plot_bgcolor=None,
            paper_bgcolor=None,
            font=dict(size=12),
            margin=dict(l=50, r=50, t=60, b=60),
            hovermode='x unified',
            showlegend=False
        )
        
        # Update axes styling with standardized colors
        fig_revenue.update_xaxes(
            gridcolor=CSS_COLORS['neutral-medium'],
            showgrid=True,
            zeroline=False,
            title_font_size=14
        )
        fig_revenue.update_yaxes(
            gridcolor=CSS_COLORS['neutral-medium'],
            showgrid=True,
            zeroline=False,
            title_font_size=14,
            tickformat='$,.0f'
        )
        
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col2:
        # Revenue insights box
        st.markdown("#### 📊 **Insights**")
        
        if len(monthly_total) >= 3:
            # Calculate insights
            revenue_trend = monthly_total['total_revenue']
            latest_3_months = revenue_trend.tail(3).mean()
            prev_3_months = revenue_trend.iloc[-6:-3].mean() if len(revenue_trend) >= 6 else revenue_trend.head(3).mean()
            
            trend_direction = "📈 **Growing**" if latest_3_months > prev_3_months else "📉 **Declining**"
            trend_pct = abs((latest_3_months - prev_3_months) / prev_3_months * 100)
            
            max_month = revenue_trend.max()
            min_month = revenue_trend.min()
            volatility = (revenue_trend.std() / revenue_trend.mean()) * 100
            
            st.markdown(f"""
            **3-Month Trend:**  
            {trend_direction} by {trend_pct:.1f}%
            
            **Performance Range:**  
            📊 High: {format_large_number(max_month)}  
            📊 Low: {format_large_number(min_month)}  
            
            **Stability:**  
            📈 Volatility: {volatility:.1f}%  
            {"🟢 Stable" if volatility < 15 else "🟡 Moderate" if volatility < 30 else "🔴 High variance"}
            
            **Current Status:**  
            {format_large_number(latest_month['total_revenue'])} in latest month
            """)

    # Product Line Monthly Performance
    st.markdown("### 🏭 Revenue by Product Line")
    
    # Monthly revenue by product line chart
    product_monthly = finance_data.groupby(['date', 'product_line'])['total_revenue'].sum().reset_index()
    
    fig_products = px.line(
        product_monthly,
        x='date',
        y='total_revenue',
        color='product_line',
        title='Monthly Revenue Trends by Product Line',
        labels={
            'date': 'Date',
            'total_revenue': 'Monthly Revenue ($)',
            'product_line': 'Product Line'
        },
        line_shape='spline'
    )
    
    # Apply pastel color scheme [following user preferences]
    fig_products.update_traces(
        line=dict(width=3),
        mode='lines+markers',
        marker=dict(size=6, opacity=0.7)
    )
    
    fig_products.update_layout(
        height=400,
        plot_bgcolor=None,
        paper_bgcolor=None,
        font=dict(size=12),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.15,
            xanchor="center",
            x=0.5
        ),
        margin=dict(l=50, r=50, t=60, b=100)
    )
    
    # Update axes with standardized colors
    fig_products.update_xaxes(
        gridcolor=CSS_COLORS['neutral-medium'],
        showgrid=True,
        zeroline=False
    )
    fig_products.update_yaxes(
        gridcolor=CSS_COLORS['neutral-medium'],
        showgrid=True,
        zeroline=False,
        tickformat='$,.0f'
    )
    
    st.plotly_chart(fig_products, use_container_width=True)

# Cross-functional Analysis Section
st.markdown("---")
st.markdown("## 🔄 Cross-Functional Analysis")

# Create tabs for different analysis views
tab1, tab2, tab3 = st.tabs(["💰🌱 Financial vs Environmental", "📊 Performance Correlation", "🎯 Strategic Insights"])

with tab1:
    st.markdown("### Financial Performance vs Environmental Impact")
    
    # Revenue vs Emissions over time - Full width
    st.markdown("#### Revenue vs CO2 Emissions Over Time")
    if not finance_data.empty and not esg_data.empty:
        # Merge data by date for comparison
        finance_monthly = finance_data.groupby('date')['total_revenue'].sum().reset_index()
        esg_monthly = esg_data.groupby('date')['total_emissions_kg_co2'].sum().reset_index()
        
        if not finance_monthly.empty and not esg_monthly.empty:
            # Create dual-axis chart using Plotly
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            # Add revenue line
            fig.add_trace(
                go.Scatter(
                    x=finance_monthly['date'],
                    y=finance_monthly['total_revenue'],
                    mode='lines+markers',
                    name='Revenue ($)',
                    line=dict(color=get_financial_color('revenue'), width=3),
                    marker=dict(size=6)
                ),
                secondary_y=False,
            )
            
            # Add emissions line
            fig.add_trace(
                go.Scatter(
                    x=esg_monthly['date'],
                    y=esg_monthly['total_emissions_kg_co2'],
                    mode='lines+markers',
                    name='CO2 Emissions (kg)',
                    line=dict(color=get_sustainability_color('emissions'), width=3),
                    marker=dict(size=6)
                ),
                secondary_y=True,
            )
            
            # Update layout
            fig.update_layout(
                hovermode='x unified',
                height=450,
                margin=dict(l=60, r=60, t=40, b=60),
                showlegend=True
            )
            
            fig.update_xaxes(title_text="Date")
            fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
            fig.update_yaxes(title_text="CO2 Emissions (kg)", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Insufficient data for revenue vs emissions comparison")
    else:
        st.info("Financial and ESG data needed for this analysis")
    
    # Add spacing
    st.markdown("---")
    
    # Profit Margin vs Sustainability Rating - Full width
    st.markdown("#### Sustainability vs Profitability Analysis")
    
    st.markdown("""
    **How to read this chart:**
    - Each bubble represents a month of business performance
    - **Bubble size** = Revenue volume (bigger = more revenue)
    - **Bubble color** = CO2 emissions (🟢 green = lower emissions, 🔴 red = higher emissions)
    - **Position** shows the relationship between using recycled materials and profit margins
    """)
    
    if not finance_data.empty and not esg_data.empty:
        # Create scatter plot
        if 'avg_profit_margin_pct' in finance_data.columns and 'avg_recycled_material_pct' in esg_data.columns:
            # Merge data for scatter plot
            finance_grouped = finance_data.groupby('date').agg({
                'avg_profit_margin_pct': 'mean',
                'total_revenue': 'sum'
            }).reset_index()
            
            esg_grouped = esg_data.groupby('date').agg({
                'avg_recycled_material_pct': 'mean',
                'total_emissions_kg_co2': 'sum'
            }).reset_index()
            
            merged_data = pd.merge(finance_grouped, esg_grouped, on='date', how='inner')
            
            if not merged_data.empty:
                fig = px.scatter(
                    merged_data,
                    x='avg_recycled_material_pct',
                    y='avg_profit_margin_pct',
                    size='total_revenue',
                    color='total_emissions_kg_co2',
                    labels={
                        'avg_recycled_material_pct': 'Recycled Materials Usage (%)',
                        'avg_profit_margin_pct': 'Profit Margin (%)',
                        'total_revenue': 'Revenue Volume ($)',
                        'total_emissions_kg_co2': 'CO2 Emissions (kg)'
                    },
                    color_continuous_scale='RdYlGn_r',
                    height=450,
                    hover_data={'total_revenue': ':$,.0f', 'total_emissions_kg_co2': ':,.0f'}
                )
                
                fig.update_layout(
                    showlegend=True,
                    hovermode='closest',
                    margin=dict(l=60, r=60, t=40, b=60)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Add interpretation
                st.markdown("##### 🔍 **What This Tells Us:**")
                
                # Calculate some insights
                high_recycled = merged_data[merged_data['avg_recycled_material_pct'] >= 60]
                low_recycled = merged_data[merged_data['avg_recycled_material_pct'] < 60]
                
                insights = []
                
                if not high_recycled.empty and not low_recycled.empty:
                    high_margin = high_recycled['avg_profit_margin_pct'].mean()
                    low_margin = low_recycled['avg_profit_margin_pct'].mean()
                    
                    if high_margin > low_margin:
                        insights.append(f"✅ **Higher recycled content appears profitable**: {high_margin:.1f}% avg margin vs {low_margin:.1f}% for lower recycled content")
                    else:
                        insights.append(f"⚠️ **Lower recycled content shows higher margins**: {low_margin:.1f}% vs {high_margin:.1f}% - opportunity to optimize")
                
                # Check correlation
                correlation = merged_data['avg_recycled_material_pct'].corr(merged_data['avg_profit_margin_pct'])
                if abs(correlation) > 0.3:
                    direction = "positive" if correlation > 0 else "negative"
                    insights.append(f"📊 **{direction.title()} correlation** between recycled materials and profit margins (r={correlation:.2f})")
                
                # Emissions insight
                low_emission_months = merged_data[merged_data['total_emissions_kg_co2'] <= merged_data['total_emissions_kg_co2'].median()]
                if not low_emission_months.empty:
                    avg_margin_low_emissions = low_emission_months['avg_profit_margin_pct'].mean()
                    insights.append(f"🌱 **Lower emission months** average {avg_margin_low_emissions:.1f}% profit margin")
                
                for insight in insights:
                    st.markdown(f"• {insight}")
                
                if not insights:
                    st.markdown("• More data needed to identify clear patterns between sustainability and profitability")
                    
            else:
                st.info("No overlapping data for profit vs sustainability analysis")
        else:
            st.info("Required columns not found for profit vs sustainability analysis")
    else:
        st.info("Both financial and ESG data needed for this analysis")

with tab2:
    st.markdown("### Performance Correlation Matrix")
    
    # Create correlation analysis
    correlation_data = []
    
    # Collect metrics from all datasets
    if not finance_data.empty:
        finance_metrics = finance_data.groupby('date').agg({
            'total_revenue': 'sum',
            'avg_profit_margin_pct': 'mean'
        }).reset_index()
        correlation_data.append(finance_metrics)
    
    if not esg_data.empty:
        esg_metrics = esg_data.groupby('date').agg({
            'total_emissions_kg_co2': 'sum',
            'avg_recycled_material_pct': 'mean',
            'avg_renewable_energy_pct': 'mean'
        }).reset_index()
        if correlation_data:
            correlation_data[0] = pd.merge(correlation_data[0], esg_metrics, on='date', how='outer')
        else:
            correlation_data.append(esg_metrics)
    
    if correlation_data and not correlation_data[0].empty:
        try:
            # Select only numeric columns for correlation
            numeric_cols = []
            for col in correlation_data[0].columns:
                if col != 'date' and pd.api.types.is_numeric_dtype(correlation_data[0][col]):
                    numeric_cols.append(col)
            
            if numeric_cols:
                numeric_df = correlation_data[0][numeric_cols]
                corr_df = numeric_df.corr()
                
                # Create heatmap
                fig = px.imshow(
                    corr_df,
                    labels=dict(x="Metrics", y="Metrics", color="Correlation"),
                    title="Performance Metrics Correlation Matrix",
                    color_continuous_scale='RdBu',
                    aspect="auto",
                    height=500
                )
                
                fig.update_layout(
                    title_font_size=16,
                    font=dict(size=10),
                    height=600,
                    margin=dict(l=60, r=60, t=80, b=60)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Insights from correlation
                st.markdown("#### 🔍 Key Correlations:")
                high_corr = []
                for i in range(len(corr_df.columns)):
                    for j in range(i+1, len(corr_df.columns)):
                        try:
                            corr_val = corr_df.iloc[i, j]
                            if pd.notna(corr_val) and isinstance(corr_val, (int, float)) and abs(corr_val) > 0.5:
                                high_corr.append({
                                    'metrics': f"{corr_df.columns[i]} vs {corr_df.columns[j]}",
                                    'correlation': corr_val,
                                    'strength': 'Strong positive' if corr_val > 0.5 else 'Strong negative'
                                })
                        except (ValueError, TypeError):
                            continue
                
                if high_corr:
                    for item in high_corr[:3]:  # Show top 3
                        st.write(f"• **{item['metrics']}**: {item['strength']} correlation ({item['correlation']:.2f})")
                else:
                    st.write("• No strong correlations found in current data")
            else:
                st.info("No numeric data available for correlation analysis")
        except Exception as e:
            st.info(f"Unable to generate correlation analysis: {str(e)}")
    else:
        st.info("Insufficient data for correlation analysis")

with tab3:
    st.markdown("### Strategic Business Insights")
    
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("#### 💡 Key Insights")
        
        insights = []
        
        # Financial insights
        if not finance_data.empty and 'avg_profit_margin_pct' in finance_data.columns:
            avg_margin = finance_data['avg_profit_margin_pct'].mean()
            if avg_margin > 15:
                insights.append("✅ Strong profit margins indicate healthy financial performance")
            elif avg_margin > 10:
                insights.append("⚠️ Moderate profit margins suggest room for optimization")
            else:
                insights.append("🔴 Low profit margins require immediate attention")
        
        # ESG insights
        if not esg_data.empty and 'avg_recycled_material_pct' in esg_data.columns:
            avg_recycled = esg_data['avg_recycled_material_pct'].mean()
            if avg_recycled > 50:
                insights.append("🌱 Excellent sustainability performance with high recycled content")
            elif avg_recycled > 25:
                insights.append("♻️ Good progress on sustainability initiatives")
            else:
                insights.append("📈 Opportunity to improve recycled material usage")
        
        # Supply chain insights
        if not supply_data.empty:
            total_orders = len(supply_data)
            if total_orders > 1000:
                insights.append("🔄 High supply chain activity indicates strong operational scale")
            else:
                insights.append("📊 Moderate supply chain activity")
        
        # Display insights
        for insight in insights:
            st.markdown(f"**{insight}**")
        
        if not insights:
            st.info("Generate insights by loading data from all modules")
    
    with col2:
        st.markdown("#### 🎯 Recommendations")
        
        recommendations = [
            "**Integrate ESG metrics into financial planning** - Track how sustainability investments impact profitability",
            "**Optimize supply chain for sustainability** - Focus on suppliers with better environmental ratings",
            "**Monitor profit margins by product line** - Identify which sustainable products are most profitable",
            "**Set targets for carbon emissions per revenue dollar** - Create a sustainability efficiency metric",
            "**Implement circular economy principles** - Increase recycled content while maintaining margins"
        ]
        
        for rec in recommendations:
            st.markdown(f"• {rec}")

# Navigation Section
st.markdown("---")
st.markdown("## 🚀 Explore Detailed Analysis")
st.markdown("Dive deeper into specific areas for comprehensive insights")

# Create navigation grid
nav_col1, nav_col2, nav_col3 = st.columns(3)

with nav_col1:
    if st.button("🌱 **ESG Insights**\n\nDetailed sustainability metrics and environmental impact analysis", key="nav_esg", use_container_width=True):
        st.switch_page("pages/1_ESG_Insights.py")
    
    if st.button("💰 **Financial Analysis**\n\nComprehensive financial forecasting and profit analysis", key="nav_finance", use_container_width=True):
        st.switch_page("pages/2_Financial_Analysis.py")

with nav_col2:
    if st.button("🔄 **Supply Chain Insights**\n\nSupply chain optimization and operational efficiency", key="nav_supply", use_container_width=True):
        st.switch_page("pages/3_Supply_Chain_Insights.py")
    
    if st.button("👥 **Customer Insights**\n\nCustomer behavior analysis and segmentation", key="nav_customer", use_container_width=True):
        st.switch_page("pages/4_Customer_Insights.py")

with nav_col3:
    if st.button("📊 **Data Browser**\n\nExplore raw data and perform ad-hoc analysis", key="nav_data", use_container_width=True):
        st.switch_page("pages/5_Data_Browser.py")
    
    if st.button("📈 **Forecasting**\n\nAdvanced forecasting models for business planning", key="nav_forecast", use_container_width=True):
        st.switch_page("pages/6_Forecasting.py")

# Footer
st.markdown("---")
st.markdown("## 👨‍💻 About the Creator")

st.markdown(f"""
<div style="background: linear-gradient(135deg, {CSS_COLORS['primary-light']}20, {CSS_COLORS['secondary-light']}20); 
           padding: 2rem; border-radius: 15px; border: 2px solid {CSS_COLORS['primary']}; margin: 2rem 0; text-align: center;">
    <div style="font-size: 2rem; font-weight: bold; color: {CSS_COLORS['primary']}; margin-bottom: 0.5rem;">
        👨‍💻 Conner Kupferberg
    </div>
    <div style="font-size: 1.2rem; color: {CSS_COLORS['neutral-dark']}; margin-bottom: 1rem;">
        Data Scientist & Business Intelligence Developer
    </div>
    <div style="font-size: 1rem; color: {CSS_COLORS['neutral-dark']}; line-height: 1.6; margin-bottom: 1.5rem;">
        Passionate about leveraging data to drive sustainable business decisions and create meaningful insights. 
        This dashboard showcases expertise in data engineering, analytics, and visualization using modern BI tools 
        including dbt, Streamlit, and advanced forecasting techniques.
    </div>
    <a href="https://connerkupferberg.com" target="_blank" 
       style="display: inline-block; background-color: {CSS_COLORS['primary']}; color: white; padding: 0.75rem 1.5rem; 
              text-decoration: none; border-radius: 25px; font-weight: bold;">
        🌐 View My Portfolio
    </a>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div style='text-align: center; color: #666; margin-top: 2rem;'>
    <p>Built with Streamlit • Powered by dbt • Data-driven insights</p>
</div>
""", unsafe_allow_html=True) 