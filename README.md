# 🌱 EcoMetrics - ESG & Financial Intelligence Platform

A comprehensive business intelligence platform for Environmental, Social, and Governance (ESG) metrics and financial performance analysis. Built with modern data engineering practices and designed for enterprise-scale sustainability analytics.

## 🚀 Live Demo

**[EcoMetrics Dashboard](https://ecometrics.streamlit.app)**

## 📊 Features

### **ESG Analytics** ✅
- Sustainability performance tracking
- Emissions analysis and reporting
- Material efficiency metrics
- Renewable energy usage monitoring
- Water conservation analytics

### **Financial Intelligence** ✅
- Revenue and profitability analysis
- Revenue efficiency by product line (revenue per transaction)
- Cost structure optimization
- Regional performance insights with full-width visualizations
- Growth trend forecasting
- Profit margin analysis

### **Supply Chain Optimization** ✅
- Inventory management and tracking
- Material flow analysis
- Operational efficiency metrics
- Supply chain cost optimization
- Sustainability impact assessment

### **Customer Insights** ✅
- Customer behavior analysis
- Segmentation and profiling
- Sustainability preferences tracking
- Customer lifetime value analysis
- Market opportunity identification

### **Interactive Dashboards** ✅
- Real-time data visualization with Altair and Plotly
- Multi-dimensional filtering
- Responsive design (mobile-friendly)
- Export capabilities
- Drill-down analytics
- Standardized monochrome pastel color scheme

### **Data Pipeline** ✅
- Robust ETL process with dbt
- Comprehensive data testing
- Transaction-level data processing
- Automated data validation
- Production-ready architecture

## 🛠️ Technology Stack

- **Frontend**: Streamlit (Python)
- **Data Processing**: dbt (Data Build Tool)
- **Database**: DuckDB
- **Analysis**: Pandas, Plotly, Altair, NumPy, Scikit-learn
- **Testing**: Comprehensive dbt tests
- **Deployment**: Streamlit Cloud

## 🎨 Design System

### **Color Standardization**
- **Monochrome Pastel Palette**: Easy-on-the-eyes blue-grey tones
- **Comparison Colors**: Distinct colors for multi-category charts
- **Heat Maps**: Standardized color schemes for performance metrics
- **Accessibility**: Optimized for extended dashboard use

### **Visualization Standards**
- **Smooth Line Charts**: Altair and Plotly with optimized styling
- **Full-Width Charts**: Responsive design for better data presentation
- **Consistent Styling**: Standardized colors and formatting across all pages
- **Mobile Optimization**: Responsive layouts for all screen sizes

## 🏗️ Architecture

```
📁 bi-portfolio/
├── 📁 dbt/                    # Data transformation layer
│   ├── 📁 models/            # SQL models
│   │   ├── 📁 finance/       # Financial analysis models
│   │   ├── 📁 sustainability/ # ESG analysis models
│   │   ├── 📁 supply_chain/  # Supply chain models
│   │   └── 📁 customer_insights/ # Customer analysis models
│   ├── 📁 tests/             # Data quality tests
│   └── 📁 macros/            # Reusable SQL macros
├── 📁 ecometrics/            # Main Streamlit application
│   ├── Home.py              # Main dashboard entry point
│   ├── 📁 pages/            # Multi-page navigation
│   ├── data_connector.py    # Database connection logic
│   ├── color_config.py      # Centralized color configuration
│   └── portfolio.duckdb     # Production database
├── 📁 src/                   # Python analysis modules
│   └── 📁 packagingco_insights/
└── 📁 data/                  # Raw and processed data
```

## 🚀 Quick Start

### **Local Development**

1. **Clone the repository**:
   ```bash
   git clone https://github.com/connerkup/ecometrics.git
   cd ecometrics
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the data pipeline**:
   ```bash
   cd dbt
   dbt deps
   dbt run
   dbt test
   ```

4. **Start the dashboard**:
   ```bash
   cd ecometrics
   streamlit run Home.py
   ```

5. **Access the application**:
   Open your browser to `http://localhost:8501`

### **Deployment to Streamlit Cloud**

1. **Prepare for deployment**:
   ```bash
   cd ecometrics
   python prepare_for_deployment.py
   ```

2. **Deploy to Streamlit Cloud**:
   - Connect your GitHub repository to Streamlit Cloud
   - Set main file to: `ecometrics/Home.py`
   - Set requirements file to: `ecometrics/requirements.txt`

## 📈 Key Metrics & Insights

### **Sustainability KPIs**
- **ESG Score**: Composite sustainability performance (0-100)
- **Emissions per Unit**: CO2 emissions efficiency
- **Recycled Content**: Material sustainability percentage
- **Renewable Energy**: Clean energy usage percentage
- **Water Efficiency**: Conservation and recycling metrics

### **Financial Performance**
- **Revenue Trends**: Growth and regional analysis
- **Revenue Efficiency**: Revenue per transaction by product line
- **Profit Margins**: Gross and net profitability
- **Cost Optimization**: Efficiency and waste reduction
- **Market Performance**: Segment and product analysis

### **Supply Chain Metrics**
- **Inventory Turnover**: Efficiency of inventory management
- **Material Flow**: Tracking of sustainable materials
- **Operational Costs**: Supply chain cost analysis
- **Sustainability Impact**: ESG metrics across supply chain

### **Customer Analytics**
- **Customer Segments**: Behavioral and demographic analysis
- **Sustainability Preferences**: Customer ESG awareness
- **Lifetime Value**: Customer profitability analysis
- **Market Opportunities**: Growth potential identification

## 🎯 Business Value

- **Data-Driven Sustainability**: Real-time ESG performance monitoring
- **Compliance Ready**: Comprehensive reporting for ESG regulations
- **Cost Optimization**: Identify efficiency opportunities
- **Strategic Planning**: Forecast trends and plan initiatives
- **Stakeholder Communication**: Clear, visual reporting
- **Visual Consistency**: Professional, standardized dashboard design

## 🧪 Data Quality & Testing

- **105+ automated tests** ensuring data accuracy
- **Comprehensive validation** of all calculations
- **Material balance checks** (percentages sum to 100%)
- **Range validation** for all metrics
- **Business logic verification** for profit margins

## 📊 Portfolio Project

This project demonstrates:

- **End-to-end data solution** from raw data to interactive dashboards
- **Modern data engineering** practices with dbt and comprehensive testing
- **Business intelligence** development with real-world applications
- **Sustainability analytics** - a rapidly growing field in business technology
- **Production-ready architecture** suitable for enterprise deployment
- **Professional design system** with standardized colors and visualizations

Perfect for showcasing data engineering, business intelligence, and sustainability technology skills!

## 🔄 Recent Updates

### **Visual Design Improvements**
- **Color Standardization**: Implemented centralized color configuration
- **Chart Enhancements**: Updated revenue efficiency metrics and regional visualizations
- **Responsive Design**: Full-width charts and mobile optimization
- **Smooth Visualizations**: Altair and Plotly with optimized styling

### **Functional Enhancements**
- **Revenue Efficiency Analysis**: New metrics for revenue per transaction
- **Regional Performance**: Full-width horizontal bar charts for better data presentation
- **Cross-Functional Analysis**: Enhanced integration between financial and ESG metrics
- **Performance Optimization**: Improved data loading and caching

## 🤝 Contributing

This is a portfolio project demonstrating modern data engineering and business intelligence capabilities. Feel free to explore the code and architecture!

## 📄 License

This project is for portfolio demonstration purposes.

---

**Built with ❤️ using modern data engineering practices**
