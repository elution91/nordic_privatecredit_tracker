"""
Nordic Private Credit Market Dashboard
File 3: dashboard.py (Streamlit web application)

Run with: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import json
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import psycopg2

# Page configuration
st.set_page_config(
    page_title="Nordic Private Credit Tracker",
    page_icon="🏛️",
    layout="wide"
)

def get_database_connection(password):
    """Get database connection with provided password"""
    return psycopg2.connect(
        host="localhost",
        database="nordic_private_credit",
        user="postgres",
        password=password,
        port=5432
    )

def load_companies_data(password):
    """Load companies data from database"""
    try:
        conn = get_database_connection(password)
        
        query = """
        SELECT 
            corporate_id, name, category, city, postal_code,
            sni_code, sni_description, legal_form_description,
            is_active, registration_date, updated_at
        FROM companies 
        WHERE api_status = 'success'
        ORDER BY updated_at DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return pd.DataFrame()

def load_analytics_data():
    """Load pre-calculated analytics data"""
    try:
        with open('market_analytics_results.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.warning("⚠️ No analytics data found. Run 'python market_analytics.py' first.")
        return {}

def create_kpi_cards(metrics):
    """Create KPI cards for the dashboard"""
    if not metrics or 'summary' not in metrics:
        st.error("No metrics data available")
        return
    
    summary = metrics['summary']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total Entities",
            value=summary.get('total_entities', 0),
            delta=None
        )
    
    with col2:
        active_entities = summary.get('active_entities', 0)
        activity_rate = summary.get('activity_rate', 0)
        st.metric(
            label="✅ Active Entities", 
            value=f"{active_entities} ({activity_rate}%)",
            delta=None
        )
    
    with col3:
        st.metric(
            label="🏢 Categories",
            value=summary.get('unique_categories', 0),
            delta=None
        )
    
    with col4:
        st.metric(
            label="🗺️ Cities",
            value=summary.get('unique_cities', 0),
            delta=None
        )

def create_geographic_chart(metrics):
    """Create geographic distribution chart"""
    if not metrics or 'geographic' not in metrics:
        return
    
    geographic = metrics['geographic']
    top_cities = geographic.get('top_cities', {})
    
    if not top_cities:
        st.warning("No geographic data available")
        return
    
    # Create bar chart
    cities = list(top_cities.keys())[:10]  # Top 10 cities
    counts = list(top_cities.values())[:10]
    
    fig = px.bar(
        x=counts,
        y=cities,
        orientation='h',
        title="🗺️ Geographic Distribution (Top 10 Cities)",
        labels={'x': 'Number of Entities', 'y': 'City'},
        color=counts,
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show concentration metric
    concentration = geographic.get('concentration_level', 'Unknown')
    concentration_index = geographic.get('concentration_index', 0)
    st.caption(f"Market Concentration: {concentration} (HHI: {concentration_index})")

def create_category_chart(metrics):
    """Create category distribution chart"""
    if not metrics or 'categories' not in metrics:
        return
    
    categories = metrics['categories']
    category_dist = categories.get('category_distribution', {})
    
    if not category_dist:
        st.warning("No category data available")
        return
    
    # Create donut chart
    labels = list(category_dist.keys())
    values = list(category_dist.values())
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.3,
        textinfo='label+percent',
        textposition='outside'
    )])
    
    fig.update_layout(
        title="📋 Market Categories Distribution",
        height=500,
        showlegend=True,
        legend=dict(orientation="v", yanchor="middle", y=0.5, xanchor="left", x=1.01)
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_vintage_chart(metrics):
    """Create registration vintage chart"""
    if not metrics or 'vintage' not in metrics:
        return
    
    vintage = metrics['vintage']
    vintage_dist = vintage.get('vintage_distribution', {})
    
    if not vintage_dist:
        st.warning("No vintage data available")
        return
    
    # Convert to sorted lists
    years = sorted(vintage_dist.keys())
    counts = [vintage_dist[year] for year in years]
    
    fig = px.line(
        x=years,
        y=counts,
        title="📅 Registration Timeline",
        labels={'x': 'Year', 'y': 'New Registrations'},
        markers=True
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Show vintage insights
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Oldest Registration", vintage.get('oldest_registration', 'N/A'))
    with col2:
        st.metric("Peak Year", f"{vintage.get('peak_registration_year', 'N/A')} ({vintage.get('peak_year_count', 0)})")
    with col3:
        st.metric("Recent Registrations", f"{vintage.get('recent_registrations', 0)} ({vintage.get('recent_registrations_rate', 0)}%)")

def create_market_trends_section(metrics):
    """Create market trends section"""
    if not metrics or 'market_trends' not in metrics:
        return
    
    trends = metrics['market_trends']
    
    st.subheader("📈 Market Trends")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        trend_direction = trends.get('trend_direction', 'Unknown')
        growth_rate = trends.get('recent_growth_rate', 0)
        
        # Color code based on trend
        if trend_direction == 'Growing':
            st.success(f"📈 {trend_direction} ({growth_rate}%)")
        elif trend_direction == 'Declining':
            st.error(f"📉 {trend_direction} ({growth_rate}%)")
        else:
            st.info(f"📊 {trend_direction} ({growth_rate}%)")
    
    with col2:
        maturity = trends.get('market_maturity', 'Unknown')
        market_age = trends.get('market_age_years', 0)
        st.info(f"🏛️ {maturity} Market ({market_age} years)")
    
    with col3:
        avg_registrations = trends.get('average_yearly_registrations', 0)
        st.info(f"📊 Avg. {avg_registrations:.1f} registrations/year")

def create_data_quality_section(metrics):
    """Create data quality section"""
    if not metrics or 'data_quality' not in metrics:
        return
    
    quality = metrics['data_quality']
    
    st.subheader("📊 Data Quality")
    
    col1, col2 = st.columns(2)
    
    with col1:
        completeness = quality.get('overall_completeness', 0)
        grade = quality.get('quality_grade', 'Unknown')
        
        # Color code based on quality
        if completeness >= 90:
            st.success(f"Grade: {grade} ({completeness}%)")
        elif completeness >= 75:
            st.warning(f"Grade: {grade} ({completeness}%)")
        else:
            st.error(f"Grade: {grade} ({completeness}%)")
    
    with col2:
        field_completeness = quality.get('field_completeness', {})
        if field_completeness:
            st.write("Field Completeness:")
            for field, percentage in field_completeness.items():
                st.progress(percentage / 100, text=f"{field}: {percentage}%")

def create_detailed_table(df):
    """Create detailed companies table"""
    if df.empty:
        st.warning("No company data available")
        return
    
    st.subheader("📋 Detailed Entity List")
    
    # Add filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        categories = ['All'] + sorted(df['category'].dropna().unique().tolist())
        selected_category = st.selectbox("Filter by Category", categories)
    
    with col2:
        cities = ['All'] + sorted(df['city'].dropna().unique().tolist())
        selected_city = st.selectbox("Filter by City", cities)
    
    with col3:
        activity_filter = st.selectbox("Filter by Activity", ['All', 'Active Only', 'Inactive Only'])
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_category != 'All':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]
    
    if selected_city != 'All':
        filtered_df = filtered_df[filtered_df['city'] == selected_city]
    
    if activity_filter == 'Active Only':
        filtered_df = filtered_df[filtered_df['is_active'] == True]
    elif activity_filter == 'Inactive Only':
        filtered_df = filtered_df[filtered_df['is_active'] == False]
    
    # Display table
    st.write(f"Showing {len(filtered_df)} of {len(df)} entities")
    
    # Select columns to display
    display_columns = ['name', 'category', 'city', 'is_active', 'registration_date', 'updated_at']
    available_columns = [col for col in display_columns if col in filtered_df.columns]
    
    if available_columns:
        st.dataframe(
            filtered_df[available_columns],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.error("No displayable columns found")

def main():
    """Main dashboard function"""
    # Header
    st.title("🏛️ Nordic Private Credit Market Tracker")
    st.markdown("Real-time transparency in the Nordic private credit market")
    
    # Initialize session state for password
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.db_password = None
    
    # Authentication section
    if not st.session_state.authenticated:
        st.subheader("🔐 Database Authentication")
        
        with st.form("auth_form"):
            password = st.text_input("PostgreSQL Password", type="password")
            submit = st.form_submit_button("Connect to Database")
            
            if submit and password:
                try:
                    # Test connection
                    test_conn = get_database_connection(password)
                    test_conn.close()
                    
                    # If successful, store password and mark as authenticated
                    st.session_state.db_password = password
                    st.session_state.authenticated = True
                    st.success("✅ Connected successfully!")
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}")
                    st.error("Please check your password and ensure PostgreSQL is running.")
        
        st.stop()
    
    # Main dashboard (only runs after authentication)
    password = st.session_state.db_password
    
    # Add logout button in sidebar
    with st.sidebar:
        st.subheader("🔧 Controls")
        if st.button("🔓 Logout"):
            st.session_state.authenticated = False
            st.session_state.db_password = None
            st.rerun()
        
        st.divider()
        
        if st.button("🔄 Refresh Data"):
            st.rerun()
        
        st.divider()
        
        st.subheader("📊 Data Pipeline")
        st.caption("Run these commands to update data:")
        st.code("python etl_pipeline.py")
        st.code("python market_analytics.py")
    
    # Load data
    with st.spinner("Loading market data..."):
        df = load_companies_data(password)
        metrics = load_analytics_data()
    
    if df.empty and not metrics:
        st.error("❌ No data available. Please run the ETL pipeline first.")
        st.code("python etl_pipeline.py")
        st.stop()
    
    # Display last update time
    if metrics and 'metadata' in metrics:
        last_update = metrics['metadata'].get('analysis_timestamp', 'Unknown')
        st.caption(f"📅 Last updated: {last_update[:19]}")
    
    # KPI Cards
    if metrics:
        create_kpi_cards(metrics)
        st.divider()
    
    # Charts in two columns
    if metrics:
        col1, col2 = st.columns(2)
        
        with col1:
            create_geographic_chart(metrics)
        
        with col2:
            create_category_chart(metrics)
        
        st.divider()
        
        # Vintage chart full width
        create_vintage_chart(metrics)
        
        st.divider()
        
        # Market trends and data quality
        create_market_trends_section(metrics)
        create_data_quality_section(metrics)
        
        st.divider()
    
    # Detailed table
    if not df.empty:
        create_detailed_table(df)
    
    # Footer
    st.divider()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.caption("📊 Data Sources: Finansinspektionen + Bolagsverket")
    
    with col2:
        st.caption(f"💾 Total Records: {len(df) if not df.empty else 0}")
    
    with col3:
        st.caption("⚡ Powered by Nordic Credit Tracker ETL")

if __name__ == "__main__":
    main()