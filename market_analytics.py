"""
Nordic Private Credit Market Analytics
File 2: market_analytics.py (Runs separately from ETL)
"""

import pandas as pd
import psycopg2
import json
from datetime import datetime, timedelta
from typing import Dict, Any
import os

class DatabaseConfig:
    """Simple database configuration"""
    
    def __init__(self):
        self.host = "localhost"
        self.database = "nordic_private_credit"
        self.user = "postgres"
        self.port = 5432
        self.password = None
    
    def get_connection(self):
        """Get database connection"""
        if not self.password:
            import getpass
            self.password = getpass.getpass(f"PostgreSQL password for {self.user}@{self.host}: ")
        
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            port=self.port
        )

class MarketAnalyzer:
    """Calculate market metrics and analytics"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
    
    def get_companies_data(self) -> pd.DataFrame:
        """Get companies data from database"""
        conn = self.db_config.get_connection()
        
        query = """
        SELECT 
            corporate_id, name, category, city, postal_code,
            sni_code, sni_description, legal_form_description,
            is_active, registration_date, updated_at,
            CASE 
                WHEN updated_at > (CURRENT_TIMESTAMP - INTERVAL '7 days') THEN 'Recent'
                WHEN updated_at > (CURRENT_TIMESTAMP - INTERVAL '30 days') THEN 'Current'
                ELSE 'Stale'
            END as data_freshness
        FROM companies 
        WHERE api_status = 'success'
        ORDER BY updated_at DESC
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    def calculate_market_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive market metrics"""
        print("📊 Calculating market metrics...")
        
        # Get fresh data
        df = self.get_companies_data()
        
        if df.empty:
            return {"error": "No data available", "timestamp": datetime.now().isoformat()}
        
        # Calculate metrics
        metrics = {
            "summary": self._calculate_summary_metrics(df),
            "geographic": self._analyze_geographic_distribution(df),
            "categories": self._analyze_categories(df),
            "vintage": self._analyze_vintage_patterns(df),
            "activity": self._analyze_activity_patterns(df),
            "data_quality": self._assess_data_quality(df),
            "market_trends": self._calculate_market_trends(df),
            "metadata": {
                "analysis_timestamp": datetime.now().isoformat(),
                "total_records_analyzed": len(df),
                "data_source": "PostgreSQL companies table"
            }
        }
        
        return metrics
    
    def _calculate_summary_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate high-level summary metrics"""
        return {
            "total_entities": len(df),
            "active_entities": len(df[df['is_active'] == True]) if 'is_active' in df.columns else None,
            "activity_rate": round(len(df[df['is_active'] == True]) / len(df) * 100, 1) if 'is_active' in df.columns else None,
            "unique_categories": df['category'].nunique() if 'category' in df.columns else None,
            "unique_cities": df['city'].nunique() if 'city' in df.columns else None,
            "data_freshness_breakdown": df['data_freshness'].value_counts().to_dict() if 'data_freshness' in df.columns else None
        }
    
    def _analyze_geographic_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze geographic distribution"""
        if 'city' not in df.columns:
            return {"error": "No city data available"}
        
        city_counts = df['city'].value_counts()
        
        # Calculate concentration index (Herfindahl-Hirschman Index)
        if not city_counts.empty:
            shares = city_counts / city_counts.sum()
            hhi = (shares ** 2).sum()
            concentration_level = "High" if hhi > 0.25 else "Medium" if hhi > 0.15 else "Low"
        else:
            hhi = 0
            concentration_level = "No data"
        
        return {
            "top_cities": city_counts.head(10).to_dict(),
            "concentration_index": round(hhi, 3),
            "concentration_level": concentration_level,
            "geographic_spread": len(city_counts),
            "stockholm_dominance": round(city_counts.get('Stockholm', 0) / len(df) * 100, 1) if 'Stockholm' in city_counts else 0
        }
    
    def _analyze_categories(self, df: pd.DataFrame) -> Dict:
        """Analyze category distribution"""
        if 'category' not in df.columns:
            return {"error": "No category data available"}
        
        category_counts = df['category'].value_counts()
        
        # Activity rate by category
        category_activity = {}
        if 'is_active' in df.columns:
            for category in category_counts.index:
                cat_df = df[df['category'] == category]
                active_count = len(cat_df[cat_df['is_active'] == True])
                category_activity[category] = {
                    "total": len(cat_df),
                    "active": active_count,
                    "activity_rate": round(active_count / len(cat_df) * 100, 1) if len(cat_df) > 0 else 0
                }
        
        return {
            "category_distribution": category_counts.to_dict(),
            "category_diversity": len(category_counts),
            "largest_category": category_counts.index[0] if not category_counts.empty else None,
            "largest_category_share": round(category_counts.iloc[0] / len(df) * 100, 1) if not category_counts.empty else 0,
            "activity_by_category": category_activity
        }
    
    def _analyze_vintage_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze registration vintage patterns"""
        if 'registration_date' not in df.columns:
            return {"error": "No registration date data available"}
        
        # Convert to datetime and extract year
        df_vintage = df.copy()
        df_vintage['registration_date'] = pd.to_datetime(df_vintage['registration_date'], errors='coerce')
        df_vintage = df_vintage.dropna(subset=['registration_date'])
        
        if df_vintage.empty:
            return {"error": "No valid registration dates"}
        
        df_vintage['registration_year'] = df_vintage['registration_date'].dt.year
        year_counts = df_vintage['registration_year'].value_counts().sort_index()
        
        # Recent registrations (last 2 years)
        recent_years = [datetime.now().year - 1, datetime.now().year]
        recent_registrations = len(df_vintage[df_vintage['registration_year'].isin(recent_years)])
        
        return {
            "vintage_distribution": year_counts.to_dict(),
            "oldest_registration": int(year_counts.index.min()) if not year_counts.empty else None,
            "newest_registration": int(year_counts.index.max()) if not year_counts.empty else None,
            "peak_registration_year": int(year_counts.idxmax()) if not year_counts.empty else None,
            "peak_year_count": int(year_counts.max()) if not year_counts.empty else None,
            "recent_registrations": recent_registrations,
            "recent_registrations_rate": round(recent_registrations / len(df_vintage) * 100, 1)
        }
    
    def _analyze_activity_patterns(self, df: pd.DataFrame) -> Dict:
        """Analyze activity patterns"""
        if 'is_active' not in df.columns:
            return {"error": "No activity data available"}
        
        active_companies = df[df['is_active'] == True]
        inactive_companies = df[df['is_active'] == False]
        
        return {
            "total_active": len(active_companies),
            "total_inactive": len(inactive_companies),
            "activity_rate": round(len(active_companies) / len(df) * 100, 1),
            "active_by_city": active_companies['city'].value_counts().head(5).to_dict() if 'city' in df.columns else {},
            "active_by_category": active_companies['category'].value_counts().head(5).to_dict() if 'category' in df.columns else {}
        }
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict:
        """Assess data quality and completeness"""
        key_fields = ['name', 'category', 'city', 'is_active', 'registration_date']
        completeness = {}
        
        for field in key_fields:
            if field in df.columns:
                non_null_count = df[field].notna().sum()
                completeness[field] = round(non_null_count / len(df) * 100, 1)
        
        # Overall data quality score

        avg_completeness = sum(completeness.values()) / len(completeness) if completeness else 0
        
        return {
            "field_completeness": completeness,
            "overall_completeness": round(avg_completeness, 1),
            "quality_grade": "Excellent" if avg_completeness >= 90 else "Good" if avg_completeness >= 75 else "Fair" if avg_completeness >= 60 else "Poor"
        }
    
    def _calculate_market_trends(self, df: pd.DataFrame) -> Dict:
        """Calculate market growth and trend indicators"""
        if 'registration_date' not in df.columns:
            return {"error": "No registration date data for trend analysis"}
        
        df_trends = df.copy()
        df_trends['registration_date'] = pd.to_datetime(df_trends['registration_date'], errors='coerce')
        df_trends = df_trends.dropna(subset=['registration_date'])
        
        if len(df_trends) < 5:
            return {"error": "Insufficient data for trend analysis"}
        
        # Group by year and calculate yearly registrations
        df_trends['year'] = df_trends['registration_date'].dt.year
        yearly_counts = df_trends['year'].value_counts().sort_index()
        
        # Calculate growth rate (last 3 years)
        recent_years = yearly_counts.tail(3)
        if len(recent_years) >= 2:
            start_count = recent_years.iloc[0]
            end_count = recent_years.iloc[-1]
            growth_rate = ((end_count - start_count) / start_count * 100) if start_count > 0 else 0
        else:
            growth_rate = 0
        
        # Market maturity indicators
        total_years = yearly_counts.index.max() - yearly_counts.index.min() + 1 if not yearly_counts.empty else 0
        avg_yearly_registrations = yearly_counts.mean() if not yearly_counts.empty else 0
        
        return {
            "yearly_registrations": yearly_counts.to_dict(),
            "recent_growth_rate": round(growth_rate, 1),
            "trend_direction": "Growing" if growth_rate > 5 else "Stable" if growth_rate > -5 else "Declining",
            "market_maturity": "Mature" if total_years > 15 else "Developing" if total_years > 8 else "Emerging",
            "average_yearly_registrations": round(avg_yearly_registrations, 1),
            "market_age_years": total_years
        }
    
    def generate_market_report(self) -> str:
        """Generate a text summary report"""
        metrics = self.calculate_market_metrics()
        
        if "error" in metrics:
            return f"❌ Report Generation Failed: {metrics['error']}"
        
        summary = metrics['summary']
        geographic = metrics['geographic']
        categories = metrics['categories']
        trends = metrics['market_trends']
        
        report = f"""
🏛️ NORDIC PRIVATE CREDIT MARKET REPORT
Generated: {metrics['metadata']['analysis_timestamp'][:19]}
{'='*50}

📊 MARKET OVERVIEW
- Total Entities: {summary['total_entities']}
- Active Entities: {summary['active_entities']} ({summary['activity_rate']}%)
- Market Categories: {summary['unique_categories']}
- Geographic Presence: {summary['unique_cities']} cities

🗺️ GEOGRAPHIC DISTRIBUTION
- Market Concentration: {geographic['concentration_level']} ({geographic['concentration_index']})
- Top Market: {list(geographic['top_cities'].keys())[0] if geographic['top_cities'] else 'N/A'} ({list(geographic['top_cities'].values())[0] if geographic['top_cities'] else 0} entities)
- Geographic Spread: {geographic['geographic_spread']} unique locations

📋 MARKET COMPOSITION
- Largest Category: {categories.get('largest_category', 'N/A')} ({categories.get('largest_category_share', 0)}%)
- Category Diversity: {categories['category_diversity']} distinct categories

📈 MARKET TRENDS
- Growth Trend: {trends.get('trend_direction', 'Unknown')}
- Market Maturity: {trends.get('market_maturity', 'Unknown')}
- Market Age: {trends.get('market_age_years', 0)} years
- Recent Growth Rate: {trends.get('recent_growth_rate', 0)}%

📅 DATA QUALITY
- Overall Completeness: {metrics['data_quality']['overall_completeness']}%
- Quality Grade: {metrics['data_quality']['quality_grade']}
"""
        
        return report


    def save_analytics_results(self, output_file: str = "market_analytics_results.json"):
   
        print("💾 Saving analytics results...")
    
        metrics = self.calculate_market_metrics()
    
    # Save detailed metrics with UTF-8 encoding
        with open(output_file, 'w', encoding='utf-8') as f:
              json.dump(metrics, f, indent=2, default=str, ensure_ascii=False)
    
    # Save summary report with UTF-8 encoding
        report = self.generate_market_report()
        report_file = output_file.replace('.json', '_report.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
             f.write(report)
    
        print(f"✅ Analytics saved to:")
        print(f"   📊 Detailed data: {output_file}")
        print(f"   📄 Summary report: {report_file}")
    
        return metrics

def load_etl_run_info():
    """Load information from the last ETL run"""
    try:
        with open('etl_last_run.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {"error": "No ETL run info found. Run etl_pipeline.py first."}

def main():
    """Main analytics runner"""
    print("📊 NORDIC PRIVATE CREDIT MARKET ANALYTICS")
    print("=" * 45)
    
    try:
        # Check if ETL has been run
        etl_info = load_etl_run_info()
        if "error" in etl_info:
            print("⚠️ Warning: No recent ETL run detected")
            print("💡 Run 'python etl_pipeline.py' first to extract fresh data")
        else:
            print(f"✅ Using data from ETL run: {etl_info['timestamp'][:19]}")
            print(f"📊 Last run processed: {etl_info.get('processed', 0)} companies")
        
        # Initialize analyzer
        analyzer = MarketAnalyzer()
        
        # Generate analytics
        print("\n🔍 Analyzing market data...")
        metrics = analyzer.save_analytics_results()
        
        if "error" in metrics:
            print(f"❌ Analytics failed: {metrics['error']}")
            return
        
        # Display summary
        print("\n" + analyzer.generate_market_report())
        
        # Show key insights
        if 'summary' in metrics:
            summary = metrics['summary']
            print(f"\n🎯 KEY INSIGHTS:")
            print(f"• Market Size: {summary['total_entities']} registered entities")
            print(f"• Activity Level: {summary['activity_rate']}% active")
            print(f"• Market Diversity: {summary['unique_categories']} categories across {summary['unique_cities']} cities")
            
            if 'market_trends' in metrics:
                trends = metrics['market_trends']
                print(f"• Market Trend: {trends.get('trend_direction', 'Unknown')} ({trends.get('recent_growth_rate', 0)}% recent growth)")
                print(f"• Market Stage: {trends.get('market_maturity', 'Unknown')} market")
        
        print(f"\n📁 Detailed results saved for dashboard use")
        
    except Exception as e:
        print(f"❌ Analytics failed: {e}")

if __name__ == "__main__":
    main()