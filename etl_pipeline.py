"""
Nordic Private Credit ETL Pipeline - Simple Structured Approach
File 1: etl_pipeline.py (Your main extraction file, cleaned up)
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import psycopg2.sql as sql
from datetime import datetime
import hashlib
import json
import os
import requests
import time
from typing import Dict, List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Production credentials and configuration
CLIENT_ID = "O6WntWGsrVnHPzAN_1ZqQxCRqlIa"
CLIENT_SECRET = "sUdVhkHba3qI71tob2WNreiAgzMa"
TOKEN_URL = "https://portal.api.bolagsverket.se/oauth2/token"
API_URL = "https://gw.api.bolagsverket.se/vardefulla-datamangder/v1/organisationer"

class DatabaseConfig:
    """Database configuration - simple version"""
    
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

class BolagsverketExtractor:
    """Simplified Bolagsverket API extractor"""
    
    def __init__(self, delay_between_requests: float = 0.02, max_workers: int = 12):
        self.api_url = API_URL
        self.delay = delay_between_requests
        self.max_workers = max_workers
        self.token = None
        self.token_expiry = None
        self.token_lock = threading.Lock()
        self.session_local = threading.local()
    
    def get_session(self):
        """Get thread-local session"""
        if not hasattr(self.session_local, 'session'):
            self.session_local.session = requests.Session()
            self.session_local.session.headers.update({
                'Accept': 'application/json',
                'User-Agent': 'Nordic-Private-Credit-Tracker/1.0'
            })
        return self.session_local.session
    
    def get_access_token(self):
        """Get OAuth2 access token (thread-safe)"""
        with self.token_lock:
            if self.token and self.token_expiry and time.time() < self.token_expiry:
                return self.token
            
            payload = {
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": "vardefulla-datamangder:read vardefulla-datamangder:ping"
            }
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
            print("Getting access token...")
            resp = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=15)
            
            if not resp.ok:
                logger.error(f"Token request failed: {resp.status_code} - {resp.text}")
                resp.raise_for_status()
            
            token_data = resp.json()
            self.token = token_data["access_token"]
            self.token_expiry = time.time() + (token_data.get('expires_in', 3600) - 300)
            
            print("✅ Access token obtained")
            return self.token
    
    def query_single_organisation(self, org_number: str) -> Dict:
        """Query a single organisation"""
        token = self.get_access_token()
        session = self.get_session()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        payload = {"identitetsbeteckning": str(org_number).strip()}
        
        try:
            response = session.post(self.api_url, json=payload, headers=headers, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                return {"org_number": org_number, "status": "success", "data": data}
            else:
                return {
                    "org_number": org_number,
                    "status": "error",
                    "error_code": response.status_code,
                    "error_message": response.text[:200]
                }
        except Exception as e:
            return {
                "org_number": org_number,
                "status": "exception",
                "error_message": str(e)[:200]
            }
    
    def parse_organisation_data(self, api_response: Dict) -> Dict:
        """Parse API response - simplified version"""
        if api_response["status"] != "success":
            return {
                "org_number": api_response["org_number"],
                "api_status": api_response["status"],
                "error": api_response.get("error_message", "")
            }
        
        try:
            data = api_response["data"]
            organisations = data.get("organisationer", [])
            
            if not organisations:
                return {
                    "org_number": api_response["org_number"],
                    "api_status": "no_data",
                    "error": "No organisation data returned"
                }
            
            org = organisations[0]
            parsed = {
                "org_number": api_response["org_number"],
                "api_status": "success",
                "query_timestamp": pd.Timestamp.now().isoformat(),
                "is_deregistered": org.get("avregistreradOrganisation") is not None
            }
            
            # Extract key information efficiently
            juridisk_form = org.get("juridiskForm", {})
            parsed.update({
                "legal_form_code": juridisk_form.get("kod"),
                "legal_form_description": juridisk_form.get("klartext")
            })
            
            # Organization names
            org_names_data = org.get("organisationsnamn", {})
            if org_names_data and "organisationsnamnLista" in org_names_data:
                namn_lista = org_names_data.get("organisationsnamnLista", [])
                if namn_lista:
                    parsed["organisation_name"] = namn_lista[0].get("namn")
            
            # Address
            address_data = org.get("postadressOrganisation", {})
            if address_data and "postadress" in address_data:
                address = address_data.get("postadress", {})
                parsed.update({
                    "street_address": address.get("utdelningsadress"),
                    "city": address.get("postort"),
                    "postal_code": address.get("postnummer"),
                    "country": address.get("land")
                })
            
            # SNI codes
            naringsgren_data = org.get("naringsgrenOrganisation", {})
            if naringsgren_data and "sni" in naringsgren_data:
                sni_list = naringsgren_data.get("sni", [])
                if sni_list:
                    main_sni = next((sni for sni in sni_list 
                                   if sni.get("kod", "").strip() and sni.get("kod") != "     "), None)
                    if main_sni:
                        parsed.update({
                            "sni_code": main_sni.get("kod"),
                            "sni_description": main_sni.get("klartext")
                        })
            
            # Registration and activity
            org_datum = org.get("organisationsdatum", {})
            parsed["registration_date"] = org_datum.get("registreringsdatum")
            
            verksam_org = org.get("verksamOrganisation", {})
            parsed["is_active"] = verksam_org.get("kod") == "JA"
            
            return parsed
            
        except Exception as e:
            return {
                "org_number": api_response["org_number"],
                "api_status": "parse_error",
                "error": f"Parse error: {str(e)[:100]}"
            }
    
    def process_organization_batch(self, org_batch: List[str]) -> List[Dict]:
        """Process a batch of organizations"""
        results = []
        for org_number in org_batch:
            api_response = self.query_single_organisation(org_number)
            parsed_data = self.parse_organisation_data(api_response)
            results.append(parsed_data)
            time.sleep(self.delay)
        return results
    
    def extract_batch_data(self, org_numbers: List[str]) -> pd.DataFrame:
        """Extract data with concurrent processing"""
        if not self.token:
            self.get_access_token()
        
        total_count = len(org_numbers)
        print(f"🚀 Processing {total_count} organisations with {self.max_workers} concurrent threads...")
        
        # Split work into batches
        batch_size = max(1, total_count // self.max_workers)
        org_batches = [org_numbers[i:i + batch_size] for i in range(0, total_count, batch_size)]
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_batch = {
                executor.submit(self.process_organization_batch, batch): batch 
                for batch in org_batches
            }
            
            completed_batches = 0
            for future in as_completed(future_to_batch):
                batch_results = future.result()
                results.extend(batch_results)
                completed_batches += 1
                
                success_count = len([r for r in results if r["api_status"] == "success"])
                processed_count = len(results)
                print(f"📊 {processed_count}/{total_count} processed ({success_count} successful) - Batch {completed_batches}/{len(org_batches)}")
        
        df = pd.DataFrame(results)
        success_count = len(df[df['api_status'] == 'success'])
        print(f"✅ EXTRACTION COMPLETE! {success_count}/{total_count} successful ({success_count/total_count*100:.1f}%)")
        
        return df

class DatabaseOperations:
    """Handle database operations"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
    
    def setup_database(self):
        """Setup database schema"""
        conn = self.db_config.get_connection()
        cur = conn.cursor()
        
        try:
            # Enable UUID extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
            
            # Drop and recreate for clean schema
            cur.execute("DROP TABLE IF EXISTS audit_log CASCADE;")
            cur.execute("DROP TABLE IF EXISTS etl_runs CASCADE;")
            cur.execute("DROP TABLE IF EXISTS companies CASCADE;")
            cur.execute("DROP VIEW IF EXISTS dashboard_companies CASCADE;")
            
            # Create companies table
            cur.execute("""
            CREATE TABLE companies (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                corporate_id VARCHAR(20) UNIQUE NOT NULL,
                name TEXT,
                category TEXT,
                api_status VARCHAR(50),
                is_active BOOLEAN,
                is_deregistered BOOLEAN,
                registration_date DATE,
                street_address TEXT,
                city VARCHAR(200),
                postal_code VARCHAR(20),
                country VARCHAR(200),
                sni_code VARCHAR(20),
                sni_description TEXT,
                legal_form_code VARCHAR(20),
                legal_form_description TEXT,
                query_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX idx_companies_corporate_id ON companies(corporate_id);")
            cur.execute("CREATE INDEX idx_companies_category ON companies(category);")
            cur.execute("CREATE INDEX idx_companies_city ON companies(city);")
            
            # ETL runs table
            cur.execute("""
            CREATE TABLE etl_runs (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                records_processed INTEGER,
                records_inserted INTEGER,
                success BOOLEAN,
                execution_time_seconds DECIMAL(10,2)
            );
            """)
            
            # Dashboard view
            cur.execute("""
            CREATE OR REPLACE VIEW dashboard_companies AS
            SELECT 
                corporate_id, name, category, city, postal_code,
                sni_code, sni_description, legal_form_description,
                is_active, registration_date, updated_at
            FROM companies 
            WHERE api_status = 'success'
            ORDER BY updated_at DESC;
            """)
            
            conn.commit()
            print("✅ Database schema created successfully")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Database setup error: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def safe_value(self, value, value_type='str'):
        """Safely convert values"""
        if pd.isna(value) or value is None or str(value).lower() == 'nan':
            return None
        
        if value_type == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ['true', '1', 'yes', 'ja', 'y']
            return bool(value)
        elif value_type == 'str':
            result = str(value).strip()
            return result if result else None
        
        return value
    
    def bulk_upsert_companies(self, df: pd.DataFrame) -> Dict:
        """Bulk upsert companies"""
        # Remove duplicates
        df_clean = df.drop_duplicates(subset=['CorporateID_Clean'], keep='first')
        duplicate_count = len(df) - len(df_clean)
        
        if duplicate_count > 0:
            print(f"🔧 Removed {duplicate_count} duplicate corporate IDs")
        
        # Prepare data
        data_rows = []
        for _, row in df_clean.iterrows():
            reg_date = row.get('registration_date')
            if pd.notna(reg_date) and isinstance(reg_date, str) and len(reg_date) >= 10:
                reg_date = reg_date[:10]
            else:
                reg_date = None
            
            row_data = (
                str(row['CorporateID_Clean']),
                self.safe_value(row.get('organisation_name'), 'str'),
                self.safe_value(row.get('Category'), 'str'),
                self.safe_value(row.get('api_status'), 'str'),
                self.safe_value(row.get('is_active'), 'bool'),
                self.safe_value(row.get('is_deregistered'), 'bool'),
                reg_date,
                self.safe_value(row.get('street_address'), 'str'),
                self.safe_value(row.get('city'), 'str'),
                self.safe_value(row.get('postal_code'), 'str'),
                self.safe_value(row.get('country'), 'str'),
                self.safe_value(row.get('sni_code'), 'str'),
                self.safe_value(row.get('sni_description'), 'str'),
                self.safe_value(row.get('legal_form_code'), 'str'),
                self.safe_value(row.get('legal_form_description'), 'str'),
                pd.to_datetime(row.get('query_timestamp')) if pd.notna(row.get('query_timestamp')) else None
            )
            data_rows.append(row_data)
        
        conn = self.db_config.get_connection()
        cur = conn.cursor()
        
        upsert_query = """
        INSERT INTO companies (
            corporate_id, name, category, api_status, is_active, is_deregistered,
            registration_date, street_address, city, postal_code, country,
            sni_code, sni_description, legal_form_code, legal_form_description, query_timestamp
        ) VALUES %s
        ON CONFLICT (corporate_id) DO UPDATE SET
            name = EXCLUDED.name,
            category = EXCLUDED.category,
            api_status = EXCLUDED.api_status,
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
        """
        
        print("💾 Executing bulk upsert...")
        execute_values(cur, upsert_query, data_rows, page_size=1000)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            'processed': len(data_rows),
            'duplicates_removed': duplicate_count
        }

def load_finansinspektionen_ids(file_path: str = "bolagsverket_corporate_ids.txt") -> List[str]:
    """Load Corporate IDs"""
    try:
        with open(file_path, 'r') as f:
            org_ids = [line.strip() for line in f if line.strip()]
        print(f"📋 Loaded {len(org_ids)} organisation IDs")
        return org_ids
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return []

def merge_with_finansinspektionen_data(bolagsverket_df: pd.DataFrame, fi_data_path: str) -> pd.DataFrame:
    """Merge with Finansinspektionen data"""
    try:
        fi_df = pd.read_csv(fi_data_path, encoding='utf-8-sig')
        print(f"📊 Loaded Finansinspektionen data: {len(fi_df)} companies")
        
        # Normalize IDs
        fi_df['CorporateID_Clean'] = pd.to_numeric(fi_df['CorporateID_Clean'], errors='coerce').fillna(0).astype(int).astype(str)
        bolagsverket_df['org_number'] = bolagsverket_df['org_number'].astype(str).str.replace('-', '').str.strip()
        
        # Merge
        merged_df = fi_df.merge(
            bolagsverket_df,
            left_on='CorporateID_Clean',
            right_on='org_number',
            how='left',
            suffixes=('', '_bolagsverket')
        )
        
        print(f"✅ Merged: {len(merged_df)} total, {merged_df['api_status'].notna().sum()} with Bolagsverket data")
        return merged_df
        
    except Exception as e:
        print(f"❌ Error merging data: {e}")
        return bolagsverket_df

def main():
    """Main ETL pipeline"""
    print("⚡ NORDIC PRIVATE CREDIT TRACKER - ETL PIPELINE")
    print("=" * 55)
    
    start_time = datetime.now()
    
    try:
        # Initialize components
        extractor = BolagsverketExtractor(delay_between_requests=0.02, max_workers=12)
        db_ops = DatabaseOperations()
        
        # Setup database
        db_ops.setup_database()
        
        # Load organization IDs
        org_ids = load_finansinspektionen_ids()
        if not org_ids:
            print("❌ No organization IDs found")
            return
        
        # Extract data
        bolagsverket_df = extractor.extract_batch_data(org_ids)
        
        # Merge with Finansinspektionen data
        merged_df = merge_with_finansinspektionen_data(bolagsverket_df, "fi_nordic_cleaned_utf8_bom.csv")
        
        # Load to database
        result = db_ops.bulk_upsert_companies(merged_df)
        
        # Log run
        execution_time = (datetime.now() - start_time).total_seconds()
        
        print(f"\n⚡ ETL PIPELINE COMPLETE!")
        print(f"📊 Processed: {result['processed']} companies")
        print(f"⏱️ Total time: {execution_time:.1f} seconds")
        
        # Save run info for analytics
        run_info = {
            'timestamp': datetime.now().isoformat(),
            'processed': result['processed'],
            'execution_time': execution_time,
            'success': True
        }
        
        with open('etl_last_run.json', 'w') as f:
            json.dump(run_info, f, indent=2)
        
        print("📄 Run info saved to etl_last_run.json")
        
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        
        # Save error info
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'success': False
        }
        
        with open('etl_last_run.json', 'w') as f:
            json.dump(error_info, f, indent=2)

if __name__ == "__main__":
    main()