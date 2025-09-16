# file: export_intakeq_appointments.py
import csv, os, time, requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from rate_limit_utils import create_api_session, make_rate_limited_request

# load env variables to environment
load_dotenv()

API_KEY = os.environ.get("INTAKEQ_API_KEY")
BASE = "https://intakeq.com/api/v1/"

# Create session with rate limiting
session, rate_limiter = create_api_session(
    API_KEY,
    base_delay=1.0,      # Start with 1 second delay
    max_delay=60.0,      # Max 60 seconds delay
    max_retries=15       # Allow more retries for large datasets
)

def get_max_last_updated_date_from_csv(fname):
    """Read existing appointments CSV and return the maximum DateCreated value"""
    if not os.path.exists(fname):
        print(f"No existing file {fname} found")
        return None
    
    try:
        # Read the CSV file
        df = pd.read_csv(fname)
        
        # Check if DateCreated column exists
        if 'LastUpdateDate' not in df.columns:
            print("LastUpdateDate column not found in CSV")
            return None
        
        # Convert DateCreated to numeric, handling any non-numeric values
        df['LastUpdateDate'] = pd.to_numeric(df['LastUpdateDate'], errors='coerce')
        
        # Remove any NaN values
        df = df.dropna(subset=['LastUpdateDate'])
        
        if df.empty:
            print("No valid LastUpdateDate values found in CSV")
            return None
        
        # Find the maximum DateCreated value
        max_timestamp = df['LastUpdateDate'].max()
        
        # Check if the timestamp is reasonable (not too old or too new)
        if max_timestamp < 1000000000000:  # Less than year 2001
            print(f"Warning: LastUpdateDatetimestamp {max_timestamp} seems too old")
            return None
        elif max_timestamp > 2000000000000:  # More than year 2033
            print(f"Warning: LastUpdateDate timestamp {max_timestamp} seems too new")
            return None
        
        # Convert Unix timestamp (milliseconds) to datetime
        max_date = datetime.fromtimestamp(max_timestamp / 1000)
        
        # Format as yyyy-MM-dd for the API
        formatted_date = max_date.strftime('%Y-%m-%d')
        
        print(f"Found max LastUpdateDate: {max_timestamp} ({formatted_date})")
        return formatted_date
        
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

def get_existing_appointment_ids(fname):
    """Read existing appointments from CSV file and return set of existing AppointmentIds"""
    existing_ids = set()
    if os.path.exists(fname):
        try:
            # Use pandas for better performance
            df = pd.read_csv(fname)
            if 'AppointmentId' in df.columns:
                # Filter out empty/NaN values
                valid_ids = df['AppointmentId'].dropna()
                existing_ids = set(valid_ids.astype(str))
                print(f"Found {len(existing_ids)} existing appointment IDs in {fname}")
            else:
                print("AppointmentId column not found in CSV")
        except Exception as e:
            print(f"Error reading existing appointments: {e}")
    return existing_ids

def fetch_all_appointments(api_url=BASE + 'appointments', include_profile=True, search=None, date_start=None, 
                          date_end=None, status=None, client_id=None, 
                          date_created_start=None, date_created_end=None,
                          updated_since=None, date_updated_end=None,
                          deleted_only=None):
    """
    Fetch all appointments with pagination and optional filters
    
    Args:
        include_profile (bool): Include profile information
        search (str): Search string to filter appointments
        date_start (str): Start date for appointment date filter (yyyy-MM-dd), defaults to latest date from appointments.csv
        date_end (str): End date for appointment date filter (yyyy-MM-dd), defaults to 3 months from today
        status (str): Appointment status filter
        client_id (str): Client ID filter
        date_created_start (str): Start date for creation date filter (yyyy-MM-dd)
        date_created_end (str): End date for creation date filter (yyyy-MM-dd)
        updated_since (str): Start date for update date filter (yyyy-MM-dd) - uses updatedSince API parameter
        date_updated_end (str): End date for update date filter (yyyy-MM-dd)
        deleted_only (bool): Only return deleted appointments
    """
    page = 1
    out = []
    
    while True:
        try:
            params = {"page": page}
            
            if include_profile:
                params["includeProfile"] = "true"
            
            if search:
                params["search"] = search
                
            if date_start:
                params["dateStart"] = date_start
                
            if date_end:
                params["dateEnd"] = date_end
                
            if status:
                params["status"] = status
                
            if client_id:
                params["clientId"] = client_id
                
            if date_created_start:
                params["dateCreatedStart"] = date_created_start
                
            if date_created_end:
                params["dateCreatedEnd"] = date_created_end
                
            if updated_since:
                params["updatedSince"] = updated_since
                
            if date_updated_end:
                params["dateUpdatedEnd"] = date_updated_end
                
            if deleted_only is not None:
                params["deletedOnly"] = str(deleted_only).lower()
            
            # Debug: Show parameters being used
            if page == 1:
                print(f"API parameters: {params}")
            
            # Use rate-limited request with automatic retry logic
            resp = make_rate_limited_request(
                session, "GET", api_url, rate_limiter,
                params=params, timeout=60
            )
            
            batch = resp.json()
            
            if not batch:
                break
                
            out.extend(batch)
            print(f"Fetched page {page} with {len(batch)} appointments", end='\r', flush=True)
            
            # Add a small delay between successful requests to be respectful
            time.sleep(0.5)
            page += 1
            
        except Exception as e:
            print(f"Unexpected error on page {page}: {e}")
            break
    
    return out

# set base url

def fetch_all_clients(api_url=BASE + "clients", include_profile=True, search=None, date_created_start=None, 
                     date_created_end=None, custom_fields=None, date_updated_start=None,
                     date_updated_end=None, external_client_id=None, deleted_only=None):
    """
    Fetch all clients with pagination and optional filters
    
    Args:
        include_profile (bool): Include profile information
        search (str): Search string to filter clients
        date_created_start (str): Start date for creation date filter (yyyy-MM-dd)
        date_created_end (str): End date for creation date filter (yyyy-MM-dd)
        custom_fields (dict): Custom field filters {fieldId: value}
        date_updated_start (str): Start date for update date filter (yyyy-MM-dd)
        date_updated_end (str): End date for update date filter (yyyy-MM-dd)
        external_client_id (str): External client ID filter
        deleted_only (bool): Only return deleted clients
    """
    page = 1
    out = []
    
    while True:
        try:
            params = {"page": page}
            
            if include_profile:
                params["includeProfile"] = "true"
            
            if search:
                params["search"] = search
                
            if date_created_start:
                params["dateCreatedStart"] = date_created_start
                
            if date_created_end:
                params["dateCreatedEnd"] = date_created_end
                
            if date_updated_start:
                params["dateUpdatedStart"] = date_updated_start
                
            if date_updated_end:
                params["dateUpdatedEnd"] = date_updated_end
                
            if external_client_id:
                params["externalClientId"] = external_client_id
                
            if deleted_only is not None:
                params["deletedOnly"] = str(deleted_only).lower()
                
            if custom_fields:
                for field_id, value in custom_fields.items():
                    params[f"custom.{field_id}"] = value
            
            # Use rate-limited request with automatic retry logic
            resp = make_rate_limited_request(
                session, "GET", api_url, rate_limiter,
                params=params, timeout=60
            )
            
            batch = resp.json()
            
            if not batch:
                break
                
            out.extend(batch)
            print(f"Fetched page {page} with {len(batch)} clients", end='\r', flush=True)
            
            # Add a small delay between successful requests to be respectful
            time.sleep(0.5)
            page += 1
            
        except Exception as e:
            print(f"Unexpected error on page {page}: {e}")
            break
    
    return out


## Download data from API
# get appointments
appts = fetch_all_appointments(include_profile=True, search=None, date_start=None, 
                          date_end=None)
path = 'data/appointments_test.csv'
pd.DataFrame(appts).to_csv(path, index=False)
print(f'\ndownloaded data to {path}')

# get clients
path = 'data/clients_test.csv'
clients = fetch_all_clients(date_created_start=None, date_created_end=None)
pd.DataFrame(clients).to_csv(path)
print(f'\ndownloaded data to {path}')


# process and deidentify data for dashboard
# 1. Client Onboarding: takes clients.csv and creates a list of dates
df = pd.read_csv('data/clients_test.csv', encoding='latin1')
cols_to_write = ['DateCreated']
path = 'data/dates_test.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

# 2. Appointments: takes appointments.csv and creates a table of appointments
df = pd.read_csv('data/appointments_test.csv', encoding='latin1')
cols_to_write = ['DateCreated', 'Status', 'CancellationDate', 'Price']
path = 'data/appt_dates_test.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

# 3. Client Summary: takes client_summary.csv and creates a table of clients
df = pd.read_csv('data/client_summary.csv')
cols_to_write = ['FirstAppointmentDate', 'LastAppointmentDate', 
                 'TotalAppointments', 'TotalPaidAmount', 'ClientStatus']
path = 'data/client_summary_totals_test.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

