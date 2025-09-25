# file: export_intakeq_appointments.py
import os, time
import pandas as pd
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
path = '/Users/davidsamuel/Projects/cumulative_onboarding/data/appointments.csv'
pd.DataFrame(appts).to_csv(path, index=False)
print(f'\ndownloaded data to {path}')

# get clients
path = '/Users/davidsamuel/Projects/cumulative_onboarding/data/clients.csv'
clients = fetch_all_clients(date_created_start=None, date_created_end=None)
pd.DataFrame(clients).to_csv(path)
print(f'\ndownloaded data to {path}')

