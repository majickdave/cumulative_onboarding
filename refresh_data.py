import pandas as pd
from datetime import datetime
import requests
from dotenv import load_dotenv
import os
from zoneinfo import ZoneInfo

load_dotenv()

API_KEY = os.getenv("INTAKEQ_API_KEY")
BASE = "https://intakeq.com/api/v1/"
TODAY = datetime.now(ZoneInfo("America/Los_Angeles")).date()

def fetch_appointments_scheduled_between(start_date, end_date):
    """
    Fetch appointments scheduled between start_date and end_date
    
    https://support.intakeq.com/article/204-intakeq-appointments-api
    """
    base_url = BASE + 'appointments'
    # create a session
    session = requests.Session()
    # add api key to headers
    session.headers.update({
        "X-Auth-Key": API_KEY,
        "User-Agent": "IntakeQ-API-Client/1.0"
    })
    # make request
    response = session.get(
        f"{base_url}?&startDate={start_date}&endDate={end_date}&deletedOnly=false"
        )
    # close session
    session.close()
    return response.json()

def update_appointments_data():
    # load existing data
    df = pd.read_csv('/Users/davidsamuel/Projects/cumulative_onboarding/data/appointments.csv')
    #last updated
    last_updated = pd.to_datetime(df['DateCreated'], unit='ms').max().tz_localize('America/Los_Angeles')
    if datetime.now(ZoneInfo("America/Los_Angeles")) > last_updated:
        # create start date from most recent appointment DateCreated
        start_date = last_updated.strftime('%Y-%m-%d')
        end_date = (TODAY + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"Fetching appointments updated since {start_date} to {end_date}")
        response = fetch_appointments_scheduled_between(start_date, end_date)
        new_data = pd.DataFrame(response)
        if not new_data.dropna().empty:
            # Keep only rows in new_data whose 'Id' is not already in df
            new_unique = new_data[~new_data['Id'].isin(df['Id'])]
            print(f"{len(new_unique)} new appointments")

            # Concatenate
            if not new_unique.dropna().empty:
                print("No new unique appointments to add.")
                new_appts = pd.concat([df, new_unique], ignore_index=True)
                new_appts.to_csv('/Users/davidsamuel/Projects/cumulative_onboarding/data/appointments.csv', index=False)
                print("Appointments data updated.")
            else:
                print("No new appointments found.")
        else:
            print("No new appointments found.")

def fetch_clients_created_between(start_date, end_date):
    """
    Fetch clients created between start_date and end_date

    https://support.intakeq.com/article/251-intakeq-client-api
    """
    base_url = BASE + 'clients'
    # create a session
    session = requests.Session()
    # add api key to headers
    session.headers.update({
        "X-Auth-Key": API_KEY,
        "User-Agent": "IntakeQ-API-Client/1.0"
    })
    # make request
    response = session.get(
        f"{base_url}?&dateCreatedStart={start_date}&dateCreatedEnd={end_date}&deletedOnly=false&includeProfile=true"
        )
    # close session
    session.close()
    return response.json()

def update_clients_data():
    # load existing data
    df = pd.read_csv('/Users/davidsamuel/Projects/cumulative_onboarding/data/clients.csv')
    # check most recent date created
    last_updated = pd.to_datetime(df['DateCreated'], unit='ms').max().tz_localize('America/Los_Angeles')
    if datetime.now(ZoneInfo("America/Los_Angeles")) > last_updated:
        # create start date from most recent appointment DateCreated
        start_date = last_updated.strftime('%Y-%m-%d')
        end_date = (TODAY + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        print(f"Fetching clients created between {start_date} and {end_date}")
        response = fetch_clients_created_between(start_date, end_date)
        new_data = pd.DataFrame(response)
        if not new_data.dropna().empty:
            # Keep only rows in new_data whose 'ClientId' is not already in df
            new_unique = new_data[~new_data['ClientId'].isin(df['ClientId'])]
            print(f"{len(new_unique)} new clients")

            if not new_unique.dropna().empty:
                # Concatenate
                new_clients = pd.concat([df, new_unique], ignore_index=True)
                new_clients.to_csv('/Users/davidsamuel/Projects/cumulative_onboarding/data/clients.csv', index=False)
                print("Clients data updated.")
            else:
                print("No new clients found.")
        else:
            print("No new clients found.")

if __name__ == "__main__":
    update_appointments_data()
    update_clients_data()