import pandas as pd
import datetime
import requests
from zoneinfo import ZoneInfo

def refresh_data_in_app(api_key):
    API_KEY = api_key
    BASE = "https://intakeq.com/api/v1/"
    now = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()

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
        df = pd.read_csv('data/appt_dates.csv')
        #last updated
        if 'Date' not in df.columns:
            df.rename(columns={'DateCreated':'Date'}, inplace=True)
        last_updated = pd.to_datetime(df['Date']).sort_values().max()
        
        if last_updated.date() == now:
            print("Appointment data is already up to date.")
        else:
            # create start date from most recent appointment DateCreated
            start_date = (last_updated - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = now.strftime('%Y-%m-%d')

            print(f"Fetching appointments updated since {start_date} to {end_date}")
            response = fetch_appointments_scheduled_between(start_date, end_date)
            new_data = pd.DataFrame(response)
            if not new_data.empty:
                # Keep only rows in new_data whose 'Id' is not already in df
                new_unique = new_data[~new_data['Id'].isin(df['Id'])]
                print(f"{len(new_unique)} new appointments")

                # Concatenate
                new_appts = pd.concat([df, new_unique], ignore_index=True)

                print("Appointments data updated.")
                return new_appts
            else:
                print("No new appointments found.")
                return None


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
        df = pd.read_csv('data/dates.csv')
        # check most recent date created
        last_updated = pd.to_datetime(df['DateCreated']).sort_values().max()
        if last_updated.date() == now:
            print("Client data is already up to date.")
        else:
            # create start date from most recent appointment DateCreated
            start_date = (pd.to_datetime(df['DateCreated']).sort_values().max() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = now.strftime('%Y-%m-%d')

            print(f"Fetching clients created between {start_date} and {end_date}")
            response = fetch_clients_created_between(start_date, end_date)
            new_data = pd.DataFrame(response)
            if not new_data.empty:
                # Keep only rows in new_data whose 'ClientId' is not already in df
                new_unique = new_data[~new_data['ClientId'].isin(df['ClientId'])]
                print(f"{len(new_unique)} new clients")

                # Concatenate
                new_clients = pd.concat([df, new_unique], ignore_index=True)
                
                print("Clients data updated.")
                return new_clients
            else:
                print("No new clients found.")
                return None

    new_clients = update_clients_data()
    new_appointments = update_appointments_data()
    
    return new_clients, new_appointments