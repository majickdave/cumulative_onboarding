# data_pipeline.py
import pandas as pd
from datetime import datetime

# A script to ingest the exported data and write 3 csv files

def run_data_pipeline(clients, appointments):
    """
    A data pipeline to process client and appointment data."""
    # 1. Client Onboarding: takes clients.csv and creates a list of dates
    if not clients.empty:
        df = clients.copy()

        cols_to_write = ['ClientId', 'DateCreated']
        if df['DateCreated'].dtype == float:
            df['DateCreated'] = pd.to_datetime(df['DateCreated'], unit="ms").dt.strftime("%m/%d/%Y")
        path = 'data/dates.csv'
        df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
        print(f'\nwrote columns: {cols_to_write}, \nto {path}')

    if not appointments.empty:
        # 2. Appointments: takes appointments.csv and creates a table of appointments
        df = appointments.copy()

        if 'Date' not in df.columns:
            df.rename(columns={'DateCreated': 'Date'}, inplace=True)
        cols_to_write = ['Id', 'Date', 'Status', 'CancellationDate', 'Price']
        if df['Date'].dtype == float:
            df['Date'] = pd.to_datetime(df['Date'], unit="ms").dt.strftime("%Y-%m-%d %H:%M")
        if df['CancellationDate'].dtype == float:
            df['CancellationDate'] = pd.to_datetime(df['CancellationDate'], unit="ms").dt.strftime("%Y-%m-%d %H:%M")
        path = 'data/appt_dates.csv'
        df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
        print(f'\nwrote columns: {cols_to_write}, \nto {path}')

        # # 3. Client Summary: takes client_summary.csv and creates a table of clients
        # df = pd.read_csv('data/client_summary.csv')
        # if last_updated.date() == datetime.now().date():
        #     print("Client data is already up to date.")
        # else:
        #     cols_to_write = ['FirstAppointmentDate', 'LastAppointmentDate', 
        #                     'TotalAppointments', 'TotalPaidAmount', 'ClientStatus']
        #     path = 'data/client_summary_totals.csv'
        #     df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
        #     print(f'\nwrote columns: {cols_to_write}, \nto {path}')

if __name__ == "__main__":
    clients = pd.read_csv('data/clients.csv', encoding='latin1')
    appts = pd.read_csv('data/appointments.csv', encoding='latin1')
    run_data_pipeline(clients, appts)

