# data_pipeline.py
import pandas as pd
from datetime import datetime

# A script to ingest the exported data and write 3 csv files

def run_data_pipeline(clients, appts, run_live=True):
    """
    A data pipeline to process client and appointment data."""
    # 1. Client Onboarding: takes clients.csv and creates a list of dates
    if not clients.empty:
        cols_to_write = ['ClientId', 'DateCreated']
        if clients['DateCreated'].dtype == float:
            clients['DateCreated'] = pd.to_datetime(clients['DateCreated'], unit="ms").dt.strftime("%m/%d/%Y")

        clients = clients[cols_to_write]


    if not appts.empty:
        # 2. Appointments: takes appointments.csv and creates a table of appointments

        if 'Date' not in appts.columns:
            appts.rename(columns={'DateCreated': 'Date'}, inplace=True)
        cols_to_write = ['Id', 'Date', 'Status', 'CancellationDate', 'Price', 'ServiceId', 'ClientId']
        if appts['Date'].dtype == float:
            appts['Date'] = pd.to_datetime(appts['Date'], unit="ms").dt.strftime("%Y-%m-%d %H:%M")
        if appts['CancellationDate'].dtype == float:
            appts['CancellationDate'] = pd.to_datetime(appts['CancellationDate'], unit="ms").dt.strftime("%Y-%m-%d %H:%M")
        
        appts = appts[cols_to_write]

    return clients, appts

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
    clients = pd.read_csv('data/clients.csv')
    appts = pd.read_csv('data/appointments.csv')
    clients, appts = run_data_pipeline(clients, appts, run_live=False)

    path = 'data/dates.csv'
    clients = clients.sort_values(by='ClientId', ascending=False)
    clients.to_csv(path, encoding='utf-8', index=False)
    print(f'\nwrote to {path}')

    path = 'data/appt_dates.csv'
    appts = appts.sort_values(by='Id', ascending=False)
    appts.to_csv(path, encoding='utf-8', index=False)
    print(f'\nwrote to {path}')



