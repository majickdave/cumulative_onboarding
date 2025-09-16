import pandas as pd

# A script to ingest the exported data and write 3 csv files


# 1. Client Onboarding: takes clients.csv and creates a list of dates
df = pd.read_csv('data/clients.csv', encoding='latin1')
cols_to_write = ['DateCreated']
path = 'data/dates.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

# 2. Appointments: takes appointments.csv and creates a table of appointments
df = pd.read_csv('data/appointments.csv', encoding='latin1')
cols_to_write = ['Date', 'Status', 'CancellationDate', 'PaidAmount']
path = 'data/appt_dates.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

# 3. Client Summary: takes client_summary.csv and creates a table of clients
df = pd.read_csv('data/client_summary.csv')
cols_to_write = ['FirstAppointmentDate', 'LastAppointmentDate', 
                 'TotalAppointments', 'TotalPaidAmount', 'ClientStatus']
path = 'data/client_summary_totals.csv'
df[cols_to_write].to_csv(path, encoding='utf-8', index=False)
print(f'\nwrote columns: {cols_to_write}, \nto {path}')

