import gspread
import pandas as pd
import sqlite3
from oauth2client.service_account import ServiceAccountCredentials

# define the scope
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name('../ordinal-tractor-350015-2f60f73c6be5.json', scope)

# authorize the client sheet
client = gspread.authorize(creds)

# column indices
ss_aecscrvcs_recommend = 240
e_total_employees = 57
e_total_employees_baseline = 5
cd_age = 4
cd_strata = 3
cd_gender = 1
client_location = 5
unique_id = 0
bd_location = 6
total_monthly_revenue_endline = 106
total_monthly_revenue_baseline = 33

# connect to sqlite DB
con = sqlite3.connect('../DB/inkomoko.db')
cur = con.cursor()


def get_records():
    # get the instance of the Spreadsheet
    end_line_sheet = client.open('Kak_endline_Test_revised')
    base_line_sheet = client.open('Kak_Baseline_Test_revised')

    # get the second sheet of the Spreadsheet
    end_line_sheet_instance = end_line_sheet.get_worksheet(0)
    base_line_sheet_instance = base_line_sheet.get_worksheet(0)

    # get all the records of the data
    end_line_records_data = end_line_sheet_instance.get_values()
    base_line_records_data = base_line_sheet_instance.get_values()

    list_of_records = [end_line_records_data, base_line_records_data]
    return list_of_records


# task1 -> Average NPS score task
def populate_nfs_data(records_df):
    satisfaction_values = records_df[ss_aecscrvcs_recommend].tolist()
    not_classified_count = 0
    detractors_count = 0
    passives_count = 0
    promoters_count = 0
    header = True

    for satisfaction_value in satisfaction_values:
        if header:
            header = False
        else:
            if satisfaction_value == '':
                not_classified_count += 1
            elif 8 < int(satisfaction_value) <= 10:
                promoters_count += 1
            elif 6 < int(satisfaction_value) <= 8:
                passives_count += 1
            elif int(satisfaction_value) >= 0:
                detractors_count += 1
    # add rows to db
    cur.execute("INSERT INTO nfs_data VALUES (?, ?)", ('promoters', promoters_count))
    cur.execute("INSERT INTO nfs_data VALUES (?, ?)", ('passives', passives_count))
    cur.execute("INSERT INTO nfs_data VALUES (?, ?)", ('detractors', detractors_count))
    cur.execute("INSERT INTO nfs_data VALUES (?, ?)", ('not_classified', not_classified_count))
    con.commit()


# task 2 -> Total number of employees by
def extract_total_employees_from_endline(endline_records_df):
    header = True
    for i in range(len(endline_records_df)):
        if header:
            header = False
        else:
            cur.execute("INSERT INTO endline_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (endline_records_df.loc[i, unique_id],
                         endline_records_df.loc[i, client_location],
                         endline_records_df.loc[i, cd_gender],
                         endline_records_df.loc[i, cd_strata],
                         endline_records_df.loc[i, cd_age],
                         endline_records_df.loc[i, bd_location],
                         endline_records_df.loc[i, total_monthly_revenue_endline],
                         endline_records_df.loc[i, e_total_employees]))
    con.commit()
    con.close()


def extract_total_employees_from_baseline(baseline_records_df):
    header = True
    for i in range(len(baseline_records_df)):
        if header:
            header = False
        else:
            cur.execute("INSERT INTO baseline_rows VALUES (?, ?, ?)",
                        (baseline_records_df.loc[i, unique_id],
                         baseline_records_df.loc[i, total_monthly_revenue_baseline],
                         baseline_records_df.loc[i, e_total_employees_baseline]))

    con.commit()


def create_db_tables():
    cur.execute('''CREATE TABLE nfs_data
                   (category text, count text)''')

    cur.execute('''CREATE TABLE endline_rows
                   (UID text, client_location text, client_gender text, client_strata text, client_age text, 
                   business_location text, total_monthly_revenue_endline text, total_employees_endline text)''')

    cur.execute('''CREATE TABLE baseline_rows
                       (UID text, total_monthly_revenue_baseline text, total_employees_baseline text)''')
    con.commit()


def main():
    # create_db_tables()
    list_of_records = get_records()
    endline_records_df = pd.DataFrame.from_dict(list_of_records[0])
    baseline_records_df = pd.DataFrame.from_dict(list_of_records[1])

    # task1 -> (satisfaction_category = Not_classified, detractors, passives and promoters), percentage
    populate_nfs_data(endline_records_df)

    # task2 -> extract Total number of employees by
    extract_total_employees_from_baseline(baseline_records_df)
    extract_total_employees_from_endline(endline_records_df)

    # task3 -> % of Clients that registered an increase in the number of employees by strata  (host & refugees) and
    # location
    # - Data from task 2 should be sufficient

    # task4 -> The average number of Total jobs created by strata (refugees & host) and location (Use Business
    # location) - Note difference between client and business location - refer to the survey codebook for clarification
    # - Data from task 2 should be sufficient

    # Task 5 -> Total Average Revenue change (Baseline/Endline)
    # in Dollars by  strata ( host & refugees), gender, and location. (use variable sr_mnthly_sales_total)
    # - Data from task 2 should be sufficient

    # task 6 -> total Number of Clients Surveyed by gender , gender , age and strata ( host & refugees) and location
    # (Only Consider Clients with both Baseline and Endline Surveys)
    # - Data from task 2 should be sufficient


if __name__ == '__main__':
    main()
