import duckdb
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def etl():
    ## FOR SPECIFIC ID CREATE TABLE HISTORICAL ALSO FOR LATER EASIER MAINTENANCE
    duckdb.sql("CREATE TABLE review_specific_id AS SELECT * FROM read_json_auto('yelp_academic_dataset_review.json') WHERE business_id = '7ATYjTIgM3jUlt4UM3IypQ' AND date BETWEEN '2018-01-01' AND '2019-01-01';")

    duckdb.sql("CREATE TEMP TABLE review_taemp AS SELECT * FROM read_json_auto('yelp_academic_dataset_review.json') WHERE business_id = '7ATYjTIgM3jUlt4UM3IypQ' AND date BETWEEN '2018-01-01' AND '2019-01-01';")

    # FOR INCREMENTAL ONLY NOT USING BACKDATE
    df = duckdb.sql("SELECT * FROM review_temp WHERE review_id not in (SELECT review_id from review_specific_id);").fetchdf()

    df['date_format'] = df['date'].dt.date

    grouped_df = df.groupby('date_format').agg(
        count_row=('business_id', 'count'),
        avg_stars=('stars', 'mean'),
        avg_useful=('useful', 'mean'),
        avg_funny=('funny', 'mean'),
        avg_cool=('cool', 'mean')
    ).reset_index()


    grouped_df['date_format'] = grouped_df['date_format'].astype(str)


    ## PROCESS OF INSERT TO SPREADSHEET

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('service-account.json', scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open("Azhar Question Number 3 Dealls")  # Replace with your spreadsheet name
    worksheet = spreadsheet.sheet1  

    worksheet.insert_row(grouped_df.columns.tolist(), 1)

    for index, row in grouped_df.iterrows():
        print(row.tolist())
        worksheet.insert_row(row.tolist(), index + 2)

    print("Data inserted successfully!")


    ## ----------------
    ## BACKFILL PROCESS
    ## ----------------

    df_backfill = duckdb.sql("SELECT date::date date, COUNT(business_id) count_row, AVG(stars) stars, AVG(useful) useful, AVG(funny) funny, AVG(cool) cool  FROM review_temp WHERE date::date = '2018-04-10' GROUP BY date::date;").fetchdf()

    data = worksheet.get_all_values()

    id_to_delete = '2018-04-10'

    row_index_to_delete = None
    for index, row in enumerate(data):
        if row[0] == id_to_delete:  # Assuming the ID is in the first column
            row_index_to_delete = index + 1  # gspread is 1-indexed
            break


    if row_index_to_delete:
        worksheet.delete_rows(row_index_to_delete)
        print(f"Deleted row with ID: {id_to_delete}")
    else:
        print(f"No row found with ID: {id_to_delete}")

        df_backfill['date'] = df_backfill['date'].astype(str)

    # new_row = ['2018-04-10', 1, 5.0, 0.0, 0.0, 0.0]
    for i, row in df_backfill.iterrows():
        new_row = row.tolist()
        worksheet.append_row(new_row)

etl()