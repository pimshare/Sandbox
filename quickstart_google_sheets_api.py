from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
import pathlib
from pdynamics import crm
import requests
import json

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']


def get_sheet(sheet_id, sheet_range):
    """Shows basic usage of the Sheets API.
        Prints values from a sample spreadsheet.
        """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id,
                                range=sheet_range).execute()
    return result.get('values', [])


def get_id(url):
    # the ID is located right after the /d/
    # https://docs.google.com/spreadsheets/d/{ID}/...
    return url.split("/")[5]


def column_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string


def get_df_from_sheet(url: str, sheet_range: str, header=False):
    url = url
    sheet_id = get_id(url)
    google_input = get_sheet(sheet_id, sheet_range)
    df = pd.DataFrame(google_input)
    if header:
        new_header = df.iloc[0]  # grab the first row for the header
        df = df[1:]  # take the data less the header row
        df.columns = new_header  # set the header row as the df header
    return df


def main():
    # if the URL looks as follows
    url = "https://docs.google.com/spreadsheets/d/192G4n_5xWRa0IzhCBiVoiexSs8ceOxFL-qo4doTma6M/edit#gid=623635317"
    # the range is {Name of Sheet}!{Range (notation just like in google sheets)}
    sheet_range = 'xlsx_generate_test'
    sheet_id = get_id(url)

    google_input = get_sheet(sheet_id, sheet_range)

    number_of_items = len(google_input[0]) - 1
    input_dict = {}

    for row in google_input[1:]:
        column_number = int(row[0]) - 1
        input_dict[column_number] = row[1:]
        if len(input_dict[column_number]) < number_of_items:
            input_dict[column_number] += ['']

    # add 0s to all GTINs
    input_dict[1] = ["0" + gtin for gtin in input_dict[1]]

    input_frame = pd.DataFrame(input_dict)
    master_frame = pd.read_excel("Excel Templates/Master_atrify.xlsx", header=None)
    new_frame = pd.concat([master_frame, input_frame])

    new_frame.to_excel("Output/Output2.xlsx", sheet_name="0 - Artikel Daten", index=False, header=False)


if __name__ == '__main__':
    main()
