import os.path

import pandas as pd
import pickle

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
credentials = None


def google_connect():
    """ Connects to google using oauth and saves the credentials
    Args: None
    Returns: None
    """
    try:
        global credentials
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                credentials = pickle.load(token)

        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(credentials, token)
    except Exception as err:
        print("An error Occured", err)


def connect_to_sheet():
    """ connects to the google sheet the ID which user provides
    Args:
    Returns:
        last_row (str) last row number of the sheet
        last_column: last column number of the sheet
        sheet_name: Name of the google sheet
        spread_sheet: values of the sheet
    """
    try:
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        print("---PLEASE ENTER THE ID OF THE SPREAD SHEET :")
        spread_sheet_id = input()
        info = sheet.get(spreadsheetId=spread_sheet_id,
                                          fields='sheets(data/rowData/values/userEnteredValue,properties(index,sheetId,title))').execute()

        sheet_index = 0
        last_row = len(info['sheets'][sheet_index]['data'][0]['rowData'])
        sheet_name = info['sheets'][sheet_index]['properties']['title']
        last_column = max([len(e['values']) for e in info['sheets'][sheet_index]['data'][0]['rowData'] if e])

        result = sheet.values().get(spreadsheetId=spread_sheet_id,
                                    range=f"1:{last_row}").execute()

        values = result.get('values', [])
        return [last_row, last_column, sheet_name, values]

    except Exception as err:
        print("An error Occured", err)


def build_csv(last_row, last_column, sheet_name, spread_sheet):
    """ Builds the csv according to the row values
        Args:
            last_row (int) last row number of the sheet
            last_column (int) last column number of the sheet
            sheet_name (str) Name of the google sheet
            spread_sheet (list) values of the sheet
    Returns: None
    """
    try:
        print("---PROCESSING THE CSV ..... ")
        print(f"---THE SHEET CONTAINS {last_row} ROWS AND {last_column} columns---")
        df = pd.DataFrame(columns=spread_sheet[0])
        for i in range(1, len(spread_sheet)):
            df.loc[i] = spread_sheet[i]

        df.to_csv(f'{sheet_name}.csv')
        print(f"---SUCCESSFULLY SAVED THE FILE AS : {sheet_name}---")
    except Exception as err:
        print("An error Occured", err)


if __name__ == '__main__':
    print("---STARTING THE APP---")
    google_connect()
    last_row, last_column, sheet_name, spread_sheet = connect_to_sheet()
    build_csv(last_row, last_column, sheet_name, spread_sheet)


