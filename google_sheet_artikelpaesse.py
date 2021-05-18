import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import openpyxl


def get_data_excel(path: str) -> dict:
    """
    :param path: where is the Excel Sheet? "{Folder in this dir}/{name of sheet}.xlsx"
    :return: dictionary with ALL worksheets
    """
    return pd.read_excel(path, sheet_name=None, header=None)


def clean_df(df: pd.DataFrame, threshold: int, header_row: int) -> pd.DataFrame:
    """
    :param df: dataframe that needs to be cleaned
    :param threshold: eliminate all columns that have less than x entries
    :param header_row: whcih row is the header row that needs to be kept and become the first row of returned df
    :return: clean df
    """
    result = df[header_row:].dropna(axis=1, how="all", thresh=threshold)
    return clean_headers_and_indices(result)


def df_columns(df: pd.DataFrame, list_of_columns: list) -> pd.DataFrame:
    """
    :param df: needs to have clean headers [0,1,2,3,...,n]
    :param list_of_columns: e.g. [0,5,6,10]
    :return: dataframe with only the specified columns
    """
    return df[list_of_columns]


def clean_headers_and_indices(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df:
    :return: df with all indexes and columns = [0,1,2,3,...,n]
    """
    return df.reset_index(drop=True).T.reset_index(drop=True).T


def authorize_google_and_open_sheet(sheet_name: str) -> gspread.models.Spreadsheet:
    """

    :param sheet_name: Name of the Worksheet. User (find username is json file under client_email) needs to be authorized for this one.
    :return: returns a google sheet
    """
    # Authorize the API
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/drive.file'
    ]
    file_name = 'client_key.json'
    creds = ServiceAccountCredentials.from_json_keyfile_name(file_name, scope)
    client = gspread.authorize(creds)

    # Fetch the sheet
    sheet = client.open(sheet_name)
    return sheet


def overwrite_sheet(df_to_write: pd.DataFrame, google_sheet: gspread.models.Spreadsheet, worksheet_index: int):
    """
    :param df_to_write: whcih df should be written to google sheets
    :param google_sheet: the sheet to write to
    :param worksheet_index: the index of the worksheet e {0,1,2,...,n}
    """
    worksheet = google_sheet.get_worksheet(worksheet_index)
    worksheet.clear()
    set_with_dataframe(worksheet, df_to_write)


def update_google_with_atrify(path_excel_sheet="atrify Excel Input/PUB_DL_4260556670003_20210518_1206_561.xlsx"):
    """
    Updates the sheet "Artikelpässe" with Artikel and Dimensions from atrify excel file
    :param path_excel_sheet: the path from working directory wher the sheet is, default is set to the current one
    """

    # (1) we get the data from an Excel Atrify Export

    template = get_data_excel(path_excel_sheet)

    # (2) we save the first sheet and eliminate all columns that have less than {threshold} entries

    threshold = 10
    header_row = 10
    artikel = clean_df(template["0 - Artikel Daten"], threshold, header_row)
    dimensionen = clean_headers_and_indices(df_columns(template["Components"][header_row:], [1, 4, 5])).dropna()

    # (3) get the sheet

    sheet = authorize_google_and_open_sheet("Artikelpässe")

    # (4) write the dfs to sheet

    # count of worksheets starts from 0
    index_artikel_atrify = 1

    overwrite_sheet(artikel, sheet, index_artikel_atrify)
    overwrite_sheet(dimensionen, sheet, index_artikel_atrify + 1)


def write_artikelpass(gtin: str):
    srcfile = openpyxl.load_workbook("Excel Templates/share_Artikeldatenblatt_REWE-Group_TEMPLATE.xlsx",
                                     read_only=False,
                                     keep_vba=True)  # to open the excel sheet and if it has macros
    sheet = srcfile["Artikeldatenblatt"]
    sheet['B33'] = gtin  # write something in B2 cell of the supplied sheet

    # write to row 1,col 1 explicitly, this type of writing is useful to write something in loops
    # sheetname.cell(row=1, column=1).value = "something"

    # save it as a new file, the original file is untouched and here I am saving it as xlsm(m here denotes macros).
    srcfile.save("Excel Templates/share_Artikeldatenblatt_REWE-Group_TEMPLATE.xlsx")

    print("Hello there")


def main():
    # todo need to call update_google_with_atrify() if you want to update the google sheet

    # todo work on this one!
    # write_artikelpass("4260556670270")
    update_google_with_atrify()


if __name__ == '__main__':
    main()
