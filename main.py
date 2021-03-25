import pandas as pd
import mysql.connector
from pathlib import Path


def print_hi(name):
    print(f'Hi, {name}')


if __name__ == '__main__':

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="#singlesourceoftruth",
        database="masterdata"
    )

    mycursor = mydb.cursor()
    # mycursor.execute("CREATE TABLE Zolltatifnummer("
    #                  "Nummer char(8),"
    #                  "Beschreibung varchar(255),"
    #                  "PRIMARY KEY(Nummer)"
    #                  ")")

    data_folder = Path("/Excel Templates")
    file_to_open = data_folder / "Standard09_03_2021_18_13_54.xlsx"

    df = pd.read_excel(file_to_open)
    sql = "INSERT INTO Zolltarifnummer (Nummer, Beschreibung) VALUES (%s, %s)"
    laenge = []
    for index, row in df.iterrows():
        nummer = row[0]
        beschreibung = row[1]

        if index > 1 and len(nummer) == 8 and not type(beschreibung) == float and len(beschreibung) > 1:
            val = (nummer, beschreibung)
            mycursor.execute(sql, val)
            laenge += [len(beschreibung)]

    print(max(laenge))
    breakpoint()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
