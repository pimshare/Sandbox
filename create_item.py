import pandas as pd
import quickstart_google_sheets_api as google

# fixed parameters to use or change only here

# todo I made a copy of the PIM Brain --> rename copy to original or use backup
URL_BRAIN = "https://docs.google.com/spreadsheets/d/13Yay87pTFYeC-mW2gXkM3g___bvQHjwFBljMYxikFKc/edit#gid=656097316"
MAP_SHEET_NAME = "Field_Map"
ITEM_SHEET_NAME = "Skizze_v2"

# the names of the columns in the brain
BC_SYSTEM_NAME = "BC"
ATRIFY_SYSTEM_NAME = "atrify"

# threshold to separate item body and item head for BC items --> Name of first field in body
BC_SEPARATOR = "Artikelkategoriencode"


def read_df_from_brain(sheet_name: str, sheet_range=None) -> pd.DataFrame:
    """
    Gets a dataframe from a google sheet
    :param sheet_name: Name of the sheet you want to get from Google Sheets / PIM Brain
    :param sheet_range: If specified, the range just like in Google Sheets, e.g. "A2:C100" / else, the whole sheet is returned
    :return: returns a dataframe
    """
    if sheet_range is not None:
        return google.get_df_from_sheet(url=URL_BRAIN, sheet_range=sheet_name + "!" + sheet_range, header=True)
    return google.get_df_from_sheet(url=URL_BRAIN, sheet_range=sheet_name, header=True)


def shape_map_items(mapping: pd.DataFrame, items: pd.DataFrame, name_of_system_column: str) -> pd.DataFrame:
    """
    Joins item values and Mapping of fields such that only the relevant fields (Column 1) of the required system
    and the values of the items are left.
    :param name_of_system_column: can be "BC" for business central and "atrify" for atrify/GS1
    :param mapping: the map that knows how the fields / headers are called in the required systems
    :param items: the items we need to put into the system
    :return: a dataframe where the first column is the field name of the required system and the rest are the items and
    their corresponding values [field_name_system / item_1 / item_2 / ...]
    """
    # now we join them to conclude in a header - value mapping
    join_df = pd.merge(items, mapping, on='PIM', how='outer')
    # and we drop everything that is not relevant for BC
    # first all values that don't have an entry in the system's fields column
    join_df.dropna(subset=[name_of_system_column], inplace=True)
    # then all columns except system fields and items
    join_df.drop(labels=[i for i in list(join_df.columns) if i is not None and i != name_of_system_column], axis=1,
                 inplace=True)
    # clean the headers
    join_df = clean_headers(join_df)
    # and then make the last column be the first
    join_df = make_last_column_first(join_df)
    join_df = clean_all(join_df)
    return join_df


def make_last_column_first(df: pd.DataFrame) -> pd.DataFrame:
    """
    For all dataframes, no matter where they come from!
    :param df: Input df
    :return: Input df with last column inserted in first place
    """
    cols = df.columns.tolist()
    cols = [cols[-1]] + cols[:-1]
    df = df[cols]
    return df


def clean_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: input df
    :return: input df with headers/columns = [0,1,2,...,n]
    """
    headers = range(len(df.columns))
    df.columns = headers
    return df


def clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    :param df: input df
    :return: input df with index/rows = [0,1,2,...,n]
    """
    index = range(len(df.index))
    df.index = index
    return df


def clean_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans headers and rows according to called methods

    """
    df = clean_headers(df)
    df = clean_index(df)
    return df


def index_equals_column_and_drop(df: pd.DataFrame, column_index=0) -> pd.DataFrame:
    """
    Makes the specified column the index and drops the column
    :param df: Input df
    :param column_index: Which column (index, not name!) should be the index? If not specified, the first column is used.
    :return: rearranged input df
    """
    df.index = df[df.columns[column_index]]
    df = df.drop(labels=df.columns[column_index], axis=1)
    df = clean_headers(df)
    return df


def main():
    # get the items
    items = read_df_from_brain(ITEM_SHEET_NAME)
    # get the mapping
    mapping = read_df_from_brain(MAP_SHEET_NAME)
    # join and clean them
    joint = shape_map_items(items=items, mapping=mapping, name_of_system_column=BC_SYSTEM_NAME)

    # todo (1) get the excel template
    # todo (2) go through all items
    # todo (3) append them accordingly to the template

    joint = index_equals_column_and_drop(joint)

    # ---------------------------------------- (2) go through all items ----------------------------------------

    for column in joint:
        item = joint[column]
        # todo this is where you need to think: do we go through the lines one by one and fill
        #  an output df or what's the plan here? and do we do this for bc / atrify or can we already generalize here?
        # item.get(key) could help here
        breakpoint()

    breakpoint()


if __name__ == '__main__':
    main()
