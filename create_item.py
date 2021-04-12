import copy

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

# the codes in BC for VPE and STK
BC_CODE_STK = "STK"
BC_CODE_VPE = "VPE"


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
    Makes the Header of DataFrame be [0,1,2,...,n]
    :param df: input df
    :return: input df with headers/columns = [0,1,2,...,n]
    """
    headers = range(len(df.columns))
    df.columns = headers
    return df


def clean_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Makes the index/rows of DataFrame be [0,1,2,...,n]
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
    :param column_index: Which column (number, not name!) should be the index? If not specified, the first column
    is used.
    :return: rearranged input df
    """
    df = df.drop(labels=range(column_index), axis=1)
    df.set_index(column_index, inplace=True)
    df = clean_headers(df)
    return df


def how_many_items(item: pd.Series, attr="GTIN", how_many_char=10) -> int:
    """
    This method reads a column (=SKU) and returns how many items have to be created in the system(s). One SKU can be
    many items (STK, VPE, UVPE).
    :param item: One item as a series which is one column of a pd.DataFrame
    :param attr: Name of the attribute which we want to count.
    :param how_many_char: Threshold on how many characters the attribute needs for the "item" to count as one
    :return: The Number of items that need to be created.
    """
    return len([len(i) for i in list(item.get(attr)) if len(i) > how_many_char])


def drop_series(item: pd.Series, char_to_drop) -> pd.Series:
    """
    Drops all "rows" = (index, value) from series where the value equals a specific character.
    :param item: Series or item
    :param char_to_drop: The specific character
    :return: the clean series / item
    """
    return item[item != char_to_drop]


def drop_series_list(item: pd.Series, chars_to_drop=None) -> pd.Series:
    """
    This just calls drop_series on the characters specified.
    :param item: Item that needs to be cleaned.
    :param chars_to_drop: Specified list of characters that names the values that need dropping.
    :return: clean item
    """
    if chars_to_drop is None:
        chars_to_drop = ["-", None, "/", ""]
    for char_to_drop in chars_to_drop:
        item = drop_series(item, char_to_drop)
    return item


def get_idx_separator(item: pd.Series, name_of_sep=BC_SEPARATOR) -> int:
    """
    Get the idx as an Integer for the separator of head and body of an item column.
    Per Default, the separator is set to be BC_SEPARATOR (for now).
    :param item: the item column
    :param name_of_sep: KEY (str) of the corresponding item column that separates the head and the body.
    :return: returns Idx of separator as an Integer
    """
    counter = 0
    for key in item.index:
        if key == name_of_sep:
            return counter
        else:
            counter += 1


def construct_item_list(item: pd.Series) -> list:
    """
    This is where the magic happens (or is supposed to). Takes the single item column from the brain
    and forms different items from there, depending on the scheme.
    :param item: the Item column
    :return: Returns a list of pd.Series that contains all items.
    """
    # let us first define where the first, second (and third if applicable) items start
    idx_first = 0
    # make second and third +1 to exclude the "Menge pro Einheit"
    idx_second = 3
    idx_third = 7
    # we count how many items we need -> maybe we don't need the count due to redundancy todo check later
    count = how_many_items(item)
    # now we get the separator
    sep = get_idx_separator(item)
    # then we get the body
    body = item.iloc[sep:]
    # the first item is always added
    items = [item.iloc[idx_first:idx_second].append(body)]
    # if there is two items, the second (VPE) also gets a slot
    if count == 2:
        items += [item.iloc[idx_second:sep].append(body)]
    # for three items, the Tray also gets a slot
    elif count == 3:
        items += [item.iloc[idx_second:idx_third].append(body)]
        items += [item.iloc[idx_third:sep].append(body)]
    # for Business Central, the items all get the corresponding Verkaufs- und Einkaufseinheiten- Codes
    if count > 1:
        for i in items[1:]:
            i["Verkaufseinheitencode"] = BC_CODE_VPE
            i["Einkaufseinheitencode"] = BC_CODE_VPE

    return items


def main():
    # get the items
    items = read_df_from_brain(ITEM_SHEET_NAME)
    # get the mapping
    mapping = read_df_from_brain(MAP_SHEET_NAME)
    # join and clean them
    joint = shape_map_items(items=items, mapping=mapping, name_of_system_column=BC_SYSTEM_NAME)

    # todo (1) get the excel template
    # todo (2) go through all items
    # todo (3) append them according to the template

    joint = index_equals_column_and_drop(joint)

    # ---------------------------------------- (2) go through all items ----------------------------------------

    for column in joint:
        # get the column of the whole item (including tray / vpe if applicable)
        item = joint[column]
        # now we drop every entry that is not specified --> watch out here, that means we are not allowed to leave
        # anything empty where we don't have the info yet
        item = drop_series_list(item)
        items = construct_item_list(item)
        # todo get the Excel template and join the series one by one
        template = pd.read_excel("Excel Templates/BC_3.xlsx", sheet_name=None, header=None)
        template_v2 = copy.deepcopy(template['Artikel'].T)
        clean = index_equals_column_and_drop(template_v2, 2)
        # okay so we have the template cleaned
        # todo
        # now we need to (1) get the old values out and (2) drop the new values in
        # todo write the join algo!
        breakpoint()

    breakpoint()


if __name__ == '__main__':
    main()
