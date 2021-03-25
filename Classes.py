# todo make a git repo

import pandas as pd
from tqdm import tqdm
import shelve
import quickstart_google_sheets_api as google

NAME_DB_ITEMS = "items"

FIRST_ROW_BC = 3
CODE_VPE = "VPE"
CODE_STK = "STK"
db_bc = shelve.open("Database/bc_items")

FIRST_ROW_ATRIFY = 11
db_a = shelve.open("Database/atrify_items")

URL_BRAIN = "https://docs.google.com/spreadsheets/d/192G4n_5xWRa0IzhCBiVoiexSs8ceOxFL-qo4doTma6M/edit#gid=681492469"

BC_DEF = {"Basiseinheit": "STK",
          "Lagerbuchungsgruppe": "FERTIG",
          "Artikelverfolgungscode": "CHARGENNR"}


class Item:

    def __init__(self, gtin, bezeichnung, nummer):
        self.gtin = gtin
        self.bezeichnung = bezeichnung
        self.nummer = nummer

    def __repr__(self):
        return f"{self.nummer}/{self.bezeichnung}/{self.gtin}"

    def __str__(self):
        return self.__repr__()


class BCItem(Item):
    basiseinheitencode = CODE_STK
    artikelverfolungscode = "CHARGENNR"
    # TODO 68,71,72 fehlen noch!
    lagerbuchungsgruppe = "FERTIG"

    def __init__(self, gtin, bezeichnung, nummer, ek_preis="", gewicht_brutto_g="", zolltarifnummer="",
                 einkaufseinheitencode="", verkaufseinheitencode="", kleinste_produktkategorie="", artikelkategorie="",
                 kst="", spende="",
                 skt_vpe="",
                 lager=""):
        Item.__init__(self, gtin, bezeichnung, nummer)
        self.ek_preis = ek_preis
        self.gewicht_brutto_g = gewicht_brutto_g
        self.zolltarifnummer = zolltarifnummer
        self.verkaufseinheitencode = verkaufseinheitencode
        self.einkaufseinheitencode = einkaufseinheitencode
        self.kleinste_produktkategorie = kleinste_produktkategorie
        self.artikelkategorie = artikelkategorie

        self.kst = kst
        self.spende = spende

        self.stk_vpe = skt_vpe

        self.lager = lager

        self.is_vpe = (CODE_VPE == self.verkaufseinheitencode or CODE_VPE == self.einkaufseinheitencode)


class AItem(Item):
    def __init__(self, gtin, bezeichnung, nummer):
        Item.__init__(self, gtin, bezeichnung, nummer)


def get_lager(df_lager: pd.DataFrame, item: BCItem):
    for values in df_lager[:][FIRST_ROW_BC:].iterrows():
        if values[1][2] == "2" and values[1][1] == item.nummer:
            return values[1][3]
    return ""


def get_kst(df_kst: pd.DataFrame, item: BCItem):
    for values in df_kst[:][FIRST_ROW_BC:].iterrows():
        if values[1][1] == item.nummer and values[1][2] == "KST":
            return values[1][3]
    return ""


def get_spende(df_spende: pd.DataFrame, item: BCItem):
    for values in df_spende[:][FIRST_ROW_BC:].iterrows():
        if values[1][1] == item.nummer and values[1][2] == "SPENDEN":
            return values[1][3]
    return ""


def create_or_update_all_items_bc(df_input: pd.DataFrame):
    # todo optimize: do the attributes of BCItems automatically,
    #  just like you did with atrify_items and name them after the column index
    # first, we create general Items:
    items = []
    main_table_name = list(df_input.keys())[0]

    if not db_bc or len(db_bc[NAME_DB_ITEMS]) < len(df_input[main_table_name]) - FIRST_ROW_BC:

        for key, value in df_input[main_table_name][:][FIRST_ROW_BC:].iterrows():
            items += [BCItem(gtin=value.get(9), bezeichnung=value.get(1), nummer=value.get(0),
                             ek_preis=value.get(4),
                             gewicht_brutto_g=value.get(5), zolltarifnummer=value.get(6),
                             einkaufseinheitencode=value.get(11), verkaufseinheitencode=value.get(10),
                             kleinste_produktkategorie=value.get(12),
                             artikelkategorie=value.get(7))]

        # then we fill the complicated stuff
        print("Getting Items from BC Excel Export:")
        for item in tqdm(items):
            item.lager = get_lager(df_input["7505 Zuordnung des Artikelattri"], item)
            item.kst = get_kst(df_input["Vorgabedimension"], item)
            item.spende = get_spende(df_input["Vorgabedimension"], item)
            item.stk_vpe = get_stk_vpe(df_input["Artikeleinheit"], item)
        db_bc[NAME_DB_ITEMS] = items


def create_or_update_all_items_atrify(df_input: pd.DataFrame):
    main_table_name = list(df_input.keys())[0]
    # todo I have no idea why the dataframe is one more item than the db_a
    #  --> if it works not necessity to investigate further
    if not db_a or len(db_a[NAME_DB_ITEMS]) < len(df_input[main_table_name][:][FIRST_ROW_ATRIFY:]) - 1:
        # the minus 1 is to also push the headers into the method input
        clean_df = drop_columns_without_values_atrify(df_input[main_table_name][:][FIRST_ROW_ATRIFY - 1:])
        clean_df.columns = clean_df.iloc[0]
        clean_df = clean_df[1:]
        items = []
        for key, value in tqdm(clean_df[:][1:].iterrows()):
            value.dropna(inplace=True)
            gtin = value["GDSN_GlobalTradeItemNumber[14 CHAR]"]
            try:
                number = value["WSCE_InternalItemIDofSupplier[80 CHAR]"]
                name = value["GDSN_DES_DescriptionOfTradeItem[200 CHAR]"]
            except KeyError:
                number = ""
                name = ""
            item = AItem(gtin, name, number)
            for i, v in value.items():
                item.__setattr__(i.replace("[", "_").replace("]", "_").replace("_", "").replace(" ", ""), v)
            items += [item]
        db_a[NAME_DB_ITEMS] = items


def drop_columns_without_values_atrify(df_input: pd.DataFrame):
    # lets get the column indexes where there is at least one value
    columns_to_consider = [idx for idx, val in enumerate(df_input[:][1:].isna().all()) if not val]
    clean_df = df_input[columns_to_consider]

    # just leave the comment as an example on how dropna() works -- its epic but we don't need it now
    # df_input = df_input[:][1:].dropna(1, "all")

    return clean_df


def get_stk_vpe(df_einheiten: pd.DataFrame, item: BCItem):
    # TODO check if that works --> it kind of does, you also need to watch out for the UMKARTON/TRAY --> the
    #  inbetweener, hab das aufm schirm!
    for values in df_einheiten[:][FIRST_ROW_BC:].iterrows():
        if values[1][0] == item.nummer and values[1][1] == CODE_VPE:
            return values[1][2]
    return ""


def get_attribute(gtin, attribute):
    for item in db_bc[NAME_DB_ITEMS]:
        if item.gtin == gtin:
            return getattr(item, attribute).value


def get_attribute_from_list(gtins, attribute):
    attributes = []
    for gtin in gtins:
        attributes += [get_attribute(gtin, attribute)]
    return attributes


def get_unique_attribute(attribute):
    attr = []
    for item in db_a[NAME_DB_ITEMS]:
        try:
            attr += [getattr(item, attribute)]
        except AttributeError:
            continue
    return set(attr)


def read_excels_from_ba_and_atrify():
    create_or_update_all_items_bc(
        pd.read_excel("Excel Templates/first Sync python.xlsx", sheet_name=None, header=None))
    create_or_update_all_items_atrify(
        pd.read_excel("atrify Excel Input/PUB_DL_4260556670003_20210323_1616_698.xlsx", sheet_name=None, header=None))


def read_item_from_google():
    # now, let's have a look at the google sheet to ease the pain when putting new items into BC / atrify
    input_df = google.get_df_from_sheet(
        url=URL_BRAIN, sheet_range="Skizze_v2!A2:C68")
    input_df.rename(columns={1: 'PIM'}, inplace=True)
    map = google.get_df_from_sheet(url=URL_BRAIN, sheet_range="Field_Map", header=True)

    # whoa --> just figured out I can join dataframes like so, which is pretty awesome
    join_df = pd.merge(input_df, map, on='PIM', how='outer')

    # todo where do we go from the joined df? --> BC first
    # transpose the DF and add rows to the Schablone
    df_to_bc(join_df)
    breakpoint()


def rename_headers(df: pd.DataFrame, row_idx_where_headers_are: int, inplace=True):
    df.columns = df.iloc[row_idx_where_headers_are]
    if inplace:
        df.drop([i for i in range(row_idx_where_headers_are + 1)])


def empty(df: pd.DataFrame, inplace=True):
    return df.drop(labels=df.index, inplace=inplace)


def clean_header(df: pd.DataFrame):
    df.columns = range(len(df.columns))


def construct_import(joined_df: pd.DataFrame):
    clean_header(joined_df)
    # first, lets take care of the joined df and duplicate it, depending on how the packaging is organized
    # todo continue here, more doing than thinking!!!
    # and rename the headers of the joint df to be able to apply this function to more than one new item
    joined_df = joined_df[[3, 2]]
    joined_df.dropna(subset=[3], inplace=True)
    clean_header(joined_df)
    item_column = 1
    joined_df[item_column + 1] = joined_df[item_column]
    if joined_df[1][3]:
        joined_df[item_column + 2] = joined_df[item_column]
    else:
        joined_df.drop(list(range(3, 7)), inplace=True)
    breakpoint()


def df_to_bc(joined_df: pd.DataFrame, significant_column="BC"):
    construct_import(joined_df)
    # read all sheets from template
    template = pd.read_excel("Excel Templates/BC.xlsx", sheet_name=None)
    for key, value in template.items():
        rename_headers(value, FIRST_ROW_BC - 2)
        empty(value)
    keys = list(template.keys())
    artikel = template[keys[0]]
    dimension = template[keys[1]]
    einheiten = template[keys[2]]


if __name__ == '__main__':
    read_item_from_google()

    # todo get the VPEs and reference them
    #  from item to item such as Item(STK) --> Item(Tray) --> Item(VPE)
    breakpoint()
    db_bc.close()
    db_a.close()
