import json
import fileinput

datamodelschema_name = r"C:\Users\HL834YP\OneDrive - EY\Documents\Projects\scip solution\python handle pbi\powerbi-vcs\pbi-vcs\samples\SM_AP_Landing_Page_out.pbit\DataModelSchema"

with open(datamodelschema_name) as json_file:
    data = json.load(json_file)

table_dict = {}
for table in data['model']['tables']:
    if 'isHidden' not in table.keys():
        table_dict[table['name']] = []
        for column in table['columns']:
            table_dict[table['name']].append(column['name'])

for table in data['model']['tables']:
    if 'isHidden' not in table.keys():
        expression = table['partitions'][0]['source']['expression']
        for column_name in table_dict[table['name']]:
            with fileinput.FileInput(datamodelschema_name, inplace=True) as file:
                for line in file:
                    replace_string_1 = "Source = Odbc.DataSource(\"dsn=\"&datasourcename, [HierarchicalNavigation=true]),"
                    replace_string_2 = "HIVE_Database = Source{[Name=\"HIVE\",Kind=\"Database\"]}[Data],"
                    replace_string_3 = "tdf_dpl_scip_Schema = HIVE_Database{[Name=databasename,Kind=\"Schema\"]}[Data],"
                    replace_string_4 = "Source = Odbc.DataSource(\"dsn=\"&datasourcename, [HierarchicalNavigation=true]),"
                    print(line.replace(column_name, column_name.upper()), end='')

    / *let
    Source = Sql.Databases("uw2ddaotdfsql01.database.windows.net"),
    EUWDSMMDEVSDP01 = Source
    {[Name = "uw2ddaotdfsdb01"]}[Data],
    dbo_CURRENCY_CONVERSION = EUWDSMMDEVSDP01
    {[Schema = "dbo", Item = "CURRENCY_CONVERSION"]}[Data],
    # "Filtered Rows" = Table.SelectRows(dbo_CURRENCY_CONVERSION, each ([Valid_To_Date] = #date(2099, 12, 31)))
in
# "Filtered Rows"*/

let
Source = Odbc.DataSource("dsn=" & datasourcename, [HierarchicalNavigation = true]),
HIVE_Database = Source
{[Name = "HIVE", Kind = "Database"]}[Data],
tdf_dpl_scip_Schema = HIVE_Database
{[Name = databasename, Kind = "Schema"]}[Data],
dpl_ap_fx_rate_master_Table = tdf_dpl_scip_Schema
{[Name = "dpl_ap_fx_rate_master", Kind = "Table"]}[Data],
# "Filtered Rows" = Table.SelectRows(dpl_ap_fx_rate_master_Table, each ([fx_rate_end_date] = #date(2099, 12, 31)))
in
# "Filtered Rows"

    Source = Sql.Databases("euwdscinprsql02.database.windows.net"),
    EUWDSCINPRSDB01 = Source
    {[Name = "EUWDSCINPRSDB01"]}[Data],
    DPL_AP_FX_RATE_MASTER = EUWDSCINPRSDB01
    {[Schema = "SMARTMAPS", Item = "DPL_AP_FX_RATE_MASTER"]}[Data],
    # "Filtered Rows" = Table.SelectRows(DPL_AP_FX_RATE_MASTER, each ([FX_RATE_END_DATE] = #date(2099, 12, 31)))
    in
    # "Filtered Rows"

