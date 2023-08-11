import json
import re
from pbivcs import compress_pbit, extract_pbit
import os
import shutil
from fuzzy_match import match

tables = {'Group': 'DF',
          'Tables': ['DPL_DF_DIM_ACTIVITY', 'DPL_DF_DIM_COST_SUBCATEGORY',
                     'DPL_DF_DIM_DISTRIBUTION_CHANNEL', 'DPL_DF_DIM_INCIDENT_TYPE',
                     'DPL_DF_DIM_PLANT', 'DPL_DF_DIM_RESOURCE',
                     'DPL_DF_DIM_TRANSPORTATION_MODE', 'DPL_DF_FACT_COST_MONTHLY',
                     'DPL_DF_FACT_INCIDENTS', 'DPL_DF_FACT_INVENTORY_MONTHLY',
                     'DPL_DF_FACT_PLANT', 'DPL_DF_FACT_RESOURCE_AVAILABILITY',
                     'DPL_DF_FACT_RESOURCE_UTILIZATION', 'DPL_DF_FACT_REVENUE',
                     'DPL_DF_FACT_TRANSPORTATION']}

path = r"C:\Users\SC773SW\Documents\Projects\SCIP E2E\pbi-vcs\samples\Test Dashboards\Fuzzy Test"

for file in os.listdir(path):
    if file.endswith(".pbit"):
        # print(os.path.join(path, file))

        input_file = os.path.join(path, file)
        output_file = input_file + '_out'
        extract_pbit(input_file, output_file, 'True')

        if 'DF_' not in output_file:
            shutil.rmtree(output_file + '\\DataMashup')

            # remove DataMashup from .zo
            fin = open(output_file + "/.zo", "rt")
            data = fin.read()
            data = data.replace('DataMashup\n', '')
            fin.close()
            fin = open(output_file + "/.zo", "wt")
            fin.write(data)
            fin.close()

        datamodelschema_name = output_file + '\\DataModelSchema'

        with open(datamodelschema_name) as json_file:
            data = json.load(json_file)

        replace_string_1 = "Source = Odbc.DataSource(\"dsn=\"&datasourcename, [HierarchicalNavigation=true]),"
        replace_string_2 = "HIVE_Database = Source{[Name=\"HIVE\",Kind=\"Database\"]}[Data],"
        replace_string_3 = "tdf_dpl_scip_Schema = HIVE_Database{[Name=databasename,Kind=\"Schema\"]}[Data],"

        for table in data['model']['tables']:
            if 'isHidden' not in table.keys():
                expression = table['partitions'][0]['source']['expression']
                if 'DF_' in datamodelschema_name:
                    # print('PowerBI table name: ', table['name'])
                    # print('Fuzzy matched table: ', match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0])
                    # print('-----------------------')
                    table_name = match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0]
                    if "Json" not in expression:
                        expression = re.sub('//.*', '', expression).strip('\n')
                    expression = re.sub('Source = Odbc.Query.*', "Source_orig = Sql.Databases(\"euwdscinprsql02.database.windows.net\"),"
                                        + "\n\tEUWDSCINPRSDB01 = Source_orig{[Name=\"EUWDSCINPRSDB01\"]}[Data],"
                                        + "\n\tSource = EUWDSCINPRSDB01{[Schema = \"SMARTMAPS\", Item = \"%s\"]}[Data]," % table_name,
                                        expression)
                    temp = expression.split('\n')
                    temp = [item for item in temp if not item.isspace()]
                    temp = [item for item in temp if item != '']
                    for i in range(0, len(temp) - 1):
                        if "SMARTMAPS" in temp[i] and '#' not in temp[i+1]:
                            temp[i] = temp[i].rstrip(',')
                        else:
                            continue
                    expression = '\n'.join(temp)
                    table['partitions'][0]['source']['expression'] = expression
                else:
                    # print('PowerBI table name: ', table['name'])
                    # print('Fuzzy matched table: ', match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0])
                    # print('-----------------------')
                    expression = re.sub(r'/\*[^*]*(?:\*(?!/)[^*]*)*\*/', '', expression)
                    expression = expression.replace(replace_string_1, "Source = Sql.Databases(\"euwdscinprsql02.database.windows.net\"),")
                    expression = expression.replace(replace_string_2, "EUWDSCINPRSDB01 = Source{[Name=\"EUWDSCINPRSDB01\"]}[Data],")
                    expression = expression.replace(replace_string_3, "%s = EUWDSCINPRSDB01{[Schema = \"SMARTMAPS\", Item = \"%s\"]}[Data],\n\t"
                                                                      "%s = Table.TransformColumnNames(%s,Text.Lower)," % ('SMARTMAPS_' + table['name'].lower() + '_Table', table['name'], table['name'].lower() + '_Table', 'SMARTMAPS_' + table['name'].lower() + '_Table'))
                    expression = expression.replace("%s = tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]," % (table['name'].lower() + '_Table', table['name'].lower()), '')
                    expression = expression.replace("%s = tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]" % (table['name'].lower() + '_Table', table['name'].lower()), '')
                    expression = expression.replace("%s= tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]," % (table['name'].lower() + '_Table', table['name'].lower()), '')
                    temp = expression.split('\n')
                    temp = [item for item in temp if not item.isspace()]
                    temp = [item for item in temp if item != '']
                    for i in range(0, len(temp) - 1):
                        if "SMARTMAPS" in temp[i] and '#' not in temp[i+1] and 'SMARTMAPS' not in temp[i+1]:
                            temp[i] = temp[i].rstrip(',')
                        else:
                            continue
                    expression = '\n'.join(temp)
                    table['partitions'][0]['source']['expression'] = expression

        for table in data['model']['expressions']:
            if 'isHidden' not in table.keys():
                expression = table['expression']
                if 'DF_' in datamodelschema_name:
                    if "Json" not in expression:
                        expression = re.sub('//.*', '', expression).strip('\n')
                    # table_name = re.search('tdf_dpl_scip_qa\.(.*)"', expression)
                    print('PowerBI table name: ', table['name'])
                    print('Fuzzy matched table: ',
                          match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0])
                    print('-----------------------')
                    table_name = match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0]
                    if table_name is not None:
                        # table_name = table_name.group(1).upper().strip('#(LF)')
                        # table_name = match.extractOne(table_name, tables['Tables'], match_type='trigram')[0]
                        # print(table_name)
                        expression = re.sub('Source = Odbc.Query.*', "Source_orig = Sql.Databases(\"euwdscinprsql02.database.windows.net\"),"
                                            + "\n\tEUWDSCINPRSDB01 = Source_orig{[Name=\"EUWDSCINPRSDB01\"]}[Data],"
                                            + "\n\tSource = EUWDSCINPRSDB01{[Schema = \"SMARTMAPS\", Item = \"%s\"]}[Data]," % table_name,
                                            expression)
                        temp = expression.split('\n')
                        temp = [item for item in temp if not item.isspace()]
                        temp = [item for item in temp if item != '']
                        for i in range(0, len(temp) - 1):
                            if "SMARTMAPS" in temp[i] and '#' not in temp[i+1]:
                                temp[i] = temp[i].rstrip(',')
                            else:
                                continue
                        expression = '\n'.join(temp)
                        table['expression'] = expression
                    else:
                        pass
                else:
                    # print('PowerBI table name: ', table['name'])
                    # print('Fuzzy matched table: ', match.extractOne(table['name'], tables['Tables'], match_type='trigram')[0])
                    # print('-----------------------')
                    expression = re.sub(r'/\*[^*]*(?:\*(?!/)[^*]*)*\*/', '', expression)
                    table_name = re.search('tdf_dpl_scip_Schema{\[Name="(.*)",Kind', expression)
                    if table_name is not None:
                        table_name = table_name.group(1).upper()
                        expression = expression.replace(replace_string_1, "Source = Sql.Databases(\"euwdscinprsql02.database.windows.net\"),")
                        expression = expression.replace(replace_string_2, "EUWDSCINPRSDB01 = Source{[Name=\"EUWDSCINPRSDB01\"]}[Data],")
                        expression = expression.replace(replace_string_3, "%s = EUWDSCINPRSDB01{[Schema = \"SMARTMAPS\", Item = \"%s\"]}[Data],\n\t"
                                                                          "%s = Table.TransformColumnNames(%s,Text.Lower)," % ('SMARTMAPS_' + table_name.lower() + '_Table', table_name, table_name.lower() + '_Table', 'SMARTMAPS_' + table_name.lower() + '_Table'))
                        expression = expression.replace("%s = tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]," % (table_name.lower() + '_Table', table_name.lower()), '')
                        expression = expression.replace("%s = tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]" % (table_name.lower() + '_Table', table_name.lower()), '')
                        expression = expression.replace("%s= tdf_dpl_scip_Schema{[Name=\"%s\",Kind=\"Table\"]}[Data]," % (table_name.lower() + '_Table', table_name.lower()), '')
                        temp = expression.split('\n')
                        temp = [item for item in temp if not item.isspace()]
                        temp = [item for item in temp if item != '']
                        for i in range(0, len(temp) - 1):
                            if "SMARTMAPS" in temp[i] and '#' not in temp[i+1] and 'SMARTMAPS' not in temp[i+1]:
                                temp[i] = temp[i].rstrip(',')
                            else:
                                continue
                        expression = '\n'.join(temp)
                        table['expression'] = expression
                    else:
                        pass

        output = open(output_file + '\\DataModelSchema_MODIFIED.json', "w")
        json.dump(data, output, indent=4)
        output.close()

        os.remove(output_file + '\\DataModelSchema')
        os.rename(output_file + '\\DataModelSchema_MODIFIED.json', output_file + '\\DataModelSchema')

        print(output_file)

        input_file = output_file
        output_file = output_file + '_2.pbit'
        compress_pbit(input_file, output_file, 'True')