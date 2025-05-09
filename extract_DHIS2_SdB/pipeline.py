from openhexa.sdk import pipeline, current_run, parameter, workspace
from openhexa.toolbox.dhis2 import DHIS2
##from openhexa.sdk.workspaces.connection import DHIS2Connection
import pandas as pd
import polars as pl


@pipeline("Extraction_DHIS2_SdB")
@parameter(
    "dhis_con",
    name="DHIS2 Connection",
    type=DHIS2Connection,
    default="snis-drc",
    required=True,
)
@parameter(
    "month",
    name="Chosen month",
    help="ISO format: yyyymm",
    type=str,
    multiple=True
    required=True
)
def Extraction_DHIS2_SdB(dhis_con: DHIS2Connection, month: str):

    dhis_con = get_dhis(dhis_con)
    org_unit = get_org_unit(dhis_con)
    data_elements = get_data_elements(dhis_con, month, org_unit)
    table = enrich_data(dhis_con, data_elements, org_unit)


@Extraction_DHIS2_SdB.task
def get_dhis(connection): 
    return DHIS2(connection)


@Extraction_DHIS2_SdB.task
def get_org_unit(connection):

    org_unit = connection.meta.organisation_units()
    org_unit = pl.DataFrame(org_unit)
    return org_unit.filter(pl.col('level')==5)
    

@Extraction_DHIS2_SdB.task
def get_data_elements(connection, month, org_unit):
    elements = connection.data_value_sets.get(
                                data_elements = ["UxD03qX5O0t", # Cas reçus 
                                                 "p14YSRkzYv9" # Jrs de non fonctionnement-Electricité
                                                 "xUROImj6y93", # Jrs de non fonctionnement-Frigo
                                                 "MtQbqqRai95", # Infirmier A1 Agents
                                                 "ezeEllgXATH", # Infirmier A2 Agents
                                                 "X2BAHvCNuB8", # Infirmier L2 Agents
                                                 "ehTcME2KSyk", # Médecin généralistes Agents
                                                 "jx7B0d5C75a"
                                                ], # Autre personnel Agents
                                periods = month, 
                                org_units = list(org_unit.get_column('id'))
                            ) 
    
    return pl.DataFrame(pd.DataFrame(elements))


@Extraction_DHIS2_SdB.task
def enrich_data(connection, dataframe, org_unit):

    data_element_name = pl.DataFrame(connection.meta.data_elements())
    category_option_combos = pl.DataFrame(connection.meta.category_option_combos()).rename({'name':'category'})

    df_org_unit = dataframe.join(org_unit, left_on='orgUnit', right_on='id').select(pl.col(['dataElement', 'categoryOptionCombo', 'period', 'orgUnit', 'name', 'value'])).rename({"name":"cs"})
    df_data_element = df_org_unit.join(data_element_name, left_on='dataElement', right_on='id').select(pl.col(['dataElement', 'categoryOptionCombo', 'name', 'period', 'orgUnit', 'cs', 'value']))
    df_all = df_data_element.join(category_option_combos, left_on='categoryOptionCombo', right_on='id').select(pl.col(['dataElement', 'name', 'categoryOptionCombo', 'category', 'period', 'orgUnit', 'cs', 'value']))

    df_all = df_all.to_pandas().rename(columns={'dataElement':'data_element', 'categoryOptionCombo':'category_option_combo', 'orgUnit':'org_unit'})

    return df_all





if __name__ == '__main__':
    Extraction_DHIS2_SdB()


