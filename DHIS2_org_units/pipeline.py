from openhexa.sdk import pipeline, current_run, parameter, workspace
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2 import dataframe
from pathlib import Path
#from openhexa.sdk.workspaces.connection import DHIS2Connection

from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from shapely.geometry import shape 
import pandas as pd
import polars as pl
import geopandas as gpd
import json


@pipeline("dhis2_org_units")
@parameter(
    "level",
    name="Niveau de l'organisation",
    type=int,
    help="Niveau 1 correspond au pays, le niveau 5 aux centres de santé",
    required=True,
    default=3
)
@parameter(
    "coor_syst",
    name='Système de coordonnées',
    type=str,
    help="Système de coordonnées des données renvoyées. La valeur par défaut étant EPSG:32734 qui correspond à la RDC",
    required=False,
    default='EPSG:32734',
    choices=["EPSG:3857", "EPSG:4326", "EPSG:32734"]
)
@parameter(
    "end_date",
    name="Date limite fermeture (AAAA-MM-JJ)",
    type=str,
    help="Lorsque le niveau de l'organisation est égal à 5 (soit les FOSA), date avant laquelle les centres de santé qui sont fermés sont supprimés.",
    required=False,
    default='2021-12-31'
)
def dhis2_org_units(
    level: int,
    coor_syst: str | "EPSG:32734",
    end_date: str | '2022'
    ):
    """ Extract org units from a DHIS2 instance and reproject if needed.
        For level = 5, remove FOSA which were closed before end_date """
    
    current_run.log_info("Début pipeline d'extraction des unités organisationnelle")

    # Connection au SNIS --------------------------
    try:
        dhis2 = DHIS2(workspace.dhis2_connection("snis-drc"), cache_dir=None)
    
    except Exception as e:
        raise Exception(f"Erreur lors de la connection au SNIS : {e}") from e
    
    # Retrieve data --------------------------
    try:
        file = retrieve_data(dhis2, level, coor_syst)
        current_run.log_info(f"{len(file)} géométries ont été extraite du SNIS pour le niveau {level} de la pyramide sanitaire.")
    
    except Exception as e:
        raise Exception(f"Une erreur est survenue lors de l'extraction des données: {e}") from e
    
    # Uncomment to get dataframe of health centers opened after end_date

    # Filter on every health facilites which were still opened after end_date. --------------------------
    # Then standardise the dataset (1 row = 1 health center + period + open/close)
    # if level==5:
    #     # Filter on dates
    #     data = file[(file['closed_date']>=datetime.fromisoformat(end_date)) | (file['closed_date'].isnull())]

    #     # Standardise 
    #     file = standardise(data, end_date)


    # Save data --------------------------
    # Create a repository by name of EPSG 
    try:
        output_path = Path(workspace.files_path) / "Pipelines/data/out/Shapes" / coor_syst
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise Exception(f"Erreur lors de la création du dossier de sortie {output_path}: {e}") from e
    
    # Save
    try:
        output_fname = Path(output_path).joinpath(f"level_{level}")
        file.to_file(output_fname, driver="GPKG")
        current_run.log_info(f"GeoDataFrame successfully saved to {output_fname}")
    except PermissionError as e:
        raise PermissionError("Error: You don't have permission to access this file.") from e
    except OSError as e:
        raise OSError(f"An I/O error occurred: {e}") from e
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}") from e
    


def retrieve_data(dhis2_connection, level, coor_syst):

    df_org_unit = dataframe.get_organisation_units(dhis2_connection, max_level=level)
    df_org_unit = df_org_unit.filter(pl.col("level")==level)

    if len(df_org_unit)==0:
        raise ValueError("Aucune geometry trouvée pour ce niveau de la pyramide.")
    
    current_run.log_info(f'Pyramide Sanitaire au niveau {level} extraite.')

    # Remove en blank space
    df_org_unit = df_org_unit.with_columns(pl.col(f"level_{level+1}_name").str.strip_char())
    df_org_unit = df_org_unit.with_columns(pl.col(f"level_{level}_name").str.strip_char())

    # Select columns (level, level+1, geometry (+ dates if level = 5))
    if level == 5:
        df_org_unit = df_org_unit.select([f"level_{level}_id", f"level_{level}_name", "opening_date", "closed_date", "geometry"])
    else:
        df_org_unit = df_org_unit.select([f"level_{level+1}_id", f"level_{level+1}_name", f"level_{level}_id", f"level_{level}_name", "geometry"])
    
    current_run.log_info("Convertion des géométries en cours")

    # Convert GeoJSON strings to Shapely geometries
    # Safe parse: skip None and empty strings
    def safe_parse(geom):
        if geom is None or geom.strip() == "":
            return None
        return shape(json.loads(geom))
    
    df_org_unit['geometry'] = df_org_unit.geometry.apply(safe_parse)

    # Convert as GeoPandas in the chosen coordinate system
    gdf_org_unit = gpd.GeoDataFrame(df_org_unit, geometry='geometry', crs='EPSG:4326').to_crs(coor_syst)

    return gdf_org_unit


# def standardise(df, date):

#     # Create the monthly period range df
#     start = datetime.fromisoformat(date) + timedelta(days=1)
#     end = datetime(2024, 12, 31) # Pour le moment mais il faudrait changer par datetime.today() une fois qu'on aura les données SdB - Ss et SIGl pour 2025 / automatiser le process 
#     nb_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
#     periods = [start + i*relativedelta(months=1) for i in range(nb_months)]
    
#     # current = start
#     # periods = []
#     # while current <= end:
#     #     periods.append(current)
#     #     current += relativedelta(months=1)
#     period = pl.DataFrame({"period": periods})

#     # Merge
#     standardized_df = pl.from_pandas(df.drop(['geometry'], axis=1)).join(period, how='cross')

#     # Evaluate if open during the selected period
#     standardized_df = standardized_df.with_columns([
#                                     pl.col("period").alias("period_start"),
#                                     (pl.col("period").dt.offset_by("1mo").alias("period_end"))
#                                     ])
    
#     standardized_df = standardized_df.with_columns((
#                         (pl.col("opening_date") < pl.col("period_end")) &
#                         ((pl.col("closed_date").is_null()) | (pl.col("closed_date") > pl.col("period_start")))
#                     ).alias("open"))
    
#     standardized_df = standardized_df.select([
#                                             "level_5_id", "level_5_name",
#                                             pl.col("period"),#.dt.strftime("%Y-%m").alias("period"),
#                                             "open"
#                                         ]).with_columns(pl.col('period').dt.strftime("%Y%m").alias("period"))

#     return standardized_df


if __name__ == "__main__":
    dhis2_org_units()