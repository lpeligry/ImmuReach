from datetime import datetime
import papermill as pm
from pathlib import Path
from openhexa.sdk import current_run, pipeline, workspace, parameter
import requests
import os 

@pipeline("Production du fichier de mouvements de population")
@parameter(
    "year",
    name='Year',
    type=int,
    required=True
)
def run_pipeline(year):
    input_notebook_path = Path(workspace.files_path) / "pop/pop_movements/code/cleaning.ipynb"
    executed_notebook_path = Path(workspace.files_path) / f"pop/pop_movements/code/output_nb/cleaning_{year}.ipynb"

    notebook_parameters = {"year": year}

    # Execute notebook
    try:
        pm.execute_notebook(
            input_path=input_notebook_path,
            output_path=executed_notebook_path,
            parameters=notebook_parameters,
            kernel_name="python3"
        )
    
    except Exception:
        raise ValueError(f"❌ Pipeline has failed")
    
    produced_file = Path(workspace.files_path) / f"pop/pop_movements/data/out/pop_movements_{year}.parquet"
    
    # Place the produced file in the pipelines output
    current_run.add_file_output(str(produced_file).replace(str(workspace.files_path), ""))
    current_run.log_info("File is availbale!")

if __name__=="__main__":
    run_pipeline()