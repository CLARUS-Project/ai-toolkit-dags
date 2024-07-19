"""
This module provides the read_data function, which is utilized by the pipeline orchestrator (Airflow) for data ingestion. 
The function implements the logic to ingest the data and transform it into a pandas format. If any additional auxiliary 
functions are required to accomplish this step, they can be defined within the same script or separated into different 
scripts and included in the Data directory.
"""

import pandas as pd
from Data import ids_agent_client
from ids_agent_client import IDSAgentClient
import config

def read_data() -> pd.DataFrame:
    """
    The function implements the logic to ingest the data and transform it into a pandas format.

    In this code example, a csv file is retrieved from a url.

    Return:
        A Pandas DataFrame representing the content of the specified file.
    """

    try:

        #if not using IDS, your own code
        #df = pd.read_csv("", delimiter=';', quotechar='"')

        #if using IDS
        ids_agent_client = IDSAgentClient()
        #Get minio enviromental variables
        
        ids_agent_client.read_dataset_from_ids(config.MLFLOW_EXPERIMENT, "34.251.246.165","34.250.205.215:30010","minio","minio123")
        if ids_agent_client == False:
            return None
        else:    
            
            df = pd.read_csv("dataset.csv", delimiter=';', quotechar='"')       
            return df
    except Exception as exc:
        print(f'error:  { str(exc)}') 
        return None