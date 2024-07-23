import pandas as pd
from io import StringIO
from ids_read.ids_agent_client  import IDSAgentClient
import config

def read_data() -> pd.DataFrame:

    """
    The function implements the logic to ingest the data and transform it into a pandas format.

    In this code example, a csv file is retrieved from a datasource.
    If not using IDS add your own code to read the datasource
    If using IDS uncomment the example code and replace <<Dataset Provider IP>> with the IP of the dataset provider partipant

    Return:
        A Pandas DataFrame representing the content of the specified file.
    """
    try:
        ids_agent_client = IDSAgentClient()
        #Start transfer dataset
        resp= ids_agent_client.get_asset_from_ids(config.MLFLOW_EXPERIMENT,"34.250.205.215:30010")
        if resp == False:
            return None
        else:    
            #Get dataset from agent volume
            response=ids_agent_client.get_dataset(config.MLFLOW_EXPERIMENT)
            data = StringIO(response)
            df = pd.read_csv(data, delimiter=';', quotechar='"')       
            return df
    
    
    
    except Exception as exc:
        print(f'error:  { str(exc)}') 
        return None
       