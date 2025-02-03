import json
import requests
import pandas as pd



from typing import Any, Optional
from io import StringIO


class IDSAgentClient:
    def __init__(self):
        pass
        
    def get(self, url: str,timeout:int) -> Optional[requests.Response]:
        """_summary_

        Args:
            url (str): _description_
            timeout (int): _description_

        Returns:
            Optional[requests.Response]: _description_
        """
        try:
          
            ret = requests.get(url, verify= False, timeout=timeout)
            """  logger.debug("GET returned {}".format(ret)) """
            return ret
        except requests.RequestException as e:
            print(
                "Request error {} on GET REQUEST to {}".format(type(e).__name__, url)
            )
            return None
        
    def get_asset_from_ids_using_minio(self, expId:str, connectorIP:str,minioEndpoint:str,minioUser:str,minioPass:str) -> bool:
        """_summary_

        Args:
            expId (str): _description_
            connectorIP (str): _description_
            minioEndpoint (str): _description_
            minioUser (str): _description_
            minioPass (str): _description_

        Returns:
            bool: _description_
        """
        try:
            #Query connector consumer to get dataset artifact from connector provider
            url = "http://34.250.205.215:8082/api/v2/consumer/asset?exp_id="+expId+"&asset_type=dataset&provider_ip="+connectorIP
            response = self.get(url,120)
            #Check operation result
            if response is None or response != 200:
                return False
            else:
                #Get csv from minio server
                # Connect to minio
                client = Minio(minioEndpoint, access_key= minioUser, secret_key=minioPass,secure=False)
                # Download asset to shared folder (connector datalake)
                minio_path =expId + "/dataset.csv"
                dest_path = "dataset.csv"
                client.fget_object("dataset", minio_path, dest_path) 
                return True
            
        except Exception as e:
           
            return False
        
    def get_asset_from_ids(self, expId:str, connectorIP:str) -> bool:
        """_summary_

        Args:
            expId (str): _description_
            connectorIP (str): _description_

        Returns:
            bool: _description_
        """
        try:
            #Query connector consumer to get dataset artifact from connector provider
            url = "http://34.250.205.215:8082/api/v2/consumer/asset?exp_id="+expId+"&asset_type=dataset&provider_ip="+connectorIP
            response = self.get(url,120)
            #Check operation result
            if response is None or response.status_code != 200:
                return False
            else:
                return True
            
        except Exception as e:
           
            return False        
        
    def get_dataset(self, expId:str):

        """_summary_

        Returns:
            _type_: _description_
        """
        try:
            #Query agent to get dataset saved in volume 
            url = "http://34.250.205.215:8082/api/v3/asset?exp_id="+expId+"&asset_type=dataset"
            #url = "http://localhost:8082/api/v2/dataset?exp_id="+expId
            response = self.get(url,120)


            #Check operation result
            if response is None or response.status_code != 200:
                return ""
            else:
                resp = response.json()
                data = resp["message"]
                return data
            
        except Exception as e:
           
            return False        
