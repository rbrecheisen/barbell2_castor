import os
import json
import logging

from datetime import datetime
from barbell2_castor.api import CastorApiClient

logging.basicConfig()
logger = logging.getLogger(__name__)


""" ------------------------------------------------------------------------------------------------------------
"""
class CastorToDict:
    
    def __init__(
            self,
            study_name, 
            client_id, 
            client_secret, 
            log_level=logging.INFO, 
            ):
        self.study_name = study_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.log_level = log_level
        logging.root.setLevel(self.log_level)
        self.data = {}

    def execute(self):
        client = CastorApiClient(self.client_id, self.client_secret)
        study = client.get_study(self.study_name)
        study_id = client.get_study_id(study)
        self.data = client.get_study_data(study_id)
        return self.data
    
    
""" ------------------------------------------------------------------------------------------------------------
What is the best way to split up the Castor data into SQL tables?
Perhaps you shouel use PostgreSQL instead of SqLite
"""
class DictToSqlite3:
    pass