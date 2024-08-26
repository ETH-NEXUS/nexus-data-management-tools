from config import config

# Labkey API
from labkey.api_wrapper import APIWrapper

# from labkey.query import QueryFilter

api = APIWrapper(config.labkey, config.project.name, use_ssl=True)
