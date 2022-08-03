# -------------------------------------------------------
#                       Matan Antebi
#                  Cell Phone: +9725229218
#         Email Address: matanantebi@mail.tau.ac.il
#                   Copyrights Reserved ©
# -------------------------------------------------------
import requests
import json


class BLSAPI:
    """
    The class 'BLSAPI' responsible for the data request from the U.S. BLS API.
    """

    def __init__(self):
        self.url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        self.headers = {'Content-type': 'application/json'}

    def post_request(self, apidict):
        data = json.dumps(apidict)
        p = requests.post(self.url, data=data, headers=self.headers)
        return p



