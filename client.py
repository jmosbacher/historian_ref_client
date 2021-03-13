import time
import getpass
import os
import requests
import pandas as pd
from datetime import datetime


class HistorianClient:
    BASE_URL = "https://xenonnt.lngs.infn.it/slowcontrol_temp"
    LOGIN = "Login"
    QUERY = "GetSCData"
    LAST_MEASURED = "getLastMeasuredValue"
    LAST_MEASURED_PMTS = "GetLastMeasuredPMTValues"
    QUERY_TYPES = ("LAB", "RAWBYTIME")
    USERNAME_ENV = "SC_USER"
    PASSWORD_ENV = "SC_PASSWORD"
    TOKEN_TTL = 24*3600 # Token time to live in seconds
    EARLY_EXPIRY = 30 # Early expiry to prevent expiration during request transit. in seconds.
    
    def __init__(self, username=None, password=None):
        self._username = username
        self._password = password
        self._token = None
        self._token_expires = 0
        
    @property
    def username(self):
        if self._username is None:
            self.get_username()
        return self._username
    
    @username.setter
    def username(self, value):
        self._username = value
    
    @property
    def password(self):
        if self._password is None:
            self.get_password()
        return self._password
    
    @password.setter
    def password(self, value):
        self._password = value
        
    @property
    def token(self):
        if self._token is None or self._token_expires < time.time():
            self.get_token()
        return self._token
    
    @property
    def headers(self):
        return {"Authorization": self.token}
    
    def get_username(self):
        user = os.getenv(self.USERNAME_ENV, None)
        if user is None:
            user = input(f"SC user [{getpass.getuser()}]: ")
        if not user:
            user = getpass.getuser()
        self._username = user
            
    def get_password(self):
        passwd = os.getenv(self.PASSWORD_ENV, None)
        if passwd is None:
            passwd = getpass.getpass("SC password: ")
        self._password = passwd
    
    def get_token(self):
        url = "/".join([self.BASE_URL, self.LOGIN])
        r = requests.post(url, data={"username": self.username, "password": self.password})
        if r.ok:
            self._token = r.json()["token"]
            self._token_expires = time.time() + self.TOKEN_TTL - self.EARLY_EXPIRY
        else:
            raise RuntimeError("Could not fetch access token, check credentials")
    
    def get_last_measured_value(self, name):
        url = "/".join([self.BASE_URL, self.LAST_MEASURED])
        params = {"name": name,
                  "EndDateUnix": int(time.time()),
                 }
        r = requests.get(url, params=params, headers=self.headers)
        r.raise_for_status()
        return r.json()
    
    def get_last_measured_pmts(self):
        url = "/".join([self.BASE_URL, self.LAST_MEASURED_PMTS])
        params = {
                  "EndDateUnix": int(time.time()),
                 }
        r = requests.get(url, params=params, headers=self.headers)
        r.raise_for_status()
        return r.json()
    
    def make_timestamp(self, date):
        if isinstance(date, int):
            return date
        if isinstance(date, float):
            return int(date)
        if isinstance(date, str):
            return int(pd.to_datetime(date).timestamp())
        if isinstance(date, datetime):
            return int(date.timestamp())
        
    def get_measurements(self, name, start_date, end_date, query_type="LAB", interval=1):
        start_date = self.make_timestamp(start_date)
        end_date = self.make_timestamp(end_date)
        
        if query_type not in self.QUERY_TYPES:
            raise ValueError(f"Invalid option for query_type, must be one of: {self.QUERY_TYPES}")
        params = {
            "name": name, 
            "StartDateUnix": start_date,
            "EndDateUnix": end_date,
            "QueryType": query_type,
            "interval": interval,
        }
        url = "/".join([self.BASE_URL, self.QUERY])
        r = requests.get(url, params=params, headers=self.headers)
        r.raise_for_status()
        return r.json()
    