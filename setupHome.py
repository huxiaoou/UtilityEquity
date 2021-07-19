import os
import sys
import numpy as np
import pandas as pd
import datetime as dt
from skyrim.whiterun import CCalendar

'''
created @ 2020-12-07
0.  main purpose is to regroup equity data from by date to by security. 

'''

DATABASE_DIR = os.path.join("C:\\", "Users", "huxia", "OneDrive", "文档", "Trading", "Database")
CALENDAR_DIR = os.path.join(DATABASE_DIR, "Calendar")
EQUITY_DIR = os.path.join(DATABASE_DIR, "Equity")
EQUITY_SECURITY_MKT_DATA_DIR = os.path.join(EQUITY_DIR, "security_mkt_data")

MD_BY_SEC_ID_DIR = os.path.join("D:\\", "DataBaseEquity", "by_security")
