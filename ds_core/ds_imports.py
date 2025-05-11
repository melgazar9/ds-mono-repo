# flake8: noqa

### common python imports ###

import ast
import gc
import inspect
import itertools
import json
import logging
import multiprocessing as mp
import os
import re
import subprocess
import sys
import time
import warnings
from collections import Counter
from configparser import ConfigParser
from datetime import datetime, timedelta
from functools import partial, reduce
from glob import glob
from zipfile import ZIP_DEFLATED, ZipFile

import dill
import feather
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly
import plotly.express as px
import seaborn as sns
import yagmail
from flatten_json import flatten as flatten_json
from pandas import json_normalize

### numeric libraries ###


# from skimpy import clean_columns


### instead of importing skimpy, such a big library for column cleaning, let's define our own

### plotting ###
