### common python imports ###

import os, time, sys, gc, yagmail, re, json, itertools, warnings, configparser, inspect, subprocess

from datetime import datetime, timedelta
from flatten_json import flatten as flatten_json
import multiprocessing as mp
from functools import partial, reduce
from collections import Counter
from zipfile import ZipFile, ZIP_DEFLATED


### numeric libraries ###

import dill, feather

import pandas as pd
import numpy as np
from pandas import json_normalize
from skimpy import clean_columns


### sklearn imports ###

import sklearn
from sklearn.exceptions import NotFittedError
from sklearn.utils.validation import check_is_fitted

from sklearn.metrics import (

    # classification metrics
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,

    # regression metrics
    mean_squared_error,
    mean_absolute_error,
    mean_squared_log_error
    )

from sklearn.preprocessing import (
    OneHotEncoder,
    MinMaxScaler,
    StandardScaler,
    RobustScaler,
    FunctionTransformer
    )

from sklearn.ensemble import (
     RandomForestRegressor,
     RandomForestClassifier,
     StackingRegressor,
     StackingClassifier,
     AdaBoostClassifier,
     AdaBoostRegressor,
     HistGradientBoostingClassifier,
     HistGradientBoostingRegressor
    )

from catboost import CatBoostRegressor, CatBoostClassifier, Pool
from lightgbm import LGBMClassifier, LGBMRegressor


### dask imports ###

import dask
from dask import delayed

