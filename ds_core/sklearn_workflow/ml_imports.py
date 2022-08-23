### sklearn imports ###

import sklearn

from sklearn.base import BaseEstimator, TransformerMixin
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

from sklearn.compose import (
    ColumnTransformer,
    make_column_transformer,
    make_column_selector
)

from sklearn.model_selection import (
    train_test_split,
    GridSearchCV,
    RandomizedSearchCV,
    StratifiedGroupKFold
)

from sklearn.impute import SimpleImputer

from sklearn.pipeline import (
    Pipeline,
    make_pipeline
)
from xgboost import XGBClassifier, XGBRegressor
from catboost import CatBoostClassifier, CatBoostRegressor, Pool
from lightgbm import LGBMClassifier, LGBMRegressor

from category_encoders.target_encoder import TargetEncoder

### dask imports ###

import dask
from dask import delayed