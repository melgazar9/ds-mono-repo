# flake8: noqa

### sklearn imports ###

import sklearn
from catboost import CatBoostClassifier, CatBoostRegressor, Pool
from category_encoders.target_encoder import TargetEncoder
from feature_engine.encoding import MeanEncoder
from feature_engine.outliers import ArbitraryOutlierCapper, OutlierTrimmer, Winsorizer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import (
    ColumnTransformer,
    make_column_selector,
    make_column_transformer,
)
from sklearn.ensemble import (
    AdaBoostClassifier,
    AdaBoostRegressor,
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
    StackingClassifier,
    StackingRegressor,
)
from sklearn.exceptions import NotFittedError
from sklearn.impute import SimpleImputer
from sklearn.metrics import (  # classification metrics; regression metrics
    accuracy_score,
    average_precision_score,
    classification_report,
    f1_score,
    log_loss,
    mean_absolute_error,
    mean_squared_error,
    mean_squared_log_error,
    precision_recall_curve,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import (
    GridSearchCV,
    RandomizedSearchCV,
    StratifiedGroupKFold,
    train_test_split,
)
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import (
    FunctionTransformer,
    LabelEncoder,
    MinMaxScaler,
    OneHotEncoder,
    OrdinalEncoder,
    RobustScaler,
    StandardScaler,
)
from sklearn.utils.validation import check_is_fitted
from xgboost import XGBClassifier, XGBRegressor, XGBRFClassifier, XGBRFRegressor

# from lightgbm import LGBMClassifier, LGBMRegressor # commented out by default because installation currently fails on the m1 chip


### dask imports ###

# import dask
# from dask import delayed
