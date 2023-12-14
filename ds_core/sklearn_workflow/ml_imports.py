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
    precision_recall_curve,
    roc_curve,
    roc_auc_score,
    log_loss,
    average_precision_score,
    classification_report,

    # regression metrics
    mean_squared_error,
    mean_absolute_error,
    mean_squared_log_error,
    r2_score
    )


from sklearn.preprocessing import (
    FunctionTransformer,

    MinMaxScaler,
    StandardScaler,
    RobustScaler,

    OneHotEncoder,
    OrdinalEncoder,
    LabelEncoder
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
from xgboost import XGBClassifier, XGBRegressor, XGBRFClassifier, XGBRFRegressor
from catboost import CatBoostClassifier, CatBoostRegressor, Pool
# from lightgbm import LGBMClassifier, LGBMRegressor # commented out by default because installation currently fails on the m1 chip

from category_encoders.target_encoder import TargetEncoder

from feature_engine.encoding import MeanEncoder
from feature_engine.outliers import Winsorizer, OutlierTrimmer, ArbitraryOutlierCapper

### dask imports ###

# import dask
# from dask import delayed
