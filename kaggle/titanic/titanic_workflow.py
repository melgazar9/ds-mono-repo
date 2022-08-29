### imports ###

from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *

### global variables ###

TRAIN_LOC = '/Users/melgazar9/Downloads/titanic/train.csv'
TEST_LOC = '/Users/melgazar9/Downloads/titanic/test.csv'
TARGET_NAME = 'Survived'

### load dfs ###

df_train = pd.read_csv(TRAIN_LOC)
df_test = pd.read_csv(TEST_LOC)

### intersection of features between train / test datasets ###

keep_cols = list(np.intersect1d(df_train.columns, df_test.columns)) + [TARGET_NAME]

df_train = df_train[keep_cols]
df_test = df_test[[i for i in keep_cols if i != TARGET_NAME]]

### create the df for mlflow ###

df_test.loc[:, TARGET_NAME] = np.nan
df_train.loc[:, 'dataset_split'] = 'train'
df_train.loc[int(df_train.shape[0] * 0.7):, 'dataset_split'] = 'val'
df_test.loc[:, 'dataset_split'] = 'test'

df = pd.concat([df_train, df_test])


### initialize feature transformer ###

ft = FeatureTransformer(target_name=TARGET_NAME, preserve_vars=['PassengerId', 'dataset_split'])

### train a model ###

mlf = \
    SklearnMLFlow(
        df=df,
        input_features=[i for i in df.columns if i not in ['dataset_split', 'PassengerId', TARGET_NAME]],
        target_name=TARGET_NAME,
        preserve_vars=['PassengerId', 'dataset_split'],
        feature_transformer=ft,
        algorithms=[CatBoostClassifier(iterations=300), XGBClassifier()],
        optimizer=ScoreThresholdOptimizer(optimization_func=accuracy_score)
    )

mlf.transform_features()
mlf.train_models()
mlf.predict_models()
mlf.optimize_models('maximize')

### save output ###

df_catboost = \
    mlf.df_out[mlf.df_out['dataset_split'] == 'test']\
        [['passenger_id', 'CatBoostClassifier_pred_class']]\
        .rename(columns={'passenger_id': 'PassengerId',
                         'CatBoostClassifier_pred_class': 'Survived'})

df_xgb = \
    mlf.df_out[mlf.df_out['dataset_split'] == 'test']\
        [['passenger_id', 'XGBClassifier_pred_class']]\
        .rename(columns={'passenger_id': 'PassengerId',
                         'XGBClassifier_pred_class': 'Survived'})


df_catboost.to_csv(TRAIN_LOC.replace('train.csv', 'df_catboost.csv'), index=False)
df_xgb.to_csv(TRAIN_LOC.replace('train.csv', 'df_xgb.csv'), index=False)