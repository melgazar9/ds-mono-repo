### imports ###

from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *
from kaggle.titanic.titanic_utils import *
np.random.seed(9)

### global variables ###

TRAIN_LOC = '~/.kaggle/titanic/train.csv'
TEST_LOC = '~/.kaggle/titanic/test.csv'
TARGET_NAME = 'Survived'

### load dfs ###

df_train = pd.read_csv(TRAIN_LOC)
df_test = pd.read_csv(TEST_LOC)

### intersection of features between train / test datasets ###

keep_cols = list(np.intersect1d(df_train.columns, df_test.columns)) + [TARGET_NAME]

df_train = df_train[keep_cols]
df_test = df_test[[i for i in keep_cols if i != TARGET_NAME]]

### set up the df for mlflow ###

df_test.loc[:, TARGET_NAME] = np.nan
df = pd.concat([df_train, df_test])
del df_train, df_test

gc.collect()

### initialize feature transformer ###

ft = FeatureTransformer(target_name=TARGET_NAME, preserve_vars=['PassengerId', 'dataset_split'])

### train a model ###

mlf = SklearnMLFlow(df=df,
                    input_features=[i for i in df.columns if i not in ['dataset_split', 'PassengerId', TARGET_NAME]],
                    target_name=TARGET_NAME,
                    preserve_vars=['PassengerId', 'dataset_split'],
                    feature_creator=TitanicFeatureCreator(),
                    splitter=TitanicSplitter(train_pct=0.80, val_pct=0.20),
                    feature_transformer=ft,
                    algorithms=[CatBoostClassifier(iterations=300, learning_rate=0.02, random_state=9),
                                XGBClassifier(learning_rate=0.05, max_depth=3, random_state=9)],
                    optimizer=ScoreThresholdOptimizer(accuracy_score))

mlf.split()
mlf.create_features()
mlf.transform_features()
mlf.train_models()
mlf.predict_models()
mlf.optimize_models('maximize') # should be custom optimization method
print(mlf.threshold_opt_results_by_split)

### save output ###

df_catboost = \
    mlf.df_out[mlf.df_out['dataset_split'] == 'submission']\
        [['passenger_id', 'CatBoostClassifier_pred_class']]\
        .rename(columns={'passenger_id': 'PassengerId',
                         'CatBoostClassifier_pred_class': 'Survived'})

df_xgb = \
    mlf.df_out[mlf.df_out['dataset_split'] == 'submission']\
        [['passenger_id', 'XGBClassifier_pred_class']]\
        .rename(columns={'passenger_id': 'PassengerId',
                         'XGBClassifier_pred_class': 'Survived'})


df_catboost.to_csv(TRAIN_LOC.replace('train.csv', 'df_catboost.csv'), index=False)
df_xgb.to_csv(TRAIN_LOC.replace('train.csv', 'df_xgb.csv'), index=False)


### run the workflow while using the validation set to reduce overfitting ###


mlf = SklearnMLFlow(df=df,
                    input_features=[i for i in df.columns if i not in ['dataset_split', 'PassengerId', TARGET_NAME]],
                    target_name=TARGET_NAME,
                    preserve_vars=['PassengerId', 'dataset_split'],
                    splitter=KaggleTitanicSplitter(train_pct=0.80, val_pct=0.20),
                    feature_transformer=ft,
                    algorithms=[CatBoostClassifier(iterations=1000,
                                                   max_depth=3,
                                                   bagging_temperature=1,
                                                   per_float_feature_quantization='0:border_count=1024',
                                                   l2_leaf_reg=5),
                                XGBClassifier(learning_rate=0.01,
                                              max_depth=3,
                                              l2_leaf_reg=3)],
                    optimizer=ScoreThresholdOptimizer(accuracy_score))

mlf.split()
mlf.transform_features()

eval_set = [
    (
        mlf.df_out[mlf.df_out[mlf.split_colname] == 'train'][mlf.output_features],
        mlf.df_out[mlf.df_out[mlf.split_colname] == 'train'][mlf.target_name]
    ),

    (
        mlf.df_out[mlf.df_out[mlf.split_colname] == 'val'][mlf.output_features],
        mlf.df_out[mlf.df_out[mlf.split_colname] == 'val'][mlf.target_name]
    )
    ]

mlf.train_models(early_stopping_rounds=5, eval_set=eval_set)
mlf.predict_models()
mlf.optimize_models('maximize')

print(mlf.threshold_opt_results_by_split)

df_catboost.to_csv(TRAIN_LOC.replace('train.csv', 'df_catboost2.csv'), index=False)
df_xgb.to_csv(TRAIN_LOC.replace('train.csv', 'df_xgb2.csv'), index=False)

subprocess.run('kaggle competitions submit -c titanic -f /home/melgazar9/.kaggle/titanic/df_catboost2.csv -m "mlf"')