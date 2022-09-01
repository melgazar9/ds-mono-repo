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

PRESERVE_VARS = ['PassengerId', 'Name', 'name_survived', 'dataset_split']

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

ft = FeatureTransformer(target_name=TARGET_NAME, preserve_vars=PRESERVE_VARS)

### train a model ###

mlf = SklearnMLFlow(df=df,
                    input_features=[i for i in df.columns if i not in PRESERVE_VARS + [TARGET_NAME]],
                    target_name=TARGET_NAME,
                    preserve_vars=PRESERVE_VARS,
                    feature_creator=TitanicFeatureCreator(),
                    splitter=TitanicSplitter(train_pct=0.70, val_pct=0.15),
                    feature_transformer=ft,
                    algorithms=[CatBoostClassifier(iterations=300, learning_rate=0.02, random_state=9),
                                XGBClassifier(learning_rate=0.05, max_depth=3, random_state=9)],
                    optimizer=ScoreThresholdOptimizer(accuracy_score),
                    evaluator=GenericMLEvaluator(
                        classification_or_regression='classification',
                        groupby_cols='dataset_split'
                        )
                    )

mlf.split()
mlf.create_features()
mlf.transform_features()
mlf.train_models()
mlf.predict_models()
mlf.optimize_models('maximize') # should be custom optimization method
mlf.get_feature_importances()

for pred in [i for i in mlf.df_out.columns if i.endswith('_pred_class')]:
    duplicate_name_indices = \
        mlf.df_out[mlf.df_out[mlf.split_colname] != 'submission']['name'] \
            .isin(mlf.df_out[mlf.df_out[mlf.split_colname] == 'submission']['name'].tolist())
    duplicate_name_indices = duplicate_name_indices[duplicate_name_indices]
    duplicate_names = mlf.df_out.loc[duplicate_name_indices.index, 'name']
    for name in duplicate_names:
        mlf.df_out.loc[mlf.df_out['name'] == name] = mlf.df_out.loc[mlf.df_out['name'] == name][mlf.target_name].max()


mlf.evaluate_models()

# print(mlf.optimizer.threshold_opt_results_by_split)


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
                    input_features=[i for i in df.columns if i not in PRESERVE_VARS + [TARGET_NAME]],
                    target_name=TARGET_NAME,
                    preserve_vars=PRESERVE_VARS,
                    feature_creator=TitanicFeatureCreator(),
                    splitter=TitanicSplitter(train_pct=0.70, val_pct=0.15),
                    feature_transformer=ft,
                    algorithms=[CatBoostClassifier(iterations=1000,
                                                   max_depth=3,
                                                   bagging_temperature=1,
                                                   per_float_feature_quantization='0:border_count=1024',
                                                   l2_leaf_reg=5),
                                XGBClassifier(learning_rate=0.01,
                                              max_depth=3,
                                              l2_leaf_reg=3)],
                    optimizer=ScoreThresholdOptimizer(accuracy_score),
                    evaluator=GenericMLEvaluator(
                        classification_or_regression='classification',
                        groupby_cols='dataset_split'
                    ))

mlf.split()
mlf.create_features()
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
mlf.evaluate_models()
mlf.get_feature_importances()

print(mlf.evaluator.evaluation_output)

df_catboost.to_csv(TRAIN_LOC.replace('train.csv', 'df_catboost2.csv'), index=False)
df_xgb.to_csv(TRAIN_LOC.replace('train.csv', 'df_xgb2.csv'), index=False)

# subprocess.run('kaggle competitions submit -c titanic -f /home/melgazar9/.kaggle/titanic/df_catboost2.csv -m "mlf"')