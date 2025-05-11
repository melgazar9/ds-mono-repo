### imports ###

import kagglehub
from titanic_utils import *

from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *

### global variables ###

# TRAIN_LOC = "/Volumes/Inland NVME 4TB/Datasets/kaggle/titanic/titanic/train.csv"
# TEST_LOC = "/Volumes/Inland NVME 4TB/Datasets/kaggle/titanic/titanic/test.csv"

np.random.seed(9)

path = kagglehub.dataset_download(
    "yasserh/titanic-dataset"
)  # Download latest version --> use this if TRAIN_LOC and TEST_LOC are net set

print("Path to dataset files:", path)

TARGET_NAME = "Survived"
PRESERVE_VARS = ["PassengerId", "Name", "name_survived", "dataset_split"]
pd.options.display.max_columns = 10

### load dfs ###

# df_train = pd.read_csv(TRAIN_LOC)
# df_test = pd.read_csv(TEST_LOC)

df = pd.read_csv(path + "/" + os.listdir(path)[0])
df_train = df.iloc[0 : int(len(df) * 0.66)]
df_test = df.iloc[int(len(df) * 0.66) :]


### intersection of features between train / test datasets ###

keep_cols = list(
    set(list(np.intersect1d(df_train.columns, df_test.columns)) + [TARGET_NAME])
)

df_train = df_train[keep_cols]
df_test = df_test[[i for i in keep_cols if i != TARGET_NAME]]

### set up the df for mlflow ###

df_test.loc[:, TARGET_NAME] = np.nan
df = pd.concat([df_train, df_test])

del df_train, df_test
gc.collect()

### train a model ###

mlf = SklearnMLFlow(
    df=df,
    input_features=[i for i in df.columns if i not in PRESERVE_VARS + [TARGET_NAME]],
    target_name=TARGET_NAME,
    preserve_vars=PRESERVE_VARS,
    feature_creator=TitanicFeatureCreator(),
    splitter=TitanicSplitter(train_pct=1, val_pct=0),
    feature_transformer=FeatureTransformer(
        target_name=TARGET_NAME, preserve_vars=PRESERVE_VARS
    ),
    algorithms=[
        CatBoostClassifier(iterations=300, learning_rate=0.02, random_state=9),
        XGBClassifier(learning_rate=0.05, max_depth=3, random_state=9),
    ],
    optimizer=ScoreThresholdOptimizer(accuracy_score),
    evaluator=GenericMLEvaluator(
        classification_or_regression="classification", groupby_cols="dataset_split"
    ),
)

mlf.split()
mlf.create_features()
mlf.transform_features()
mlf.train()
mlf.predict()
mlf.assign_threshold_opt_rows(pct_train_for_opt=0.90, pct_val_for_opt=1)
mlf.optimize("maximize")  # should be custom optimization method

mlf.get_feature_importances()
print(mlf.feature_importances)

for pred in [i for i in mlf.df_out.columns if i.endswith("_pred_class")]:
    duplicate_name_indices = mlf.df_out[mlf.df_out[mlf.split_colname] != "submission"][
        "name"
    ].isin(mlf.df_out[mlf.df_out[mlf.split_colname] == "submission"]["name"].tolist())
    duplicate_name_indices = duplicate_name_indices[duplicate_name_indices]
    duplicate_names = mlf.df_out.loc[duplicate_name_indices.index, "name"]
    for name in duplicate_names:
        mlf.df_out.loc[mlf.df_out["name"] == name, pred] = mlf.df_out.loc[
            mlf.df_out["name"] == name
        ][mlf.target_name].max()


mlf.evaluate()
print(mlf.evaluator.evaluation_output)

### save output ###

df_catboost = mlf.df_out[mlf.df_out["dataset_split"] == "submission"][
    ["passenger_id", "CatBoostClassifier_pred_class"]
].rename(
    columns={"passenger_id": "PassengerId", "CatBoostClassifier_pred_class": "Survived"}
)

df_xgb = mlf.df_out[mlf.df_out["dataset_split"] == "submission"][
    ["passenger_id", "XGBClassifier_pred_class"]
].rename(
    columns={"passenger_id": "PassengerId", "XGBClassifier_pred_class": "Survived"}
)

# df_catboost.to_csv(TRAIN_LOC.replace("train.csv", "df_catboost.csv"), index=False)
# df_xgb.to_csv(TRAIN_LOC.replace("train.csv", "df_xgb.csv"), index=False)

### run the workflow while using the validation set to reduce overfitting ###

mlf = SklearnMLFlow(
    df=df,
    input_features=[i for i in df.columns if i not in PRESERVE_VARS + [TARGET_NAME]],
    target_name=TARGET_NAME,
    preserve_vars=PRESERVE_VARS,
    feature_creator=TitanicFeatureCreator(),
    splitter=TitanicSplitter(train_pct=0.7, val_pct=0.15),
    feature_transformer=FeatureTransformer(
        target_name=TARGET_NAME, preserve_vars=PRESERVE_VARS
    ),
    algorithms=[
        CatBoostClassifier(
            loss_function="Logloss",
            eval_metric="Logloss",
            boosting_type="Ordered",  # use permutations
            random_seed=9,
            use_best_model=True,
            one_hot_max_size=5,
            silent=True,
            depth=3,
            iterations=3000,
            learning_rate=0.03,
            l2_leaf_reg=8,
            border_count=5,
        ),
        XGBClassifier(
            n_estimators=1500,
            learning_rate=0.08,
            max_depth=3,
            grow_policy="lossguide",
            gamma=0.8,
            subsample=0.8,
            l2_leaf_reg=9,
            colsample_bytree=0.8,
            early_stopping_rounds=5,
            n_jobs=-1,
        ),
    ],
    optimizer=ScoreThresholdOptimizer(f1_score),
    evaluator=GenericMLEvaluator(
        classification_or_regression="classification", groupby_cols="dataset_split"
    ),
)

mlf.split()
mlf.create_features()
mlf.transform_features()

eval_set = [
    (
        mlf.df_out[mlf.df_out[mlf.split_colname] == "train"][mlf.output_features],
        mlf.df_out[mlf.df_out[mlf.split_colname] == "train"][mlf.target_name],
    ),
    (
        mlf.df_out[mlf.df_out[mlf.split_colname] == "val"][mlf.output_features],
        mlf.df_out[mlf.df_out[mlf.split_colname] == "val"][mlf.target_name],
    ),
]

mlf.train(eval_set=eval_set)
mlf.predict()
mlf.assign_threshold_opt_rows(pct_train_for_opt=0.90, pct_val_for_opt=1)
mlf.optimize("maximize")

for pred in [i for i in mlf.df_out.columns if i.endswith("_pred_class")]:
    duplicate_name_indices = mlf.df_out[mlf.df_out[mlf.split_colname] != "submission"][
        "name"
    ].isin(mlf.df_out[mlf.df_out[mlf.split_colname] == "submission"]["name"].tolist())
    duplicate_name_indices = duplicate_name_indices[duplicate_name_indices]
    duplicate_names = mlf.df_out.loc[duplicate_name_indices.index, "name"]
    for name in duplicate_names:
        mlf.df_out.loc[mlf.df_out["name"] == name, pred] = mlf.df_out.loc[
            mlf.df_out["name"] == name
        ][mlf.target_name].max()

mlf.evaluate()
mlf.get_feature_importances()

print(mlf.feature_importances)
print(mlf.evaluator.evaluation_output)

# df_catboost.to_csv(TRAIN_LOC.replace("train.csv", "df_catboost2.csv"), index=False)
# df_xgb.to_csv(TRAIN_LOC.replace("train.csv", "df_xgb2.csv"), index=False)

# subprocess.run('kaggle competitions submit -c titanic -f ~/.kaggle/titanic/df_catboost2.csv -m "mlf"')
