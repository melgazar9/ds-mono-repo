# from kaggle.amex_default.amex_utils import *
from amex_utils import *  # noqa: F403, F405
from ds_core.ds_imports import *  # noqa: F403, F405
from ds_core.ds_utils import *  # noqa: F403, F405
from ds_core.sklearn_workflow.ml_utils import *  # noqa: F403, F405

np.random.seed(9)

### global variables ###

start = time.time()

# DF_LOC = '/Users/melgazar9/.kaggle/amex-default-prediction/df_amex.feather'
DF_LOC = "/Users/melgazar9/.kaggle/amex-default-prediction/df_amex_slim.feather"

df = pd.read_feather(DF_LOC)

# df = pd.concat([df.head(10000), df.tail(10000)]) # for debugging

TARGET_NAME = "target"
PRESERVE_VARS = ["S_2", "customer_ID", "dataset_split"]
CUSTOMER_HISTORY_COLS = ["B_38", "D_39", "D_89"]
CUSTOMER_HISTORY_CAT_COLS = ["B_38"]


### initialize feature transformer ###

### train a model ###

mlf = SklearnMLFlow(
    df=df,
    input_features=[i for i in df.columns if i not in PRESERVE_VARS + [TARGET_NAME]],
    target_name=TARGET_NAME,
    preserve_vars=PRESERVE_VARS,
    feature_creator=AmexFeatureCreator(
        customer_history_cols=CUSTOMER_HISTORY_COLS,
        customer_history_cat_cols=CUSTOMER_HISTORY_CAT_COLS,
    ),
    splitter=AmexSplitter(train_pct=0.70, val_pct=0.15),
    feature_transformer=FeatureTransformer(
        target_name=TARGET_NAME, preserve_vars=PRESERVE_VARS
    ),
    algorithms=[
        CatBoostClassifier(iterations=300, learning_rate=0.02, random_state=9),
        XGBClassifier(learning_rate=0.05, max_depth=3, random_state=9),
    ],
    optimizer=ScoreThresholdOptimizer(f1_score),
    evaluator=GenericMLEvaluator(
        classification_or_regression="classification", groupby_cols="dataset_split"
    ),
)

mlf.split()
mlf.create_features()
mlf.transform_features()
mlf.train()
mlf.predict()
mlf.optimize("maximize")
mlf.evaluate()
mlf.get_feature_importances()

print(f"\n{mlf.evaluator.evaluation_output}\n")

print("\nTotal time taken:", round((time.time() - start) / 60, 3), "minutes\n")

### submission ###

df_catboost = (
    mlf.df_out[mlf.df_out[mlf.split_colname] == "submission"][
        ["customer_id", "CatBoostClassifier_pred"]
    ]
    .groupby("customer_id")
    .mean()
    .rename({"customer_id": "customer_ID", "CatBoostClassifier_pred": "prediction"})
)

# subprocess.run('kaggle competitions submit -c amex-default-prediction -f
#      ~/.kaggle/amex-default-prediction/df_catboost.csv -m "mlf"')
