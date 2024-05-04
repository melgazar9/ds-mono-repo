from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *

# Create a predictable dataset where every time the category A shows up it's a positive case.
# It would be extremely disappointing if the model can't score 100% accuracy on this simple task.

df = pd.DataFrame(
    {
        "feature1": [
            "A",
            "B",
            "C",
            "A",
            "A",
            "B",
            "C",
            "A",
            "B",
            "D",
            "E",
            "C",
            "A",
            "A",
            "B",
        ],
        "preserve_var": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "target": [1, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0],
    }
)

mlf = SklearnMLFlow(
    df=df,
    input_features=[i for i in df.columns if i not in ["preserve_var"] + ["target"]],
    target_name="target",
    preserve_vars=["preserve_var", "dataset_split"],
    feature_creator=None,
    splitter=SimpleSplitter(train_pct=0.75, val_pct=0.1),
    feature_transformer=FeatureTransformer(
        target_name="target", preserve_vars="preserve_var"
    ),
    algorithms=[
        CatBoostClassifier(iterations=300, learning_rate=0.02, random_state=9),
        XGBClassifier(learning_rate=1, random_state=9),
    ],
    optimizer=ScoreThresholdOptimizer(accuracy_score),
    evaluator=GenericMLEvaluator(
        classification_or_regression="classification", groupby_cols="dataset_split"
    ),
)

mlf.split()
mlf.transform_features()
mlf.train()
mlf.predict()

mlf.algorithms[0].predict(mlf.df_out[mlf.output_features]) == mlf.df_out["target"]
mlf.algorithms[1].predict(mlf.df_out[mlf.output_features]) == mlf.df_out["target"]
