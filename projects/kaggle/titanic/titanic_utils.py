from ds_core.sklearn_workflow.ml_utils import *


class TitanicFeatureCreator:
    def get_survived_categories(self, df, features=None, target_name="survived"):

        features = [features] if isinstance(features, str) else features
        features = (
            [i for i in df.columns if i != target_name]
            if features is None
            else features
        )

        self.survival_categories = {}
        for feature in features:
            self.survival_categories[feature] = {}
            for v in df[feature]:
                if not pd.isnull(v):
                    self.survival_categories[feature].update(
                        {
                            df.loc[df[feature] == v, feature].iloc[0]: max(
                                df.loc[df[feature] == v, target_name].max(), 0
                            )
                        }
                    )

        return self

    def fit_transform(self, df, features=("name", "cabin")):
        self.get_survived_categories(df, features=features)

        for key, d in self.survival_categories.items():
            for k, v in d.items():
                df.loc[df[key] == k, key + "_survived"] = max(d[k], 0)
            df[key + "_survived"] = df[key + "_survived"].fillna(2)

        return df


class TitanicSplitter(AbstractSplitter):
    def __init__(self, train_pct=0.7, val_pct=0.15):
        super().__init__()
        self.train_pct = train_pct
        self.val_pct = val_pct

    def split(self, df):
        df = df.reset_index(drop=True)
        non_submission_shape = df[df["survived"].notnull()].shape[0]
        df.loc[0 : int(non_submission_shape * self.train_pct), self.split_colname] = (
            "train"
        )

        df.loc[
            int(non_submission_shape * self.train_pct)
            + 1 : np.ceil(non_submission_shape * self.train_pct)
            + np.floor(non_submission_shape * self.val_pct),
            self.split_colname,
        ] = "val"

        df[self.split_colname] = df[self.split_colname].fillna("test")
        df.loc[df["survived"].isnull(), self.split_colname] = "submission"

        return df
