from ds_core.sklearn_workflow.ml_utils import *

class TitanicFeatureCreator:

    def get_survived_categories(self, df, features=None, target_name='survived'):

        features = [features] if isinstance(features, str) else features
        features = [i for i in df.columns if i != target_name] if features is None else features

        self.survival_categories = {}
        for feature in features:
            self.survival_categories[feature] = {}
            for v in df[feature]:
                if not pd.isnull(v):
                    self.survival_categories[feature].update(
                        {df.loc[df[feature] == v, feature].iloc[0]: max(df.loc[df[feature] == v, target_name].max(), 0)}
                    )

        return self


    def fit_transform(self, df, features=('name', 'cabin')):
        self.get_survived_categories(df, features=features)

        for key, d in self.survival_categories.items():
            for k, v in d.items():
                df.loc[df[key] == k, key + '_survived'] = max(d[k], 0)
            df[key + '_survived'].fillna(0, inplace=True)

        return df

class TitanicSplitter(AbstractSplitter):

    def __init__(self, train_pct=0.7, val_pct=0.15):
        super().__init__()
        self.train_pct = train_pct
        self.val_pct = val_pct

    def split(self, df):
        df.reset_index(drop=True, inplace=True)
        non_submission_shape = df[df['survived'].notnull()].shape[0]
        df.loc[0: int(non_submission_shape * self.train_pct), self.split_colname] = 'train'

        df.loc[int(non_submission_shape * self.train_pct) + 1:
               np.ceil(non_submission_shape * self.train_pct) + np.floor(non_submission_shape * self.val_pct),
        self.split_colname] = 'val'

        df[self.split_colname].fillna('test', inplace=True)
        df.loc[df['survived'].isnull(), self.split_colname] = 'submission'

        return df

class TitanicOptimizer(ScoreThresholdOptimizer):

    def __init__(self):
        super().__init__()

    def run_optimization(self):

        self.optimize()
        ### assess the predicted class from the chosen optimized threshold(s) by split ###

        if splits_to_assess is not None:
            self.threshold_opt_results_by_split = pd.DataFrame()
            for pred_class in [i + '_class' for i in fits]:
                _threshold_results_by_split = \
                    pd.DataFrame(
                        self.df_out[self.df_out[self.split_colname].isin(splits_to_assess)] \
                            .groupby(self.split_colname) \
                            .apply(lambda x: self.optimizer.optimization_func(x[self.target_name], x[pred_class])),
                        columns=[pred_class])

                self.threshold_opt_results_by_split = \
                    pd.concat([self.threshold_opt_results_by_split, _threshold_results_by_split], axis=1)

            self.threshold_opt_results_by_split.index = \
                self.threshold_opt_results_by_split.index.astype(pd.CategoricalDtype(splits_to_assess, ordered=True))
            self.threshold_opt_results_by_split.sort_index(inplace=True)
        return self