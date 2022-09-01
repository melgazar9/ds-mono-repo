from ds_core.sklearn_workflow.ml_utils import *

class KaggleTitanicSplitter(AbstractSplitter):

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


def get_survived_categories(df, features, target_name='Survived'):
    features = [features] if isinstance(features, str) else features
    survival_categories = {}
    for feature in features:
        survival_categories[feature] = {}
        for v in df[feature]:
            if not pd.isnull(v):
                survival_categories[feature].update(
                    {df.loc[df[feature] == v, feature].iloc[0]: max(df.loc[df[feature] == v, target_name].max(), 0)}
                )

    return survival_categories