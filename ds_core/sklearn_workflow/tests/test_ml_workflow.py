from ds_core.ds_imports import *
from ds_core.sklearn_workflow.ml_utils import *
from sklearn.datasets import load_iris

data = load_iris()

X_train, X_val, y_train, y_val = train_test_split(data['data'], data['target'])
X_val, X_test, y_val, y_test = train_test_split(X_val, y_val)

X_train, X_val, X_test = \
    pd.DataFrame(X_train, columns=data['feature_names']),\
    pd.DataFrame(X_val, columns=data['feature_names']),\
    pd.DataFrame(X_test, columns=data['feature_names'])

X_train.loc[:, 'dataset_split'] = 'train'
X_val.loc[:, 'dataset_split'] = 'val'
X_test.loc[:, 'dataset_split'] = 'test'

df = pd.concat([
    pd.concat([X_train, X_val, X_test]),
    pd.concat([pd.Series(y_train), pd.Series(y_val), pd.Series(y_test)]).rename('target')
    ],
    axis=1)

mlf = SklearnMLflow(df=df,
                    target='target',
                    input_features=data['feature_names'],
                    algorithms=[XGBClassifier(), LGBMClassifier()])

mlf.transform_features()
mlf.train_models()
mlf.predict_models()
mlf.df_out