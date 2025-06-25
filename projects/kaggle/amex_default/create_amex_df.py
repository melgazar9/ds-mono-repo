from amex_utils import *
from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *

start = time.time()

### global variables ###

TRAIN_LOC = "~/.kaggle/amex-default-prediction/train_data.csv"
TRAIN_LABELS_LOC = "~/.kaggle/amex-default-prediction/train_labels.csv"
TEST_LOC = "~/.kaggle/amex-default-prediction/test_data.csv"

TARGET_NAME = "target"
PRESERVE_VARS = ["PassengerId", "Name", "name_survived", "dataset_split"]


### load dfs ###

df_train_orig = pd.read_csv(TRAIN_LOC)
df_train_labels_orig = pd.read_csv(TRAIN_LABELS_LOC)
df_test_orig = pd.read_csv(TEST_LOC)

df_train_orig = df_train_orig.set_index("customer_ID")
df_train_labels_orig = df_train_labels_orig.set_index("customer_ID")
df_test_orig = df_test_orig.set_index("customer_ID")

df_train = df_train_orig.join(df_train_labels_orig)
assert df_train["target"].isnull().sum() == 0
df_test_orig["target"] = np.nan

assert len([i for i in df_train.columns if i not in df_test_orig.columns]) == 0
assert len([i for i in df_test_orig.columns if i not in df_train.columns]) == 0

col_order = list(df_train.columns)
df = pd.concat([df_train[col_order], df_test_orig[col_order]])

### save the df ###

df.reset_index(inplace=True)
df.to_feather("~/.kaggle/amex-default-prediction/train_data.csv".replace("train_data.csv", "df_amex.feather"))

print("\nTotal time taken:", round((time.time() - start) / 60, 3), "minutes\n")
