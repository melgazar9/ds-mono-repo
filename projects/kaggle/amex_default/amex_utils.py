from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_utils import *


class AmexFeatureCreator:
    def __init__(
        self,
        id_col="customer_id",
        datetime_cols=("s_2"),
        customer_history_cols=None,
        customer_history_cat_cols=None,
        target="target",
    ):
        self.id_col = id_col
        self.datetime_cols = datetime_cols
        self.customer_history_cols = customer_history_cols
        self.customer_history_cat_cols = customer_history_cat_cols
        self.target = target

        self.customer_history_cols = (
            [] if self.customer_history_cols is None else self.customer_history_cols
        )
        self.customer_history_cat_cols = (
            []
            if self.customer_history_cat_cols is None
            else self.customer_history_cat_cols
        )
        self.datetime_feature_creator = None
        self.ordinal_encoder = None
        self._customer_history_cols = None

    def fit_datetime_features(self, df):
        self.datetime_feature_creator = FunctionTransformer(
            lambda x: create_datetime_features(x, datetime_cols=self.datetime_cols)
        )

        self.datetime_feature_creator.fit(df)

        return self

    def fit_transform_customer_history(self, df):
        """
        NOTE
        ----
        This method is not yet designed for production use. FunctionTransformer does not store the
        information passed to it. To make this code work on new Amex predictions, concatenate the
        full df (train/val/test/unseen) and then apply the fit_transform method to the full dataset.
        """

        ### attempt colmapping fix ###

        input_mapping = dict(
            id_col=self.id_col,
            datetime_cols=self.datetime_cols,
            customer_history_cols=self.customer_history_cols,
            customer_history_cat_cols=self.customer_history_cat_cols,
            target=self.target,
        )

        for col_group in input_mapping.keys():
            if isinstance(input_mapping[col_group], str):
                input_mapping[col_group] = [input_mapping[col_group]]
            for col in input_mapping[col_group]:
                if col not in df.columns:
                    cleaned_col = list(
                        clean_columns(pd.DataFrame(columns=[col])).columns
                    )[0]
                    if cleaned_col in df.columns:
                        input_mapping[col_group] = list(
                            map(
                                lambda x: x.replace(col, cleaned_col),
                                input_mapping[col_group],
                            )
                        )
                    else:
                        raise ValueError(
                            f"Could not find column {col} or {cleaned_col}"
                        )

        self.id_col = input_mapping.get("id_col")[0]
        self.datetime_cols = input_mapping["datetime_cols"]
        self.customer_history_cols = input_mapping["customer_history_cols"]
        self.customer_history_cat_cols = input_mapping["customer_history_cat_cols"]
        self.target = input_mapping.get("target")[0]

        # fit_transform df

        if len(self.customer_history_cat_cols):
            self.ordinal_encoder = OrdinalEncoder(
                handle_unknown="use_encoded_value",
                unknown_value=-99,
                encoded_missing_value=np.nan,
            )

            df[self.customer_history_cat_cols] = self.ordinal_encoder.fit_transform(
                df[self.customer_history_cat_cols]
            )

        self._customer_history_cols = list(
            set(
                self.customer_history_cat_cols
                + self.customer_history_cols
                + [self.id_col]
            )
        )

        df = create_customer_history(
            df, generate_lagging_mean_cols=self.customer_history_cols
        )

        return df

    def fit_transform(self, df):
        """For this project, I reduced the complexity to only support fit_transform for feature creation"""

        ### datetime_features creator ###

        # self.fit_datetime_features(X)
        # X = self.datetime_feature_creator.transform(X)

        ### customer history creator ###

        df = self.fit_transform_customer_history(df)

        return df


def create_datetime_features(df, datetime_cols, include_hour=False, make_copy=False):

    if make_copy:
        df = df.copy()

    datetime_cols = [datetime_cols] if isinstance(datetime_cols, str) else datetime_cols

    assert isinstance(datetime_cols, (tuple, list))

    for dt_col in datetime_cols:

        if not is_datetime(df[dt_col]):
            df[dt_col] = pd.to_datetime(df[dt_col])

        if include_hour:
            df.loc[:, "hour"] = df[dt_col].dt.hour

        df.loc[:, f"{dt_col}_day"] = df[dt_col].dt.isocalendar().day
        df.loc[:, f"{dt_col}_week"] = df[dt_col].dt.isocalendar().week
        df.loc[:, f"{dt_col}_month"] = df[dt_col].dt.month
        df.loc[:, f"{dt_col}_dayofweek"] = df[dt_col].dt.dayofweek
        df.loc[:, f"{dt_col}_dayofyear"] = df[dt_col].dt.dayofyear
        df.loc[:, f"{dt_col}_quarter"] = df[dt_col].dt.quarter
        df.loc[:, f"{dt_col}_is_month_start"] = df[dt_col].dt.is_month_start
        df.loc[:, f"{dt_col}_is_month_end"] = df[dt_col].dt.is_month_end
        df.loc[:, f"{dt_col}_is_quarter_start"] = df[dt_col].dt.is_quarter_start
        df.loc[:, f"{dt_col}_is_quarter_end"] = df[dt_col].dt.is_quarter_end

        holidays = calendar().holidays(
            start=str(np.min(np.array(df[dt_col]))),
            end=str(np.max(np.array(df[dt_col]))),
        )

        df.loc[:, f"{dt_col}_is_holiday"] = df[dt_col].isin(holidays)

    return df


def find_abnormal_features(df, pct_na_thres=0.85, copy=True):

    df = df.copy() if copy else df

    single_value_cols = []
    excessive_na_cols = dict()
    nrows = df.shape[0]
    for col in df.columns:
        if df[col].nunique() == 1:
            single_value_cols.append(col)
            continue

        pct_nas = df[col].isna().sum() / nrows

        if pct_nas >= pct_na_thres:
            excessive_na_cols[col] = pct_nas

    return dict(
        single_value_cols=single_value_cols, excessive_na_cols=excessive_na_cols
    )


def create_customer_history(
    df,
    customer_id_col="customer_id",
    generate_lagging_mean_cols=None,
    n_jobs_lag=mp.cpu_count(),
):
    """
    Description
    -----------
    Creates lagging customer history features within the same dataset.
    Runs inplace to save memory.
    Parameters
    ----------
    :param df: pandas df (not vaex!)
    :param customer_id_col: str of customer id column
    :param generate_lagging_mean_cols: list of cols to apply time-dependant lagging mean to
    :param n_jobs_lag: number of workers that will run when calling dask.compute for parallel mean lagging features
    :return:
    """

    ### customer history columns ###

    if "n" in df.columns:
        warnings.warn("n is in df.columns and will be overwritten!")

    df["n"] = 1

    df["n_prev_ids"] = df.groupby(customer_id_col)["n"].transform(
        lambda x: x.shift().expanding().sum().fillna(0)
    )

    if isinstance(generate_lagging_mean_cols, (list, tuple)):

        if n_jobs_lag == 1:
            for feature in generate_lagging_mean_cols:
                df["mean_prev_" + feature] = df.groupby(customer_id_col)[
                    feature
                ].transform(lambda x: x.shift().expanding().mean().fillna(0))
        else:

            def parallelize_lag(_df, feature):
                _df["mean_prev_" + feature] = _df.groupby(customer_id_col)[
                    feature
                ].transform(lambda x: x.shift().expanding().mean().fillna(0))
                return _df

            num_workers = np.abs(n_jobs_lag)

            delayed_list = [
                delayed(parallelize_lag(df, feature))
                for feature in generate_lagging_mean_cols
            ]
            tuple_of_dfs = dask.compute(*delayed_list, num_workers=num_workers)
            list_of_dfs = [tuple_of_dfs[i] for i in range(len(tuple_of_dfs))]

            df = reduce(
                lambda x, y: pd.merge(
                    x,
                    y[[i for i in y.columns if i not in x.columns]],
                    how="inner",
                    left_index=True,
                    right_index=True,
                ),
                list_of_dfs,
            )

    df = df.drop("n", axis=1)
    return df


class CalcAmexMetrics:
    """
    Description
    -----------
    Calculate standard ML metrics for the kaggle Amex competition
    Parameters
    ----------
    df: pandas df to evaluate
    target: str of target column name
    fits: list of pred_cols to evaluate (e.g. ['XGBClassifier_pred', 'dummy_pred'])
    split_colname: str of split colname (e.g. 'dataset_split')
    round_importance_decimals: int to round the output feature importance text for easier reading
    """

    def __init__(
        self,
        df,
        target,
        fits,
        split_colname="dataset_split",
        round_importance_decimals=3,
    ):

        self.df = df
        self.target = target
        self.fits = fits
        self.split_colname = split_colname
        self.round_importance_decimals = round_importance_decimals

        assert (
            self.df[self.target].isnull().sum() == 0
        ), "There cannot be NAs in the target variable"

    @staticmethod
    def amex_metric(y_true: pd.Series, y_pred: pd.Series) -> float:
        """
        Description
        -----------
        From the Amex Kaggle site:
            The evaluation metric, M, for this competition is the mean of two measures of rank ordering:
            Normalized Gini Coefficient, G, and default rate captured at 4%, D.
        """

        def top_four_percent_captured(
            y_true: pd.DataFrame, y_pred: pd.DataFrame
        ) -> float:
            df = pd.concat([y_true, y_pred], axis="columns").sort_values(
                y_pred.name, ascending=False
            )
            df["weight"] = df[y_true.name].apply(lambda x: 20 if x == 0 else 1)
            four_pct_cutoff = int(0.04 * df["weight"].sum())
            df["weight_cumsum"] = df["weight"].cumsum()
            df_cutoff = df.loc[df["weight_cumsum"] <= four_pct_cutoff]
            return (df_cutoff[y_true.name] == 1).sum() / (df[y_true.name] == 1).sum()

        def weighted_gini(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:
            df = pd.concat([y_true, y_pred], axis="columns").sort_values(
                y_pred.name, ascending=False
            )
            df["weight"] = df[y_true.name].apply(lambda x: 20 if x == 0 else 1)
            df["random"] = (df["weight"] / df["weight"].sum()).cumsum()
            total_pos = (df[y_true.name] * df["weight"]).sum()
            df["cum_pos_found"] = (df[y_true.name] * df["weight"]).cumsum()
            df["lorentz"] = df["cum_pos_found"] / total_pos
            df["gini"] = (df["lorentz"] - df["random"]) * df["weight"]
            return df["gini"].sum()

        def normalized_weighted_gini(
            y_true: pd.DataFrame, y_pred: pd.DataFrame
        ) -> float:
            y_true_pred = y_true.copy()
            y_true_pred.name = "prediction"
            return weighted_gini(y_true, y_pred) / weighted_gini(y_true, y_true_pred)

        g = normalized_weighted_gini(y_true, y_pred)
        d = top_four_percent_captured(y_true, y_pred)

        return 0.5 * (g + d)

    @staticmethod
    def create_metric_dict(y_true, y_pred, threshold=0.5):
        """
        Parameters
        ----------
        y_true: pd.Series of actual y value
        y_pred: pd.Series of predicted y_value score (not the class)
        threshold: float value - if pred score is > than the threshold, flag prediction to be 1
        ----------
        Returns
        dictionary of classification scores
        -------
        """

        output_dict = dict(
            amex_metric=CalcAmexMetrics.amex_metric(y_true, y_pred),
            accuracy=accuracy_score(y_true, y_pred > threshold),
            precision=precision_score(y_true, y_pred > threshold),
            recall=recall_score(y_true, y_pred > threshold),
            F1=f1_score(y_true, y_pred > threshold),
        )

        return output_dict

    def calc_amex_metrics(
        self, groupby_split=True, melt_id_vars=None, dataset_order=None, drop_nas=True
    ):
        """
        Description
        -----------
        Select the datasets and needed columns
        melt the multiple fit columns into longer df w/ fit & fit_values columns
        drop NAs if specified
        group by dataset & fit
        apply the metric fn to each group
        cast the results to a dict and then df
        melt the df
        change the index of metric names to a column
        rename the columns appropriately
        sort the dataset values
        create a rounded column of the metric values for the plot's text
        Parameters
        ----------
        groupby_split: Bool whether to groupby.apply self.split_colname
        melt_id_vars: list of cols to melt when creating the ML metric summary
        dataset_order: dict to reorganize the output summary table for easier reading
            (e.g. {'train': 0, 'val': 1, 'test': 2})
        drop_nas: Bool to drop NA values before evaluation
        Returns: dict of ML metrics
        """

        if dataset_order is None:
            dataset_order = {"train": 0, "val": 1, "test": 2}

        melt_id_vars = [self.split_colname] if melt_id_vars is None else melt_id_vars
        all_needed_cols = list(set(melt_id_vars + self.fits + [self.target]))

        if groupby_split:
            if len(melt_id_vars) == 1:
                melted_metrics_df = (
                    self.df.loc[:, all_needed_cols]
                    .melt(
                        id_vars=list(set(list(melt_id_vars) + [self.target])),
                        var_name="fit",
                        value_name="value",
                    )
                    .pipe(lambda df: df.dropna() if drop_nas else df)
                    .groupby([self.split_colname, "fit"])
                    .apply(
                        lambda df: self.create_metric_dict(df[self.target], df["value"])
                    )
                    .pipe(lambda x: pd.DataFrame(dict(x)))
                    .pipe(pd.melt, ignore_index=False)
                    .reset_index()
                    .rename(
                        columns=dict(
                            index="metric",
                            variable_0=self.split_colname,
                            variable_1="fit",
                        )
                    )
                    .sort_values(
                        by=self.split_colname, key=lambda x: x.map(dataset_order)
                    )
                    .assign(
                        value=lambda df: df.value.values.round(
                            self.round_importance_decimals
                        )
                    )
                )

            else:
                melted_metrics_df = (
                    self.df.loc[:, all_needed_cols]
                    .melt(
                        id_vars=list(set(list(melt_id_vars) + [self.target])),
                        var_name="fit",
                        value_name="value",
                    )
                    .pipe(lambda df: df.dropna() if drop_nas else df)
                    .groupby(list(melt_id_vars) + ["fit"])
                    .apply(
                        lambda df: self.create_metric_dict(df[self.target], df["value"])
                    )
                    .pipe(
                        lambda df: pd.concat(
                            [
                                pd.DataFrame(df),
                                pd.json_normalize(pd.DataFrame(df)[0]).set_index(
                                    pd.DataFrame(df).index
                                ),
                            ],
                            axis=1,
                            sort=False,
                        )
                    )
                    .drop(0, axis=1)
                    .melt(ignore_index=False)
                    .reset_index()
                    .rename(
                        columns=dict(
                            index="metric",
                            variable_0=self.split_colname,
                            variable_1="fit",
                        )
                    )
                    .sort_values(
                        by=self.split_colname, key=lambda x: x.map(dataset_order)
                    )
                    .assign(
                        value=lambda df: df.value.values.round(
                            self.round_importance_decimals
                        )
                    )
                )
        else:
            melted_metrics_df = (
                df[self.fits + [self.target]]
                .melt(id_vars=self.target, value_name="value")
                .pipe(
                    lambda df: pd.DataFrame.from_dict(
                        self.create_metric_dict(df[self.target], df["value"]),
                        orient="index",
                    )
                )
                .pipe(lambda df: df.reset_index())
            )

            melted_metrics_df.columns = ["metric", "value"]

        return melted_metrics_df

    def plot(
        self,
        metrics_df=None,
        x="metric",
        y="value",
        facet_col="fit",
        barmode="group",
        facet_col_wrap=None,
        facet_row=None,
        height=None,
        width=None,
        text_position="outside",
        color=None,
        y_matches=None,
        save_plot_loc=None,
        title=None,
    ):

        metrics_df = self.calc_amex_metrics() if metrics_df is None else metrics_df
        color = self.split_colname if color is None else color

        metrics_df["text"] = (
            (metrics_df["value"] * 100).round(2).astype(str).pipe(lambda x: x + "%")
        )

        fig = px.bar(
            metrics_df,
            x=x,
            y=y,
            facet_col=facet_col,
            facet_row=facet_row,
            facet_col_wrap=facet_col_wrap,
            color=color,
            barmode=barmode,
            height=height,
            width=width,
            text="text",
            title=title,
        )

        fig.update_traces(textposition=text_position)
        fig.update_yaxes(
            range=[metrics_df[y].min() - 0.05, metrics_df[y].max() + 0.05],
            matches=y_matches,
        )

        if save_plot_loc:
            plotly.offline.plot(fig, filename=save_plot_loc)

        return fig

    def evaluate(self, **kwargs):
        self.plot(**kwargs)
        return


def focal_loss(alpha=0.25, gamma=1):
    from scipy.misc import derivative

    def custom_loss(y_pred, y_true):
        a, g = alpha, gamma

        def fl(x, t):
            p = 1 / (1 + np.exp(-x))
            return (
                -(a * t + (1 - a) * (1 - t))
                * ((1 - (t * p + (1 - t) * (1 - p))) ** g)
                * (t * np.log(p) + (1 - t) * np.log(1 - p))
            )

        # partial_fl = lambda x: fl(x, y_true)  # I believe this was written by
        # amex themselves, but it fails flake8 check E722
        def partial_fl(x):
            return fl(x, y_true)

        grad = derivative(partial_fl, y_pred, n=1, dx=1e-6)
        hess = derivative(partial_fl, y_pred, n=2, dx=1e-6)
        return grad, hess

    return custom_loss


def amex_loss():
    def amex_metric(y_true: pd.Series, y_pred: pd.Series) -> float:
        """
        Description
        -----------
        From the Amex Kaggle site:
            The evaluation metric, M, for this competition is the mean of two measures of rank ordering:
            Normalized Gini Coefficient, G, and default rate captured at 4%, D.
        """

        def top_four_percent_captured(
            y_true: pd.DataFrame, y_pred: pd.DataFrame
        ) -> float:

            y_true_s = pd.Series(y_true)
            y_pred_s = pd.Series(y_pred)
            y_true_s.name = "y_true"
            y_pred_s.name = "y_pred"

            df = pd.concat([y_true_s, y_pred_s], axis="columns").sort_values(
                y_pred_s.name, ascending=False
            )

            df["weight"] = df[y_true_s.name].apply(lambda x: 20 if x == 0 else 1)
            four_pct_cutoff = int(0.04 * df["weight"].sum())
            df["weight_cumsum"] = df["weight"].cumsum()
            df_cutoff = df.loc[df["weight_cumsum"] <= four_pct_cutoff]
            return (df_cutoff[y_true_s.name] == 1).sum() / (
                df[y_true_s.name] == 1
            ).sum()

        def weighted_gini(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:

            y_true_s = pd.Series(y_true)
            y_pred_s = pd.Series(y_pred)
            y_true_s.name = "y_true"
            y_pred_s.name = "y_pred"

            df = pd.concat([y_true_s, y_pred_s], axis="columns").sort_values(
                y_pred_s.name, ascending=False
            )
            df["weight"] = df[y_true_s.name].apply(lambda x: 20 if x == 0 else 1)
            df["random"] = (df["weight"] / df["weight"].sum()).cumsum()
            total_pos = (df[y_true_s.name] * df["weight"]).sum()
            df["cum_pos_found"] = (df[y_true_s.name] * df["weight"]).cumsum()
            df["lorentz"] = df["cum_pos_found"] / total_pos
            df["gini"] = (df["lorentz"] - df["random"]) * df["weight"]
            return df["gini"].sum()

        def normalized_weighted_gini(
            y_true: pd.DataFrame, y_pred: pd.DataFrame
        ) -> float:
            # y_true_pred = y_true.copy()
            # y_true_pred.name = 'prediction'
            return weighted_gini(y_true, y_pred) / weighted_gini(y_true, y_true)

        g = normalized_weighted_gini(y_true, y_pred)
        d = top_four_percent_captured(y_true, y_pred)
        return np.array([g]), np.array([d])
        # return 0.5 * (g + d)

    return amex_metric


class AmexSplitter(AbstractSplitter):
    def __init__(self, train_pct=0.70, val_pct=0.15):
        super().__init__()
        self.train_pct = train_pct
        self.val_pct = val_pct

    def split(self, df, train_pct=0.70, val_pct=0.15, target_name="target"):
        df = df.reset_index(drop=True)
        non_submission_shape = df[df[target_name].notnull()].shape[0]

        df.loc[
            0 : int(non_submission_shape * self.train_pct), self.split_colname
        ] = "train"

        df.loc[
            int(non_submission_shape * self.train_pct)
            + 1 : np.ceil(non_submission_shape * self.train_pct)
            + np.floor(non_submission_shape * self.val_pct),
            self.split_colname,
        ] = "val"

        df[self.split_colname] = df[self.split_colname].fillna("test")
        df.loc[df[target_name].isnull(), self.split_colname] = "submission"
        return df
