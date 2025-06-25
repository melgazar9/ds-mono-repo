# -*- coding: utf-8 -*-
"""
Created on Sat Jan 30 10:57:45 2021

@author: melgazar9
"""

import inspect

from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_imports import *


class MLFlowLogger:
    """Logger classes"""

    def __init__(self, log_level=logging.INFO):
        self.log_level = log_level

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        ch = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s  %(name)s:  %(levelname)s  %(message)s")
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)


class SklearnMLFlow(MLFlowLogger):
    """
    Description
    -----------
    Automated machine learning workflow pipeline that runs ML steps in the proper sequence
    """

    def __init__(
        self,
        df,
        input_features,
        target_name=None,
        split_colname="dataset_split",
        preserve_vars=None,
        clean_column_names=True,
        splitter=None,
        feature_creator=None,
        feature_transformer=None,
        resampler=None,
        algorithms=(),
        optimizer=None,
        evaluator=None,
    ):
        super().__init__()

        self.df_input = df.copy()
        self.df_out = df.copy()
        self.target_name = target_name
        self.split_colname = split_colname
        self.clean_column_names = clean_column_names

        self.new_features = None
        self.new_feature_groups = None
        self.feature_groups = None
        self.output_cols = None
        self.df_train_resampled = None
        self.threshold_opt_name = None
        self.feature_importances = None

        if input_features is None:
            if self.target_name is not None:
                # supervised learning set all columns != self.target_name as input features
                self.input_features = list(self.df_input.drop(self.target_name, axis=1).columns)
            elif self.target_name is None:
                # unsupervised learning set all columns as input features
                self.input_features = list(self.df_input.columns)
        else:
            self.input_features = input_features

        self.output_features = input_features.copy()
        self.preserve_vars = (
            preserve_vars
            if preserve_vars is not None
            else [i for i in self.df_input.columns if i not in self.input_features + [self.target_name]]
        )

        if self.split_colname not in self.preserve_vars:
            self.preserve_vars.append(self.split_colname)

        if self.clean_column_names:
            self.df_out = clean_columns(self.df_out)
            self.input_features = clean_columns(pd.DataFrame(columns=self.input_features)).columns.tolist()
            self.output_features = clean_columns(pd.DataFrame(columns=self.output_features)).columns.tolist()
            self.preserve_vars = clean_columns(pd.DataFrame(columns=self.preserve_vars)).columns.tolist()
            self.target_name = clean_columns(pd.DataFrame(columns=[self.target_name])).columns[0]

        self.splitter = splitter
        self.feature_creator = feature_creator
        self.feature_transformer = (
            feature_transformer
            if feature_transformer is not None
            else FeatureTransformer(target_name=self.target_name, preserve_vars=self.preserve_vars)
        )
        self.resampler = resampler
        self.algorithms = algorithms
        self.optimizer = optimizer
        self.evaluator = evaluator
        self.input_cols = self.input_features + self.preserve_vars

        if self.target_name not in self.input_cols:
            self.input_cols.append(self.target_name)

        assert len(self.input_cols) == len(
            set(self.input_cols)
        ), "Not all features were considered! Check your inputs: input_features, preserve_vars, target_name"

    def split(self):
        self.df_out = self.splitter.split(self.df_out)
        return self

    def create_features(self):
        """
        Description
        -----------
        This is the first step in the ML workflow.
        This method creates features, and currently only supports fit_transform for complexity reasons.
        Input: self.df_input
        Output: self.df_output
        """

        self.df_out = self.feature_creator.fit_transform(self.df_out)
        self.output_features = [i for i in self.df_out.columns if i not in self.preserve_vars + [self.target_name]]
        self.new_features = list(set(self.output_features) - set(self.input_features))

        return self

    def transform_features(self, feature_groups=None):
        """
        Description
        -----------
        The second stage of the ML workflow. It takes as input the output from self.create_features.
        If self.create_features isn't called (e.g. feature_creator isn't defined), then
            self.df_input is the input df, otherwise self.df_out is the input df.

        Parameters
        ----------
        feature_groups: dict of input parameters passed to the FeatureTransformer class
            Example: dict(lc_features=['low_card_feature1', 'low_card_feature2'],
                          hc_features=['high_card_feature1'],
                          numeric_features=['numeric_feature1'])
        """

        if hasattr(self, "new_features") and self.new_features is not None and len(self.new_features):

            self.feature_transformer.instantiate_column_transformer(
                self.df_out[self.df_out[self.split_colname] == "train"],
                self.df_out[self.df_out[self.split_colname] == "train"][self.target_name],
            )

            ### Assign feature groups ###

            if feature_groups is None:
                # assign new features to feature groups
                self.new_feature_groups = FeatureTransformer(
                    target_name=self.target_name, preserve_vars=self.preserve_vars
                ).detect_feature_groups(self.df_out[self.new_features])

                # append the existing feature groups with the new features
                for fg in self.new_feature_groups.keys():
                    if len(self.new_feature_groups[fg]) > 0:
                        self.feature_transformer.feature_groups[fg] = list(
                            set(self.feature_transformer.feature_groups[fg] + self.new_feature_groups[fg])
                        )
            else:
                self.feature_groups = feature_groups

        self.feature_transformer.target_name = self.target_name
        self.feature_transformer.preserve_vars = self.preserve_vars

        self.feature_transformer.fit(
            self.df_out[self.df_out[self.split_colname] == "train"],
            self.df_out[self.df_out[self.split_colname] == "train"][self.target_name],
        )

        self.df_out = self.feature_transformer.transform(self.df_out)
        self.output_cols = self.feature_transformer.output_cols
        self.output_features = self.feature_transformer.output_features
        return self

    def resample(self):
        if self.resampler is not None:
            try:
                self.df_train_resampled = self.resampler.fit_resample(
                    self.df_out[self.df_out[self.split_colname] == "train"],
                    self.df_out[self.df_out[self.split_colname] == "train"][self.target_name],
                )
            except Exception:
                try:
                    self.df_train_resampled = self.resampler.fit_transform(
                        self.df_out[self.df_out[self.split_colname] == "train"],
                        self.df_out[self.df_out[self.split_colname] == "train"][self.target_name],
                    )
                except Exception:
                    raise ValueError("Could not fit the resampler.")

            return self

    def train(self, **fit_params):
        if hasattr(self, "df_train_resampled") and isinstance(self.df_train_resampled, pd.DataFrame):
            X_train = self.df_train_resampled.drop(self.target_name, axis=1)
            y_train = self.df_train_resampled[self.target_name]
        else:
            X_train = self.df_out[self.df_out[self.split_colname] == "train"][self.output_features]
            y_train = self.df_out[self.df_out[self.split_colname] == "train"][self.target_name]

        if hasattr(self.algorithms, "fit"):
            self.logger.info(f"Running {type(self.algorithms).__name__}...")
            self.algorithms.fit(X_train, y_train, **fit_params)
        else:
            assert isinstance(self.algorithms, (tuple, list))
            for algo in self.algorithms:
                self.logger.info(f"Running{type(algo).__name__}...")
                algo.fit(X_train, y_train, **fit_params)

        self.logger.info("Model training done!")
        return self

    def predict(self):
        self.logger.info("Predicting models...")

        if not isinstance(self.algorithms, (tuple, list)) and hasattr(self.algorithms, "predict_proba"):
            self.df_out[type(self.algorithms).__name__ + "_pred"] = self.algorithms.predict_proba(
                self.df_out[self.output_features]
            )[:, 1]

        elif not isinstance(self.algorithms, (tuple, list)) and hasattr(self.algorithms, "decision_function"):
            self.df_out[type(self.algorithms).__name__ + "_pred"] = algo.decision_function(self.df_out[self.output_features])[
                :, 1
            ]

        else:
            assert isinstance(self.algorithms, (tuple, list))

            for algo in self.algorithms:
                if hasattr(algo, "predict_proba"):
                    self.logger.info("Running predict_proba as predict method.")
                    self.df_out[type(algo).__name__ + "_pred"] = algo.predict_proba(self.df_out[self.output_features])[:, 1]
                elif hasattr(algo, "decision_function"):
                    self.logger.info("Running decision_function as predict method.")
                    self.df_out[type(algo).__name__ + "_pred"] = algo.decision_function(self.df_out[self.output_features])[:, 1]
                elif hasattr(algo, "predict"):
                    self.logger.info("Running predict as predict method.")
                    self.df_out[type(algo).__name__ + "_pred"] = algo.predict(self.df_out[self.output_features])
                else:
                    raise ValueError("Could not run prediction on dataset.")

        assert (
            len([i for i in self.df_out.columns if i.endswith("_pred")]) > 0
        ), "Could not set prediction colname on self.df_out"

        return self

    def assign_threshold_opt_rows(self, pct_train_for_opt=0.25, pct_val_for_opt=1, threshold_opt_name="use_for_threshold_opt"):
        self.threshold_opt_name = threshold_opt_name

        self.df_out.loc[:, threshold_opt_name] = False

        ### assign the train rows to be used for threshold optimization ###

        # pct_train_of_val ex) val set has 100 rows, then train_set_for_opt should be 10 rows, but this is not
        # how it currently works. Based on experience, models have performed much worse.
        # n_train_values = int(self.df_out[self.df_out[self.split_colname] == 'val'].shape[0] * pct_train_for_opt)

        n_train_values = int(self.df_out[self.df_out[self.split_colname] == "train"].shape[0] * pct_train_for_opt)
        # tail might not be a good way to choose the rows, but in case the data is
        # sorted it makes sense as a default
        opt_train_indices = self.df_out[self.df_out[self.split_colname] == "train"].tail(n_train_values).index
        self.df_out.loc[opt_train_indices, threshold_opt_name] = True

        ### assign the val rows to be used for threshold optimization ###

        n_val_values = int(self.df_out[self.df_out[self.split_colname] == "val"].shape[0] * pct_val_for_opt)

        # tail might not be a good way to choose the rows, but in case the data is
        # sorted it makes sense as a default
        opt_val_indices = self.df_out[self.df_out[self.split_colname] == "val"].tail(n_val_values).index

        self.df_out.loc[opt_val_indices, threshold_opt_name] = True

        return self

    def optimize(
        self,
        minimize_or_maximize,
        splits_to_assess=("train", "val", "test"),
        is_classification=True,
        **assign_threshold_opt_rows_params,
    ):

        self.assign_threshold_opt_rows(**assign_threshold_opt_rows_params)

        fits = [i for i in self.df_out.columns if i.endswith("_pred")]

        splits_to_assess = [splits_to_assess] if isinstance(splits_to_assess, str) else splits_to_assess
        splits_to_assess = list(splits_to_assess) if isinstance(splits_to_assess, tuple) else splits_to_assess

        self.optimizer.run_optimization(
            fits=fits,
            minimize_or_maximize=minimize_or_maximize,
            df=self.df_out[(self.df_out[self.threshold_opt_name]) & (self.df_out[self.split_colname].isin(splits_to_assess))],
            target_name=self.target_name,
        )

        ### assign the positive class based on the optimal threshold ###

        if is_classification:
            for fit in self.optimizer.best_thresholds.keys():
                thres_opt = ThresholdOptimizer(df=self.df_out, pred_column=fit, pred_class_col=fit + "_class", make_copy=False)

                if self.optimizer.best_thresholds[fit].index.name != "threshold":
                    self.optimizer.best_thresholds[fit] = self.optimizer.best_thresholds[fit].set_index("threshold")

                thres_opt.assign_predicted_class(self.optimizer.best_thresholds[fit])

        return self

    def evaluate(self, splits_to_evaluate=("train", "val", "test"), **kwargs):
        evaluator_params = {}
        for param in [i for i in inspect.getfullargspec(self.evaluator.__init__).args if i != "self"]:
            evaluator_params[param] = getattr(self.evaluator, param)
        for k in kwargs.keys():
            evaluator_params[k] = kwargs[k]

        splits_to_evaluate = [splits_to_evaluate] if isinstance(splits_to_evaluate, str) else splits_to_evaluate
        splits_to_evaluate = list(splits_to_evaluate) if isinstance(splits_to_evaluate, tuple) else splits_to_evaluate
        assert isinstance(splits_to_evaluate, list), "could not convert splits_to_evaluate to a list"

        self.evaluator.df = self.df_out[self.df_out[self.split_colname].isin(splits_to_evaluate)].copy()
        self.evaluator.target_name = self.target_name
        # self.evaluator.fits = [i for i in self.df_out.columns if i.endswith('_pred_class')]

        self.evaluator.evaluate(**evaluator_params)
        return self

    def get_feature_importances(self):
        self.feature_importances = {}
        for algo in self.algorithms:
            self.feature_importances[type(algo).__name__] = FeatureImportance(
                model=algo, df=self.df_out, input_features=self.output_features
            ).get_feature_importance()
        return self

    def run_workflow(
        self,
        split_params=None,
        create_features_params=None,
        transform_features_params=None,
        resampler_params=None,
        train_model_params=None,
        predict_model_params=None,
        optimize_models_params=None,
        evaluate_model_params=None,
    ):

        ### split data ###

        if self.splitter is not None:
            self.split() if split_params is None else self.split(**split_params)

        ### create features ###

        if self.feature_creator is not None:
            (self.create_features() if create_features_params is None else self.create_features(**create_features_params))

        ### transform features ###

        if self.feature_transformer is not None:
            (
                self.transform_features()
                if transform_features_params is None
                else self.transform_features(**transform_features_params)
            )

        ### resample data ###

        if self.resampler is not None:
            (self.resample() if resampler_params is None else self.resample(**resampler_params))

        ### train models ###

        self.train() if train_model_params is None else self.train(**train_model_params)

        ### sanity check that all algorithms are fitted ###

        fitted_algorithms = []
        for a in self.algorithms:
            try:
                check_is_fitted(a)
                fitted_algorithms.append(a)
            except NotFittedError:
                pass

        ### predict models ###

        if len(fitted_algorithms) == len(self.algorithms):
            (self.predict() if predict_model_params is None else self.predict(**predict_model_params))

            ### optimize models ###

            if self.optimizer is not None:
                (self.optimize() if optimize_models_params is None else self.optimize(**optimize_models_params))

            ### evaluate models ###

            if self.evaluator is not None:
                (self.evaluate() if evaluate_model_params is None else self.evaluate(**evaluate_model_params))

            ### get feature importances ###

            self.get_feature_importances()

        else:
            raise NotFittedError("Not all algorithms have been fit!")

        return self


def get_column_names_from_column_transformer(column_transformer, clean_column_names=True, verbose=False):
    """
    Reference: Kyle Gilde
    https://github.com/kylegilde/Kaggle-Notebooks/blob/master/Extracolumn_transformering-and-Plotting-Scikit-Feature-Names-and-Importances/feature_importance.py

    Description: Get the column names from the a ColumnTransformer containing transformers & pipelines

    Parameters
    ----------
    verbose: Bool indicating whether to self.logger.info summaries. Default set to True.
    Returns
    -------
    a list of the correcolumn_transformer feature names
    Note:
    If the ColumnTransformer contains Pipelines and if one of the transformers in the Pipeline is adding completely new
    columns, it must come last in the pipeline. For example, OneHotEncoder, MissingIndicator &
    SimpleImputer(add_indicator=True) add columns to the dataset that didn't exist before, so there should come last in
    the Pipeline. Inspiration: https://github.com/scikit-learn/scikit-learn/issues/12525
    """

    assert isinstance(column_transformer, ColumnTransformer), "Input isn't a ColumnTransformer"

    check_is_fitted(column_transformer)

    try:
        new_column_names = column_transformer.get_feature_names_out()

    except Exception:

        new_column_names, transformer_list = [], []

        for i, transformer_item in enumerate(column_transformer.transformers_):

            transformer_name, transformer, orig_feature_names = transformer_item
            orig_feature_names = list(orig_feature_names)

            if len(orig_feature_names) == 0:
                continue

            if verbose:
                self.logger.info(f"{i}.Transformer/Pipeline: {transformer_name} {transformer.__class__.__name__}")
                self.logger.info(f"{i}.Transformer/Pipeline: {transformer_name} {transformer.__class__.__name__}")
                self.logger.info(f"n_orig_feature_names:{len(orig_feature_names)}")

            if transformer == "drop" or transformer == "passthrough":
                continue

            try:
                names = transformer.get_feature_names_out()

            except Exception:

                try:
                    names = transformer[:-1].get_feature_names_out()

                except Exception:

                    if isinstance(transformer, Pipeline):

                        # if pipeline, get the last transformer in the Pipeline
                        names = []
                        for t in transformer:
                            try:
                                transformer_feature_names = t.get_feature_names_out()
                            except Exception:
                                try:
                                    transformer_feature_names = t.get_feature_names_out(orig_feature_names)
                                except Exception:
                                    try:
                                        transformer_feature_names = t[:-1].get_feature_names_out()
                                    except Exception:
                                        transformer = transformer.steps[-1][1]
                                        try:
                                            transformer_feature_names = transformer.cols
                                        except Exception:
                                            raise ValueError(f"Could not get column names for transformer {t}")

                            [names.append(i) for i in transformer_feature_names if i not in names]

                    if hasattr(transformer, "get_feature_names_out"):
                        if "input_features" in transformer.get_feature_names_out.__code__.co_varnames:
                            names = list(transformer.get_feature_names_out(input_features=orig_feature_names))

                        else:
                            names = list(transformer.get_feature_names_out())

                    elif hasattr(transformer, "get_feature_names"):
                        if "input_features" in transformer.get_feature_names.__code__.co_varnames:
                            names = list(transformer.get_feature_names(orig_feature_names))
                        else:
                            names = list(transformer.get_feature_names())

                    elif hasattr(transformer, "indicator_") and transformer.add_indicator:
                        # is this transformer one of the imputers & did it call the
                        # MissingIndicator?
                        missing_indicator_indices = transformer.indicator_.features_
                        missing_indicators = [orig_feature_names[idx] + "_missing_flag" for idx in missing_indicator_indices]
                        names = orig_feature_names + missing_indicators

                    elif hasattr(transformer, "features_"):
                        # is this a MissingIndicator class?
                        missing_indicator_indices = transformer.features_
                        missing_indicators = [orig_feature_names[idx] + "_missing_flag" for idx in missing_indicator_indices]

                    else:
                        names = orig_feature_names

                    if verbose:
                        self.logger.info(f"n_new_features:{len(names)}")
                        self.logger.info(f"new_features: {names}")

            new_column_names.extend(names)
            transformer_list.extend([transformer_name] * len(names))

        if column_transformer.remainder == "passthrough":
            passthrough_cols = column_transformer.feature_names_in_[column_transformer.transformers_[-1][-1]]
            new_column_names = list(new_column_names) + [i for i in passthrough_cols if i not in new_column_names]

    if clean_column_names:
        new_column_names = [i.replace("remainder__", "") for i in new_column_names]
        new_column_names = list(clean_columns(pd.DataFrame(columns=new_column_names)).columns)

    return new_column_names


class FeatureTransformer(TransformerMixin, MLFlowLogger):
    """
    Parameters
    ----------
    preserve_vars : A list of variables that won't be fitted or transformed by any sort of feature engineering
    target_name : A string - the name of the target variable.
    remainder : A string that gets passed to the column transformer whether to
                drop preserve_vars or keep them in the final dataset
                options are 'drop' or 'passthrough'
    max_lc_cardinality : A natural number - one-hot encode all features with unique categories <= to this value
    fe_pipeline_dict : Set to None to use "standard" feature engineering pipeline.
        Otherwise, supply a dictionary of pipelines to hc_pipe, lc_pipe, numeric_pipe, and custom_pipe
            numeric_pipe: numeric pipeline
            hc_pipe: high-cardinal pipeline
            lc_pipe: low-cardinal pipe
    n_jobs : An int - the number of threads to use
    make_copy : boolean to make_copy X_train and X_test while preprocessing

    Attributes
    ----------
    detect_feature_groups attributes are dictionary attributes
    fit attributes are sklearn ColumnTransformer attributes

    Returns
    -------
    Method detect_features returns a dictionary
    Method fit returns a ColumnTransformer object
    We can call fit_transform because we inherited the sklearn base TransformerMixin class

    """

    def __init__(
        self,
        target_name=None,
        preserve_vars=None,
        fe_pipeline_dict=None,
        remainder="passthrough",
        max_lc_cardinality=11,
        run_detect_feature_groups=True,
        numeric_features=None,
        lc_features=None,
        hc_features=None,
        overwrite_detection=True,
        n_jobs=-1,
        clean_column_names=True,
        make_copy=True,
        verbose=True,
    ):
        super().__init__()
        self.preserve_vars = preserve_vars
        self.target_name = target_name
        self.FE_pipeline_dict = fe_pipeline_dict
        self.remainder = remainder
        self.max_lc_cardinality = max_lc_cardinality
        self.run_detect_feature_groups = run_detect_feature_groups
        self.numeric_features = [] if numeric_features is None else numeric_features
        self.lc_features = [] if lc_features is None else lc_features
        self.hc_features = [] if hc_features is None else hc_features
        self.overwrite_detection = overwrite_detection
        self.n_jobs = n_jobs
        self.clean_column_names = clean_column_names
        self.verbose = verbose
        self.make_copy = make_copy
        self.preserve_vars = [] if self.preserve_vars is None else self.preserve_vars

        self.column_transformer = ColumnTransformer(transformers=[], remainder=self.remainder, n_jobs=self.n_jobs)

        self.input_features = None
        self.feature_groups = None
        self.output_cols = None
        self.preserve_vars_orig = None

    def detect_feature_groups(self, X):

        if self.make_copy:
            X = X.copy()

        if not self.run_detect_feature_groups:
            if self.verbose:
                self.logger.info("Not detecting dtypes.")

            feature_dict = {
                "numeric_features": self.numeric_features,
                "lc_features": self.lc_features,
                "hc_features": self.hc_features,
            }
            if self.FE_pipeline_dict is not None and "custom_pipe" in self.FE_pipeline_dict.keys():
                feature_dict["custom_features"] = list(self.FE_pipeline_dict["custom_pipe"].values())[0]
            return feature_dict

        if self.FE_pipeline_dict is not None and "custom_pipe" in self.FE_pipeline_dict.keys():
            custom_features = list(itertools.chain(*self.FE_pipeline_dict["custom_pipe"].values()))
        else:
            custom_features = []

        assert (
            len(
                np.intersect1d(
                    list(set(self.numeric_features + self.lc_features + self.hc_features + custom_features)), self.preserve_vars
                )
            )
            == 0
        ), "There are duplicate features in preserve_vars either the input\
             numeric_features, lc_features, or hc_features"

        detected_numeric_vars = make_column_selector(dtype_include=np.number)(
            X[[i for i in X.columns if i not in self.preserve_vars + [self.target_name] + custom_features]]
        )

        detected_lc_vars = [
            i
            for i in X.loc[:, (X.nunique(dropna=False) <= self.max_lc_cardinality) & (X.nunique(dropna=False) > 1)].columns
            if i not in self.preserve_vars + [self.target_name] + custom_features
        ]

        detected_hc_vars = (
            X[[i for i in X.columns if i not in self.preserve_vars + custom_features]]
            .select_dtypes(["object", "category"])
            .apply(lambda col: col.nunique(dropna=False))
            .loc[lambda x: x > self.max_lc_cardinality]
            .index.tolist()
        )

        discarded_features = [i for i in X.isnull().sum()[X.isnull().sum() == X.shape[0]].index if i not in self.preserve_vars]

        numeric_features = list(
            set(
                [
                    i
                    for i in self.numeric_features
                    + [
                        i
                        for i in detected_numeric_vars
                        if i not in list(self.lc_features) + list(self.hc_features) + list(discarded_features) + custom_features
                    ]
                ]
            )
        )

        lc_features = list(
            set(
                [
                    i
                    for i in self.lc_features
                    + [
                        i
                        for i in detected_lc_vars
                        if i
                        not in list(self.numeric_features) + list(self.hc_features) + list(discarded_features) + custom_features
                    ]
                ]
            )
        )

        hc_features = list(
            set(
                [
                    i
                    for i in self.hc_features
                    + [
                        i
                        for i in detected_hc_vars
                        if i
                        not in list(self.numeric_features) + list(self.lc_features) + list(discarded_features) + custom_features
                    ]
                ]
            )
        )

        if self.verbose:
            self.logger.info(
                "Overlap between numeric and lc_features: " + str(list(set(np.intersect1d(numeric_features, lc_features))))
            )
            self.logger.info(
                "Overlap between numeric and hc_features: " + str(list(set(np.intersect1d(numeric_features, hc_features))))
            )
            self.logger.info(
                "Overlap between numeric lc_features and hc_features: "
                + str(list(set(np.intersect1d(lc_features, hc_features))))
            )
            self.logger.info("Overlap between lc_features and hc_features will be moved to lc_features")

        if self.overwrite_detection:
            numeric_features = [
                i for i in numeric_features if i not in lc_features + hc_features + discarded_features + custom_features
            ]

            lc_features = [
                i for i in lc_features if i not in hc_features + numeric_features + discarded_features + custom_features
            ]

            hc_features = [
                i for i in hc_features if i not in lc_features + numeric_features + discarded_features + custom_features
            ]

        else:
            numeric_overlap = [
                i
                for i in numeric_features
                if i in lc_features or i in hc_features and i not in discarded_features + custom_features
            ]

            lc_overlap = [
                i
                for i in lc_features
                if i in hc_features or i in numeric_features and i not in discarded_features + custom_features
            ]

            hc_overlap = [
                i
                for i in hc_features
                if i in lc_features or i in numeric_features and i not in discarded_features + custom_features
            ]

            if numeric_overlap or lc_overlap or hc_overlap:
                raise AssertionError(
                    "There is an overlap between numeric, \
                                      lc, and hc features! \
                                      To ignore this set overwrite_detection to True."
                )

        all_features = list(set(numeric_features + lc_features + hc_features + discarded_features + custom_features))

        all_features_debug = set(all_features) - set([i for i in X.columns if i not in self.preserve_vars + [self.target_name]])

        if len(all_features_debug) > 0:
            self.logger.info("{}".format(all_features_debug))
            raise AssertionError(
                "There was a problem detecting all features!! \
                Check if there is an overlap between preserve_vars and other custom input features"
            )

        if self.verbose:
            self.logger.info(f"numeric_features: {numeric_features}")
            self.logger.info(f"lc_features: {lc_features}")
            self.logger.info(f"hc_features: {hc_features}")
            self.logger.info(f"discarded_features: {discarded_features}")
            self.logger.info(f"custom_features: {custom_features}")

        feature_dict = {
            "numeric_features": numeric_features,
            "lc_features": lc_features,
            "hc_features": hc_features,
            "custom_features": custom_features,
            "discarded_features": discarded_features,
        }

        return feature_dict

    def instantiate_column_transformer(self, X, y=None):
        if self.target_name is None and y is not None:
            self.target_name = y.name

        assert y is not None or self.target_name is not None, "Both self.target_name and y cannot be None!"

        self.feature_groups = self.detect_feature_groups(X)

        # set a default transformation pipeline if FE_pipeline_dict is not specified
        if self.FE_pipeline_dict is None:

            ### Default pipelines ###

            na_replacer = FunctionTransformer(
                lambda x: x.replace([-np.inf, np.inf, None, "None", "", " ", "nan", "Nan"], np.nan),
                feature_names_out="one-to-one",
            )

            numeric_pipe = make_pipeline(
                na_replacer,
                # Winsorizer(distribution='gaussian', tail='both', fold=3, missing_values = 'ignore'),
                MinMaxScaler(feature_range=(0, 1)),
                SimpleImputer(strategy="median", add_indicator=True),
            )

            hc_pipe = make_pipeline(
                na_replacer,
                FunctionTransformer(lambda x: x.astype(str), feature_names_out="one-to-one"),
                # MeanEncoder()
                TargetEncoder(
                    cols=self.feature_groups["hc_features"],
                    drop_invariant=False,
                    return_df=True,
                    handle_missing="value",
                    handle_unknown="value",
                    min_samples_leaf=12,
                    smoothing=8,
                ),
            )

            lc_pipe = make_pipeline(na_replacer, OneHotEncoder(handle_unknown="ignore", sparse_output=False))

            custom_pipe = None

        else:
            hc_pipe = self.FE_pipeline_dict["hc_pipe"]
            numeric_pipe = self.FE_pipeline_dict["numeric_pipe"]
            lc_pipe = self.FE_pipeline_dict["lc_pipe"]

            custom_pipe = self.FE_pipeline_dict["custom_pipe"] if "custom_pipe" in self.FE_pipeline_dict.keys() else {}

        transformers = [
            ("hc_pipe", hc_pipe, self.feature_groups["hc_features"]),
            ("numeric_pipe", numeric_pipe, self.feature_groups["numeric_features"]),
            ("lc_pipe", lc_pipe, self.feature_groups["lc_features"]),
        ]

        for alias, transformer, cols in transformers:
            if isinstance(transformer, Pipeline):
                for t in transformer:
                    if "cols" in list(inspect.signature(t.__class__).parameters.keys()):
                        t.cols = cols
            else:
                if "cols" in list(inspect.signature(transformer.__class__).parameters.keys()):
                    transformer.cols = transformer[-1]

        if custom_pipe:
            setattr(self, "custom_features", list(set(np.concatenate(list(custom_pipe.values())))))
            i = 0
            for cp in custom_pipe.keys():
                transformers.append(("custom_pipe{}".format(str(i)), cp, custom_pipe[cp]))
                i += 1

        self.column_transformer.transformers = transformers

    def fit(self, X, y=None):

        self.instantiate_column_transformer(X, y)

        if y is None:
            self.column_transformer.fit(X)
        else:
            self.column_transformer.fit(X, y)

        self.output_cols = get_column_names_from_column_transformer(
            self.column_transformer, clean_column_names=self.clean_column_names, verbose=self.verbose
        )

        self.input_features = (
            self.feature_groups["numeric_features"]
            + self.feature_groups["lc_features"]
            + self.feature_groups["hc_features"]
            + self.feature_groups["custom_features"]
        )

        if len(self.preserve_vars):
            self.preserve_vars_orig = self.preserve_vars.copy()
            self.preserve_vars = self.preserve_vars_orig + self.feature_groups["discarded_features"]

        setattr(self, "output_features", [i for i in self.output_cols if i not in self.preserve_vars + [self.target_name]])

        assert len(list(set(self.output_features + self.preserve_vars + [self.target_name]))) == len(self.output_cols)
        assert len(set(self.output_cols)) == len(self.output_cols)

        return self

    def transform(self, X, return_df=True):

        X_out = self.column_transformer.transform(X)

        if return_df:
            return pd.DataFrame(list(X_out), columns=self.output_cols)
        else:
            return X_out


class FeatureImportance(MLFlowLogger):
    """
    Description
    -----------
    Calculate feature importance for a given model. The results can either return a table or a plot.

    Parameters
    ----------
    model: sklearn fitted model object (e.g. xgboost model after training)
    df: pandas df that contains the input features to the model
    input_features: list of input features that went into the model training
    round_decimals: int of decimals to round the output feature importances to

    verbose: Bool to self.logger.info statements during calculation

    """

    def __init__(self, model=None, df=None, input_features=None, round_decimals=3, verbose=False):
        self.model = model
        self.df = df
        self.input_features = input_features
        self.round_decimals = round_decimals
        self.verbose = verbose

        if self.df is not None:
            self.input_features = self.df.columns if self.input_features is None else self.input_features

        super().__init__()

    def get_feature_importance(self):

        assert (
            self.df is not None or self.input_features is not None
        ) and self.model is not None, """
               Both the input self.df and self.input_features \
               cannot be None when calculating feature importance!
             """

        # sklearn tree models usually have the feature_importance_ attribute
        try:
            importances = self.model.feature_importances_
            feature_importances = pd.Series(importances, index=self.input_features).sort_values(ascending=False).reset_index()

        # sklearn linear models usually have the .coef_ attribute
        except AttributeError:
            try:
                feature_importances = pd.DataFrame(self.model.coef_, index=self.input_features, columns=["coefs"])
                feature_importances["importance"] = feature_importances["coefs"].abs()
                feature_importances = (
                    feature_importances.sort_values(by="importance", ascending=False).reset_index().drop("coefs", axis=1)
                )
            except Exception:
                raise ValueError("Cannot get feature importance for this model")

        assert len(self.input_features) == len(
            feature_importances
        ), f"The number of feature names {len(self.input_features)}\
              and importance values {len(feature_importances)} don't match"

        feature_importances.columns = ["feature", "importance"]

        if self.round_decimals:
            feature_importances["importance"] = round(feature_importances["importance"], self.round_decimals)

        return feature_importances

    def plot_importance(
        self,
        feature_importances=None,
        top_n_features=100,
        orientation="h",
        height=None,
        width=200,
        height_per_feature=10,
        yaxes_tickfont_family="Courier New",
        yaxes_tickfont_size=15,
        **px_bar_params,
    ):
        """
        Description
        -----------
        Plot the Feature Names & Importances

        Parameters
        ----------

        feature_importances: output of the get_feature_importance method
        top_n_features: the number of features to plot. Set to none to return all feature importances.
        orientation: the plot orientation, 'h' (default) or 'v'
        height: the height of the plot, the default is top_n_features * height_per_feature
        width:  the width of the plot, default is 500
        height_per_feature: if height is None, the plot height is calculated by top_n_features * height_per_feature.
            This allows all the features enough space to be displayed
        yaxes_tickfont_family: the font for the feature names. Default is Courier New.
        yaxes_tickfont_size: the font size for the feature names. Default is 15.
        **px_bar_params: additional parameters that go into plotly.express parameters

        Returns: plot object
        """

        height = top_n_features * height_per_feature if height is None else height
        feature_importances = self.get_feature_importance() if feature_importances is None else feature_importances

        if feature_importances.shape[0] >= 1:

            feature_importances = feature_importances.nlargest(top_n_features, "importance").sort_values(
                by="importance", ascending=False
            )

            n_all_importances = feature_importances.shape[0]

            title_text = (
                "All Feature Importances"
                if top_n_features > n_all_importances or top_n_features is None
                else f"Top {top_n_features} (of {n_all_importances}) Feature Importances"
            )

            feature_importances = feature_importances.sort_values(by="importance")
            if self.round_decimals:
                feature_importances["text"] = feature_importances["importance"].round(self.round_decimals).astype(str)
            else:
                feature_importances["text"] = feature_importances["importance"].astype(str)

            # create the plot
            fig = px.bar(
                feature_importances,
                x="importance",
                y="feature",
                orientation=orientation,
                width=width,
                height=height,
                text="text",
                **px_bar_params,
            )

            fig.update_layout(title_text=title_text, title_x=0.5)
            fig.update(layout_showlegend=False)
            fig.update_yaxes(tickfont=dict(family=yaxes_tickfont_family, size=yaxes_tickfont_size))
            return fig
        else:
            return


class ImbResampler(MLFlowLogger):
    """
    Parameters
    ----------
    algorithm: The resampling algorithm to use
    In fit params:
    X: The training dataframe that does not include the target variable
    y: A pandas Series of the target variable
    -------
    Attributes
    See imblearn documentation for the attributes of whichever resampling algorithm you choose
    -------
    Returns a resampled pandas dataframe
    -------
    """

    def __init__(self, algorithm):
        super().__init__()
        self.algorithm = algorithm

    def fit_transform(self, X, y):
        ds_self.logger.info("Running {}".format(type(self.algorithm).__name__))
        df_resampled = self.algorithm.fit_resample(X, y)
        return df_resampled


def r_squared(y_true, y_pred):
    return np.corrcoef(y_true, y_pred)[0, 1] ** 2


class CalcMLMetrics(MLFlowLogger):
    """
    Description
    -----------

    Calculate standard ML metrics for regression or classification.
    For regression: RMSE, MAE, and R2
    For classification: accuracy, precision, recall, F1

    Parameters
    ----------
    round_decimals: int to round the output feature importance text for easier reading
    """

    def __init__(self, round_decimals=3):
        self.round_decimals = round_decimals
        self.target_name = None
        self.df = None
        self.fits = None
        self.metric_fn = None

        super().__init__()

    @staticmethod
    def calc_regression_metrics(y_true=None, y_pred=None):
        """
        Parameters
        ----------
        y_true: actual y value
        y_pred: predicted y_value

        Returns
        -------
        dictionary of regression scores
        """

        output_dict = dict(
            r2=r_squared(y_true, y_pred),
            sklearn_r2=r2_score(y_true, y_pred),
            rmse=np.sqrt(mean_squared_error(y_true, y_pred)),
            mae=mean_absolute_error(y_true, y_pred),
        )
        return output_dict

    @staticmethod
    def calc_classification_metrics(y_true, y_pred, threshold=0.5):
        """
        Parameters
        ----------
        y_true: pd.Series of actual y value
        y_pred: pd.Series of predicted y_value score (not the predicted class)
        threshold: float value - if pred score is > than the threshold, flag prediction to be 1
        ----------

        Returns
        -------
        dictionary of classification scores

        """

        assert not y_true.isnull().any(), "y_true cannot have NAs!"
        assert not y_pred.isnull().any(), "y_pred cannot have NAs!"

        output_dict = dict(
            accuracy=accuracy_score(y_true, y_pred > threshold),
            precision=precision_score(y_true, y_pred > threshold),
            recall=recall_score(y_true, y_pred > threshold),
            f1=f1_score(y_true, y_pred > threshold),
            roc_auc=roc_auc_score(y_true, y_pred),
            pr_auc=average_precision_score(y_true, y_pred, average="weighted"),
            logloss=log_loss(y_true, y_pred),
        )
        return output_dict

    def calc_ml_metrics(
        self,
        df,
        target_name,
        fits,
        classification_or_regression=None,
        groupby_cols=None,
        clf_threshold=0.5,
        drop_nas=True,
        groupby_col_order=dict(dataset_split=("train", "val", "test")),
        num_threads=1,
    ):
        """

        Description
        -----------
        Select the datasets and needed columns
        melt the multiple fit columns into long df w/ fit & fit_values columns
        drop NAs if specified
        groupby_cols if specified
        apply the metric fn to each group
        cast the results to a dict and then df
        sort the dataset values if specified
        create a rounded column of the metric values for the plot's text

        Parameters
        ----------
        :param df: pandas df witht he target_name and fits
        :param target_name: str of the target name
        :param fits: list or tuple of fits in the dataframe to evaluate
        :param classification_or_regression: str if the model is a classification or regression model
        :param clf_threshold: float threshold for classification models to predict a positive class
        :param groupby_cols: list or tuple of cols to groupby when calculating the ML metrics
        :param drop_nas: Bool to drop NA values before evaluation
        :param groupby_col_order: dict of the order to sort the groupby_cols
        :param num_threads: int of num_threads to use. -1 means use all threads

        Returns: pandas dataframe with calculated ML metrics

        """

        # TODO: Make groupby_col_order immutable

        assert df[target_name].isnull().sum() == 0, "There cannot be NAs in the target variable"
        num_threads = mp.cpu_count() if num_threads < 0 else num_threads

        self.df = (
            df[list(set(fits + [target_name]))]
            if groupby_cols is None
            else df[list(set(fits + [target_name] + [groupby_cols]))]
        )

        self.target_name = target_name
        self.fits = fits
        self.groupby_cols = groupby_cols

        if classification_or_regression.lower().startswith("reg"):
            self.metric_fn = self.calc_regression_metrics
        else:
            self.metric_fn = self.calc_classification_metrics
            self.metric_fn.threshold = clf_threshold

        if isinstance(self.fits, tuple):
            self.fits = list(self.fits)
        elif isinstance(self.fits, str):
            self.fits = [self.fits]

        assert len(self.fits) > 0, "fits cannot empty."

        if num_threads > 1:
            raise NotImplementedError("Multi-threading not implemented yet!")

        if self.groupby_cols is None:
            self.metrics_df = pd.DataFrame()
            for fit in self.fits:
                _metrics_df = pd.DataFrame.from_dict(
                    self.metric_fn(df[self.target_name], df[fit]), orient="index", columns=[fit]
                )

                self.metrics_df = pd.concat([self.metrics_df, _metrics_df], axis=1)
        else:
            if isinstance(self.groupby_cols, str):
                self.groupby_cols = [self.groupby_cols]
            elif isinstance(self.groupby_cols, tuple):
                self.groupby_cols = list(self.groupby_cols)

            if len(self.fits) == 1:
                self.metrics_df = (
                    df.groupby(self.groupby_cols, dropna=drop_nas)
                    .apply(lambda x: self.metric_fn(x[self.target_name], x[self.fits[0]]))
                    .reset_index()
                    .pipe(lambda x: x[self.groupby_cols].join(json_normalize(x[0])))
                )

            else:
                self.metrics_df = pd.DataFrame()
                for fit in self.fits:
                    _metrics_df = (
                        df.groupby(self.groupby_cols, dropna=drop_nas)
                        .apply(lambda x: self.metric_fn(x[self.target_name], x[fit]))
                        .reset_index()
                        .pipe(lambda x: x[self.groupby_cols].join(json_normalize(x[0])))
                        .assign(fit=fit)
                    )

                    self.metrics_df = pd.concat([self.metrics_df, _metrics_df])

            if groupby_col_order is not None:
                for col in groupby_col_order.keys():
                    col_order = pd.CategoricalDtype(groupby_col_order[col], ordered=True)
                    self.metrics_df[col] = self.metrics_df[col].astype(col_order)

                self.metrics_df = self.metrics_df.sort_values(by=list(groupby_col_order.keys()))

        return self.metrics_df

    def plot_metrics(
        self,
        metrics_output=None,
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
        **calc_ml_metrics_params,
    ):

        metrics_output = self.calc_ml_metrics(**calc_ml_metrics_params) if metrics_output is None else metrics_output

        if self.groupby_cols is None:
            metrics_df = metrics_output.reset_index().melt(id_vars="index")
            metrics_df = metrics_df.rename(columns={"index": "metric", "variable": "fit"})
        else:
            metrics_df = metrics_output.melt(id_vars=list(set(self.groupby_cols + ["fit"])))
            metrics_df = metrics_df.rename(columns={"variable": "metric"})

        metrics_df["text"] = (metrics_df["value"] * 100).round(2).astype(str).pipe(lambda x: x + "%")

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
        fig.update_yaxes(range=[metrics_df[y].min() - 0.05, metrics_df[y].max() + 0.1], matches=y_matches)

        if width is None:
            for data in fig.data:
                data["width"] = 1

        if save_plot_loc:
            plotly.offline.plot(fig, filename=save_plot_loc)
        return fig


class ThresholdOptimizer(MLFlowLogger):
    """
    Description
    -----------
    Parent class that gets inherited by other optimizers.

    Parameters
    ----------
    df: pandas df that contains the prediction column
    pred_column: str of the fit column that needs to be a continuous score (not the class prediction)
    pred_class_col: str of the new column name to create if the pred_column is >= the optimal threshold
    make_copy: bool whether to create a copy of the dataframe prior to computation
    """

    def __init__(self, df=None, pred_column=None, pred_class_col="pred_class", make_copy=True):

        self.df = df
        self.pred_column = pred_column
        self.pred_class_col = pred_class_col
        self.make_copy = make_copy

        if self.make_copy and isinstance(df, pd.DataFrame):
            self.df = self.df.copy()

    def optimize(self, thresholds=None):
        """
        Description
        -----------
        Calculate the optimization function for each threshold in thresholds.

        Parameters
        ----------
        thresholds: array of thresholds to test (if None use default value in overridden method)

        Returns
        -------
        pandas dataframe of the threshold_df containing each threshold that is tested as an index and metrics as columns

        """

        raise ValueError("The optimize method must be overridden.")

    @staticmethod
    def get_best_thresholds(threshold_df):
        """
        Description
        -----------
        Get the optimal thresholds from the output after running self.optimize()

        Parameters
        ----------
        threshold_df: pandas dataframe that is output of self.optimize()

        Returns
        -------
        pandas dataframe of the optimal thresholds and metrics
        """
        return

    def assign_predicted_class(self, best_threshold_df):
        """
        Description: Assign a positive predicted class based on the output of get_best_thresholds

        Parameters
        ----------
        best_threshold_df: pandas dataframe output from self.get_best_thresholds

        Returns
        -------
        pandas df with the new pred_class from the optimal thresholds in best_threshold_df
        """

        assert (self.df is not None) and (
            self.pred_column is not None
        ), "Both self.df and self.pred_column must be provided when calling self.assign_predicted_class."

        if self.pred_class_col in self.df.columns:
            warnings.warn(
                f"pred_class_col {self.pred_class_col} is in self.df. \
                The column {self.pred_class_col} will be overwritten."
            )

        if len(best_threshold_df.index.names) == 1:

            assert best_threshold_df.index.name == "threshold"
            groupby_cols = None
        else:
            assert (
                len([i for i in np.intersect1d(best_threshold_df.columns, self.df.columns) if i != "threshold"]) == 0
            ), "column intersection between df.columns and best_threshold_df, excluding threshold, must have length 0."

            groupby_cols = [i for i in list(best_threshold_df.index.names) if i != "threshold"]

        self.df.loc[:, self.pred_class_col] = 0

        if groupby_cols is None:
            threshold_cutoff = best_threshold_df.index.get_level_values("threshold").min()
            self.df.loc[self.df[self.pred_column] >= threshold_cutoff, self.pred_class_col] = 1
            return self.df

        else:
            if "threshold" in self.df.columns:
                self.df = self.df.drop("threshold", axis=1)

            self.df = self.df.merge(best_threshold_df.reset_index()[groupby_cols + ["threshold"]], on=groupby_cols, how="left")
            self.df.loc[self.df[self.pred_column] >= self.df["threshold"], self.pred_class_col] = 1
        return self.df


class ScoreThresholdOptimizer(ThresholdOptimizer):
    """

    Description
    -----------
    Find the optimal thresholds from a continuous predicted score based on the input optimization function.
    Example: If optimizing accuracy, this class would return the thresholds that result in the best accuracy.

    Parameters
    ----------
    optimization_func: function that we want to optimize for all thresholds to test in thresholds
        The optimization_func must take at least two parameters called y_true and y_pred (see sklearn metrics)
    y_pred: pandas Series of prediction
    y_true: pandas Series of the true value
    """

    def __init__(self, optimization_func, y_pred=None, y_true=None):
        super().__init__()

        self.optimization_func = optimization_func
        self.y_pred = y_pred
        self.y_true = y_true

        self.thresholds = None
        self.scores = None
        self.threshold_df = None
        self.best_score = None
        self.best_thresholds = None

    def optimize(self, thresholds=None, num_threads=1):
        """
        Description: Find the optimal thresholds based on the optimization_func passed.

        Parameters
        ----------
        thresholds: array of thresholds that will be passed to the optimization process.
            If None (default), the thresholds are set to be an array between 0 and 1 in increments of 0.01.

        num_threads: int of number of threads to use when running self.optimize()

        Returns
        -------
        pandas df of all thresholds tested and the score calculated in the optimization_fn
        """

        assert type(self.optimization_func).__name__ == "function", "optimization_func must be a function!"

        self.thresholds = np.arange(0, 100) / 100 if thresholds is None else thresholds
        score_df = pd.DataFrame({"y_pred": self.y_pred, "y_true": self.y_true})

        if num_threads == 1:
            self.scores = {}
            for thres in self.thresholds:
                score_df.loc[:, "pred_class"] = 0
                score_df.loc[score_df["y_pred"] >= thres, "pred_class"] = 1

                if (
                    "y_true" in inspect.getfullargspec(self.optimization_func).args
                    and "y_pred" in inspect.getfullargspec(self.optimization_func).args
                ):

                    self.scores[thres] = self.optimization_func(y_true=score_df["y_true"], y_pred=score_df["pred_class"])
                else:
                    self.scores[thres] = self.optimization_func(score_df["y_true"], score_df["pred_class"])

            self.threshold_df = pd.DataFrame.from_dict(self.scores, orient="index").reset_index()
            self.threshold_df.columns = ["threshold", "score"]
        else:
            raise NotImplementedError("Multi-threading not implemented yet.")

        return self

    def get_best_thresholds(self, minimize_or_maximize, threshold_df=None):
        """
        Description: Pull the best scores from the output of self.optimize_score.

        Parameters
        ----------
        minimize_or_maximize: str whether we want to minimize or maximize the output of self.optimize_score.
            This will determine how we decide the best thresholds. Valid values are only 'minimize' or 'maximize'
        threshold_df: output of self.optimize_score
        """

        self.threshold_df = self.threshold_df if threshold_df is None else threshold_df

        if isinstance(self.threshold_df.index, pd.MultiIndex):
            groupby_cols = [i for i in self.threshold_df.index.names if i != "threshold"]
            if minimize_or_maximize.lower() == "maximize":
                best_score = self.threshold_df.groupby(groupby_cols).apply(lambda x: x[x["score"] == x["score"].max()])

            elif minimize_or_maximize.lower() == "maximize":
                best_score = self.threshold_df.groupby(groupby_cols).apply(lambda x: x[x["score"] == x["score"].max()])

            else:
                raise ValueError("minimize_or_maximize must be set to 'maximize' or 'minimize'")

            indices = np.flatnonzero(pd.Index(best_score.index.names).duplicated()).tolist()

            best_score = best_score.reset_index(indices, drop=True)
        else:
            if minimize_or_maximize.lower() == "maximize":
                self.best_score = self.threshold_df[self.threshold_df["score"] == self.threshold_df["score"].max()]
            elif minimize_or_maximize.lower() == "minimize":
                self.best_score = self.threshold_df[self.threshold_df["score"] == self.threshold_df["score"].min()]

        return self

    def run_optimization(self, fits, minimize_or_maximize, df=None, target_name=None, num_threads=1):
        """
        Description
        -----------
        Runs the full optimization pipeline by calling methods in the proper sequence.
        Note that if multiple fits are passed, self.best_score will return the most recent best threshold df
            and self.best_thresholds is a dictionary containing the best thresholds for each fit

        :param fits: list or tuple of fits to optimize
        :param minimize_or_maximize: bool whether to minimize or maximize the optimization function
        :param df: optional pandas df consisting of the fit columns
        :param target_name: str of the target name
        :param num_threads: int of number of threads to use
        :return: self
        """

        fits = [fits] if isinstance(fits, str) else fits
        df = df[fits + [target_name]]

        ### sanity checks ###

        if df is None and len(fits) > 1:
            raise AssertionError("The parameter df must be specified (with fit columns present) len(fits) > 1.")

        if num_threads > 1:
            raise NotImplementedError("Multi-threading not implemented yet!")

        ### run the optimization ###

        if len(fits) == 1:
            self.optimize()
            self.get_best_thresholds()
        else:
            self.y_true = df[target_name].copy() if self.y_true is None else self.y_true

            self.best_thresholds = {}
            for fit in fits:
                self.y_pred = df[fit].copy()
                self.optimize()
                self.get_best_thresholds(minimize_or_maximize)
                self.best_thresholds[fit] = self.best_score

        return self


class AbstractSplitter(MLFlowLogger):
    def __init__(self, split_colname="dataset_split"):
        super().__init__()
        self.split_colname = split_colname

    def split(self):
        raise ValueError("The split method must be overridden.")


class SimpleSplitter(AbstractSplitter):
    def __init__(self, train_pct=0.7, val_pct=0.15):
        super().__init__()

        self.train_pct = train_pct
        self.val_pct = val_pct

    def split(self, df):
        """
        Add a column from self.dataset_split that splits the pandas dataframe (e.g. train, val, test)

        :param df: pandas dataframe
        :returns pandas df
        """
        assert isinstance(df, pd.DataFrame), "Input data must be a pandas df."

        df = df.reset_index(drop=True)
        df.loc[0 : int(df.shape[0] * self.train_pct), self.split_colname] = "train"

        val_start = int(df.shape[0] * self.train_pct)
        val_end = int(df.shape[0] * self.train_pct) + int(df.shape[0] * self.val_pct)
        test_start = int(df.shape[0] * self.train_pct) + int(df.shape[0] * self.val_pct)

        df.loc[val_start:val_end, self.split_colname] = "val"
        df.loc[test_start:, self.split_colname] = "test"
        return df


class AbstractEvaluator(MLFlowLogger):
    def __init__(self):
        super().__init__()

    def evaluate(self):
        raise ValueError("The evaluate method must be overridden.")


class GenericMLEvaluator(AbstractEvaluator):
    def __init__(
        self,
        df=None,
        target_name=None,
        fits=None,
        classification_or_regression=None,
        groupby_cols=None,
        clf_threshold=0.5,
        drop_nas=True,
        groupby_col_order=dict(dataset_split=("train", "val", "test")),
        num_threads=1,
        round_decimals=3,
    ):
        self.df = df
        self.target_name = target_name
        self.fits = fits
        self.classification_or_regression = classification_or_regression
        self.groupby_cols = groupby_cols
        self.clf_threshold = clf_threshold
        self.drop_nas = drop_nas
        self.groupby_col_order = groupby_col_order
        self.num_threads = num_threads
        self.round_decimals = round_decimals

        self.evaluation_output = None

    def evaluate(self, **calc_ml_metrics_params):
        evaluator = CalcMLMetrics(round_decimals=self.round_decimals)

        if self.classification_or_regression.lower() == "classification":
            self.fits = [i for i in self.df.columns if i.endswith("_pred_class")]
        else:
            self.fits = [i for i in self.df.columns if i.endswith("_pred")]

        self.evaluation_output = evaluator.calc_ml_metrics(
            df=self.df,
            target_name=self.target_name,
            fits=self.fits,
            classification_or_regression=self.classification_or_regression,
            groupby_cols=self.groupby_cols,
            clf_threshold=self.clf_threshold,
            drop_nas=self.drop_nas,
            groupby_col_order=self.groupby_col_order,
            num_threads=self.num_threads,
        )

        self.evaluation_output = self.evaluation_output.reset_index(drop=True)

        self.evaluation_output = self.evaluation_output[["fit"] + [i for i in self.evaluation_output.columns if i != "fit"]]

        return self
