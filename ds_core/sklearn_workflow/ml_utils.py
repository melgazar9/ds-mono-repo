# -*- coding: utf-8 -*-
"""
Created on Sat Jan 30 10:57:45 2021

@author: melgazar9
"""
import inspect

from ds_core.ds_imports import *
from ds_core.ds_utils import *
from ds_core.sklearn_workflow.ml_imports import *

class SklearnMLFlow:

    input_features: list
    feature_creator: object
    feature_transformer: object
    resampler: object
    algorithms: tuple
    optimizer: object
    evaluator: object

    def __init__(self,
                 df,
                 input_features,
                 target_name=None,
                 split_colname='dataset_split',
                 preserve_vars=None,
                 clean_column_names=True,
                 feature_creator=None,
                 feature_transformer=None,
                 resampler=None,
                 algorithms=(),
                 optimizer=None,
                 evaluator=None):

        self.df_input = df.copy()
        self.df_out = df.copy()
        self.target_name = target_name
        self.split_colname = split_colname
        self.clean_column_names = clean_column_names

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

        self.preserve_vars = preserve_vars if preserve_vars is not None \
            else [i for i in self.df_input.columns if i not in self.input_features + [self.target_name]]

        if self.clean_column_names:
            self.df_out = clean_columns(self.df_out)
            self.input_features = clean_columns(pd.DataFrame(columns=self.input_features)).columns.tolist()
            self.output_features = clean_columns(pd.DataFrame(columns=self.output_features)).columns.tolist()
            self.preserve_vars = clean_columns(pd.DataFrame(columns=self.preserve_vars)).columns.tolist()
            self.target_name = clean_columns(pd.DataFrame(columns=[self.target_name])).columns[0]

        self.feature_creator = feature_creator

        self.feature_transformer = feature_transformer if feature_transformer is not None \
            else FeatureTransformer(target_name=self.target_name, preserve_vars=self.preserve_vars)

        self.resampler = resampler
        self.algorithms = algorithms
        self.optimizer = optimizer
        self.evaluator = evaluator

        self.input_cols = self.input_features + self.preserve_vars

        if self.target_name not in self.input_cols:
            self.input_cols.append(self.target_name)

        assert len(self.input_cols) == len(set(self.input_cols)), \
            "Not all features were considered! Check your inputs: input_features, preserve_vars, target_name"

    def create_features(self):

        """
        Description
        -----------
        This is the first step in the ML workflow.
        This method creates features, and currently only supports fit_transform for complexity reasons.
        Input: self.df_out, which is the same as self.df_input based on the __init__
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

        if hasattr(self, 'new_features') and len(self.new_features):
            self.feature_transformer.instantiate_column_transformer(
                self.df_out[self.df_out[self.split_colname] == 'train'],
                self.df_out[self.df_out[self.split_colname] == 'train'][self.target_name]
            )

            ### Assign feature groups ###

            if feature_groups is None:
                # assign new features to feature groups
                self.new_feature_groups = \
                    FeatureTransformer(target_name=self.target_name,
                                       preserve_vars=self.preserve_vars)\
                        .detect_feature_groups(self.df_out[self.new_features])

                # append the existing feature groups with the new features
                for fg in self.new_feature_groups.keys():
                    if len(self.new_feature_groups[fg]) > 0:
                        self.feature_transformer.feature_groups[fg] = \
                            self.feature_transformer.feature_groups[fg] + \
                            self.new_feature_groups[fg]
            else:
                self.feature_groups = feature_groups

            assert (len(find_list_duplicates(self.feature_transformer.output_features)) == 0), \
                "Failed to find new features."

        self.feature_transformer.target_name = self.target_name
        self.feature_transformer.preserve_vars = self.preserve_vars

        self.feature_transformer.fit(
            self.df_out[self.df_out[self.split_colname] == 'train'],
            self.df_out[self.df_out[self.split_colname] == 'train'][self.target_name]
        )

        self.df_out = self.feature_transformer.transform(self.df_out)
        self.output_cols = self.feature_transformer.output_cols
        self.output_features = self.feature_transformer.output_features

        return self

    def resample(self):
        if self.resampler is not None:
            try:
                self.df_train_resampled = \
                    self.resampler.fit_resample(
                        self.df_out[self.df_out[self.split_colname] == 'train'],
                        self.df_out[self.df_out[self.split_colname] == 'train'][self.target_name]
                    )
            except:
                try:
                    self.df_train_resampled = \
                        self.resampler.fit_transform(
                            self.df_out[self.df_out[self.split_colname] == 'train'],
                            self.df_out[self.df_out[self.split_colname] == 'train'][self.target_name]
                        )
                except:
                    raise ValueError("Could not fit the resampler.")

            return self

    def train_models(self, **fit_params):

        if hasattr(self, 'df_train_resampled'):
            X_train = self.df_train_resampled.drop(self.target_name, axis=1)
            y_train = self.df_train_resampled[self.target_name]
        else:
            X_train = self.df_out[self.df_out[self.split_colname] == 'train'][self.output_features]
            y_train = self.df_out[self.df_out[self.split_colname] == 'train'][self.target_name]

        if hasattr(self.algorithms, 'fit'):
            print(f"Running {type(self.algorithms).__name__}...\n")
            self.algorithms\
                .fit(X_train, y_train, **fit_params)
        else:
            assert isinstance(self.algorithms, (tuple, list))

            for algo in self.algorithms:
                print(f"Running{type(algo).__name__}...\n")
                algo.fit(X_train, y_train, **fit_params)
        print("\nModel training done!\n")
        return self

    def predict_models(self):
        if hasattr(self.algorithms, 'predict_proba'):
            self.df_out[type(self.algorithms).__name__ + '_pred'] = \
                self.algorithms.predict_proba(self.df_out[self.output_features])[:, 1]

        elif hasattr(self.algorithms, 'decision_function'):
            self.df_out[type(self.algorithms).__name__ + '_pred'] = \
                        algo.decision_function(self.df_out[self.output_features])[:, 1]
        else:
            assert isinstance(self.algorithms, (tuple, list))
            for algo in self.algorithms:
                if hasattr(algo, 'predict_proba'):
                    self.df_out[type(algo).__name__ + '_pred'] = \
                        algo.predict_proba(self.df_out[self.output_features])[:, 1]
                elif hasattr(algo, 'decision_function'):
                    self.df_out[type(algo).__name__ + '_pred'] = \
                        algo.decision_function(self.df_out[self.output_features])[:, 1]

        return self

    def assign_threshold_opt_rows(self,
                                  pct_train_for_opt=0.10,
                                  pct_val_for_opt=1,
                                  threshold_opt_name='use_for_threshold_opt'):
        self.threshold_opt_name = threshold_opt_name

        self.df_out.loc[:, threshold_opt_name] = False

        ### assign the train rows to be used for threshold optimization ###

        n_train_values = int(self.df_out[self.df_out[self.split_colname] == 'val'].shape[0] * pct_train_for_opt)

        # tail might not be a good way to choose the rows, but in case the data is sorted it makes sense as a default
        opt_train_indices = self.df_out[self.df_out[self.split_colname] == 'train'].tail(n_train_values).index
        self.df_out.loc[opt_train_indices, threshold_opt_name] = True


        ### assign the val rows to be used for threshold optimization ###

        n_val_values = int(self.df_out[self.df_out[self.split_colname] == 'val'].shape[0] * pct_val_for_opt)

        # tail might not be a good way to choose the rows, but in case the data is sorted it makes sense as a default
        opt_val_indices = self.df_out[self.df_out[self.split_colname] == 'val'].tail(n_val_values).index

        self.df_out.loc[opt_val_indices, threshold_opt_name] = True
        return self

    def optimize_models(self, maximize_or_minimize):
        if self.df_out[self.target_name].nunique() > 2:
            raise NotImplementedError('Threshold optimization currently only implemented for binary classification.')

        self.assign_threshold_opt_rows()

        if self.optimizer.y_true is None:
            self.optimizer.y_true = self.df_out[self.df_out[self.threshold_opt_name]][self.target_name]

        self.optimized_fits = {}
        for pred_col in [i for i in self.df_out.columns if i.endswith('_pred')]:
            self.optimizer.y_pred = self.df_out[self.df_out[self.threshold_opt_name]][pred_col]
            self.optimizer.run_optimization(maximize_or_minimize)
            self.optimized_fits[pred_col] = self.optimizer.best_score

            # assign the positive predicted class based on the chosen optimal threshold
            self.optimizer.df = self.df_out
            self.optimizer.pred_column = pred_col
            self.optimizer.pred_class_col = pred_col + '_class'
            self.optimizer.make_copy = False
            self.optimizer.assign_positive_class(self.optimized_fits[pred_col])

        return self
    def evaluate_models(self, **kwargs):

        ### set a default evaluator for the Amex competition ###

        if self.evaluator is None:
            # fits = [type(a).__name__ + '_pred' for a in self.algorithms]
            fits = [f for f in self.df_out.columns if 'pred' in f]
            if 'dummy' in self.df_out.columns and 'dummy' not in fits:
                fits.append('dummy')

            self.evaluator.df = self.df_out
            self.evaluator.target_name = self.target_name
            self.evaluator.fits = fits

        self.evaluator.evaluate(**kwargs)

        return self

    def run_ml_workflow(self,
                        create_features_params=None,
                        transform_features_params=None,
                        resampler_params=None,
                        train_model_params=None,
                        predict_model_params=None,
                        optimization_params=None,
                        evaluate_model_params=None):

        ### create features ###

        if self.feature_creator is not None:
            if create_features_params is None:
                self.create_features()
            else:
                self.create_features(**create_features_params)


        ### transform features ###

        if self.feature_transformer is not None:
            if transform_features_params is None:
                self.transform_features()
            else:
                self.transform_features(**transform_features_params)

        ### resample data ###

        if self.resampler is not None:
            if resampler_params is None:
                self.resample()
            else:
                self.resample(**resampler_params)


        ### train models ###

        if train_model_params is None:
            self.train_models()
        else:
            self.train_models(**train_model_params)

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

            if predict_model_params is None:
                self.predict_models()
            else:
                self.predict_models(**predict_model_params)


            ### run optimization ###

            if self.optimizer is not None:
                self.optimizer.run_optimization(**optimization_params)
                for pred in [i for i in self.df_out.columns if i.endswith('_pred')]:
                    self.df_out.loc[:, pred + '_class'] = 0
                    self.df_out.loc[
                        self.df_out[pred] >= self.optimizer.best_score['threshold'].iloc[0],
                        pred + '_class'] = 1

            ### evaluate models ###

            if evaluate_model_params is None:
                self.evaluate_models()
            else:
                self.evaluate_models(**evaluate_model_params)
        else:
            raise NotFittedError('Not all algorithms have been fit!')

        return self


def get_column_names_from_ColumnTransformer(column_transformer, clean_column_names=True, verbose=False):
    """
    Reference: Kyle Gilde: https://github.com/kylegilde/Kaggle-Notebooks/blob/master/Extracolumn_transformering-and-Plotting-Scikit-Feature-Names-and-Importances/feature_importance.py
    Description: Get the column names from the a ColumnTransformer containing transformers & pipelines

    Parameters
    ----------
    verbose: Bool indicating whether to print summaries. Default set to True.
    Returns
    -------
    a list of the correcolumn_transformer feature names
    Note:
    If the ColumnTransformer contains Pipelines and if one of the transformers in the Pipeline is adding completely new columns,
    it must come last in the pipeline. For example, OneHotEncoder, MissingIndicator & SimpleImputer(add_indicator=True) add columns
    to the dataset that didn't exist before, so there should come last in the Pipeline.
    Inspiration: https://github.com/scikit-learn/scikit-learn/issues/12525
    """

    assert isinstance(column_transformer, ColumnTransformer), "Input isn't a ColumnTransformer"

    check_is_fitted(column_transformer)

    try:
        new_column_names = column_transformer.get_feature_names_out()
    except:
        new_column_names, transformer_list = [], []
        for i, transformer_item in enumerate(column_transformer.transformers_):
            transformer_name, transformer, orig_feature_names = transformer_item
            orig_feature_names = list(orig_feature_names)

            if len(orig_feature_names) == 0:
                continue

            if verbose:
                print(f"\n\n{i}.Transformer/Pipeline: {transformer_name} {transformer.__class__.__name__}\n")
                print(f"\tn_orig_feature_names:{len(orig_feature_names)}")

            if transformer == 'drop' or transformer == 'passthrough':
                continue

            try:
                names = transformer.get_feature_names_out()

            except:

                try:
                    names = transformer[:-1].get_feature_names_out()

                except:

                    if isinstance(transformer, Pipeline):

                        # if pipeline, get the last transformer in the Pipeline
                        names = []
                        for t in transformer:
                            try:
                                transformer_feature_names = t.get_feature_names_out()
                            except:
                                try:
                                    transformer_feature_names = t.get_feature_names_out(orig_feature_names)
                                except:
                                    try:
                                        transformer_feature_names = t[:-1].get_feature_names_out()
                                    except:
                                        transformer = transformer.steps[-1][1]
                                        try:
                                            transformer_feature_names = transformer.cols
                                        except:
                                            raise ValueError(f"Could not get column names for transformer {t}")

                            [names.append(i) for i in transformer_feature_names if i not in names]

                    if hasattr(transformer, 'get_feature_names_out'):
                        if 'input_features' in transformer.get_feature_names_out.__code__.co_varnames:
                            names = list(transformer.get_feature_names_out(input_features=orig_feature_names))

                        else:
                            names = list(transformer.get_feature_names_out())

                    elif hasattr(transformer, 'get_feature_names'):
                        if 'input_features' in transformer.get_feature_names.__code__.co_varnames:
                            names = list(transformer.get_feature_names(orig_feature_names))
                        else:
                            names = list(transformer.get_feature_names())

                    elif hasattr(transformer, 'indicator_') and transformer.add_indicator:
                        # is this transformer one of the imputers & did it call the MissingIndicator?
                        missing_indicator_indices = transformer.indicator_.features_
                        missing_indicators = [orig_feature_names[idx] + '_missing_flag' for idx in
                                              missing_indicator_indices]
                        names = orig_feature_names + missing_indicators

                    elif hasattr(transformer, 'features_'):
                        # is this a MissingIndicator class?
                        missing_indicator_indices = transformer.features_
                        missing_indicators = [orig_feature_names[idx] + '_missing_flag' for idx in
                                              missing_indicator_indices]

                    else:
                        names = orig_feature_names

                    if verbose:
                        print(f"\tn_new_features:{len(names)}")
                        print(f"\tnew_features: {names}\n")

            new_column_names.extend(names)
            transformer_list.extend([transformer_name] * len(names))

        if column_transformer.remainder == 'passthrough':
            passthrough_cols = column_transformer.feature_names_in_[column_transformer.transformers_[-1][-1]]
            new_column_names = list(new_column_names) + [i for i in passthrough_cols if i not in new_column_names]

    if clean_column_names:
        new_column_names = [i.replace('remainder__', '') for i in new_column_names]
        new_column_names = list(clean_columns(pd.DataFrame(columns=new_column_names)).columns)

    return new_column_names

class FeatureTransformer(TransformerMixin):

    """
        Parameters
        ----------
        preserve_vars : A list of variables that won't be fitted or transformed by any sort of feature engineering
        target_name : A string - the name of the target variable.
        remainder : A string that gets passed to the column transformer whether to
                    drop preserve_vars or keep them in the final dataset
                    options are 'drop' or 'passthrough'
        max_lc_cardinality : A natural number - one-hot encode all features with unique categories <= to this value
        FE_pipeline_dict : Set to None to use "standard" feature engineering pipeline.
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

    def __init__(self,
                 target_name=None,
                 preserve_vars=None,
                 FE_pipeline_dict=None,
                 remainder='passthrough',
                 max_lc_cardinality=11,
                 run_detect_feature_groups=True,
                 numeric_features=None,
                 lc_features=None,
                 hc_features=None,
                 overwrite_detection=True,
                 n_jobs=-1,
                 clean_column_names=True,
                 make_copy=True,
                 verbose=True):

        self.preserve_vars =preserve_vars
        self.target_name = target_name
        self.FE_pipeline_dict = FE_pipeline_dict
        self.remainder = remainder
        self.max_lc_cardinality = max_lc_cardinality
        self.run_detect_feature_groups = run_detect_feature_groups
        self.numeric_features = [] if numeric_features is None else numeric_features
        self.lc_features = [] if lc_features is None else lc_features
        self.hc_features = [] if hc_features is None else hc_features
        self.overwrite_detection = overwrite_detection
        self.n_jobs = n_jobs
        self.clean_column_names = clean_column_names,
        self.verbose = verbose
        self.make_copy = make_copy
        self.preserve_vars = [] if self.preserve_vars is None else self.preserve_vars

        self.column_transformer = ColumnTransformer(transformers=[],
                                                    remainder=self.remainder,
                                                    n_jobs=self.n_jobs)

    def detect_feature_groups(self, X):

        if self.make_copy: X = X.copy()

        if not self.run_detect_feature_groups:
            if self.verbose: print('Not detecting dtypes.')

            feature_dict = {'numeric_features': self.numeric_features,
                            'lc_features': self.lc_features,
                            'hc_features': self.hc_features}
            if self.FE_pipeline_dict is not None and 'custom_pipe' in self.FE_pipeline_dict.keys():
                feature_dict['custom_features'] = list(self.FE_pipeline_dict['custom_pipe'].values())[0]
            return feature_dict

        if self.FE_pipeline_dict is not None and 'custom_pipe' in self.FE_pipeline_dict.keys():
            custom_features = list(itertools.chain(*self.FE_pipeline_dict['custom_pipe'].values()))
        else:
            custom_features = []

        assert len(np.intersect1d(list(set(self.numeric_features + \
                                           self.lc_features + \
                                           self.hc_features + \
                                           custom_features)), \
                                  self.preserve_vars)) == 0, \
            'There are duplicate features in preserve_vars either the input\
             numeric_features, lc_features, or hc_features'

        detected_numeric_vars = make_column_selector(dtype_include=np.number)(
            X[[i for i in X.columns \
               if i not in self.preserve_vars + \
               [self.target_name] + \
               custom_features]])

        detected_lc_vars = [i for i in X.loc[:, (X.nunique(dropna=False) <= self.max_lc_cardinality) & \
                                                (X.nunique(dropna=False) > 1)].columns \
                            if i not in self.preserve_vars + \
                            [self.target_name] + \
                            custom_features]

        detected_hc_vars = X[[i for i in X.columns \
                              if i not in self.preserve_vars + \
                              custom_features]] \
            .select_dtypes(['object', 'category']) \
            .apply(lambda col: col.nunique(dropna=False)) \
            .loc[lambda x: x > self.max_lc_cardinality] \
            .index.tolist()

        discarded_features = [i for i in X.isnull().sum()[X.isnull().sum() == X.shape[0]].index \
                              if i not in self.preserve_vars]

        numeric_features = list(set([i for i in self.numeric_features + \
                                     [i for i in detected_numeric_vars \
                                      if i not in list(self.lc_features) + \
                                      list(self.hc_features) + \
                                      list(discarded_features) + \
                                      custom_features]]))

        lc_features = list(set([i for i in self.lc_features + \
                                [i for i in detected_lc_vars \
                                 if i not in list(self.numeric_features) + \
                                 list(self.hc_features) + \
                                 list(discarded_features) + \
                                 custom_features]]))

        hc_features = list(set([i for i in self.hc_features + \
                                [i for i in detected_hc_vars \
                                 if i not in list(self.numeric_features) + \
                                 list(self.lc_features) + \
                                 list(discarded_features) + \
                                 custom_features]]))

        if self.verbose:
            print('Overlap between numeric and lc_features: ' + \
                  str(list(set(np.intersect1d(numeric_features, lc_features)))))
            print('Overlap between numeric and hc_features: ' + \
                  str(list(set(np.intersect1d(numeric_features, hc_features)))))
            print('Overlap between numeric lc_features and hc_features: ' + \
                  str(list(set(np.intersect1d(lc_features, hc_features)))))
            print('Overlap between lc_features and hc_features will be moved to lc_features')

        if self.overwrite_detection:
            numeric_features = [i for i in numeric_features \
                                if i not in lc_features + \
                                hc_features + \
                                discarded_features + \
                                custom_features]

            lc_features = [i for i in lc_features \
                           if i not in hc_features + \
                           numeric_features + \
                           discarded_features + \
                           custom_features]

            hc_features = [i for i in hc_features if i not in \
                           lc_features + \
                           numeric_features + \
                           discarded_features + \
                           custom_features]

        else:
            numeric_overlap = [i for i in numeric_features \
                               if i in lc_features \
                               or i in hc_features \
                               and i not in discarded_features + \
                               custom_features]

            lc_overlap = [i for i in lc_features \
                          if i in hc_features \
                          or i in numeric_features \
                          and i not in discarded_features + \
                          custom_features]

            hc_overlap = [i for i in hc_features \
                          if i in lc_features \
                          or i in numeric_features \
                          and i not in discarded_features + \
                          custom_features]

            if numeric_overlap or lc_overlap or hc_overlap:
                raise AssertionError('There is an overlap between numeric, \
                                     lc, and hc features! \
                                     To ignore this set overwrite_detection to True.')

        all_features = list(set(numeric_features + \
                                lc_features + \
                                hc_features + \
                                discarded_features + \
                                custom_features))

        all_features_debug = set(all_features) - \
                             set([i for i in X.columns \
                                  if i not in \
                                  self.preserve_vars + [self.target_name]])

        if len(all_features_debug) > 0:
            print('\n{}\n'.format(all_features_debug))
            raise AssertionError('There was a problem detecting all features!! \
                Check if there is an overlap between preserve_vars and other custom input features')

        if self.verbose:
            print(f'\nnumeric_features: {numeric_features}')
            print(f'\nlc_features: {lc_features}')
            print(f'\nhc_features: {hc_features}')
            print(f'\ndiscarded_features: {discarded_features}')
            print(f'\ncustom_features: {custom_features}')

        feature_dict = {'numeric_features': numeric_features,
                        'lc_features': lc_features,
                        'hc_features': hc_features,
                        'custom_features': custom_features,
                        'discarded_features': discarded_features}

        return feature_dict

    def instantiate_column_transformer(self, X, y=None):

        if self.target_name is None and y is not None:
            self.target_name = y.name

        assert y is not None or self.target_name is not None, '\n Both self.target_name and y cannot be None!'

        self.feature_groups = self.detect_feature_groups(X)

        # set a default transformation pipeline if FE_pipeline_dict is not specified
        if self.FE_pipeline_dict is None:

            ### Default pipelines ###

            na_replacer = \
                FunctionTransformer(lambda x: \
                                        x.replace([-np.inf, np.inf, None, 'None', \
                                                   '', ' ', 'nan', 'Nan'], \
                                                  np.nan),
                                    feature_names_out='one-to-one')

            numeric_pipe = make_pipeline(
                na_replacer,
                # Winsorizer(distribution='gaussian', tail='both', fold=3, missing_values = 'ignore'),
                MinMaxScaler(feature_range=(0, 1)),
                SimpleImputer(strategy='median', add_indicator=True)
            )

            hc_pipe = make_pipeline(
                na_replacer,
                FunctionTransformer(lambda x: x.astype(str), feature_names_out='one-to-one'),
                TargetEncoder(cols=self.hc_features,
                              return_df=True,
                              handle_missing='value',
                              handle_unknown='value',
                              min_samples_leaf=10)
            )

            lc_pipe = make_pipeline(
                na_replacer,
                OneHotEncoder(handle_unknown='ignore', sparse=False)
            )

            custom_pipe = None

        else:
            hc_pipe = self.FE_pipeline_dict['hc_pipe']
            numeric_pipe = self.FE_pipeline_dict['numeric_pipe']
            lc_pipe = self.FE_pipeline_dict['lc_pipe']

            custom_pipe = self.FE_pipeline_dict['custom_pipe'] \
                if 'custom_pipe' in self.FE_pipeline_dict.keys() else {}

        transformers = [
            ('hc_pipe', hc_pipe, self.feature_groups['hc_features']),
            ('numeric_pipe', numeric_pipe, self.feature_groups['numeric_features']),
            ('lc_pipe', lc_pipe, self.feature_groups['lc_features'])
        ]

        for alias, transformer, cols in transformers:
            if isinstance(transformer, Pipeline):
                for t in transformer:
                    if 'cols' in list(inspect.signature(t.__class__).parameters.keys()):
                        t.cols = cols
            else:
                if 'cols' in list(inspect.signature(t.__class__).parameters.keys()):
                    t.cols = transformer[-1]

        if custom_pipe:
            setattr(self, 'custom_features', list(set(np.concatenate(list(custom_pipe.values())))))
            i = 0
            for cp in custom_pipe.keys():
                transformers.append(('custom_pipe{}'.format(str(i)), cp, custom_pipe[cp]))
                i += 1

        self.column_transformer.transformers = transformers

    def fit(self, X, y=None):
        self.instantiate_column_transformer(X, y)

        if y is None:
            self.column_transformer.fit(X)
        else:
            self.column_transformer.fit(X, y)

        self.output_cols = get_column_names_from_ColumnTransformer(self.column_transformer,
                                                                   clean_column_names=self.clean_column_names,
                                                                   verbose=self.verbose)
        input_features = \
            self.feature_groups['numeric_features'] + \
            self.feature_groups['lc_features'] + \
            self.feature_groups['hc_features'] + \
            self.feature_groups['custom_features']

        if len(self.preserve_vars):
            self.preserve_vars_orig = self.preserve_vars.copy()
            self.preserve_vars = list(clean_columns(pd.DataFrame(columns=self.output_cols[len(input_features): ])).columns)

        setattr(self, 'output_features',
                [i for i in self.output_cols
                 if i not in self.preserve_vars])

        assert len(self.output_features + self.preserve_vars) == len(self.output_cols)
        assert len(set(self.output_cols)) == len(self.output_cols)

        return self

    def transform(self, X, return_df=True):
        X_out = self.column_transformer.transform(X)

        if return_df:
            return pd.DataFrame(list(X_out), columns=self.output_cols)
        else:
            return X_out


class FeatureImportance:
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

    verbose: Bool to print statements during calculation

    """

    def __init__(self, model=None, df=None, input_features=None, round_decimals=3, verbose=False):
        self.model = model
        self.df = df
        self.input_features = input_features
        self.round_decimals = round_decimals
        self.verbose = verbose

        if self.df is not None:
            self.input_features = self.df.columns if self.input_features is None else self.input_features

    def get_feature_importance(self):

        assert (self.df is not None or self.input_features is not None) \
               and self.model is not None, \
            "Both the input self.df and self.input_features \
             cannot be None when calculating feature importance!"

        # sklearn tree models usually have the feature_importance_ attribute
        try:
            importances = self.model.feature_importances_
            feature_importances = \
                pd.Series(importances,
                          index=self.input_features)\
                    .sort_values(ascending=False).reset_index()

        # sklearn linear models usually have the .coef_ attribute
        except AttributeError:
            try:
                feature_importances = pd.DataFrame(self.model.coef_, index=self.input_features, columns=['coefs'])
                feature_importances['importance'] = feature_importances['coefs'].abs()
                feature_importances.sort_values(by='importance', ascending=False, inplace=True)
                feature_importances.reset_index(inplace=True)
                feature_importances.drop('coefs', inplace=True, axis=1)
            except:
                print("Cannot get feature importance for this model")
                return

        assert len(self.input_features) == len(feature_importances), \
            f"The number of feature names {len(self.input_features)}\
              and importance values {len(importances)} don't match"

        feature_importances.columns = ['feature', 'importance']

        if self.round_decimals:
            feature_importances['importance'] = round(feature_importances['importance'], self.round_decimals)
        return feature_importances

    def plot_importance(self,
                        feature_importances=None,
                        top_n_features=100,
                        orientation='h',
                        height=None,
                        width=200,
                        height_per_feature=10,
                        yaxes_tickfont_family='Courier New',
                        yaxes_tickfont_size=15,
                        **px_bar_params):

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

            feature_importances = \
                feature_importances \
                    .nlargest(top_n_features, 'importance') \
                    .sort_values(by='importance', ascending=False)

            n_all_importances = feature_importances.shape[0]

            title_text = f"All Feature Importances" if top_n_features > n_all_importances \
                                                       or top_n_features is None \
                else f'Top {top_n_features} (of {n_all_importances}) Feature Importances'

            feature_importances.sort_values(by='importance', inplace=True)
            if self.round_decimals:
                feature_importances['text'] = feature_importances['importance'].round(self.round_decimals).astype(str)
            else:
                feature_importances['text'] = feature_importances['importance'].astype(str)

            # create the plot
            fig = px.bar(feature_importances,
                         x='importance',
                         y='feature',
                         orientation=orientation,
                         width=width,
                         height=height,
                         text='text',
                         **px_bar_params)

            fig.update_layout(title_text=title_text, title_x=0.5)
            fig.update(layout_showlegend=False)
            fig.update_yaxes(tickfont=dict(family=yaxes_tickfont_family, size=yaxes_tickfont_size))

            return fig

        else:
            return


class ImbResampler():
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
        self.algorithm = algorithm

    def fit_transform(self, X, y):
        ds_print('Running {}'.format(type(self.algorithm).__name__))
        df_resampled = self.algorithm.fit_resample(X, y)
        return df_resampled

def timeseries_split(df,
                     datetime_col='date',
                     split_by_datetime_col=True,
                     train_prop=0.7,
                     val_prop=0.15,
                     target_name='target',
                     return_single_df=True,
                     split_colname='dataset_split',
                     sort_df_params={},
                     reset_datetime_index=True,
                     make_copy=True):
    """
    Get the column names from the a ColumnTransformer containing transformers & pipelines
    Parameters
    ----------
    df: a pandas (compatible) dataframe
    datetime_col: str date column to run the timeseries split on
    train_prop: float - proportion of train orders assuming the data is sorted in ascending order
        example: 0.7 means 70% of the df will train orders
    val_prop: float - proportion of val orders
        e.g. 0.1 means 10% of the df will be val orders, where the val orders come after the train orders
    target_name: str - name of the target variable
    return_single_df: If true then a new column <split_colname> will be concatenated to the df with 'train', 'val', or 'test'
    split_colname: If return_single_df is True, then this is the name of the new split col
    sort_df_params: Set to False or empty dict to not sort the df by any column.
        To sort, specify as dict the input params to either df.sort_index or df.sort_values.
    reset_datetime_index: Bool to reset the datetime index if splitting by datetime_col
    Returns
    -------
    Either a tuple of dataframes: X_train, y_train, X_val, y_val, X_test, y_test
      Or the same df with a new <split_colname> having ['train', 'val', 'test'] or 'None' populated

    """

    if make_copy: df = df.copy()

    if len(df.index) != df.index[-1] - df.index[0]:
        df.reset_index(inplace=True)

    nrows = len(df)
    if sort_df_params:
        if list(sort_df_params.keys())[0].lower() == 'index' and 'index' not in df.columns:
            df.sort_index(**sort_df_params)
        else:
            df.sort_values(**sort_df_params)

    elif datetime_col in df.columns:
        df.set_index(datetime_col, inplace=True)
        if reset_datetime_index:
            df.reset_index(datetime_col, inplace=True)

    if 'index' in df.columns:
        df.drop('index', axis=1, inplace=True)

    if return_single_df:

        if split_by_datetime_col:
            # max_train_date = df.iloc[0:int(np.floor(nrows*train_prop))][datetime_col].max()
            # max_val_date = df.iloc[int(np.floor(nrows*train_prop)): int(np.floor(nrows*(1 - val_prop)))][datetime_col].max()

            dates = np.unique(df['date'].sort_values())
            max_train_date = dates[int(len(dates) * train_prop)]
            max_val_date = dates[int(len(dates) * (1 - val_prop))]

            df.loc[df[datetime_col] < max_train_date, split_colname] = 'train'
            df.loc[(df[datetime_col] >= max_train_date) & (df[datetime_col] < max_val_date), split_colname] = 'val'
            df.loc[df[datetime_col] >= max_val_date, split_colname] = 'test'
        else:
            df.loc[0:int(np.floor(nrows * train_prop)), split_colname] = 'train'
            df.loc[int(np.floor(nrows * train_prop)): int(np.floor(nrows * (1 - val_prop))), split_colname] = 'val'
            df.loc[int(np.floor(nrows * (1 - val_prop))):, split_colname] = 'test'

        return df

    else:

        X_train = df.iloc[0:int(np.floor(nrows * train_prop))][feature_list]
        y_train = df.iloc[0:int(np.floor(nrows * train_prop))][target_name]

        X_val = df.iloc[int(np.floor(nrows * train_prop)):int(np.floor(nrows * (1 - val_prop)))][feature_list]
        y_val = df.iloc[int(np.floor(nrows * train_prop)):int(np.floor(nrows * (1 - val_prop)))][target_name]

        X_test = df.iloc[int(np.floor(nrows * (1 - val_prop))):][feature_list]
        y_test = df.iloc[int(np.floor(nrows * (1 - val_prop))):][target_name]
        return X_train, y_train, X_val, y_val, X_test, y_test


class ThresholdOptimizer:

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

    def __init__(self, df, pred_column, pred_class_col='pred_class', make_copy=True):

        self.df = df
        self.pred_column = pred_column
        self.pred_class_col = pred_class_col
        self.make_copy = make_copy

        if self.make_copy:
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
    def best_scores(threshold_df):

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

    def assign_positive_class(self, best_threshold_df):

        """
        Description: Assign a positive predicted class based on the output of best_scores

        Parameters
        ----------
        best_threshold_df: pandas dataframe output from self.best_scores

        Returns
        -------
        pandas df with the new pred_class from the optimal thresholds in best_threshold_df

        """

        assert (self.df is not None) and (self.pred_column is not None), \
            "Both self.df and self.pred_column must be provided when calling self.assign_positive_class."

        if (self.pred_class_col in self.df.columns):
            warnings.warn(f"pred_class_col {self.pred_class_col} is in self.df. \
                The column {self.pred_class_col} will be overwritten.")

        if len(best_threshold_df.index.names) == 1:
            groupby_cols = None

        else:
            assert (len([i for i in np.intersect1d(best_threshold_df.columns, self.df.columns)
                         if i != 'threshold']) == 0), \
                "column intersection between df.columns and best_threshold_df, excluding threshold, must have length 0."
            groupby_cols = [i for i in list(best_threshold_df.index.names) if i != 'threshold']

        self.df.loc[:, self.pred_class_col] = 0

        if groupby_cols is None:
            if best_threshold_df.index.name == 'threshold':
                threshold_cutoff = best_threshold_df.index.get_level_values('threshold').min()
            else:
                threshold_cutoff = best_threshold_df['threshold'].min()
            self.df.loc[self.df[self.pred_column] >= threshold_cutoff, self.pred_class_col] = 1
            return self.df
        else:
            if 'threshold' in self.df.columns:
                self.df.drop('threshold', axis=1, inplace=True)
            self.df = \
                self.df.merge(best_threshold_df.reset_index()[groupby_cols + ['threshold']],
                              on=groupby_cols,
                              how='left')

            self.df.loc[self.df[self.pred_column] >= self.df['threshold'], self.pred_class_col] = 1

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
        self.optimization_func = optimization_func
        self.y_pred = y_pred
        self.y_true = y_true

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

        assert str(type(self.optimization_func)) == "<class 'function'>", 'optimization_func must be a function!'

        self.thresholds = np.arange(0, 100) / 100 if thresholds is None else thresholds

        score_df = pd.DataFrame({'y_pred': self.y_pred, 'y_true': self.y_true})

        if num_threads == 1:
            self.scores = {}
            for thres in self.thresholds:
                score_df.loc[:, 'pred_class'] = 0
                score_df.loc[score_df['y_pred'] >= thres, 'pred_class'] = 1
                self.scores[thres] = self.optimization_func(y_pred=score_df['pred_class'], y_true=score_df['y_true'])

            self.threshold_df = pd.DataFrame.from_dict(self.scores, orient='index').reset_index()
            self.threshold_df.columns = ['threshold', 'score']
        else:
            raise NotImplementedError('Multi-threading not implemented yet.')

        return self

    def best_scores(self, minimize_or_maximize, threshold_df=None):
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
            groupby_cols = [i for i in self.threshold_df.index.names if i != 'threshold']
            if minimize_or_maximize.lower() == 'maximize':
                best_score = self.threshold_df.groupby(groupby_cols).apply(
                    lambda x: x[x['score'] == x['score'].max()]
                )

            elif minimize_or_maximize.lower() == 'maximize':
                best_score = self.threshold_df.groupby(groupby_cols).apply(
                    lambda x: x[x['score'] == x['score'].max()]
                )

            else:
                raise ValueError("maximize_or_minimize must be set to 'maximize' or 'minimize'")

            indices = np.flatnonzero(pd.Index(best_score.index.names).duplicated()).tolist()
            best_score.reset_index(indices, drop=True, inplace=True)
        else:
            if minimize_or_maximize.lower() == 'maximize':
                self.best_score = self.threshold_df[self.threshold_df['score'] == self.threshold_df['score'].max()]
            elif minimize_or_maximize.lower() == 'minimize':
                self.best_score = self.threshold_df[self.threshold_df['score'] == self.threshold_df['score'].min()]

        return self

    def run_optimization(self, maximize_or_minimize):
        self.optimize()
        self.best_scores(maximize_or_minimize)
        return self