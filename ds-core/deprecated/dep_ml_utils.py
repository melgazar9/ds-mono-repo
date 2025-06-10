# flake8: noqa


class RunModel:
    """
    Parameters
    ----------
    algorithm : A class object that must contain fit and transform attributes
    X_train : The training data - a pandas dataframe or array-like, sparse matrix of shape (n_samples, n_features)
              The input samples datatypes will be converted to ``dtype=np.float32`` if convert_float32 == True
              Note many algorithms already do this internally.
    y_train : a pandas series or numpy array containing the target variable data
    X_test : The testing data - a pandas dataframe or array-like, sparse matrix of shape (n_samples, n_features)
             The input samples datatypes will be converted to ``dtype=np.float32``
             In production we use this as df_full (all data consisting of train, val, and test)
    features : A list of training features used to train the model.
               All feature datatypes in X_train must be numeric
    seed : integer random seed
    convert_float32 : boolean to convert X_train, X_val (if supplied), and X_test to datatype float32
    bypass_all_numeric : boolean - set to True if you want to bypass the error that should normally get thrown if all variables are not numeric
    df_full : pandas df - the original "full" dataframe that includes train, val, and test - NOT transformed
    map_predictions_to_df_full : boolean whether to map model predictions onto df_full
    NOTE: If map_predictions_to_df_full == True, then df_full needs to be the original full
    pandas df that consists of train, val, and test data, and X_test needs to be df_full_transformed
    ****** To use a validation set during training you need to pass fit_params ******
    -------
    Attributes
    To get train_model attributes see the documenation for which algorithm you choose
    predict_model are pandas dataframe attributes
    run_everything attributes are dictionary attributes
    -------
    Returns
    train_model returns a model object
    test_model returns a dataframe
    run_everything returns a dictionary
    -------
    """

    def __init__(
        self,
        features,
        X_test=None,
        X_train=None,
        y_train=None,
        algorithm=None,
        eval_set=None,
        copy=True,
        prediction_colname="prediction",
        seed=100,
        convert_float32=True,
        bypass_all_numeric=False,
        df_full=None,
        map_predictions_to_df_full=True,
        predict_proba=False,
        use_eval_set_when_possible=True,
        **kwargs
    ):

        self.features = features
        self.X_test = X_test
        self.X_train = X_train
        self.y_train = y_train
        self.algorithm = algorithm
        self.eval_set = eval_set
        self.use_eval_set_when_possible = use_eval_set_when_possible
        self.prediction_colname = prediction_colname
        self.seed = seed
        self.convert_float32 = convert_float32
        self.copy = copy
        self.bypass_all_numeric = bypass_all_numeric
        self.df_full = df_full
        self.map_predictions_to_df_full = map_predictions_to_df_full
        self.predict_proba = predict_proba
        self.kwargs = kwargs

        if self.copy:
            self.X_train, self.X_test = self.X_train.copy(), self.X_test.copy()
            self.y_train = self.y_train.copy()

        if self.convert_float32:
            # self.X_train[self.features] = parallize_pandas_func(self.X_train[self.features], 'astype', dtype='float32', copy=self.copy)
            # self.X_test[self.features] = parallize_pandas_func(self.X_test[self.features], 'astype', dtype='float32', copy=self.copy)
            self.X_train[self.features] = self.X_train[self.features].astype("float32")
            self.X_test[self.features] = self.X_test[self.features].astype("float32")

        if (self.use_eval_set_when_possible) and (self.eval_set is not None):
            X_val_tmp, y_val_tmp = self.eval_set[0][0], self.eval_set[0][1]

            if self.copy:
                X_val_tmp, y_val_tmp = X_val_tmp.copy(), y_val_tmp.copy()

            if self.convert_float32:
                # self.X_val[self.features] = parallize_pandas_func(self.X_val[self.features], 'astype', dtype='float32', copy=self.copy)
                X_val_tmp[self.features] = X_val_tmp[self.features].astype("float32")

            self.eval_set = [(X_val_tmp, y_val_tmp)]
            del X_val_tmp, y_val_tmp

    def train_model(self):

        np.random.seed(self.seed)

        assert all(
            f in self.X_train.columns for f in self.features
        ), "Missing features in X_train!"

        if not self.bypass_all_numeric:  # need to check all features are numeric
            assert len(
                list(
                    set(
                        [
                            i
                            for i in self.X_train.select_dtypes(
                                include=np.number
                            ).columns
                            if i in self.features
                        ]
                    )
                )
            ) == len(self.features), "Not all features in X_train are numeric!"

            if (self.use_eval_set_when_possible) and (self.eval_set is not None):
                assert all(
                    f in self.eval_set[0][0].columns for f in self.features
                ), "Missing features in X_val!"
                assert len(
                    list(
                        set(
                            [
                                i
                                for i in self.eval_set[0][0]
                                .select_dtypes(include=np.number)
                                .columns
                                if i in self.features
                            ]
                        )
                    )
                ) == len(self.features), "Not all features in X_val are numeric!"

        assert self.X_train.shape[0] == len(
            self.y_train
        ), "X_train shape does not match y_train shape!"

        ds_print("\nRunning " + type(self.algorithm).__name__ + "...\n")

        if (
            ("fit_params" in self.kwargs.keys())
            and (set(list(self.kwargs["fit_params"].keys()) + ["eval_set"])).issubset(
                list(inspect.signature(self.algorithm.fit).parameters)
            )
            and (len(self.kwargs["fit_params"]) > 0)
        ):

            # All kwargs in the fit_params are available in the model.fit object
            # (e.g. list(inspect.getfullargspec(RandomForestClassifier.fit))[0] must have all params inside fit_params)

            ds_print(
                "\nUsing a validation set for "
                + type(self.algorithm).__name__
                + "...\n"
            )

            if self.use_eval_set_when_possible and self.eval_set is not None:
                model = self.algorithm.fit(
                    self.X_train,
                    self.y_train,
                    eval_set=self.eval_set,
                    **self.kwargs["fit_params"]
                )
            else:
                model = self.algorithm.fit(
                    self.X_train, self.y_train, **self.kwargs["fit_params"]
                )

        elif (
            self.use_eval_set_when_possible
            and self.eval_set is not None
            and {"eval_set"}.issubset(
                list(inspect.signature(XGBRegressor.fit).parameters)
            )
        ):

            ds_print(
                "\nUsing a validation set for "
                + type(self.algorithm).__name__
                + "...\n"
            )
            model = self.algorithm.fit(
                self.X_train, self.y_train, eval_set=self.eval_set
            )

        else:
            ds_print(
                "\nNot using an eval_set for " + type(self.algorithm).__name__ + "...\n"
            )
            model = self.algorithm.fit(self.X_train, self.y_train)
            ds_print("\n" + type(model).__name__ + " training done!\n")

        return model

    def predict_model(self, model):
        """
        Parameters
        ----------
        model : a trained model object
        X_test : a pandas dataframe or np.array-like object to perform predictions on
        """

        if ("predict_params" in self.kwargs) and (
            len(self.kwargs["predict_params"]) > 0
        ):

            if not self.predict_proba:
                predictions = model.predict(
                    self.X_test[self.features], **self.kwargs["predict_params"]
                )
            else:
                try:
                    predictions = model.predict_proba(
                        self.X_test[self.features], **self.kwargs["predict_params"]
                    )[:, 1]
                except:
                    try:
                        predictions = model.decision_function(
                            self.X_test[self.features], **self.kwargs["predict_params"]
                        )[:, 1]
                    except:
                        sys.exit(
                            "model does not have predict_proba or decision_function attribute"
                        )
        else:

            if not self.predict_proba:
                predictions = model.predict(self.X_test[self.features])
            else:
                try:
                    predictions = model.predict_proba(self.X_test[self.features])[:, 1]
                except:
                    try:
                        predictions = model.decision_function(
                            self.X_test[self.features]
                        )[:, 1]
                    except:
                        ds_print(
                            "model does not have predict_proba or decision_function attribute"
                        )
                        sys.exit()

        if not self.map_predictions_to_df_full:
            ds_print("Returning only X_test to df_pred in model_dict")
            assert len(predictions) == self.X_test.shape[0]
            self.X_test[self.prediction_colname] = predictions
            return self.X_test
        else:
            if self.copy:
                self.df_full = self.df_full.copy()
            ds_print("Returning df_full to df_pred in model_dict")
            assert self.df_full is not None, "df_full is None"
            assert len(predictions) == self.df_full.shape[0]
            self.df_full[self.prediction_colname] = predictions
            return self.df_full

    def train_and_predict(self):

        run_model_dict = dict()
        run_model_dict["model"] = self.train_model()
        run_model_dict["df_pred"] = self.predict_model(model=run_model_dict["model"])

        return run_model_dict


### thinking about how to integrate split optimization ###
class SplitOptimizer(ScoreThresholdOptimizer):
    """
    The goal of this class is to optimize the predicted class decision by capturing the maximum score value from
        self.optimize, while minimumizing the distance across the train, val, and test to reduce over-fitting.
    """

    def __init__(self, split_colname="dataset_split"):
        self.split_colname = split_colname

    def run_split_optimization(
        self,
        df,
        fits,
        minimize_or_maximize,
        split_colname="dataset_split",
        target_name=None,
        num_threads=1,
    ):
        """
        Description
        -----------
        Like ScoreThresholdOptimizer, this method runs the optimization pipeline, calling methods in the proper sequence.
        Note that if multiple fits are passed, self.best_score will return the most recent best threshold df
            and self.best_thresholds is a dictionary containing the best thresholds for each fit

        :param df: optional pandas df consisting of the fit columns
        :param fits: list or tuple of fits to optimize
        :param minimize_or_maximize: bool whether to minimize or maximize the optimization function
        :param split_colname: str of the split column that must contain values 'train', 'val', and 'test'
        :param target_name: str of the target name
        :param num_threads: int of number of threads to use
        :return: self
        """

        self.run_score_optimization(
            df=df,
            fits=fits,
            minimize_or_maximize=minimize_or_maximize,
            target_name=target_name,
            splits_to_assess=("train", "val"),
            num_threads=num_threads,
        )

        ### assess the predicted class from the chosen optimized threshold(s) by split ###

        if splits_to_assess is not None:
            self.threshold_opt_results_by_split = pd.DataFrame()
            for fit in fits:
                _threshold_results_by_split = pd.DataFrame(
                    df[df[self.split_colname].isin(splits_to_assess)]
                    .groupby(self.split_colname)
                    .apply(
                        lambda x: self.optimization_func(x[target_name], x[pred_class])
                    ),
                    columns=[pred_class],
                )

                self.threshold_opt_results_by_split = pd.concat(
                    [self.threshold_opt_results_by_split, _threshold_results_by_split],
                    axis=1,
                )

            self.threshold_opt_results_by_split.index = (
                self.threshold_opt_results_by_split.index.astype(
                    pd.CategoricalDtype(splits_to_assess, ordered=True)
                )
            )
            self.threshold_opt_results_by_split.sort_index(inplace=True)
        return self
