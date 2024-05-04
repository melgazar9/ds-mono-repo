def parallize_pandas_func(
    df, df_attribute, parallelize_by_col=True, num_workers=mp.cpu_count(), **kwargs
):
    """parallelize by row not implemented yet"""
    start_pos = 0
    chunk_len = int(np.floor(len(df.columns) / num_workers))
    delayed_list = []
    end_pos = chunk_len

    if parallelize_by_col:
        for chunk in range(num_workers):
            if chunk != num_workers - 1:
                df_subset = df.iloc[:, start_pos:end_pos]
                delayed_list.append(delayed(getattr(df_subset, df_attribute)(**kwargs)))
                start_pos += chunk_len
                end_pos += chunk_len
            else:
                df_subset = df.iloc[:, start_pos:]
                delayed_list.append(delayed(getattr(df_subset, df_attribute)(**kwargs)))

        dask_tuple = dask.compute(*delayed_list)
        df_out = pd.concat([i for i in dask_tuple], axis=1)
        return df_out
