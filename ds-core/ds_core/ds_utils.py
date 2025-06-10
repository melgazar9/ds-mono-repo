import ast
import json
import os
import re
import subprocess
import time
from collections import Counter
from datetime import datetime
from glob import glob
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import yagmail


class MetaclassMethodEnforcer:
    """
    Description
    -----------
    A metaclass enforcer that ensures certain method names exist within a class.
    Enforce classes to use the same methods for code reliability across team members
    """

    def __init__(self, required_methods, parent_class):
        self.required_methods = required_methods
        self.parent_class = parent_class

    @staticmethod
    def override(f, cls):
        def method(*args, **kwargs):
            return f(*args, **kwargs)

        method.__name__ = f.__name__
        method.__doc__ = f.__doc__
        return method

    def enforce(self):
        class MetaEnforcer(type):
            def __init__(cls, name, bases, cls_dict):
                method_map = dict()

                for m in self.required_methods:
                    if m in cls_dict and callable(cls_dict[m]):
                        method_map[m] = cls_dict[m]
                        continue

                    for b in bases:
                        if isinstance(b, MetaEnforcer):
                            continue

                        if m in b.__dict__ and callable(b.__dict__[m]):
                            method_map[m] = b.__dict__[m]
                            break

                if len(method_map) < len(self.required_methods):
                    raise ValueError(
                        f"{cls.__name__} must have (or inherit) methods "
                        f"[{', '.join(self.required_methods)}]"
                    )

                for m in self.required_methods:
                    setattr(cls, m, self.override(method_map[m], cls))

        MetaEnforcer.__name__ = "Meta" + self.parent_class
        return MetaEnforcer


def find_list_duplicates(input_list):
    return [item for item, count in Counter(input_list).items() if count > 1]


def merge_dicts(*dict_args):
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


def flatten_multindex_columns(df):
    new_cols = list(
        pd.Index(
            [str(e[0]).lower() + "_" + str(e[1]).lower() for e in df.columns.tolist()]
        ).str.replace(" ", "_")
    )
    return new_cols


def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]


def cur_timestamp(clean_string=False):
    ts = datetime.today().replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    if clean_string:
        ts = ts.replace(" ", "__").replace(":", "_")
    return ts


def zip_dir(directory, output_loc, exclude_suffix=".dill"):
    zf = ZipFile("%s" % (output_loc), "w", ZIP_DEFLATED)
    abs_src = os.path.abspath(directory)
    for dirname, subdirs, files in os.walk(directory):
        for filename in files:
            if not filename.endswith(exclude_suffix):
                absname = os.path.abspath(os.path.join(dirname, filename))
                arcname = absname[len(abs_src) + 1 :]
                zf.write(absname, arcname)
    zf.close()
    return


def json_string_to_dict(json_string):
    try:
        string_as_dict = json.loads(json_string)
    except Exception:
        try:
            string_as_dict = json.loads(json_string.replace("'", '"'))
        except Exception:
            try:
                string_as_dict = ast.literal_eval(json_string)
            except Exception:
                raise AssertionError("Could not parse input json_string!")
    if isinstance(string_as_dict, str):
        try:
            string_as_dict = json.loads(string_as_dict)
        except Exception:
            try:
                string_as_dict = json.loads(string_as_dict.replace("'", '"'))
            except Exception:
                try:
                    string_as_dict = ast.literal_eval(string_as_dict)
                except Exception:
                    raise AssertionError("String parsing failed!")

    return string_as_dict


def get_contents_timestamps_from_dir(
    directory,
    get_files_only=False,
    get_directories_only=False,
    excluded_files=None,
    excluded_dirs=None,
):
    assert not (
        get_files_only and get_directories_only
    ), "Both parameters get_files_only and get_directories_only cannot be set to True!"

    if not directory.endswith("/"):
        directory = f"{directory}/"

    excluded_files = (
        (excluded_files,) if isinstance(excluded_files, str) else excluded_files
    )
    if excluded_dirs is not None:
        if isinstance(excluded_dirs, str):
            excluded_dirs = (excluded_dirs,)
        excluded_dirs = [i[0:-1] if i.endswith("/") else i for i in excluded_dirs]

    if not get_directories_only:
        list_of_files = sorted(
            filter(os.path.isfile, glob(directory + "*")), key=os.path.getmtime
        )
        if excluded_files is not None and len(excluded_files):
            list_of_files = [
                i
                for i in list_of_files
                if i not in [f"{directory}{j}" for j in excluded_files]
            ]
    else:
        list_of_files = []

    if not get_files_only:
        list_of_dirs = sorted(
            filter(os.path.isdir, glob(directory + "*")), key=os.path.getmtime
        )
        if excluded_dirs is not None and len(excluded_dirs):
            list_of_dirs = [
                i
                for i in list_of_dirs
                if i not in [f"{directory}{j}" for j in excluded_dirs]
            ]
    else:
        list_of_dirs = []

    list_of_contents = list(set(list_of_files + list_of_dirs))

    df_contents = pd.DataFrame()
    if list_of_contents:
        print(f"*** {df_contents} ***")
        for content_path in list_of_contents:
            modified_timestamp = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.gmtime(os.path.getmtime(content_path))
            )
            df_tmp = pd.DataFrame(
                {"content_path": [content_path], "last_modified": [modified_timestamp]}
            )
            df_contents = pd.concat([df_contents, df_tmp], axis=0)

        df_contents = df_contents.drop_duplicates()
        df_contents["last_modified"] = pd.to_datetime(df_contents["last_modified"])

    return df_contents


def remove_old_contents(
    directory,
    start_timestamp=datetime.now(),
    lookback_days=7,
    remove_files_only=False,
    remove_directories_only=False,
    excluded_files=(".gitignore", ".gitkeep"),
    excluded_dirs=None,
):

    df_contents = get_contents_timestamps_from_dir(
        directory,
        get_files_only=remove_files_only,
        get_directories_only=remove_directories_only,
        excluded_files=excluded_files,
        excluded_dirs=excluded_dirs,
    )

    if df_contents.shape[0]:
        min_keep_timestamp = pd.to_datetime(
            start_timestamp - pd.Timedelta(days=lookback_days)
        )
        contents_to_delete = df_contents[
            df_contents["last_modified"] < min_keep_timestamp
        ]["content_path"].tolist()

        ### delete the files ###
        subprocess.run(f"rm -rf {' '.join(contents_to_delete)}", shell=True)
    return


def clean_strings(lst):
    cleaned_list = [
        re.sub(r"[^a-zA-Z0-9_]", "_", s) for s in lst
    ]  # remove special characters
    cleaned_list = [
        re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower() for s in cleaned_list
    ]  # camel case -> snake case
    cleaned_list = [
        re.sub(r"_+", "_", s).strip("_").lower() for s in cleaned_list
    ]  # clean leading and trailing underscores
    return cleaned_list


def clean_columns(df):
    df.columns = clean_strings(df.columns)
    return df


def send_email(
    to_addrs, from_addr=None, subject="", body="", files=None, password=None
):
    if password is None or from_addr is None:
        email_credentials = os.environ.get("EMAIL_CREDENTIALS")

        email_credentials = json_string_to_dict(email_credentials)

        assert isinstance(email_credentials, dict), "Error parsing email credentials."

        if from_addr is None:
            from_addr = email_credentials["username"]
        if password is None:
            password = email_credentials["password"]

    to_addrs = [to_addrs] if isinstance(to_addrs, str) else to_addrs

    yag = yagmail.SMTP(from_addr, password)

    files = [""] if files is None else files
    files = [files] if isinstance(files, str) else files
    contents = [body] + files
    yag.send(to=to_addrs, subject=subject, contents=contents)
    return
