from ds_core.ds_imports import *

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
    def overwrite_new(f, cls):
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
                    raise ValueError(f"{cls.__name__} must have (or inherit) methods "
                                     f"[{', '.join(self.required_methods)}]")

                for m in self.required_methods:
                    setattr(cls, m, self.overwrite_new(method_map[m], cls))

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
    new_cols = list(pd.Index([str(e[0]).lower() + '_' + str(e[1]).lower()
                              for e in df.columns.tolist()]).str.replace(' ', '_'))
    return new_cols

def flatten_list(lst):
    return [v for item in lst for v in (item if isinstance(item, list) else [item])]

def cur_timestamp(clean_string=True):
    ts = datetime.today().replace(second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    if clean_string:
        ts = ts.replace(' ', '__').replace(':', '_')
    return ts

def zip_dir(directory, output_loc, exclude_suffix='.dill'):
    zf = ZipFile("%s" % (output_loc), "w", ZIP_DEFLATED)
    abs_src = os.path.abspath(directory)
    for dirname, subdirs, files in os.walk(directory):
        for filename in files:
            if not filename.endswith(exclude_suffix):
                absname = os.path.abspath(os.path.join(dirname, filename))
                arcname = absname[len(abs_src) + 1:]
                zf.write(absname, arcname)
    zf.close()
    return

def send_email(to_addrs,
               from_addr=None,
               subject='',
               body='',
               files=None,
               password=None):

    if password is None or from_addr is None:
        email_credentials = os.environ.get('EMAIL_CREDENTIALS')

        try:
            email_credentials = json.loads(email_credentials)
        except json.decoder.JSONDecodeError:
            email_credentials = json.loads(email_credentials.replace("'", "\""))

        assert isinstance(email_credentials, dict), "Error parsing email credentials."

        if from_addr is None:
            from_addr = email_credentials['username']
        if password is None:
            password = email_credentials['password']

    to_addrs = [to_addrs] if isinstance(to_addrs, str) else to_addrs

    yag = yagmail.SMTP(from_addr, password)

    files = [''] if files is None else files
    files = [files] if isinstance(files, str) else files
    contents = [body] + files
    yag.send(to=to_addrs, subject=subject, contents=contents)
    return

def json_string_to_dict(json_string):
    try:
        string_as_dict = json.loads(json_string)
    except:
        try:
            string_as_dict = json.loads(json_string.replace("'", '\"'))
        except:
            try:
                string_as_dict = ast.literal_eval(json_string)
            except:
                raise AssertionError('Could not parse input json_string!')
    if isinstance(string_as_dict, str):
        try:
            string_as_dict = json.loads(string_as_dict)
        except:
            try:
                string_as_dict = json.loads(string_as_dict.replace("'", '\"'))
            except:
                try:
                    string_as_dict = ast.literal_eval(string_as_dict)
                except:
                    raise AssertionError('String parsing failed!')

    return string_as_dict