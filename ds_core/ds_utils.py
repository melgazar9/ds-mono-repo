from ds_core.ds_imports import *

class MetaclassMethodEnforcer:

    """
    Description
    -----------
    Goal: Design a metaclass enforcer that ensures certain method names exist within a class.
    """

    def __init__(self):
        pass


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

def valid_ip_address(ip: str) -> str:
    try:
        return "ipv4" if type(ip_address(ip)) is IPv4Address else "ipv6"
    except ValueError:
        return "invalid"

def flatten_multindex_columns(df):
    new_cols = list(pd.Index([str(e[0]).lower() + '_' + str(e[1]).lower()
                              for e in df.columns.tolist()]).str.replace(' ', '_'))
    return new_cols

def flatten_list(lst):
    # return [v1 for v2 in lst for v1 in v2]
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