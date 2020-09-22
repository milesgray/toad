#
# Some utilities
#
import __main__
import os
import sys
from configparser import ConfigParser

from luigi.task import flatten
from luigi.tools.deps_tree import bcolors
import warnings
from typing import List
import pandas as pd

def optimize_floats(df: pd.DataFrame) -> pd.DataFrame:
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
    return df

def optimize_ints(df: pd.DataFrame) -> pd.DataFrame:
    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
    return df

def optimize_objects(df: pd.DataFrame, datetime_features: List[str]) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object']):
        if col not in datetime_features:
            num_unique_values = len(df[col].unique())
            num_total_values = len(df[col])
            if float(num_unique_values) / num_total_values < 0.5:
                df[col] = df[col].astype('category')
            else:
                df[col] = df[col].astype("string")
        else:
            df[col] = pd.to_datetime(df[col])
    return df

def optimize_pd(df: pd.DataFrame, datetime_features: List[str] = []):
    return optimize_floats(optimize_ints(optimize_objects(df, datetime_features)))

def print_tree(task, indent='', last=True, clip_params=False):
    '''
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    '''
    # dont bother printing out warnings about tasks with no output
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete\\(\\) method')
        is_task_complete = task.complete()
    is_complete = (bcolors.OKGREEN + 'COMPLETE' if is_task_complete else bcolors.OKBLUE + 'PENDING') + bcolors.ENDC
    name = task.__class__.__name__
    params = task.to_str_params(only_significant=True)
    if len(params)>1 and clip_params:
        params = next(iter(params.items()), None)  # keep only one param
        params = str(dict([params]))+'[more]'
    result = '\n' + indent
    if(last):
        result += '└─--'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += '[{0}-{1} ({2})]'.format(name, params, is_complete)
    children = flatten(task.requires())
    for index, child in enumerate(children):
        result += print_tree(child, indent, (index+1) == len(children), clip_params)
    return result

def write_section(section_name):
    """
    Writes section names for configuration files in the format
    [section_name]

    Args:
        section_name: a string representing the section name

    Returns:
        the string [section_name]\n
    """

    return '[{}]\n'.format(section_name)

def write_option(option_name, option_value, last=False):
    """
    Writes option and value pairs for configuration files in the
    format option=value

    Args:
        option_name: a string representing the option name
        option_value: a string representing the option value
        last: a boolean; if True, a new line is added at the end

    Returns:
        the string option_name=option_value if last is False or
        the string option_name=option_value\n if last is True
    """

    out = '{}={}\n'.format(option_name, option_value)

    if last:
        out += '\n'

    return out

def get_script_name():
    """
    Gets the name of the current script being executed

    This function returns the name of the current script being executed
    if the Python session is run in non-interactive mode.

    Returns:
        a string with the name of the script, if the script executed in
        non-interactive mode; otherwise, the function returns None
    """
    
    # check whether Python is in interactive mode
    interactive = not hasattr(__main__, '__file__')

    if interactive:
        return None
    else:
        script_name = os.path.basename(sys.argv[0])
        return script_name

def get_config(section, option):
    """
    Returns the value of an option stored in a section
    of a configuration file

    Args:
        section: a string corresponding to the name of a section of a
            configuration file
        option: a string corresponding to an option contained in a section
            of a configuration file

    Returns:
        a string, corresponding to the value of the target option in the
        target section
    """

    parser = ConfigParser()

    cf = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'local.conf'
    )

    parser.read(cf)

    return parser.get(section, option)

class PrintLogger(object):
    def send_log(self, msg, status):
        print(msg,status)

    def send(self, data):
        print(data)

class AverageMeter(object):
    """Computes and stores the average and current value"""
    def __init__(self):
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count
