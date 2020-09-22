import os, sys
import pathlib
from pathlib import Path
import collections
import logging

import luigi
from luigi.task import flatten
import luigi.tools.deps
from luigi.util import inherits, requires
from luigi.cmdline_parser import CmdlineParser
from luigi.local_target import LocalFileSystem

from toad import targets
from toad.targets import CacheTarget, PdCacheTarget, DataTarget, CSVPandasTarget

from toad import config
from toad.config import GlobalConfig, isdebug
from toad.config import dir, dirpath

import toad.cache
from toad.cache import data as data

import toad.utils
from toad import decorators
from toad.decorators import multiple_inherits, multiple_requires

__all__ = [    
    'targets', 'CacheTarget', 'PdCacheTarget', 'DataTarget', 'CSVPandasTarget',
    'config', 'GlobalConfig', 'isdebug', 'dir', 'dirpath',
    'flatten', 'inherits', 'requires', 'CmdlineParser', 'LocalFileSystem',
    'data', 'decorators', 'multiple_inherits', 'multiple_requires'
]

print('Welcome to TOAD - yet another customized LUIGI framework!') # luigi @ https://github.com/spotify/luigi,  d6tflow @ https://github.com/d6t/d6tflow
logger = logging.getLogger(toad.config.logger_name)

def set_dir(dir=None):
    """
    Initialize toad

    Args:
        dir (str): data output directory

    """
    if dir is None:
        dirpath = toad.config.dirpath
        dirpath.mkdir(exist_ok=True)
    else:
        dirpath = pathlib.Path(dir)
        toad.config.dir = dir
        toad.config.dirpath = dirpath

    toad.config.isinit = True
    return dirpath

def preview(tasks, indent='', last=True, clip_params=False):
    """
    Preview task flows

    Args:
        tasks (obj, list): task or list of tasks
    """
    if not isinstance(tasks, (list,)):
        tasks = [tasks]

    result = ""
    for t in tasks:
        tree = toad.utils.print_tree(t, indent=indent, last=last, clip_params=clip_params)
        result = f"{result}{tree}"
    return result

def run(*args, **kwargs):
    """
    Wrapper around toad.build to allow execution from command line by calling a python script
    with this in it's main execution block.

    Args:
        tasks (obj, list): task or list of tasks
        forced (list): list of forced tasks
        forced_all (bool): force all tasks
        forced_all_upstream (bool): force all tasks including upstream
        confirm (list): confirm invalidating tasks
        workers (int): number of workers
        abort (bool): on errors raise exception
        kwargs: keywords to pass to luigi.build
    """
    forced = kwargs.get('forced', None)
    forced_all = kwargs.get('forced_all', False)
    forced_all_upstream = kwargs.get('forced_all_upstream', False)
    confirm = kwargs.get('confirm', True)
    workers = kwargs.get('workers', 1)
    abort = kwargs.get('abort', True)

    cmdline_args = sys.argv[1:]

    with CmdlineParser.global_instance(cmdline_args) as cp:
        if toad.config.show_params_on_run:
            print("\n\nCommand line args:\n{}".format(cp.known_args))
        root_task = cp.get_task_obj()
        if toad.config.isdebug:
            print("\nRoot task:\n{}".format(root_task))
        if "forced" in cp.known_args:
            forced = cp.known_args.forced 
        if "forced_all" in cp.known_args:
            if toad.config.isdebug:
                print("\n\ncp.known_args.forced_all: {}".format(cp.known_args.forced_all))
            forced_all = cp.known_args.forced_all 
        if "forced_all_upstream" in cp.known_args:
            forced_all_upstream = cp.known_args.forced_all_upstream 
        if "confirm" in cp.known_args:
            confirm = cp.known_args.confirm
        if "workers" in cp.known_args:
            workers = cp.known_args.workers 
        if "abort" in cp.known_args:
            abort = cp.known_args.abort 
        return build(root_task, 
            forced=forced, forced_all=forced_all, 
            forced_all_upstream=forced_all_upstream, 
            confirm=confirm, workers=workers, abort=abort, **kwargs)    

def build(tasks, forced=None, forced_all=False, forced_all_upstream=False, confirm=True, workers=1, abort=True, **kwargs):
    """
    Run tasks on central scheduler. See luigi.build for additional details

    Args:
        tasks (obj, list): task or list of tasks
        forced (list): list of forced tasks
        forced_all (bool): force all tasks
        forced_all_upstream (bool): force all tasks including upstream
        confirm (list): confirm invalidating tasks
        workers (int): number of workers
        abort (bool): on errors raise exception
        kwargs: keywords to pass to luigi.build

    """
    if not isinstance(tasks, (list,)):
        tasks = [tasks]

    if forced_all:
        forced = tasks
    if forced_all_upstream:
        for t in tasks:
            invalidate_upstream(t,confirm=confirm)
    if forced is not None:
        if not isinstance(forced, (list,)):
            forced = [forced]
        invalidate = []
        for tf in forced:
            for tup in tasks:
                invalidate.append(taskflow_downstream(tf,tup))
        invalidate = set().union(*invalidate)
        invalidate = {t for t in invalidate if t.complete()}
        if len(invalidate)>0:
            if confirm:
                print('Forced tasks', invalidate)
                c = input('Confirm invalidating forced tasks (y/n)')
            else:
                c = 'y'
            if c == 'y':
                [t.invalidate(confirm=False) for t in invalidate]
            else:
                return None

    opts = {**{'workers':workers, 'log_level':toad.config.log_level},**kwargs}
    result = luigi.build(tasks, detailed_summary=True, **opts)
    if abort and not result:
        raise RuntimeError('Exception found running flow, check trace')

    return result

def taskflow_upstream(task, only_complete=False):
    """
    Get all upstream inputs for a task

    Args:
        task (obj): task

    """

    def traverse(t, path=None):
        if path is None: path = []
        path = path + [t]
        for node in flatten(t.requires()):
            if not node in path:
                path = traverse(node, path)
        return path

    tasks = traverse(task)
    if only_complete:
        tasks = [t for t in tasks if t.complete()]
    return tasks

def taskflow_downstream(task, task_downstream, only_complete=False):
    """
    Get all downstream outputs for a task

    Args:
        task (obj): task
        task_downstream (obj): downstream target task

    """
    tasks = luigi.tools.deps.find_deps(task_downstream, task.task_family)
    if only_complete:
        tasks = {t for t in tasks if t.complete()}
    return tasks

def invalidate_all(confirm=True):
    """
    Invalidate all tasks by deleting all files in data directory

    Args:
        confirm (bool): confirm operation

    """
    # record all tasks that run and their output vs files present
    raise NotImplementedError()

def invalidate_orphans(confirm=True):
    """
    Invalidate all unused task outputs

    Args:
        confirm (bool): confirm operation

    """
    # record all tasks that run and their output vs files present
    raise NotImplementedError()

def show(task):
    """
    Show task execution status

    Args:
        tasks (obj, list): task or list of tasks
    """
    preview(task)

def invalidate_upstream(task, confirm=False):
    """
    Invalidate all tasks upstream tasks in a flow.

    For example, you have 3 dependant tasks. Normally you run Task3 but you've changed parameters for Task1. By invalidating Task3 it will check the full DAG and realize Task1 needs to be invalidated and therefore Task2 and Task3 also.

    Args:
        task (obj): task to invalidate. This should be an upstream task for which you want to check upstream dependencies for invalidation conditions
        confirm (bool): confirm operation

    """
    tasks = taskflow_upstream(task, only_complete=True)
    if len(tasks)==0:
        print('no tasks to invalidate')
        return True
    if confirm:
        print('Compeleted tasks to invalidate:')
        print(tasks)
        c = input('Confirm invalidating tasks (y/n)')
    else:
        c = 'y'
    if c=='y':
        [t.invalidate(confirm=False) for t in tasks]

def invalidate_downstream(task, task_downstream, confirm=False):
    """
    Invalidate all downstream tasks in a flow.

    For example, you have 3 dependant tasks. Normally you run Task3 but you've changed parameters for Task1. By invalidating Task3 it will check the full DAG and realize Task1 needs to be invalidated and therefore Task2 and Task3 also.

    Args:
        task (obj): task to invalidate. This should be an downstream task for which you want to check downstream dependencies for invalidation conditions
        task_downstream (obj): downstream task target
        confirm (bool): confirm operation

    """
    tasks = taskflow_downstream(task, task_downstream, only_complete=True)
    if len(tasks)==0:
        print('no tasks to invalidate')
        return True
    if confirm:
        print('Compeleted tasks to invalidate:')
        print(tasks)
        c = input('Confirm invalidating tasks (y/n)')
    else:
        c = 'y'
    if c=='y':
        [t.invalidate(confirm=False) for t in tasks]
        return True
    else:
        return False

def clone_parent(cls):
    def requires(self):
        return self.clone_parent()

    setattr(cls, 'requires', requires)
    return cls

def get_files_in_folder(root_dir, extension, fs=None):
    '''
    Returns all files with a specific extension within a directory.

    Return:
        List of absolute paths as strings to each file in specified directory
    '''
    if fs is None:
        fs = PathFileSystem()
    paths = [str(p) for p in fs.listdir(root_dir, ext=extension)]

    return paths

def get_paths_in_folder(root_dir, extension, fs=None):
    '''
    Returns all files with a specific extension within a directory.

    Return:
        List of each file in specified directory as pathlib.Path objects
    '''
    if fs is None:
        fs = PathFileSystem()
    paths = [p for p in fs.getdirs(root_dir, ext=extension)]

    return paths
    
class PathFileSystem(LocalFileSystem):
    def getdirs(self, path, glob_pattern="*", skip_pattern="", recursive=True):
        '''
        Returns a list of all files within a directory with an optional filter on extension by passing ``ext``. Set ``recursive to ``False``
        to only search the root folder.

        Args:
            path (string or Path):
                target path to count files in
            glob_pattern (string, optional):
                Pattern passed into the glob method, uses * syntax 
            ext (string, optional):
                Extension to limit counted files to
            skip_pattern (string, optional):
                Remove any filenames containing this value from results
            recursive (bool):
                True to traverse entire directory, False to only look at root
        Returns:
            List of files
        '''
        if isinstance(path, str):
            path = Path(path)
        if not isinstance(path, Path):
            path = Path(str(path))
        if not isinstance(path, Path):
            raise ValueError(
                "Not a valid path: {} can not convert to pathlib.Path".format(path))

        if recursive:
            if skip_pattern == "":
                return [p for p in path.rglob(f"{glob_pattern}") if p.is_dir()]
            else:
                return [p for p in path.rglob(f"{glob_pattern}") if p.is_dir() and not p.match(skip_pattern)]
        else:
            if skip_pattern == "":
                return [p for p in path.glob(f"{glob_pattern}") if p.is_dir()]
            else:
                return [p for p in path.glob(f"{glob_pattern}") if p.is_dir() and not p.match(skip_pattern)]

    def listdir(self, path, ext="", glob_pattern="*", skip_pattern="", recursive=True):
        '''
        Returns a list of all files within a directory with an optional filter on extension by passing ``ext``. Set ``recursive to ``False``
        to only search the root folder.

        Args:
            path (string or Path):
                target path to count files in
            glob_pattern (string, optional):
                Pattern passed into the glob method, uses * syntax 
            ext (string, optional):
                Extension to limit counted files to
            skip_pattern (string, optional):
                Remove any filenames containing this value from results
            recursive (bool):
                True to traverse entire directory, False to only look at root
        Returns:
            List of files
        '''
        if isinstance(path, str):
            path = Path(path)
        if not isinstance(path, Path):
            path = Path(str(path))
        if not isinstance(path, Path):
            raise ValueError("Not a valid path: {} can not convert to pathlib.Path".format(path))

        if recursive:
            if skip_pattern == "":
                return [p for p in path.rglob(f"{glob_pattern}{ext}")]
            else:
                return [p for p in path.rglob(f"{glob_pattern}{ext}") if skip_pattern not in p.name]
        else:
            if skip_pattern == "":
                return [p for p in path.glob(f"{glob_pattern}{ext}")]
            else:
                return [p for p in path.glob(f"{glob_pattern}{ext}") if skip_pattern not in p.name]

    def counterdir(self, path, ext="", glob_pattern="*", skip_pattern="", recursive=True):
        '''
        Returns a :py:class:`collections.Counter` object wih counts of each filetype in ``path``. Use ``ext`` to limit
        results to only files with extensions equal to ``ext``. ``recursive`` to ``True`` means traverse
        entire directory structure.

        Args:
            path (string or Path):
                target path to count files in
            glob_pattern (string, optional):
                Pattern passed into the glob method, uses * syntax 
            ext (string, optional):
                Extension to limit counted files to, must include . - '.jpg' not 'jpg'
            recursive (bool):
                True to traverse entire directory, False to only look at root
        Returns:
            Counter object with the counts for each type of file
        '''
        if isinstance(path, str):
            path = Path(path)
        if not isinstance(path, Path):
            path = Path(str(path))
        if not isinstance(path, Path):
            raise ValueError("Not a valid path: {} can not convert to pathlib.Path".format(path))

        if recursive:
            if skip_pattern == "":
                counter = collections.Counter(
                    p.suffix for p in path.rglob(f"{glob_pattern}{ext}"))
            else:
                counter = collections.Counter(p.suffix for p in path.rglob(
                    f"{glob_pattern}{ext}") if skip_pattern not in p.name)
        else:
            if skip_pattern == "":
                counter = collections.Counter(
                    p.suffix for p in path.glob(f"{glob_pattern}{ext}"))
            else:
                counter = collections.Counter(p.suffix for p in path.glob(
                    f"{glob_pattern}{ext}") if skip_pattern not in p.name)

        return counter

    def countdir(self, path, ext="", skip_pattern="", recursive=True):
        '''
        Returns the count of all files that match the ``ext`` in the ``path``, either just the root with ``recursive`` set
        to ``False`` or the entire directory structure with it set to ``True``

        Args:
            path (string or Path):
                target path to count files in
            ext (string, optional):
                Extension to limit counted files to, can be a subset of an extension such as 'p' to match 'py' and 'pdf'
            recursive (bool):
                True to traverse entire directory, False to only look at root
        Returns:
            Sum total of all files that matched
        '''
        counter = self.counterdir(path, ext=ext, skip_pattern=skip_pattern, recursive=recursive)

        return sum(counter.values())
