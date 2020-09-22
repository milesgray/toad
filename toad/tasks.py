import os
import time
import enum
import pathlib
import traceback
from pathlib import Path

import luigi
import numpy as np

import toad
import toad.targets
import toad.config

cfg = toad.config.GlobalConfig()

class DataTask(luigi.Task):
    """
    Task which has data as input and output
    Args:
        target_class (obj): target data format
        target_ext (str): file extension
        persist (list): list of string to identify data
        data (dict): data container for all outputs
    """
    class OutputLocation(enum.Enum):
        """
        Used to populate choices for default output folder
        """
        OTHER = 1
        ARTIFACT = 2
        DATA = 3
        OUTPUT = 4

    target_class = toad.targets.DataTarget
    target_ext = 'ext'
    persist = ['data']

    output_location = luigi.EnumParameter(default=OutputLocation.OUTPUT, enum=OutputLocation, description="Folder root for outputs of this task to be saved to, also where to look for completed work. Choices refer to paths set in luigi config. Select 'other' for any other location and set ")
    output_path = luigi.Parameter(default="config", significant=False, description="For custom output path when 'other' output location selected. Otherwise leave as default.")

    def __init__(self, *args, **kwargs):
        self._start_time = time.time()
        super(DataTask, self).__init__(*args, **kwargs)

        self.config = cfg
        self.isdebug = "t" if toad.config.isdebug or cfg.is_debug else "f"        
        self.output_folder = ""
        self.classname = type(self).__name__
        self.fs = toad.PathFileSystem()

    @property
    def arifact_path(self):
        return Path(self.config.artifact_path)

    @property
    def data_path(self):
        return Path(self.config.data_path)

    @property
    def timestamp(self):
        timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
        return timestamp

    @property
    def filename(self):
        '''
        If build_filename is defined, this is the 'stem' of the output file, It is called when the Task object is
        setting up and creates the output Target objects - this filename string is used if it does
        not throw an exception. No support for multiple output files yet.

        Returns:
            A string representation of the stem of the output target file.
        '''
        return self.build_filename()

    def build_filename(self):
        '''
        This returns the 'stem' of the output file to the filename property, It is called when the Task object is
        setting up and creates the output Target objects - this filename string is used if it does
        not throw an exception. No support for multiple output files yet.

        Returns:
            A string representation of the stem of the output target file.
        '''
        raise NotImplementedError()

    def get_files(self, path, extension):        
        matches = [str(p) for p in self.fs.listdir(path, ext=extension)]

        return np.array(matches)

    @property
    def output_save_path(self):
        '''
        The Path used when saving this task's output targets. Override if more complx logic needed.
        Change self.output_folder to change location a single time.

        Returns:
            A pathlib.Path object representing the path to the Target output file
        '''
        if self.output_folder == "":
            self.output_folder = self.config.data_path

            if self.output_location == DataTask.OutputLocation.OUTPUT:
                self.output_folder = self.config.output_path
            elif self.output_location == DataTask.OutputLocation.DATA:
                self.output_folder = self.config.data_path
            elif self.output_location == DataTask.OutputLocation.ARTIFACT:
                self.output_folder = self.config.artifact_path
            elif self.output_location == DataTask.OutputLocation.OTHER:
                if str(self.output_path) != "":
                    self.output_folder = self.output_path                            

        return Path(self.output_folder)

    @property
    def output_save_folder(self):
        return str(self.output_save_path.absolute())

    def debg(self, message):
        '''
        Helper method that sends a debug log only if `isdebug` member is True

        Args:
            message (string):
                The message to print through toad.logger.debug
        '''
        if self.isdebug.lower() in "true":
            toad.logger.debug(message)

    def _clean_forfilename(self, value, maxlen=10, step=2, reverse=False):
        '''
        Helper method that cleans values to be suitable for being part of a filename. Replaces slashes, dashes and periods.
        Also enforces a max length by dividing the length by `step` in a loop until the length is no longer greater than `maxlen`.

        Args:
            value (string):
                a value that is intended to be part of a filename
            maxlen (int number):
                the maximum number of characters the result can be
            step (int number):
                how much of the length is divided by each iteration when shortening

        Returns:
            A string with no special characters that will change path behavior and is not overly long.
        '''
        value = value.replace("/", "").replace("-", "_").replace(".", "_")
        length = len(value)
        while length > maxlen:
            if reverse:
                value = value[-(length/step):]
            else:
                value = value[:(length/step)]
        return value

    def invalidate(self, confirm=True):
        """
        Invalidate a task, eg by deleting output file
        """
        if confirm:
            c = input(
                'Confirm invalidating task: {} (y/n)'.format(self.__class__.__qualname__))
        else:
            c = 'y'
        if c == 'y' and self.complete():
            if self.persist == ['data']:  # 1 data shortcut
                self.output().invalidate()
                if isinstance(self.output(), toad.targets._LocalPathTarget):
                    self.debg("invalidated {} at {}".format(
                        type(self.output()), self.output().path))
                elif isinstance(self.output(), list):
                    for out in self.output():
                        self.debg("invalidated {} at {}".format(
                            type(out), out.path))
            else:
                [t.invalidate() for t in self.output().values()]
        return True

    def complete(self, cascade=True):
        complete = super().complete()
        if toad.config.check_dependencies and cascade and not getattr(self, 'external', False):
            complete = complete and all(
                [t.complete() for t in luigi.task.flatten(self.requires())])
        return complete

    def _getpath(self, dirpath, k, subdir=True):
        tidroot = getattr(self, 'target_dir', self.task_id.split('_')[0])

        try:
            fname = self.filename
        except:
            fname = '{}-{}'.format(self.task_id, k) \
                if (settings.save_with_param and getattr(self, 'save_attrib', True)) \
                    else '{}'.format(k)

        if isinstance(fname, luigi.LocalTarget):
            fname = str(fname.path)
        if isinstance(fname, pathlib.Path):
            fname = str(fname.name)
        if isinstance(fname, str):
            fname += '.{}'.format(self.target_ext)

        if subdir:
            path = dirpath / tidroot / fname
        else:
            path = dirpath / fname
        return path

    def get_files_in_folder(self, root_dir, extension):
        paths = [str(p) for p in self.fs.listdir(root_dir, ext=extension)]

        return paths

    def input_line_count(self, path=None):
        def blocks(files, size=65536):
            while True:
                b = files.read(size)
                if not b:
                    break
                yield b
        path = self.input().path if path is None else path
        with open(path, "r", encoding="utf-8", errors='ignore') as f:
            return sum(bl.count("\n") for bl in blocks(f))

    def output(self):
        """
        Similar to luigi task output
        """
        try:
            # output_save path uses output_folder parameter unless it is left as the default 'config', which then uses the output_path in config.py
            dirpath = self.output_save_path
        except:
            dirpath = toad.config.dirpath

        save_ = getattr(self, 'persist', [])
        output = dict([(k, self.target_class(self._getpath(dirpath, k))) for k in save_])
        if self.persist == ['data']:  # 1 data shortcut
            output = output['data']

        self.debg(f"{type(self).__name__} output set to {output.path.absolute()}")

        return output

    def inputLoad(self, keys=None, cached=False):
        """
        Load all or several inputs from task
        Args:
            keys (list): list of data to load
            as_dict (bool): cache data in memory
            cached (bool): cache data in memory
        Returns: list or dict of all task output
        """
        input = self.input()
        if isinstance(input, tuple):
            data = [o.load() for o in input]
        elif isinstance(input, dict):
            keys = input.keys() if keys is None else keys
            data = {k: v.load(cached) for k, v in self.input().items() if k in keys}
            data = list(data.values())
        else:
            data = input.load()
        return data

    def outputLoad(self, keys=None, as_dict=False, cached=False):
        """
        Load all or several outputs from task
        Args:
            keys (list): list of data to load
            as_dict (bool): cache data in memory
            cached (bool): cache data in memory
        Returns: list or dict of all task output
        """
        if not self.complete():
            raise RuntimeError('Cannot load, task not complete, run task first')
        keys = self.persist if keys is None else keys
        if self.persist == ['data']:  # 1 data shortcut
            return self.output().load()

        data = {k: v.load(cached) for k, v in self.output().items() if k in keys}
        if not as_dict:
            data = list(data.values())
        return data

    def save(self, data, **kwargs):
        """
        Persist data to target
        Args:
            data (dict): data to save. 
        keys are the self.persist keys and values is data
        """
        if self.persist == ['data']:  # 1 data shortcut
            self.output().save(data, **kwargs)
        else:
            targets = self.output()
            if not set(data.keys()) == set(targets.keys()):
                raise ValueError('Save dictionary needs to consistent with Task.persist')
            for k, v in data.items():
                targets[k].save(v, **kwargs)

class EventTask(DataTask):
    save_attrib = True

    is_clone = luigi.Parameter(default=False)
    invalidate_all = luigi.BoolParameter(
        default=False, description="Add this with no value invalidate upstream tasks")
    invalidate = luigi.BoolParameter(
        default=False, description="Add this with no value invalidate this task")

    def __init__(self, *args, **kwargs):
        super(EventTask, self).__init__(*args, **kwargs)

        self.set_status_message = self._set_status_message
        self.set_progress_percentage = self._set_progress_percentage
        self.set_tracking_url = None    # not sure what this will do

        self.is_clone = kwargs.get('is_clone', "False")
        self.enable_st = kwargs.get('enable_st', False)
        self.last_progress_update = None

        if self.is_clone == "False" and isinstance(self.output(), toad.targets._LocalPathTarget):
            if self.invalidate:
                super(EventTask, self).invalidate(confirm=False)
            if self.invalidate_all:
                super(EventTask, self).invalidate(confirm=False)

                input_target = self.input()
                # handle different types of inputs - only toad based targets have invalidate, this supports luigi and lists
                if isinstance(input_target, tuple):
                    for o in input_target:
                        if not isinstance(o, toad.targets._LocalPathTarget):
                            o = toad.targets._LocalPathTarget(path=o.path)
                        o.invalidate()
                elif isinstance(input_target, dict):
                    for _, v in input_target.items():
                        if v is not toad.targets._LocalPathTarget:
                            v = toad.targets._LocalPathTarget(path=v.path)
                        v.invalidate()
                elif isinstance(input_target, list):
                    for i in input_target:
                        if not isinstance(i, toad.targets._LocalPathTarget):
                            i = toad.targets._LocalPathTarget(path=i.path)
                        i.invalidate()
                elif not isinstance(input_target, toad.targets._LocalPathTarget):
                    toad.targets._LocalPathTarget(
                        path=input_target.path).invalidate()
                else:
                    input_target.invalidate()

    def clone(self, cls=None, **kwargs):
        """
        Creates a new instance from an existing instance where some of the args have changed.
        There's at least two scenarios where this is useful (see test/clone_test.py):
        * remove a lot of boiler plate when you have recursive dependencies and lots of args
        * there's task inheritance and some logic is on the base class
        :param cls:
        :param kwargs:
        :return:
        """
        if cls is None:
            cls = self.__class__

        new_k = {}
        # param name, param class
        for param_name, _ in cls.get_params():
            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name):
                new_k[param_name] = getattr(self, param_name)

        new_k["is_clone"] = "True"

        return cls(**new_k)

    def build_filename(self):
        '''
        Default filename set to task_id, which is unique based on the first few parameters
        '''
        filename = "{}".format(self.task_id)

        return filename

    """
        These are just shorter versions of the set_x_message methods.
        Use them instead of making your own logger because they also
        raise luigi events that are sent to the scheduler and anything 
        else listening (fleep)
    """

    def error(self, message, raise_exception=True):
        self.set_error_message(str(message), raise_exception=raise_exception)

    def final(self, message):
        self.set_final_message(str(message))

    def start(self, message):
        self.set_start_message(str(message))

    def info(self, message):
        toad.logger.info(str(message))
        #self.set_status_message(str(message))

    def progress(self, description, i, total, send_rate=1000):
        self.send_progress(description, i, total, send_rate=send_rate)

    def debug(self, message):
        toad.logger.debug(str(message))
        #self.set_status_message(str(message))

    def warning(self, message):
        toad.logger.warning(message)
        
    """
        These all wrap a log message of the appropriate type, raise an event,
        and otherwise integrate into the scheduler.
    """

    def set_error_message(self, message, raise_exception=True):
        toad.logger.exception(message)
        exception = Exception(message) 
        self.trigger_event(luigi.event.Event.PROCESS_FAILURE, exception)
        if raise_exception:
            traceback_string = traceback.format_exc()
            message = "{}\n{}".format(message, traceback_string)
            raise exception

    def set_final_message(self, message):
        self.progress_message = message
        #toad.logger.info(message)
        self.trigger_event(luigi.event.Event.PROGRESS, message=str(message))

    def set_start_message(self, message):
        self._set_status_message(message)
        #toad.logger.info(message)
        self.trigger_event(luigi.event.Event.PROGRESS, message=str(message))

    def _set_status_message(self, message):
        self.progress_message = str(message)

    def _set_progress_percentage(self, i):
        self.progress_percentage = i
        self.trigger_event(luigi.event.Event.PROGRESS,
                           message=self.progress_message, percentage=self.progress_percentage)

    def send_progress(self, description, i, total, send_rate=1000, do_log=True):
        if total == 0: total = 1

        if i % send_rate == 0:
            percentage = float(float(i) / float(total) * 100.)
            message = "{}: {:2.2f}%, {} / {}".format(
                description, percentage, i, total)

            if self.last_progress_update:
                dt = time.time() - self.last_progress_update
                dps = float(float(send_rate) / dt)
                left = float(float(total - i) / dps)
                message = "{} @ {:2.2f}/s {:2.2f} secs remain".format(
                    message, dps, left)            

            toad.logger.info(str(message))
            # triggers luigi.event.Event.PROGRESS
            self.set_status_message(message)
            self.set_progress_percentage(percentage)
            self.last_progress_update = time.time()


    @property
    def accepts_messages(self):
        """
        For configuring which scheduler messages can be received. When falsy, this tasks does not
        accept any message. When True, all messages are accepted.
        """
        return True


@EventTask.event_handler(luigi.Event.START)
def start(task):
    pass


@EventTask.event_handler(luigi.Event.SUCCESS)
def success(task):
    pass


@EventTask.event_handler(luigi.Event.PROGRESS)
def progress(*args, **kwargs):
    pass


@EventTask.event_handler(luigi.Event.FAILURE)
def handle_fail(task, exception):
    with open('{}/task_errrors.log'.format(cfg.log_path), 'a') as f:
        time_string = time.strftime("%a, %d %b %Y %X +0000", time.gmtime())
        f.write("\n\n[{}]\t\tProblem detected for {}\n\t\t{}".format(
            time_string, task, exception))


@EventTask.event_handler(luigi.Event.BROKEN_TASK)
def broken_dep(task, exception):
    pass


@EventTask.event_handler(luigi.Event.PROCESSING_TIME)
def processing_time(task, processing_time):
    pass


class CacheTask(EventTask):
    """
    Task which saves to cache
    """
    target_class = toad.targets.CacheTarget
    target_ext = 'cache'


class CachePandasTask(EventTask):
    """
    Task which saves to cache pandas dataframes
    """
    target_class = toad.targets.PdCacheTarget
    target_ext = 'cache'


class JsonTask(EventTask):
    """
    Task which saves to json
    """
    target_class = toad.targets.JsonTarget
    target_ext = 'json'


class PickleTask(EventTask):
    """
    Task which saves to pickle
    """
    target_class = toad.targets.PickleTarget
    target_ext = 'pkl'


class CSVPandasTask(EventTask):
    """
    Task which saves to CSV
    """
    target_class = toad.targets.CSVPandasTarget
    target_ext = 'csv'


class CSVGZPandasTask(EventTask):
    """
    Task which saves to GZipped CSV
    """
    target_class = toad.targets.CSVGZPandasTarget
    target_ext = 'csv.gz'


class H5PandasTask(EventTask):
    """
    Task which saves to HDF5
    """
    target_class = toad.targets.H5PandasTarget
    target_ext = 'hdf5'

if toad.targets.TENSORFLOW_AVAILABLE:
    class H5KerasTask(EventTask):
        """
        Task which saves to HDF5
        """
        target_class = toad.targets.H5KerasTarget
        target_ext = 'hdf5'

class TaskAggregator(luigi.Task):
    """
    Task which yields other tasks
    NB: Use this function by implementing `run()` which should do nothing but yield other tasks
    example::
        class TaskCollector(d6tflow.tasks.TaskAggregator):
            def run(self):
                yield Task1()
                yield Task2()
    """

    def invalidate(self, confirm=True):
        [t.invalidate(confirm) for t in self.run()]

    def complete(self, cascade=True):
        return all([t.complete(cascade) for t in self.run()])

    def output(self):
        return [t.output() for t in self.run()]

    def outputLoad(self, keys=None, as_dict=False, cached=False):
        return [t.outputLoad(keys,as_dict,cached) for t in self.run()]

class TaskMatplotlib(TaskData):
    """
    Task which saves plots to png, does not load
    """
    target_class = d6tflow.targets.MatplotlibTarget
    target_ext = 'png'