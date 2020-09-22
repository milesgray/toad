import json
import pickle
import pathlib

import luigi
import pandas as pd
try:
    from tensorflow.keras.models import load_model
    TENSORFLOW_AVAILABLE = True
except:
    TENSORFLOW_AVAILABLE = False

from toad.cache import data as cache
import toad.config

class CacheTarget(luigi.LocalTarget):
    """
    Saves to in-memory cache, loads to python object
    """
    def exists(self):
        return self.path in cache

    def invalidate(self):
        if self.path in cache:
            cache.pop(self.path)

    def load(self, cached=True):
        """
        Load from in-memory cache
        Returns: python object
        """
        if self.exists():
            return cache.get(self.path)
        else:
            raise RuntimeError('Target does not exist, make sure task is complete')

    def save(self, df):
        """
        Save dataframe to in-memory cache
        Args:
            df (obj): pandas dataframe
        Returns: filename
        """
        cache[self.path] = df
        return self.path

class PdCacheTarget(CacheTarget):
    pass

class _LocalPathTarget(luigi.LocalTarget):
    """
    Local target with `self.path` as `pathlib.Path()`
    """

    def __init__(self, path=None, logger=None):
        super().__init__(path)        
        self.path = pathlib.Path(path)
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        if logger: logger.debug("{} instantiated with {} as path".format(type(self).__name__, self.path))

    def exists(self):
        return self.path.exists()

    def invalidate(self):
        if self.exists():
            self.path.unlink()
        return not self.exists()

class DataTarget(_LocalPathTarget):
    """
    Local target which saves in-memory data (eg dataframes) to persistent storage (eg files) and loads from storage to memory

    This is an abstract class that you should extend.

    """
    def load(self, fun, cached=False, **kwargs):
        """
        Runs a function to load data from storage into memory

        Args:
            fun (function): loading function
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to `fun`

        Returns: data object

        """
        if self.exists():
            if not cached or not toad.config.cached or self.path not in cache:
                opts = {**{},**kwargs}
                df = fun(self.path, **opts)
                if cached or toad.config.cached:
                    cache[self.path] = df
                return df
            else:
                return cache.get(self.path)
        else:
            raise RuntimeError('Target does not exist, make sure task is complete')


    def save(self, df, fun, **kwargs):
        """
        Runs a function to save data from memory into storage

        Args:
            df (obj): data to save
            fun (function): saving function
            **kwargs: arguments to pass to `fun`

        Returns: filename

        """
        fun = getattr(df, fun)
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        fun(self.path, **kwargs)
        return self.path

class CSVPandasTarget(DataTarget):
    """
    Saves to CSV, loads to pandas dataframe

    """
    def load(self, cached=False, **kwargs):
        """
        Load from csv to pandas dataframe

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pd.read_csv

        Returns: pandas dataframe

        """
        def funload(x):
            with open(x,"rb" ) as fhandle:
                data = pickle.load(fhandle)
            return data
        return super().load(funload, cached, **kwargs)


    def save(self, df, **kwargs):
        """
        Save dataframe to csv

        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_csv

        Returns: filename

        """
        opts = {**{'index':False},**kwargs}
        return super().save(df, 'to_csv', **opts)

class CSVGZPandasTarget(CSVPandasTarget):
    """
    Saves to CSV gzip, loads to pandas dataframe
    """
    def save(self, df, **kwargs):
        """
        Save dataframe to csv gzip
        Args:
            df (obj): pandas dataframe
            kwargs : additional arguments to pass to df.to_csv
        Returns: filename
        """
        opts = {**{'index':False, 'compression':'gzip'},**kwargs}
        return super().save(df, 'to_csv', **opts)

class JsonTarget(DataTarget):
    """
    Saves to json, loads to dict

    """
    def load(self, cached=False, **kwargs):
        """
        Load from json to dict

        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to json.load

        Returns: dict

        """
        def read_json(path, **opts):
            with open(path, 'r') as fhandle:
                df = json.load(fhandle)
            return df['data']
        return super().load(read_json, cached, **kwargs)


    def save(self, dict_, **kwargs):
        """
        Save dict to json

        Args:
            dict_ (dict): python dict
            kwargs : additional arguments to pass to json.dump

        Returns: filename

        """
        def write_json(path, _dict_, **opts):
            with open(path, 'w') as fhandle:
                json.dump(_dict_, fhandle, **opts)
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        opts = {**{'indent':4},**kwargs}
        write_json(self.path, {'data':dict_}, **opts)
        return self.path

class PickleTarget(DataTarget):
    """
    Saves to pickle, loads to python obj
    """

    def load(self, cached=False, **kwargs):
        """
        Load from pickle to obj
        Args:
            cached (bool): keep data cached in memory
            **kwargs: arguments to pass to pickle.load
        Returns: dict
        """
        def funload(x):
            with open(x, "rb") as fhandle:
                data = pickle.load(fhandle)
            return data
        return super().load(funload, cached, **kwargs)

    def save(self, obj, **kwargs):
        """
        Save obj to pickle
        Args:
            obj (obj): python object
            kwargs : additional arguments to pass to pickle.dump
        Returns: filename
        """
        (self.path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "wb") as fhandle:
            pickle.dump(obj, fhandle, **kwargs)
        return self.path

class MatplotlibTarget(_LocalPathTarget):
    """
    Saves to png. Does not load
    """

    def load(self):
        raise RuntimeError('Images can only be saved not loaded')

    def save(self, obj, **kwargs):
        """
        Save obj to pickle and png
        Args:
            obj (obj): python object
            plotkwargs (dict): additional arguments to plt.savefig()
            kwargs : additional arguments to pass to pickle.dump
        Returns: filename
        """
        fig = obj.get_figure()
        fig.savefig(self.path, **kwargs)
        return self.path

class PlotlyTarget(_LocalPathTarget):
    """
    Saves to png. Does not load
    """

    def load(self):
        raise RuntimeError('Images can only be saved not loaded')

    def save(self, figs, **kwargs):
        """
        Save obj to pickle and png
        Args:
            obj (obj): python object
            plotkwargs (dict): additional arguments to plt.savefig()
            kwargs : additional arguments to pass to pickle.dump
        Returns: filename
        """
        dashboard = open(self.path, 'w')
        dashboard.write("<html><head></head><body>" + "\n")        
        for fig in figs:
            inner_html = fig.to_html().split('<body>')[1].split('</body>')[0]
            dashboard.write(inner_html)
        dashboard.write("</body></html>" + "\n")

        return self.path

class H5PandasTarget(DataTarget):
    def load(self, cached=False, **kwargs):
        opts = {**{'key':'data'},**kwargs}
        return super().load(pd.read_hdf, cached, **opts)

    def save(self, df, **kwargs):
        opts = {**{'key':'data'},**kwargs}
        return super().save(df, 'to_h

if TENSORFLOW_AVAILABLE:
    class H5KerasTarget(DataTarget):
        def load(self, cached=False, **kwargs):
            return super().load(load_model, cached, **kwargs)

        def save(self, df, **kwargs):
            return super().save(df, 'save', **kwargs)
