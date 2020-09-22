"""
This module has a collection of tasks for traversing directories to build an initial list of files
to be processed. These tasks are a common 'first' task in a DAG for data related tasks that involved 
reading in datasets. The different tasks in this module represent different directory structure traversal
algorithms.

Easiest way to attach them is by using `requires`
.. code-block:: python
    @requires(CrawlDirectoryForIdentityFileList)
    ProcessImagesTask(toad.tasks.CSVPandasTask):
        ...
"""

import io
import os
import sys
import enum
import logging
import pathlib
from collections import defaultdict

import luigi
import pandas as pd

import toad
import toad.tasks


class CrawlDirectoryForIdentityFileList(toad.tasks.EventTask):
    """Task that adds every single file path under root.

        Parameters:
            path (string):
                absolute path of the directory to traverse
    """
    path = luigi.Parameter(description="Absoloute path to the directory to crawl")
    ext = luigi.Parameter(default='.jpg', description="File extension to limit results to, default .jpg")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config.artifact_path, "file_id_list_{}.tsv".format(self.path.replace("/", "-"))))

    def run(self):
        self.start("Crawling directory with a folder per identity - {}".format(self.path))
        path = pathlib.Path(self.path)
        dir_paths = toad.PathFileSystem().listdir(self.path, glob_pattern="**")
        path_dirs = [d.glob(f"*{self.ext}") for d in dir_paths if d != path]
        total = 0
        for p in path_dirs: total += len([x for x in p])
        if total == 0: raise Exception(f"No files found in {str(self.path)} with any extension")
        count = 0
        with self.output().open('w') as f:            
            id_count = 0

            for d in path_dirs:
                for p in d:
                    f.write(f"{id_count}\t{str(p)}\n")
                    count += 1

                id_count += 1

        self.final("Done crawling directory, found {} files and {} identities".format(count, id_count))

class CrawlDirectoryForFileList(toad.tasks.EventTask):
    """ Task that assumes a dir structure root with only folders, inside each folder is face chips of an ID.

        Parameters:
            path (string):
                absolute path of the directory to traverse
    """
    path = luigi.Parameter(description="Absoloute path to the directory to crawl")
    ext = luigi.Parameter(default='.jpg', description="File extension to limit results to, default .jpg")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config.artifact_path, "file_list_{}.tsv".format(self.path.replace("/", "-"))))

    def run(self):
        self.start("Crawling directory for every file in every folder, but not in the root - {}".format(self.path))
        
        paths = toad.PathFileSystem().listdir(self.path, ext=self.ext)
        if len(paths) == 0: raise Exception(f"No files found in {str(self.path)}")
        count = 0
        with self.output().open('w') as f:
            first = True
            
            for (dirpath, _, filenames) in os.walk(self.path):
                if first:
                    first = False
                    continue

                for filename in filenames:
                    if filename.endswith(self.ext):
                        f.write('{}\n'.format(dirpath + "/" + filename))
                        count += 1

        self.final("Done crawling directory, found {} files".format(count))

class CrawlFlatDirectoryForFileList(toad.tasks.EventTask):
    """ Task that assumes a dir where the only files are the images to retrieve, with no folders.

        Parameters:
            path (string):
                absolute path of the directory to traverse
    """
    path = luigi.Parameter(description="Absoloute path to the directory to crawl")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config.artifact_path, "file_flat_list_{}.tsv".format(str(self.path).replace("/", "-"))))

    def run(self):
        self.start("Crawling directory for every file, every folder, all of them - {}".format(self.path))
        
        paths = toad.PathFileSystem().listdir(self.path)
        if len(paths) == 0: raise Exception(f"No files found in {str(self.path)} with any extension")
        count = 0
        with self.output().open('w') as f:
            for p in paths:
                f.write(f"{str(p)}\n")
                count += 1

        self.final(f"Done crawling directory, found {count} files")

class CrawlFullDirectoryForFileList(toad.tasks.EventTask):
    """ Task that assumes a dir where the only files are the images to retrieve, with no folders.

        Parameters:
            path (string):
                absolute path of the directory to traverse
            ext (string):
                extension of files to select
    """
    path = luigi.Parameter(description="Absoloute path to the directory to crawl")
    ext = luigi.Parameter(default='.jpg', description="File extension to limit results to, default .jpg")

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget(os.path.join(self.config.artifact_path, "file_full_list_{}.csv".format(self.path.replace("/", "-"))))

    def run(self):
        self.start("Crawling directory for every folder for files with specified extention {} - {}".format(self.ext, self.path))        

        paths = toad.PathFileSystem().listdir(self.path, ext=self.ext)
        if len(paths) == 0: raise Exception(f"No files found in {str(self.path)} with {self.ext} extension")
        count = 0
        with self.output().open('w') as f:
            f.write('full_path\n')
            for p in paths:
                f.write(f"{str(p)}\n")
                count += 1

        self.final(f"Done crawling directory, found {count} files")

class CrawlDirectory(toad.tasks.CSVPandasTask):
    """
    Task that traverses a directory and outputs the contents based on the algorithm specified.

    Parameters:
        path (string):
            absolute path of the directory to traverse
        ext (string):
            extension of files to select
        mode (Algorithm):
            how to navigate directory structure and how to format results
    """
    class Algorithm(enum.Enum):
        FULL = 1
        FLAT = 2
        FOLDER = 3
        ID_FOLDER = 4

    mode = luigi.EnumParameter(default=Algorithm.FULL, enum=Algorithm, description="")
    path = luigi.Parameter(description="Absoloute path to the directory to crawl")
    ext = luigi.Parameter(
      default='.jpg', description="File extension to limit results to, default .jpg")

    def requires(self):
        return []

    def build_filename(self):
        return "file_{}_list_{}.csv".format(str(self.mode), self.path.replace("/", "-"))

    def run(self):
        self.start(f"Crawling directory for every folder for files with specified extention {self.ext} - {self.path}")
    
        results = defaultdict(list)
        
        if self.mode == CrawlDirectory.Algorithm.FULL:
            paths = pathlib.Path(str(self.path)).rglob('*{}'.format(self.ext))
            if len(paths) == 0: raise Exception(f"No files found in {str(self.path)} with {self.ext} extension")
            results["full_path"] = [str(p) for p in paths]                
        elif self.mode == CrawlDirectory.Algorithm.FLAT:            
            paths = toad.PathFileSystem().listdir(self.path)
            if len(paths) == 0: raise Exception(f"No files found in {str(self.path)} with any extension")
            results["full_path"] = [str(p) for p in paths]
        elif self.mode == CrawlDirectory.Algorithm.FOLDER:
            first = True
            paths = toad.PathFileSystem().listdir(self.path, ext=self.ext)
            if len(paths) == 0: raise Exception(f"No files found in {str(self.path)}")

            for (dirpath, _, filenames) in os.walk(self.path):
                if first:
                    first = False
                    continue

                for filename in filenames:
                    if filename.endswith(self.ext):
                        results["full_path"].append(dirpath + "/" + filename)
        elif self.mode == CrawlDirectory.Algorithm.ID_FOLDER:
            path = pathlib.Path(self.path)
            dir_paths = toad.PathFileSystem().listdir(self.path, glob_pattern="**")
            path_dirs = [d.glob(f"*{self.ext}") for d in dir_paths if d != path]
            total = 0
            for p in path_dirs:
                total += len([x for x in p])
            if total == 0:
                raise Exception(f"No files found in {str(self.path)} with any extension")
            
            with self.output().open('w') as f:
                id_count = 0

                for d in path_dirs:
                    for p in d:
                        results["full_path"].append(str(p))
                        results["id_count"].append(id_count)

                    id_count += 1
        else:
            path=pathlib.Path(str(self.path))
            for p in path.rglob('*{}'.format(self.ext)):
                results["full_path"].append(str(p))

        df = pd.DataFrame(results)
        self.save(df, header=True, index=False, encoding='utf-8')
        self.final("Done crawling directory, found {} files".format(df.index))

if __name__ == '__main__':
    import luigi_fleep.api
    bot = luigi_fleep.api.FleepBot()

    with luigi_fleep.api.fleep_notify(bot):
        luigi.run()                       
