import pathlib

import streamlit as st
import pandas as pd
import luigi
from luigi.util import flatten

class Task:
    def __init__(self, task, index):
        self.index = index
        self.luigi = task
        self.is_complete = task.complete()
        self.name = task.__class__.__name__
        self.outputs = self.gather_outputs()
        self.parents = self.gather_parents()
        self.hash = f"{self.index}-{self.name}-{self.is_complete}__"

    def gather_outputs(self):
        outputs = self.luigi.output()
        if type(outputs) is not list:
            outputs = [outputs]
        results = []
        for i, o in enumerate(outputs):
            results.append(TaskOutput(o, self, i))
        return results

    def gather_parents(self):
        return flatten(self.luigi.requires())

    def render_outputs(self, name):
        for output in self.outputs:
            output.render()

class TaskOutput:
    def __init__(self, output, task, index):
        self.luigi = output
        self.task = task
        self.index = index
        self.file = TaskOutputFactory.build(self)

    def render(self, index, name):
        self.file.render(index, name)

class TaskOutputFactory:
    @staticmethod
    def build(output):
        if hasattr(output.luigi, "path"):
            out_path = pathlib.Path(output.luigi.path)
            if "Analyze" in str(type(task)):
                return TaskOutputAnalyze(output, output.task)
            elif out_path.suffix == ".csv":
                return TaskOutputCSV(output, output.task)
            elif out_path.suffix == ".tsv":
                return TaskOutputTSV(output, output.task)
            elif out_path.suffix == ".html":
                return TaskOutputHTML(output, output.task)
            else:
                return TaskOutputUnknown(output, output.task)
        else:
            return TaskOutputNoPath(output, output.task)

class TaskOutputFile:
    def __init__(self, output, task):
        self.task = task
        self.path = pathlib.Path(output.luigi.path)
        self.exists = self.path.parent.exists()
        if self.exists:
            self.complete = self.path.exists()
            self.others = [p for p in out_path.parent.glob("*.*") 
                                    if p != out_path]
        self.select = None
        self.delete = None

    def render(self, index, name):
        if self.exists:
            if self.complete:
                self._render(index, name)
                self._render_delete(index, name)
            else:
                self.error(f"`{str(self.path)}` does not exist")

            if any(self.others):
                see_others = st.checkbox(
                    f"See other files in folder {str(self.path.parent)}", 
                    key=f"{name}_{t_name}_{task_i}_otherfiles")

    def _render(self, index, name):
        pass

    def _render_delete(self, index, name):
        self.delete = st.checkbox(
            f"DELETE", 
            key=f"{name}_{t_name}_{task_i}_deleteoutput")
        if self.delete:
            if st.button("ARE YOU SURE?", 
                         key=f"{name}_{t_name}_{task_i}_deleteoutput_confirm"):
                try:
                    pathlib.Path(output.path).unlink()
                    return True
                except Exception as e:
                    st.error(f"Failed: {e}")

class TaskOutputAnalyze(TaskOutputFile):
    def _render(self, index, name):
        self.select = st.checkbox(
            f"{self.task.name} Build Charts", 
            key=f"{name}_{self.hash}_{index}_buildchart")
        if self.select:
            try:
                in_df = self.task.inputLoad()
                figs = self.task.build_figures(in_df)
                for fig in figs:
                    st.plotly_chart(fig)
            except Exception as e:
                st.error(f"Failed to build charts:\n{e}")

class TaskOutputCSV(TaskOutputFile):
    def render(self, index, name):
        self.select = st.checkbox(
            f"OPEN", 
            key=f"{name}_{self.hash}_{index}_openoutput")
        if self.select:
            try:
                df = pd.read_csv(self.output.path)
                st.dataframe(df)
            except Exception as e:
                st.error(f"Failed: {e}")

class TaskOutputTSV(TaskOutputFile):
    def render(self, index, name):
        self.select = st.checkbox(
            f"OPEN", 
            key=f"{name}_{self.hash}_{index}_openoutput")
        if self.select:
            try:
                df = pd.read_csv(self.output.path, delimiter="/t")
                st.dataframe(df)
            except Exception as e:
                st.error(f"Failed: {e}")

class TaskOutputHTML(TaskOutputFile):
    def render(self, index, name):
        self.select = st.checkbox(
            f"OPEN", 
            key=f"{name}_{self.hash}_{index}_openoutput")
        if self.select:
            try:
                st.markdown(build_iframe(out_path),
                            unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Failed: {e}")

class TaskOutputUnknown(TaskOutputFile):
    def render(self, index, name):
        st.warning(f"Unrecognized file format found: {self.path.suffix}")

class TaskOutputNoPath(TaskOutputFile):
    def render(self, index, name):
        try:
            loaded = self.task.outputLoad()
            st.info(f"No path associated with this output: {loaded}")
            if loaded:
                st.write(loaded)
        except Exception as e:
            st.error(f"Bad output! No path associated with {e}")
