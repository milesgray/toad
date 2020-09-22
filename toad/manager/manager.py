import os, sys, time
import pathlib, inspect, importlib
import textwrap
import warnings
import subprocess
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

import streamlit as st
import numpy as np
import pandas as pd
import luigi

import style
import task
import filesystem

LOG_DIR = "/data/luigi/logs/manager/"

def enqueue_output(file, queue):
    for line in iter(file.readline, ''):
        queue.put(line)
    file.close()

def read_popen_pipes(p):
    with ThreadPoolExecutor(2) as pool:
        q_stdout, q_stderr = Queue(), Queue()

        pool.submit(enqueue_output, p.stdout, q_stdout)
        pool.submit(enqueue_output, p.stderr, q_stderr)

        detach_reader_button = st.checkbox(f"Kill Thread")
        status_box = st.empty()
        start_time = time.time()

        while True:
            if detach_reader_button:
                print("DETACH SIGNAL RECIEVED - BREAKING")
                break
            if p.poll() is not None and q_stdout.empty() and q_stderr.empty():
                print("NO ACTIVITY - BREAKING")
                break            

            out_line = err_line = ''

            try:
                out_line = q_stdout.get_nowait()
            except Empty:
                pass
            try:
                err_line = q_stderr.get_nowait()
            except Empty:
                pass

            yield (out_line, err_line)

class EndTaskException(Exception):
    pass
class TaskExecution:
    def __init__(self, p, command, directory, task_name, log_mode='start'):
        self.log_full_path = self.create_log(command, directory, task_name, log_mode=log_mode)
        self.log_lines_written = 0
        self.p = p
        self.output = ""
        self.total_output = ""
        self.retval = None
        self.output_text = st.empty()

    def final_process_timeout(self, timeout=None, kill=False, te=None):
        if kill and te:
            print(f"timeout! {te} - killing process")
            self.p.kill()
        if timeout:
            print("attempt communicate with 10 sec timeout...")
            outs, errs = self.p.communicate(timeout=10)
        else:
            outs, errs = self.p.communicate()
        if outs:
            line = outs.decode('utf-8')
            print(f"final output: {line}")
            self.stream_log(line)
            self.output = f"{self.output}{line}"
            self.total_output = f"{self.total_output}\n\n{self.output}"
        if errs:
            line = errs.decode('utf-8')
            print(f"final error: {line}")
            self.stream_log(line)
            self.output = f"{self.output}{line}"
            self.total_output = f"{self.total_output}\n\n{self.output}"
        self.retval = self.p.poll()
        print(f"poll result: {self.retval}")
        
        if kill and te:
            st.error(f"Killed process! {te}\nfinal output: {outs}\nfinal errors: {errs}")
        
        return self.retval

    def process_async(self, out_line, err_line):
        if out_line:
            line = out_line.decode('utf-8')

            if line.startswith("INFO: Start"):
                self.stream_log(f"\n\n{line}")
                st.markdown("-------")
                self.output_text = st.empty()
                self.total_output = f"{self.total_output}\n\n{line}" if self.total_output != "" else f"{line}"
                self.output = ""
            else:
                self.stream_log(line)

            self.output = f"{self.output}{line}"
            self.output_text.code(self.output)
            if line.startswith("This progress looks"):
                self.total_output = f"{self.total_output}\n\n{self.output}"
                print("Exit task output listening loop...")
                try:
                    self.retval = self.final_process_timeout()
                except subprocess.TimeoutExpired as te:
                    self.retval = self.final_process_timeout(kill=True, te=te)
                finally:
                    self.finalize_log()
                raise EndTaskException()
        if err_line:
            line = err_line.decode('utf-8')
            st.error(line)
            self.total_output = f"{self.total_output}\n\n{line}" if self.total_output != "" else f"{line}"
            print("Exit task output listening loop...")
            try:
                print("attempt communicate with 10 sec timeout...")
                self.retval = self.final_process_timeout(timeout=10)
            except subprocess.TimeoutExpired as te:
                self.retval = self.final_process_timeout(kill=True, te=te)
            finally:
                self.finalize_log()
            raise EndTaskException()

    def create_log(self, command, directory, task_name, log_mode='start'):
        try:
            print(f"Writing log for {task_name}")
            dir_path = pathlib.Path(directory) / task_name
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)

            contents = [c for c in sorted(dir_path.glob("*.log"))]
            next_log = len(contents)
            if next_log:
                try:
                    next_log = int(contents[-1].stem) + 1
                except Exception as e:
                    st.warning(
                        "Issue incrementing log count... using {} as the next log. Error was: {}".format(next_log, e))

            full_path = dir_path / "{}.log".format(next_log)
            with full_path.open(mode="w") as f:
                f.write("{} time: {}\n".format(log_mode, time.strftime(
                    '%a, %d %b %Y %H:%M:%S GMT', time.localtime())))
                f.write("command: {}\n".format(command))

            return full_path
        except Exception as e:
            print(f"Failed to create log file for {task_name}: {e}")

    def stream_log(self, log_data, verbose=False):
        try:
            with self.log_full_path.open(mode="a") as f:
                f.write(log_data)
            return log_data.count('\n')
        except Exception as e:
            if verbose:
                print(f"Failed to write to log at {str(self.log_full_path)}:\n\t{e}")
            return 0

    def finalize_log(self, verbose=True):
        try:
            with self.log_full_path.open(mode="a") as f:
                f.write("end time: {}\n".format(time.strftime(
                    '%a, %d %b %Y %H:%M:%S GMT', time.localtime())))
        except Exception as e:
            if verbose:
                print(f"Failed to write to log at {str(self.log_full_path)}:\n\t{e}")
                
def start_task(task_name, file_name, module, params, async_output=True):
    # make shell command
    create_task = "PYTHONPATH=. python3 {}.py {} {}" \
        .format(file_name, task_name, " ".join(["--{}-{} {}".format(task_name, k.replace("_", "-"), v) for k, v in params.items()]))
    st.markdown("`{}`".format(create_task))
    #use shell command
    p = subprocess.Popen(create_task, shell=True, close_fds=True,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    st.markdown("### Execution Output")
    executor = TaskExecution(p, create_task, LOG_DIR, task_name)        
    for out_line, err_line in read_popen_pipes(p):
        with st.spinner('Executing Task...'):
            try:
                executor.process_async(out_line, err_line)
            except EndTaskException:
                break
            except Exception as e:
                print(f"Problem during async processing of {task_name}:\n{e}\n")
    print("escaped listening loop!")

    st.info("**retval:** `{}`".format(executor.retval))
    st.write(time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.localtime()))

def get_logs(directory, task_name):
    dir_path = pathlib.Path(directory) / task_name
    if not dir_path.exists():
        return []
    contents = [c for c in sorted(dir_path.glob("*.log"))]

    return contents

def get_upstream_tasks(task):
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore', message='Task .* without outputs has no custom complete\\(\\) method')
        is_task_complete = task.complete()

    result = []    
    name = task.__class__.__name__
    outputs = task.output()
    if type(outputs) is not list:
        outputs = [outputs]

    result.append((name, outputs, task, is_task_complete))

    children = luigi.task.flatten(task.requires())
    for child in children:
        result += get_upstream_tasks(child)

    return result

def print_tree(task, indent='\t', last=True, clip_params=False):
    '''
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    '''
    # dont bother printing out warnings about tasks with no output
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action='ignore', message='Task .* without outputs has no custom complete\\(\\) method')
        is_task_complete = task.complete()
    is_complete = ('COMPLETE' if is_task_complete else 'PENDING')
    name = task.__class__.__name__
    result = '\n' + indent
    if(last):
        result += '└─--'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += f'[{name}] - ({is_complete})'
    children = flatten(task.requires())
    for index, child in enumerate(children):
        result += print_tree(child, indent, (index+1) ==
                             len(children), clip_params)
    return result

def render_task_outputs(i, output, name, t_name, up_task, task_i):
    if hasattr(output, "path"):
        up_task_name = str(type(up_task))
        out_path = pathlib.Path(output.path)
        if out_path.parent.exists():
            st.markdown(f"{i} - {out_path.parent}")
            if out_path.exists():                
                st.success(f"`{output.path}` exists!")            
                if "Analyze" in up_task_name:
                    build_chart = st.checkbox(
                        f"{t_name} Build Charts", key=f"{name}_{t_name}_{task_i}_buildchart")
                    if build_chart:
                        try:
                            in_df = up_task.inputLoad()
                            figs = up_task.build_figures(in_df)
                            for fig in figs:
                                st.plotly_chart(fig)
                        except Exception as e:
                            st.error(f"Failed to build charts:\n{e}")
                if out_path.suffix == ".csv":                    
                    open_csv = st.checkbox(f"OPEN", key=f"{name}_{t_name}_{task_i}_openoutput")
                    if open_csv:
                        try:
                            df = pd.read_csv(output.path)
                            st.dataframe(df)
                        except Exception as e:
                            st.error(f"Failed: {e}")
                elif out_path.suffix == ".tsv":
                    open_tsv = st.checkbox(f"OPEN", key=f"{name}_{t_name}_{task_i}_openoutput")
                    if open_tsv:
                        try:
                            df = pd.read_csv(output.path, delimiter="/t")
                            st.dataframe(df)
                        except Exception as e:
                            st.error(f"Failed: {e}")
                elif out_path.suffix == ".html":
                    open_html = st.checkbox(
                        f"OPEN", key=f"{name}_{t_name}_{task_i}_openoutput")
                    if open_html:
                        try:
                            st.markdown(build_iframe(out_path),
                                    unsafe_allow_html=True)
                        except Exception as e:
                            st.error(f"Failed: {e}")
                delete = st.checkbox(f"DELETE", key=f"{name}_{t_name}_{task_i}_deleteoutput")
                if delete:
                    if st.button("ARE YOU SURE?", key=f"{name}_{t_name}_{task_i}_deleteoutput_confirm"):
                        try:
                            pathlib.Path(output.path).unlink()
                            return True
                        except Exception as e:
                            st.error(f"Failed: {e}")
            else:
                st.error(f"`{output.path}` does not exist")
            if any([1 for p in out_path.parent.glob("*.*") if p != out_path]):
                see_others = st.checkbox(f"See other files in folder {str(out_path.parent)}", key=f"{name}_{t_name}_{task_i}_otherfiles")
                if see_others:
                    for p in out_path.parent.glob("*"):
                        if not p.is_dir():
                            p_hash = str(p).replace("/", "-").replace(".", "-")
                            p_hash = f"{t_name}_{name}_{task_i}_{p_hash}"
                            if p.suffix == ".csv":
                                open_file = st.checkbox(f"Open {str(p.name)}", key=f"{i}open_{p_hash}")
                                if open_file:
                                    if "Analyze" in str(type(up_task)):
                                        try:
                                            figs = up_task.build_figures(pd.read_csv(p))
                                            for fig in figs:
                                                st.plotly_chart(fig)
                                        except Exception as e:
                                            st.error(f"Failed to build charts:\n{e}")
                                    else:
                                        try:
                                            st.dataframe(pd.read_csv(p))
                                        except Exception as e:
                                            st.error(f"Failed: {e}")
                            elif p.suffix == ".tsv":
                                open_file = st.checkbox(f"Open {str(p.name)}", key=f"{i}open_{p_hash}")
                                if open_file:
                                    try:
                                        st.dataframe(pd.read_csv(p, delimiter="/t"))
                                    except Exception as e:
                                        st.error(f"Failed: {e}")
                            elif p.suffix == ".html":
                                open_file = st.checkbox(f"Open {str(p.name)}", key=f"{i}open_{p_hash}")
                                if open_file:
                                    try:
                                        st.markdown(build_iframe(p),
                                            unsafe_allow_html=True)
                                    except Exception as e:
                                        st.error(f"Failed: {e}")
                            else:
                                st.markdown(f"{p.name}")
            st.markdown("-------")
    else:
        st.warning(f"Unknown output type: {type(output)}")

    return False

def render_status(name, clss):
    status_tree = print_tree(clss(), indent='\n')
    st.code(status_tree)
    upstream_tasks = get_upstream_tasks(clss())
    for task_i, (t_name, outputs, up_task, is_complete) in enumerate(upstream_tasks): 
        st.markdown(f"**{t_name}** - {len(outputs)} outputs - reporting complete: {is_complete}")
        for i, output in enumerate(outputs):
            if render_task_outputs(i, output, name, t_name, up_task, task_i):
                break

def render_module(module_selected):
    if module_selected != style.EMPTY_OPTION:
        if module_selected not in modules.keys():
            with st.spinner("Loading python file"):
                spec = filesystem.get_spec(files[module_selected], module_selected)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                modules[module_selected] = module 

        show_docs = st.sidebar.checkbox(f"Show {module_selected} Description")
        if show_docs:
            st.markdown(modules[module_selected].__doc__)

        module_classes = inspect.getmembers(modules[module_selected], inspect.isclass)
        for name, clss in module_classes:
            if "luigi." in str(clss) or "toad." in str(clss):
                continue
            st.sidebar.subheader("{}::{}".format(name, str(clss)))
            st.sidebar.markdown(clss.__doc__)

            show_status = st.sidebar.checkbox("Show {} Status".format(name), value="")
            if show_status:
                render_status(name, clss)
                   
            show_history = st.sidebar.checkbox(
                "Show {} Logs".format(name), value="")
            if show_history:
                history = get_logs(LOG_DIR, name)
                st.markdown("### {} Logs".format(len(history)))
                for h in history:
                    state = "Success" if "progress looks :)" in h.read_text(
                    ) else "Error"
                    show_log = st.checkbox(
                        f"View {h.stem} - {time.ctime(os.path.getmtime(str(h)))} - {state}")
                    if show_log:
                        st.code("{}\n\n{}".format(str(h), h.read_text()))
                        #st.markdown("{}\n\n{}".format(str(h), h.read_text().replace("\n", "\n\n")))
                st.markdown("-------")

            show_params = st.sidebar.checkbox(
                "Show {} Parameters".format(name), value="")
            if show_params:                
                # list of luigi.Parameter type members
                members = [attr for attr in dir(clss) if not callable(
                    getattr(clss, attr)) and not attr.startswith("_")]                
                st.success(f"All members: {members}")
                st.markdown(f"## {name} Parameters for Execution")
                task_params = {}
                for inst_name, inst in inspect.getmembers(clss):
                    if "luigi.parameter" in str(type(inst)):
                        st.markdown("-------")
                        param_type = str(type(inst)).split(
                            '.')[-1].replace("'>", "")
                        input_label = "{}".format(inst_name)
                        st.info(inst.description)

                        if param_type == "Parameter":
                            param_value = st.text_input(input_label, value='"{}"'.format(
                                inst._default), key=f"{name}_{inst_name}")
                            task_params[inst_name] = param_value.strip('"')
                        elif param_type == "IntParameter":
                            param_value = st.number_input(
                                input_label, value=inst._default, key=f"{name}_{inst_name}")
                            task_params[inst_name] = param_value
                        elif param_type == "FloatParameter":
                            param_value = st.number_input(
                                input_label, value=inst._default, key=f"{name}_{inst_name}")
                            task_params[inst_name] = param_value
                        elif param_type == "ListParameter":
                            st.warning(
                                "Lists must be in JSON format with double quotes surrounding each element and single quotes surrounding the entire list.")
                            if isinstance(inst._default, list):
                                if len(inst._default):
                                    if isinstance(inst._default[0], str):
                                        default = '","'.join(inst._default)
                                        default = f'["{default}"]'
                                    else:
                                        default = ','.join([str(d) for d in inst._default])
                                        default = f'[{default}]'
                                else:
                                    default = "[]"
                            elif isinstance(inst._default, str):
                                default = inst._default.strip("'")
                            else:
                                default = str(inst._default).strip("'")
                            default = f"'{default}'"
                            param_value = st.text_input(
                                input_label, value=default, key=f"{name}_{inst_name}")
                            task_params[inst_name] = param_value.strip('"')
                        elif param_type == "BoolParameter":
                            param_value = st.checkbox(
                                input_label, value=inst._default, key=f"{name}_{inst_name}")
                            if param_value:
                                task_params[inst_name] = ""
                        elif param_type == "EnumParameter":
                            param_value = st.selectbox(input_label, add_empty(
                                [e.name.upper() for e in inst._enum]))
                            if param_value != style.EMPTY_OPTION:
                                task_params[inst_name] = param_value
                        else:
                            st.error("Unknown type of param")

                        #task_params[inst_name] = param_value
                #task = getattr(module, name)(**task_params)
                if st.button(f"Execute {name}", key=f"{name}_exe"):
                    st.markdown("## Starting a toad Task!")
                    st.write(time.strftime(
                        '%a, %d %b %Y %H:%M:%S GMT', time.localtime()))
                    start_task(name, module_selected,
                            modules[module_selected], task_params)
                st.markdown("-------")

            st.sidebar.markdown("-------")

modules = {}
# START HERE -------------------------
# ------------------------------------
# ====================================
style.setup_style()
with st.spinner('Reading current directory for python files...'):
    files = filesystem.get_files('.')
    folders = filesystem.get_child_folders('.')

st.title("Dataset Task Manager Dashboard")
st.info("Use the sidebar drop down to select a task file to get started!")

st.sidebar.markdown(style.build_start_here(), unsafe_allow_html=True)
show_folders = st.sidebar.checkbox("Show Folders", value="")
if show_folders:
    for folder, files in folders.items():        
        show_folder = st.sidebar.checkbox(
            f"{folder}", value="", key=f"{folder}_show_folder")
        if show_folder:
            with st.spinner(f'Reading {folder} for python files...'):
                st.sidebar.markdown(style.build_path_banner(str(pathlib.Path(f"./{folder}").absolute())), unsafe_allow_html = True)
                module_selected = st.sidebar.selectbox(
                    "Select a task module to see which tasks it has available", style.add_empty([str(k) for k in sorted(files.keys())]))
                render_module(module_selected)

files = filesystem.get_files('.')
st.sidebar.markdown(style.build_path_banner(str(pathlib.Path('.').absolute())), unsafe_allow_html=True)
module_selected = st.sidebar.selectbox(
    "Select a task module to see which tasks it has available", style.add_empty([str(k) for k in sorted(files.keys())]))
render_module(module_selected)

