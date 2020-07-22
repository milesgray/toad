import filesystem

class Job:
    """
    A Job represents a single python file that contains a number of Luigi task definitions that relate to each other.
    The python file is dynamically loaded in real time when a job is created and starts to lazily build state information
    as it is requested. State info includes things like the Luigi Status of each Task, any output files that belong to a Task,
    and a history of previous executions. The history is tracked by Toad currently and is different than the Luigi server's history -
    Toad saves the entire system output of each execution as a text file as a means of recording a history.  This is geared toward Tasks
    that are routinely run, but not automated extremely frequently.
    """
    def __init__(self, file):
        self.specspec = filesystem.get_spec(files[module_selected], module_selected)
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        
    def render(self):
        show_docs = st.sidebar.checkbox(f"Show {module_selected} Description")
        if show_docs:
            st.markdown(modules[module_selected].__doc__)

        module_classes = inspect.getmembers(modules[module_selected], inspect.isclass)
        for name, clss in module_classes:
            if "luigi." in str(clss) or "truigi." in str(clss):
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

                if st.button(f"Execute {name}", key=f"{name}_exe"):
                    st.markdown("## Starting a Truigi Task!")
                    st.write(time.strftime(
                        '%a, %d %b %Y %H:%M:%S GMT', time.localtime()))
                    start_task(name, module_selected,
                            modules[module_selected], task_params)
                st.markdown("-------")

            st.sidebar.markdown("-------")