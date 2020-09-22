
import streamlit as st

EMPTY_OPTION = '----'
COLOR = "#FFF"
BACKGROUND_COLOR = "#000"

@st.cache
def add_empty(names):
    return [EMPTY_OPTION] + names

def local_css(file_name):
    with open(file_name) as f:
        st.markdown('<style>{}</style>'.format(f.read()),
                    unsafe_allow_html=True)
def remote_css(url):
    st.markdown('<style src="{}"></style>'.format(url), unsafe_allow_html=True)

def icon_css(icone_name):
    remote_css('https://fonts.googleapis.com/icon?family=Material+Icons')

def icon(icon_name):
    st.markdown('<i class="material-icons">{}</i>'.format(icon_name),
                unsafe_allow_html=True)

def setup_style(max_width=1250, 
                padding_top=1,
                padding_right=0.5,
                padding_bottom=10,
                padding_left=0.5):
    content_gradient = "linear-gradient(30deg,#333,#000)"
    sidebar_gradient = "linear-gradient(-190deg,#333,#000)"
    widget_gradient = "linear-gradient(165deg,#666,#000)"
     
    st.markdown(
        f"""
        <style>
            .reportview-container .main .block-container{{
                max-width: {max_width}px;
                padding-top: {padding_top}rem;
                padding-right: {padding_right}rem;
                padding-left: {padding_left}rem;
                padding-bottom: {padding_bottom}rem;
            }}
            .reportview-container .main {{
                color: {COLOR};
                background-color: {BACKGROUND_COLOR};
                background-image: {content_gradient};
            }}
            .sidebar .sidebar-content {{
                color: {COLOR};
                background-color: {BACKGROUND_COLOR};
                background-image: {sidebar_gradient};
            }}
            .row-widget label {{
                color: {COLOR};
                background-image: {widget_gradient};
            }}
            .row-widget label div {{
                color: {COLOR};
            }}        
            ul, .row-widget div div span {{
                background-color: #333 !important;
                color: #FFF;    
            }}
            input.st-bj, input.st-bj {{
                color: #FFF !important;
                background-color: #333 !important;  
            }}
            .st-ds {{
                background-color: #333 !important;    
            }}
            .alert-info {{
                background: rgba(0,104,201,0.25);
                border-color: rgba(0, 104, 201, 1);
                color: rgb(119, 119, 255);   
            }}
            .alert-success {{
                background: rgba(9,171,59,.25);
                border-color: rgba(9,171,59,1);
                color: rgb(102, 255, 102);
            }}
            .alert-danger {{
                background: rgba(255,43,43,.25);
                border-color: rgba(255,43,43,1);
                color: #FF6666;
            }}
            .alert-warning {{
                background: rgba(250,202,43,.25);
                border-color: rgba(250,202,43,1);
                color: #c7ad55;
            }}
            .overlayBtn {{
                opacity: 0.8;
                border: #FFF solid 3px;
                color: #FFF; 
                background: #333;               
            }}            
            code, pre {{
                padding: .2em .4em;
                margin: 0;
                color: #09ab3b !important;
                background-color: #222222 !important;
            }}
            .reportview-container .dataframe.data {{
                background-color: #333;
            }}
            .reportview-container .element-container .fullScreenFrame--expanded {{
                background-color: #555;
            }}
            .reportview-container .element-container .fullScreenFrame--expanded {{
                background-color: transparent;
            }}
            .dataframe.col-header, .reportview-container .dataframe.corner, .reportview-container .dataframe.row-header {{
                background-image: {content_gradient};
                color: #FFF !important;
            }}
            .stButton-enter, .stButton-exit {{
                border: 2px solid #FFF !important;
                margin: -10px;    
                color: #FFF !important;
                opacity: 0.8 !important;
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">',
                unsafe_allow_html=True)
@st.cache
def build_start_here():
    span_style = "font-size: 20px;padding:0px 35px;top:-5px;position:relative;"
    icon_1 = "<i class='material-icons'>settings_input_component</i>"
    icon_2 = "<i class='material-icons'>settings_input_component</i>"
    div_class = "alert alert-success stAlert"
    return f"<div class='{div_class}'>{icon_1}<span style='{span_style}'>Start here</span>{icon_2}</div>"

@st.cache
def build_path_banner(path):
    span_style = "font-size: 15px;display:block;"
    icon_1 = "<i class='material-icons'>folder_open</i>"
    icon_2 = "<i class='material-icons'>keyboard_arrow_down</i>"
    div_class = "alert alert-info stAlert"
    div_style = "padding:2px;"
    return f"<div class='{div_class}' style='{div_style}'><span style='{span_style}'>{path}</span>{icon_1}{icon_2}</div>"

@st.cache
def build_iframe(path, width=700, height=500):
    path = path if isinstance(path, str) else str(path)
    uniqueid = path.replace("/", "-").replace(".", "__")
    return f"<iframe id='{uniqueid}' width='{width}' height='{height}' src='file://{path}'></iframe>"
