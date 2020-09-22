import luigi

class GlobalConfig(luigi.Config):
	"""Pulls from the luigi config file to populate values. 

	Instantiate this to get all of the paths as set in the luigi config. 
	There is an instance in this file, config.global_cfg that should be the only instance needed. 
	DataTask has a member variable called `cfg` that is a reference to that local instance so most tasks
	can simple call `self.cfg.*`

	Many of the more often used values, such as the paths, have shortcut references in this file and can be
	accessed like so: 
	.. code-block:: python
		self.output = toad.config.outpath

		if toad.config.debug:
			logger = logging.getLogger(toad.config.logger_name)
	"""
	home_path = luigi.Parameter(default="/opt/data/luigi")
	data_path = luigi.Parameter(default="/opt/data/luigi/data")
	artifact_path = luigi.Parameter(default="/opt/data/luigi/artifacts")
	log_path = luigi.Parameter(default="/opt/data/luigi/logs")
	output_path = luigi.Parameter(default="/opt/data/luigi/output")
	trueface_key = luigi.Parameter(default="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbW90aW9uIjp0cnVlLCJmciI6dHJ1ZSwicGFja2FnZV9pZCI6bnVsbCwiZXhwaXJ5X2RhdGUiOiIyMDIwLTA3LTI0IiwidGhyZWF0X2RldGVjdGlvbiI6dHJ1ZSwibWFjaGluZXMiOjEsImFscHIiOnRydWUsIm5hbWUiOiJDeXJ1cyIsInRrZXkiOiJuZXciLCJleHBpcnlfdGltZV9zdGFtcCI6MTU5NTU0ODgwMC4wLCJhdHRyaWJ1dGVzIjp0cnVlLCJ0eXBlIjoib2ZmbGluZSIsImVtYWlsIjoiY3lydXNAdHJ1ZWZhY2UuYWkifQ.TbNqihIPCGlZlO8SAWE1I75_ga-Av2ggyuJfgu8z4o4")
	trueface_token_path = luigi.Parameter(default="/home/miles/token.txt")
	trueface_fd_path = luigi.Parameter(default="/home/miles/models/face_detect/trueface/")
	logger_name = luigi.Parameter(default="luigi-interface")
	is_debug = luigi.BoolParameter(default=True)
	use_memory_cache = luigi.BoolParameter(default=False)
	check_dependencies = luigi.BoolParameter(default=True)
	check_crc = luigi.BoolParameter(default=False)
	log_level = luigi.Parameter(default="DEBUG")

class memsql(luigi.Config):
	"""
	[memsql]
	marker-table = "luigi_table_updates"
	faces-connection-string = "mysql://root:ZLsY~fA72mhVscZLsYfA72mhVsc@127.0.0.1:3316/faces"
	host = "127.0.0.1"
	port = 3316
	db = "faces"
	user = "root"
	password = "ZLsY~fA72mhVscZLsYfA72mhVsc"
	"""	
	marker_table = luigi.Parameter(default="luigi_table_updates")
	faces_connection_string = luigi.Parameter(default="mysql://root:ZLsY~fA72mhVscZLsYfA72mhVsc@127.0.0.1:3316/faces")
	host = luigi.Parameter(default="127.0.0.1")
	port = luigi.IntParameter(default=3316)
	db = luigi.Parameter(default="faces")
	user = luigi.Parameter(default="root")
	password = luigi.Parameter(default="ZLsY~fA72mhVscZLsYfA72mhVsc")
	


isdebug = True # extra logging
show_params_on_run = False # Logs all values that are being internally tracked as parameters when toad.run is called
luigi.notifications.DEBUG = isdebug
isinit = False # is initialized?
isinitpipe = False # is pipe initialized?
cached = False # cache files in memory

# https://medium.com/@ageitgey/python-3-quick-tip-the-easy-way-to-deal-with-file-paths-on-windows-mac-and-linux-11a072b58d5f
# https://docs.python.org/3/library/pathlib.html
from pathlib import Path
dir = "/opt/data/luigi/data"
dirpath = Path(dir)

db=dirpath/'.toad.json'

check_dependencies = True
check_crc = False
log_level = "DEBUG"
logger_name = "luigi-interface"

uri = None
