#!/usr/bin/env python


# DB Constants
#DB_HOST = "focal.usc.edu"
DB_HOST = "moment.usc.edu"
DB_PORT = 3306
DB_USER_WR = "cybershk"
DB_PASS_WR = "re@lStil1"
DB_USER = "cybershk_ro"
DB_PASS = "CyberShake2007"
DB_NAME = "CyberShake"

# Valid computing resources
HOST_LIST = ["unknown", "hpc", "mercury", "abe", "ranger", "kraken", \
                 "sdsc", "hpc-local", "stampede", "bluewaters", "titan", \
		"cori", "summit"]

# Valid users
USER_LIST = ["cybershk", "tera3d", "scottcal", "kmilner", "maechlin", \
                 "patrices", "davidgil" ]

# Valid states
START_STATE = "Initial"
PLOT_STATE = "Plotting"
DONE_STATE = "Verified"
DELETED_STATE = "Deleted"
SGT_STATES = ["SGT Started", "SGT Generated", "SGT Error",]
PP_STATES = ["PP Started", "Curves Generated", "PP Error",]
ACTIVE_STATES = ["Initial", "SGT Started", "SGT Error", \
                     "SGT Generated", "PP Started", "PP Error", \
                     "Curves Generated", "Plotting", "Verify Error",]

# Dictionary expressing the state-transition-diagram
STATUS_STD = {"Initial": ["Initial", "SGT Started", "SGT Error", "Deleted",], \
              "SGT Started":["SGT Started", "SGT Generated", "SGT Error", \
                                 "Deleted",], \
              "SGT Error":["Initial", "SGT Started", "SGT Error", "Deleted",], \
              "SGT Generated":["SGT Generated", "PP Started", "PP Error", \
                                   "Deleted",], \
              "PP Started":["PP Started", "Curves Generated", "PP Error", \
                                "Deleted",], \
              "PP Error":["SGT Generated", "PP Started", "PP Error", \
                              "Deleted",], \
              "Curves Generated":["Curves Generated", "Plotting", "Verified", \
                                      "Verify Error", "Deleted",], \
              "Plotting":["Plotting", "Verified", "Verify Error", \
                              "Deleted", ], \
              "Verify Error":["Verify Error", "Plotting", "Verified", \
                                  "Deleted"], \
              "Verified":["Verified",], \
              "Deleted":["Deleted", ]}


# OpenSHA scripts and config
# Logging into OpenSHA with user cybershk stopped working
# See if we can do this on shock instead
OPENSHA_LOGIN = 'cybershk@shock.usc.edu'
#OPENSHA_LOGIN = 'cybershk@opensha.usc.edu'
OPENSHA_DIR = '/home/shock-ssd/scottcal/opensha'
OPENSHA_SCATTER_SCRIPT = '%s/make_scatter_map.sh' % (OPENSHA_DIR)
OPENSHA_INTERPOLATED_SCRIPT = '%s/make_interpolated_map.sh' % (OPENSHA_DIR)
OPENSHA_CURVE_SCRIPT = '%s/curve_plot_wrapper.sh' % (OPENSHA_DIR)
OPENSHA_XML_DIR = '/home/shock-ssd/scottcal/opensha/conf'
OPENSHA_ERF_XML = '%s/%s' % (OPENSHA_XML_DIR, 'MeanUCERF.xml')
OPENSHA_AF_XML = '%s/%s,%s/%s,%s/%s,%s/%s' % \
    (OPENSHA_XML_DIR, 'cb2014.xml', \
         OPENSHA_XML_DIR, 'bssa2014.xml', \
         OPENSHA_XML_DIR, 'cy2014.xml', \
         OPENSHA_XML_DIR, 'ask2014.xml')
OPENSHA_DBPASS_FILE = '/home/shock/scottcal/runs/config/db_pass.txt'


# Directories containing hazard curve images
CURVE_DIRS = ["/home/shock/scottcal/db_products/curves/"]
CURVE_DIR = CURVE_DIRS[0]


# Path to the most recent scatter plot
SCATTER_IMG = "/home/shock/scottcal/db_products/scatter_map_cb.png"


# Path to the most recent interpolated maps
INTERPOLATED_ALL_IMG = "/home/shock/scottcal/db_products/interpolatedMap/map.png"
INTERPOLATED_GRID_IMG = "/home/shock/scottcal/db_products/interpolatedMap/allGrid.png"


# Website URL
WEB_URL = "http://strike.scec.org/cybershake/status/"


# Maximum column lengths
MAX_RUN_SUBMIT_DIR = 256
MAX_RUN_COMMENT = 128
MAX_RUN_NOTIFY_USER = 128
