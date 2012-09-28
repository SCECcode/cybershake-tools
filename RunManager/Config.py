#!/usr/bin/env python


# DB Constants
DB_HOST = "focal.usc.edu"
DB_PORT = 3306
DB_USER_WR = "cybershk"
DB_PASS_WR = "***REMOVED***"
DB_USER = "cybershk_ro"
DB_PASS = "CyberShake2007"
DB_NAME = "CyberShake"

# Valid computing resources
HOST_LIST = ["unknown", "hpc", "mercury", "abe", "ranger", "kraken", \
                 "sdsc"]

# Valid users
USER_LIST = ["cybershk", "tera3d", "scottcal", "kmilner", "maechlin", \
                 "patrices"]

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
OPENSHA_LOGIN = 'cybershk@opensha.usc.edu'
OPENSHA_DIR = '/home/scec-00/cybershk/opensha'
OPENSHA_SCATTER_SCRIPT = '%s/make_scatter_map.sh' % (OPENSHA_DIR)
OPENSHA_INTERPOLATED_SCRIPT = '%s/make_interpolated_map.sh' % (OPENSHA_DIR)
OPENSHA_CURVE_SCRIPT = '%s/curve_plot_wrapper.sh' % (OPENSHA_DIR)
OPENSHA_XML_DIR = '/home/scec-00/cybershk/opensha/OpenSHA/src/org/opensha/sha/cybershake/conf'
OPENSHA_ERF_XML = '%s/%s' % (OPENSHA_XML_DIR, 'MeanUCERF.xml')
OPENSHA_AF_XML = '%s/%s,%s/%s,%s/%s,%s/%s' % \
    (OPENSHA_XML_DIR, 'cb2008.xml', \
         OPENSHA_XML_DIR, 'ba2008.xml', \
         OPENSHA_XML_DIR, 'cy2008.xml', \
         OPENSHA_XML_DIR, 'as2008.xml')
OPENSHA_DBPASS_FILE = '/home/scec-00/cybershk/config/db_pass.txt'


# Directories containing hazard curve images
CURVE_DIRS = ["/home/scec-00/cybershk/opensha/curves/", \
                  "/home/scec-00/tera3d/opensha/curves/"]


# Path to the most recent scatter plot
SCATTER_IMG = "/home/scec-00/cybershk/opensha/scatter/map_cb.png"


# Path to the most recent interpolated maps
INTERPOLATED_ALL_IMG = "/home/scec-00/cybershk/opensha/interpolatedMap/map.png"
INTERPOLATED_GRID_IMG = "/home/scec-00/cybershk/opensha/interpolatedMap/allGrid.png"


# Website URL
WEB_URL = "http://intensity.usc.edu/cybershake/status/"


# Maximum column lengths
MAX_RUN_SUBMIT_DIR = 256
MAX_RUN_COMMENT = 128
MAX_RUN_NOTIFY_USER = 128
