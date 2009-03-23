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
HOST_LIST = ["unknown", "hpc", "mercury", "abe", "ranger", "kraken", "sdsc"]

# Valid users
USER_LIST = ["cybershk", "tera3d", "scottcal", "kmilner", "maechlin", "patrices"]

# Valid states
START_STATE = "Initial"
DONE_STATE = "Verified"
DELETED_STATE = "Deleted"
SGT_STATES = ["SGT Started", "SGT Generated", "SGT Error",]
PP_STATES = ["PP Started", "Curves Generated", "PP Error",]
ACTIVE_STATES = ["Initial", "SGT Started", "SGT Error", "SGT Generated", \
                     "PP Started", "PP Error", "Curves Generated", "Verify Error",]

# Dictionary expressing the state-transition-diagram
STATUS_STD = {"Initial": ["Initial", "SGT Started", "Deleted",], \
              "SGT Started":["SGT Started", "SGT Generated", "SGT Error", "Deleted",], \
              "SGT Error":["Initial", "SGT Started", "SGT Error", "Deleted",], \
              "SGT Generated":["SGT Generated", "PP Started", "Deleted",], \
              "PP Started":["PP Started", "Curves Generated", "PP Error", "Deleted",], \
              "PP Error":["SGT Generated", "PP Started", "PP Error", "Deleted",], \
              "Curves Generated":["Curves Generated", "Verified", "Verify Error", "Deleted",], \
              "Verify Error":["Verify Error", "Verified", "Deleted"], \
              "Verified":["Verified",], \
              "Deleted":["Deleted", ]}

# Directory containing hazard curve images
CURVE_DIR = "/home/scec-00/cybershk/opensha/curves/"

# Website URL
WEB_URL = "http://intensity.usc.edu/cybershake/status/"
