import os

try:
    with open(os.path.join(os.path.dirname(__file__), "version"), "r") as f:
        version_line = f.readline().strip()
        VERSION, BUILD_NUMBER = version_line.split(",")
except:
    VERSION = "local"
    BUILD_NUMBER = "0"
