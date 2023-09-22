import os

# load version information file that is expected to be added to the source folder in the Docker build process
# the format of the file is expected to be a single line with two comma separated values: "version" and "build_number"
# if not present returns dev values: version=local, build_number=0

try:
    with open(os.path.join(os.path.dirname(__file__), "version"), "r") as f:
        version_line = f.readline().strip()
        VERSION, BUILD_NUMBER = version_line.split(",")
except:
    VERSION = "local"
    BUILD_NUMBER = "0"
