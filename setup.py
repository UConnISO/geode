import sys
from setuptools import setup, find_packages

# TODO: We should probably specify that this doesn't work for python3 either
if sys.version_info < (2, 6, 6):
    raise RuntimeError("geode requires Python 2.6.6+")

with open("README.md") as f:
    long_description = f.read()

setup(
    name="geode",
    version="0.0.1",

    description="utility for aggregating network information",
    long_description=long_description,
    packages=find_packages()
)
