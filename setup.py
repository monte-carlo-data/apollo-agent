import distutils.text_file
from pathlib import Path
from typing import List, Optional

from setuptools import setup, find_packages


def parse_requirements(file_name: Optional[str] = "requirements.in") -> List[str]:
    return distutils.text_file.TextFile(
        filename=str(Path(__file__).with_name(file_name))  # type: ignore
    ).readlines()


def get_long_description():
    with open("README.md", "r") as fh:
        return fh.read()


setup(
    name="mcd-agent",
    use_scm_version=True,
    license="https://github.com/monte-carlo-data/apollo-agent/blob/main/LICENSE.md",
    description="Monte Carlo's Agent",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Monte Carlo Data, Inc",
    author_email="info@montecarlodata.com",
    url="https://www.montecarlodata.com/",
    packages=find_packages(exclude=["tests*", "utils*", "examples*"]),
    include_package_data=True,
    install_requires=parse_requirements(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    setup_requires=["setuptools", "setuptools_scm"],
)
