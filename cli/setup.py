#! /usr/bin/env python
#
# Copyright 2019-2020 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import codecs
import os
import re

from setuptools import find_packages
from setuptools import setup


HERE = os.path.abspath(os.path.dirname(__file__))
ROOT_DIR = os.path.abspath(os.path.join(HERE, os.path.pardir))


#####
# Helper functions
#####
def read(*filenames, **kwargs):
    """
    Build an absolute path from ``*filenames``, and  return contents of
    resulting file.  Defaults to UTF-8 encoding.
    """
    encoding = kwargs.get("encoding", "utf-8")
    sep = kwargs.get("sep", "\n")
    buf = []
    for fl in filenames:
        with codecs.open(os.path.join(HERE, fl), "rb", encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


def find_meta(meta):
    """Extract __*meta*__ from META_FILE."""
    re_str = r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta)
    meta_match = re.search(re_str, META_FILE, re.M)
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError("Unable to find __{meta}__ string.".format(meta=meta))


def get_recent_changelog(filename):
    """Grab the entry for the latest release."""
    contents = read(filename).split("\n")
    # Skip the title (first two lines)
    contents = contents[2:]

    # Find the latest release
    start_line, end_line = None, None
    for i, line in enumerate(contents):
        # if we've found the start & end, we're done
        if all([start_line, end_line]):
            break

        # 4 dashes are a horizontal line in rST, so look for headers with 5+
        if line.startswith("-----"):
            if start_line is None:
                start_line = i - 1
                continue
            if end_line is None:
                end_line = i - 1
                continue

    recent_log_lines = contents[start_line:end_line]
    recent_log = "\n".join(recent_log_lines)
    return recent_log


def get_main_readme_intro():
    """Grab the intro from the README from the root of the project."""
    readme_path = os.path.join(ROOT_DIR, "README.rst")
    contents = read(readme_path).split(".. start-long-desc")[1:]
    return "\n".join(contents)


def get_long_description(package_dir):
    """Generate the long description from the README and changelog."""
    cl_base_path = f"reference/{package_dir}/changelog"
    cl_file_path = os.path.join(ROOT_DIR, f"docs/src/{cl_base_path}.rst")
    cl_url = f"{PROJECT_URLS['Documentation']}/en/latest/{cl_base_path}.html"

    # When tox builds the library, it loses where the ROOT_DIR is; this
    # is also a problem with our integration tests. So we'll just ignore
    # if there's an error and continue on
    main_readme, recent_changelog = "", ""
    try:
        main_readme = get_main_readme_intro()
        recent_changelog = get_recent_changelog(cl_file_path)
    except Exception:
        pass

    desc = (
        read("README.rst")
        + "\n"
        + main_readme
        + "\n"
        + "Release Information\n"
        + "===================\n\n"
        + recent_changelog
        + f"\n\n`Full Changelog <{cl_url}>`_.\n\n"
    )
    return desc


#####
# Project-specific constants
#####
NAME = "klio-cli"
PACKAGE_NAME = "klio_cli"
PACKAGES = find_packages(where="src")
META_PATH = os.path.join("src", PACKAGE_NAME, "__init__.py")
CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "Natural Language :: English",
    "Operating System :: POSIX :: Linux",
    "Operating System :: MacOS :: MacOS X",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: Implementation :: CPython",
    "License :: OSI Approved :: Apache Software License",
    "Topic :: Software Development :: Libraries",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: Multimedia",
    "Topic :: Multimedia :: Graphics",
    "Topic :: Multimedia :: Sound/Audio",
    "Topic :: Multimedia :: Video",
    "Topic :: Scientific/Engineering",
    "Topic :: Scientific/Engineering :: Image Processing",
]
KEYWORDS = ["klio", "apache", "beam", "audio"]
PROJECT_URLS = {
    "Documentation": "https://docs.klio.io",
    "Bug Tracker": "https://github.com/spotify/klio/issues",
    "Source Code": "https://github.com/spotify/klio"
}
META_FILE = read(META_PATH)
INSTALL_REQUIRES = [
    "klio-core>=0.2.0",
    "click",
    "dateparser",
    "docker",
    "emoji",
    "jinja2",
    "glom",
    "google-api-core",
    "google-api-python-client",
    "google-cloud-monitoring",
    "google-cloud-pubsub",
    "google-cloud-storage",
    "protobuf",
    "pyyaml",
    "setuptools",
]
EXTRAS_REQUIRE = {
    "docs": ["sphinx", "interrogate"],
    "tests": [
        "coverage",
        "mock",  # for py2 - remove when dropping support
        "pytest>=4.3.0",  # 4.3.0 dropped last use of `convert`
        "pytest-cov",
        "pytest-mock",
    ],
}
EXTRAS_REQUIRE["dev"] = (
    EXTRAS_REQUIRE["docs"]
    + EXTRAS_REQUIRE["tests"]
    + ["klio-devtools", "bumpversion", "wheel"]
)
# support 3.6, 3.7, & 3.8, matching Beam's support
PYTHON_REQUIRES = ">=3.6, <3.9"

setup(
    name=NAME,
    version=find_meta("version"),
    description=find_meta("description"),
    long_description=get_long_description("cli"),
    long_description_content_type="text/x-rst",
    url=find_meta("uri"),
    project_urls=PROJECT_URLS,
    keywords=KEYWORDS,
    author=find_meta("author"),
    author_email=find_meta("email"),
    maintainer=find_meta("author"),
    maintainer_email=find_meta("email"),
    packages=PACKAGES,
    package_dir={"": "src"},
    include_package_data=True,
    classifiers=CLASSIFIERS,
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    entry_points={"console_scripts": ["klio = klio_cli.cli:main"]},
    python_requires=PYTHON_REQUIRES,
)
