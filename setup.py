# -*- coding: utf-8 -*-

import os
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def find_version():
    for line in open("aiotarantool_queue/__init__.py"):
        if line.startswith("__version__"):
            return re.match(r"""__version__\s*=\s*(['"])([^'"]+)\1""", line).group(2)

setup(
    name="aiotarantool_queue",
    packages=["aiotarantool_queue"],
    package_dir={"aiotarantool_queue": os.path.join("aiotarantool_queue")},
    version=find_version(),
    author="Dmitry Shveenkov",
    author_email="shveenkov@mail.ru",
    url="https://github.com/shveenkov/aiotarantool-queue-python",
    classifiers=[
        "Programming Language :: Python :: 3.4",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database :: Front-Ends"
    ],
    # install_requires=[
    #     "tarantool>=0.5.1",
    # ],
    description="Tarantool Queue python bindings for asyncio",
    long_description=open("README.rst").read()
)
