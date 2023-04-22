import os
from setuptools import setup

with open( os.path.join(os.path.abspath(os.path.join(__file__, os.pardir)), "scripts/__VERSION__"), "r" ) as f:
    __VERSION__ =  f.read()

setup(
    name="vule-magics-python2",
    version=__VERSION__,
    author="Vu Le",
    author_email="ltnv24@gmail.com",
    description="Jupyter Notebook Python2 MagicCommands",
    long_description="",
    long_description_content_type="text/markdown",
    packages=["vule_magics", "scripts"],
    package_data={"scripts": ["__VERSION__"]},
    classifiers=[
        "Programming Language :: Python :: 2",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)