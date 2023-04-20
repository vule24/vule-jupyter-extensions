import os
import setuptools

with open( os.path.join(os.path.abspath(os.path.join(__file__, os.pardir)), "scripts/__VERSION__"), "r" ) as f:
    __VERSION__ =  f.read()

setuptools.setup(
    name="vule-sparkmagic",
    version=__VERSION__,
    author="Le Tuan Vu",
    author_email="ltnv24@gmail.com",
    description="Spark magic commands on Jupyter Lab",
    long_description="",
    long_description_content_type="text/markdown",
    packages=['vule_sparkmagic', 'scripts'],
    package_data={"scripts": ["__VERSION__"]},
    classifiers=[
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 3',
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)