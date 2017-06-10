from setuptools import setup, find_packages
import sys

setup(
    name='distribuPy',
    author="Houston Lucas",
    author_email="houstonlucas@nevada.unr.edu",
    description='Distributes computation to several machines.',
    url="https://github.com/houstonlucas/distribuPy",
    keywords="parallelism networking",
    version='0.1dev',
    packages=find_packages(),

)