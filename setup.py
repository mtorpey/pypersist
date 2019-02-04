from setuptools import setup
import os

def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    return open(path).read().strip()

setup(name='pypersist',
      version=read("VERSION"),
      description='Persistent memoisation framework for Python',
      url='https://github.com/mtorpey/pypersist',
      author='Michael Torpey',
      author_email='mct25@st-andrews.ac.uk',
      license='GPL',
      packages=['pypersist'],
      long_description=read('README.md'),
      zip_safe=False)
