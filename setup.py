
import os
from distutils.core import setup


os.system("./setup.sh")

setup(name='KVStore',
      version="0.1",
      packages=["KVStore"]
)