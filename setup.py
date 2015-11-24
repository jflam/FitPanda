from distutils.core import setup
from Cython.Build import cythonize

setup(
    name = 'Fast CRC16',
    ext_modules = cythonize("crc.pyx"),
)
