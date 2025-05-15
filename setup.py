from setuptools import setup
from Cython.Build import cythonize
import pathlib

# setup(
#     ext_modules=cythonize(str(pathlib.Path(__file__).parent / "src" / "dandi_s3_log_parser" / "_s3_log_reader_c.pyx"))
# )
# Complains can't find it

# setup(
#     ext_modules=cythonize("src/dandi_s3_log_parser/_s3_log_reader_c.pyx")
# )
# Complained about absolute path

from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        name="test_cython",
        sources=["src/dandi_s3_log_parser/_s3_log_reader/_s3_log_reader_c.pyx"]
    )
]

setup(
    ext_modules=cythonize(extensions)
)