import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name="xrpl_arangodb_importer",
    version="0.0.1",
    author="N3TC4T",
    author_email="netcat.av@gmail.com",
    description="XRPL Arangodb Importer",
    license="MIT",
    url="https://github.com/N3TC4T/xrpl-arangodb-importer",
    scripts=['bin/arangodb_importer'],
    packages=find_packages(),
    install_requires=[
        'xrpl_websocket', 'pyArango', 'python-benedict'
    ],
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)
