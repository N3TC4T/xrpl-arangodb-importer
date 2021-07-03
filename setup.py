import os, sys
from shutil import rmtree
from setuptools import setup, find_packages
from setuptools.command.install import install

class InstallCommand(install):
    def post_install(self):
      xrpl_deserilizer = "xrpl-deserializer-c"

      if os.path.isdir(xrpl_deserilizer):
        rmtree(xrpl_deserilizer)

      os.system("git clone https://github.com/XRPLF/{}".format(xrpl_deserilizer))
      with open("{}/main.c".format(xrpl_deserilizer), "a") as file:
          file.write("\n")
          file.write("char* de(uint8_t* raw, uint16_t len) { b58_sha256_impl = calc_sha_256; uint8_t* output = 0; if (!deserialize(&output, raw, len, 0, 0, 0)) return ""; return ((char*) output); }")

      os.system("cd {} && gcc main.c base58.c sha-256.c -O3  -fPIC -shared -o xd.so".format(xrpl_deserilizer))
    def run(self):
      install.run(self)
      self.post_install()


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
    cmdclass={
        'install': InstallCommand,
    },
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
)
