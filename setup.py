import os, sys, subprocess
from shutil import rmtree
from setuptools import setup, find_packages
from setuptools.command.install import install

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


class InstallCommand(install):
    def install_requires(self):
        subprocess.call(
            [sys.executable, '-m', 'pip', 'install', '-r' , 'requirements.txt']
        )

    def post_install(self):
        package = "xrpl-deserializer-c"
        dir_path = "src/deps/{}".format(package)

        if os.path.isdir(dir_path):
            rmtree(dir_path)

        os.system("git clone https://github.com/XRPLF/{} {}".format(package, dir_path))
        with open("{}/main.c".format(dir_path), "a") as file:
            file.write("\n")
            file.write("uint8_t *de(uint8_t* raw, uint16_t len) { b58_sha256_impl = calc_sha_256; uint8_t* output = 0; if (!deserialize(&output, raw, len, 0, 0, 0)) return 0; return output; }\nvoid freeme(char *ptr) { free(ptr); ptr = NULL; }")

        os.system("cd {} && gcc main.c base58.c sha-256.c -O3  -fPIC -shared -o xd.so -w".format(dir_path))
    
    def run(self):
        install.run(self)

        self.install_requires()
        self.post_install()


setup(
    name="xrpl_arangodb_importer",
    version="0.0.1",
    author="N3TC4T",
    author_email="netcat.av@gmail.com",
    description="XRPL Arangodb Importer",
    license="MIT",
    url="https://github.com/N3TC4T/xrpl-arangodb-importer",
    packages=find_packages(),
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
