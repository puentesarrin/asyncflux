# -*- coding: utf-8 *-*
import os
import subprocess
import sys

try:
    from setuptools import setup
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

from distutils.cmd import Command
from asyncflux import __version__


with open('README.rst') as f:
    readme_content = f.read()


class DocCommand(Command):

    description = "generate or test documentation"
    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]
    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "docs/_build/doctest"
            mode = "doctest"
        else:
            path = "docs/_build/%s" % __version__
            mode = "html"
            try:
                os.makedirs(path)
            except:
                pass
        status = subprocess.call(["sphinx-build", "-E",
                                  "-b", mode, "docs", path])
        if status:
            raise RuntimeError("documentation step '%s' failed" % (mode,))
        sys.stdout.write("\nDocumentation step '%s' performed, results here:\n"
                         "   %s/\n" % (mode, path))

setup(
    name='asyncflux',
    version=__version__,
    url='https://github.com/puentesarrin/asyncflux',
    description='Asynchronous client for InfluxDB and Tornado.',
    long_description=readme_content,
    author='Jorge Puente-Sarrín',
    author_email='puentesarrin@gmail.com',
    packages=['asyncflux'],
    keywords=['asyncflux', 'tornado', 'influxdb', 'influx', 'async'],
    install_requires=['tornado >= 3.0'],
    license='Apache License, Version 2.0',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy'],
    test_suite='tests.runtests',
    cmdclass={"doc": DocCommand}
)
