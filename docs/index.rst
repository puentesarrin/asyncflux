.. Asyncflux documentation master file, created by
   sphinx-quickstart on Mon May 19 03:36:27 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Asyncflux
=========

Asynchronous client for InfluxDB_ and Tornado_.

Installation
============

You can use pip_ to install Asyncflux:

.. code-block:: bash

   $ pip install git+https://github.com/puentesarrin/asyncflux.git

Documentation
=============

Sphinx_ is needed to generate the documentation. Documentation can be generated
by issuing the following commands:

.. code-block:: bash

   $ cd docs
   $ make html

Or simply:

.. code-block:: bash

   $ python setup.py doc

Also, the current documentation can be found at ReadTheDocs_.

License
=======

Asyncflux is available under the |apache-license|_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   modules/index
   releases/index

.. _InfluxDB: http://influxdb.org
.. _Tornado: http://tornadoweb.org
.. _pip: http://pypi.python.org/pypi/pip
.. _Sphinx: http://sphinx-doc.org
.. _ReadTheDocs: https://asyncflux.readthedocs.org
.. _apache-license: http://www.apache.org/licenses/LICENSE-2.0.html
.. |apache-license| replace:: Apache License, Version 2.0
