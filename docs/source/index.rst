.. PyDruid documentation master file, created by
   sphinx-quickstart on Mon Mar  3 16:38:17 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pydruid's documentation
===================================

pydruid exposes a simple API to create, execute, and analyze `Druid <http://druid.io/>`_ queries. pydruid can parse query results into `Pandas <http://pandas.pydata.org/>`_ DataFrame objects for subsequent data analysis, which offers a tight integration between `Druid <http://druid.io/>`_, the `SciPy <http://www.scipy.org/stackspec.html>`_ stack (for scientific computing) and `scikit-learn <http://scikit-learn.org/stable/>`_ (for machine learning). Additionally, pydruid can export query results into TSV or JSON for further processing with your favorite tool, e.g., R, Julia, Matlab, or Excel.

Below is a reference for the PyDruid class, describing the functions to use for querying and exporting, complete with examples. For additional examples, see the `pydruid README <https://github.com/druid-io/pydruid>`_.

PyDruid Reference
=================

.. toctree::
   :maxdepth: 2

.. automodule:: pydruid
 
.. autoclass:: client.PyDruid
    :members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

