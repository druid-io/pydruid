.. pydruid documentation master file, created by
   sphinx-quickstart on Thu Dec 22 17:53:31 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pydruid's documentation!
===================================

pydruid exposes a simple API to create, execute, and analyze `Druid <http://druid.io/>`_ queries. pydruid can parse query results into `Pandas <http://pandas.pydata.org/>`_ DataFrame objects for subsequent data analysis, which offers a tight integration between `Druid <http://druid.io/>`_, the `SciPy <http://www.scipy.org/stackspec.html>`_ stack (for scientific computing) and `scikit-learn <http://scikit-learn.org/stable/>`_ (for machine learning). Additionally, pydruid can export query results into TSV or JSON for further processing with your favorite tool, e.g., R, Julia, Matlab, or Excel.

Below is a reference for the PyDruid class, describing the functions to use for querying and exporting, complete with examples. For additional examples, see the `pydruid README <https://github.com/druid-io/pydruid>`_.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

pydruid.client module
---------------------

.. automodule:: pydruid.client
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.async_client module
---------------------------

.. automodule:: pydruid.async_client
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.query module
--------------------

.. automodule:: pydruid.query
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.utils.aggregators module
--------------------------------

.. automodule:: pydruid.utils.aggregators
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.utils.dimensions module
-------------------------------

.. automodule:: pydruid.utils.dimensions
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.utils.filters module
----------------------------

.. automodule:: pydruid.utils.filters
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.utils.having module
---------------------------

.. automodule:: pydruid.utils.having
    :members:
    :undoc-members:
    :show-inheritance:

pydruid.utils.postaggregator module
-----------------------------------

.. automodule:: pydruid.utils.postaggregator
    :members:
    :undoc-members:
    :show-inheritance:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
