.. Pemi documentation master file, created by
   sphinx-quickstart on Fri Feb 16 14:35:08 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. Documentation can be built using docker-compose run -w /app/docs --rm app sphinx-build -E . _build/html

Welcome to Pemi's documentation!
================================

Pemi is a framework for building testable ETL processes and workflows.

Motivation
----------

There are too many ETL tools.  So why do we need another one?  Many
tools emphasize performance, or scalability, or building ETL jobs
"code free" using GUI tools.  One of the features often
lacking is the ability to build
testable data integration solutions.  ETL can be exceedingly
complex, and small changes to code can have large effects on output,
and potentially devastating effects on the cleanliness of your data
assets.  Pemi was conceived with the goal of being able to build
highly complex ETL workflows while maintaining testability.

This project aims to be largely agnostic to the way data is
represented and manipulated.  There is currently some support for
working with `Pandas DataFrames <http://pandas.pydata.org/>`_,
in-database transformations (via `SqlAlchemy <https://www.sqlalchemy.org/>`_)
and `Apache Spark DataFrames <https://spark.apache.org>`_.  Adding new data
representations is a matter of creating a new Pemi DataSubject class.

Pemi does not orchestrate the execution of ETL jobs (for that kind of
functionality, see `Apache Airflow <https://airflow.apache.org>`_ or
`Luigi <https://github.com/spotify/luigi>`_).  And as stated above, it
does not force a developer to work with data using specific representations.
Instead, the main role for Pemi fits in the space between manipulating
data and job orchestration.



Index
-----

**Getting Started**

* :doc:`install`
* :doc:`concepts_features`
* :doc:`tests`
* :doc:`tutorial`
* :doc:`roadmap`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started

   install.rst
   concepts_features.rst
   tests.rst
   tutorial.rst
   roadmap.rst


**Reference**

* :doc:`pipe`
* :doc:`data_subject`
* :doc:`schema`
* :doc:`testing`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Reference

   pipe.rst
   data_subject.rst
   schema.rst
   testing.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
