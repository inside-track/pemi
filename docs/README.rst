Welcome to Pemi's documentation!
================================

Pemi is a framework for building testable ETL processes and workflows.  Users
define **pipes** that define how to collect, transform, and deliver data.  Pipes
can be combined with other pipes to build out complex and modular data pipelines.
Testing is a first-class feature of Pemi and comes with a testing API to allow for
describing test coverage in a manner that is natural for data transformations.


`Full documentation on readthedocs <http://pemi.readthedocs.io/en/latest/index.html>`_



Install Pemi
============

.. inclusion-marker-install-pemi-begin

Pemi can be installed from pip::

    pip install pemi

.. inclusion-marker-install-pemi-end



Concepts and Features
=====================

.. inclusion-marker-concepts-features-begin

Pipes
-----

The principal abstraction in Pemi is the **Pipe**.  A pipe can be composed
of **Data Sources**, **Data Targets**, and other **Pipes**.  When
a pipe is executed, it collects data form the data sources, manipulates that data,
and loads the results into the data targets.  For example, here's a simple
"Hello World" pipe. It takes a list of names in the form of a Pandas DataFrame
and returns a Pandas DataFrame saying hello to each of them.

.. code-block:: python

    import pandas as pd

    import pemi
    from pemi.fields import *

    class HelloNamePipe(pemi.Pipe):
        # Override the constructor to configure the pipe
        def __init__(self):
            # Make sure to call the parent constructor
            super().__init__()

            # Add a data source to our pipe - a pandas dataframe called 'input'
            self.source(
                pemi.PdDataSubject,
                name='input',
                schema = pemi.Schema(
                    name=StringField()
                )

            )

            # Add a data target to our pipe - a pandas dataframe called 'output'
            self.target(
                pemi.PdDataSubject,
                name='output'
            )

        # All pipes must define a 'flow' method that is called to execute the pipe
        def flow(self):
            self.targets['output'].df = self.sources['input'].df.copy()
            self.targets['output'].df['salutation'] = self.sources['input'].df['name'].apply(
                lambda v: 'Hello ' + v
            )

To use the pipe, we have to create an instance of it::

    pipe = HelloNamePipe()

and give some data to the source named "input"::

    pipe.sources['input'].df = pd.DataFrame({
        'name': ['Buffy', 'Xander', 'Willow', 'Dawn']
    })

+--------+
| name   |
+========+
| Buffy  |
+--------+
| Xander |
+--------+
| Willow |
+--------+
| Dawn   |
+--------+

The pipe performs the data transformation when the ``flow`` method is called::

    pipe.flow()

The data target named "output" is then populated::

    pipe.targets['output'].df


+--------+--------------+
| name   | salutation   |
+========+==============+
| Buffy  | Hello Buffy  |
+--------+--------------+
| Xander | Hello Xander |
+--------+--------------+
| Willow | Hello Willow |
+--------+--------------+
| Dawn   | Hello Dawn   |
+--------+--------------+

Data Subjects
-------------

**Data Sources** and **Data Targets** are both types of **Data
Subjects**.  A data subject is mostly just a reference to an object
that can be used to manipulate data.  In the [Pipes](#pipes) example
above, we defined the data source called "input" as using the
``pemi.PdDataSubject`` class.  This means that this data subject refers
to a Pandas DataFrame object.  Calling the `df` method on this data subject
simply returns the Pandas DataFrame, which can be manipulated in all the ways
that Pandas DataFrames can be manipulated.

Pemi supports 3 data subjects natively, but can easily be extended to support others.  The
3 supported data subjects are

* ``pemi.PdDataSubject`` - Pandas DataFrames
* ``pemi.SaDataSubject`` - SQLAlchemy Engines
* ``pemi.SparkDataSubject`` - Apache Spark DataFrames

Schemas
-------

A data subject can optionally be associated with a **Schema**.
Schemas can be used to validate that the data object of the data
subject conforms to the schema.  This is useful when data is passed
from the target of one pipe to the source of another because it
ensures that downstream pipes get the data they are expecting.

For example, suppose we wanted to ensure that our data had fields called ``id`` and ``name``.
We would define a data subject like::

    from pemi.fields import *

    ds = pemi.PdDataSubject(
        schema=pemi.Schema(
            id=IntegerField(),
            name=StringField()
        )
    )

If we provide the data subject with a dataframe that does not have a field::

    df = pd.DataFrame({
        'name': ['Buffy', 'Xander', 'Willow']
    })

    ds.df = df

Then an error will be raised when the schema is validated (which happens automatically when
data is passed between pipes, as we'll see below)::

    ds.validate_schema()
    #=> MissingFieldsError: DataFrame missing expected fields: {'id'}

We'll also see later that defining a data subject with a schema also
aids with writing tests.  So while optional, defining data subjects
with an associated schema is highly recommended.

Referencing data subjects in pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Data subjects are rarely defined outside the scope of a pipe as done
in [Schemas](#schemas).  Instead, they are usually defined in the
constructor of a pipe as in [Pipes](#pipes).  Two methods of the
``pemi.Pipe`` class are used to define data subjects: ``source`` and
``target``.  These methods allow one to specify the data subject class
that the data subject will use, give it a name, assign a schema, and
pass on any other arguments to the specific data subject class.

For example, if we were to define a pipe that was meant to use an
Apache Spark dataframe as a source::

    spark_session = ...
    class MyPipe(pemi.Pipe):
        def __init__(self):
            super().__init__()

            self.source(
                pemi.SparkDataSubject,
                name='my_spark_source',
                schema=pemi.Schema(
                    id=IntegerField(),
                    name=StringField()
                ),
                spark=spark_session
            )

When ``self.source`` is called, it builds the data subject from the options provided
and puts it in a dictionary that is associated with the pipe.  The spark data frame
can then be accessed from within the flow method as::

            def flow(self):
                self.sources['my_spark_source'].df

Types of Pipes
--------------

Most user pipes will typically inherit from the main ``pemi.Pipe`` class.  However,
the topology of the pipe can classify it according to how it might be used.  While
the following definitions can be bent in some ways, they are useful for describing
the purpose of a given pipe.

* A **Source Pipe** is a pipe that is used to extract data from some
  external system and convert it into a Pemi data subject.  This data
  subject is the *target* of the *source* pipe.

* A **Target Pipe** is a pipe that is used to take a data subject and
  convert it into a form that can be loaded into some external system.
  This data subject is the *source* of the *target* pipe.

* A **Transformation Pipe** is a pipe that takes one or more data sources,
  transforms them, and delivers one more target sources.

* A **Job Pipe** is a pipe that is self-contained and does not specify any
  source or target data subjects.  Instead, it is usually composed of other
  pipes that are connected to each other.

Pipe Connections
----------------

A pipe can be composed of other pipes that are each connected to each
other.  These connections for a directed acyclic graph (DAG).  When
then connections between all pipes are executed, the pipes that form
the nodes of the DAG are executed in the order specified by the DAG
(in parallel, when possible -- parallel execution is made possible
under the hood via `Dask graphs
<https://dask.pydata.org/en/latest/custom-graphs.html>`_).  The data
objects referenced by the node pipes' data subjects are passed between
the pipes according.

As a minimal example showing how connections work, let's define
a dummy source pipe that just generates a Pandas dataframe with
some data in it::

    class MySourcePipe(pemi.Pipe):
        def __init__(self):
            super().__init__()

            self.target(
                pemi.PdDataSubject,
                name='main'
            )

        def flow(self):
            self.targets['main'].df = pd.DataFrame({
                'id': [1,2,3],
                'name': ['Buffy', 'Xander', 'Willow']
            })

And a target pipe that just prints the "salutation" field::

    class MyTargetPipe(pemi.Pipe):
        def __init__(self):
            super().__init__()

            self.source(
                pemi.PdDataSubject,
                name='main'
            )

        def flow(self):
            for idx, row in self.sources['main'].df.iterrows():
                print(row['salutation'])

Now we define a job pipe that will connect the dummy source pipe to
our hello world pipe and connect that to our dummy target pipe::

    class MyJob(pemi.Pipe):
        def __init__(self):
            super().__init__()

            self.pipe(
                name='my_source_pipe',
                pipe=MySourcePipe()
            )
            self.connect('my_source_pipe', 'main').to('hello_pipe', 'input')

            self.pipe(
                name='hello_pipe',
                pipe=HelloNamePipe()
            )
            self.connect('hello_pipe', 'output').to('my_target_pipe', 'main')

            self.pipe(
                name='my_target_pipe',
                pipe=MyTargetPipe()
            )

        def flow(self):
            self.connections.flow()

In the flow method we call ``self.connections.flow()``.  This calls the
``flow`` method of each pipe defined in the connections graph and
transfers data between them, in the order specified by the DAG.

The job pipe can be executed by calling its ``flow`` method::

    MyJob().flow()
    # => Hello Buffy
    # => Hello Xander
    # => Hello Willow

Furthermore, if you're running this in a Jupyter notebook, you can see a graph of the
connections by running::

    MyJob().connections.graph()


Referencing pipes in pipes
^^^^^^^^^^^^^^^^^^^^^^^^^^

Referencing pipes within pipes works the same way as for data sources and targets.
For example, if we wanted to run the ``MyJob`` job pipe and then look at the
source of the "hello_pipe"::

    job  = MyJob()
    job.flow()
    job.pipes['hello_pipe'].sources['input'].df


.. inclusion-marker-concepts-features-end

Where to go from here
=====================

`Full documentation on readthedocs <http://pemi.readthedocs.io/en/latest/index.html>`_


Contributing
============

If you want to contribute to the development of Pemi, you'll need to be able to run the test
suite locally.  To get started, copy the example environment file to a file you can
edit locally if needed:

    >>> cp example.env .env

All of the tests are run inside of a docker container, which you can build using

    >>> inv build

Once the containers are built, spin up the containers to run the tests

    >>> inv up

And then run the tests using something like (you may prefer different pytest options):

    >>> inv test --pytest="-s -x -vv --tb=short --color=yes tests"

The test container also launches a local Jupyter notebook server.  This can be a convenient tool to
have when developing Pemi.  To access the notebook severs, just visit http://localhost:8890/lab
in a web browser (the specific port can be configured in the ``.env`` file).

Take down the container using

    >>> inv down
