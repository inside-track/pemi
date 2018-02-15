# Pemi - Python Extract Map Integrate

Pemi is a framework for building testable ETL processes and workflows.

## Motivation

There are too many ETL tools.  So why do we need another one?  Many
tools emphasize performance, or scalability, or building ETL jobs
"code free" using GUI tools.  One of the features either completely
lacking, or only included as an afterthought, is the ability to build
testable data integration solutions.  ETL is often exceedingly
complex, and small changes to code can have large effects on output
and potentially devastating effects on the cleanliness of your data
assets.  Pemi was conceived with the goal of being able to build
highly complex ETL workflows while maintaining testability.

This project aims to be largely agnostic to the way data is
represented and manipulated.  There is currently some support for
working with [Pandas DataFrames](http://pandas.pydata.org/),
in-database transformations (via [SqlAlchemy](https://www.sqlalchemy.org/))
and [Apache Spark DataFrames](https://spark.apache.org/).  Adding new data
representations is a matter of creating a new Pemi DataSubject class.

Pemi does not orchestrate the execution of ETL jobs (for that kind of
functionality, see [Apache Airflow](https://airflow.apache.org/) or
[Luigi](https://github.com/spotify/luigi)).  And as stated above, it
does not force a developer to work with data using specific representations.
Instead, the main role for Pemi fits in the space between manipulating
data and job orchestration.


## Concepts and Features

### Pipes

The principal abstraction in Pemi is the **Pipe**.  A pipe can be composed
of **Data Sources**, **Data Targets**, and other **Pipes**.  When
a pipe is executed, it collects data form the data sources, manipulates that data,
and loads the results into the data targets.  For example, here's a simple
"Hello World" pipe. It takes a list of names in the form of a Pandas DataFrame
and returns a Pandas DataFrame saying hello to each of them.

```python
import pandas as pd

import pemi
from pemi.fields import *

class HelloNamePipe(pemi.Pipe):
    # Override the constructor to configure the pipe
    def __init__(self, **kwargs):
        # Make sure to call the parent constructor
        super().__init__(**kwargs)

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
```


To use the pipe, we have to create an instance of it
```python
pipe = HelloNamePipe()
```
and give some data to the source named "input"
```python
pipe.sources['input'].df = pd.DataFrame({
    'name': ['Buffy', 'Xander', 'Willow', 'Dawn']
})
```
| name   |
| :-     |
| Buffy  |
| Xander |
| Willow |
| Dawn   |

The pipe performs the data transformation when the `flow` method is called.
```python
pipe.flow()
```
The data target named "output" is then populated
```python
pipe.targets['output'].df
```
| name   | salutation   |
| :-     | :-           |
| Buffy  | Hello Buffy  |
| Xander | Hello Xander |
| Willow | Hello Willow |
| Dawn   | Hello Dawn   |


### Data Subjects

**Data Sources** and **Data Targets** are both types of **Data
Subjects**.  A data subject is mostly just a reference to an object
that can be used to manipulate data.  In the [Pipes](#pipes) example
above, we defined the data source called "input" as using the
`pemi.PdDataSubject` class.  This means that this data subject refers
to a Pandas DataFrame object.  Calling the `df` method on this data subject
simply returns the Pandas DataFrame, which can be manipulated in all the ways
that Pandas DataFrames can be manipulated.

Pemi supports 3 data subjects natively, but can easily be extended to support others.  The
3 supported data subjects are

* `pemi.PdDataSubject` - Pandas DataFrames
* `pemi.SaDataSubject` - SQLAlchemy Engines
* `pemi.SparkDataSubject` - Apache Spark DataFrames

#### Schemas

A data subject can optionally be associated with a **Schema**.
Schemas can be used to validate that the data object of the data
subject conforms to the schema.  This is useful when data is passed
from the target of one pipe to the source of another because it
ensures that downstream pipes get the data they are expecting.

For example, suppose we wanted to ensure that our data had fields called `id` and `name`.
We would define a data subject like
```python
from pemi.fields import *

ds = pemi.PdDataSubject(
    schema=pemi.Schema(
        id=IntegerField(),
        name=StringField()
    )
)
```
If we provide the data subject with a dataframe that does not have a field,
```python
df = pd.DataFrame({
    'name': ['Buffy', 'Xander', 'Willow']
})

ds.df = df
```
Then an error will be raised when the schema is validated (which happens automatically when
data is passed between pipes, as we'll see below)
```python
ds.validate_schema()
#=> MissingFieldsError: DataFrame missing expected fields: {'id'}
```

We'll also see later that defining a data subject with a schema also
aids with writing tests.  So while optional, defining data subjects
with an associated schema is highly recommended.

#### Referencing data subjects in pipes

Data subjects are rarely defined outside the scope of a pipe as done
in [Schemas](#schemas).  Instead, they are usually defined in the
constructor of a pipe as in [Pipes](#pipes).  Two methods of the `pemi.Pipe` class are used
to define data subjects: `source` and `target`.  These methods allow one to specify the
data subject class that the data subject will use, give it a name, assign a schema,
and pass on any other arguments to the specific data subject class.

For example, if we were to define a pipe that was meant to use an Apache Spark dataframe as
a source,
```python
spark_session = ...
class MyPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.source(
            pemi.SparkDataSubject,
            name='my_spark_source',
            schema=pemi.Schema(
                id=IntegerField(),
                name=StringField()
            ),
            spark=spark_session
        )
```
When `self.source` is called, it builds the data subject from the options provided
and puts it in a dictionary that is associated with the pipe.  The spark data frame
can then be accessed from within the flow method as
```python
        def flow(self):
            self.sources['my_spark_source'].df
```

### Types of Pipes

Most user pipes will typically inherit from the main `pemi.Pipe` class.  However,
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

### Pipe Connections

A pipe can be composed of other pipes that are each connected to each
other.  These connections for a directed acyclic graph (DAG).  When
then connections between all pipes are executed, the pipes that form
the nodes of the DAG are executed in the order specified by the DAG
(in parallel, when possible -- parallel execution is made possible
under the hood via [Dask
graphs](https://dask.pydata.org/en/latest/custom-graphs.html)).  The
data objects referenced by the node pipes' data subjects are passed
between the pipes according.

As a minimal example showing how connections work, let's define
a dummy source pipe that just generates a Pandas dataframe with
some data in it.
```python
class MySourcePipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.target(
            pemi.PdDataSubject,
            name='main'
        )

    def flow(self):
        self.targets['main'].df = pd.DataFrame({
            'id': [1,2,3],
            'name': ['Buffy', 'Xander', 'Willow']
        })
```

And a target pipe that just prints the "salutation" field.
```python
class MyTargetPipe(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.source(
            pemi.PdDataSubject,
            name='main'
        )

    def flow(self):
        for idx, row in self.sources['main'].df.iterrows():
            print(row['salutation'])
```

Now we define a job pipe that will connect the dummy source pipe to
our hello world pipe and connect that to our dummy target pipe.
```python
class MyJob(pemi.Pipe):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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
```
In the flow method we call `self.connections.flow()`.  This calls the
`flow` method of each pipe defined in the connections graph and
transfers data between them, in the order specified by the DAG.

The job pipe can be executed by calling its `flow` method.
```python
MyJob().flow()
# => Hello Buffy
# => Hello Xander
# => Hello Willow
```

Furthermore, if you're running this in a Jupyter notebook, you can see a graph of the
connections by running
```python
MyJob().connections.graph()
```

#### Referencing pipes in pipes
Referencing pipes within pipes works the same way as for data sources and targets.
For example, if we wanted to run the `MyJob` job pipe and then look at the
source of the "hello_pipe":
```python
job  = MyJob()
job.flow()
job.pipes['hello_pipe'].sources['input'].df
```


### Testing

Testing is an essential component of software development that is
often neglected in the data and ETL world.  Pemi was designed to fill
that gap, and facilitate writing expressive data transformation tests.
Pemi tests run on top of the popular Python testing framework
[Pytest](https://docs.pytest.org/en/latest/).

The concepts involved in testing Pemi pipes include

* A **Scenario** describes the transformation that is being tested
  (e.g., a Pemi pipe), and the data sources and targets that are the
  subject of the test.  Scenarios are composed of one more **Cases**.
* A **Case** is a set of **Conditions** and **Expectations** that describe
  how the pipe is supposed to function.
* A **Condition** describes how the the data for a particular case is
  to be initialized -- e.g., "when the field 'name' has the value 'Xander'".
* An **Expectation** describes the expected result, after the pipe has
  been executed -- e.g., "then the field 'salutation' has the value
  'Hello Xander'".

To see these concepts play out in an example, let's write a simple
test for our `HelloNamePipe`.  In this README, we'll talk through it
in stages, but the full example can be found in
[tests/test_readme.py](tests/test_readme.py).

To aid with grouping cases into distinct scenarios, scenarios are defined using
a context-manager pattern.  So if we want to build a scenario called "Testing HelloNamePipe",
we set that up like
```python
import pemi.testing as pt

with pt.Scenario('Testing HelloNamePipe') as scenario:
    pipe = HelloNamePipe()

    scenario.setup(
        runner=pipe.flow,
        case_keys=case_keys(),
        sources={
            'input': pipe.sources['input']
        },
        targets={
            'output': pipe.targets['output']
        }
    )
```

Within this block, we create an instance of `HelloNamePipe` as the
variable `pipe` and configure the scenario using the `setup` method.
Let's quickly review the parameters of `scenario.setup(...)`:

* `runner` - This is a function that is called to execute the process
  we'll be testing.  It is only called once per scenario after all
  cases have been defined.
* `case_keys` - Case keys are needed to identify which records belong
  to which cases.  More on this below.
* `sources`/`targets` - These are the sources/targets that will be the subject of
  testing.  Defined as a dictionary, the keys are a short-hand for referencing
  the specific data subjects indicated in the values.


With testing, we're specifying how a data transformation is supposed
to behave under certain conditions.  Typically, we're focused on how
subtle variations in the values of fields in the sources affect the
values of fields in the targets.  Each of these subtle variations
defines a **Case** that was mentioned above.  Now, it would be
possible to have the tester execute the runner for every case that
needed to be tested.  However, this could result in exceedingly slow
tests, particularly when the overhead of loading data and executing a
process is high (like it is for in-database transformations, and even
more so for Apache Spark).  Therefore, Pemi testing was built to only
execute the runner once for each scenario, regardless of how many
cases are defined. This can only work if the records of the targets
can be associated with a particular case in which the conditions and
expectations are defined.

This brings us to **Case Keys**.  The case_keys argument in the
scenario setup needs to be a generator that yields a dictionary.  The
keys of this dictionary are the targets of the scenario.  The values
are also a dictionary, but they are a dictionary where the keys are
the names of fields and the values are a value for the field that
**must be unique across all cases**.  In this case, we define the
`case_keys` generator as
```python
    def case_keys():
        scooby_ids = pemi.data.UniqueIdGenerator('scooby-{}'.format)
        while True:
            scooby_id = next(scooby_ids)
            yield {
                'input': {'id': scooby_id},
                'output': {'id': scooby_id}
            }
```

`pemi.data.UniqueIdGenerator` is a class used to help build generators
that can conform to any format needed.  Although in this case, a
simple incrementing integer would also suffice.

#### Column-Oriented Tests
With the hard part out of the way, we can now define our first test
case.  Cases are also defined using a context-manager pattern.  To test
that the salutations are behaving correctly we could write
```python
    with scenario.case('Populating salutation') as case:
        case.when(
            pt.when.source_conforms_to_schema(scenario.sources['input']),
            pt.when.source_has_keys(scenario.sources['input'], scenario.case_keys),
            pt.when.source_field_has_value(scenario.sources['input'], 'name', 'Dawn')
        ).then(
            pt.then.target_field_has_value(scenario.targets['output'], 'salutation', 'Hello Dawn')
        )
```
The conditions set up the data to be tested:
* `pt.when.source_conforms_to_schema` loads dummy data into the source
  called 'input', and uses the schema to determine the valid values
  that can be used.
* `pt.when.source_has_keys` uses the `scenario.case_keys` generator to
  populate the `id` field of `input` with unique values that will also
  be used to collect the target and relate that to this case.
* `pt.when.source_field_has_value` sets up the `name` field of the
  source data to have the value `Dawn`.

The expectations are then:
* `pt.then.target_field_has_value` the target field `salutations` on
  the output has the value `Hello Dawn`.  If we were to modify this
  value to be `Goodbye Dawn, don't let any vampires bite you neck`, then
  the test would fail.

This style of testing is referred to as "Column-Oriented" because we're only focused
on the values of particular columns.  We do not care about how the individual records
are ordered or related to one another.

#### Row-Oriented Tests
Column-oriented tests are not always sufficient to describe data
transformations.  Sometimes we care about how rows are related.  For
example, we might need to describe how to drop duplicate records, or
how to join two data sources together.  To that end, we can write
"Row-Oriented" tests.  While the example we are working with here
doesn't have any row operations, we can still write a test case that
highlights how it can work.
```python
    with scenario.case('Dealing with many records') as case:
        ex_input = pemi.data.Table(
            '''
            | id       | name  |
            | -        | -     |
            | {sid[1]} | Spike |
            | {sid[2]} | Angel |
            '''.format(
                sid=scenario.case_keys.cache('input', 'id')
            )
        )

        ex_output = pemi.data.Table(
            '''
            | id       | salutation  |
            | -        | -           |
            | {sid[1]} | Hello Spike |
            | {sid[2]} | Hello Angel |
            '''.format(
                sid=scenario.case_keys.cache('output', 'id')
            )
        )

        case.when(
            pt.when.source_conforms_to_schema(scenario.sources['input']),
            pt.when.example_for_source(scenario.sources['input'], ex_input)
        ).then(
            pt.then.target_matches_example(scenario.targets['output'], ex_output)
        )
```

In this case, we set up two data tables to show how the output records
are related to the input records.  Using examples built with
`pemi.data.Table`, we can focus the test case on just those fields
that we care about.  If we had a source that had 80 fields in it, we
would only need to define those that we care about for this particular
test.  Pemi will use the schema defined for that source to fill in the
other fields with dummy data.

In the above example we use `scenario.case_keys.cache` to reference
the unique identifiers that group a set of records into a case.  In
`ex_input`, `{sid[1]}` will evaluate to some value specified by the
case key generator (e.g., `scooby-9` or `scooby-12`, etc.).  However,
when `{sid[1]}` is referenced in the `ex_output`, it will use the same
value that was generated for the `ex_input`.

A complete version of this test can be found in
[tests/test_readme.py](tests/test_readme.py).  Other good examples can
be found in [tests/test_testing.py](tests/test_testing.py) and
[tests/test_job.py](tests/test_job.py).

#### Running Tests

Pemi tests require that the pytest package be installed in your
project.  Furthermore, you'll need to tell pytest that you want to use
pemi tests by added the following to your `conftest.py`:
```python
import pemi
pytest_plugins = ['pemi.pytest']
```


## Tutorial

* TODO: Build an example repo that uses Pemi and has a full job
* TODO: Build a full integration job based off of the CSV job
* TODO: Guide for writing your own data subjects
* TODO: Guide for writing custom test conditions
* TODO: Row-focused tests vs column-focused tests

## Future

* Streaming - I would like to be able to support streaming data subjects (like Kafka).
* Auto-documentation - The testing framework should be able to support building
  documentation by collecting test scenario and case definitions.  Documents could be built
  to look something like Gherkin.

## Contributing to Pemi Development

Clone this repository to somewhere on your local development machine.

Download and install the [docker community edition](https://www.docker.com/)
that is appropriate for your host OS.

We recommend using [invoke](http://www.pyinvoke.org/) to setup and
control the environment for developing and testing Pemi.  This will
require that you install invoke in your host OS.  You may be able to
get away with just running `pip install invoke`.  However, the
recommended method is to download and install
[miniconda](https://conda.io/miniconda.html).  Then, create a de-jobs
specific environment and install invoke in this environment:

    conda create --name pemi python=3.6
    source activate pemi
    pip install invoke

**Note**: if you use miniconda, you will have to run `source activate pemi`
each time you start a new terminal session.

Copy the `example.env` file to `.env`.  This will allow you to customize
certain development-specific variables.

Once invoke is installed, you can run spin up the development and testing environments with

    inv up

And then run all of the tests with

    inv test

(when debugging, I like to use `inv test --pytest="-s -vv --color=yes --tb=short tests"`)

The development environment includes a [Jupyter](http://jupyter.org/)
server with JupyterLab, which you can access by visiting
[http://localhost:8890/lab](http://localhost:8890/lab) (the port can
be configured in `.env`).
