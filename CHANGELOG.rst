Pemi Changelog
==============

v0.5.7
------

* Add columns to empty remainders of PdForkPipe.
* Resolves a bug where tests using SaDataSubjects were
  falsely passing the target_has_fields test.

v0.5.6
------

* Reverted creating a parent_pipe reference, which was causing pickling issues.

v0.5.5
------

* Raise an exception if duplicate case/scenario names are used
* New option for data subject connections that will raise an error if schemas fields don't
  match up with dataframe fields.
* Allow embedded # in data tables


v0.5.4
------

* Resolves an issue pickling Pemi fields.

v0.5.3
------

* Improved pipe connection graphing.
* Source has(have) value(s) will now coerce values according to the field types before
  sending data to pipe to be tested.  This may result in some tests that falsely pass
  to now start raising errors.
* Fixed a pytest warning error following pytest upgrade.


v0.5.2
------

* Relaxes some packages requirements.

v0.5.1
------

* Decimal faker now honors precision/scale when faking data.

v0.5.0
------

* **BREAKING** Substantial refactoring of the way that test scenarios are configured.  See docs
  for details.

v0.4.3
------

* Adds PemiTabular methods that will create an HTML file when tests fail.  This can
  help with debugging failed tests.

v0.4.0
------

* **BREAKING** Allows one to make more natural connections to nested pipes.  The sources
  of a pipe can now be connected to the sources of nested pipes and the targets of
  nested pipes can now be connected to the targets of the parent pipe.  This will break
  code that has workarounds for this issue.
* Prevents Pandas from raising a warning when concatenating CSV files

v0.3.2.1
--------
* pemi.transforms.isblank now treats pandas.NaT as blank

v0.3.2
------
* pemi.transforms.isblank no longer raises an error when given an array or dict with
  more than 1 element.

v0.3.1
------
* Allow CSV and SqlAlchemy pipes to work without defining a Pemi schema

v0.3.0
------
* Removed ``fake_with`` option when constructing test data tables.  Custom data fakers should
  now be specified in schema field metadata field ``faker``, which expects a function that
  when called will return faked data.
* Replaced internal pd_mapper with pandas-mapper package.  Users will have to migrate
  to pandas-mapper if they were using PdMapper/PdMap functionality in Pemi.
* Removed support for ``allow_null`` metadata.  Never used in production.
* Upgraded many package dependencies.
* Removed various unused features.
