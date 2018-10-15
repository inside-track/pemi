Pemi Changelog
==============

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
