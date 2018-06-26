Pemi Changelog
==============

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
