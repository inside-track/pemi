import copy

from collections import OrderedDict

class Schema:
    '''
    A schema is a thing.

    '''
    def __init__(self, *args, **kwargs):
        self.fields = OrderedDict()
        for field in args:
            self.fields[field.name] = field

        for name, field in kwargs.items():
            field.name = name
            self.fields[name] = field

    def __getitem__(self, key):
        if isinstance(key, list):
            return Schema(**{self[k].name: self[k] for k in key})
        return self.fields[key]

    def keys(self):
        return list(self.fields.keys())

    def values(self):
        return list(self.fields.values())

    def items(self):
        return self.fields.items()

    def coercions(self):
        return {f.name: f.coerce for f in self.fields.values()}

    def __str__(self):
        return "\n".join(
            ['{} -> {}'.format(name, meta.__str__()) for name, meta in self.fields.items()]
        )

    def __repr__(self):
        return '<{}({}):\n{}\n>'.format(self.__class__.__name__, id(self), self.__str__())

    def __eq__(self, other):
        return self.fields == other.fields

    def __contains__(self, key):
        return key in self.fields

    def __len__(self):
        return len(self.fields)

    def merge(self, other):
        merged_fields = {**self.fields, **other.fields}

        for name, field in merged_fields.items():
            if name in self.fields:
                self_meta = copy.deepcopy(self.fields[name].metadata)
            else:
                self_meta = {}

            if name in other.fields:
                other_meta = copy.deepcopy(other.fields[name].metadata)
            else:
                other_meta = {}

            merged_field_metadata = {**self_meta, **other_meta}
            merged_fields[name] = type(field)(**merged_field_metadata)

        return Schema(**merged_fields)

    def copy(self):
        return copy.deepcopy(self)

    def rename(self, mapper):
        new_fields = []
        for field in self.fields.values():
            new_field = copy.deepcopy(field)
            new_field.name = mapper.get(field.name, field.name)
            new_fields.append(new_field)
        return Schema(*new_fields)

    def metapply(self, elem, func):
        '''
        Allows one to create/modify metadata elements using a function

        Args:
            elem (str): Name of the metadata element to create or modify
            func (func): Function that accepts a single ``pemi.Field`` argument and returns
              the value of the metadata element indicated by ``elem``

        Returns:
            pemi.Schema: A new ``pemi.Schema`` with the updated metadata

        Example:
            Suppose we wanted to add some metadata to a schema that will be used to
            construct a SQL statement::

                pemi.schema.Schema(
                    id=StringField(),
                    name=StringField()
                ).metapply(
                    'sql',
                    lambda field: 'students.{} AS student_{}'.format(field.name, field.name)
                )
        '''

        new_schema = self.copy()
        for _, field in new_schema.items():
            field.metadata[elem] = func(field)
        return new_schema

    def select(self, func):
        'Returns a new schema with the fields selected via a function (func) of the field'
        return Schema(**{name:copy.deepcopy(field) for name, field in self.items() if func(field)})
