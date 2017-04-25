from collections import OrderedDict

import pemi.field
from pemi.field import Field

class FieldExistsError(KeyError): pass

class Schema:
    def __init__(self, *field_tuples):
        self.fields = self.__fields_from_tuples(field_tuples)

    def __fields_from_tuples(self, field_tuples):
        fields_dict = OrderedDict()
        for field in field_tuples:
            name = field[0]
            ftype = field[1]
            metadata = field[2]

            if name in fields_dict:
                raise FieldExistsError("Field '{}' already defined".format(name))

            fields_dict[name] = pemi.field.ftypes[ftype](name, **metadata)
        return fields_dict

    def __getitem__(self, key):
        return self.fields[key]

    def keys(self):
        return self.fields.keys()

    def values(self):
        return self.fields.values()

    def items(self):
        return self.fields.items()

    def in_converters(self):
        return {f.name: f.in_converter for f in self.fields.values()}

    def str_converters(self):
        return {f.name: str for f in self.fields.values()}

    def __str__(self):
        return "\n".join(['{} -> {}'.format(name, meta.__str__()) for name, meta in self.fields.items()])
