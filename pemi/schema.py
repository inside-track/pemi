import pemi.field

from collections import OrderedDict

class Schema:
    def __init__(self, schema={}):
        if schema.__class__ == Schema:
            self.fields = schema.fields
        else:
            self.fields = self.__fields_from_dict(schema)


    def __fields_from_dict(self, schema_dict):
        fields_dict = OrderedDict()

        for name, meta in schema_dict.items():
            fields_dict[name] = pemi.field.ftypes[meta['type']](name, **meta)
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
