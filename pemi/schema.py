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
            init_meta = {k:v for (k,v) in meta.items() if k != 'ftype'}
            fields_dict[name] = pemi.field.ftypes[meta['ftype']](name, **init_meta)
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

    def merge(self, other):
        merged_fields = {**self.fields, **other.fields}
        for name, field in merged_fields.items():
            if name in self.fields:
                self_meta = self.fields[name].metadata
            else:
                self_meta = {}

            if name in other.fields:
                other_meta = other.fields[name].metadata
            else:
                other_meta = {}

            merged_field_metadata = {**self_meta, **other_meta}
            merged_fields[name] = merged_field_metadata

        return Schema(merged_fields)
