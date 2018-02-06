from collections import OrderedDict

class Schema:
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
            merged_fields[name] = type(field)(**merged_field_metadata)

        return Schema(**merged_fields)

    def rename(self, mapper):
        new_fields = []
        for field in self.fields.values():
            new_field = type(field)(mapper.get(field.name, field.name), **field.metadata)
            new_fields.append(new_field)
        return Schema(*new_fields)

    def select(self, func):
        'Returns a new schema with the fields selected via a function (func) of the field'
        return Schema(**{name:field for name, field in self.items() if func(field)})
