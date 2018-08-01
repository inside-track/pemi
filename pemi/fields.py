import decimal
import datetime
import json

import dateutil

import pemi.transforms

__all__ = [
    'StringField',
    'IntegerField',
    'FloatField',
    'DateField',
    'DateTimeField',
    'BooleanField',
    'DecimalField',
    'JsonField'
]


class CoercionError(ValueError): pass
class DecimalCoercionError(ValueError): pass

def convert_exception(fun):
    def wrapper(self, value):
        try:
            coerced = fun(self, value)
        except Exception as err:
            raise CoercionError('Unable to coerce value "{}" to {}: {}: {}'.format(
                value,
                self.__class__.__name__,
                err.__class__.__name__,
                err
            ))
        return coerced
    return wrapper

#pylint: disable=too-few-public-methods
class Field:
    '''
    A field is a thing that is inherited
    '''

    def __init__(self, name=None, **metadata):
        self.name = name
        self.metadata = metadata

        default_metadata = {'null': None}
        self.metadata = {**default_metadata, **metadata}
        self.null = self.metadata['null']

    @convert_exception
    def coerce(self, value):
        raise NotImplementedError

    def __str__(self):
        return '<{} {}>'.format(self.__class__.__name__, self.__dict__.__str__())

    def __eq__(self, other):
        return type(self) is type(other) \
            and self.metadata == other.metadata \
            and self.name == other.name


class StringField(Field):
    def __init__(self, name=None, **metadata):
        metadata['null'] = metadata.get('null', '')
        super().__init__(name=name, **metadata)

    @convert_exception
    def coerce(self, value):
        if pemi.transforms.isblank(value):
            return self.null
        return str(value).strip()


class IntegerField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name=name, **metadata)
        self.coerce_float = self.metadata.get('coerce_float', False)

    @convert_exception
    def coerce(self, value):
        if pemi.transforms.isblank(value):
            return self.null
        if self.coerce_float:
            return int(float(value))
        return int(value)


class FloatField(Field):
    @convert_exception
    def coerce(self, value):
        if pemi.transforms.isblank(value):
            return self.null
        return float(value)


class DateField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name=name, **metadata)
        self.format = self.metadata.get('format', '%Y-%m-%d')
        self.infer_format = self.metadata.get('infer_format', False)


    @convert_exception
    def coerce(self, value):
        if hasattr(value, 'strip'):
            value = value.strip()

        if pemi.transforms.isblank(value):
            return self.null
        return self.parse(value)

    def parse(self, value):
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        if not self.infer_format:
            return datetime.datetime.strptime(value, self.format).date()
        return dateutil.parser.parse(value).date()

class DateTimeField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name=name, **metadata)
        self.format = self.metadata.get('format', '%Y-%m-%d %H:%M:%S')
        self.infer_format = self.metadata.get('infer_format', False)


    @convert_exception
    def coerce(self, value):
        if hasattr(value, 'strip'):
            value = value.strip()

        if pemi.transforms.isblank(value):
            return self.null
        return self.parse(value)

    def parse(self, value):
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime.combine(value, datetime.time.min)
        if not self.infer_format:
            return datetime.datetime.strptime(value, self.format)
        return dateutil.parser.parse(value)


class BooleanField(Field):
    # when defined, the value of unknown_truthiness is used when no matching is found
    def __init__(self, name=None, **metadata):
        super().__init__(name=name, **metadata)

        self.true_values = self.metadata.get(
            'true_values',
            ['t', 'true', 'y', 'yes', 'on', '1']
        )
        self.false_values = self.metadata.get(
            'false_values',
            ['f', 'false', 'n', 'no', 'off', '0']
        )

    @convert_exception
    def coerce(self, value):
        if hasattr(value, 'strip'):
            value = value.strip()

        if isinstance(value, bool):
            return value
        if pemi.transforms.isblank(value):
            return self.null
        return self.parse(value)

    def parse(self, value):
        value = str(value).lower()
        if value in self.true_values:
            return True
        if value in self.false_values:
            return False
        if 'unknown_truthiness' in self.metadata:
            return self.metadata['unknown_truthiness']
        raise ValueError('Not a boolean value')

class DecimalField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name=name, **metadata)
        self.precision = self.metadata.get('precision', 16)
        self.scale = self.metadata.get('scale', 2)
        self.truncate_decimal = self.metadata.get('truncate_decimal', False)
        self.enforce_decimal = self.metadata.get('enforce_decimal', True)

    @convert_exception
    def coerce(self, value):
        if pemi.transforms.isblank(value):
            return self.null
        return self.parse(value)

    def parse(self, value):
        dec = decimal.Decimal(str(value))

        if dec != dec: #pylint: disable=comparison-with-itself
            return dec

        if self.truncate_decimal:
            dec = round(dec, self.scale)

        if self.enforce_decimal:
            detected_precision = len(dec.as_tuple().digits)
            detected_scale = -dec.as_tuple().exponent

            if detected_precision > self.precision:
                msg = ('Decimal coercion error for "{}".  ' \
                    + 'Expected precision: {}, Actual precision: {}').format(
                        dec, self.precision, detected_precision
                    )
                raise DecimalCoercionError(msg)
            if detected_scale > self.scale:
                msg = ('Decimal coercion error for "{}".  ' \
                    + 'Expected scale: {}, Actual scale: {}').format(
                        dec, self.scale, detected_scale
                    )
                raise DecimalCoercionError(msg)

        return dec

class JsonField(Field):
    @convert_exception
    def coerce(self, value):
        if pemi.transforms.isblank(value):
            return self.null

        try:
            return json.loads(value)
        except TypeError:
            return value

#pylint: enable=too-few-public-methods
