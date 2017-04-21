import decimal
import json
import datetime

class ConversionError(ValueError): pass
class RequiredValueError(ValueError): pass

class Field:
    def __init__(self, name=None, ftype='object', required=False, **metadata):
        self.name = name
        self.ftype = ftype
        self.required = required
        self.null = None
        self.metadata = {'ftype': ftype, 'required': required, **metadata}


    def _is_required(self, value):
        if self.required and not value:
            raise RequiredValueError('Missing required value for {}'.format(self.name))
        else:
            return value

    def in_converter(self, value):
        if not self._is_required(value):
            return self.null
        else:
            return self._type_converter(value)

    def _type_converter(self, value):
        return value

    def __getitem__(self, key):
        return self.metadata[key]

    def __str__(self):
        return self.__dict__.__str__()

class StringField(Field):
    def __init__(self, name=None, length=None, **metadata):
        super().__init__(name, 'string', **metadata)
        self.length = length
        self.null = ''
        self.metadata = {'length': length, **metadata}

    def _type_converter(self, value):
        return str(value)

class IntegerField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name, 'integer', **metadata)

    def _type_converter(self, value):
        return int(value)

class DateField(Field):
    def __init__(self, name=None, in_format='%Y-%m-%d', out_format='%Y-%m-%d',**metadata):
        super().__init__(name, 'date', **metadata)
        self.in_format = in_format
        self.out_format = out_format
        self.metadata = {'in_format': in_format, 'out_format': out_format, **metadata}

    def _type_converter(self, value):
        return datetime.datetime.strptime(value, self.in_format).date()

class DateTimeField(Field):
    def __init__(self, name=None, in_format='%Y-%m-%d %H:%M:%S', out_format='%Y-%m-%d %H:%M:%S',**metadata):
        super().__init__(name, 'datetime', **metadata)
        self.in_format = in_format
        self.out_format = out_format
        self.metadata = {'in_format': in_format, 'out_format': out_format, **metadata}
        self.pd_converter = self.in_converter

    def _type_converter(self, value):
        return datetime.datetime.strptime(value, self.in_format)

class FloatField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name, 'float', **metadata)

    def _type_converter(self, value):
        return float(value)

class DecimalField(Field):
    def __init__(self, name=None, precision=None, scale=None, **metadata):
        super().__init__(name, 'decimal', **metadata)
        self.precision = precision
        self.scale = scale
        self.metadata = {'precision': precision, 'scale': scale, **metadata}

    def _type_converter(self, value):
        dec = decimal.Decimal(value)

        detected_precision = len(dec.as_tuple().digits)
        detected_scale = -dec.as_tuple().exponent

        if detected_precision > self.precision:
            raise ConversionError('Decimal conversion error for "{}".  Expected precision: {}, Actual precision: {}'.format(
                value, self.precision, detected_precision
            ))
        if detected_scale > self.scale:
            raise ConversionError('Decimal conversion error for "{}".  Expected scale: {}, Actual scale: {}'.format(
                value, self.scale, detected_scale
            ))

        return dec

class BooleanField(Field):
    # TODOC: Point out that if unknown_truthiness is set, then that is used when no matching value is found (see tests)
    def __init__(self, name=None, map_true=[], map_false=[], **metadata):
        super().__init__(name, 'boolean', **metadata)

        self.true_values = ['t','true','y','yes','on','1'] + map_true
        self.false_values = ['f','false','n','no','off','0'] + map_false

    def _type_converter(self, value):
        if value.lower() in self.true_values:
            return True
        elif value.lower() in self.false_values:
            return False
        elif 'unknown_truthiness' in self.metadata:
            return self.metadata['unknown_truthiness']
        elif not value:
            None
        else:
            raise ConversionError('Unable to convert "{}" to a boolean'.format(value))


class JsonField(Field):
    def __init__(self, name=None, **metadata):
        super().__init__(name, 'json', **metadata)

    def _type_converter(self, value):
        return json.loads(value)

ftypes = {
   'object': Field,
   'string': StringField,
   'integer': IntegerField,
   'date': DateField,
   'datetime': DateTimeField,
   'float': FloatField,
   'decimal': DecimalField,
   'boolean': BooleanField,
   'json': JsonField
}
