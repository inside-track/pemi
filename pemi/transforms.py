# TODO: concatenate, prefix, postfix, nvl, ifblank, blankif

def validate_no_null(field):
    def _validate(value):
        if value == field.null:
            raise ValueError("null is not allowed for field '{}'".format(field.name))
        return value
    return _validate
