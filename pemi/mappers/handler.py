from collections import OrderedDict

class Handler:
    def __init__(self, mode='catch', recode=None):
        # modes: raise, catch, recode, warn

        self.mode = mode
        self.recode = recode
        self.errors = OrderedDict()

        self.apply_func = getattr(self, '_{}'.format(mode))

    def apply(self, func, row_arg, idx):
        try:
            return func(row_arg)
        except Exception as err:
            return self.apply_func(err, row_arg, idx)


    def mapping_error(self, err, idx):
        return {'mode': self.mode, 'index': idx, 'type': err.__class__.__name__, 'message': str(err)}

    def _raise(self, err, row_arg, idx):
        print(mapping_error(err, idx))
        raise err

    def _recode(self, err, row_arg, idx):
        self.errors[idx] = self.mapping_error(err, idx)
        return self.recode(row_arg)

    def _warn(self, err, row_arg, idx):
        print('WARNING: {}'.format(self.mapping_error(err, idx)))
        return None

    def _catch(self, err, row_arg, idx):
        self.errors[idx] = self.mapping_error(err, idx)
        return None
