class VirtualColumn(object):
    def __init__(self, **args):
        if args['type'] == 'expression':
            self.virtual_column = {
                'type': 'expression',
                'name': args['name'],
                'expression': args['expression'],
                'outputType': args['outputType']
            }
        else:
            raise NotImplemented(
                'VirtualColumn type: {0} does not exist'.format(args['type']))

    @staticmethod
    def build_virtual_column(val):
        return val.virtual_column
