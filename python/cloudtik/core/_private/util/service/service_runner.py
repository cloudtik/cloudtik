import json


class ServiceRunner:
    def __init__(self):
        self.stop_event = None

    def run(self):
        pass


def cmd_args_to_call_args(cmd_args):
    args = []
    kwargs = {}
    if not cmd_args:
        return args, kwargs
    for arg in cmd_args:
        if arg.count('=') >= 1:
            key, value = arg.split('=', 1)
        else:
            key, value = None, arg
        try:
            value = json.loads(value)
        except ValueError:
            pass
        if key:
            kwargs[key] = value
        else:
            args.append(value)
    return args, kwargs
