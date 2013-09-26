def gen_args(name, args):
    usage = [""]
    usage += [name + ':']
    usage += \
        ["    {key}: <{usage}> ({default})".format(
            key=key, usage=_usage, default=default)
         for (key, _usage, default, _) in args]
    usage.append('')
    usage.append(name + ':')
    usage += \
        ["    {key}: {default}".format(
                key = key, default = default)
         for (key, _, default, _) in args]
    usage = '\n'.join('    ' + i for i in usage)
    def ret(config):
        class Object(object): pass
        obj = Object()
        for (key, usage, default, conv) in args:
            if key in config:
                setattr(obj, key, conv(config[key]))
            else:
                setattr(obj, key, conv(default))
        return obj
    return usage, ret

def argify(name, args):
    (usage, config_func) = gen_args(name, args)
    def ret1(f):
        def ret2(**kwargs):
            config = kwargs.get('config', {})
            if config is None:
                config = {}
            kwargs['config'] = config_func(config)
            return f(**kwargs)
        ret2.__doc__ = f.__doc__ + usage
        return ret2
    return ret1
