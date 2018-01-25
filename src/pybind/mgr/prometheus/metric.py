import copy
import math

import static_metrics


def get_metrics_spec():
    """
    Load the metrics configuration, and build a spec representing the set of
    metrics we'll use.
    """
    metrics_spec = copy.deepcopy(static_metrics.groups)
    for name, group in metrics_spec.items():
        templates = group.pop('templates', dict())
        defaults = group.pop('defaults', dict())
        for name, item in group.get('metrics', dict()).items():
            apply_defaults(defaults, item)
            apply_templates(templates, name, item)
            if 'name' not in item:
                item['name'] = name
    return metrics_spec


def apply_defaults(defaults, item):
    for name, value in defaults.items():
        if name not in item:
            item[name] = value
    return item


def apply_templates(templates, item_name, item):
    for name, value in templates.items():
        new_value = value.format(item_name)
        if name not in item:
            item[name] = new_value
    return item


class Metric(object):
    def __init__(self, mtype, name, desc, labels=None):
        self.mtype = mtype
        self.name = name
        self.desc = desc
        self.labelnames = labels    # tuple if present
        self.value = dict()         # indexed by label values

    def set(self, value, labelvalues=None):
        # labelvalues must be a tuple
        labelvalues = labelvalues or ('',)
        self.value[labelvalues] = value

    def str_expfmt(self):

        def promethize(path):
            ''' replace illegal metric name characters '''
            result = path.replace('.', '_').replace('+', '_plus')\
                .replace('::', '_')

            # Hyphens usually turn into underscores, unless they are
            # trailing
            if result.endswith("-"):
                result = result[0:-1] + "_minus"
            else:
                result = result.replace("-", "_")

            return "ceph_{0}".format(result)

        def floatstr(value):
            ''' represent as Go-compatible float '''
            if value == float('inf'):
                return '+Inf'
            if value == float('-inf'):
                return '-Inf'
            if math.isnan(value):
                return 'NaN'
            return repr(float(value))

        name = promethize(self.name)
        expfmt = '''
# HELP {name} {desc}
# TYPE {name} {mtype}'''.format(
            name=name,
            desc=self.desc,
            mtype=self.mtype,
        )

        for labelvalues, value in self.value.items():
            if self.labelnames:
                labels = zip(self.labelnames, labelvalues)
                labels = ','.join('%s="%s"' % (k, v) for k, v in labels)
            else:
                labels = ''
            if labels:
                fmtstr = '\n{name}{{{labels}}} {value}'
            else:
                fmtstr = '\n{name} {value}'
            expfmt += fmtstr.format(
                name=name,
                labels=labels,
                value=floatstr(value),
            )
        return expfmt
