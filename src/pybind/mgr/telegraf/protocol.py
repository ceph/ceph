from telegraf.utils import format_string, format_value


class Line(object):
    def __init__(self, measurement, values, tags=None, timestamp=None):
        self.measurement = measurement
        self.values = values
        self.tags = tags
        self.timestamp = timestamp

    def get_output_measurement(self):
        return format_string(self.measurement)

    def get_output_values(self):
        if not isinstance(self.values, dict):
            metric_values = {'value': self.values}
        else:
            metric_values = self.values

        sorted_values = sorted(metric_values.items())
        sorted_values = [(k, v) for k, v in sorted_values if v is not None]

        return u','.join(u'{0}={1}'.format(format_string(k), format_value(v)) for k, v in sorted_values)

    def get_output_tags(self):
        if not self.tags:
            self.tags = dict()

        sorted_tags = sorted(self.tags.items())

        return u','.join(u'{0}={1}'.format(format_string(k), format_string(v)) for k, v in sorted_tags)

    def get_output_timestamp(self):
        return ' {0}'.format(self.timestamp) if self.timestamp else ''

    def to_line_protocol(self):
        tags = self.get_output_tags()

        return u'{0}{1} {2}{3}'.format(
            self.get_output_measurement(),
            "," + tags if tags else '',
            self.get_output_values(),
            self.get_output_timestamp()
        )
