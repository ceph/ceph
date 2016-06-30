from rest_framework import serializers
from rest_framework import fields


if False:
    class BooleanField(serializers.BooleanField):
        """
        Version of BooleanField which handles fields which are 1,0
        """
        def to_native(self, value):
            if isinstance(value, int) and value in [0, 1]:
                return bool(value)
            else:
                super(BooleanField, self).to_native(value)
else:
    # rest-framework 3 booleanfield handles 0, 1
    BooleanField = fields.BooleanField


if False:
    class UuidField(serializers.CharField):
        """
        For strings like Ceph service UUIDs and Ceph cluster FSIDs
        """
        type_name = "UuidField"
        type_label = "uuid string"
else:
    # rest-framework 3 has built in uuid field.
    UuidField = fields.UUIDField

if False:
    class EnumField(serializers.CharField):
        def __init__(self, mapping, *args, **kwargs):
            super(EnumField, self).__init__(*args, **kwargs)
            self.mapping = mapping
            self.reverse_mapping = dict([(v, k) for (k, v) in self.mapping.items()])
            if self.help_text:
                self.help_text += " (one of %s)" % ", ".join(self.mapping.values())

        def from_native(self, value):
            return self.reverse_mapping.get(value, value)

        def to_native(self, value):
            return self.mapping.get(value, value)
else:
    #rest-framework 3 has ChoiceField
    EnumField = fields.ChoiceField
