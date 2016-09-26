
from distutils.version import StrictVersion

import rest_framework
from rest_framework import serializers
import rest.app.serializers.fields as fields
from rest.app.types import CRUSH_RULE_TYPE_REPLICATED, \
    CRUSH_RULE_TYPE_ERASURE, USER_REQUEST_COMPLETE, \
    USER_REQUEST_SUBMITTED, OSD_FLAGS, severity_str, SEVERITIES


class ValidatingSerializer(serializers.Serializer):
    @property
    def init_data(self):
        """
        Compatibility alias for django rest framework 2 vs. 3
        """
        return self.initial_data

    def is_valid(self, http_method):
        if StrictVersion(rest_framework.__version__) < StrictVersion("3.0.0"):
            self._errors = super(ValidatingSerializer, self).errors or {}
        else:
            # django rest framework >= 3 has different is_Valid prototype
            # than <= 2
            super(ValidatingSerializer, self).is_valid(False)

        if self.init_data is not None:
            if http_method == 'POST':
                self._errors.update(
                    self.construct_errors(self.Meta.create_allowed,
                                          self.Meta.create_required,
                                          self.init_data.keys(),
                                          http_method))

            elif http_method in ('PATCH', 'PUT'):
                self._errors.update(
                    self.construct_errors(self.Meta.modify_allowed,
                                          self.Meta.modify_required,
                                          self.init_data.keys(),
                                          http_method))
            else:
                self._errors.update([[http_method, 'Not a valid method']])

        return not self._errors

    def construct_errors(self, allowed, required, init_data, action):
        errors = {}

        not_allowed = set(init_data) - set(allowed)
        errors.update(
            dict([x, 'Not allowed during %s' % action] for x in not_allowed))

        required = set(required) - set(init_data)
        errors.update(
            dict([x, 'Required during %s' % action] for x in required))

        return errors

    def get_data(self):
        # like http://www.django-rest-framework.org/api-guide/serializers#dynamically-modifying-fields
        filtered_data = {}
        for field, value in self.init_data.iteritems():
            filtered_data[field] = self.data[field]

        return filtered_data


class PoolSerializer(ValidatingSerializer):
    class Meta:
        fields = ('name', 'id', 'size', 'pg_num', 'crush_ruleset', 'min_size',
                  'crash_replay_interval', 'crush_ruleset',
                  'pgp_num', 'hashpspool', 'full', 'quota_max_objects',
                  'quota_max_bytes')
        create_allowed = ('name', 'pg_num', 'pgp_num', 'size', 'min_size',
                          'crash_replay_interval', 'crush_ruleset',
                          'quota_max_objects', 'quota_max_bytes', 'hashpspool')
        create_required = ('name', 'pg_num')
        modify_allowed = ('name', 'pg_num', 'pgp_num', 'size', 'min_size',
                          'crash_replay_interval', 'crush_ruleset',
                          'quota_max_objects', 'quota_max_bytes', 'hashpspool')
        modify_required = ()

    # Required in creation
    name = serializers.CharField(required=False, source='pool_name',
                                 help_text="Human readable name of the pool, may"
                                           "change over the pools lifetime at user request.")
    pg_num = serializers.IntegerField(required=False,
                                      help_text="Number of placement groups in this pool")

    # Not required in creation, immutable
    id = serializers.CharField(source='pool', required=False,
                               help_text="Unique numeric ID")

    # May be set in creation or updates
    size = serializers.IntegerField(required=False,
                                    help_text="Replication factor")
    min_size = serializers.IntegerField(required=False,
                                        help_text="Minimum number of replicas required for I/O")
    crash_replay_interval = serializers.IntegerField(required=False,
                                                     help_text="Number of seconds to allow clients to "
                                                               "replay acknowledged, but uncommitted requests")
    crush_ruleset = serializers.IntegerField(required=False,
                                             help_text="CRUSH ruleset in use")
    # In 'ceph osd pool set' it's called pgp_num, but in 'ceph osd dump' it's called
    # pg_placement_num :-/
    pgp_num = serializers.IntegerField(source='pg_placement_num',
                                       required=False,
                                       help_text="Effective number of placement groups to use when calculating "
                                                 "data placement")

    # This is settable by 'ceph osd pool set' but in 'ceph osd dump' it only appears
    # within the 'flags' integer.  We synthesize a boolean from the flags.
    hashpspool = serializers.BooleanField(required=False,
                                          help_text="Enable HASHPSPOOL flag")

    # This is synthesized from ceph's 'flags' attribute, read only.
    full = serializers.BooleanField(required=False,
                                    help_text="True if the pool is full")

    quota_max_objects = serializers.IntegerField(required=False,
                                                 help_text="Quota limit on object count (0 is unlimited)")
    quota_max_bytes = serializers.IntegerField(required=False,
                                               help_text="Quota limit on usage in bytes (0 is unlimited)")


class OsdSerializer(ValidatingSerializer):
    class Meta:
        fields = ('uuid', 'up', 'in', 'id', 'reweight', 'server', 'pools',
                  'valid_commands', 'public_addr', 'cluster_addr')
        create_allowed = ()
        create_required = ()
        modify_allowed = ('up', 'in', 'reweight')
        modify_required = ()

    id = serializers.IntegerField(read_only=True, source='osd',
                                  help_text="ID of this OSD within this cluster")
    uuid = fields.UuidField(read_only=True,
                            help_text="Globally unique ID for this OSD")
    up = fields.BooleanField(required=False,
                             help_text="Whether the OSD is running from the point of view of the rest of the cluster")
    _in = fields.BooleanField(required=False,
                              help_text="Whether the OSD is 'in' the set of OSDs which will be used to store data")
    reweight = serializers.FloatField(required=False,
                                      help_text="CRUSH weight factor")
    server = serializers.CharField(read_only=True,
                                   help_text="FQDN of server this OSD was last running on")
    pools = serializers.ListField(
        help_text="List of pool IDs which use this OSD for storage",
        required=False)
    valid_commands = serializers.CharField(read_only=True,
                                           help_text="List of commands that can be applied to this OSD")

    public_addr = serializers.CharField(read_only=True,
                                        help_text="Public/frontend IP address")
    cluster_addr = serializers.CharField(read_only=True,
                                         help_text="Cluster/backend IP address")


class OsdConfigSerializer(ValidatingSerializer):
    class Meta:
        fields = OSD_FLAGS
        create_allowed = ()
        create_required = ()
        modify_allowed = OSD_FLAGS
        modify_required = ()

    pause = serializers.BooleanField(
        help_text="Disable IO requests to all OSDs in cluster", required=False)
    noup = serializers.BooleanField(
        help_text="Prevent OSDs from automatically getting marked as Up by the monitors. This setting is useful for troubleshooting",
        required=False)
    nodown = serializers.BooleanField(
        help_text="Prevent OSDs from automatically getting marked as Down by the monitors. This setting is useful for troubleshooting",
        required=False)
    noout = serializers.BooleanField(
        help_text="Prevent Down OSDs from being marked as out", required=False)
    noin = serializers.BooleanField(
        help_text="Prevent OSDs from booting OSDs from being marked as IN. Will cause cluster health to be set to WARNING",
        required=False)
    nobackfill = serializers.BooleanField(
        help_text="Disable backfill operations on cluster", required=False)
    norecover = serializers.BooleanField(
        help_text="Disable replication of Placement Groups", required=False)
    noscrub = serializers.BooleanField(
        help_text="Disables automatic periodic scrub operations on OSDs. May still be initiated on demand",
        required=False)
    nodeepscrub = serializers.BooleanField(
        help_text="Disables automatic periodic deep scrub operations on OSDs. May still be initiated on demand",
        required=False)


class CrushRuleSerializer(serializers.Serializer):
    class Meta:
        fields = (
        'id', 'name', 'ruleset', 'type', 'min_size', 'max_size', 'steps',
        'osd_count')

    id = serializers.IntegerField(source='rule_id')
    name = serializers.CharField(source='rule_name',
                                 help_text="Human readable name")
    ruleset = serializers.IntegerField(
        help_text="ID of the CRUSH ruleset of which this rule is a member")
    type = fields.EnumField({CRUSH_RULE_TYPE_REPLICATED: 'replicated',
                             CRUSH_RULE_TYPE_ERASURE: 'erasure'},
                            help_text="Data redundancy type")
    min_size = serializers.IntegerField(
        help_text="If a pool makes more replicas than this number, CRUSH will NOT select this rule")
    max_size = serializers.IntegerField(
        help_text="If a pool makes fewer replicas than this number, CRUSH will NOT select this rule")
    steps = serializers.ListField(
        help_text="List of operations used to select OSDs")
    osd_count = serializers.IntegerField(
        help_text="Number of OSDs which are used for data placement")


class CrushRuleSetSerializer(serializers.Serializer):
    class Meta:
        fields = ('id', 'rules')

    id = serializers.IntegerField()
    rules = CrushRuleSerializer(many=True)


class RequestSerializer(serializers.Serializer):
    class Meta:
        fields = (
        'id', 'state', 'error', 'error_message', 'headline', 'status',
        'requested_at', 'completed_at')

    id = serializers.CharField(
        help_text="A globally unique ID for this request")
    state = serializers.CharField(
        help_text="One of '{complete}', '{submitted}'".format(
            complete=USER_REQUEST_COMPLETE, submitted=USER_REQUEST_SUBMITTED))
    error = serializers.BooleanField(
        help_text="True if the request completed unsuccessfully")
    error_message = serializers.CharField(
        help_text="Human readable string describing failure if ``error`` is True")
    headline = serializers.CharField(
        help_text="Single sentence human readable description of the request")
    status = serializers.CharField(
        help_text="Single sentence human readable description of the request's current "
                  "activity, if it has more than one stage.  May be null.")
    requested_at = serializers.DateTimeField(
        help_text="Time at which the request was received by calamari server")
    completed_at = serializers.DateTimeField(
        help_text="Time at which the request completed, may be null.")


class ServiceSerializer(serializers.Serializer):
    class Meta:
        fields = ('type', 'id')

    type = serializers.CharField()
    id = serializers.CharField()


class ServerSerializer(serializers.Serializer):
    class Meta:
        fields = ('hostname', 'services', 'ceph_version')

    # Identifying information
    hostname = serializers.CharField(help_text="Domain name")

    ceph_version = serializers.CharField(
        help_text="The version of Ceph installed."
    )
    services = ServiceSerializer(many=True,
                                 help_text="List of Ceph services seen"
                                           "on this server")


class ConfigSettingSerializer(serializers.Serializer):
    class Meta:
        fields = ('key', 'value')

    # This is very simple for now, but later we may add more things like
    # schema information, allowed values, defaults.

    key = serializers.CharField(help_text="Name of the configuration setting")
    value = serializers.CharField(
        help_text="Current value of the setting, as a string")


class MonSerializer(serializers.Serializer):
    class Meta:
        fields = ('name', 'rank', 'in_quorum', 'server', 'addr')

    name = serializers.CharField(help_text="Human readable name")
    rank = serializers.IntegerField(
        help_text="Unique of the mon within the cluster")
    in_quorum = serializers.BooleanField(
        help_text="True if the mon is a member of current quorum")
    server = serializers.CharField(
        help_text="Hostname of server running the OSD")
    addr = serializers.CharField(help_text="IP address of monitor service")
    leader = serializers.BooleanField(
        help_text="True if this monitor is the leader of the quorum. False otherwise")


class CliSerializer(serializers.Serializer):
    class Meta:
        fields = ('out', 'err', 'status')

    out = serializers.CharField(help_text="Standard out")
    err = serializers.CharField(help_text="Standard error")
    status = serializers.IntegerField(help_text="Exit code")


# Declarative metaclass definitions are great until you want
# to use a reserved word
if StrictVersion(rest_framework.__version__) < StrictVersion("3.0.0"):
    # In django-rest-framework 2.3.x (Calamari used this)
    OsdSerializer.base_fields['in'] = OsdSerializer.base_fields['_in']
    OsdConfigSerializer.base_fields['nodeep-scrub'] = \
    OsdConfigSerializer.base_fields['nodeepscrub']
else:
    OsdSerializer._declared_fields['in'] = OsdSerializer._declared_fields[
        '_in']
    del OsdSerializer._declared_fields['_in']
    OsdSerializer._declared_fields['in'].source = "in"
    OsdConfigSerializer._declared_fields['nodeep-scrub'] = \
    OsdConfigSerializer._declared_fields['nodeepscrub']


