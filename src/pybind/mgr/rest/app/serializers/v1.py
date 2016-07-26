
from django.contrib.auth.models import User
from django.utils import dateformat

from rest_framework import serializers
import dateutil.parser


def to_unix(t):
    if t is None:
        return None
    return int(dateformat.format(t, 'U')) * 1000


class ClusterSerializer(serializers.Serializer):
    class Meta:
        fields = ('cluster_update_time', 'cluster_update_time_unix', 'id', 'name')

    cluster_update_time = serializers.SerializerMethodField('get_update_time')
    name = serializers.Field()
    id = serializers.Field()

    # FIXME: we should not be sending out time in two formats: if API consumers want
    # unix timestamps they can do the conversion themselves.
    cluster_update_time_unix = serializers.SerializerMethodField('get_update_time_unix')

    def get_update_time(self, obj):
        return obj.update_time

    def get_update_time_unix(self, obj):
        update_time = dateutil.parser.parse(obj.update_time)
        return to_unix(update_time)

    # NB calamari 1.0 had cluster_atttempt_time, which no longer makes sense
    # because we're listening for events, not polling.  TODO: expunge from GUI code.


class UserSerializer(serializers.ModelSerializer):
    """
    Serializer for the Django User model.

    Used to expose a django-rest-framework user management resource.
    """
    class Meta:
        model = User
        fields = ('id', 'username', 'password', 'email')

    def to_native(self, obj):
        # Before conversion, remove the password field. This prevents the hash
        # from being displayed when requesting user details.
        if 'password' in self.fields:
            del self.fields['password']
        return super(UserSerializer, self).to_native(obj)

    def restore_object(self, attrs, instance=None):
        user = super(UserSerializer, self).restore_object(attrs, instance)
        if user:
            # This will perform the Django-specific password obfuscation
            user.set_password(attrs['password'])
        return user


class ClusterSpaceSerializer(serializers.Serializer):
    space = serializers.Field()

    class Meta:
        fields = ('space',)


class ClusterHealthSerializer(serializers.Serializer):
    report = serializers.Field()

    class Meta:
        fields = ('report', 'cluster_update_time', 'cluster_update_time_unix')

    # FIXME: should not be copying this field onto health counters etc, clients should get
    # it by querying the cluster directly.
    cluster_update_time = serializers.Field()
    cluster_update_time_unix = serializers.SerializerMethodField('get_cluster_update_time_unix')

    def get_cluster_update_time_unix(self, obj):
        update_time = dateutil.parser.parse(obj.cluster_update_time)
        return to_unix(update_time)


class ClusterHealthCountersSerializer(serializers.Serializer):
    pg = serializers.SerializerMethodField('get_pg')
    mds = serializers.SerializerMethodField('get_mds')
    mon = serializers.SerializerMethodField('get_mon')
    osd = serializers.SerializerMethodField('get_osd')

    class Meta:
        fields = ('pg', 'mds', 'mon', 'osd', 'cluster_update_time', 'cluster_update_time_unix')

    def get_pg(self, obj):
        return obj.counters['pg']

    def get_mds(self, obj):
        return obj.counters['mds']

    def get_mon(self, obj):
        return obj.counters['mon']

    def get_osd(self, obj):
        return obj.counters['osd']

    # FIXME: should not be copying this field onto health counters etc, clients should get
    # it by querying the cluster directly.
    cluster_update_time = serializers.Field()
    cluster_update_time_unix = serializers.SerializerMethodField('get_cluster_update_time_unix')

    def get_cluster_update_time_unix(self, obj):
        update_time = dateutil.parser.parse(obj.cluster_update_time)
        return to_unix(update_time)


class OSDDetailSerializer(serializers.Serializer):
    class Meta:
        # FIXME: should just be returning the OSD as the object
        fields = ('osd',)

    osd = serializers.Field()


class OSDListSerializer(serializers.Serializer):
    # TODO: the OSD list resource should just return a list, so that
    # this serializer class isn't necessary
    osds = serializers.Field()
    pg_state_counts = serializers.SerializerMethodField('get_pg_state_counts')

    def get_pg_state_counts(self, obj):
        return dict((s, len(v)) for s, v in obj.osds_by_pg_state.iteritems())

    class Meta:
        fields = ('osds', 'pg_state_counts')


class PoolSerializer(serializers.Serializer):
    class Meta:
        fields = ('pool_id', 'name', 'quota_max_bytes', 'quota_max_objects', 'used_objects', 'used_bytes', 'id', 'cluster')

    id = serializers.IntegerField()
    cluster = serializers.CharField()
    pool_id = serializers.IntegerField()
    name = serializers.CharField()
    quota_max_bytes = serializers.IntegerField()
    quota_max_objects = serializers.IntegerField()
    used_objects = serializers.IntegerField()
    used_bytes = serializers.IntegerField()


class ServiceStatusSerializer(serializers.Serializer):
    class Meta:
        fields = ('type', 'service_id', 'name')

    type = serializers.SerializerMethodField('get_type')
    service_id = serializers.SerializerMethodField('get_service_id')
    name = serializers.SerializerMethodField('get_name')

    def get_type(self, obj):
        return obj['id'][1]

    def get_service_id(self, obj):
        return obj['id'][2]

    def get_name(self, obj):
        return "%s.%s" % (self.get_type(obj), self.get_service_id(obj))


class ServerSerializer(serializers.Serializer):
    class Meta:
        fields = ('addr', 'hostname', 'name', 'services')

    services = ServiceStatusSerializer(source='services', many=True)

    addr = serializers.SerializerMethodField('get_addr')
    hostname = serializers.CharField()
    name = serializers.SerializerMethodField('get_name')

    def get_name(self, obj):
        return obj.hostname

    def get_addr(self, obj):
        return obj.fqdn


class InfoSerializer(serializers.Serializer):
    class Meta:
        fields = ('version', 'license', 'registered', 'hostname', 'fqdn', 'ipaddr', 'bootstrap_url', 'bootstrap_rhel',
                  'bootstrap_ubuntu')

    version = serializers.CharField(help_text="Calamari server version")
    license = serializers.CharField(help_text="Calamari license metadata")
    registered = serializers.CharField(help_text="Calamari registration metadata")
    hostname = serializers.CharField(help_text="Hostname of Calamari server")
    fqdn = serializers.CharField(help_text="Fully qualified domain name of Calamari server")
    ipaddr = serializers.CharField(help_text="IP address of Calamari server")
    bootstrap_url = serializers.CharField(help_text="URL to minion bootstrap script")
    bootstrap_rhel = serializers.CharField(help_text="Minion bootstrap command line for Red Hat systems")
    bootstrap_ubuntu = serializers.CharField(help_text="Minion bootstrap command line for Ubuntu systems")
