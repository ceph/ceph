from django.conf.urls import patterns, url, include
from rest_framework import routers
import rest.app.views.v2

router = routers.DefaultRouter(trailing_slash=False)

urlpatterns = patterns(
    '',

    # This has to come after /user/me to make sure that special case is handled
    url(r'^', include(router.urls)),

    # About ongoing operations in cthulhu
    url(r'^request/(?P<request_id>[a-zA-Z0-9-]+)/cancel$',
        rest.app.views.v2.RequestViewSet.as_view({'post': 'cancel'}),
        name='request-cancel'),
    url(r'^request/(?P<request_id>[a-zA-Z0-9-]+)$',
        rest.app.views.v2.RequestViewSet.as_view({'get': 'retrieve'}),
        name='request-detail'),
    url(r'^request$',
        rest.app.views.v2.RequestViewSet.as_view({'get': 'list'}),
        name='request-list'),
    url(r'^cluster/request/(?P<request_id>[a-zA-Z0-9-]+)$',
        rest.app.views.v2.RequestViewSet.as_view({'get': 'retrieve'}),
        name='cluster-request-detail'),
    url(r'^cluster/request$',
        rest.app.views.v2.RequestViewSet.as_view({'get': 'list'}),
        name='cluster-request-list'),

    # OSDs, Pools, CRUSH
    url(r'^cluster/crush_rule_set$',
        rest.app.views.v2.CrushRuleSetViewSet.as_view({'get': 'list'}),
        name='cluster-crush_rule_set-list'),
    url(r'^cluster/crush_rule$',
        rest.app.views.v2.CrushRuleViewSet.as_view({'get': 'list'}),
        name='cluster-crush_rule-list'),
    url(r'^cluster/pool$', rest.app.views.v2.PoolViewSet.as_view(
        {'get': 'list', 'post': 'create'}),
        name='cluster-pool-list'),
    url(r'^cluster/pool/(?P<pool_id>\d+)$',
        rest.app.views.v2.PoolViewSet.as_view({
            'get': 'retrieve',
            'patch': 'update',
            'delete': 'destroy'}),
        name='cluster-pool-detail'),

    url(r'^cluster/osd$',
        rest.app.views.v2.OsdViewSet.as_view({'get': 'list'}),
        name='cluster-osd-list'),
    url(r'^cluster/osd/(?P<osd_id>\d+)$',
        rest.app.views.v2.OsdViewSet.as_view(
            {'get': 'retrieve', 'patch': 'update'}),
        name='cluster-osd-detail'),
    url(r'^cluster/osd /command$', rest.app.views.v2.OsdViewSet.as_view(
        {'get': 'get_implemented_commands'})),
    url(r'^cluster/osd/(?P<osd_id>\d+)/command$',
        rest.app.views.v2.OsdViewSet.as_view(
            {'get': 'get_valid_commands'})),

    url(r'^cluster/osd/(?P<osd_id>\d+)/command/(?P<command>[a-zA-Z_]+)$',
        rest.app.views.v2.OsdViewSet.as_view(
            {'get': 'validate_command', 'post': 'apply'})),
    url(r'^cluster/osd_config$',
        rest.app.views.v2.OsdConfigViewSet.as_view(
            {'get': 'osd_config', 'patch': 'update'})),

    url(r'^cluster/mon$',
        rest.app.views.v2.MonViewSet.as_view({'get': 'list'}),
        name='cluster-mon-list'),
    url(r'^cluster/mon/(?P<mon_id>[a-zA-Z0-9-\.]+)$',
        rest.app.views.v2.MonViewSet.as_view(
            {'get': 'retrieve'}), name='cluster-mon-detail'),

    # Direct access to SyncObjects, mainly for debugging
    url(r'^cluster/sync_object$',
        rest.app.views.v2.SyncObject.as_view({'get': 'describe'}),
        name='cluster-sync-object-describe'),
    url(r'^cluster/sync_object/(?P<sync_type>[a-zA-Z0-9-_]+)$',
        rest.app.views.v2.SyncObject.as_view({'get': 'retrieve'}),
        name='cluster-sync-object'),
    url(r'^server/(?P<fqdn>[a-zA-Z0-9-\.]+)/debug_job',
        rest.app.views.v2.DebugJob.as_view({'post': 'create'}),
        name='server-debug-job'),

    url(r'^cluster/server$',
        rest.app.views.v2.ServerViewSet.as_view({'get': 'list'}),
        name='cluster-server-list'),
    url(r'^cluster/server/(?P<fqdn>[a-zA-Z0-9-\.]+)$',
        rest.app.views.v2.ServerViewSet.as_view(
            {'get': 'retrieve'}), name='cluster-server-detail'),

    # Ceph configuration settings
    url(r'^cluster/config$',
        rest.app.views.v2.ConfigViewSet.as_view({'get': 'list'})),
    url(r'^cluster/config/(?P<key>[a-zA-Z0-9_]+)$',
        rest.app.views.v2.ConfigViewSet.as_view({'get': 'retrieve'})),
)
