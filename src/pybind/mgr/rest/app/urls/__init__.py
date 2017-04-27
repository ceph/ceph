from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns(
    '',

    # These views are needed for the django-rest-framework debug interface
    # to be able to log in and out.  The URL path doesn't matter, rest_framework
    # finds the views by name.
    url(r'^api/rest_framework/', include('rest_framework.urls', namespace='rest_framework')),

    url(r'^api/v2/', include('rest.app.urls.v2')),
)

#handler500 = 'calamari_web.views.server_error'
