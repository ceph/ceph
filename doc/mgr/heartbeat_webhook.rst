heartbeat_webhook module
========================

This module is designed to be used with a heartbeat style 
webhook receiver. This allows for very simple and robust 
state monitoring.

This module will send a webhook to the specified
webhook URL once per interval when the cluster is in a 
``HEALTH_OK`` state. In the case that the webhook stops being
received, the monitoring service should trigger an alert. 

This module was tested against 
`Dead Mans Snitch <https://deadmanssnitch.com>`_, but it should
work for any similar service (such as Robodash, Cronitor, 
Alerta heartbeats, etc.)

Configuration
-------------

For this module to function, a webhook URL must be specified::

    ceph config set mgr mgr/heartbeat_webhook/webhook_url "https://example.com/webhook"

Below is a complete list of configuration options:

.. list-table:: Configuration Options
   :header-rows: 1

   * - Name
     - Description
     - Type
     - Default Value
     - Required
   * - webhook_url
     - Url to which HTTP POST requests are sent
     - string
     - ''
     - true
   * - interval
     - Time between webhooks
     - seconds
     - 60
     - talse
   * - send_on_warn
     - When true, webhooks will also be sent when the cluster is ``HEALTH_WARN``
     - bool
     - false
     - false

Commands
--------

heartbeat_webhook exposes one command for testing
the configured webhook receiver::

    ceph heartbeat_webhook test

This will send a test webhook and return the heartbeat code 
and body of the response.
