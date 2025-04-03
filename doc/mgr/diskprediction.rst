.. _diskprediction:

=====================
Diskprediction Module
=====================

The *diskprediction* module leverages Ceph device health check to collect disk health metrics and uses internal predictor module to produce the disk failure prediction and returns back to Ceph. It doesn't require any external server for data analysis and output results. Its internal predictor's accuracy is around 70%.

Enabling
========

Run the following command to enable the *diskprediction_local* module in the Ceph
environment::

    ceph mgr module enable diskprediction_local


To enable the local predictor::

    ceph config set global device_failure_prediction_mode local

To disable prediction::

    ceph config set global device_failure_prediction_mode none


*diskprediction_local* requires at least six datasets of device health metrics to
make prediction of the devices' life expectancy. And these health metrics are
collected only if health monitoring is :ref:`enabled <enabling-monitoring>`.

Run the following command to retrieve the life expectancy of given device.

::

    ceph device predict-life-expectancy <device id>

Configuration
=============

The module performs the prediction on a daily basis by default. You can adjust
this interval with::

  ceph config set mgr mgr/diskprediction_local/predict_interval <interval-in-seconds>

Debugging
=========

If you want to debug the DiskPrediction module mapping to Ceph logging level,
use the following command.

::

    [mgr]

        debug mgr = 20

With logging set to debug for the manager the module will print out logging
message with prefix *mgr[diskprediction]* for easy filtering.

