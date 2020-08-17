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

To disable prediction,::

    ceph config set global device_failure_prediction_mode none


The local predictor module requires at least six datasets of device health metrics to implement the prediction.
Run the following command to retrieve the life expectancy of given device.

::

    ceph device predict-life-expectancy <device id>


Debugging
=========

If you want to debug the DiskPrediction module mapping to Ceph logging level,
use the following command.

::

    [mgr]

        debug mgr = 20

With logging set to debug for the manager the module will print out logging
message with prefix *mgr[diskprediction]* for easy filtering.

