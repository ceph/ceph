.. _diskprediction:

=====================
Diskprediction Module
=====================

The ``diskprediction`` module leverages Ceph device health checks to collect
disk health metrics and uses the internal predictor module to produce disk
failure predictions and returns them back to Ceph. It requires no external
server for data analysis and the outputting of results. Its internal
predictor's accuracy is around 70%.

Enabling
========

Run the following command to enable the ``diskprediction_local`` module in the
Ceph environment:

.. prompt:: bash #

   ceph mgr module enable diskprediction_local


Run the following command to enable the local predictor:

.. prompt:: bash #

   ceph config set global device_failure_prediction_mode local

Run the following command to disable prediction:

.. prompt:: bash #

   ceph config set global device_failure_prediction_mode none


``diskprediction_local`` requires at least six datasets of device health
metrics to make prediction of the devices' life expectancy. And these health
metrics are collected only if health monitoring is :ref:`enabled
<enabling-monitoring>`.

Run the following command to retrieve the life expectancy of a given device:

.. prompt:: bash #

   ceph device predict-life-expectancy <device id>

Configuration
=============

The module performs the prediction on a daily basis by default. Adjust this
interval by running a command of the following form:

.. prompt:: bash #

   ceph config set mgr mgr/diskprediction_local/predict_interval <interval-in-seconds>

Debugging
=========

To debug the DiskPrediction module mapping to Ceph logging level,
use the following command.

::

    [mgr]

        debug mgr = 20

With logging set to ``debug`` for the Manager, the module will print logging
messages with the prefix ``mgr[diskprediction]``. This facilitates filtering. 

