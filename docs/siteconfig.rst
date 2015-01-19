.. _site_config:

Site and Client Configuration
=============================

Teuthology requires several configuration options to be set, and provides many other optional ones. They are looked for in ``~/.teuthology.yaml`` if it exists, or ``/etc/teuthology.yaml`` if it doesn't.

Here is a sample configuration with many of the options set and documented::

    # lab_domain: the domain name to append to all short hostnames
    lab_domain: example.com

    # The root directory to use for storage of all scheduled job logs and 
    # other data.
    archive_base: /home/teuthworker/archive

    # The default machine_type value to use when not specified. Currently 
    # only used by teuthology-suite.
    default_machine_type: awesomebox

    # The host and port to use for the beanstalkd queue. This is required 
    # for scheduled jobs.
    queue_host: localhost
    queue_port: 11300

    # The URL of the lock server (paddles). This is required for scheduled 
    # jobs.
    lock_server: http://paddles.example.com:8080/

    # The URL of the results server (paddles).
    results_server: http://paddles.example.com:8080/

    # This URL of the results UI server (pulpito). You must of course use 
    # paddles for pulpito to be useful.
    results_ui_server: http://pulpito.example.com/

    # Email address that will receive job results summaries.
    results_email: ceph-qa@example.com

    # Email address that job results summaries originate from
    results_sending_email: teuthology@example.com

    # How long (in seconds) teuthology-results should wait for jobs to finish
    # before considering them 'hung'
    results_timeout: 43200

    # Gitbuilder archive that stores e.g. ceph packages
    gitbuilder_host: gitbuilder.example.com

    # Where all git repos are considered to reside.
    ceph_git_base_url: https://github.com/ceph/

    # Where teuthology and ceph-qa-suite repos should be stored locally
    src_base_path: /home/foo/src

    # Whether or not teuthology-suite, when scheduling, should update 
    # itself from git. This is disabled by default.
    automated_scheduling: false

    # How often, in seconds, teuthology-worker should poll its child job 
    # processes
    watchdog_interval: 120

    # How long a scheduled job should be allowed to run, in seconds, before 
    # it is killed by the worker process.
    max_job_time: 259200
