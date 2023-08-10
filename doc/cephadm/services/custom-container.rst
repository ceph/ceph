========================
Custom Container Service
========================

The orchestrator enables custom containers to be deployed using a YAML file.
A corresponding :ref:`orchestrator-cli-service-spec` must look like:

.. code-block:: yaml

    service_type: container
    service_id: foo
    placement:
        ...
    spec:
      image: docker.io/library/foo:latest
      entrypoint: /usr/bin/foo
      uid: 1000
      gid: 1000
      args:
        - "--net=host"
        - "--cpus=2"
      ports:
        - 8080
        - 8443
      envs:
        - SECRET=mypassword
        - PORT=8080
        - PUID=1000
        - PGID=1000
      volume_mounts:
        CONFIG_DIR: /etc/foo
      bind_mounts:
        - ['type=bind', 'source=lib/modules', 'destination=/lib/modules', 'ro=true']
      dirs:
        - CONFIG_DIR
      files:
        CONFIG_DIR/foo.conf:
          - refresh=true
          - username=xyz
          - "port: 1234"

where the properties of a service specification are:

* ``service_id``
    A unique name of the service.
* ``image``
    The name of the Docker image.
* ``uid``
    The UID to use when creating directories and files in the host system.
* ``gid``
    The GID to use when creating directories and files in the host system.
* ``entrypoint``
    Overwrite the default ENTRYPOINT of the image.
* ``args``
    A list of additional Podman/Docker command line arguments.
* ``ports``
    A list of TCP ports to open in the host firewall.
* ``envs``
    A list of environment variables.
* ``bind_mounts``
    When you use a bind mount, a file or directory on the host machine
    is mounted into the container. Relative `source=...` paths will be
    located below `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``volume_mounts``
    When you use a volume mount, a new directory is created within
    Docker’s storage directory on the host machine, and Docker manages
    that directory’s contents. Relative source paths will be located below
    `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``dirs``
    A list of directories that are created below
    `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
* ``files``
    A dictionary, where the key is the relative path of the file and the
    value the file content. The content must be double quoted when using
    a string. Use '\\n' for line breaks in that case. Otherwise define
    multi-line content as list of strings. The given files will be created
    below the directory `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.
    The absolute path of the directory where the file will be created must
    exist. Use the `dirs` property to create them if necessary.
* ``init_containers``
    A list of "init container" definitions. An init container exists to
    run prepratory steps before the primary container starts. Init containers
    are optional. One or more container can be defined.
    Each definition can contain the following fields:

    * ``image``
        The name of the container image. If left unspecified, the init
        container will inherit the image value from the top level spec.
    * ``entrypoint``
        Customize the default entrypoint of the image.
    * ``entrypoint_args``
        Arguments that will be passed to the entrypoint. Behaves the same
        as the generic ``extra_entrypoint_args`` field.
    * ``volume_mounts``
        Same as the Custom Container spec's ``volume_mounts`` - selects what
        volumes will be mounted into the init container. If left unspecified,
        the init container will inherit the primary container's value(s).
    * ``envs``
        A list of environment variables.
    * ``privileged``
        A boolean indicate if the container should run with privileges or not. If
        left unspecified, the init container will inherit the primary container's
        value.


Example with init containers:

.. code-block:: yaml

    service_type: container
    service_id: foo
    placement:
        ...
    spec:
      image: quay.io/example/foosystem:latest
      entrypoint: /usr/bin/foo
      uid: 1000
      gid: 1000
      ports:
        - 8889
      dirs:
        - CONFIG_DIR
        - DATA_DIR
      volume_mounts:
        CONFIG_DIR: /etc/foo
        DATA_DIR: /var/lib/foo
      files:
        CONFIG_DIR/foo.conf:
          - db_path=/var/lib/foo/db
      init_containers:
        - image: quay.io/example/curly:howard
          entrypoint: bash
          entrypoint_args:
            - argument: "-c"
            - argument: "[ -f /var/lib/foo/db ] || curl -o /var/lib/foo/sample.dat https://foo.example.com/samples/1.dat"
          volume_mounts:
            DATA_DIR: /var/lib/foo
        - entrypoint: /usr/bin/foo-initialize-db
          entrypoint_args:
            - "--option=threads=8"
        - entrypoint: /usr/local/bin/import-sample-datasets.sh
          entrypoint_args:
            - "/var/lib/foo/sample.dat"
          envs:
            - FOO_SOURCE_MISSING=ignore
            - FOO_CLEANUP=yes


.. note:: Init containers are currently implemented as a step that runs
   before the service is started and is subject to start-up timeouts.
   The total run time of all init containers can not exceed 200 seconds
   or the service will fail to start.
