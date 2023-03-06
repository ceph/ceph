===============================================================================
A Detailed Documentation on How to Set up Ceph Kerberos Authentication
===============================================================================

This document provides details on the Kerberos authorization protocol. This is
the 1st draft and we will try to keep it updated along with code changes that
might take place.

Several free implementations of this protocol are available (MIT, Heimdal,
MS...), covering a wide range of operating systems. The Massachusetts
Institute of Technology (MIT), where Kerberos was originally developed,
continues to develop their Kerberos package and it is the implementation we
chose to work with. `MIT Kerberos <http://web.mit.edu/Kerberos/>`_.

Please, provide feedback to Daniel Oliveira (doliveira@suse.com)

*Last update: Dec 3, 2018*

|

Background
----------

Before we get into *Kerberos details*, let us define a few terms so we can
understand what to expect from it, *what it can and can't do*:

Directory Services
    A directory service is a customizable information store that functions as
    a single point from which users can locate resources and services
    distributed throughout the network. This customizable information store
    also gives administrators a single point for managing its objects and their
    attributes. Although this information store appears as a single point to
    the users of the network, it is actually most often stored in a distributed
    form. A directory service consists of at least one *Directory Server and a
    Directory Client* and are implemented based on *X.500 standards*.

    *OpenLDAP, 389 Directory Server, MS Active Directory, NetIQ eDirectory* are
    some good examples.

    A directory service is often characterized as a *write-once-read-many-times
    service*, meaning the data that would normally be stored in an directory
    service would not be expected to change on every access.

    The database that forms a directory service *is not designed for
    transactional data*.

|

LDAP (Lightweight Directory Access Protocol v3)
    LDAP is a set of LDAP Protocol Exchanges *(not an implementation of a
    server)* that defines the method by which data is accessed. LDAPv3 is a
    standard defined by the IETF in RFC 2251 and describes how data is
    represented in the Directory Service (the Data Model or DIT).

    Finally, it defines how data is loaded into (imported) and saved from
    (exported) a directory service (using LDIF). LDAP does not define how data
    is stored or manipulated. Data Store is an 'automagic' process as far as
    the standard is concerned and is generally handled by back-end modules.

    No Directory Service implementation has all the features of LDAP v3
    protocol implemented. All Directory Server implementations have their
    different problems and/or anomalies, and features that may not return
    results as another Directory Server implementation would.

|

Authentication
    Authentication is about validating credentials (like User Name/ID and
    password) to verify the identity. The system determines whether one is what
    they say they are using their credentials.

    Usually, authentication is done by a username and password, and sometimes
    in conjunction with *(single, two, or multi) factors of authentication*,
    which refers to the various ways to be authenticated.

|

Authorization
    Authorization occurs after the identity is successfully authenticated by
    the system, which ultimately gives one full permission to access the
    resources such as information, files, databases, and so forth, almost
    anything. It determines the ability to access the system and up to what
    extent (what kind of permissions/rights are given and to where/what).

|

Auditing
    Auditing takes the results from both *authentication and authorization* and
    records them into an audit log. The audit log records all actions
    taking by/during the authentication and authorization for later review by
    the administrators. While authentication and authorization are preventive
    systems (in which unauthorized access is prevented), auditing is a reactive
    system (in which it gives detailed log of how/when/where someone accessed
    the environment).

|

Kerberos (KRB v5)
    Kerberos is a network *authentication protocol*. It is designed to provide
    strong authentication for client/server applications by using secret-key
    cryptography (symmetric key). A free implementation of this protocol is
    available from the MIT. However, Kerberos is available in many commercial
    products as well.

    It was designed to provide secure authentication to services over an
    insecure network. Kerberos uses tickets to authenticate a user, or service
    application and never transmits passwords over the network in the clear.
    So both client and server can prove their identity without sending any
    unencrypted secrets over the network.

    Kerberos can be used for single sign-on (SSO). The idea behind SSO is
    simple, we want to login just once and be able to use any service that we
    are entitled to, without having to login on each of those services.

|

Simple Authentication and Security Layer (SASL)
    SASL **(RFC 4422)** is a framework that helps developers to implement
    different authentication mechanisms (implementing a series of challenges
    and responses), allowing both clients and servers to negotiate a mutually
    acceptable mechanism for each connection, instead of hard-coding them.

    Examples of SASL mechanisms:

        * ANONYMOUS **(RFC 4505)**

            - For guest access, meaning *unauthenticated*

        * CRAM-MD5 **(RFC 2195)**

            - Simple challenge-response scheme based on *HMAC-MD5*.
              It does not establish any security layer. *Less secure than
              DIGEST-MD5 and GSSAPI.*

        * DIGEST-MD5 **(RFC 2831)**

            - HTTP Digest compatible *(partially)* challenge-response scheme
              based upon MD5, offering a *data security layer*. It is preferred
              over PLAIN text passwords, protecting against plain text attacks.
              It is a mandatory authentication method for LDAPv3 servers.

        * EXTERNAL **(RFCs 4422, 5246, 4301, 2119)**

            - Where *authentication is implicit* in the context (i.e; for
              protocols using IPsec or TLS [TLS/SSL to performing certificate-
              based authentication] already). This method uses public keys for
              strong authentication.

        * GS2 **(RFC 5801)**

            - Family of mechanisms supports arbitrary GSS-API mechanisms in
              SASL

        * NTLM (MS Proprietary)

            - MS Windows NT LAN Manager authentication mechanism

        * OAuth 1.0/2.0 **(RFCs 5849, 6749, 7628)**

            - Authentication protocol for delegated resource access

        * OTP **(RFC 2444)**

            - One-time password mechanism *(obsoletes the SKEY mechanism)*

        * PLAIN **(RFC 4616)**

            - Simple Cleartext password mechanism **(RFC 4616)**. This is not a
              preferred mechanism for most applications because of its relative
              lack of strength.

        * SCRAM **(RFCs 5802, 7677)**

            - Modern challenge-response scheme based mechanism with channel
              binding support

|

Generic Security Services Application Program Interface (GSSAPI)
    GSSAPI **(RFCs 2078, 2743, 2744, 4121, 4752)** is widely used by protocol
    implementers as a way to implement Kerberos v5 support in their
    applications. It provides a generic interface and message format that can
    encapsulate authentication exchanges from any authentication method that
    has a GSSAPI-compliant library.

    It does not define a protocol, authentication, or security mechanism
    itself; it instead makes it easier for application programmers to support
    multiple authentication mechanisms by providing a uniform, generic API for
    security services. It is a set of functions that include both an API and a
    methodology for approaching authentication, aiming to insulate application
    protocols from the specifics of security protocols as much as possible.

    *Microsoft Windows Kerberos* implementation does not include GSSAPI support
    but instead includes a *Microsoft-specific API*, the *Security Support
    Provider Interface (SSPI)*. In Windows, an SSPI client can communicate with
    a *GSSAPI server*.

    *Most applications that support GSSAPI also support Kerberos v5.*

|

Simple and Protected GSSAPI Negotiation Mechanism (SPNEGO)
    As we can see, GSSAPI solves the problem of providing a single API to
    different authentication mechanisms. However, it does not solve the problem
    of negotiating which mechanism to use. In fact for GSSAPI to work, the two
    applications communicating with each other must know in advance what
    authentication mechanism they plan to use, which usually is not a problem
    if only one mechanism is supported (meaning Kerberos v5).

    However, if there are multiple mechanisms to choose from, a method is
    needed to securely negotiate an authentication mechanism that is mutually
    supported between both client and server; which is where
    *SPNEGO (RFC 2478, 4178)* makes a difference.

    *SPNEGO* provides a framework for two parties that are engaged in
    authentication to select from a set of possible authentication mechanisms,
    in a manner that preserves the opaque nature of the security protocols to
    the application protocol that uses it.

    It is a security protocol that uses a *GSSAPI authentication mechanism* and
    negotiates among several available authentication mechanisms in an
    implementation, selecting one for use to satisfy the authentication needs
    of the application protocol.

    It is a *meta protocol* that travels entirely in other application
    protocols; it is never used directly without an application protocol.

|

*Why is this important and why do we care? Like, at all?*

    Having this background information in mind, we can easily describe things
    like:

        1. *Ceph Kerberos authentication* is based totally on MIT *Kerberos*
        implementation using *GSSAPI*.

        2. At the moment we are still using *Kerberos default backend
        database*, however we plan on adding LDAP as a backend which would
        provide us with *authentication with GSSAPI (KRB5)* and *authorization
        with LDAP (LDAPv3)*, via *SASL mechanism*.

|

Before We Start
---------------

We assume the environment already has some external services up and running
properly:

    * Kerberos needs to be properly configured, which also means (for both
      every server and KDC):

        - Time Synchronization (either using `NTP <http://www.ntp.org/>`_  or `chrony <https://chrony.tuxfamily.org/>`_).

            + Not only Kerberos, but also Ceph depends and relies on time
              synchronization.

        - DNS resolution

            + Both *(forward and reverse)* zones, with *fully qualified domain
              name (fqdn)* ``(hostname + domain.name)``

            + KDC discover can be set up to use DNS ``(srv resources)`` as
              service location protocol *(RFCs 2052, 2782)*, as well as *host
              or domain* to the *appropriate realm* ``(txt record)``.

            + Even though these DNS entries/settings are not required to run a
              ``Kerberos realm``, they certainly help to eliminate the need for
              manual configuration on all clients.

            + This is extremely important, once most of the Kerberos issues are
              usually related to name resolution. Kerberos is very picky when
              checking on systems names and host lookups.

    * Whenever possible, in order to avoid a *single point of failure*, set up
      a *backup, secondary, or slave*, for every piece/part in the
      infrastructure ``(ntp, dns, and kdc servers)``.


Also, the following *Kerberos terminology* is important:

    * Ticket

        - Tickets or Credentials, are a set of information that can be used to
          verify the client's identity. Kerberos tickets may be stored in a
          file, or they may exist only in memory.

        - The first ticket obtained is a ticket-granting ticket (TGT), which
          allows the clients to obtain additional tickets. These additional
          tickets give the client permission for specific services. The
          requesting and granting of these additional tickets happens
          transparently.

            + The TGT, which expires at a specified time, permits the client to
              obtain additional tickets, which give permission for specific
              services. The requesting and granting of these additional tickets
              is user-transparent.

    * Key Distribution Center (KDC).

        - The KDC creates a ticket-granting ticket (TGT) for the client,
          encrypts it using the client's password as the key, and sends the
          encrypted TGT back to the client. The client then attempts to decrypt
          the TGT, using its password. If the client successfully decrypts the
          TGT (i.e., if the client gave the correct password), it keeps the
          decrypted TGT, which indicates proof of the client's identity.

        -  The KDC is comprised of three components:

            + Kerberos database, which stores all the information about the
              principals and the realm they belong to, among other things.
            + Authentication service (AS)
            + Ticket-granting service (TGS)

    * Client

        - Either a *user, host or a service* who sends a request for a ticket.

    * Principal

        - It is a unique identity to which Kerberos can assign tickets.
          Principals can have an arbitrary number of components. Each component
          is separated by a component separator, generally ``/``. The last
          component is the *realm*, separated from the rest of the principal by
          the realm separator, generally ``@``.

        - If there is no realm component in the principal, then it will be
          assumed that the principal is in the default realm for the context in
          which it is being used.

        - Usually, a principal is divided into three parts:

            + The ``primary``, the ``instance``, and the ``realm``

            + The format of a typical Kerberos V5 principal is
              ``primary/instance@REALM``.

            + The ``primary`` is the first part of the principal. In the case
              of a user, it's the same as the ``username``. For a host, the
              primary is the word ``host``. For Ceph, will use ``ceph`` as a
              primary name which makes it easier to organize and identify Ceph
              related principals.

            + The ``instance`` is an optional string that qualifies the
              primary. The instance is separated from the primary by a slash
              ``/``. In the case of a user, the instance is usually ``null``,
              but a user might also have an additional principal, with an
              instance called ``admin``, which one uses to administrate a
              database.

              The principal ``johndoe@MYDOMAIN.COM`` is completely separate
              from the principal ``johndoe/admin@MYDOMAIN.COM``, with a
              separate password, and separate permissions. In the case of a
              host, the instance is the fully qualified hostname,
              i.e., ``osd1.MYDOMAIN.COM``.

            + The ``realm`` is the Kerberos realm. Usually, the Kerberos realm
              is the domain name, in *upper-case letters*. For example, the
              machine ``osd1.MYDOMAIN.COM`` would be in the realm
              ``MYDOMAIN.COM``.

    * Keytab

        - A keytab file stores the actual encryption key that can be used in
          lieu of a password challenge for a given principal. Creating keytab
          files are useful for noninteractive principals, such as *Service
          Principal Names*, which are often associated with long-running
          processes like Ceph daemons. A keytab file does not have to be a
          "1:1 mapping" to a single principal. Multiple different principal
          keys can be stored in a single keytab file:

            + The keytab file allows a user/service to authenticate without
              knowledge of the password. Due to this, *keytabs should be
              protected* with appropriate controls to prevent unauthorized
              users from authenticating with it.

            + The default client keytab file is ``/etc/krb5.keytab``

|

The 'Ceph side' of the things
------------------------------

In order to configure connections (from Ceph nodes) to the KDC:

1. Login to the Kerberos client (Ceph server nodes) and confirm it is properly
   configured, by checking and editing ``/etc/krb5.conf`` file properly:  ::

    /etc/krb5.conf
    [libdefaults]
        dns_canonicalize_hostname = false
        rdns = false
        forwardable = true
        dns_lookup_realm = true
        dns_lookup_kdc = true
        allow_weak_crypto = false
        default_realm = MYDOMAIN.COM
        default_ccache_name = KEYRING:persistent:%{uid}
    [realms]
        MYDOMAIN.COM = {
            kdc = kerberos.mydomain.com
            admin_server = kerberos.mydomain.com
            ...
        }
    ...


2. Login to the *KDC Server* and confirm it is properly configured to
   authenticate to the Kerberos realm in question:

    a. Kerberos related DNS RRs:  ::

        /var/lib/named/master/mydomain.com
        kerberos                IN A        192.168.10.21
        kerberos-slave          IN A        192.168.10.22
        _kerberos               IN TXT      "MYDOMAIN.COM"
        _kerberos._udp          IN SRV      1 0 88 kerberos
        _kerberos._tcp          IN SRV      1 0 88 kerberos
        _kerberos._udp          IN SRV      20 0 88 kerberos-slave
        _kerberos-master._udp   IN SRV      0 0 88 kerberos
        _kerberos-adm._tcp      IN SRV      0 0 749 kerberos
        _kpasswd._udp           IN SRV      0 0 464 kerberos
        ...


    b. KDC configuration file:  ::

        /var/lib/kerberos/krb5kdc/kdc.conf
        [kdcdefaults]
                kdc_ports = 750,88
        [realms]
                MYDOMAIN.COM = {
                    acl_file = /var/lib/kerberos/krb5kdc/kadm5.acl
                    admin_keytab = FILE:/var/lib/kerberos/krb5kdc/kadm5.keytab
                    default_principal_flags = +postdateable +forwardable +renewable +proxiable
                                                            +dup-skey -preauth -hwauth +service
                                                            +tgt-based +allow-tickets -pwchange
                                                            -pwservice
                    dict_file = /var/lib/kerberos/krb5kdc/kadm5.dict
                    key_stash_file = /var/lib/kerberos/krb5kdc/.k5.MYDOMAIN.COM
                    kdc_ports = 750,88
                    max_life = 0d 10h 0m 0s
                    max_renewable_life = 7d 0h 0m 0s
                }
        ...


3. Still on the KDC Server, run the Kerberos administration utility;
   ``kadmin.local`` so we can list all the principals already created.  ::

    kadmin.local:  listprincs
    K/M@MYDOMAIN.COM
    krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
    kadmin/admin@MYDOMAIN.COM
    kadmin/changepw@MYDOMAIN.COM
    kadmin/history@MYDOMAIN.COM
    kadmin/kerberos.mydomain.com@MYDOMAIN.COM
    root/admin@MYDOMAIN.COM
    ...


4. Add a *principal for each Ceph cluster node* we want to be authenticated by
   Kerberos:

    a. Adding principals:  ::

        kadmin.local:  addprinc -randkey ceph/ceph-mon1
        Principal "ceph/ceph-mon1@MYDOMAIN.COM" created.
        kadmin.local:  addprinc -randkey ceph/ceph-osd1
        Principal "ceph/ceph-osd1@MYDOMAIN.COM" created.
        kadmin.local:  addprinc -randkey ceph/ceph-osd2
        Principal "ceph/ceph-osd2@MYDOMAIN.COM" created.
        kadmin.local:  addprinc -randkey ceph/ceph-osd3
        Principal "ceph/ceph-osd3@MYDOMAIN.COM" created.
        kadmin.local:  addprinc -randkey ceph/ceph-osd4
        Principal "ceph/ceph-osd4@MYDOMAIN.COM" created.
        kadmin.local:  listprincs
        K/M@MYDOMAIN.COM
        krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
        kadmin/admin@MYDOMAIN.COM
        kadmin/changepw@MYDOMAIN.COM
        kadmin/history@MYDOMAIN.COM
        kadmin/kerberos.mydomain.com@MYDOMAIN.COM
        root/admin@MYDOMAIN.COM
        ceph/ceph-mon1@MYDOMAIN.COM
        ceph/ceph-osd1@MYDOMAIN.COM
        ceph/ceph-osd2@MYDOMAIN.COM
        ceph/ceph-osd3@MYDOMAIN.COM
        ceph/ceph-osd4@MYDOMAIN.COM
        ...


    b. This follows the same idea if we are creating a *user principal*  ::

        kadmin.local:  addprinc johndoe
        WARNING: no policy specified for johndoe@MYDOMAIN.COM; defaulting to no policy
        Enter password for principal "johndoe@MYDOMAIN.COM":
        Re-enter password for principal "johndoe@MYDOMAIN.COM":
        Principal "johndoe@MYDOMAIN.COM" created.
        ...


5. Create a *keytab file* for each Ceph cluster node:

    As the default client keytab file is ``/etc/krb5.keytab``, we will want to
    use a different file name, so we especify which *keytab file to create* and
    which *principal to export keys* from:  ::

        kadmin.local:  ktadd -k /etc/gss_client_mon1.ktab ceph/ceph-mon1
        Entry for principal ceph/ceph-mon1 with kvno 2, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_mon1.ktab.
        Entry for principal ceph/ceph-mon1 with kvno 2, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_mon1.ktab.
        Entry for principal ceph/ceph-mon1 with kvno 2, encryption type des3-cbc-sha1 added to keytab WRFILE:/etc/gss_client_mon1.ktab.
        Entry for principal ceph/ceph-mon1 with kvno 2, encryption type arcfour-hmac added to keytab WRFILE:/etc/gss_client_mon1.ktab.
        kadmin.local:  ktadd -k /etc/gss_client_osd1.ktab ceph/ceph-osd1
        Entry for principal ceph/ceph-osd1 with kvno 2, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd1.ktab.
        Entry for principal ceph/ceph-osd1 with kvno 2, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd1.ktab.
        Entry for principal ceph/ceph-osd1 with kvno 2, encryption type des3-cbc-sha1 added to keytab WRFILE:/etc/gss_client_osd1.ktab.
        Entry for principal ceph/ceph-osd1 with kvno 2, encryption type arcfour-hmac added to keytab WRFILE:/etc/gss_client_osd1.ktab.
        kadmin.local:  ktadd -k /etc/gss_client_osd2.ktab ceph/ceph-osd2
        Entry for principal ceph/ceph-osd2 with kvno 2, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd2.ktab.
        Entry for principal ceph/ceph-osd2 with kvno 2, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd2.ktab.
        Entry for principal ceph/ceph-osd2 with kvno 2, encryption type des3-cbc-sha1 added to keytab WRFILE:/etc/gss_client_osd2.ktab.
        Entry for principal ceph/ceph-osd2 with kvno 2, encryption type arcfour-hmac added to keytab WRFILE:/etc/gss_client_osd2.ktab.
        kadmin.local:  ktadd -k /etc/gss_client_osd3.ktab ceph/ceph-osd3
        Entry for principal ceph/ceph-osd3 with kvno 3, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd3.ktab.
        Entry for principal ceph/ceph-osd3 with kvno 3, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd3.ktab.
        Entry for principal ceph/ceph-osd3 with kvno 3, encryption type des3-cbc-sha1 added to keytab WRFILE:/etc/gss_client_osd3.ktab.
        Entry for principal ceph/ceph-osd3 with kvno 3, encryption type arcfour-hmac added to keytab WRFILE:/etc/gss_client_osd3.ktab.
        kadmin.local:  ktadd -k /etc/gss_client_osd4.ktab ceph/ceph-osd4
        Entry for principal ceph/ceph-osd4 with kvno 4, encryption type aes256-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd4.ktab.
        Entry for principal ceph/ceph-osd4 with kvno 4, encryption type aes128-cts-hmac-sha1-96 added to keytab WRFILE:/etc/gss_client_osd4.ktab.
        Entry for principal ceph/ceph-osd4 with kvno 4, encryption type des3-cbc-sha1 added to keytab WRFILE:/etc/gss_client_osd4.ktab.
        Entry for principal ceph/ceph-osd4 with kvno 4, encryption type arcfour-hmac added to keytab WRFILE:/etc/gss_client_osd4.ktab.

        # ls -1 /etc/gss_client_*
        /etc/gss_client_mon1.ktab
        /etc/gss_client_osd1.ktab
        /etc/gss_client_osd2.ktab
        /etc/gss_client_osd3.ktab
        /etc/gss_client_osd4.ktab


    We can also check these newly created keytab client files by:  ::

        # klist -kte /etc/gss_client_mon1.ktab
        Keytab name: FILE:/etc/gss_client_mon1.ktab
        KVNO Timestamp           Principal
        ---- ------------------- ------------------------------------------------------
           2 10/8/2018 14:35:30 ceph/ceph-mon1@MYDOMAIN.COM (aes256-cts-hmac-sha1-96)
           2 10/8/2018 14:35:31 ceph/ceph-mon1@MYDOMAIN.COM (aes128-cts-hmac-sha1-96)
           2 10/8/2018 14:35:31 ceph/ceph-mon1@MYDOMAIN.COM (des3-cbc-sha1)
           2 10/8/2018 14:35:31 ceph/ceph-mon1@MYDOMAIN.COM (arcfour-hmac)
        ...


6. A new *set parameter* was added in Ceph, ``gss_ktab_client_file`` which
   points to the keytab file related to the Ceph node *(or principal)* in
   question.

    By default it points to ``/var/lib/ceph/$name/gss_client_$name.ktab``. So,
    in the case of a Ceph server ``osd1.mydomain.com``, the location and name
    of the keytab file should be: ``/var/lib/ceph/osd1/gss_client_osd1.ktab``

    Therefore, we need to ``scp`` each of these newly created keytab files from
    the KDC to their respective Ceph cluster nodes (i.e):
    ``# for node in mon1 osd1 osd2 osd3 osd4; do scp /etc/gss_client_$node*.ktab root@ceph-$node:/var/lib/ceph/$node/; done``

    Or whatever other way one feels comfortable with, as long as each keytab
    client file gets copied over to the proper location.

    At this point, even *without using any keytab client file* we should be
    already able to authenticate a *user principal*:  ::

        # kdestroy -A && kinit -f johndoe && klist -f
        Password for johndoe@MYDOMAIN.COM:
        Ticket cache: KEYRING:persistent:0:0
        Default principal: johndoe@MYDOMAIN.COM

        Valid starting       Expires              Service principal
        10/10/2018 15:32:01  10/11/2018 07:32:01  krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
            renew until 10/11/2018 15:32:01, Flags: FRI
        ...


    Given that the *keytab client file* is/should already be copied and available at the
    Kerberos client (Ceph cluster node), we should be able to authenticate using it before
    continuing:  ::

        # kdestroy -A && kinit -k -t /etc/gss_client_mon1.ktab -f 'ceph/ceph-mon1@MYDOMAIN.COM' && klist -f
        Ticket cache: KEYRING:persistent:0:0
        Default principal: ceph/ceph-mon1@MYDOMAIN.COM

        Valid starting       Expires              Service principal
        10/10/2018 15:54:25  10/11/2018 07:54:25  krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
            renew until 10/11/2018 15:54:25, Flags: FRI
        ...


7. The default client keytab is used, if it is present and readable, to
   automatically obtain initial credentials for GSSAPI client applications. The
   principal name of the first entry in the client keytab is used by default
   when obtaining initial credentials:

    a. The ``KRB5_CLIENT_KTNAME environment`` variable.
    b. The ``default_client_keytab_name`` profile variable in ``[libdefaults]``.
    c. The hardcoded default, ``DEFCKTNAME``.

    So, what we do is to internally, set the environment variable
    ``KRB5_CLIENT_KTNAME`` to the same location as ``gss_ktab_client_file``,
    so ``/var/lib/ceph/osd1/gss_client_osd1.ktab``, and change the ``ceph.conf``
    file to add the new authentication method.  ::

        /etc/ceph/ceph.conf
        [global]
            ...
            auth_cluster_required = gss
            auth_service_required = gss
            auth_client_required = gss
            gss_ktab_client_file = /{$my_new_location}/{$my_new_ktab_client_file.keytab}
            ...


8. With that the GSSAPIs will then be able to read the keytab file and using
   the process of name and service resolution *(provided by the DNS)*, able to
   request a *TGT* as follows:

    a. User/Client sends principal identity and credentials to the KDC Server
       (TGT request).
    b. KDC checks its internal database for the principal in question.
    c. a TGT is created and wrapped by the KDC, using the principal's key
       (TGT + Key).
    d. The newly created TGT, is decrypted and stored in the credentials
       cache.
    e. At this point, Kerberos/GSSAPI aware applications (and/or services) are
       able to check the list of active TGT in the keytab file.

|
|

** *For Ceph Developers Only* **
=================================

We certainly could have used straight native ``KRB5 APIs`` (instead of
``GSSAPIs``), but we wanted a more portable option as regards network security,
which is the hallmark of the ``GSS`` *(Generic Security Standard)* ``-API``.
It does not actually provide security services itself.

Rather, it is a framework that provides security services to callers in a
generic way.

.. ditaa::

    +---------------------------------+
    |        Application              |
    +---------------------------------+
    | Protocol (RPC, Etc. [Optional]) |
    +---------------------------------+
    |         GSS-API                 |
    +---------------------------------+
    |   Security Mechs (Krb v5, Etc)  |
    +---------------------------------+


The GSS-API does two main things:

    1. It creates a security context in which data can be passed between
       applications. A context can be thought of as a sort of *"state of trust"*
       between two applications.

       Applications that share a context know who each other are and thus can
       permit data transfers between them as long as the context lasts.

    2. It applies one or more types of protection, known as *"security services"*,
       to the data to be transmitted.


GSS-API provides several types of portability for applications:

    a. **Mechanism independence.** GSS-API provides a generic interface to the
       mechanisms for which it has been implemented. By specifying a default
       security mechanism, an application does not need to know which mechanism
       it is using (for example, Kerberos v5), or even what type of mechanism
       it uses. As an example, when an application forwards a user's credential
       to a server, it does not need to know if that credential has a Kerberos
       format or the format used by some other mechanism, nor how the
       credentials are stored by the mechanism and accessed by the application.
       (If necessary, an application can specify a particular mechanism to use)

    b. **Protocol independence.** The GSS-API is independent of any
       communications protocol or protocol suite. It can be used with
       applications that use, for example, sockets, RCP, or TCP/IP.
       RPCSEC_GSS "RPCSEC_GSS Layer" is an additional layer that smoothly
       integrates GSS-API with RPC.

    c. **Platform independence.** The GSS-API is completely oblivious to the
       type of operating system on which an application is running.

    d. **Quality of Protection independence.** Quality of Protection (QOP) is
       the name given to the type of algorithm used in encrypting data or
       generating cryptographic tags; the GSS-API allows a programmer to ignore
       QOP, using a default provided by the GSS-API.
       (On the other hand, an application can specify the QOP if necessary.)

    The basic security offered by the GSS-API is authentication. Authentication
    is the verification of an identity: *if you are authenticated, it means
    that you are recognized to be who you say you are.*

    The GSS-API provides for two additional security services, if supported by the
    underlying mechanisms:

    1. **Integrity:** It's not always sufficient to know that an application
       sending you data is who it claims to be. The data itself could have
       become corrupted or compromised.

       The GSS-API provides for data to be accompanied by a cryptographic tag,
       known as an ``Message Integrity Code (MIC)``, to prove that the data
       that arrives at your doorstep is the same as the data that the sender
       transmitted. This verification of the data's validity is known as
       *"integrity"*.

    2. **Confidentiality:** Both authentication and integrity, however, leave
       the data itself alone, so if it's somehow intercepted, others can read
       it.

       The GSS-API therefore allows data to be encrypted, if underlying
       mechanisms support it. This encryption of data is known as *"confidentiality"*.

|

Mechanisms Available With GSS-API:

    The current implementation of the GSS-API works only with the Kerberos v5 security
    mechanism.  ::

        Mechanism Name          Object Identifier       Shared Library  Kernel Module
        ----------------------  ----------------------  --------------  --------------
        diffie_hellman_640_0    1.3.6.4.1.42.2.26.2.4   dh640-0.so.1
        diffie_hellman_1024_0   1.3.6.4.1.42.2.26.2.5   dh1024-0.so.1
        SPNEGO                  1.3.6.1.5.5.2
        iakerb                  1.3.6.1.5.2.5
        SCRAM-SHA-1             1.3.6.1.5.5.14
        SCRAM-SHA-256           1.3.6.1.5.5.18
        GSS-EAP (arc)           1.3.6.1.5.5.15.1.1.*
        kerberos_v5             1.2.840.113554.1.2.2    gl/mech_krb5.so gl_kmech_krb5

        Therefore:
            Kerberos Version 5 GSS-API Mechanism
            OID {1.2.840.113554.1.2.2}

            Kerberos Version 5 GSS-API Mechanism
            Simple and Protected GSS-API Negotiation Mechanism
            OID {1.3.6.1.5.5.2}


    There are two different formats:

        1. The first, ``{ 1 2 3 4 }``, is officially mandated by the GSS-API
           specs. ``gss_str_to_oid()`` expects this first format.

        2. The second, ``1.2.3.4``, is more widely used but is not an official
           standard format.

    Although the GSS-API makes protecting data simple, it does not do certain
    things, in order to maximize its generic nature. These include:

        a. Provide security credentials for a user or application. These must
           be provided by the underlying security mechanism(s). The GSS-API
           does allow applications to acquire credentials, either automatically
           or explicitly.

        b. Transfer data between applications. It is the application's
           responsibility to handle the transfer of all data between peers,
           whether it is security-related or "plain" data.

        c. Distinguish between different types of transmitted data (for
           example, to know or determine that a data packet is plain data and
           not GSS-API related).

        d. Indicate status due to remote (asynchronous) errors.

        e. Automatically protect information sent between processes of a
           multiprocess program.

        f. Allocate string buffers ("Strings and Similar Data") to be passed to
           GSS-API functions.

        g. Deallocate GSS-API data spaces. These must be explicitly deallocated
           with functions such as ``gss_release_buffer()`` and
           ``gss_delete_name()``.

|

These are the basic steps in using the GSS-API:

    1. Each application, sender and recipient, acquires credentials explicitly,
       if credentials have not been acquired automatically.

    2. The sender initiates a security context and the recipient accepts it.

    3. The sender applies security protection to the message (data) it wants to
       transmit. This means that it either encrypts the message or stamps it
       with an identification tag. The sender transmits the protected message.
       (The sender can choose not to apply either security protection, in which
       case the message has only the default GSS-API security service
       associated with it. That is authentication, in which the recipient knows
       that the sender is who it claims to be.)

    4. The recipient decrypts the message (if needed) and verifies it
       (if appropriate).

    5. (Optional) The recipient returns an identification tag to the sender for
       confirmation.

    6. Both applications destroy the shared security context. If necessary,
       they can also deallocate any *"leftover"* GSS-API data.

    Applications that use the GSS-API should include the file ``gssapi.h``.

    Good References:
        - `rfc1964 <https://tools.ietf.org/html/rfc1964>`_.
        - `rfc2743 <https://tools.ietf.org/html/rfc2743>`_.
        - `rfc2744 <https://tools.ietf.org/html/rfc2744>`_.
        - `rfc4178 <https://tools.ietf.org/html/rfc4178>`_.
        - `rfc6649 <https://tools.ietf.org/html/rfc6649>`_.
        - `MIT Kerberos Documentation <https://web.mit.edu/kerberos/krb5-latest/doc/appdev/gssapi.html>`_.

|

** *Kerberos Server Setup* **
------------------------------

First and foremost, ``this is not a recommendation for a production
environment``. We are not covering ``Master/Slave replication cluster`` or
anything production environment related (*ntp/chrony, dns, pam/nss, sssd, etc*).

Also, on the server side there might be different dependencies and/or
configuration steps needed, depending on which backend database will be used.
``LDAP as a backend database`` is a good example of that.

On the client side there are different steps depending on which client backend
configuration will be used. For example ``PAM/NSS`` or ``SSSD`` (along with
LDAP for identity service, [and Kerberos for authentication service]) which is
the best suited option for joining ``MS Active Directory domains``, and doing
``User Logon Management``.

By no means we intend to cover every possible scenario/combination here. These
steps are for a simple *get a (MIT) Kerberos Server up and running*.

Please, note that *rpm packages might have slightly different names*, as well
as the locations for the binaries and/or configuration files, depending on
which Linux distro we are referring to.

Finally, keep in mind that some Linux distros will have their own ``wizards``,
which can perform the basic needed configuration:  ::

    SUSE:
        Kerberos server:
            yast2 auth-server

        Kerberos client:
            pam/nss: yast2 ldapkrb
            sssd: yast2 auth-client


However, we are going through the ``manual configuration``.


In order to get a new MIT KDC Server running:

1. Install the KDC server by:

    a. Install the needed packages:  ::

        SUSE: zypper install krb5 krb5-server krb5-client
            Additionally:
                for development: krb5-devel
                if using 'sssd': sssd-krb5 sssd-krb5-common

        REDHAT: yum install krb5-server krb5-libs krb5-workstation
            Additionally: 'Needs to be checked'


    b. Edit the KDC Server configuration file:  ::

        /var/lib/kerberos/krb5kdc/kdc.conf
        [kdcdefaults]
                kdc_ports = 750,88
        [realms]
                MYDOMAIN.COM = {
                    acl_file = /var/lib/kerberos/krb5kdc/kadm5.acl
                    admin_keytab = FILE:/var/lib/kerberos/krb5kdc/kadm5.keytab
                    default_principal_flags = +postdateable +forwardable +renewable +proxiable
                                                            +dup-skey -preauth -hwauth +service
                                                            +tgt-based +allow-tickets -pwchange
                                                            -pwservice
                    dict_file = /var/lib/kerberos/krb5kdc/kadm5.dict
                    key_stash_file = /var/lib/kerberos/krb5kdc/.k5.MYDOMAIN.COM
                    kdc_ports = 750,88
                    max_life = 0d 10h 0m 0s
                    max_renewable_life = 7d 0h 0m 0s
                }
        ...


    c. Edit the Kerberos Client configuration file:  ::

        /etc/krb5.conf
        [libdefaults]
            dns_canonicalize_hostname = false
            rdns = false
            forwardable = true
            dns_lookup_realm = true     //--> if using DNS/DNSMasq
            dns_lookup_kdc = true       //--> if using DNS/DNSMasq
            allow_weak_crypto = false
            default_realm = MYDOMAIN.COM
            default_ccache_name = KEYRING:persistent:%{uid}

        [realms]
            MYDOMAIN.COM = {
                kdc = kerberos.mydomain.com
                admin_server = kerberos.mydomain.com
                ...
            }
        ...


2. Create the Kerberos database:  ::

    SUSE: kdb5_util create -s

    REDHAT: kdb5_util create -s


3. Enable and Start both 'KDC and KDC admin' servers:  ::

    SUSE: systemctl enable/start krb5kdc
          systemctl enable/start kadmind

    REDHAT: systemctl enable/start krb5kdc
            systemctl enable/start kadmin


4. Create a Kerberos Administrator
    Kerberos principals can be created either locally on the KDC server itself
    or through the network, using an 'admin principal'. On the KDC server,
    using ``kadmin.local``:

    a. List the existing principals:  ::

        kadmin.local:  listprincs
        K/M@MYDOMAIN.COM
        krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
        kadmin/admin@MYDOMAIN.COM
        kadmin/changepw@MYDOMAIN.COM
        kadmin/history@MYDOMAIN.COM
        kadmin/kerberos.mydomain.com@MYDOMAIN.COM
        root/admin@MYDOMAIN.COM
        ...


    b. In case we don't have a built-in 'admin principal', we then create one
    (whatever ``principal name``, we are using ``root``, once by default
    ``kinit`` tries to authenticate using the same system login user name,
    unless a ``principal`` is passed as an argument ``kinit principal``):  ::

        # kadmin.local -q "addprinc root/admin"
        Authenticating as principal root/admin@MYDOMAIN.COM with password.
        WARNING: no policy specified for root/admin@MYDOMAIN.COM; defaulting to no policy
        Enter password for principal "root/admin@MYDOMAIN.COM":


    c. Confirm the newly created 'admin principal' has the needed permissions
       in the KDC ACL (if ACLs are changed, ``kadmind`` needs to be restarted):  ::

        SUSE: /var/lib/kerberos/krb5kdc/kadm5.acl
        REDHAT: /var/kerberos/krb5kdc/kadm5.acl

        ###############################################################################
        #Kerberos_principal      permissions     [target_principal]      [restrictions]
        ###############################################################################
        #
        */admin@MYDOMAIN.COM     *


    d. Create a simple 'user principal' (same steps as by *The 'Ceph side' of
       the things*; 4a):  ::

        kadmin.local:  addprinc johndoe
        WARNING: no policy specified for johndoe@MYDOMAIN.COM; defaulting to no policy
        Enter password for principal "johndoe@MYDOMAIN.COM":
        Re-enter password for principal "johndoe@MYDOMAIN.COM":
        Principal "johndoe@MYDOMAIN.COM" created.


    e. Confirm the newly created 'user principal' is able to authenticate (same
       steps as by *The 'Ceph side' of the things*; 6):  ::

        # kdestroy -A && kinit -f johndoe && klist -f
        Password for johndoe@MYDOMAIN.COM:
        Ticket cache: KEYRING:persistent:0:0
        Default principal: johndoe@MYDOMAIN.COM

        Valid starting       Expires              Service principal
        11/16/2018 13:11:16  11/16/2018 23:11:16  krbtgt/MYDOMAIN.COM@MYDOMAIN.COM
                renew until 11/17/2018 13:11:16, Flags: FRI
        ...


5. At this point, we should have a *simple (MIT) Kerberos Server up and running*:

     a. Considering we will want to work with keytab files, for both 'user and
        service' principals, refer to The *'Ceph side' of the things* starting
        at step 4.

     b. Make sure you are comfortable with following and their ``manpages``:  ::

            krb5.conf       -> Krb client config file
            kdc.conf        -> KDC server config file

            krb5kdc         -> KDC server daemon
            kadmind         -> KDC administration daemon

            kadmin          -> Krb administration tool
            kdb5_util       -> Krb low-level database administration tool

            kinit           -> Obtain and cache Kerberos ticket-granting ticket tool
            klist           -> List cached Kerberos tickets tool
            kdestroy        -> Destroy Kerberos tickets tool


6. Name Resolution
    As mentioned earlier, Kerberos *relies heavily on name resolution*. Most of
    the Kerberos issues are usually related to name resolution, since Kerberos
    is *very picky* on both *systems names* and *host lookups*.

    a. As described in *The 'Ceph side' of the things*; step 2a, DNS RRs
       greatly improves service location and host/domain resolution, by using
       ``(srv resources)`` and ``(txt record)`` respectively (as per
       *Before We Start*; *DNS resolution*).  ::

        /var/lib/named/master/mydomain.com
        kerberos                IN A        192.168.10.21
        kerberos-slave          IN A        192.168.10.22
        _kerberos               IN TXT      "MYDOMAIN.COM"
        _kerberos._udp          IN SRV      1 0 88 kerberos
        _kerberos._tcp          IN SRV      1 0 88 kerberos
        _kerberos._udp          IN SRV      20 0 88 kerberos-slave
        _kerberos-master._udp   IN SRV      0 0 88 kerberos
        _kerberos-adm._tcp      IN SRV      0 0 749 kerberos
        _kpasswd._udp           IN SRV      0 0 464 kerberos
        ...


    b. For a small network or development environment, where a *DNS server is
       not available*, we have the option to use ``DNSMasq``, an
       ease-to-configure lightweight DNS server (along with some other
       capabilities).

       These records can be added to ``/etc/dnsmasq.conf`` (in addition to the
       needed 'host records'):  ::

        /etc/dnsmasq.conf
        ...
        txt-record=_kerberos.mydomain.com,"MYDOMAIN.COM"
        srv-host=_kerberos._udp.mydomain.com,kerberos.mydomain.com,88,1
        srv-host=_kerberos._udp.mydomain.com,kerberos-2.mydomain.com,88,20
        srv-host=_kerberos-master._udp.mydomain.com,kerberos.mydomain.com,88,0
        srv-host=_kerberos-adm._tcp.mydomain.com,kerberos.mydomain.com,749,0
        srv-host=_kpasswd._udp.mydomain.com,kerberos.mydomain.com,464,0
        srv-host=_kerberos._tcp.mydomain.com,kerberos.mydomain.com,88,1
        ...


    c. After 'b)' is all set, and ``dnsmasq`` service up and running, we can
       test it using:  ::

        # nslookup kerberos
        Server:     192.168.10.1
        Address:    192.168.10.1#53

        Name:   kerberos.mydomain.com
        Address: 192.168.10.21

        # host -t SRV _kerberos._tcp.mydomain.com
        _kerberos._tcp.mydomain.com has SRV record 1 0 88 kerberos.mydomain.com.

        # host -t SRV {each srv-host record}
        # host -t TXT _kerberos.mydomain.com
        _kerberos.mydomain.com descriptive text "MYDOMAIN.COM"
        ...


    f. As long as ``name resolution`` is working properly, either ``dnsmasq``
       or ``named``, Kerberos should be able to find the needed service
       records.
