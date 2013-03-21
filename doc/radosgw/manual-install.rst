======================================
 Install Apache, FastCGI and RADOS GW
======================================

.. note:: If you deploy Ceph with Chef cookbooks, you may skip this section. 

Install Packages
----------------

To install RADOS Gateway, you must install Apache and FastCGI first. :: 

	sudo apt-get update && sudo apt-get install apache2 libapache2-mod-fastcgi

100-Continue Support
--------------------
	
The Ceph community provides a slightly optimized version of the  ``apache2``
and ``fastcgi`` packages. The material difference is that  the Ceph packages are
optimized for the ``100-continue`` HTTP response,  where the server determines
if it will accept the request by first  evaluating the request header. See `RFC
2616, Section 8`_ for details  on ``100-continue``. You can find the Apache and
FastCGI packages modified for Ceph here:

- `Apache Oneiric`_
- `Apache Precise`_
- `FastCGI Oneric`_
- `FastCGI Precise`_

You may also clone Ceph's Apache and FastCGI git repositories:: 

   git clone --recursive https://github.com/ceph/mod_fastcgi.git
   git clone --recursive https://github.com/ceph/apache2.git

.. _Apache Oneiric: http://gitbuilder.ceph.com/apache2-deb-oneiric-x86_64-basic/ 
.. _Apache Precise: http://gitbuilder.ceph.com/apache2-deb-precise-x86_64-basic/
.. _FastCGI Oneric: http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-oneiric-x86_64-basic/ 
.. _FastCGI Precise: http://gitbuilder.ceph.com/libapache-mod-fastcgi-deb-precise-x86_64-basic/
.. _RFC 2616, Section 8: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html	

.. important: If you do NOT use a modified fastcgi as described above,
   you should disable 100-Continue support by adding the following to
   your ``ceph.conf``::

       rgw print continue = false

Apache Configuration
--------------------

Enable the URL rewrite modules for Apache and FastCGI. For example:: 

	sudo a2enmod rewrite
	sudo a2enmod fastcgi
	
By default, the ``/etc/apache2/httpd.conf`` file is blank.	Add a line for the
``ServerName`` and provide the fully qualified domain name of the host where 
you will install RADOS GW. For example:: 
	
	ServerName {fqdn}
	
Restart Apache so that the foregoing changes take effect. ::

	sudo service apache2 restart
	
Then, install RADOS Gateway. For example:: 

	sudo apt-get install radosgw


Enable SSL
----------

Some REST clients use HTTPS by default. So you should consider enabling SSL
for Apache on the server machine. ::

	sudo a2enmod ssl

Once you enable SSL, you should generate an SSL certificate. :: 

	sudo mkdir /etc/apache2/ssl
	sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/apache2/ssl/apache.key -out /etc/apache2/ssl/apache.crt

Then, restart Apache. ::

	service apache2 restart
