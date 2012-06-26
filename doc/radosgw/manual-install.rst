======================================
 Install Apache, FastCGI and RADOS GW
======================================

.. note: If you deploy Ceph with Chef cookbooks, you may skip this section. 

To install RADOS Gateway, you must install Apache and FastCGI first. :: 

	sudo apt-get update && sudo apt-get install apache2 libapache2-mod-fastcgi
	
.. note:: The Ceph community provides a slightly optimized version of the 
   ``apache2`` and ``fastcgi`` packages. The material difference is that 
   the Ceph packages are optimized for the ``100-continue`` HTTP response, 
   where the server determines if it will accept the request by first 
   evaluating the request header. See `RFC 2616, Section 8`_ for details 
   on ``100-continue``.

.. _RFC 2616, Section 8: http://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html	
	
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
