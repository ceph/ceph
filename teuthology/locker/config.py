"""
This file contains database configuration.

The schema can be created with::

    CREATE TABLE machine (
        name varchar(255),
        type enum('burnupi','plana','vps') NOT NULL DEFAULT 'plana',
        up boolean NOT NULL,
        locked boolean NOT NULL,
        locked_since timestamp NOT NULL DEFAULT '0000-00-00T00:00:00',
        locked_by varchar(64),
        description text,
        sshpubkey text NOT NULL,
        PRIMARY KEY (name),
        INDEX (locked),
        INDEX (up));

If using MySQL, be sure to use an engine that supports
transactions, like InnoDB.
"""
import web

# Change these values to the connection info for your database.
DB = web.database(dbn='dbms', db='db', user='user', pw='password', host='host')
