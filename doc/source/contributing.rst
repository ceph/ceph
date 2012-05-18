==========================
 Contributing Source Code
==========================
If you are making source contributions, you must be added to the Ceph
project on github. You must also generate keys and add them to your
github account.

Generate SSH Keys
-----------------
You must generate SSH keys for github to clone the Ceph
repository. If you do not have SSH keys for ``github``, execute::

	ssh-keygen -d

Get the key to add to your ``github`` account (the following example
assumes you used the default file path)::

	cat .ssh/id_dsa.pub

Copy the public key.

Add the Key
-----------
Go to your your ``github`` account,
click on "Account Settings" (i.e., the 'tools' icon); then,
click "SSH Keys" on the left side navbar.

Click "Add SSH key" in the "SSH Keys" list, enter a name for
the key, paste the key you generated, and press the "Add key"
button.
