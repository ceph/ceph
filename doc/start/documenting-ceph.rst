==================
 Documenting Ceph
==================

One of the ways you can help Ceph is to contribute documentation to the project.
Until 2012, Ceph documentation consisted primarily of scholarly papers, wiki
articles and whatever restructuredText contributions a few core developers made.
Since early 2012, most of the Ceph documentation has been written by a single
person! The Ceph project is growing rapidly, and will benefit greatly with more
participation from the community. Contributions don't need to be extensive to
benefit the project. We welcome any contributions you may offer. 


Making Contributions
====================

Making a documentation contribution generally involves the same procedural 
sequence as making a code contribution, except that you must build documentation
source instead of compiling program source.


.. ditaa:: +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |      Get the Source        |------->|       Create a Branch       |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |       Make a Change        |------->|       Build the Source      |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |     Commit the Change      |------->|       Push the Change       |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+
                                                                |
                         +--------------------------------------+ 
                         |
                         v
           +----------------------------+        +-----------------------------+
           |                            |        |                             |
           |    Make a Pull Request     |------->|   Notify Relevant Parties   |
           |                            |        |                             |
           +----------------------------+        +-----------------------------+



Getting the Source
------------------

Ceph documentation lives in the Ceph repository right along side the Ceph source
code under the ``ceph/doc`` directory. 

The most common way to make contributions if you don't have push access to the
Ceph repository is to use the `Fork and Pull`_ approach. To use this approach,
you must:

- Create a  `github`_ account (if you don't have one)
- Fork the Ceph project
- Clone your forked project

If you have push access for the Ceph repository, you can simply, 
clone the Ceph source code repository::

	git clone --recursive https://github.com/ceph/ceph.git


Creating a Branch
-----------------

To distinguish branches that include only documentation updates, we prepend them
with ``wip-doc`` by convention, following the form ``wip-doc-{your-branch-name}``.

.. note:: Do not include anything but document changes in a documentation branch, 
   or your pull request will be rejected.


Before you create a branch and start making changes, we recommend making a pull 
request on the ``master`` branch so that your repository has all the recent 
changes. :: 

	git pull

Before you create your branch name, ensure that it doesn't already exist in the
local or remote repository. ::

	git branch -a | grep wip-doc-{your-branch-name}

If it doesn't exist, create your branch::

	git checkout -b wip-doc-{your-branch-name}


Make a Change
-------------

Making changes simply involves adding, modifying or deleting restructuredText 
files, and ensuring appropriate linking from another document or table of
contents. See below for details.

The larger the change, the more likely a merge conflict... 



Building the Source
-------------------

To build the documentation, navigate to the ``ceph`` repository directory;
then execute the build script:: 

	cd ceph
	admin/build-doc

The build script will produce an output of errors and warnings. You MUST
fix errors before committing a change, and you SHOULD fix warnings.

.. important:: You must validate ALL HYPERLINKS. If a hyperlink is broken,
   it automatically breaks the build!

The first time you build the documentation, the script will notify you if
you do not have the dependencies installed. To run Sphinx, at least 
the following are required:

- python-dev
- python-pip
- python-virtualenv
- libxml2-dev
- libxslt-dev
- doxygen
- ditaa
- graphviz

Install each dependency that isn't installed on your host. For Debian/Ubuntu 
distributions, execute the following:: 

	sudo apt-get install python-dev python-pip python-virtualenv libxml2-dev libxslt-dev doxygen ditaa graphviz ant

.. For CentOS/RHEL distributions, execute the following:: 

.. 	sudo yum install python-dev python-pip python-virtualenv libxml2-dev libxslt-dev doxygen ditaa graphviz ant



Once you build the documentation set, you may navigate to the source directory
to view it::

	cd build-doc/output

There should be an ``html`` directory and a ``man`` directory containing
documentation in HTML and manpage formats respectively.


Committing Changes
------------------

An easy way to manage your documentation commits is to use visual tools for
``git``. For example, ``gitk`` provides a graphical interface for viewing the
repository history, and ``git-gui`` provides a graphical interface for viewing
your uncommitted changes, staging them for commit, committing the changes and
pushing them to your forked Ceph repository.

Ceph documentation commits are simple, but follow a strict convention:

- A commit MUST have 1 file per commit (it simplifies rollback)
- A commit MUST have a comment
- A commit comment MUST be prepended with ``doc:``
- The comment summary MUST be one line only. (strict)
- Additional comments MAY follow a blank line after the summary, 
  but should be terse.
- A commit MAY include ``fixes: {bug number}``
- Commits MUST include ``signed-off by: {email address}`` (strict)


The following is a common commit comment (preferred):: 

	doc: Fixes a spelling error and a broken hyperlink.
	
	signed-off by: john.doe@gmail.com


The following comment includes a reference to a bug. :: 

	doc: Fixes a spelling error and a broken hyperlink.

	fixes: #1234
	
	signed-off by: john.doe@gmail.com


The following comment includes a terse sentence following
the comment summary. There is a carriage return between the
summary line and the description:: 

	doc: Added mon setting to monitor config reference
	
	Describes 'mon setting', which is a new setting added
	to config_opts.h.
	
	signed-off by: john.doe@gmail.com
	

Pushing Commits
---------------

Once you have one or more commits, you must push them from your local copy of
the repository to ``github``. ::

	git push



Making Pull Requests
--------------------

As noted earlier, you can make documentation contributions using the `Fork and
Pull`_ approach.




Documentation Style Guide
=========================




Headings
--------

#. **Document Titles:** Document titles use the ``=`` character overline and 
   underline with a leading and trailing space. See `Document Title`_ for 
   details.

#. **Section Titles:** Section tiles use the ``=`` character underline with no
   leading or trailing spaces for text. Two carriage returns should precede a 
   section title (unless an inline reference precedes it). See `Sections`_ for
   details.

#. **Subsection Titles:** Subsection titles use the ``_`` character underline 
   with no leading or trailing spaces for text.  Two carriage returns should 
   precede a subsection title (unless an inline reference precedes it).


Text Body
---------

#. **Paragraphs**: Paragraphs have a leading and a trailing carriage return, 
   and should be 80 characters wide or less so that the documentation can be 
   read in native format in a command line terminal.

#. **Numbered Lists:** Numbered lists should use autonumbering by starting
   a numbered indent with ``#.`` instead of the actual number so that
   numbered paragraphs can be repositioned without requiring manual 
   renumbering.

#. **Literal Text:** To create an example of literal text (e.g., command line
   usage), terminate the preceding paragraph with ``::`` or enter a carriage
   return to create an empty line after the preceding paragraph; then, enter
   ``::`` on a separate line followed by another empty line. Then, begin the
   literal text with tab indentation (preferred) or space indentation of 3 
   characters.




#. **Hyperlinks:** Hyperlinks should use the use the backtick (accent grave) 
   character followed by the link text, another backtick and an underscore.
   Links to headings within the document are implied by this convention. Links
   to external documents should use the ``.. _Link Text: <hyperlink>`` convention.



Process
=======

Talk to me first on anything big.
Don't leave stuff hanging out for long.
Pull updates frequently to avoid merge conflicts


Ceph Configuration Settings
---------------------------

``ceph/src/common/config_opts.h``.



Ceph Manpages
-------------














.. _Python Sphinx: http://sphinx-doc.org
.. _resturcturedText: http://docutils.sourceforge.net/rst.html
.. _Fork and Pull: https://help.github.com/articles/using-pull-requests
.. _github: http://github.com
.. _Document Title: http://docutils.sourceforge.net/docs/user/rst/quickstart.html#document-title-subtitle
.. _Sections: http://docutils.sourceforge.net/docs/user/rst/quickstart.html#sections