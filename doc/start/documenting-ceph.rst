==================
 Documenting Ceph
==================

One of the ways you can help Ceph is to contribute documentation to the project.
Until 2012, Ceph documentation consisted primarily of scholarly papers, wiki
articles and restructuredText contributions from a few core developers. Since
early 2012, most of the Ceph documentation has been written by a single person!
The Ceph project is growing rapidly, and will benefit greatly with more
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
code under the ``ceph/doc`` directory. The most common way to make contributions
if you don't have push access to the Ceph repository is to use the `Fork and
Pull`_ approach. To use this approach, you must:

- Create a  `github`_ account (if you don't have one)
- Fork the Ceph project
- Clone your forked project

If you have push access for the Ceph repository, you can simply, 
clone the Ceph source code repository::

	git clone --recursive https://github.com/ceph/ceph.git

All Ceph documentation resides under the ``ceph/doc`` directory and
subdirectories of the Ceph repository. Ceph organizes documentation into an
information architecture primarily by its main components.

- **Ceph Storage Cluster:** The Ceph Storage Cluster documentation resides
  under the ``doc/rados`` directory.
  
- **Ceph Block Device:** The Ceph Block Device documentation resides under
  the ``doc/rbd`` directory.
  
- **Ceph Object Storage:** The Ceph Object Storage documentation resides under
  the ``doc/radosgw`` directory.

- **Ceph Filesystem:** The Ceph Filesystem documentation resides under the 
  ``doc/cephfs`` directory.
  
- **Installation (Quick):** Quick start documentation resides under the
  ``doc/start`` directory.
  
- **Installation (Manual):** Manual installation documentation resides under
  the ``doc/install`` directory.
  
- **Manpage:** Manpage source resides under the ``doc/man`` directory.

- **Developer:** Developer documentation resides under the ``doc/dev`` 
  directory.

- **Images:** If you include images such as JPEG or PNG files, you should 
  store them under the ``doc/images`` directory.


Creating a Branch
-----------------

To distinguish branches that include only documentation updates, we prepend them
with ``wip-doc`` by convention, following the form ``wip-doc-{your-branch-name}``.
If the branch relates to a single bug, it assumes the bug number. For example, 
if a documentation branch is a fix for bug report #7000, the branch name should be
``wip-doc-7000`` by convention.

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

Modifying a document simply involves opening a restructuredText file, changing
its contents, and saving the changes. See `Documentation Style Guide`_ for
details on syntax requirements.

Adding a document involves creating a new restructuredText file under the
``doc`` directory or its subdirectories and saving the file with a ``*.rst``
file extension. You must also include a reference to the  document: a hyperlink
or a table of contents entry. The ``index.rst`` file of a top-level directory
usually contains a TOC, where you can add the new file name. All documents must
have a title. See `Headings`_ for details.

Your new document doesn't get tracked by ``git`` automatically. When you want 
to add the document to the repository,  you must use ``git add 
{path-to-filename}``. For example, from the top level  directory of the
repository, adding an ``example.rst`` file to the ``rados`` subdirectory would
look like this::

	git add doc/rados/example.rst

Deleting a document involves removing it from the repository with ``git rm
{path-to-filename}``. For example:: 

	git rm doc/rados/example.rst

You must also remove any reference to the document from other documents.


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

- A commit MUST have 1 file per commit (it simplifies rollback).
- A commit MUST have a comment.
- A commit comment MUST be prepended with ``doc:``. (strict)
- The comment summary MUST be one line only. (strict)
- Additional comments MAY follow a blank line after the summary, 
  but should be terse.
- A commit MAY include ``fixes: {bug number}``.
- Commits MUST include ``signed-off by: {email address}``. (strict)

.. tip:: Follow the foregoing convention particularly where it says 
   ``(strict)`` or you will be asked to modify your commit to comply with 
   this convention.

The following is a common commit comment (preferred):: 

	doc: Fixes a spelling error and a broken hyperlink.
	
	signed-off by: john.doe@gmail.com


The following comment includes a reference to a bug. :: 

	doc: Fixes a spelling error and a broken hyperlink.

	fixes: #1234
	
	signed-off by: john.doe@gmail.com


The following comment includes a terse sentence following the comment summary.
There is a carriage return between the summary line and the description:: 

	doc: Added mon setting to monitor config reference
	
	Describes 'mon setting', which is a new setting added
	to config_opts.h.
	
	signed-off by: john.doe@gmail.com
	

Pushing Commits
---------------

Once you have one or more commits, you must push them from the local copy of the
repository to ``github``. A graphical tool like ``git-gui`` provides a user
interface for pushing to the repository. ::

	git push



Making Pull Requests
--------------------

As noted earlier, you can make documentation contributions using the `Fork and
Pull`_ approach.




Documentation Style Guide
=========================

One objective of the Ceph documentation project is to ensure the readability of
the documentation in both native restructuredText format and its rendered
formats such as HTML. Navigate to your Ceph repository and view a document in
its native format. You may notice that it is generally as legible in a terminal
as it is in its rendered HTML format. Additionally, you may also notice that
diagrams in ``ditaa`` format also render reasonably well in text mode. ::

	cat doc/architecture.rst | less

Review the following style guides to maintain this consistency.


Headings
--------

#. **Document Titles:** Document titles use the ``=`` character overline and 
   underline with a leading and trailing space on the title text line. 
   See `Document Title`_ for details.

#. **Section Titles:** Section tiles use the ``=`` character underline with no
   leading or trailing spaces for text. Two carriage returns should precede a 
   section title (unless an inline reference precedes it). See `Sections`_ for
   details.

#. **Subsection Titles:** Subsection titles use the ``_`` character underline 
   with no leading or trailing spaces for text.  Two carriage returns should 
   precede a subsection title (unless an inline reference precedes it).


Text Body
---------

As a general rule, we prefer text to wrap at column 80 so that it is legible in
a command line interface without leading or trailing white space. Where
possible, we prefer to maintain this convention with text, lists, literal text
(exceptions allowed), tables, and ``ditaa`` graphics.

#. **Paragraphs**: Paragraphs have a leading and a trailing carriage return, 
   and should be 80 characters wide or less so that the documentation can be 
   read in native format in a command line terminal.

#. **Literal Text:** To create an example of literal text (e.g., command line
   usage), terminate the preceding paragraph with ``::`` or enter a carriage
   return to create an empty line after the preceding paragraph; then, enter
   ``::`` on a separate line followed by another empty line. Then, begin the
   literal text with tab indentation (preferred) or space indentation of 3 
   characters.

#. **Indented Text:** Indented text such as bullet points 
   (e.g., ``- some text``) may span multiple lines. The text of subsequent
   lines should begin at the same character position as the text of the
   indented text (less numbers, bullets, etc.).

   Indented text may include literal text examples. Whereas, text indentation
   should be done with spaces, literal text examples should be indented with
   tabs. This convention enables you to add an additional indented paragraph
   following a literal example by leaving a blank line and beginning the
   subsequent paragraph with space indentation.

#. **Numbered Lists:** Numbered lists should use autonumbering by starting
   a numbered indent with ``#.`` instead of the actual number so that
   numbered paragraphs can be repositioned without requiring manual 
   renumbering.

#. **Code Examples:** Ceph supports the use of the 
   ``.. code-block::<language>`` role, so that you can add highlighting to 
   source examples. This is preferred for source code. However, use of this 
   tag will cause autonumbering to restart at 1 if it is used as an example 
   within a numbered list. See `Showing code examples`_ for details.


Paragraph Level Markup
----------------------

The Ceph project uses `paragraph level markup`_ to highlight points.

#. **Tip:** Use the ``.. tip::`` directive to provide additional information
   that assists the reader or steers the reader away from trouble.

#. **Note**: Use the ``.. note::`` directive to highlight an important point.

#. **Important:** Use the ``.. important::`` directive to highlight important
   requirements or caveats (e.g., anything that could lead to data loss). Use
   this directive sparingly, because it renders in red.

#. **Version Added:** Use the ``.. versionadded::`` directive for new features
   or configuration settings so that users know the minimum release for using
   a feature.
   
#. **Version Changed:** Use the ``.. versionchanged::`` directive for changes
   in usage or configuration settings.

#. **Deprecated:** Use the ``.. deprecated::`` directive when CLI usage, 
   a feature or a configuration setting is no longer preferred or will be 
   discontinued.

#. **Topic:** Use the ``.. topic::`` directive to encapsulate text that is
   outside the main flow of the document. See the `topic directive`_ for
   additional details.


TOC and Hyperlinks
------------------

All documents must be linked from another document or a table of contents,
otherwise you will receive a warning when building the documentation.

The Ceph project uses the ``.. toctree::`` directive. See `The TOC tree`_
for details. When rendering a TOC, consider specifying the ``:maxdepth:`` 
parameter so the rendered TOC is reasonably terse.

Document authors should prefer to use the ``:ref:`` syntax where a link target
contains a specific unique identifier (e.g., ``.. _unique-target-id:``), and  a
reference to the target specifically references the target  (e.g.,
``:ref:`unique-target-id```) so that if source files are moved or the
information architecture changes, the links will still work. See
`Cross referencing arbitrary locations`_ for details.

Ceph documentation also uses the backtick (accent grave) character followed by
the link text, another backtick and an underscore. Sphinx allows you to
incorporate the link destination inline; however, we prefer to use the use the
``.. _Link Text: ../path`` convention at the bottom of the document, because it
improves the readability of the document in a command line interface.






.. _Python Sphinx: http://sphinx-doc.org
.. _resturcturedText: http://docutils.sourceforge.net/rst.html
.. _Fork and Pull: https://help.github.com/articles/using-pull-requests
.. _github: http://github.com
.. _Document Title: http://docutils.sourceforge.net/docs/user/rst/quickstart.html#document-title-subtitle
.. _Sections: http://docutils.sourceforge.net/docs/user/rst/quickstart.html#sections
.. _Cross referencing arbitrary locations: http://sphinx-doc.org/markup/inline.html#ref-role
.. _The TOC tree: http://sphinx-doc.org/markup/toctree.html
.. _Showing code examples: http://sphinx-doc.org/markup/code.html
.. _paragraph level markup: http://sphinx-doc.org/markup/para.html
.. _topic directive: http://docutils.sourceforge.net/docs/ref/rst/directives.html#topic