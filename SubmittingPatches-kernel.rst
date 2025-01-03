Submitting Patches to Ceph - Kernel Components
==============================================

Submission of patches to the Ceph kernel code is subject to the same rules
and guidelines as any other patches to the Linux Kernel. These are set out in
``Documentation/process/submitting-patches.rst`` in the kernel source tree.

What follows is a condensed version of those rules and guidelines, updated based
on the Ceph project's best practices.


.. contents::
   :depth: 3


Signing contributions
---------------------

In order to keep the record of code attribution clean within the source
repository, follow these guidelines for signing patches submitted to the
project. These definitions are taken from those used by the Linux kernel
and many other open source projects.


1. Sign your work
#################

To improve tracking of who did what, especially with patches that can
percolate to their final resting place in the kernel through several
layers of maintainers, we've introduced a "sign-off" procedure on
patches that are being emailed around.

The sign-off is a simple line at the end of the explanation for the
patch, which certifies that you wrote it or otherwise have the right to
pass it on as a open-source patch. The rules are pretty simple: if you
can certify the below:

Developer's Certificate of Origin 1.1
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By making a contribution to this project, I certify that:

   (a) The contribution was created in whole or in part by me and I
       have the right to submit it under the open source license
       indicated in the file; or

   (b) The contribution is based upon previous work that, to the best
       of my knowledge, is covered under an appropriate open source
       license and I have the right under that license to submit that
       work with modifications, whether created in whole or in part
       by me, under the same open source license (unless I am
       permitted to submit under a different license), as indicated
       in the file; or

   (c) The contribution was provided directly to me by some other
       person who certified (a), (b) or (c) and I have not modified
       it.

   (d) I understand and agree that this project and the contribution
       are public and that a record of the contribution (including all
       personal information I submit with it, including my sign-off) is
       maintained indefinitely and may be redistributed consistent with
       this project or the open source license(s) involved.

then you just add a line saying ::

        Signed-off-by: Random J Developer <random@developer.example.org>


using your real name (sorry, no pseudonyms or anonymous contributions.)

Some people also put extra tags at the end. They'll just be ignored for
now, but you can do this to mark internal company procedures or just
point out some special detail about the sign-off. 

If you are a subsystem or branch maintainer, sometimes you need to slightly
modify patches you receive in order to merge them, because the code is not
exactly the same in your tree and the submitters'. If you stick strictly to
rule (c), you should ask the submitter to rediff, but this is a totally
counter-productive waste of time and energy. Rule (b) allows you to adjust
the code, but then it is very impolite to change one submitter's code and
make them endorse your bugs. To solve this problem, it is recommended that
you add a line between the last Signed-off-by header and yours, indicating
the nature of your changes. While there is nothing mandatory about this, it
seems like prepending the description with your mail and/or name, all
enclosed in square brackets, is noticeable enough to make it obvious that
you are responsible for last-minute changes. Example ::

        Signed-off-by: Random J Developer <random@developer.example.org>
        [lucky@maintainer.example.org: struct foo moved from foo.c to foo.h]
        Signed-off-by: Lucky K Maintainer <lucky@maintainer.example.org>

This practise is particularly helpful if you maintain a stable branch and
want at the same time to credit the author, track changes, merge the fix,
and protect the submitter from complaints. Note that under no circumstances
can you change the author's identity (the From header), as it is the one
which appears in the changelog.

Special note to back-porters: It seems to be a common and useful practise
to insert an indication of the origin of a patch at the top of the commit
message (just after the subject line) to facilitate tracking. For instance,
here's what we see in 2.6-stable ::

        Date:   Tue May 13 19:10:30 2008 +0000

        SCSI: libiscsi regression in 2.6.25: fix nop timer handling

        commit 4cf1043593db6a337f10e006c23c69e5fc93e722 upstream

And here's what appears in 2.4 ::

        Date:   Tue May 13 22:12:27 2008 +0200

        wireless, airo: waitbusy() won't delay

        [backport of 2.6 commit b7acbdfbd1f277c1eb23f344f899cfa4cd0bf36a]

Whatever the format, this information provides a valuable help to people
tracking your trees, and to people trying to trouble-shoot bugs in your
tree.


2. When to use ``Acked-by:`` and ``Cc:``
########################################

The ``Signed-off-by:`` tag indicates that the signer was involved in the
development of the patch, or that he/she was in the patch's delivery path.

If a person was not directly involved in the preparation or handling of a
patch but wishes to signify and record their approval of it then they can
arrange to have an ``Acked-by:`` line added to the patch's changelog.

``Acked-by:`` is often used by the maintainer of the affected code when that
maintainer neither contributed to nor forwarded the patch.

``Acked-by:`` is not as formal as ``Signed-off-by:``. It is a record that the acker
has at least reviewed the patch and has indicated acceptance. Hence patch
mergers will sometimes manually convert an acker's "yep, looks good to me"
into an ``Acked-by:``.

``Acked-by:`` does not necessarily indicate acknowledgement of the entire patch.
For example, if a patch affects multiple subsystems and has an ``Acked-by:`` from
one subsystem maintainer then this usually indicates acknowledgement of just
the part which affects that maintainer's code. Judgement should be used here.
When in doubt people should refer to the original discussion in the mailing
list archives.

If a person has had the opportunity to comment on a patch, but has not
provided such comments, you may optionally add a "Cc:" tag to the patch.
This is the only tag which might be added without an explicit action by the
person it names. This tag documents that potentially interested parties
have been included in the discussion


3. Using ``Reported-by:``, ``Tested-by:`` and ``Reviewed-by:``
##############################################################

If this patch fixes a problem reported by somebody else, consider adding a
``Reported-by:`` tag to credit the reporter for their contribution. This tag should
not be added without the reporter's permission, especially if the problem was
not reported in a public forum. That said, if we diligently credit our bug
reporters, they will, hopefully, be inspired to help us again in the future.

A ``Tested-by:`` tag indicates that the patch has been successfully tested (in
some environment) by the person named. This tag informs maintainers that
some testing has been performed, provides a means to locate testers for
future patches, and ensures credit for the testers.

``Reviewed-by:``, instead, indicates that the patch has been reviewed and found
acceptable according to the Reviewer's Statement:

Reviewer's statement of oversight
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By offering my ``Reviewed-by:`` tag, I state that:

   (a) I have carried out a technical review of this patch to
       evaluate its appropriateness and readiness for inclusion into
       the mainline kernel.

   (b) Any problems, concerns, or questions relating to the patch
       have been communicated back to the submitter. I am satisfied
       with the submitter's response to my comments.

   (c) While there may be things that could be improved with this
       submission, I believe that it is, at this time, (1) a
       worthwhile modification to the kernel, and (2) free of known
       issues which would argue against its inclusion.

   (d) While I have reviewed the patch and believe it to be sound, I
       do not (unless explicitly stated elsewhere) make any
       warranties or guarantees that it will achieve its stated
       purpose or function properly in any given situation.

A ``Reviewed-by`` tag is a statement of opinion that the patch is an
appropriate modification of the kernel without any remaining serious
technical issues. Any interested reviewer (who has done the work) can
offer a ``Reviewed-by`` tag for a patch. This tag serves to give credit to
reviewers and to inform maintainers of the degree of review which has been
done on the patch. ``Reviewed-by:`` tags, when supplied by reviewers known to
understand the subject area and to perform thorough reviews, will normally
increase the likelihood of your patch getting into the kernel.


Preparing and sending patches
-----------------------------

For the kernel client, patches are expected to be  emailed directly to the
email list ``ceph-devel@vger.kernel.org`` (note: *not* ``dev@ceph.io``) and reviewed
in the email list.

The best way to generate a patch for manual submission is to work from
a Git checkout of the Ceph kernel client (kernel modules) repository located at
https://github.com/ceph/ceph-client. You can then generate patches
with the 'git format-patch' command. For example,

.. code-block:: bash

   $ git format-patch HEAD^^ -o mything

will take the last two commits and generate patches in the mything/
directory. The commit you specify on the command line is the
'upstream' commit that you are diffing against. Note that it does
not necessarily have to be an ancestor of your current commit. You
can do something like

.. code-block:: bash

   $ git checkout -b mything
   # ... do lots of stuff ...
   $ git fetch
   # ...find out that origin/unstable has also moved forward...
   $ git format-patch origin/unstable -o mything

and the patches will be against origin/unstable.

The ``-o`` dir is optional; if left off, the patch(es) will appear in
the current directory. This can quickly get messy.

You can also add ``--cover-letter`` and get a '0000' patch in the
mything/ directory. That can be updated to include any overview
stuff for a multipart patch series. If it's a single patch, don't
bother.

Make sure your patch does not include any extra files which do not
belong in a patch submission. Make sure to review your patch -after-
generated it with ``diff(1)``, to ensure accuracy.

If your changes produce a lot of deltas, you may want to look into
splitting them into individual patches which modify things in
logical stages. This will facilitate easier reviewing by other
kernel developers, very important if you want your patch accepted.
There are a number of scripts which can aid in this.

The ``git send-email`` command make it super easy to send patches
(particularly those prepared with git format patch). It is careful to
format the emails correctly so that you don't have to worry about your
email client mangling whitespace or otherwise screwing things up. It
works like so:

.. code-block:: bash

   $ git send-email --to ceph-devel@vger.kernel.org my.patch

for a single patch, or

.. code-block:: bash

   $ git send-email --to ceph-devel@vger.kernel.org mything

to send a whole patch series (prepared with, say, git format-patch).


No MIME, no links, no compression, no attachments. Just plain text
------------------------------------------------------------------

Developers need to be able to read and comment on the changes you are
submitting. It is important for a kernel developer to be able to
"quote" your changes, using standard e-mail tools, so that they may
comment on specific portions of your code.

For this reason, all patches should be submitting e-mail "inline".
WARNING: Be wary of your editor's word-wrap corrupting your patch,
if you choose to cut-n-paste your patch.

Do not attach the patch as a MIME attachment, compressed or not.
Many popular e-mail applications will not always transmit a MIME
attachment as plain text, making it impossible to comment on your
code. A MIME attachment also takes Linus a bit more time to process,
decreasing the likelihood of your MIME-attached change being accepted.

Exception: If your mailer is mangling patches then someone may ask
you to re-send them using MIME.


Style Guide
-----------

The Linux Kernel has coding style conventions, which are set forth in
``Documentation/process/coding-style.rst``. Please adhere to these conventions.
