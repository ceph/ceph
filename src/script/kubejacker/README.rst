
This tool is for developers who want to run their WIP Ceph code
inside a Rook/kubernetes cluster without waiting for packages
to build.

It simply takes a Rook image, overlays all the binaries from your
built Ceph tree into it, and spits out a new Rook image.  This will
only work as long as your build environment is sufficiently similar
(in terms of dependencies etc) to the version of Ceph that was
originally in the images you're injecting into.

