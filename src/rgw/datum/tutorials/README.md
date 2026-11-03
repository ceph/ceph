# Datum tutorials

Small examples of using Datum, with focused explanations and predictable output.
Core lessons use C++23, including generated record views. An optional build setting
checks the same record-view examples with the native C++26 reflection backend.

All sections and their examples are provisional while Datum continues to evolve.

Read the [tutorial](tutorial.md), starting with
[hello, world! — in pieces](tutorial.md#1-hello-world--in-pieces).
The [build appendix](tutorial.md#appendix-a-building-and-running) gives the setup,
build, run, and output-check commands.

The landing page's interactive read planner combines overlapping requests, cached data, and a
limit on how much data to read per batch. Its [complete program](examples/read_plan.cpp) is a
regular C++23 example: [build and check it](tutorial.md#run-the-interactive-read-planner-example)
without native Ceph dependencies. The diagram, code, and downloadable program follow its controls.

For **Domain Vocabulary: Ceph**, start with
[Bytes with a Ceph accent](tutorial.md#18-bytes-with-a-ceph-accent). It connects native
buffers to the byte copying, clipping, and ownership mechanisms taught earlier.

## Preview the documentation

Use Python 3.11 or newer. The site reads this guide, the complete examples, and
Datum's maintained design and interface guide directly from their source files.
Use the Datum source accompanying this tutorial so the byte vocabulary and
examples match. Chapters 16–17 are a separate
RGW integration preview; their additional local source dependency is explained in
the build appendix. Chapter 18 teaches the shared Ceph buffer vocabulary without
those RGW bindings. Prefer Datum byte geometry throughout, adding native vocabulary
only at the boundary. Set `datum_source` to the matching Datum directory:

```sh
python3 -m venv build/docs-venv
build/docs-venv/bin/python -m pip install -r requirements-docs.txt

datum_source=/path/to/ceph/src/rgw/datum
DATUM_SOURCE_DIR="$datum_source" build/docs-venv/bin/python -m mkdocs serve \
  --dev-addr 127.0.0.1:8018
```

Open <http://127.0.0.1:8018/>. The title page links the Tutorial, Design, Build and
run, complete Examples, and Index. Lessons have individual pages, reading groups,
and previous/next links, all generated from the single `tutorial.md` source.
The Index provides a task finder and linked definitions; References collects the
API sources and annotated background reading. Editable LaTeX and rendered SVG accompany all five
figures; the build appendix gives their rebuild command.

To produce the static site in `build/site/`:

```sh
DATUM_SOURCE_DIR="$datum_source" build/docs-venv/bin/python -m mkdocs build --strict
```

The site build needs no C++ compilation. Use the Datum source matching the guide's
qualified interfaces. Ceph's developer documentation publishes the same guide and
Datum design source; this MkDocs setup provides a standalone local preview.
Open `build/site/index.html` directly or serve the site over HTTP; document links
name their HTML files so either route opens the lesson itself.
