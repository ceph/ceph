# Datum tutorials

Small C++26 examples of using Datum, with focused explanations and predictable output.

All sections and their examples are provisional while Datum continues to evolve.

Read the [tutorial](tutorial.md), starting with
[hello, world! — in pieces](tutorial.md#1-hello-world--in-pieces).
The [build appendix](tutorial.md#appendix-a-building-and-running) gives the setup,
build, run, and output-check commands.

## Preview the documentation

Use Python 3.11 or newer. The site reads this guide, the complete examples, and
Datum's maintained design and interface guide directly from their source files.
The core lessons follow [Datum PR #72714](https://github.com/ceph/ceph/pull/72714),
qualified at `c22d6be23c73cb113a3d13fea6ba5cd5dfae9cc8`. Chapters 16–17 are a separate
RGW integration preview; their additional local source dependency is explained in
the build appendix. Set `datum_source` to the qualified Datum directory:

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
The exact **2½: An Interval Interlude** heading is retained. The Index provides
a task finder and linked definitions; References collects the API sources and
annotated background reading. Editable LaTeX and rendered SVG accompany all five
figures; the build appendix gives their rebuild command.

To produce the static site in `build/site/`:

```sh
DATUM_SOURCE_DIR="$datum_source" build/docs-venv/bin/python -m mkdocs build --strict
```

The site build needs no C++ compilation. Use the Datum source matching the guide's
qualified interfaces. Ceph's developer documentation publishes the same guide and
Datum design source; this MkDocs setup provides a standalone local preview.
