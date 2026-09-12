Documentation is written in **reStructuredText** (`.rst`) and built with Sphinx. See `doc/dev/documenting.rst` for full authoring guidelines.

Do not attempt to build docs locally unless explicitly directed to. Advise the user that github will generate docs 
automatically and provide link. 

## Structure

- `doc/` — user-facing documentation (published to docs.ceph.com)
- `doc/dev/` — developer documentation (internals, design docs, workflows)

User-visible changes to Ceph functionality **must** include corresponding updates under `doc/`. Developer internals go under `doc/dev/`.

## Design docs

Complex features need a design doc in `doc/dev/` before or alongside implementation. **Never modify an approved design doc during implementation without explicit permission.**

## Diagrams

Prefer Graphviz (`.dot` files referenced via `.. graphviz::`) for directed graphs. For SVG diagrams created with Inkscape, commit both the `.svg` source and an exported `.png`; reference the `.png` in `.rst` files.
