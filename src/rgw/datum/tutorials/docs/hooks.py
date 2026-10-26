"""Render the canonical guide, library design, and runnable sources without copying authored text."""

import io
import os
import posixpath
import re
import zipfile
from pathlib import Path

import markdown
from mkdocs.exceptions import PluginError
from mkdocs.structure.files import File
from pygments.formatters import HtmlFormatter

PROJECT = Path(__file__).resolve().parent.parent
FENCES = re.compile(r"(^```[^\n]*\n.*?^```[ \t]*$)", re.M | re.S)
PARTS = [
    ("Bytes, keys, and ranges", 1, 6,
     "Read fragmented bytes, select ranges, and measure covered data."),
    ("Keeping and observing data", 7, 12,
     "Retain storage, observe fields, and save an entry position."),
    ("Work and continuation", 13, 15,
     "Account for work, traverse pages, and keep query controls."),
    ("RGW integration preview", 16, 17,
     "Read native RGW frames and apply page traversal using separate consumer adapters."),
]


def transform_prose(text, transform):
    return "".join(
        part if index % 2 else transform(part)
        for index, part in enumerate(FENCES.split(text))
    )


def promote_headings(text):
    return transform_prose(text, lambda part: re.sub(r"^##", "#", part, flags=re.M))


def toc(text):
    parser = markdown.Markdown(extensions=["toc", "fenced_code", "tables", "sane_lists"])
    parser.convert(text)
    return parser.toc_tokens[0]


def reading_links(groups, source):
    lines = []
    for group in groups:
        lines.extend(["### " + group["title"], "", group["description"], ""])
        for chapter in group["chapters"]:
            target = posixpath.relpath(chapter["file"], posixpath.dirname(source) or ".")
            lines.append(f'- [{chapter["label"]}]({target})')
        lines.append("")
    return "\n".join(lines)


def on_files(files, config):
    guide = (PROJECT / "tutorial.md").read_text(encoding="utf-8")
    revisions = set(re.findall(r"raw\.githubusercontent\.com/chardan/ceph/([0-9a-f]{40})/", guide))
    if len(revisions) != 1:
        raise PluginError("The tutorial must identify one qualified Datum revision.")
    config.extra["datum_revision"] = revisions.pop()

    source = os.environ.get("DATUM_SOURCE_DIR")
    if not source or not (Path(source) / "README.md").is_file():
        raise PluginError("Set DATUM_SOURCE_DIR to the qualified Datum directory from the build appendix.")
    design = (Path(source) / "README.md").read_text(encoding="utf-8")

    def add(name, *, content=None, path=None):
        files.append(File.generated(
            config, name, content=content, abs_src_path=str(path) if path else None,
        ))

    add("design.md", content="# Design and reference\n\n" + design.split("\n", 1)[1].lstrip())
    add("sources/datum-design.txt", content=design)
    add("sources/tutorial.md.txt", path=PROJECT / "tutorial.md")
    appendix = guide.split("## Appendix A Building and running\n", 1)[1].split("## Bibliography\n", 1)[0]
    build_page = "# Build and run\n" + promote_headings(appendix)
    references = "# References\n" + promote_headings(guide.split("## Bibliography\n", 1)[1])
    add("build-and-run.md", content=build_page)
    add("references.md", content=references)
    add("stylesheets/code.css", content=HtmlFormatter(style="default").get_style_defs(".codehilite"))

    for directory in ["examples", "expected", "figures", "cmake"]:
        for path in sorted((PROJECT / directory).rglob("*")):
            if path.is_file():
                add(path.relative_to(PROJECT).as_posix(), path=path)

    stems = [
        path.stem for path in (PROJECT / "examples").glob("*.cpp")
        if not path.stem.endswith("_variation")
    ]
    stems.sort(key=lambda stem: (int(stem[:2]), "half" in stem))
    sections = list(re.finditer(r"^## (\d+½?):? (.+)\n", guide, re.M))
    if len(sections) != len(stems):
        raise PluginError("Each tutorial lesson must have a complete program and variation.")
    original_toc = toc(guide)["children"]
    links = {}

    def link_headings(original, rendered, filename):
        links[original["id"]] = filename
        # The source guide also supports GitHub's heading spelling.
        alias = re.sub(r"[^\w\s-]", "", original["name"].lower()).replace(" ", "-")
        links[alias] = filename
        for before, after in zip(original["children"], rendered["children"]):
            links[before["id"]] = filename + "#" + after["id"]

    chapters = []
    for index, (section, stem) in enumerate(zip(sections, stems)):
        end = sections[index + 1].start() if index + 1 < len(sections) else guide.index("## Appendix A")
        content = promote_headings(guide[section.start():end])
        filename = "tutorial/" + stem.replace("_", "-") + ".md"
        title = original_toc[index]["name"]
        chapter = {
            "number": section[1], "title": title,
            "label": re.sub(r"^\d+½?:?\s+", "", title),
            "file": filename, "url": filename.removesuffix(".md") + "/", "stem": stem,
        }
        number = int(re.match(r"\d+", section[1])[0])
        if 7 <= number <= 11:
            dependency, setup = "Reflection", "requirements"
        elif number == 12:
            dependency, setup = "Reflection and codec", "enable-the-codec-lesson"
        elif number >= 16:
            dependency, setup = "RGW integration preview", "enable-the-native-rgw-examples"
        else:
            dependency, setup = "Core C++26", "requirements"
        chapter["dependency"] = dependency
        chapter["setup_url"] = "build-and-run/#" + setup
        chapters.append(chapter)
        add(filename, content=content)
        link_headings(original_toc[index], toc(content), filename)

    for index, chapter in enumerate(chapters):
        for direction, offset in [("previous", -1), ("next", 1)]:
            following = index + offset
            chapter[direction] = (
                {"url": chapters[following]["url"], "title": chapters[following]["title"]}
                if 0 <= following < len(chapters) else None
            )
    groups = [
        {"title": title, "description": description, "chapters": [chapter for chapter in chapters
         if first <= int(re.match(r"\d+", chapter["number"])[0]) <= last]}
        for title, first, last, description in PARTS
    ]
    config.extra["reading_groups"] = groups
    config.extra["chapters_by_file"] = {chapter["file"]: chapter for chapter in chapters}
    config.extra["chapter_links"] = reading_links(groups, "index.md")
    link_headings(original_toc[-2], toc(build_page), "build-and-run.md")
    link_headings(original_toc[-1], toc(references), "references.md")
    config.extra["guide_links"] = links
    introduction = guide[:sections[0].start()].replace("# Datum tutorials", "# Tutorial", 1)
    add("tutorial/index.md", content=introduction + "\n" + reading_links(groups, "tutorial/index.md")
        + "\n[Complete guide as Markdown](../sources/tutorial.md.txt) · "
          "[References](../references.md)\n")
    for item in config.nav:
        if "Tutorial" in item:
            item["Tutorial"] = [{"Contents": "tutorial/index.md"}] + [
                {chapter["label"]: chapter["file"]} for chapter in chapters
            ]

    rows = []
    for chapter in chapters:
        stem = chapter["stem"]
        expected = "01_hello_fragments" if stem.startswith("01_") else stem
        variation_output = expected if stem.startswith("01_") else stem + "_variation"
        rows.append(
            f'| [{chapter["label"]}]({chapter["file"]}) | [Program](examples/{stem}.cpp) '
            f'· [Variation](examples/{stem}_variation.cpp) '
            f'| [Program output](expected/{expected}.txt) '
            f'· [Variation output](expected/{variation_output}.txt) |'
        )
    add("examples.md", content="\n".join([
        "# Complete examples", "", "Each lesson has a runnable program and a small variation.",
        "[Download the tutorial sources](datum-tutorials.zip), then follow [Build and run](build-and-run.md).", "",
        "| Lesson | Code | Expected output |", "| --- | --- | --- |", *rows,
    ]) + "\n")

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        for name in ["CMakeLists.txt", "README.md", "tutorial.md", "mkdocs.yml", "requirements-docs.txt"]:
            bundle.write(PROJECT / name, "datum-tutorials/" + name)
        for directory in ["examples", "expected", "figures", "cmake", "docs"]:
            for path in sorted((PROJECT / directory).rglob("*")):
                if path.is_file() and "__pycache__" not in path.parts:
                    bundle.write(path, "datum-tutorials/" + path.relative_to(PROJECT).as_posix())
    add("datum-tutorials.zip", content=archive.getvalue())
    return files


def on_page_markdown(markdown, page, config, **kwargs):
    if page.is_homepage:
        markdown = markdown.replace("<!-- datum:chapters -->", config.extra["chapter_links"])

    def rewrite_link(match):
        destination = match[1]
        path, separator, anchor = destination.partition("#")
        target = None
        if path in ["tutorial.md", "../tutorial.md", "tutorials/tutorial.md"] or (
            not path and anchor in config.extra["guide_links"]
        ):
            target = config.extra["guide_links"].get(anchor, "tutorial/index.md")
        elif path == "../../../doc/dev/radosgw/datum/index.rst":
            target = "index.md"
        elif path == "docs/lookup.md":
            target = "lookup.md"
        elif path.startswith(("examples/", "expected/", "figures/")):
            target = destination
        else:
            design = (
                "https://github.com/chardan/ceph/blob/" + config.extra["datum_revision"]
                + "/src/rgw/datum/README.md#"
            )
            if destination.startswith(design):
                target = "design.md#" + destination.removeprefix(design)
        if target:
            path, separator, anchor = target.partition("#")
            destination = posixpath.relpath(path, posixpath.dirname(page.file.src_uri) or ".")
            destination += separator + anchor
        return "](" + destination + ")"

    return transform_prose(markdown, lambda part: re.sub(r"\]\(([^)\n]+)\)", rewrite_link, part))
