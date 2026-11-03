"""Restore ``:substitutions:`` support for the ``prompt`` directive.

sphinx-substitution-extensions dropped its ``prompt`` support in 2024.8.6, so
subclass sphinx-prompt here and reinstate the option.

sphinx-prompt installs the same module twice, as ``sphinx_prompt`` and as
``sphinx-prompt`` (releases before 1.8 ship only the latter).  Each copy owns a
separate prompt cache, and only the copy registered as an extension gets its
cache cleared between documents.  The module imported here and the one passed to
``setup_extension()`` must therefore be the same one, otherwise the prompt CSS is
emitted for the first document alone and every later prompt renders bare.
"""

import importlib

from docutils.parsers.rst import directives

try:
    import sphinx_prompt
except ImportError:
    sphinx_prompt = importlib.import_module("sphinx-prompt")


class SubstitutionPrompt(sphinx_prompt.PromptDirective):
    option_spec = {
        **sphinx_prompt.PromptDirective.option_spec,
        "substitutions": directives.flag,
    }

    def run(self):
        if "substitutions" in self.options:
            substitution_defs = self.state.document.substitution_defs
            for i, line in enumerate(self.content):
                for name, value in substitution_defs.items():
                    line = line.replace(f"|{name}|", value.astext())
                self.content[i] = line
        return super().run()


def setup(app):
    app.setup_extension(sphinx_prompt.__name__)
    app.add_directive("prompt", SubstitutionPrompt, override=True)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
