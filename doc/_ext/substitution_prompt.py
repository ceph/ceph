import sphinx.application
import sphinx_prompt
from docutils import nodes
from docutils.statemachine import StringList
from docutils.parsers.rst import directives


class SubstitutionPrompt(sphinx_prompt.PromptDirective):
    optional_arguments = 3
    option_spec = (sphinx_prompt.PromptDirective.option_spec or {}).copy()
    option_spec["substitutions"] = directives.flag
    has_content = True

    def run(self) -> list[nodes.raw]:
        self.assert_has_content()

        if "substitutions" in self.options:
            substitution_defs = self.state.document.substitution_defs
            new_content = []
            for item in self.content:
                for name, value in substitution_defs.items():
                    item = item.replace(f"|{name}|", value.astext())
                new_content.append(item)
            self.content = StringList(new_content)
        return super().run()


def setup(app: sphinx.application.Sphinx) -> dict[str, bool]:
    app.setup_extension("sphinx-prompt")
    app.add_directive("subsprompt", SubstitutionPrompt)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
