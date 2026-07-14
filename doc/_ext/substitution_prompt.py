import sphinx_prompt
from docutils.parsers.rst import directives
from docutils.statemachine import StringList


class SubstitutionPrompt(sphinx_prompt.PromptDirective):
    option_spec = sphinx_prompt.PromptDirective.option_spec or {}
    option_spec["substitutions"] = directives.flag

    def run(self):
        if "substitutions" in self.options:
            substitution_defs = self.state.document.substitution_defs
            new_content = StringList()
            for item in self.content:
                for name, value in substitution_defs.items():
                    item = item.replace(f"|{name}|", value.astext())
                new_content.extend(StringList(initlist=[item]))
            self.content = new_content
        return super().run()


def setup(app):
    app.setup_extension("sphinx-prompt")
    app.add_directive("prompt", SubstitutionPrompt, override=True)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
