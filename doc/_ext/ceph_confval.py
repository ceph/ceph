from sphinx.domains.python import PyField
from sphinx.locale import _
from sphinx.util.docfields import Field


def setup(app):
    app.add_object_type(
        'confval',
        'confval',
        objname='configuration value',
        indextemplate='pair: %s; configuration value',
        doc_field_types=[
            PyField(
                'type',
                label=_('Type'),
                has_arg=False,
                names=('type',),
                bodyrolename='class'
            ),
            Field(
                'default',
                label=_('Default'),
                has_arg=False,
                names=('default',),
            ),
            Field(
                'required',
                label=_('Required'),
                has_arg=False,
                names=('required',),
            ),
            Field(
                'example',
                label=_('Example'),
                has_arg=False,
            )
        ]
    )
    app.add_object_type(
        'confval_section',
        'confval_section',
        objname='configuration section',
        indextemplate='pair: %s; configuration section',
        doc_field_types=[
            Field(
                'example',
                label=_('Example'),
                has_arg=False,
            )]
    )
    return {
        'version': 'builtin',
        'parallel_read_safe': True,
        'parallel_write_safe': True,
    }
