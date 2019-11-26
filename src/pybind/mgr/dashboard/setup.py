from setuptools import setup, find_packages
from pkg_resources import normalize_path

# https://packaging.python.org/guides/making-a-pypi-friendly-readme/
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="Ceph-Dashboard",
    version_format='{tag}.dev{commitcount}+{gitsha}',
    url="https://ceph.io/",
    description="ceph-mgr-dashboard is a manager module, providing a web-based application \
        to monitor and manage many aspects of a Ceph cluster and related components. \
        See the Dashboard documentation at http://docs.ceph.com/ for details and a \
        detailed feature overview.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Ceph-Dashboard Team",
    author_email="dev@ceph.io",
    project_urls={
        "Bug Tracker": "https://tracker.ceph.com/projects/mgr/issues/",
        "Documentation": "https://docs.ceph.com/docs/master/mgr/dashboard/",
        "Source Code": "https://github.com/ceph/ceph/tree/master/src/pybind/mgr/dashboard",
    },
    platforms='any',
    py_modules=[
        '__init__',
        'awsauth',
        'cherrypy_backports',
        'conftest',
        'exceptions',
        'grafana',
        'module',
        'rest_client',
        'security',
        'settings',
        'setup',
        'tools',
    ],
    packages=find_packages(exclude=['tests']),
    #include_package_data=True,
    package_data={
        #'': ['*.pyc'],
    },
    data_files=[
        #(),
    ],
    setup_requires=[
        'setuptools-git-version',
    ],
    install_requires=[
        "bcrypt",
        "bcrypt",
        "CherryPy",
        "enum34; python_version<'3.4'",
        "more-itertools",
        "PyJWT",
        "pyopenssl",
        "python3-saml",
        "requests",
        "Routes",
        "six",
    ],
    zip_safe=False
)
