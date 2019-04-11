from setuptools import setup, find_packages

tests_require = [
    'pytest',
]

setup(
    name='rbench',
    version='0.1dev',
    packages=find_packages(),
    author='Abhishek Lekshmanan',
    author_email='abhishek@suse.com',
    entry_points = dict(
        console_scripts = [
            'rbench = rbench.main:main'
        ]
    ),
    setup_requires=[
        'pytest-runner'
    ],
    install_requires=[
        'aiohttp',
        'boto3'
    ],
    tests_require=tests_require,
    extras_require= {
        'test': tests_require
    }
)
