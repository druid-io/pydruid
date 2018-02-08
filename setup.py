import sys
from setuptools import setup

install_requires = [
    "six >= 1.9.0",
    "requests",
]

extras_require = {
    "pandas": ["pandas"],
    "async": ["tornado"],
    "sqlalchemy": ["sqlalchemy"],
    "cli": ["pygments", "prompt_toolkit", "tabulate"],
}

# only require simplejson on python < 2.6
if sys.version_info < (2, 6):
    install_requires.append("simplejson >= 3.3.0")

setup(
    name='pydruid',
    version='0.4.1',
    author='Druid Developers',
    author_email='druid-development@googlegroups.com',
    packages=['pydruid', 'pydruid.db', 'pydruid.utils'],
    url='https://pypi.python.org/pypi/pydruid/',
    license='Apache License, Version 2.0',
    description='A Python connector for Druid.',
    long_description='See https://github.com/druid-io/pydruid for more information.',
    install_requires=install_requires,
    extras_require=extras_require,
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'six', 'mock'],
    entry_points={
        'console_scripts': [
            'pydruid = pydruid.console:main',
        ],
        'sqlalchemy.dialects': [
            'druid = pydruid.db.sqlalchemy:DruidHTTPDialect',
            'druid.http = pydruid.db.sqlalchemy:DruidHTTPDialect',
            'druid.https = pydruid.db.sqlalchemy:DruidHTTPSDialect',
        ],
    },
    include_package_data=True,
)
