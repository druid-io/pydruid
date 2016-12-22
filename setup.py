import sys
from setuptools import setup

install_requires = [
    "six >= 1.9.0",
]

extras_require = {
    "pandas": ["pandas"],
    "async": ["tornado"]
}

# only require simplejson on python < 2.6
if sys.version_info < (2, 6):
    install_requires.append("simplejson >= 3.3.0")

setup(
    name='pydruid',
    version='0.3.1',
    author='Druid Developers',
    author_email='druid-development@googlegroups.com',
    packages=['pydruid', 'pydruid.utils'],
    url='https://pypi.python.org/pypi/pydruid/',
    license='Apache License, Version 2.0',
    description='A Python connector for Druid.',
    long_description='See https://github.com/druid-io/pydruid for more information.',
    install_requires=install_requires,
    extras_require=extras_require,
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'six', 'mock'],
)
