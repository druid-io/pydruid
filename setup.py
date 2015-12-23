import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        status = pytest.main(self.pytest_args)
        sys.exit(status)


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
    version='0.2.4',
    author='Deep Ganguli',
    author_email='deep@metamarkets.com',
    packages=['pydruid', 'pydruid.utils'],
    url='http://pypi.python.org/pypi/pydruid/',
    license='Apache License, Version 2.0',
    description='A Python connector for Druid.',
    long_description='See https://github.com/druid-io/pydruid for more information.',
    install_requires=install_requires,
    extras_require=extras_require,
    tests_require=['pytest', 'six', 'mock'],
    cmdclass={'test': PyTest},
)
