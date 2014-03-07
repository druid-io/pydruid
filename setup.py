from setuptools import setup

setup(
    name='pyDruid',
    version='0.2.0',
    author='Deep Ganguli',
    author_email='deep@metamarkets.com',
    packages=['pydruid', 'pydruid.utils'],
    url='http://pypi.python.org/pypi/pyDruid/',
    license='LICENSE',
    description='A Python connector for Druid.',
    long_description='See https://github.com/metamx/pydruid for more information.',
    install_requires=[
        "simplejson >= 3.3.0",
    ],
)
