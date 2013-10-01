from distutils.core import setup

setup(
    name='pyDruid',
    version='0.01.2',
    author='Deep Ganguli',
    author_email='deep@metamarkets.com',
    packages=['pydruid', 'pydruid.utils'],
    url='http://pypi.python.org/pypi/pyDruid/',
    license='LICENSE',
    description='Druid analytical data-store Python library',
    long_description=open('README').read(),
    install_requires=[
        "pandas >= 0.12",
        "simplejson >= 3.3.0",
        "matplotlib >= 1.3.0"
    ],
)