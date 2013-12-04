from setuptools import setup

setup(
    name='pyDruid',
    version='0.1.7',
    author='Deep Ganguli',
    author_email='deep@metamarkets.com',
    packages=['pydruid', 'pydruid.utils'],
    url='http://pypi.python.org/pypi/pyDruid/',
    license='LICENSE',
    description='Druid analytical data-store Python library',
    long_description=open('README.txt').read(),
    install_requires=[
        "simplejson >= 3.3.0"
    ],
)
