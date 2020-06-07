import io

from setuptools import find_packages, setup

install_requires = ["requests"]

extras_require = {
    "pandas": ["pandas"],
    "async": ["tornado"],
    "sqlalchemy": ["sqlalchemy"],
    "cli": ["pygments", "prompt_toolkit", "tabulate"],
}

with io.open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pydruid",
    version="0.6.0",
    author="Druid Developers",
    author_email="druid-development@googlegroups.com",
    packages=find_packages("pydruid"),
    package_dir={"": "pydruid"},
    url="https://druid.apache.org",
    project_urls={
        "Bug Tracker": "https://github.com/druid-io/pydruid/issues",
        "Documentation": "https://pythonhosted.org/pydruid/",
        "Source Code": "https://github.com/druid-io/pydruid",
    },
    license="Apache License, Version 2.0",
    description="A Python connector for Druid.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=install_requires,
    extras_require=extras_require,
    tests_require=["pytest"],
    entry_points={
        "console_scripts": ["pydruid = pydruid.console:main"],
        "sqlalchemy.dialects": [
            "druid = pydruid.db.sqlalchemy:DruidHTTPDialect",
            "druid.http = pydruid.db.sqlalchemy:DruidHTTPDialect",
            "druid.https = pydruid.db.sqlalchemy:DruidHTTPSDialect",
        ],
    },
    include_package_data=True,
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
