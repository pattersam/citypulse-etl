import os
from setuptools import setup

setup(
    name = "citypulse-etl",
    version = "0.0.1",
    author = "Sam Patterson",
    description = "A Python extract, transform, load (ETL) pipeline for the CityPulse Smart City dataset.",
    license = "MIT",
    keywords = "citypulse extract-transform-load etl",
    url = "https://gitlab.com/s-a-m/citypulse-etl",
    package_dir={'':'src'},
    packages=['citypulse_etl'],
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
    install_requires=[
        'pandas',
        'python-dotenv',
        'requests',
        'SQLAlchemy',
    ],
    entry_points = {
        'console_scripts': ['citypulse-etl=citypulse_etl.cli:main'],
    }
)
