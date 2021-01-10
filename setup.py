from pathlib import Path

from setuptools import find_packages, setup

from covid_chance import __title__

setup(
    name='covid-chance',
    version='0.2.1',
    description=__title__,
    long_description=(Path(__file__).parent / 'README.md').read_text(),
    url='https://www.github.com/jakubvalenta/covid-chance',
    author='Jakub Valenta',
    author_email='jakub@jakubvalenta.cz',
    license='GNU General Public License v3 or later (GPLv3+)',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Artistic Software',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',  # noqa: E501
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    packages=find_packages(),
    install_requires=[
        'beautifulsoup4>=4.9.0',
        'colored',
        'feedparser',
        'jinja2',
        'lxml',
        'psycopg2',
        'python-twitter',
        'regex',
        'requests',
        'sqlalchemy',
    ],
    entry_points={'console_scripts': ['covid-chance=covid_chance.cli:main']},
)
