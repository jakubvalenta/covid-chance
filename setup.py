from pathlib import Path

from setuptools import find_packages, setup

from coronavirus_opportunity_bot import __title__

setup(
    name='coronavirus-opportunity-bot',
    version='0.1.0',
    description=__title__,
    long_description=(Path(__file__).parent / 'README.md').read_text(),
    url='https://www.github.com/jakubvalenta/coronavirus-opportunity-bot',
    author='Jakub Valenta',
    author_email='jakub@jakubvalenta.cz',
    license='Apache Software License',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: End Users/Desktop',
        'Topic :: Utilities',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(),
    install_requires=['feedparser', 'luigi'],
)
