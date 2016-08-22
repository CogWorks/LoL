import os.path

from setuptools import setup, find_packages
from pip.req import parse_requirements

__version__ = '1.0'

descr_file = os.path.join(os.path.dirname(__file__), 'readme.txt')
setup_dir = os.path.dirname(os.path.realpath(__file__))
path_req = os.path.join(setup_dir, 'requirements.txt')
install_reqs = parse_requirements(path_req, session=False)

reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='LoL',
    version=__version__,
    
    packages=['lol'],

    description='LoL Scraper allows you to use Riot-Watcher to scrape into MySQL.',
    long_description=open(descr_file).read(),
    author='Matthew-Donald Sangster',
    url='https://github.com/CogWorks/LoL',
    classifiers=[
        'License :: RPI',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Text Processing',
        'Topic :: Games/Entertainment :: Real Time Strategy',
        'Topic :: Games/Entertainment :: Role-Playing'
    ],
    license='RPI',
    install_requires=reqs,
 )
