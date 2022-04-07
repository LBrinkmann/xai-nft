from setuptools import setup, find_packages
import sys
import os.path


def load_requirements(filename='requirements.txt'):
    try:
        with open(filename) as f:
            lines = f.readlines()
        return lines
    except:
        return ''


setup(
    name='xai',
    version="0.0.1",
    description='',
    url='',
    author='Levin Brinkmann',
    author_email='',
    license='',
    packages=[package for package in find_packages()
              if package.startswith('xai')],
    zip_safe=False,
    install_requires=load_requirements(),
    extras_require={'dev': load_requirements('requirements_dev.txt')},
    scripts=[
    ],
    entry_points={
        'console_scripts': [

        ],
    },
)
