login.hpc.kuleuven.be#!/usr/bin/env python

from setuptools import setup, find_packages

#one line description
with open('DESCRIPTION') as F:
    description = F.read().strip()

#version number
with open('VERSION') as F:
    version = F.read().strip()

entry_points = {
    'console_scripts': [
        'm3 = mad3.cli:dispatch'
        ]}

setup(name='mad3',
      version=version,
      description=description,
      author='',
      author_email='',
      entry_points = entry_points,
      include_package_data=True,
      url='https://encrypted.google.com/#q=mad3&safe=off',
      packages=find_packages(),
      install_requires=[
                'Leip',
                ],
      classifiers = [
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        ]
     )
