#!/usr/bin/env python

import os

"""The setup script."""

from setuptools import setup, find_packages

requirements = [
]

setup_requirements = []

test_requirements = []

setup(
    author="Ralph Brecheisen",
    author_email='r.brecheisen@maastrichtuniversity.nl',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Utilities for interfacing with Castor EDC",
    install_requires=requirements,
    license="MIT license",
    include_package_data=True,
    keywords='barbell2_castor',
    name='barbell2_castor',
    packages=find_packages(include=['barbell2_castor', 'barbell2_castor.*']),
    setup_requires=setup_requirements,
    entry_points={
        'console_scripts': [
        ],
    },
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/rbrecheisen/barbell2_castor',
    version=os.environ['VERSION'],
    zip_safe=False,
)
