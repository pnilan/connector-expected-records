from setuptools import setup, find_packages

setup(
    name='connector_expected_records',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
      'deepdiff'
    ],
    entry_points={
        'console_scripts': [
            'connector_expected_records=connector_expected_records.main:main',
        ],
    },
    extras_require={
      'test': [
        'pytest',
        'pytest-cov'
      ]
    }
)
