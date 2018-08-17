# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'hcf_backend',
    version      = '0.2',
    packages     = find_packages(),
    install_requires=(
        'frontera==0.7.1',
        'humanize==0.5.1',
        'requests>=2.18.4',
        'retrying>=1.3.3',
        'scrapinghub>=2.0.0',
    ),
    scripts=["bin/hcfpal.py"]
)
