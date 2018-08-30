# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = 'hcf-backend',
    version      = '0.2.3.1',
    description  = 'ScrapyCloud HubStorage frontier backend for Frontera',
    long_description = open('README.rst').read(),
    author_email = 'info@scrapinghub.com',
    license      = 'BSD',
    url          = 'https://github.com/scrapinghub/hcf-backend',
    maintainer   ='Scrapinghub',
    maintainer_email = 'info@scrapinghub.com',
    packages     = find_packages(),
    install_requires = (
        'frontera==0.7.1',
        'humanize==0.5.1',
        'requests>=2.18.4',
        'retrying>=1.3.3',
        'scrapinghub>=2.0.0',
    ),
    scripts = ["bin/hcfpal.py", "bin/hcfmanager.py"],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ]
)
