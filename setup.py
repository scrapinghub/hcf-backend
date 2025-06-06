# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name="hcf-backend",
    version="0.6.0",
    description="ScrapyCloud HubStorage frontier backend for Frontera",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    license="BSD",
    url="https://github.com/scrapinghub/hcf-backend",
    maintainer="Scrapinghub",
    packages=find_packages(),
    install_requires=(
        "frontera>=0.7.2,<0.8",
        "humanize>=0.5.1",
        "requests>=2.18.4",
        "retrying>=1.3.3",
        "scrapinghub>=2.3.1",
        "shub-workflow>=1.10.20",
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
