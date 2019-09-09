# -*- coding: UTF-8 -*-
import setuptools

with open("README.md", "r") as readme:
  long_desc = readme.read()

setuptools.setup(
  name="ors",
  version="0.1.0",
  author="Nick Newstead",
  author_email="nick.newstead@canada.ca",
  description="A Python package to query the ORS API and store the returned road network distances",
  long_description=long_desc,
  long_description_content_type="text/markdown",
  url="https://github.com/nick-newstead/ors",
  license="MIT",
  packages=setuptools.find_packages(),
  classifiers=[
    "Development Status :: 1 - Planning",
    "License :: OSI Approved :: MIT License",
    "Topic :: Scientific/Engineering :: GIS",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7"
  ],
  project_urls={
    "Documentation": "https://github.com/nick-newstead/ors/wiki",
    "Source code": "https://github.com/nick-newstead/ors/tree/master/ors",
    "Bug tracker": "https://github.com/nick-newstead/ors/issues",
    "License": "https://github.com/nick-newstead/ors/tree/master/LICENSE.md"
  },
  install_requires=["numpy", "pandas", "openrouteservice"],
  python_requires=">=3.5",
  include_package_data=True
)