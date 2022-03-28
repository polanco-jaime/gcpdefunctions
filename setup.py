from setuptools import setup, find_packages
from codecs import open
from os import path

__author__ = 'Jaime Polanco'
__license__ = "japj182"
__email__ = 'jaime.polanco@javeriana.edu.co'

# Get the long description from the README file
# with open(path.join(here, 'README.md'), encoding='utf-8') as f:
#    long_description = f.read()

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()


setup(name='gcpdataengfunctions',
      version='0.1.1',
      license='Jaime Polanco Development',
      #package_dir={'':'gdp_dataeng_functions'},
      packages=find_packages("gcpdataengfunctions", exclude=["*.test", "*.test.*", "test.*", "test"]) ,
      description='Ways for downloading the last file loaded into a specific bucket and way for loading this data into bigquery',
      url='https://github.com/JAPJ182/gcpdataengfunctions',
      author='Jaime Polanco',
      author_email='jaime.polanco@javeriana.edu.co',
      classifiers=[
          # How mature is this project? Common values are
          #   3 - Alpha
          #   4 - Beta
          #   5 - Production/Stable
          'Development Status :: 4 - Beta ',

          # Indicate who your project is intended for
          'Intended Audience :: Developers',
          'Topic :: Software Development :: Build Tools',
          # Specify the Python versions you support here. In particular, ensure
          # that you indicate whether you support Python 2, Python 3 or both.
          'Programming Language :: Python',
          'Programming Language :: Python :: 3'
      ],
      keywords='dynamic-networks',
      install_requires=requirements,
      long_description=long_description,
      long_description_content_type='text/markdown',
#       packages=find_packages(exclude=["*.test", "*.test.*", "test.*", "test", "dynetx.test", "dynetx.test.*"]),
      )
