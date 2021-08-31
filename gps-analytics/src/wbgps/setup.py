from setuptools import setup

setup(name='wbgps',
      version='0.1',
      description='Module for GPS mobility data analysis',
      #  url='http://github.com/ollin18/namehere',
      author='Ollin Demian Langle Chimal, Lucio Melito, Lorenzo Lucchini, Lorenzo Candeago',
      author_email='olanglechimal@worldbank.org',
      license='MIT',
      packages=['wbgps'],
      install_requires=[
          'pandas',
          'numpy',
          'infomap',
          'infostop',
          'pyspark',
          'sklearn',
          'seaborn',
          'datetime'],
      zip_safe=False)
