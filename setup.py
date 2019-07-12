import glob
import os
from setuptools import setup, find_packages

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

setup(name='flashflood',
      version='0.0.1',
      description='Event recorder providing fast, scalable playback.',
      url='https://github.com/xbrianh/flashflood.git',
      author='Brian Hannafious',
      author_email='bhannafi@ucsc.edu',
      license='MIT',
      packages=find_packages(exclude=['tests']),
      scripts=glob.glob('scripts/*'),
      zip_safe=False,
      install_requires=install_requires,
      platforms=['MacOS X', 'Posix'],
      test_suite='test',
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 3.6'
      ]
      )
