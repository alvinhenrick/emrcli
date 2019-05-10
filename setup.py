from os import path, getcwd

from setuptools import setup, find_packages

package_name = 'emrcli'

try:
    with open(path.join(getcwd(), 'VERSION')) as version_file:
        version = version_file.read().strip()
except IOError:
    raise


def parse_requirements(file):
    with open(file, "r") as fs:
        return [r for r in fs.read().splitlines() if
                (len(r.strip()) > 0 and not r.strip().startswith("#") and not r.strip().startswith("--"))]


requirements = parse_requirements('requirements.txt')
test_requirements = parse_requirements('requirements-dev.txt')

setup(name=package_name,
      version=version,
      license='MIT License,',
      description='EMR Command Line Utility',
      author='Alvin Henrick',
      author_email='share.code@aol.com',
      url='https://github.com/alvinhenrick/emr_cli',
      packages=find_packages(exclude=['configs', 'tests']),
      install_requires=requirements,
      tests_require=test_requirements,
      include_package_data=True,
      entry_points={
          'console_scripts': [
              'emrcli = aws.utils.commands:cli'
          ]
      },
      zip_safe=False)
