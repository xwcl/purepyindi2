from setuptools import setup
from os import path

HERE = path.abspath(path.dirname(__file__))
PROJECT = 'purepyindi2'

with open(path.join(HERE, 'README.md'), encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

with open(path.join(HERE, PROJECT, 'VERSION'), encoding='utf-8') as f:
    VERSION = f.read().strip()

extras = {
    'dev': ['pytest'],
    'indi2influx': ['influxdb-client'],
    'ipyindi': ['IPython'],
    'speedup': ['ciso8601'],
}
all_deps = set()
for _, deps in extras.items():
    for dep in deps:
        all_deps.add(dep)
extras['all'] = list(all_deps)

setup(
    name=PROJECT,
    version=VERSION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    packages=[PROJECT],
    python_requires='>=3.9, <4',
    install_requires=[],
    package_data={  # Optional
        PROJECT: ['VERSION'],
    },
    extras_require=extras,
    entry_points={
        'console_scripts': [
            f'ipyindi={PROJECT}.commands.ipyindi:main',
            f'indiproxy={PROJECT}.commands.indiproxy:main',
            f'indi2influx={PROJECT}.commands.indi2influx:main',
            f'indi2json={PROJECT}.commands.indi2json:main',
        ],
    },
    project_urls={  # Optional
        'Bug Reports': f'https://github.com/xwcl/{PROJECT}/issues',
    },
)
