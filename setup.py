from setuptools import setup, find_packages

with open('requirements.txt') as f:
    reqs = f.read()

setup(
    name='elasticsearching',
    version='0.1',
    packages=find_packages(include=["esutil"]),
    license='LICENSE',
    long_description=open('README.md').read(),
    install_requires=reqs.strip().split('\n'),
)