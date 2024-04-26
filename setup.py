from setuptools import setup, find_packages

with open('requirements.txt') as f:
    install_requirement = f.readlines()

setup(
    name='utilspy',
    version='0.1.0',
    packages=find_packages(),
    install_requires=install_requirement,
    author='many98',
    author_email="",
    description='A simple connector and utility package',
    url='https://github.com/Many98/utilspy',
    python_requires=">=3.8",
)
