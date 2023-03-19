from setuptools import setup

setup(
    name='fetcher',
    packages=['fetcher'],
    version='6.2',
    author='lawn',
    url='https://github.com/lawnn/fetcher.git',
    install_requires=['requests', 'asyncio', 'matplotlib', 'pandas', 'numpy', 'polars', 'pytz']
)
