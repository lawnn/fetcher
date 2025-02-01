from setuptools import setup

setup(
    name='fetcher',
    packages=['fetcher'],
    version='8.0.0',
    author='lawn',
    url='https://github.com/lawnn/fetcher.git',
    install_requires=['requests', 'asyncio', 'matplotlib', 'plotly', 'pandas', 'numpy', 'polars', 'pytz', 'ccxt']
)
