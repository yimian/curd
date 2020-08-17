from setuptools import setup, find_packages


setup(
    name='yicurd',
    version='1.0.0',
    url='https://github.com/yimian/curd',
    description='db operations',
    license='MIT',
    keywords='db operations',
    packages=find_packages(),
    install_requires=[],
    extras_require={
        'cassandra': ['cassandra-driver==3.11.0'],
        'hbase': ['phoenixdb==0.7'],
        'mysql': ['PyMySQL==0.7.11']
    }
)
