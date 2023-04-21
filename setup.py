from setuptools import setup, find_packages

VERSION = '0.2.7' 
DESCRIPTION = 'Quokka'
LONG_DESCRIPTION = """
Dope way to do cloud analytics\n
Check out https://github.com/marsupialtail/quokka\n
or https://marsupialtail.github.io/quokka/\n
"""

# Setting up
setup(
        name="pyquokka", 
        version=VERSION,
        author="Tony Wang",
        author_email="zihengw@stanford.edu",
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        package_data = {"":[ "redis.conf", "leader_startup.sh", "common_startup.sh", "leader_start_ray.sh"]},
        install_requires=[
            'cffi',
            'pyarrow',
            'duckdb>=0.6.0',
            'redis',
            'boto3',
            'numpy',
            'pandas',
            #'protobuf==3.20.*', uncomment if Ray does not work on Apple
            'protobuf',
            'ray>=2.0.0',
            'psutil',
            'h5py',
            'polars>=0.17.0', # latest version of Polars generally
            'sqlglot>=11.4.2',
            'graphviz',
            'tqdm',
            'aiohttp',
            'botocore',
            'ldbpy',
            'threadpoolctl',
            'parallel-ssh'
            ], # add any additional packages that 
        extra_requires = {
                "datalake" : ["pyiceberg", "deltalake"]
            },
        license='http://www.apache.org/licenses/LICENSE-2.0',
        keywords=['python'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: POSIX :: Linux",
        ]
)
