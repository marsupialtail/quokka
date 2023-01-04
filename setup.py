from setuptools import setup, find_packages

VERSION = '0.1.4' 
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
        package_data = {"":["redis.conf", "leader_startup.sh"]},
        install_requires=['pyarrow',
            'redis',
            'boto3',
            'pandas',
            'numpy',
            'protobuf==3.20.*', # or Ray will not work
            'ray',
            'psutil',
            'h5py',
            'polars==0.14.*', # latest version,0.13 has some breaking APIs
            'sqlglot',
            'graphviz'
            ], # add any additional packages that 
        license='http://www.apache.org/licenses/LICENSE-2.0',
        keywords=['python'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: POSIX :: Linux",
        ]
)
