from setuptools import setup, find_packages

# Core dependencies (always required)
install_requires = [
    "DBUtils>=3.0.3",
    "cryptography>=41.0.0",
]

# Optional dependencies
extras_require = {
    "mysql": ["PyMySQL>=1.1.0"],
    "postgres": ["psycopg[binary]>=3.1.0"],
    "oracle": ["oracledb>=2.0.0"],
    "sqlserver": ["pyodbc>=4.0.39"],
    "db2": ["ibm_db>=3.2.0"],
    "spark": ["pyspark>=3.3.0", "delta-spark>=2.3.0"],
    "sqlalchemy": ["SQLAlchemy>=2.0.0"],
    "all": [
        "PyMySQL>=1.1.0",
        "psycopg[binary]>=3.1.0",
        "oracledb>=2.0.0",
        "pyodbc>=4.0.39",
        "ibm_db>=3.2.0",
        "ibm-db-sa>=0.4.0",
        "pyspark>=3.3.0",
        "delta-spark>=2.3.0",
        "SQLAlchemy>=2.0.0"
    ],
}

setup(
    name="longjrm",
    version="0.1.2",
    author="Mike Gong at LONGINFO",
    description="JRM Library for Python",
    packages=find_packages(),
    include_package_data=True, 
    package_data={
        'longjrm': [
            'connection/driver_map.json',
            # Add other non-Python files here
        ],
    },
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",

        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)