from setuptools import setup, find_packages

# Core dependencies (always required)
install_requires = [
    "PyMySQL~=1.1.0",
    "psycopg2-binary~=2.9.0", 
    "pymongo~=4.6.0",
    "DBUtils~=3.0.3",
]

# Optional dependencies
extras_require = {
    "sqlalchemy": ["SQLAlchemy~=2.0.0"],
    "all": ["SQLAlchemy~=2.0.0"],  # Install all optional dependencies
}

setup(
    name="longjrm",
    version="0.0.2",
    author="Mike Gong at LONGINFO",
    description="JRM Library for Python",
    packages=find_packages(),
    install_requires=install_requires,
    extras_require=extras_require,
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)