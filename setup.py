import sys

if sys.version_info[:2] < (3, 7):
    raise ImportError(
        """
    This version of p1_data_client_python don't support python versions less than 3.7
    """
    )

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

INSTALL_REQUIRES = ["pandas>=1.1.1", "requests>=2.24.0"]
TEST_REQUIRES = ["pytest>=6.0.1", "pytest-xdist>=2.1.0"]
PACKAGES = [
    "p1_data_client_python",
]

# TODO: (GP): Please add info to the: author_email, maintainer, maintainer_email
setup(
    name="p1_data_client_python",
    description="Package for P1 Data API access",
    keywords=["p1_data_client_python", "API", "data", "financial", "economic"],
    version="0.0.1",
    author="p1_data_client_python",
    author_email="",
    maintainer="",
    maintainer_email="",
    url="https://github.com/ParticleDev/p1_data_client_python",
    license="MIT",
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=INSTALL_REQUIRES,
    tests_require=TEST_REQUIRES,
    python_requires=">= 3.7",
    test_suite="pytest",
    packages=PACKAGES,
)
