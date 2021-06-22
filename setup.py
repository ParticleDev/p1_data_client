import os
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

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "p1_data_client_python")
)

import version  # NOQA

INSTALL_REQUIRES = ["pandas>=1.0.0",
                    "requests>=2.18.0",
                    "tqdm>=4.50.0",
                    "halo>=0.0.31"]
TEST_REQUIRES = ["pytest>=5.0.0"]
PACKAGES = [
    "p1_data_client_python",
    "p1_data_client_python.helpers",
    "p1_data_client_python.edgar"
]

project_urls = {
    "Site": "https://particle.one/",
    "API registration": "https://particle.one/api-access",
}

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    version=version.VERSION,
    name="p1_data_client_python",
    description="Package for P1 Data API access",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=[
        "p1_data_client_python",
        "API",
        "data",
        "financial",
        "economic",
        "particle",
        "particleone",
        "particle.one",
    ],
    author="ParticleOne inc.",
    author_email="malanin@particle.one",
    maintainer="",
    maintainer_email="",
    url="https://github.com/ParticleDev/p1_data_client",
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
    project_urls=project_urls,
)
