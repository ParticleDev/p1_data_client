<!--ts-->
   * [Particle.One Data API](#particleone-data-api)
      * [Description](#description)
      * [Prerequisites](#prerequisites)
      * [Installation](#installation)
      * [Getting a Particle.One token](#getting-a-particleone-token)
   * [Quick Start](#quick-start)
      * [Data API](#data-api)
      * [EDGAR Data API](#edgar-data-api)
      * [Run tests](#run-tests)
      * [Useful links](#useful-links)
   * [License](#license)



<!--te-->

# Particle.One Data API

## Description

This package contains a Python client code to access data and examples of how to
use it.

- `p1_data_client_python`
  - A Python module wrapping the Data REST API and EDGAR Data REST API into a
    Pandas-friendly interface
- `notebooks/p1_data_client_example.ipynb`
  - An example Jupyter notebook showing how to use the Python module
- `notebooks/p1_data_api_v1_example.ipynb`
  - An example Jupyter notebook showing how to connect to the Data REST API
    directly
- `notebooks/p1_edgar_data_client_example.ipynb`
  - An example Jupyter notebook showing how to use the EDGAR Data REST API
- `test/client_examples.py`
  - Minimal example of API and EDGAR clients

## Prerequisites

- Python version >= 3.7

## Installation through PyPI

- You can install the Particle.One data python client in 3 different ways:

1) Install from PyPI:
   ```bash
   pip install p1_data_client_python
   ```

2) Install and build from the source
- Assuming the name of the Github repo is <GITHUB_REPO> (e.g.,
  `p1_data_client_python`)
   ```bash
   pip install git+https://github.com/ParticleDev/<INSERT_GITHUB_REPO_NAME>.git
   ```

2) Install without building the package from source:

  ```bash
  # Check out the code.
  git clone git@github.com:ParticleDev/<INSERT_GITHUB_REPO_NAME>.git
  cd p1_data_client_python

  # Create environment.
  python3 -m venv ./venv
  source venv/bin/activate
  pip install -r requirements.txt
  ```

- Test that the package is visible:

  ```bash
  python -c "import p1_data_client_python; print(p1_data_client_python)"
  <module 'p1_data_client_python' (namespace)>

  > python -c "import p1_data_client_python.version as version; print(version.VERSION)"
  ```

- When installing from source, you need to add the path of this package to
  `PYTHONPATH`, e.g.:
  ```bash
  export PYTHONPATH=$(pwd):$(pwd)/p1_data_client_python
  ```

## Getting a Particle.One token

- Go to `https://particle.one/` and request a free token
- The token looks like `e44e7c6b04ef3ea1cfb7a8a67db74751c177259e`

## Configuring through environment variables

- To use the notebooks, unit tests, and examples you need to configure the
  environment with:

  ```bash
  export P1_API_TOKEN='your_api_token_here'
  export P1_EDGAR_API_TOKEN='your_edgar_token_here'
  ```

- E.g., from `test/set_env_vars.sh`
  ```bash
  export P1_API_TOKEN='e44e7c6b04ef3ea1cfb7a8a67db74751c177259e'
  export P1_EDGAR_API_TOKEN='8c9c9458b145202c7a6b6cceaabd82023e957a46d6cf7061ed8e1c94a168f2fd'
  ```

## Run tests (only when installing from source)

- After configuring the environment variables, run all tests with:

  ```bash
  pytest -vv
  =============================================================================================== test session starts ================================================================================================
  platform linux -- Python 3.7.3, pytest-6.0.2, py-1.9.0, pluggy-0.13.1
  rootdir: /wd/saggese/src/p1_data_client_python
  plugins: openfiles-0.4.0, astropy-header-0.1.2, flaky-3.7.0, doctestplus-0.4.0, remotedata-0.3.1, arraydiff-0.3, hypothesis-5.3.0
  collected 20 items

  test/test_client.py ....
  test/test_client_jupyter.py .
  test/test_client_mock.py ....
  test/test_edgar_client.py ........
  test/test_edgar_client_mock.py ...
  ================================================================================================ 9 passed in 3.46s =================================================================================================
  ```

## Notebook

- After configuring the environment variables, run a notebook server with:

  ```bash
  jupyter notebook '--ip=*' --browser chrome . --port 9999
  ```

## Useful links

- GitHub repo
  - `https://github.com/ParticleDev/p1_data_client_python`
- Rest API documentation
  - `https://doc.particle.one`
- Data entry point
  - `https://data.particle.one`

# License

[MIT License](license.txt)
