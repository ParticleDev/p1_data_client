<!--ts-->
   * [Particle.One Data API client](#particleone-data-api-client)
      * [Description](#description)
      * [Prerequisites](#prerequisites)
      * [Installation](#installation)
      * [Quick Start](#quick-start)
      * [Jupyter notebook](#jupyter-notebook)
      * [Tests](#tests)
      * [License](#license)



<!--te-->

# Particle.One Data API

## Description

This package contains:
- `p1_data_client_python` 
  - A Python module wrapping the Data REST API and Edgar Data REST API into a
    Pandas-friendly interface
- `notebooks/p1_data_client_example.ipynb`
  - An example Jupyter notebook showing how to use the Python module
- `notebooks/p1_edgar_data_client_example.ipynb`
  - An example Jupyter notebook showing how to use the Edgar Data REST API
- `notebooks/p1_data_api_v1_example.ipynb`
  - An example Jupyter notebook showing how to connect to the Data REST API
    directly

## Prerequisites

- Python version >= 3.7

## Installation

- To install from PyPI:
  ```bash
  > pip install p1_data_client_python
  ```

- To install from source:
  ```bash
  # Check out the code.
  > git clone git@github.com:ParticleDev/p1_data_client_python.git
  > cd p1_data_client_python

  # Create environment.
  > python3 -m venv ./venv
  > source venv/bin/activate
  > pip install -r requirements.txt

  # Test.
  # Ensure the package is visible.
  # If not you need to add to PYTHONPATH the path of this package.
  # > export PYTHONPATH=$PYTHONPATH:$(pwd)
  > python -c "import p1_data_client_python; print(p1_data_client_python)"
  <module 'p1_data_client_python' (namespace)>
  > export P1_API_TOKEN='your_token_here'
  # E.g, export P1_API_TOKEN = 'e44e7c6b04ef3ea1cfb7a8a67db74751c1772590'
  > pytest -x
  ```

## Getting a Particle.One token

- Go to `https://particle.one/` and request a free token
- The token looks like `e44e7c6b04ef3ea1cfb7a8a67db74751c1772590`

# Quick Start

## Data API

- An example of how to use the Data API from Python is:
  ```python
  import p1_data_client_python.client as p1_data

  API_URL = "https://data.particle.one"
  TOKEN = '<your_token_here>'
  # E.g., TOKEN = 'e44e7c6b04ef3ea1cfb7a8a67db74751c1772590'

  client = p1_data.Client(base_url=API_URL, token=TOKEN)

  client.get_metadata_type('COMMODITIES')

  client.search(text='Price', commodity=['Coal'], country=['Belize', 'Brazil'])

  client.get_payload('00158d049d149197f67115a6cc3224e956e5c9e0')
  ```

## Edgar Data API

- An example of how to use the Edgar API from Python is:
  ```python
  import p1_data_client_python.edgar_client as p1_edg

  TOKEN = '<your_token_here>'
  # E.g., TOKEN = 'e44e7c6b04ef3ea1cfb7a8a67db74751c1772590'

  client = p1_edg.EdgarClient(token=TOKEN)

  # Map GVKEY to CIK and vice versa.
  gvkey_mapper = p1_edg.GvkeyCikMapper(token=TOKEN)
  gvkey_mapper.get_gvkey_from_cik(cik='0000940800', as_of_date='2007-01-18')
  gvkey_mapper.get_cik_from_gvkey(gvkey='061411', as_of_date='2007-01-18')

  # Get Compustat item mapper.
  item_mapper = p1_edg.CompustatItemMapper(token="1234567890")
  item_mapper.get_item_from_keywords(keywords='short-term short term')
  item_mapper.get_mapping()

  # Get data.
  client.get_payload(form_name='8-K',
       cik=1002910,
       start_date='2021-11-04',
       end_date='2020-11-04',
       item='OIBDPQ'
       )
  ``` 

## Run tests

- To run all tests with `bash` just run:
  ```bash
  > export P1_API_TOKEN='your_token_here'
  # E.g, export P1_API_TOKEN = 'e44e7c6b04ef3ea1cfb7a8a67db74751c1772590'
  > pytest -x
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
``

## Useful links

- GitHub repo
  - https://github.com/ParticleDev/p1_data_client_python
- Rest API documentation
  - https://doc.particle.one/
- Data entry point
  - https://data.particle.one/

# License

[MIT License](license.txt)
