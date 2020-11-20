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
- A Python module `p1_data_client_python` wrapping the P1 Data REST API 
and P1 Edgar Data REST API to a pandas-friendly interface
- An example notebook on how to use the Python module
  `notebook/p1_data_client_example.ipynb`

- An example Jupyter notebook on how to connect to the P1 Data REST API directly:
  `notebooks/p1_data_api_v1_example.ipynb`

## Prerequisites

- Python version >= 3.7

## Installation

- To install from PyPI:
  ```shell script
  > pip install p1_data_client_python
  ```

- To install the packages from source:
  ```
  > python3 -m venv ./venv
  > source venv/bin/activate
  > pip install -r requirements.txt
  # Ensure the package is visible. If not you need to add to PYTHONPATH the path
  # of this package.
  > python -c "import p1_data_client_python; print(p1_data_client_python)"
  <module 'p1_data_client_python' (namespace)>
  ```

## Getting a Particle.One token

- Go to `https://particle.one/` and request a free token
- The token looks like `e44e7c6b04ef3ea1cfb7a8a67db74751c177259e`

## Quick Start

### Data API

```python
import p1_data_client_python.client as p1_data

API_URL = "https://data.particle.one"
TOKEN = '<Put your token here>'

client = p1_data.Client(base_url=API_URL, token=TOKEN)

client.get_metadata_type('COMMODITIES')

client.search(text='Price', commodity=['Coal'], country=['Belize', 'Brazil'])

client.get_payload('00158d049d149197f67115a6cc3224e956e5c9e9')
```

### Edgar Data API

```python

import p1_data_client_python.edgar_client as p1_edg

TOKEN = '<Put your token here>'

client = p1_edg.EdgarClient(token=TOKEN)
client.get_payload(form_name='8-K',
                   cik=1002910,
                   start_date='2021-11-04',
                   end_date='2020-11-04',
                   items=['OIBDPQ', 'NIQ']
                   )

gvkey_mapper = p1_edg.GvkeyCikMapper(token=TOKEN)
gvkey_mapper.get_gvkey_from_cik(cik='0000940800', as_of_date='2007-01-18')
gvkey_mapper.get_cik_from_gvkey(gvkey='061411', as_of_date='2007-01-18')

item_mapper = p1_edg.CompustatItemMapper(token="1234567890")
item_mapper.get_item_from_keywords(keywords=['short-term', 'short term'])
item_mapper.get_mapping()
``` 

## Run tests

- To run all tests with `bash` just run:
  ```bash
  > export P1_API_TOKEN=<place your token here>
  > pytest -x
  =============================================================================================== test session starts ================================================================================================
  platform linux -- Python 3.7.3, pytest-6.0.2, py-1.9.0, pluggy-0.13.1
  rootdir: /wd/saggese/src/p1_data_client_python
  plugins: openfiles-0.4.0, astropy-header-0.1.2, flaky-3.7.0, doctestplus-0.4.0, remotedata-0.3.1, arraydiff-0.3, hypothesis-5.3.0
  collected 9 items

  test/test_client.py ....                                                                                                                                                                                     [ 44%]
  test/test_client_jupyter.py .                                                                                                                                                                                [ 55%]
  test/test_client_mock.py ....                                                                                                                                                                                [100%]

  ================================================================================================ 9 passed in 3.46s =================================================================================================
``

## License

[MIT License](license.txt)
