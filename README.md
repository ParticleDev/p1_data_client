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

# Particle.One Data API client

## Description

This is user-friendly wrapper for P1 Data REST API

## Prerequisites

* Python version >= 3.7

## Installation

```shell script
pip install p1_data_client_python
```

## Quick Start

```python

import p1_data_client_python.client as p1_data

API_URL = '<Put url to your REST API server>'
TOKEN = '<Put your token here>'

client = p1_data.Client(base_url=API_URL, token=TOKEN)

client.get_metadata_type('COMMODITIES')

client.search(text='Price', commodity=['Coal'], country=['Belize', 'Brazil'])

client.get_payload('00158d049d149197f67115a6cc3224e956e5c9e9')
```

## Jupyter notebook

Full function list can be found in
[jupyter notebook](python_client/notebooks/p1_data_client_example.ipynb)

## Tests

To run all tests just run:

```shell script
P1_API_TOKEN=<place your token here>
pytest test -v
```

If you want to run it in parallel:

```shell script
P1_API_TOKEN=<place your token here>
pytest test -v -n 4
```

where "4" is number of workers, as usual is equal number of cores in a
processor.

## License

[MIT License](license.txt)
