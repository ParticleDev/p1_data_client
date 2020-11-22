#!/usr/bin/env python3

# export P1_API_URL="https://data.particle.one"; export P1_API_TOKEN='e44e7c6b04ef3ea1cfb7a8a67db74751c177259e'
# export P1_EDGAR_API_URL="https://data.dev.alpha.service.particle.one/edgar/v1"; export P1_EDGAR_API_TOKEN='8c9c9458b145202c7a6b6cceaabd82023e957a46d6cf7061ed8e1c94a168f2fd'

import os
import p1_data_client_python.client as p1_data
import p1_data_client_python.edgar_client as p1_edg

P1_API_URL = os.environ["P1_API_URL"]
print("P1_API_URL=", P1_API_URL)

P1_API_TOKEN = os.environ["P1_API_TOKEN"]
print("P1_API_TOKEN=", P1_API_TOKEN)

# Data.
print("Running Data tests...")
client = p1_data.Client(base_url=P1_API_URL, token=P1_API_TOKEN)

client.get_metadata_type('COMMODITIES')

client.search(text='Price', commodity=['Coal'], country=['Belize', 'Brazil'])

client.get_payload('00158d049d149197f67115a6cc3224e956e5c9e9')

# Edgar.
print("Running EDGAR tests...")

P1_API_URL = os.environ["P1_EDGAR_API_URL"]
print("P1_API_URL=", P1_API_URL)

P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]
print("P1_API_TOKEN=", P1_API_TOKEN)

client = p1_edg.EdgarClient(base_url=P1_API_URL, token=P1_API_TOKEN)

# Map GVKEY to CIK and vice versa.
gvkey_mapper = p1_edg.GvkeyCikMapper(base_url=P1_API_URL, token=P1_API_TOKEN)
gvkey_mapper.get_gvkey_from_cik(cik='0000940800', as_of_date='2007-01-18')
gvkey_mapper.get_cik_from_gvkey(gvkey='061411', as_of_date='2007-01-18')

# Get Compustat item mapper.
item_mapper = p1_edg.CompustatItemMapper(base_url=P1_API_URL, token=P1_API_TOKEN)
item_mapper.get_item_from_keywords(keywords='short-term short term')
item_mapper.get_mapping()

# Get data.
client.get_payload(form_name='8-K',
   cik=1002910,
   start_date='2021-11-04',
   end_date='2020-11-04',
   item='OIBDPQ'
   )
