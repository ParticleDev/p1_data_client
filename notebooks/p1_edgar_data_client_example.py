# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown] pycharm={"name": "#%% md\n"}
# # P1 Edgar data client for REST API

# %% [markdown]
# ## Initialization

# %% pycharm={"is_executing": false, "name": "#%%\n"}
# %load_ext autoreload
# %autoreload 2

import os
import json
import pprint
from typing import Any

import pandas as pd

if False:
    import sys
    sys.path.append("/commodity_research/p1_data_client_python_private")
    print(sys.path)
    os.environ["P1_API_TOKEN"]='e44e7c6b04ef3ea1cfb7a8a67db74751c177259e'
    os.environ["P1_EDGAR_API_TOKEN"]='8c9c9458b145202c7a6b6cceaabd82023e957a46d6cf7061ed8e1c94a168f2fd'

import p1_data_client_python.edgar_client as p1_edg

# Enter your token here.
# You can get your token by signing up at `www.particle.one`.
# P1_API_TOKEN = "YOUR_TOKEN_HERE"
# An example token is like:

P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]
print("P1_API_TOKEN=", P1_API_TOKEN)


# %% [markdown]
# ## Quick start
#
# There are 3 steps:
# 1. Get information about company identifiers
# 2. Get information about financial items available
# 3. Download data
#
# ## Mappers
#
# ### GvkCikMapper
#
# It handles CIK <-> GVK transformation.

# %% pycharm={"is_executing": false, "name": "#%%\n"}
gvk_mapper = p1_edg.GvkCikMapper(token=P1_API_TOKEN)
gvk_mapper.get_gvk_from_cik(cik=940800, as_of_date="2007-01-18")

# %% pycharm={"is_executing": false, "name": "#%%\n"}
gvk_mapper.get_cik_from_gvk(gvk=61411, as_of_date="2007-01-18")

# %% [markdown]
# ### ItemMapper
#
# It provides mapping between keywords and description of Compustat items.

# %% pycharm={"is_executing": false, "name": "#%%\n"}
item_mapper = p1_edg.ItemMapper(token=P1_API_TOKEN)
item_mapper.get_item_from_keywords(keywords=["short-term", "short term"])

# %% pycharm={"is_executing": false, "name": "#%%\n"}
item_mapper.get_mapping()

# %% [markdown]
# ## Metadata

# %%
client = p1_edg.EdgarClient(token=P1_API_TOKEN)


# %%
def display_df(df: pd.DataFrame) -> None:
    print("num_rows=%s" % df.shape[0])
    display(df.head(3))

    
def print_payload(payload: str, n: int = 300) -> None:
    print(pprint.pformat(payload)[:n])


# %%
# Get forms for a subset of forms and CIKs.
headers = client.get_form_headers(
            form_type=['13F-HR', '10-K', '3', '4'],
            cik=[918504, 1048286, 5272, 947263, 1759760, 320193],
            start_date='2020-10-30',
            end_date='2020-10-30',
)
display_df(headers)

# %%
# Get forms for a subset of forms and all CIKs for 1 year.
headers = client.get_form_headers(
            form_type=['4'],
            cik=None,
            start_date='2020-01-01',
            end_date='2020-01-31',
)
display_df(headers)

# %% [markdown]
# ## Payload data

# %% [markdown]
# ### Form8

# %% pycharm={"is_executing": false, "name": "#%%\n"}
# Get all Form8 data for one CIK, one item in a range of time.
payload = client.get_form8_payload(
    cik=18498, start_date="2020-01-04", end_date="2020-12-04", item="ACT_QUARTER",
)
display_df(payload)

# %% pycharm={"is_executing": false, "name": "#%%\n"}
# Get all Form8 data for multiple CIK, all items, and entire period of time.
payload = client.get_form8_payload(cik=[18498, 319201, 5768])
display_df(payload)

# %% [markdown]
# ### Form4
#

# %% [markdown]
# #### Examples of queries

# %% pycharm={"is_executing": false}
# Initalize the client.
client = p1_edg.EdgarClient(token=P1_API_TOKEN)

# %% pycharm={"name": "#%%\n"}
# Get Form4 data for one CIK and one day, as dataframe.
payload = client.get_form4_payload(
    cik=1524358, start_date="2015-10-23", end_date="2020-10-23", output_type="dataframes"
)

# %%
payload.keys()

# %%
display_df(payload['general_info'])

# %%
# Get Form4 data for one CIK and a week.
payload = client.get_form4_payload(
    cik=1002910, start_date="2015-10-20", end_date="2015-10-27",
)

# %%
# Get Form4 data for multiple CIKs and a week.
payload = client.get_form4_payload(
    cik=[910521, 883241, 80424], start_date="2020-12-10", end_date="2020-12-17", output_type="dataframes"
)

# %%
display_df(payload['metadata'])

# %% pycharm={"name": "#%%\n"}
# Get Form4 data for all companies and one day.
payload = client.get_form4_payload(
    start_date="2020-12-17", end_date="2020-12-17",
)
print_payload(payload)

# %% [markdown] pycharm={"name": "#%% md\n"}
# #### How to handle and show payload data

# %% pycharm={"name": "#%%\n"}
# Print out a length, and a table names inside a payload.
print("len(payload)=%s" % len(payload))
print("payload.keys()=%s" % payload.keys())

# Show a metadata of a payload.
print('payload["metadata"]=\n%s' % pprint.pformat(payload["metadata"][:2]))

# Print prettified "general_info" table of a payload.
print_payload(payload["general_info"])

# %% [markdown]
# ### Form13

# %% [markdown]
# #### Examples of queries

# %% pycharm={"name": "#%%\n"}
# Initalize the client.
client = p1_edg.EdgarClient(token=P1_API_TOKEN)

# %%
# Get Form13 data for one filer as CIK and one day.
payload = client.get_form13_payload(
    cik=1259313, start_date="2015-11-16", end_date="2015-11-16",
)
display_df(payload['metadata'])

# %% pycharm={"is_executing": false, "name": "#%%\n"}
# Get Form13 data for one filed company as CUSIP and one day.
payload = client.get_form13_payload(
    cusip="01449J204", start_date="2015-11-16", end_date="2015-11-16"
)
print_payload(payload)


# %% pycharm={"name": "#%%\n"}
# Get Form13 data for a list of CUSIPs and one day.
payload = client.get_form13_payload(
    cusip=["002824100", "01449J204"], start_date="2016-11-15", end_date="2016-11-15", output_type="dataframes"
)
print_payload(payload)

# %% [markdown]
# #### How to handle and show payload data

# %% pycharm={"name": "#%%\n"}
# Print out a length, and a table names inside a payload.
print("len(payload)=%s" % len(payload))
print("payload.keys()=%s" % payload.keys())

# %% pycharm={"name": "#%%\n"}
# Show a metadata of a payload.
display_df(payload["metadata"])

# %% [markdown]
# ### Form10

# %%
# Get Form10 data for one CIK and 2 days.
payload = client.get_form10_payload(
    cik=1002910, start_date="2020-05-11", end_date="2020-05-12",
)

# %%
print("len(payload)=%s" % len(payload))
print("payload[0].keys()=%s" % payload[0].keys())

# %%
print('payload[0]["meta"]=\n%s' % pprint.pformat(payload[0]["meta"]))

# %%
json_str = payload[0]["data"]
print(pprint.pformat(payload[0]["data"])[:2000])

