# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.4.2
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
import pprint

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
# ## Payload data

# %% [markdown]
# ### Form8

# %%
def _print_payload(payload: str, n: int = 300) -> None:
    print(pprint.pformat(payload)[:n])


# %% pycharm={"is_executing": false, "name": "#%%\n"}
client = p1_edg.EdgarClient(token=P1_API_TOKEN)

payload = client.get_form8_payload(
    cik=18498, start_date="2020-01-04", end_date="2020-12-04", item="ACT_QUARTER",
)
payload

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_form8_payload(cik=[18498, 319201, 5768])

# %% [markdown]
# ### Form10

# %%
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


# %% [markdown]
# ### Form4
#

# %% [markdown]
# #### Examples of queries

# %%
# Initalize the client.
client = p1_edg.EdgarClient(token=P1_API_TOKEN)

# %% pycharm={"name": "#%%\n"}
# Get the data for one day and one company.
payload = client.get_form4_payload(
    cik=1524358, start_date="2015-10-23", end_date="2020-10-23",
)
_print_payload(payload)

# %%
# Get the data for a week and one company.
payload = client.get_form4_payload(
    cik=1002910, start_date="2015-10-20", end_date="2015-10-27",
)
_print_payload(payload)

# %%
# Get the data for a week and list of companies.
payload = client.get_form4_payload(
    cik=[910521, 883241, 80424], start_date="2020-12-10", end_date="2020-12-17",
)
_print_payload(payload)

# %%
# Get the data for one day for all companies.
payload = client.get_form4_payload(
    start_date="2020-12-17", end_date="2020-12-17",
)
_print_payload(payload)

# %% [markdown] pycharm={"name": "#%% md\n"}
# #### Examples, how to handle and show payload data

# %% pycharm={"name": "#%%\n"}
# Print out a length, and a table names inside a payload.
print("len(payload)=%s" % len(payload))
print("payload.keys()=%s" % payload.keys())

# Show a metadata of a payload.
print('payload["metadata"]=\n%s' % pprint.pformat(payload["metadata"][:2]))

# Print prettified "general_info" table of a payload.
_print_payload(payload["general_info"])

# %% [markdown]
# ### Form4 and Form13
#

# %% [markdown]
# #### Examples of queries

# %% pycharm={"name": "#%%\n"}
# Initalize the client.
client = p1_edg.EdgarClient(token=P1_API_TOKEN)

# %% pycharm={"name": "#%%\n"}
# Form4. Get the data for one day and one company.
payload = client.get_form4_payload(
    cik=1524358, start_date="2015-10-23", end_date="2020-10-23",
)
_print_payload(payload)

# %%
# Form13. Get the data for one day and one company.
payload = client.get_form13_payload(
    cik=1259313, start_date="2015-11-16", end_date="2015-11-16",
)
_print_payload(payload)

# %%
# Form13. Get the data for one day and one cusip.
payload = client.get_form13_payload(
    cusip="01449J204", start_date="2015-11-16", end_date="2015-11-16",
)
_print_payload(payload)

# %%
# Form13. Get the data for one day and list of cusip's.
payload = client.get_form13_payload(
    cusip=["002824100", "01449J204"], start_date="2016-11-15", end_date="2016-11-15",
)
_print_payload(payload)

# %% pycharm={"name": "#%%\n"}
# Form4. Get the data for a week and one company.
payload = client.get_form4_payload(
    cik=1002910, start_date="2015-10-20", end_date="2015-10-27",
)
_print_payload(payload)

# %% pycharm={"name": "#%%\n"}
# Form4. Get the data for a week and list of companies.
payload = client.get_form4_payload(
    cik=[910521, 883241, 80424], start_date="2020-12-10", end_date="2020-12-17",
)
_print_payload(payload)

# %% pycharm={"name": "#%%\n"}
# Form4. Get the data for one day for all companies.
payload = client.get_form4_payload(
    start_date="2020-12-17", end_date="2020-12-17",
)
_print_payload(payload)

# %% [markdown]
# #### Examples, how to handle and show payload data

# %% pycharm={"name": "#%%\n"}
# Print out a length, and a table names inside a payload.
print("len(payload)=%s" % len(payload))
print("payload.keys()=%s" % payload.keys())

# %% pycharm={"name": "#%%\n"}
# Show a metadata of a payload.
print('payload["metadata"]=\n%s' % pprint.pformat(payload["metadata"][:50]))

# %% pycharm={"name": "#%%\n"}
# Print prettified "general_info" table of a payload.
json_str = payload["general_info"][:50]
print(pprint.pformat(payload["general_info"]))
