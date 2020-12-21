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
# # P1 data client for REST API
#
# - This Jupyter notebook is an example of how to use the Python Data Client

# %% [markdown]
# ## Initialization

# %% pycharm={"is_executing": false, "name": "#%%\n"}
# %load_ext autoreload
# %autoreload 2

import os

import p1_data_client_python.client as p1_data

# Enter your token here.
# You can get your token by signing up at `www.particle.one`.
# TOKEN = "YOUR_TOKEN_HERE"
# An example token is like:
# TOKEN = "e44e7c6b04ef3ea1cfb7a8a67db74751c177259e"
TOKEN = os.environ["P1_API_TOKEN"]

client = p1_data.Client(token=TOKEN)

# %% [markdown]
# ## Quick start
#
# ### Entities description
#
# There are three main objects:
#
# * [Metadata information](#metadata)
# * [Payload identifiers](#payload)
# * [Time series data](#timeseries)
#
# So, we have make three simple steps:
#
# 1. Get a metadata information for next usage
# 2. Select payload identifiers by metadata values
# 3. Grab time series data by selected payload ID
#
# Let's jump in!
#
# <a id="metadata"></a>
# ## Metadata information
#
# All metadata types listed in our client object:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
print(client.list_of_metadata)

# %% [markdown]
# Right now we have next list of metadata types:
#
# * [COMMODITIES](#commodities)
# * [BUSINESS-CATEGORIES](#business-categories)
# * [COUNTRIES](#countries)
# * [FREQUENCIES](#frequencies)
#
# It can be changed in the future
#
# <a id="commodities"></a>
# ### List of COMMODITIES:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_metadata_type("COMMODITIES")

# %% [markdown]
# <a id="business-categories"></a>
# ### List of BUSINESS-CATEGORIES:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_metadata_type("BUSINESS-CATEGORIES")

# %% [markdown]
# <a id="countries"></a>
# ### List of COUNTRIES:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_metadata_type("COUNTRIES")

# %% [markdown]
# <a id="frequencies"></a>
# ### List of FREQUENCIES:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_metadata_type("FREQUENCIES")

# %% [markdown]
#
# <a id="payload"></a>
# ## Payload identifiers
#
# After we armed be a metadata, we have to use it for obtaining payload identifiers
#
# Let's construct a query for it.
#
# For example we have to know prices for a Coal in the Belize and Brazil:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.search(text="Price", commodity=["Coal"], country=["Belize", "Brazil"])


# %% [markdown]
# The *payload_id* field contain a desired value for next time series search.
#
# Keep in mind that we can put any type of information in the *name* field.
#
# #### Full list parameters for search
#
# - `text`: string. Works as a filter. Free text. Everything that have no match with this phrase will be filtered out.
# - `commodity`: list of [strings](#commodities). Works as a filter.
# - `business_category`: [string](#business-categories). Works as a filter.
# - `country`: list of [strings](#countries). Works as a filter.
# - `frequency`: list of [strings](#frequencies). Works as a filter.
#
# If search conditions will be too broad then server can't return it at one time.
# Right now the every page/chunk of data limited by 1000 lines.
#
# For example we want to get first three page, 3000 lines for a Coal prices:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.search(text="Price", commodity=["Coal"])

# %% [markdown]
# In this case you have to iterate next two pages by the following code:

# %% pycharm={"is_executing": false, "name": "#%%\n"}
for page in client.search_pages(pages_limit=2):
    print(page)

# %% [markdown]
# Keep in mind, that we can iterate over pages after first search only.
#
# <a id="timeseries"></a>
# ## Time series data
#
# And the final step: get time series data!

# %% pycharm={"is_executing": false, "name": "#%%\n"}
client.get_payload("00158d049d149197f67115a6cc3224e956e5c9e9")
