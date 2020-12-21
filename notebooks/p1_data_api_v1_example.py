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

# %% [markdown]
# # P1 REST API
#
# - This Jupyter notebook is an example of how to access the RestAPI interface, described at:
#   https://doc.particle.one/

# %% [markdown]
# # Credentials / Settings

# %%
# %load_ext autoreload
# %autoreload 2

import json
import os

import pandas as pd
import requests

# %%
# Enter your token here.
# You can get your token by signing up at `www.particle.one`.
# P1_API_TOKEN = "YOUR_TOKEN_HERE"
# An example token is like:
# P1_API_TOKEN = "e44e7c6b04ef3ea1cfb7a8a67db74751c177259e"
P1_API_TOKEN = os.environ["P1_API_TOKEN"]
print("P1_API_TOKEN=", P1_API_TOKEN)

HEADERS = {
    "Authorization": f"Token {P1_API_TOKEN}",
    "Content-Type": "application/json",
}

# %% [markdown]
# # Search query structure
#
# Search query is a Python `dict` with the following structure:
# ```python
# query = {
#     "text": "",
#     "commodity": [],
#     "business_category": "",
#     "country": [],
#     "frequency": []
# }
# ```
# The fields are:
# - `text`: string. Works as a filter. Free text. Everything that have no match with this phrase will be filtered out.
# - `commodity`: list of strings. Works as a filter. You can find valid values in paragraph 7.1 of this notebook.
# - `business_category`: string. Works as a filter. You can find valid values in paragraph 7.2 of this notebook.
# - `country`: list of strings. Works as a filter. You can find valid values in paragraph 7.3 of this notebook.
# - `frequency`: list of strings. Works as a filter. You can find valid values in paragraph 7.4 of this notebook.
#
# Combination of fields work with logical operator AND.
# E.g. you will get all records that satisfy all filters.
#
# `text` **AND** `commodity` **AND** `business_category` **AND** `country` **AND** `frequency`

# %% [markdown]
# # Imports


# %% [markdown]
# # POST data-api/v1/search-count/
# Returns count for the given query.

# %%
# Build entrypoint url.
base_url = "https://data.particle.one"
count_url = os.path.join(base_url, "data-api/v1/search-count/")
print("count_url=", count_url)

# %%
# Prepare query.
query = {
    "text": "",
    "commodity": ["Corn"],
    "business_category": "",
    "country": [],
    "frequency": [],
}
payload = json.dumps(query)

# %%
# Perform query.
response = requests.request("POST", count_url, headers=HEADERS, data=payload)
data = json.loads(response.text.encode("utf8"))
print("data=", data)

# %% [markdown]
# # POST data-api/v1/search/
#
# - It returns the first chunk of the payload metadata for the given query, where a chunk is 1000 records.
# - It also returns `scroll_id` to get the next portion of the data.

# %%
search_url = os.path.join(base_url, "data-api/v1/search/")
print("search_url=", search_url)

# %%
# Prepare query.
query = {
    "text": "Gas",
    "commodity": [],
    "business_category": "",
    "country": [],
    "frequency": [],
}
payload = json.dumps(query)

# %%
# Perform query.
response = requests.request("POST", search_url, headers=HEADERS, data=payload)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

assert "detail" not in data, data

print("total_count=", data["total_count"])

# Saving scroll_id for the next query.
scroll_id = data["scroll_id"]
print("scroll_id=", scroll_id)

df = pd.DataFrame.from_records(data["rows"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# # GET data-api/v1/search-scroll/?scroll_id=

# %%
# Build entrypoint url.

# We use scroll id from the previous query.
search_scroll_url = os.path.join(
    base_url, f"data-api/v1/search-scroll/?scroll_id={scroll_id}"
)
print("search_scroll_url=", search_scroll_url)

# %%
# Perform query.

response = requests.request("GET", search_scroll_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))
print("data['rows'][0]=", data["rows"][0])

df = pd.DataFrame.from_records(data["rows"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# # GET data-api/v1/payload/?payload_id=
# Returns payload for the given `payload_id`

# %%
# Build entrypoint url.

# We use one of the `payload_id` from one of the previous queries.
payload_id = "8f26ba4734df3a62352cce9d64987d64da54b400"
payload_url = os.path.join(
    base_url, f"data-api/v1/payload/?payload_id={payload_id}"
)
print("payload_url=", payload_url)

# %%
# Perform query.
response = requests.request("GET", payload_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

df = pd.DataFrame.from_records(data["payload_data"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# # Helpers
#
# To search we use several predefined lists of names for each parameter of metadata. Such as:
# - `commodity`
# - `business-category`
# - `country`
# - `frequency`
#
# Each parameter has its own set of valid names.

# %% [markdown]
# ## GET data-api/v1/commodities/

# %%
# Build entrypoint url.
commodities_url = os.path.join(base_url, "data-api/v1/commodities/")
print("commodities_url=", commodities_url)

# %%
# Perform query.
response = requests.request("GET", commodities_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

df = pd.DataFrame.from_records(data["data"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# ## GET data-api/v1/business-categories/

# %%
# Build entrypoint url.
bc_url = os.path.join(base_url, "data-api/v1/business-categories/")
print("bc_url=", bc_url)

# %%
# Perform query.

response = requests.request("GET", bc_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

df = pd.DataFrame.from_records(data["data"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# ## GET data-api/v1/countries/

# %%
# Build entrypoint url.
countries_url = os.path.join(base_url, "data-api/v1/countries/")
print("countries_url=", countries_url)

# %%
# Perform query.
response = requests.request("GET", countries_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

df = pd.DataFrame.from_records(data["data"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())

# %% [markdown]
# ## GET data-api/v1/frequencies/

# %%
# Build entrypoint url.
frequencies_url = os.path.join(base_url, "data-api/v1/frequencies/")
print(frequencies_url)

# %%
# Perform query.
response = requests.request("GET", frequencies_url, headers=HEADERS)
data = json.loads(response.text.encode("utf8"))
print("data.keys()=", list(data.keys()))

df = pd.DataFrame.from_records(data["data"])
print("df.shape=", df.shape)
print("df.head()=")
display(df.head())
