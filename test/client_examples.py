#!/usr/bin/env python3

import os
import pprint

import helpers.io_ as io_
import p1_data_client_python.client as p1_data
import p1_data_client_python.edgar_client as p1_edg

P1_API_TOKEN = os.environ["P1_API_TOKEN"]
print("P1_API_TOKEN=", P1_API_TOKEN)

# Data.
print("Running Data tests...")
client = p1_data.Client(token=P1_API_TOKEN)

client.get_metadata_type("COMMODITIES")

client.search(text="Price", commodity=["Coal"], country=["Belize", "Brazil"])

client.get_payload("00158d049d149197f67115a6cc3224e956e5c9e9")

# Edgar.
print("Running EDGAR tests...")

P1_API_URL = os.environ.get("P1_EDGAR_API_URL")
print("P1_API_URL=", P1_API_URL)

P1_API_TOKEN = os.environ.get("P1_EDGAR_API_TOKEN")
print("P1_API_TOKEN=", P1_API_TOKEN)

client = p1_edg.EdgarClient(token=P1_API_TOKEN)

# Map Gvk to CIK and vice versa.
print("# GvkCikMapper")
gvk_mapper = p1_edg.GvkCikMapper(token=P1_API_TOKEN)
print(
    "result=%s" % gvk_mapper.get_gvk_from_cik(cik=940800, as_of_date="2007-01-18")
)
print(
    "result=%s" % gvk_mapper.get_cik_from_gvk(gvk=61411, as_of_date="2007-01-18")
)

# Get an item mapper.
print("# ItemMapper")
item_mapper = p1_edg.ItemMapper(token=P1_API_TOKEN)
print(
    "result=%s"
    % item_mapper.get_item_from_keywords(keywords="short-term short term")
)
print("result=%s" % item_mapper.get_mapping())

# Get data for form 3, 4, 5.
print("# Form 3, 4, 5")
result = client.get_form4_payload(
    cik=6955, start_date="2011-01-19", end_date="2011-01-19",
)
print("result=\n%s" % pprint.pformat(result, indent=2))

# Get data for form 8.
print("# Form 8")

result = client.get_form8_payload(
    cik=1002910,
    start_date="2021-11-04",
    end_date="2020-11-04",
    item="OIBDPQ",
)
print("result=%s" % result)

# Get data for form10.
print("# Form 10")
payload = client.get_form10_payload(cik=1002910)
file_name = os.path.join(os.path.dirname(__file__), "form10_test.json")
io_.to_json(file_name, {"data": payload})
print("Result saved into %s" % file_name)

# Get data for form 13.
print("# Form 13")
result = client.get_form13_payload(
    start_date="2020-12-10", end_date="2020-12-17",
)
print("result=\n%s" % pprint.pformat(result, indent=2))

#
print("Test completed successfully")
