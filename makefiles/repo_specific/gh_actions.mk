# #############################################################################
# GH Actions
# #############################################################################

# Run fast tests.
fast_test_gh_actions:
	PYTHONPATH=$(shell pwd):$(shell pwd)/p1_data_client_python \
	pytest -vv
