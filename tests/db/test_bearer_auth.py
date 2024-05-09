# -*- coding: utf-8 -*-

from pydruid.db.api import BearerAuth
import pytest
import requests
import unittest


class TestBearerAuth:

    # BearerAuth object is initialized with a token
    def test_initialized_with_token(self):
        token = "my_token"
        auth = BearerAuth(token)
        assert auth.token == token

    # The token is None
    def test_token_is_none(self):
        token = None
        auth = BearerAuth(token)
        assert auth.token is None

    # The __call__ method adds an Authorization header with the token to the request object
    def test_adds_authorization_header(self):
        token = "my_token"
        auth = BearerAuth(token)
        request = requests.Request()
        modified_request = auth(request)
        assert modified_request.headers["Authorization"] == f"Bearer {token}"

    # The __call__ method returns the modified request object
    def test_returns_modified_request_object(self):
        token = "my_token"
        auth = BearerAuth(token)
        request = requests.Request()
        modified_request = auth(request)
        assert modified_request is request

    # The token is an empty string
    def test_token_is_empty_string(self):
        token = ""
        auth = BearerAuth(token)
        request = requests.Request()
        modified_request = auth(request)
        assert modified_request.headers["Authorization"] == "Bearer "
