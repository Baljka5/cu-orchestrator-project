import pytest
from src.orchestration.router import route

def test_route_threshold():
    assert route("policy", 0.1) == "other"
