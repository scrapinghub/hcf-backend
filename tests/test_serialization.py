from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from os import environ
from unittest.mock import MagicMock

import pytest

from hcf_backend import HCFBackend


@dataclass(frozen=True)
class Request:
    url: str
    method: str = "GET"
    headers: dict = field(default_factory=dict)
    cookies: dict = field(default_factory=dict)
    meta: dict = field(default_factory=dict)


class MockManager:
    def __init__(self):
        self.settings = {
            "HCF_PROJECT_ID": -1,
            "HCF_PRODUCER_FRONTIER": True,
            "HCF_CONSUMER_FRONTIER": True,
            "HCF_CONSUMER_SLOT": "toscrape.com",
            "STATS_MANAGER": MagicMock(),
        }
        self.request_model = Request


class MockProducer:
    def __init__(self, storage):
        self.storage = storage

    def add_request(self, slot: str, request):
        assert isinstance(request["fp"], str)
        self.storage.append(request)

    def flush(self):
        pass


class MockConsumer:
    def __init__(self, storage):
        self.storage = storage

    def read(self, slot: str, mincount: int | None = None):
        yield {
            "id": int("00013967d8af7b0001", 16),
            "requests": [
                [request["fp"], request.get("qdata", None)] for request in self.storage
            ],
        }

    def delete(self, slot: str, batch_ids: list[int]):
        self.storage.clear()


@pytest.mark.parametrize(
    "request_",
    [
        Request(
            url="url",
            meta={b"frontier_fingerprint": "cf26ac1ad2ab62ff35cc46773fa1acea5cdbca96"},
        ),
    ],
)
def test_add_seeds(request_):
    original_request = deepcopy(request_)

    environ["PROJECT_ID"] = "-1"
    environ["SH_APIKEY"] = "foo"
    backend = HCFBackend(manager=MockManager())
    backend.frontier_start()
    storage = []
    backend.producer = MockProducer(storage)
    backend.consumer = MockConsumer(storage)

    seeds = [request_]
    backend.add_seeds(seeds)
    processed_requests = backend.get_next_requests(max_next_requests=len(seeds))
    for request in processed_requests:
        request.meta.pop(b"created_at")
        request.meta.pop(b"depth")
    assert [original_request] == processed_requests


def test_add_seeds_binary_fingerprint():
    environ["PROJECT_ID"] = "-1"
    environ["SH_APIKEY"] = "foo"
    backend = HCFBackend(manager=MockManager())
    backend.frontier_start()
    storage = []
    backend.producer = MockProducer(storage)
    backend.consumer = MockConsumer(storage)

    request = Request(
        url="url",
        meta={
            b"frontier_fingerprint": b"\xcf&\xac\x1a\xd2\xabb\xff5\xccFw?\xa1\xac\xea\\\xdb\xca\x96"
        },
    )
    seeds = [request]

    with pytest.raises(ValueError, match=r"must be a string"):
        backend.add_seeds(seeds)
