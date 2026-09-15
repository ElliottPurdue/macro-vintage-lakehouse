"""Test doubles for the FRED HTTP API, the clock and the S3 client."""

from __future__ import annotations

from botocore.exceptions import ClientError


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None, reason=""):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self.reason = reason

    def json(self):
        if self._body is None:
            raise ValueError("no JSON body")
        return self._body


class FakeSession:
    """Plays back queued responses, or raises queued exceptions, and records each request."""

    def __init__(self, replies):
        self._replies = list(replies)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        reply = self._replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeS3:
    """The S3 calls the pipeline makes, backed by a dict."""

    def __init__(self):
        self.objects = {}
        self.puts = 0

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[(Bucket, Key)] = Body
        self.puts += 1
        return {}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        keys = sorted(key for bucket, key in self.objects if bucket == Bucket and key.startswith(Prefix))
        yield {"Contents": [{"Key": key} for key in keys]} if keys else {"KeyCount": 0}

    def keys(self):
        return sorted(key for _, key in self.objects)
