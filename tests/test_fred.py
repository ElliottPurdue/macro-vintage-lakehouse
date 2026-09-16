import logging

import pytest
import requests

from fakes import FakeResponse, FakeSession, FakeTime
from macro_lake.fred import FredClient, FredError

KEY = "0123456789abcdef0123456789abcdef"

ROWS = [
    {"date": f"2024-0{month}-01", "value": str(month), "realtime_start": "2024-06-07", "realtime_end": "9999-12-31"}
    for month in range(1, 6)
]


def make_client(replies, **options):
    session, fake_time = FakeSession(replies), FakeTime()
    options.setdefault("min_interval", 0.0)
    client = FredClient(KEY, session=session, sleep=fake_time.sleep, clock=fake_time.clock, **options)
    return client, session, fake_time


def observations_page(rows, count):
    return FakeResponse(body={"count": count, "observations": rows})


def test_pages_through_every_vintage():
    client, session, _ = make_client([observations_page(ROWS[:3], 5), observations_page(ROWS[3:], 5)])
    assert client.observations_all_vintages("TEST") == ROWS
    assert [params["offset"] for _, params in session.calls] == [0, 3]
    for url, params in session.calls:
        assert url.endswith("/series/observations")
        assert (params["realtime_start"], params["realtime_end"]) == ("1776-07-04", "9999-12-31")
        assert (params["api_key"], params["file_type"]) == (KEY, "json")


def test_an_as_of_date_narrows_the_real_time_window():
    client, session, _ = make_client([observations_page(ROWS[:1], 1)])
    client.observations_all_vintages("TEST", as_of="2024-06-28")
    _, params = session.calls[0]
    assert (params["realtime_start"], params["realtime_end"]) == ("1776-07-04", "2024-06-28")


def test_count_changing_between_pages_is_an_error():
    client, _, _ = make_client([observations_page(ROWS[:3], 5), observations_page(ROWS[3:], 6)])
    with pytest.raises(FredError, match="count changed from 5 to 6"):
        client.observations_all_vintages("TEST")


def test_empty_page_before_the_count_is_reached_is_an_error():
    client, _, _ = make_client([observations_page(ROWS[:3], 5), observations_page([], 5)])
    with pytest.raises(FredError, match="empty page at offset 3 of 5"):
        client.observations_all_vintages("TEST")


def test_retries_rate_limits_and_server_errors_with_backoff():
    client, _, fake_time = make_client(
        [
            FakeResponse(429, {"error_code": 429, "error_message": "Too Many Requests."}),
            FakeResponse(503, {}),
            FakeResponse(body={"seriess": [{"id": "TEST"}]}),
        ]
    )
    assert client.series("TEST") == {"id": "TEST"}
    assert fake_time.sleeps == [2.0, 4.0]
    assert client.requests_made == 3


def test_retry_after_header_sets_the_wait():
    client, _, fake_time = make_client(
        [FakeResponse(429, {}, headers={"Retry-After": "7"}), FakeResponse(body={"seriess": [{"id": "TEST"}]})]
    )
    client.series("TEST")
    assert fake_time.sleeps == [7.0]


def test_client_errors_are_not_retried():
    client, session, _ = make_client(
        [FakeResponse(400, {"error_code": 400, "error_message": "Bad Request.  The series does not exist."})]
    )
    with pytest.raises(FredError, match="HTTP 400: Bad Request.  The series does not exist."):
        client.series("NOPE")
    assert len(session.calls) == 1


def test_gives_up_after_max_attempts():
    client, session, _ = make_client([FakeResponse(500, {})] * 3, max_attempts=3)
    with pytest.raises(FredError, match="HTTP 500"):
        client.series("TEST")
    assert len(session.calls) == 3


def test_api_key_stays_out_of_errors_and_logs(caplog):
    leak = requests.ConnectionError(f"Max retries exceeded with url: /fred/series?series_id=TEST&api_key={KEY}")
    client, _, _ = make_client([leak, leak], max_attempts=2)
    with caplog.at_level(logging.WARNING), pytest.raises(FredError) as raised:
        client.series("TEST")
    assert KEY not in str(raised.value)
    assert "<FRED_API_KEY>" in str(raised.value)
    assert raised.value.__context__ is None
    assert KEY not in caplog.text
    assert "<FRED_API_KEY>" in caplog.text


def test_requests_are_spaced_out():
    client, _, fake_time = make_client([FakeResponse(body={"seriess": [{}]})] * 3, min_interval=0.5)
    for _ in range(3):
        client.series("TEST")
    assert fake_time.sleeps == [0.5, 0.5]
