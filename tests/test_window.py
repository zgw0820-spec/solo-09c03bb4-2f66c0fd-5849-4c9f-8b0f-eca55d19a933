from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.config import EngineConfig, LinkConfig
from app.main import create_app


def make_client(a_delivery=60, a_ack=30, b_delivery=30, b_ack=15) -> TestClient:
    config = EngineConfig(
        link_a=LinkConfig("A", a_delivery, a_ack),
        link_b=LinkConfig("B", b_delivery, b_ack),
    )
    return TestClient(create_app(config))


def wait_for(predicate, timeout_s=8.0, interval_s=0.01):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def links_by_id(client):
    resp = client.get("/api/links")
    assert resp.status_code == 200, resp.text
    return {x["id"]: x for x in resp.json()["links"]}


def received(client, flow_id):
    resp = client.get(f"/api/flows/{flow_id}/received")
    assert resp.status_code == 200, resp.text
    return resp.json()["delivery_order"]


def create_flow(client, fid, link="A"):
    r = client.post("/api/flows", json={"flow_id": fid, "link": link})
    assert r.status_code == 201, r.text
    return r.json()


def send(client, fid, seq, expect=201):
    r = client.post(f"/api/flows/{fid}/packets", json={"seq": seq})
    assert r.status_code == expect, r.text
    return r.json()


@pytest.fixture()
def client():
    with make_client() as c:
        yield c


def test_default_window_is_one_and_listed(client):
    a = links_by_id(client)["A"]
    assert a["window_size"] == 1
    assert a["window"] == 1
    assert a["window_bounds"] == {"min": 1, "max": 64}
    assert a["in_flight"] == []
    assert a["in_flight_packet_ids"] == []
    assert a["in_flight_count"] == 0
    assert a["unacked_packet_ids"] == []


@pytest.mark.parametrize("field", ["window_size", "window", "window_limit", "send_window", "max_in_flight"])
def test_window_can_be_set_with_aliases(client, field):
    r = client.patch("/api/links/A", json={field: 8})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["window_size"] == 8
    assert body["in_flight_limit"] == 8
    assert links_by_id(client)["A"]["window_size"] == 8


def test_window_dedicated_route(client):
    assert client.put("/api/links/A/window", json={"window_size": 12}).status_code == 200
    assert links_by_id(client)["A"]["window_size"] == 12
    assert client.post("/api/links/B/window", json={"window": 3}).status_code == 200
    assert links_by_id(client)["B"]["window_size"] == 3
    r = client.patch("/api/links/A/window?window_size=5", json={})
    assert r.status_code == 200, r.text
    assert links_by_id(client)["A"]["window_size"] == 5


def test_window_range_validation(client):
    for bad in (0, 65, -3):
        r = client.patch("/api/links/A", json={"window_size": bad})
        assert r.status_code == 422, (bad, r.status_code, r.text)
    r = client.put("/api/links/A/window", json={"window_size": 99})
    assert r.status_code == 422
    assert client.patch("/api/links/Z", json={"window_size": 4}).status_code == 404


def test_in_flight_listing_and_slot_release_on_ack(client):
    client.patch("/api/links/A", json={"delivery_delay_ms": 20, "ack_delay_ms": 150, "window_size": 4})
    create_flow(client, "f")
    ids = [send(client, "f", i)["packet"]["packet_id"] for i in range(1, 5)]
    a = links_by_id(client)["A"]
    assert a["window_size"] == 4
    assert a["in_flight_count"] == 4
    assert a["in_flight_packet_ids"] == ids
    assert a["in_flight_packets"] == ids
    assert [p["packet_id"] for p in a["in_flight_packet_details"]] == ids
    assert all(p["status"] in ("delivering", "delivered", "acked") for p in a["in_flight_packet_details"])
    # Slots release as ACKs arrive; eventually empty.
    assert wait_for(lambda: links_by_id(client)["A"]["in_flight_count"] == 0, 5)
    assert wait_for(lambda: len(received(client, "f")) == 4, 5)
    assert [x["seq"] for x in received(client, "f")] == [1, 2, 3, 4]


def test_window_caps_sending_and_fills_from_queue_in_order(client):
    client.patch("/api/links/A", json={"delivery_delay_ms": 30, "ack_delay_ms": 300, "window_size": 2})
    create_flow(client, "f")
    ids = [send(client, "f", i)["packet"]["packet_id"] for i in range(1, 6)]
    a = links_by_id(client)["A"]
    assert a["in_flight_count"] == 2
    assert a["in_flight_packet_ids"] == ids[:2]
    assert a["queue_depth"] == 3
    # After all ACKs everything is sent and delivered in order.
    assert wait_for(lambda: len(received(client, "f")) == 5, 8)
    assert [x["seq"] for x in received(client, "f")] == [1, 2, 3, 4, 5]
    # Hand-off happens at arrival; the final ACK (and slot release) trails it.
    assert wait_for(lambda: links_by_id(client)["A"]["in_flight_count"] == 0, 5)
    assert links_by_id(client)["A"]["queue_depth"] == 0


def test_window_improves_throughput_with_large_ack_delay(client):
    n, d, ack = 8, 20, 250
    # Window 1 baseline: serial delivery+ack per packet.
    client.patch("/api/links/A", json={"delivery_delay_ms": d, "ack_delay_ms": ack, "window_size": 1})
    create_flow(client, "serial")
    t0 = time.monotonic()
    for i in range(1, n + 1):
        send(client, "serial", i)
    assert wait_for(lambda: len(received(client, "serial")) == n, 10)
    serial_elapsed = time.monotonic() - t0

    client.post("/api/reset")
    client.patch("/api/links/A", json={"delivery_delay_ms": d, "ack_delay_ms": ack, "window_size": n})
    create_flow(client, "pipelined")
    t0 = time.monotonic()
    for i in range(1, n + 1):
        send(client, "pipelined", i)
    assert wait_for(lambda: len(received(client, "pipelined")) == n, 10)
    pipe_elapsed = time.monotonic() - t0
    assert [x["seq"] for x in received(client, "pipelined")] == list(range(1, n + 1))
    # Serial needs roughly n*(d+ack); a full window needs about d+ack once.
    assert serial_elapsed > 1.5, serial_elapsed
    assert pipe_elapsed < serial_elapsed / 2, (pipe_elapsed, serial_elapsed)


def test_later_fast_packet_waits_for_earlier_slow_packet(client):
    client.patch("/api/links/A", json={"delivery_delay_ms": 500, "ack_delay_ms": 60, "window_size": 2})
    create_flow(client, "f")
    p1 = send(client, "f", 1)["packet"]["packet_id"]
    # New delivery setting only affects packets sent after the change.
    client.patch("/api/links/A", json={"delivery_delay_ms": 40})
    p2 = send(client, "f", 2)["packet"]["packet_id"]

    # p2 physically arrives quickly but must not be handed to the receiver
    # before slow p1 (~500ms).
    deadline = time.monotonic() + 0.25
    while time.monotonic() < deadline:
        assert received(client, "f") == []
        time.sleep(0.02)
    detail2 = client.get(f"/api/packets/{p2}").json()
    # It physically arrived early (its ACK clock starts there and may already
    # have fired), but it has not been handed to the receiver yet.
    assert detail2["status"] in ("delivered", "acked")
    assert detail2["arrived_at"] is not None and detail2["handed_at"] is None

    assert wait_for(lambda: len(received(client, "f")) == 2, 5)
    order = received(client, "f")
    assert [x["seq"] for x in order] == [1, 2]
    detail2 = client.get(f"/api/packets/{p2}").json()
    assert detail2["handed_at"] is not None
    detail1 = client.get(f"/api/packets/{p1}").json()
    assert detail1["delivery_delay_ms_used"] == 500
    assert detail2["delivery_delay_ms_used"] == 40


def test_lower_window_takes_effect_immediately_and_resumes_below_limit(client):
    # Stagger arrivals/ACKs: p1 slow, p2 fast.
    client.patch("/api/links/A", json={"delivery_delay_ms": 450, "ack_delay_ms": 80, "window_size": 2})
    create_flow(client, "f")
    p1 = send(client, "f", 1)["packet"]["packet_id"]
    client.patch("/api/links/A", json={"delivery_delay_ms": 40})
    p2 = send(client, "f", 2)["packet"]["packet_id"]

    # Lower to 1 while both are in flight; existing packets continue.
    r = client.patch("/api/links/A", json={"window_size": 1})
    assert r.json()["window_size"] == 1
    p3 = send(client, "f", 3)["packet"]["packet_id"]

    a = links_by_id(client)["A"]
    assert a["in_flight_count"] == 2  # already sent packets keep running
    assert a["queue_depth"] == 1      # p3 waits for a slot

    # p2 ACKs around 120ms; occupancy becomes 1, equal to the new limit, so
    # p3 must still not be dispatched while slow p1 is unacknowledged.
    assert wait_for(
        lambda: p2 not in links_by_id(client)["A"]["in_flight_packet_ids"], 3
    )
    time.sleep(0.05)
    a = links_by_id(client)["A"]
    assert p1 in a["in_flight_packet_ids"]
    assert p3 not in a["in_flight_packet_ids"]
    assert a["queue_depth"] == 1
    assert client.get(f"/api/packets/{p3}").json()["status"] == "queued"

    # Once p1 ACKs occupancy drops below 1 and p3 is sent; final order holds.
    assert wait_for(lambda: len(received(client, "f")) == 3, 5)
    assert [x["seq"] for x in received(client, "f")] == [1, 2, 3]


def test_removal_waits_for_full_snapshot_even_if_later_packet_acks_first(client):
    client.patch("/api/links/A", json={"delivery_delay_ms": 450, "ack_delay_ms": 60, "window_size": 2})
    create_flow(client, "f")
    send(client, "f", 1)
    client.patch("/api/links/A", json={"delivery_delay_ms": 40})
    send(client, "f", 2)

    rm = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 5},
    ).json()
    rid = rm["removal_id"]
    assert rm["snapshot_packet_count"] == 2

    # Fast later packet is confirmed first, but the removal must stay pending
    # until slow p1 is confirmed too.
    assert wait_for(
        lambda: client.get(f"/api/removals/{rid}").json()["snapshot_acked"] == 1, 3
    )
    time.sleep(0.1)
    detail = client.get(f"/api/removals/{rid}").json()
    assert detail["status"] == "pending"

    assert wait_for(
        lambda: client.get(f"/api/removals/{rid}").json()["status"] == "succeeded", 5
    )
    assert wait_for(lambda: len(received(client, "f")) == 2, 5)
    assert [x["seq"] for x in received(client, "f")] == [1, 2]


def test_reset_clears_in_flight_and_restores_window_one(client):
    client.patch("/api/links/A", json={"delivery_delay_ms": 30, "ack_delay_ms": 5000, "window_size": 8})
    create_flow(client, "f")
    for i in range(1, 5):
        send(client, "f", i)
    assert links_by_id(client)["A"]["in_flight_count"] == 4

    r = client.post("/api/reset")
    assert r.status_code == 200
    for link in r.json()["links"]:
        assert link["window_size"] == 1
        assert link["in_flight_count"] == 0
        assert link["in_flight_packet_ids"] == []
        assert link["queue_depth"] == 0
    a = links_by_id(client)["A"]
    assert a["window_size"] == 1 and a["in_flight"] == []
