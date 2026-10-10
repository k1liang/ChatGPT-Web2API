from __future__ import annotations

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "nontext_channel_discovery.py"
SPEC = importlib.util.spec_from_file_location("nontext_channel_discovery", SCRIPT)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mod)


def test_extract_user_message_id_uses_last_user_message():
    body = (
        '{"messages":['
        '{"id":"u1","author":{"role":"user"}},'
        '{"id":"a1","author":{"role":"assistant"}},'
        '{"id":"u2","author":{"role":"user"}}]}'
    )
    assert mod._extract_user_message_id(body) == "u2"
    assert mod._extract_user_message_id("{}") is None
    assert mod._extract_user_message_id("not-json") is None


def test_normalize_endpoint_drops_query_and_fragment():
    assert (
        mod._normalize_endpoint(
            "GET",
            "https://example.com/path/to/a.png?sig=secret#frag",
        )
        == "GET https://example.com/path/to/a.png"
    )
    assert mod._normalize_endpoint("GET", "blob:https://chatgpt.com/abc") is None


def test_body_probe_candidate_is_broad_but_excludes_main_stream_and_large_bodies():
    base = {
        "mime_type": "application/json",
        "resource_type": "Fetch",
        "encoded_data_length": 100,
        "url": "https://chatgpt.com/backend-api/image/foo",
    }
    assert mod._body_probe_candidate(base) is True
    assert mod._body_probe_candidate({**base, "url": "https://chatgpt.com/backend-api/f/conversation"}) is False
    assert mod._body_probe_candidate({**base, "encoded_data_length": 3 * 1024 * 1024}) is False
    assert mod._body_probe_candidate({**base, "resource_type": "Image"}) is False


def test_payload_excerpt_is_bounded_and_hashed():
    out = mod._payload_excerpt("abcdef", limit=3)
    assert out["len"] == 6
    assert out["text"] == "abc"
    assert out["truncated"] is True
    assert len(out["sha256"]) == 64


def test_summarize_nominates_repeatable_s8_only_endpoint_and_excludes_probe():
    turns = [
        {"label": "S1-r1", "scenario": "S1", "repeat": 1},
        {"label": "S1-r2", "scenario": "S1", "repeat": 2},
        {"label": "S1-r3", "scenario": "S1", "repeat": 3},
        {"label": "S8-r1", "scenario": "S8", "repeat": 1},
        {"label": "S8-r2", "scenario": "S8", "repeat": 2},
        {"label": "S8-r3", "scenario": "S8", "repeat": 3},
    ]
    common = "GET https://chatgpt.com/backend-api/common"
    image = "GET https://chatgpt.com/backend-api/image/result"
    probe = "GET https://cdn.example.com/probe.png"
    events = []
    for label in ("S1-r1", "S1-r2", "S1-r3", "S8-r1", "S8-r2", "S8-r3"):
        events.append(
            {
                "event": "Network.requestWillBeSent",
                "label": label,
                "phase": "send",
                "endpoint": common,
            }
        )
    for label in ("S8-r1", "S8-r2", "S8-r3"):
        events.append(
            {
                "event": "Network.requestWillBeSent",
                "label": label,
                "phase": "send",
                "endpoint": image,
            }
        )
        events.append(
            {
                "event": "Network.requestWillBeSent",
                "label": label,
                "phase": "asset_probe",
                "endpoint": probe,
            }
        )

    summary = mod.summarize_capture(turns, events, {})
    assert summary["endpoint_presence"]["S1"][common] == 3
    assert summary["endpoint_presence"]["S8"][common] == 3
    assert summary["endpoint_presence"]["S8"][image] == 3
    assert probe not in summary["endpoint_presence"]["S8"]
    assert summary["image_specific_endpoint_candidates"] == [
        {"endpoint": image, "s8_turns": 3, "s1_turns": 0}
    ]


def test_summarize_keeps_realtime_and_body_keyword_evidence():
    turns = [
        {"label": "S1-r1", "scenario": "S1", "repeat": 1},
        {"label": "S8-r1", "scenario": "S8", "repeat": 1},
    ]
    events = [
        {
            "event": "Network.webSocketFrameReceived",
            "label": "S8-r1",
            "phase": "send",
            "request_id": "ws1",
            "endpoint": "WS wss://example.com/socket",
            "opcode": 1,
            "keyword_hits": ["image"],
        }
    ]
    requests = {
        "r1": {
            "label": "S8-r1",
            "phase": "send",
            "endpoint": "GET https://example.com/result",
            "body_keyword_hits": ["asset", "image"],
            "body_len": 50,
        }
    }
    summary = mod.summarize_capture(turns, events, requests)
    assert summary["realtime_events"][0]["request_id"] == "ws1"
    assert summary["response_body_keyword_hits"][0]["hits"] == ["asset", "image"]
