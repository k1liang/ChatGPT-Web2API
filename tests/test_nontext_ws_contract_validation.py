from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "nontext_ws_contract_validation.py"
SPEC = importlib.util.spec_from_file_location("nontext_ws_contract_validation", SCRIPT)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


def test_download_metadata_url_replays_a220_observed_context():
    assert mod._download_metadata_url("file_abc123", "conv-1") == (
        "/backend-api/files/download/file_abc123?conversation_id=conv-1"
        "&download_intent=false&inline=true&include_library_file_state=true"
    )


def _asset_message(
    *,
    message_id="tool-1",
    parent_id="user-1",
    turn_id="turn-1",
    status="finished_successfully",
    role="tool",
    channel="final",
    mime="image/png",
    size=123,
    width=64,
    height=32,
):
    return {
        "id": message_id,
        "author": {"role": role},
        "status": status,
        "recipient": "all",
        "channel": channel,
        "metadata": {
            "parent_id": parent_id,
            "turn_exchange_id": turn_id,
            "working_turn_id": turn_id,
        },
        "content": {
            "content_type": "multimodal_text",
            "parts": [
                {
                    "content_type": "image_asset_pointer",
                    "asset_pointer": "sediment://file_abc123",
                    "mime_type": mime,
                    "size_bytes": size,
                    "width": width,
                    "height": height,
                }
            ],
        },
    }


def _end_message(*, message_id="end-1", turn_id="turn-1", end_turn=True):
    return {
        "id": message_id,
        "author": {"role": "assistant"},
        "status": "finished_successfully",
        "end_turn": end_turn,
        "recipient": "all",
        "channel": "final",
        "metadata": {
            "turn_exchange_id": turn_id,
            "working_turn_id": turn_id,
        },
        "content": {"content_type": "text", "parts": [""]},
    }


def _payload(*messages):
    import json

    return json.dumps(
        {
            "type": "message",
            "payload": {"conversation_id": "conv-1", "messages": list(messages)},
            "metadata": None,
        }
    )


def test_parse_valid_asset_and_terminal():
    assets, terminals = mod.parse_ws_payload(
        _payload(_asset_message(), _end_message())
    )
    assert len(assets) == 1
    assert len(terminals) == 1
    asset = assets[0]
    pointer = asset.pointers[0]
    assert asset.conversation_id == "conv-1"
    assert asset.parent_id == "user-1"
    assert asset.turn_exchange_id == "turn-1"
    assert pointer.file_id == "file_abc123"
    assert pointer.mime_type == "image/png"
    assert pointer.size_bytes == 123
    assert terminals[0].conversation_id == "conv-1"
    assert terminals[0].turn_exchange_id == "turn-1"


def test_parse_rejects_nonfinal_or_incomplete_asset_contract():
    cases = [
        _asset_message(status="in_progress"),
        _asset_message(role="assistant"),
        _asset_message(channel="analysis"),
        _asset_message(mime="text/plain"),
        _asset_message(size=0),
        _asset_message(width=0),
        _asset_message(height=0),
    ]
    for message in cases:
        assets, _ = mod.parse_ws_payload(_payload(message))
        assert assets == []


def test_parse_rejects_working_turn_mismatch():
    message = _asset_message()
    message["metadata"]["working_turn_id"] = "other"
    assets, _ = mod.parse_ws_payload(_payload(message))
    assert assets == []

    end = _end_message()
    end["metadata"]["working_turn_id"] = "other"
    _, terminals = mod.parse_ws_payload(_payload(end))
    assert terminals == []


def test_parse_rejects_nonterminal_assistant():
    _, terminals = mod.parse_ws_payload(_payload(_end_message(end_turn=False)))
    assert terminals == []


def test_parse_ignores_bad_json_without_retaining_text():
    assert mod.parse_ws_payload("not-json") == ([], [])


def test_resolution_contract_requires_exact_file_size_and_download_id():
    pointer = mod.ImagePointer(
        file_id="file_abc123",
        asset_pointer="sediment://file_abc123",
        mime_type="image/png",
        size_bytes=123,
        width=64,
        height=32,
    )
    good = {
        "meta_http_status": 200,
        "status": "success",
        "download_path": "/backend-api/estuary/content",
        "download_id": "file_abc123",
        "asset_http_status": 200,
        "content_type": "image/png",
        "byte_len": 123,
        "sha256": "a" * 64,
    }
    assert mod._asset_resolution_ok(pointer, good) is True
    assert mod._asset_resolution_ok(pointer, {**good, "byte_len": 122}) is False
    assert mod._asset_resolution_ok(pointer, {**good, "download_id": "file_other"}) is False


def test_dom_oracle_requires_message_id_dimensions_and_loaded_image():
    pointer = mod.ImagePointer(
        file_id="file_abc123",
        asset_pointer="sediment://file_abc123",
        mime_type="image/png",
        size_bytes=123,
        width=64,
        height=32,
    )
    asset = mod.AssetMessage(
        message_id="tool-1",
        conversation_id="conv-1",
        parent_id="user-1",
        turn_exchange_id="turn-1",
        working_turn_id="turn-1",
        pointers=(pointer,),
    )
    event = {
        "state": {
            "gallery_count": 1,
            "images": [
                {
                    "message_ids": "tool-1",
                    "complete": True,
                    "natural_width": 64,
                    "natural_height": 32,
                }
            ],
        }
    }
    oracle = mod._dom_image_oracle(event, asset)
    assert oracle == {
        "gallery_count": 1,
        "image_count": 1,
        "message_id_match": True,
        "dimension_match": True,
        "loaded": True,
    }


def test_gate_rejects_stale_asset_accepted_by_text_turn():
    image = {
        "label": "I1",
        "kind": "image",
        "anchor": "u1",
        "asset": {"message_id": "a1"},
        "terminal": {"message_id": "e1"},
        "resolved": [{"contract_ok": True}],
        "dom_oracle": {
            "loaded": True,
            "message_id_match": True,
            "dimension_match": True,
        },
        "matching_asset_count": 1,
        "projection_stats": {"count": 0},
        "raw_ws_payload_retained": 0,
        "error": None,
    }
    text = {
        "label": "T1",
        "kind": "text",
        "anchor": "u2",
        "text_match": True,
        "matching_asset_count": 1,
        "projection_stats": {"count": 0},
        "raw_ws_payload_retained": 0,
        "error": None,
    }
    gate = mod.evaluate_gate([image, image, image, text, text, text])
    assert gate["pass"] is False
    assert any(f["reason"] == "stale_asset_accepted" for f in gate["failures"])
