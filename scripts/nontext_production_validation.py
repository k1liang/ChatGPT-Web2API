"""A2.22 production non-text acceptance matrix.

Frozen matrix:
  D:/Study/workspace/gaifan/.gaifan/temp/nontext-production-matrix-20261010.md

This driver exercises CDPDriver.send_and_stream directly.  It never uses the
A2.21 diagnostic ContractTap as an authority.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

GAIFAN_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_ROOT = GAIFAN_ROOT / ".gaifan" / "temp" / "nontext-production"

IMAGE_TURNS = (
    ("P1", "Generate an image of a red cube on white background."),
    ("P2", "Generate an image of a blue sphere on white background."),
    ("P3", "Generate an image of a green pyramid on white background."),
)
TEXT_TURNS = (
    ("C1", "Reply with exactly: MARKER-A222-1", "MARKER-A222-1"),
    ("C2", "Reply with exactly: MARKER-A222-2", "MARKER-A222-2"),
    ("C3", "Reply with exactly: MARKER-A222-3", "MARKER-A222-3"),
)
IMAGE_TIMEOUT = 180.0
TEXT_TIMEOUT = 90.0


def _projection_zero(stats: dict) -> bool:
    return int((stats or {}).get("count") or 0) == 0


def _runtime_raw_zero(stats: dict) -> bool:
    runtime = (stats or {}).get("non_text_runtime") or {}
    return int(runtime.get("raw_ws_payload_retained") or 0) == 0


def _validate_asset(asset: dict) -> dict:
    result = {
        "file_id": asset.get("file_id"),
        "mime_type": asset.get("mime_type"),
        "size_bytes": asset.get("size_bytes"),
        "width": asset.get("width"),
        "height": asset.get("height"),
        "sha256": asset.get("sha256"),
        "data_url_len": 0,
        "decoded_len": 0,
        "decoded_sha256": None,
        "data_url_prefix_ok": False,
        "contract_ok": False,
        "error": None,
    }
    try:
        file_id = asset["file_id"]
        mime_type = asset["mime_type"]
        size_bytes = int(asset["size_bytes"])
        width = int(asset["width"])
        height = int(asset["height"])
        sha256 = asset["sha256"]
        data_url = asset["data_url"]
        if not isinstance(file_id, str) or not file_id:
            raise ValueError("missing file_id")
        if not isinstance(mime_type, str) or not mime_type.startswith("image/"):
            raise ValueError("invalid mime_type")
        if size_bytes <= 0 or width <= 0 or height <= 0:
            raise ValueError("non-positive declared dimensions/size")
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ValueError("invalid sha256")
        prefix = f"data:{mime_type};base64,"
        result["data_url_len"] = len(data_url) if isinstance(data_url, str) else 0
        result["data_url_prefix_ok"] = isinstance(data_url, str) and data_url.startswith(prefix)
        if not result["data_url_prefix_ok"]:
            raise ValueError("invalid data_url prefix")
        raw = base64.b64decode(data_url[len(prefix):], validate=True)
        decoded_sha = hashlib.sha256(raw).hexdigest()
        result["decoded_len"] = len(raw)
        result["decoded_sha256"] = decoded_sha
        result["contract_ok"] = len(raw) == size_bytes and decoded_sha == sha256
        if not result["contract_ok"]:
            result["error"] = "decoded bytes do not match declared size/hash"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


async def _collect_turn(driver, prompt: str, timeout: float):
    chunks = []
    async for chunk in driver.send_and_stream(prompt, timeout=timeout):
        chunks.append(chunk)
    return chunks


async def run_image_turn(driver, *, label: str, prompt: str) -> dict:
    row = {
        "label": label,
        "kind": "image",
        "prompt": prompt,
        "error": None,
    }
    try:
        chunks = await _collect_turn(driver, prompt, IMAGE_TIMEOUT)
        non_empty = [c for c in chunks if c.delta]
        non_text_chunks = [c for c in chunks if c.non_text_result is not None]
        terminal = chunks[-1] if chunks else None
        payload = non_text_chunks[0].non_text_result if len(non_text_chunks) == 1 else None
        parsed_delta = None
        delta_matches = False
        if len(non_empty) == 1:
            try:
                parsed_delta = json.loads(non_empty[0].delta)
                delta_matches = parsed_delta == payload
            except Exception:
                parsed_delta = None
        asset_checks = []
        if isinstance(payload, dict):
            asset_checks = [_validate_asset(a) for a in payload.get("assets") or []]
        row.update(
            {
                "chunk_count": len(chunks),
                "non_empty_chunk_count": len(non_empty),
                "non_text_chunk_count": len(non_text_chunks),
                "finish_reason": getattr(terminal, "finish_reason", None),
                "payload_type": payload.get("type") if isinstance(payload, dict) else None,
                "payload_source": payload.get("source") if isinstance(payload, dict) else None,
                "conversation_id": (
                    payload.get("conversation_id") if isinstance(payload, dict) else None
                ),
                "turn_exchange_id": (
                    payload.get("turn_exchange_id") if isinstance(payload, dict) else None
                ),
                "asset_message_id": (
                    payload.get("asset_message_id") if isinstance(payload, dict) else None
                ),
                "terminal_message_id": (
                    payload.get("terminal_message_id") if isinstance(payload, dict) else None
                ),
                "delta_matches_payload": delta_matches,
                "asset_count": len(asset_checks),
                "assets": asset_checks,
                "stream_stats": driver.stream_primary_stats,
                "projection_stats": driver.projection_stats,
            }
        )
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["stream_stats"] = driver.stream_primary_stats
        row["projection_stats"] = driver.projection_stats
    return row


async def run_text_turn(
    driver, *, label: str, prompt: str, marker: str
) -> dict:
    row = {
        "label": label,
        "kind": "text",
        "prompt": prompt,
        "marker": marker,
        "error": None,
    }
    try:
        chunks = await _collect_turn(driver, prompt, TEXT_TIMEOUT)
        text = "".join(c.delta for c in chunks if c.delta)
        non_text_count = sum(1 for c in chunks if c.non_text_result is not None)
        terminal = chunks[-1] if chunks else None
        row.update(
            {
                "chunk_count": len(chunks),
                "text_match": text.strip() == marker,
                "text_len": len(text),
                "non_text_chunk_count": non_text_count,
                "finish_reason": getattr(terminal, "finish_reason", None),
                "stream_stats": driver.stream_primary_stats,
                "projection_stats": driver.projection_stats,
            }
        )
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {exc}"
        row["stream_stats"] = driver.stream_primary_stats
        row["projection_stats"] = driver.projection_stats
    return row


def evaluate_gate(rows: list[dict]) -> dict:
    failures: list[dict] = []
    images = [r for r in rows if r.get("kind") == "image"]
    texts = [r for r in rows if r.get("kind") == "text"]
    if len(images) != 3:
        failures.append({"reason": "wrong_image_count", "actual": len(images)})
    if len(texts) != 3:
        failures.append({"reason": "wrong_text_count", "actual": len(texts)})

    file_ids: list[str] = []
    for row in images:
        stats = row.get("stream_stats") or {}
        checks = [
            (row.get("error") is None, "turn_error", row.get("error")),
            (row.get("finish_reason") == "stop", "finish_reason", row.get("finish_reason")),
            (row.get("non_empty_chunk_count") == 1, "non_empty_chunk_count", row.get("non_empty_chunk_count")),
            (row.get("non_text_chunk_count") == 1, "non_text_chunk_count", row.get("non_text_chunk_count")),
            (row.get("delta_matches_payload") is True, "delta_payload_mismatch", row.get("delta_matches_payload")),
            (row.get("payload_type") == "image", "payload_type", row.get("payload_type")),
            (row.get("payload_source") == "websocket_asset_terminal", "payload_source", row.get("payload_source")),
            (bool(row.get("conversation_id")), "missing_conversation_id", row.get("conversation_id")),
            (bool(row.get("turn_exchange_id")), "missing_turn_exchange_id", row.get("turn_exchange_id")),
            (stats.get("completion_source") == "ws_non_text", "completion_source", stats.get("completion_source")),
            (stats.get("non_text_outcome") == "matched", "non_text_outcome", stats.get("non_text_outcome")),
            (stats.get("non_text_resolution") == "matched", "non_text_resolution", stats.get("non_text_resolution")),
            (row.get("asset_count") == 1, "asset_count", row.get("asset_count")),
            (all(a.get("contract_ok") for a in row.get("assets") or []), "asset_contract", row.get("assets")),
            (_projection_zero(row.get("projection_stats") or {}), "projection_gets", row.get("projection_stats")),
            (_runtime_raw_zero(stats), "raw_ws_retained", stats.get("non_text_runtime")),
        ]
        for ok, reason, detail in checks:
            if not ok:
                failures.append({"label": row.get("label"), "reason": reason, "detail": detail})
        for asset in row.get("assets") or []:
            if asset.get("file_id"):
                file_ids.append(asset["file_id"])

    for row in texts:
        stats = row.get("stream_stats") or {}
        checks = [
            (row.get("error") is None, "turn_error", row.get("error")),
            (row.get("text_match") is True, "text_match", row.get("text_match")),
            (row.get("non_text_chunk_count") == 0, "stale_non_text", row.get("non_text_chunk_count")),
            (row.get("finish_reason") == "stop", "finish_reason", row.get("finish_reason")),
            (stats.get("completion_source") in {"stream", "dom_secondary"}, "completion_source", stats.get("completion_source")),
            (stats.get("completion_source") != "ws_non_text", "wrong_non_text_source", stats.get("completion_source")),
            (stats.get("non_text_outcome") != "matched", "non_text_matched_text_turn", stats.get("non_text_outcome")),
            (_projection_zero(row.get("projection_stats") or {}), "projection_gets", row.get("projection_stats")),
        ]
        for ok, reason, detail in checks:
            if not ok:
                failures.append({"label": row.get("label"), "reason": reason, "detail": detail})

    if len(file_ids) != 3 or len(set(file_ids)) != 3:
        failures.append(
            {
                "reason": "image_file_ids_not_unique",
                "file_ids": file_ids,
            }
        )

    return {
        "pass": not failures,
        "image_turns": len(images),
        "text_turns": len(texts),
        "failure_count": len(failures),
        "failures": failures,
        "rows": rows,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", type=int, required=True)
    args = parser.parse_args()

    if os.getenv("W2A_NONTEXT_PRODUCTION") != "1":
        print("REFUSED: set W2A_NONTEXT_PRODUCTION=1 for the live A2.22 matrix")
        return 2

    from chatgpt_web2api.cdp_driver import CDPDriver
    from chatgpt_web2api.chrome import ChromeProcess
    from chatgpt_web2api.config import Config

    run_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    out_dir = OUTPUT_ROOT / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    config = Config()
    config.chrome.cdp_port = args.cdp_port
    chrome = ChromeProcess(config)
    await chrome.ensure_running()
    driver = CDPDriver(
        cdp_port=args.cdp_port,
        tab_mode="owned",
        parallel_tabs=True,
    )
    rows: list[dict] = []

    try:
        await driver.connect()
        for idx in range(3):
            image_label, image_prompt = IMAGE_TURNS[idx]
            text_label, text_prompt, marker = TEXT_TURNS[idx]
            await driver.navigate_new_chat()
            await asyncio.sleep(1.0)

            print(f"[a2.22] {image_label} ...", flush=True)
            image_row = await run_image_turn(
                driver, label=image_label, prompt=image_prompt
            )
            rows.append(image_row)
            print(
                f"[a2.22] {image_label} source="
                f"{(image_row.get('stream_stats') or {}).get('completion_source')} "
                f"assets={image_row.get('asset_count')} "
                f"error={image_row.get('error')}",
                flush=True,
            )

            print(f"[a2.22] {text_label} ...", flush=True)
            text_row = await run_text_turn(
                driver,
                label=text_label,
                prompt=text_prompt,
                marker=marker,
            )
            rows.append(text_row)
            print(
                f"[a2.22] {text_label} source="
                f"{(text_row.get('stream_stats') or {}).get('completion_source')} "
                f"text={text_row.get('text_match')} "
                f"non_text={text_row.get('non_text_chunk_count')} "
                f"error={text_row.get('error')}",
                flush=True,
            )

        gate = evaluate_gate(rows)
        artifact = {
            "run_id": run_id,
            "target_id": driver.target_id,
            "gate": gate,
        }
        artifact_path = out_dir / "production-gate.json"
        with artifact_path.open("w", encoding="utf-8") as f:
            json.dump(artifact, f, ensure_ascii=False, indent=2)
        print(
            f"[a2.22] pass={gate['pass']} failures={gate['failure_count']} "
            f"artifact={artifact_path}",
            flush=True,
        )
        for failure in gate["failures"]:
            print(f"[a2.22] FAIL {failure}", flush=True)
        return 0 if gate["pass"] else 1
    finally:
        try:
            await driver.close()
        except Exception:
            pass
        if chrome.owns_chrome:
            await chrome.stop()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
