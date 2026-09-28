"""Structural regressions for the retired polling detector.

Production send_and_stream no longer uses CompletionDetector. These checks keep
the DOM-drift fixes locked in the legacy detector itself.
"""

import inspect

from chatgpt_web2api.completion_detector import CompletionDetector


def test_has_action_js_walks_depth_8():
    src = inspect.getsource(CompletionDetector.stream_until_complete)
    assert "d <= 8" in src
    assert "d <= 4" not in src


def test_has_action_js_geometry_accepts_above_message():
    src = inspect.getsource(CompletionDetector.stream_until_complete)
    assert "lastRect.top - 180" in src
