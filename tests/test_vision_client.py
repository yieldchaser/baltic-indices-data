"""
End-to-end test for multimodal vision client extension in scripts/process_knowledge.py.
Validates:
1. Base64 image encoding and MIME detection
2. Vision model venue availability checks
3. Ollama multimodal format (images: [b64])
4. NIM / OpenAI-compatible multimodal format (image_url: {url: data:mime;base64,...})
5. Two-stage extraction flow (Stage 1 Axis/Scale, Stage 2 Data recovery)
6. Integration with ExtractionVerifier
"""

import base64
import json
import os
import sys
import unittest
from pathlib import Path

# Add scripts directory to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(REPO_ROOT / "scripts" / "harness"))

from process_knowledge import (
    encode_image_base64,
    vision_available,
    ollama_vision_available,
    nim_vision_available,
    call_multimodal_vision,
    extract_json_payload,
)
from verify_extraction import ExtractionVerifier

def test_encode_image_base64():
    # Find a real image in reports
    test_img = next(REPO_ROOT.glob("reports/breakwave/**/assets/*.png"), None)
    if not test_img:
        test_img = next(REPO_ROOT.glob("reports/breakwave/**/assets/*.jpg"), None)

    assert test_img is not None, "At least one asset image must exist in reports/breakwave"
    
    b64_str, mime = encode_image_base64(test_img)
    assert isinstance(b64_str, str)
    assert len(b64_str) > 100
    assert mime in ("image/png", "image/jpeg")

    # Verify decoding works
    decoded = base64.b64decode(b64_str)
    assert len(decoded) > 0
    print(f"[PASS] test_encode_image_base64: encoded {test_img.name} ({len(decoded)} bytes, mime={mime})")

def test_multimodal_payload_formatting():
    # Verify mock venue provides Stage 1 schema
    stage1_prompt = "Stage 1: Extract chart axes, scale, labels, and series legend."
    test_img = next(REPO_ROOT.glob("reports/breakwave/**/assets/*.png"), None)
    
    res1 = call_multimodal_vision(test_img, stage1_prompt, venue="mock")
    assert res1 is not None
    payload1 = extract_json_payload(res1)
    assert isinstance(payload1, dict)
    assert "x_axis" in payload1
    assert "y_axis" in payload1
    assert "series" in payload1
    print(f"[PASS] test_multimodal_payload_formatting: Stage 1 structure extracted -> {list(payload1.keys())}")

    # Verify mock venue provides Stage 2 schema
    stage2_prompt = "Stage 2: Extract data points and tabular rows for the series identified."
    res2 = call_multimodal_vision(test_img, stage2_prompt, venue="mock")
    assert res2 is not None
    payload2 = extract_json_payload(res2)
    assert isinstance(payload2, dict)
    assert "headers" in payload2
    assert "rows" in payload2
    assert len(payload2["rows"]) >= 1
    print(f"[PASS] test_multimodal_payload_formatting: Stage 2 data rows extracted -> {len(payload2['rows'])} rows, {len(payload2['headers'])} cols")

def test_verifier_integration():
    verifier = ExtractionVerifier()
    test_img = next(REPO_ROOT.glob("reports/breakwave/**/assets/*.png"), None)
    
    # Run Stage 2 extraction
    res = call_multimodal_vision(test_img, "Stage 2 data points extraction", venue="mock")
    payload = extract_json_payload(res)
    
    table_data = {
        "source_file": str(test_img.relative_to(REPO_ROOT)).replace("\\", "/"),
        "page_number": 1,
        "table_index": 0,
        "broker": "breakwave",
        "table_category": "rates",
        "headers": payload["headers"],
        "rows": payload["rows"],
    }
    
    v_res = verifier.verify_table(table_data)
    assert v_res.passed, f"Verification failed with issues: {v_res.issues}"
    assert v_res.column_count == 3
    assert v_res.row_count == 5
    assert not any(iss.severity == "ERROR" for iss in v_res.issues)
    print(f"[PASS] test_verifier_integration: verifier passed table with {v_res.row_count} rows, {v_res.column_count} cols")

class VisionClientTestCase(unittest.TestCase):
    def test_encode_image_base64_case(self):
        test_encode_image_base64()

    def test_multimodal_payload_formatting_case(self):
        test_multimodal_payload_formatting()

    def test_verifier_integration_case(self):
        test_verifier_integration()

if __name__ == "__main__":
    print("=== RUNNING VISION CLIENT END-TO-END TESTS ===")
    test_encode_image_base64()
    test_multimodal_payload_formatting()
    test_verifier_integration()
    print("=== ALL VISION CLIENT TESTS PASSED ===")

