"""Comprehensive pytest & unittest suite for the GoHighLevel Lead Scoring Service."""

from __future__ import annotations

import base64
import json
import os
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

try:
    import pytest
except ImportError:  # pragma: no cover - graceful fallback when pytest is not installed
    pytest = None  # type: ignore[assignment]

from src.handlers.lambda_function import (
    build_mock_api_gateway_event,
    lambda_handler,
)
from src.models.lead_event import LeadEvent, normalize_custom_fields
from src.rules.scoring_rules import (
    DEFAULT_SCORING_RULES,
    LeadScoringEngine,
    ScoringRule,
)
from src.services.ghl_pipeline_manager import GHLAPIError, GHLPipelineManager


# ---------------------------------------------------------------------------
# 1. Scoring Rules Engine Tests (Positive, Negative, Budget, Overrides)
# ---------------------------------------------------------------------------


class TestScoringRulesEngine(unittest.TestCase):
    """Test suite for positive/negative scoring rules and custom overrides."""

    def setUp(self) -> None:
        self.engine = LeadScoringEngine()

    def test_positive_rule_booked_appointment(self) -> None:
        event = LeadEvent.from_webhook_payload(
            {"contactId": "cnt_01", "activity": "Booked Appointment"}
        )
        result = self.engine.evaluate(event)
        self.assertEqual(result.points_delta, 20)
        self.assertEqual(result.matched_rule_names, ["Booked Appointment"])

    def test_positive_rule_clicked_sms_link(self) -> None:
        event = LeadEvent.from_webhook_payload(
            {"contactId": "cnt_02", "activity": "Clicked SMS Link"}
        )
        result = self.engine.evaluate(event)
        self.assertEqual(result.points_delta, 10)
        self.assertEqual(result.matched_rule_names, ["Clicked SMS Link"])

    def test_positive_rule_form_submitted(self) -> None:
        event = LeadEvent.from_webhook_payload(
            {"contactId": "cnt_03", "activity": "Form Submitted"}
        )
        result = self.engine.evaluate(event)
        self.assertEqual(result.points_delta, 15)
        self.assertEqual(result.matched_rule_names, ["Form Submitted"])

    def test_positive_rule_budget_over_5k_variations(self) -> None:
        passing_payloads = [
            {"contactId": "c1", "customFields": {"Budget > $5k": True}},
            {"contactId": "c2", "customFields": {"budget": "Budget > $5k"}},
            {"contactId": "c3", "customFields": {"budget": "> $5k"}},
            {"contactId": "c4", "customFields": {"budget": "$7,500"}},
            {"contactId": "c5", "customFields": {"budget": 10000}},
            {
                "contactId": "c6",
                "customFields": [
                    {"id": "cf_1", "key": "contact.budget", "field_value": "$6.5k"}
                ],
            },
        ]
        for payload in passing_payloads:
            with self.subTest(payload=payload):
                result = self.engine.evaluate(payload)
                self.assertEqual(result.points_delta, 5)
                self.assertIn("Budget > $5k", result.matched_rule_names)

    def test_budget_under_or_equal_5k_does_not_trigger(self) -> None:
        non_matching_payloads = [
            {"contactId": "c1", "customFields": {"Budget > $5k": False}},
            {"contactId": "c2", "customFields": {"budget": "$3,000"}},
            {"contactId": "c3", "customFields": {"budget": 5000}},
            {"contactId": "c4", "customFields": {"budget": "< $5k"}},
            {"contactId": "c5", "customFields": {"budget": "under $5,000"}},
        ]
        for payload in non_matching_payloads:
            with self.subTest(payload=payload):
                result = self.engine.evaluate(payload)
                self.assertEqual(result.points_delta, 0)
                self.assertNotIn("Budget > $5k", result.matched_rule_names)

    def test_negative_rule_unsubscribed(self) -> None:
        event = LeadEvent.from_webhook_payload(
            {"contactId": "cnt_04", "activity": "Unsubscribed"}
        )
        result = self.engine.evaluate(event)
        self.assertEqual(result.points_delta, -15)
        self.assertEqual(result.matched_rule_names, ["Unsubscribed"])
        self.assertEqual(len(result.negative_matches), 1)

    def test_negative_rule_no_show(self) -> None:
        event = LeadEvent.from_webhook_payload(
            {"contactId": "cnt_05", "activity": "No Show"}
        )
        result = self.engine.evaluate(event)
        self.assertEqual(result.points_delta, -10)
        self.assertEqual(result.matched_rule_names, ["No Show"])

    def test_combined_positive_and_negative_signals(self) -> None:
        payload = {
            "contactId": "cnt_combo",
            "activities": [
                "Booked Appointment",  # +20
                "Clicked SMS Link",    # +10
                "Form Submitted",      # +15
                "No Show",             # -10
            ],
            "customFields": {"budget": "$8,000"},  # +5
        }
        result = self.engine.evaluate(payload)
        self.assertEqual(result.points_delta, 40)
        self.assertEqual(len(result.positive_matches), 4)
        self.assertEqual(len(result.negative_matches), 1)

    def test_custom_rule_overrides_and_dynamic_registration(self) -> None:
        custom_engine = LeadScoringEngine(
            rule_overrides={
                "Booked Appointment": 30,  # Override from +20 to +30
                "no_show": {"points": -25},  # Override from -10 to -25
                "Attended Live Demo": 18,  # Brand-new custom activity rule
            }
        )
        custom_engine.disable_rule("Clicked SMS Link")
        custom_engine.register_rule(
            ScoringRule(
                rule_id="enterprise_domain",
                name="Enterprise Tier",
                points=12,
                rule_type="custom_field",
                field_key="company_size",
                expected_value="enterprise",
            )
        )

        payload = {
            "contactId": "cnt_override",
            "activities": [
                "Booked Appointment",  # +30
                "Clicked SMS Link",    # disabled -> 0
                "Attended Live Demo",  # +18
                "No Show",             # -25
            ],
            "customFields": {"company_size": "Enterprise"},  # +12
        }
        result = custom_engine.evaluate(payload)
        self.assertEqual(result.points_delta, 35)
        self.assertIn("Booked Appointment", result.matched_rule_names)
        self.assertIn("Attended Live Demo", result.matched_rule_names)
        self.assertIn("Enterprise Tier", result.matched_rule_names)
        self.assertNotIn("Clicked SMS Link", result.matched_rule_names)


# ---------------------------------------------------------------------------
# 2. Threshold Crossings & GHL Pipeline Manager Tests
# ---------------------------------------------------------------------------


class TestGHLPipelineThresholdsAndPayloads(unittest.TestCase):
    """Test threshold crossings (>= 50 and < 0), GHL API v2 payloads, and retries."""

    def test_hot_lead_threshold_crossing_at_50(self) -> None:
        """Score crossing >= 50 adds 'Hot Lead' tag and moves opportunity to 'Priority Outreach'."""
        manager = GHLPipelineManager(dry_run=True, priority_stage_id="stage_prio_99")
        manager.seed_mock_contact("cnt_hot", lead_score=30, opportunity_id="opp_hot_1")

        event = LeadEvent.from_webhook_payload(
            {
                "contactId": "cnt_hot",
                "opportunityId": "opp_hot_1",
                "activity": "Booked Appointment",  # +20 -> exact 50
            }
        )
        scoring_result = LeadScoringEngine().evaluate(event)
        action_result = manager.process_scoring_result(event, scoring_result)

        self.assertEqual(action_result.previous_score, 30)
        self.assertEqual(action_result.points_delta, 20)
        self.assertEqual(action_result.new_score, 50)
        self.assertEqual(action_result.threshold_status, "HOT_LEAD")
        self.assertEqual(action_result.tags_added, ["Hot Lead"])
        self.assertEqual(action_result.opportunity_stage_moved, "Priority Outreach")

        # Verify GHL API v2 calls and payload formatting
        ops = {
            f"{op['method']} {op['endpoint']}": op["payload"]
            for op in action_result.api_operations
        }
        self.assertIn("PUT /contacts/cnt_hot", ops)
        self.assertEqual(
            ops["PUT /contacts/cnt_hot"],
            {
                "customFields": [
                    {
                        "id": "lead_score",
                        "key": "lead_score",
                        "field_value": 50,
                    }
                ]
            },
        )
        self.assertIn("POST /contacts/cnt_hot/tags", ops)
        self.assertEqual(ops["POST /contacts/cnt_hot/tags"], {"tags": ["Hot Lead"]})

        self.assertIn("PUT /opportunities/opp_hot_1", ops)
        self.assertEqual(
            ops["PUT /opportunities/opp_hot_1"],
            {
                "pipelineStageId": "stage_prio_99",
                "stageName": "Priority Outreach",
                "status": "open",
            },
        )

        self.assertIn("POST /contacts/cnt_hot/notes", ops)
        note_body = ops["POST /contacts/cnt_hot/notes"]["body"]
        self.assertIn("Previous Score: 30 | Change: +20 | New Total: 50", note_body)
        self.assertIn("Booked Appointment: +20 pts", note_body)
        self.assertIn("Hot Lead", note_body)
        self.assertIn("Priority Outreach", note_body)

    def test_disqualified_threshold_crossing_below_zero(self) -> None:
        """Score dropping < 0 adds 'Disqualified' tag and does not promote opportunity."""
        manager = GHLPipelineManager(dry_run=True)
        manager.seed_mock_contact("cnt_disq", lead_score=5)

        event = LeadEvent.from_webhook_payload(
            {
                "contactId": "cnt_disq",
                "activity": "Unsubscribed",  # -15 -> new score -10
            }
        )
        scoring_result = LeadScoringEngine().evaluate(event)
        action_result = manager.process_scoring_result(event, scoring_result)

        self.assertEqual(action_result.previous_score, 5)
        self.assertEqual(action_result.points_delta, -15)
        self.assertEqual(action_result.new_score, -10)
        self.assertEqual(action_result.threshold_status, "DISQUALIFIED")
        self.assertEqual(action_result.tags_added, ["Disqualified"])
        self.assertIsNone(action_result.opportunity_stage_moved)

        ops = {
            f"{op['method']} {op['endpoint']}": op["payload"]
            for op in action_result.api_operations
        }
        self.assertEqual(
            ops["POST /contacts/cnt_disq/tags"],
            {"tags": ["Disqualified"]},
        )
        self.assertNotIn("PUT /opportunities/opp_cnt_disq", ops)
        self.assertIn("Previous Score: 5 | Change: -15 | New Total: -10", action_result.note_body)

    def test_standard_score_range_triggers_no_threshold_tags(self) -> None:
        """Score remaining in [0, 49] updates score and note without threshold tags."""
        manager = GHLPipelineManager(dry_run=True)
        manager.seed_mock_contact("cnt_std", lead_score=10)

        event = LeadEvent.from_webhook_payload(
            {
                "contactId": "cnt_std",
                "activity": "No Show",  # -10 -> exact 0 (not < 0)
            }
        )
        scoring_result = LeadScoringEngine().evaluate(event)
        action_result = manager.process_scoring_result(event, scoring_result)

        self.assertEqual(action_result.new_score, 0)
        self.assertEqual(action_result.threshold_status, "STANDARD")
        self.assertEqual(action_result.tags_added, [])
        self.assertIsNone(action_result.opportunity_stage_moved)

    def test_live_http_client_headers_and_retry_backoff(self) -> None:
        """Verify live HTTP mode sends GHL v2 headers and retries on HTTP 429."""
        mock_session = MagicMock()

        rate_limited_resp = MagicMock()
        rate_limited_resp.status_code = 429
        rate_limited_resp.text = "Rate limit exceeded"

        success_get_resp = MagicMock()
        success_get_resp.status_code = 200
        success_get_resp.text = json.dumps(
            {
                "contact": {
                    "id": "cnt_live",
                    "customFields": [{"id": "lead_score", "key": "lead_score", "value": 40}],
                }
            }
        )
        success_get_resp.json.return_value = json.loads(success_get_resp.text)

        generic_ok_resp = MagicMock()
        generic_ok_resp.status_code = 200
        generic_ok_resp.text = json.dumps({"status": "ok"})
        generic_ok_resp.json.return_value = {"status": "ok"}

        mock_session.request.side_effect = [
            rate_limited_resp,  # 1st attempt on GET /contacts/cnt_live -> 429
            success_get_resp,   # 2nd attempt on GET /contacts/cnt_live -> 200
            generic_ok_resp,    # PUT /contacts/cnt_live
            generic_ok_resp,    # POST /contacts/cnt_live/tags
            generic_ok_resp,    # PUT /opportunities/opp_live_1
            generic_ok_resp,    # POST /contacts/cnt_live/notes
        ]

        manager = GHLPipelineManager(
            api_key="ghl_pat_live_secret_token",
            dry_run=False,
            max_retries=2,
            backoff_factor=0.0,
            session=mock_session,
        )

        event = LeadEvent.from_webhook_payload(
            {
                "contactId": "cnt_live",
                "opportunityId": "opp_live_1",
                "activity": "Form Submitted",  # +15 -> 40 + 15 = 55 (HOT_LEAD)
            }
        )
        scoring_result = LeadScoringEngine().evaluate(event)
        action_result = manager.process_scoring_result(event, scoring_result)

        self.assertFalse(action_result.dry_run)
        self.assertEqual(action_result.new_score, 55)
        self.assertEqual(mock_session.request.call_count, 6)

        first_call_kwargs = mock_session.request.call_args_list[0].kwargs
        self.assertEqual(
            first_call_kwargs["headers"]["Authorization"],
            "Bearer ghl_pat_live_secret_token",
        )
        self.assertEqual(first_call_kwargs["headers"]["Version"], "2021-07-28")


# ---------------------------------------------------------------------------
# 3. End-to-End AWS Lambda Handler Tests
# ---------------------------------------------------------------------------


class TestLambdaHandlerEndToEnd(unittest.TestCase):
    """End-to-end tests for AWS API Gateway proxy event processing in lambda_handler."""

    def test_lambda_handler_mock_api_gateway_event_hot_lead(self) -> None:
        api_event = build_mock_api_gateway_event()
        response = lambda_handler(api_event, None)

        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["status"], "success")
        self.assertEqual(body["contactId"], "cnt_984210a")
        self.assertEqual(body["previousScore"], 25)
        # Booked Appointment (+20) + Clicked SMS Link (+10) + Budget > $5k (+5) = +35
        self.assertEqual(body["pointsDelta"], 35)
        self.assertEqual(body["newScore"], 60)
        self.assertEqual(body["thresholdStatus"], "HOT_LEAD")
        self.assertIn("Hot Lead", body["tagsAdded"])
        self.assertEqual(body["opportunityStageMoved"], "Priority Outreach")

    def test_lambda_handler_base64_encoded_payload(self) -> None:
        raw_payload = {
            "contactId": "cnt_b64",
            "activity": "Unsubscribed",
            "customFields": {"lead_score": 10},
        }
        encoded = base64.b64encode(json.dumps(raw_payload).encode("utf-8")).decode("utf-8")
        event = {
            "httpMethod": "POST",
            "isBase64Encoded": True,
            "body": encoded,
        }
        response = lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 200)
        body = json.loads(response["body"])
        self.assertEqual(body["newScore"], -5)
        self.assertEqual(body["thresholdStatus"], "DISQUALIFIED")
        self.assertEqual(body["tagsAdded"], ["Disqualified"])

    def test_lambda_handler_returns_400_on_missing_contact_id(self) -> None:
        event = {
            "httpMethod": "POST",
            "body": json.dumps({"activity": "Booked Appointment"}),
        }
        response = lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 400)
        body = json.loads(response["body"])
        self.assertEqual(body["status"], "error")
        self.assertIn("contactId", body["message"])

    def test_lambda_handler_returns_400_on_malformed_json(self) -> None:
        event = {
            "httpMethod": "POST",
            "body": "{not-valid-json",
        }
        response = lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 400)

    def test_lambda_handler_webhook_secret_authentication(self) -> None:
        event = build_mock_api_gateway_event()
        with patch.dict(os.environ, {"GHL_WEBHOOK_SECRET": "top_secret_123"}):
            # Missing header -> 401
            unauthorized_res = lambda_handler(event, None)
            self.assertEqual(unauthorized_res["statusCode"], 401)

            # Valid header -> 200
            event["headers"]["X-GHL-Signature"] = "top_secret_123"
            authorized_res = lambda_handler(event, None)
            self.assertEqual(authorized_res["statusCode"], 200)

    def test_lambda_handler_upstream_ghl_api_error_returns_502(self) -> None:
        failing_manager = MagicMock(spec=GHLPipelineManager)
        failing_manager.process_scoring_result.side_effect = GHLAPIError(
            "Upstream 503 Service Unavailable", status_code=503
        )
        event = build_mock_api_gateway_event()
        response = lambda_handler(event, None, pipeline_manager=failing_manager)
        self.assertEqual(response["statusCode"], 502)
        body = json.loads(response["body"])
        self.assertEqual(body["upstreamStatusCode"], 503)


if __name__ == "__main__":
    unittest.main(verbosity=2)
