"""AWS Lambda handler for GoHighLevel (GHL API v2) Automated Lead Scoring Webhooks."""

from __future__ import annotations

import base64
import hmac
import json
import logging
import os
from typing import Any, Dict, Optional

from src.models.lead_event import LeadEvent
from src.rules.scoring_rules import LeadScoringEngine
from src.services.ghl_pipeline_manager import GHLAPIError, GHLPipelineManager

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )


def _build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """Construct an AWS API Gateway proxy integration HTTP response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }


def _extract_webhook_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and parse JSON payload from an AWS API Gateway proxy event or direct invocation."""
    if not isinstance(event, dict):
        raise ValueError("Lambda event must be a dictionary.")

    if "body" in event and event["body"] is not None:
        raw_body = event["body"]
        if isinstance(raw_body, dict):
            return raw_body
        if isinstance(raw_body, str):
            if event.get("isBase64Encoded"):
                try:
                    raw_body = base64.b64decode(raw_body).decode("utf-8")
                except Exception as exc:
                    raise ValueError(f"Invalid base64 encoded request body: {exc}") from exc
            raw_body = raw_body.strip()
            if not raw_body:
                raise ValueError("Request body is empty.")
            try:
                parsed = json.loads(raw_body)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Malformed JSON in request body: {exc.msg}") from exc
            if not isinstance(parsed, dict):
                raise ValueError("Parsed JSON body must be an object.")
            return parsed
        raise ValueError(f"Unsupported request body type: {type(raw_body).__name__}")

    # Direct Lambda invocation where `event` is already the webhook payload
    return event


def _verify_webhook_secret(event: Dict[str, Any]) -> bool:
    """Validate optional shared webhook secret if GHL_WEBHOOK_SECRET is configured."""
    expected_secret = os.environ.get("GHL_WEBHOOK_SECRET", "").strip()
    if not expected_secret:
        return True

    raw_headers = event.get("headers") or {}
    if not isinstance(raw_headers, dict):
        return False

    normalized_headers = {str(k).lower(): str(v) for k, v in raw_headers.items() if v is not None}
    provided_secret = (
        normalized_headers.get("x-ghl-signature")
        or normalized_headers.get("x-webhook-secret")
        or normalized_headers.get("x-api-key")
        or ""
    )
    return hmac.compare_digest(expected_secret, provided_secret)


def _load_env_rule_overrides() -> Optional[Dict[str, Any]]:
    """Parse optional SCORING_RULE_OVERRIDES JSON from environment variables."""
    raw_overrides = os.environ.get("SCORING_RULE_OVERRIDES", "").strip()
    if not raw_overrides:
        return None
    try:
        parsed = json.loads(raw_overrides)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError as exc:
        logger.warning("Ignoring invalid SCORING_RULE_OVERRIDES JSON: %s", exc)
    return None


def lambda_handler(
    event: Dict[str, Any],
    context: Optional[Any] = None,
    *,
    engine: Optional[LeadScoringEngine] = None,
    pipeline_manager: Optional[GHLPipelineManager] = None,
) -> Dict[str, Any]:
    """AWS Lambda entry point for processing GoHighLevel lead scoring webhooks."""
    request_id = getattr(context, "aws_request_id", "local-invocation")
    logger.info("Processing GHL webhook event (request_id=%s)", request_id)

    if not isinstance(event, dict):
        return _build_response(
            400,
            {"status": "error", "message": "Event payload must be a JSON object."},
        )

    if not _verify_webhook_secret(event):
        logger.warning("Unauthorized webhook attempt blocked (request_id=%s)", request_id)
        return _build_response(
            401,
            {"status": "error", "message": "Unauthorized: invalid webhook secret."},
        )

    try:
        payload = _extract_webhook_payload(event)
        lead_event = LeadEvent.from_webhook_payload(payload)
    except ValueError as exc:
        logger.warning("Invalid webhook payload: %s", exc)
        return _build_response(
            400,
            {"status": "error", "message": str(exc)},
        )

    try:
        scoring_engine = engine or LeadScoringEngine(
            rule_overrides=_load_env_rule_overrides()
        )
        ghl_manager = pipeline_manager or GHLPipelineManager()

        scoring_result = scoring_engine.evaluate(lead_event)
        action_result = ghl_manager.process_scoring_result(
            event=lead_event,
            scoring_result=scoring_result,
        )

        response_payload = {
            "status": "success",
            "requestId": request_id,
            "contactId": action_result.contact_id,
            "previousScore": action_result.previous_score,
            "pointsDelta": action_result.points_delta,
            "newScore": action_result.new_score,
            "thresholdStatus": action_result.threshold_status,
            "matchedRules": [m.to_dict() for m in scoring_result.matched_rules],
            "tagsAdded": action_result.tags_added,
            "opportunityStageMoved": action_result.opportunity_stage_moved,
            "noteBody": action_result.note_body,
            "dryRun": action_result.dry_run,
            "apiOperations": action_result.api_operations,
        }
        logger.info(
            "Completed lead scoring for contact=%s: %d -> %d (%s)",
            action_result.contact_id,
            action_result.previous_score,
            action_result.new_score,
            action_result.threshold_status,
        )
        return _build_response(200, response_payload)

    except GHLAPIError as exc:
        logger.error("GHL API v2 communication error: %s", exc)
        return _build_response(
            502,
            {
                "status": "error",
                "message": str(exc),
                "upstreamStatusCode": exc.status_code,
            },
        )
    except Exception as exc:  # pragma: no cover - defensive catch-all
        logger.exception("Unhandled error in lambda_handler: %s", exc)
        return _build_response(
            500,
            {"status": "error", "message": f"Internal server error: {exc}"},
        )


def build_mock_api_gateway_event() -> Dict[str, Any]:
    """Create a realistic AWS API Gateway v1/v2 proxy event carrying a GHL v2 webhook payload."""
    webhook_payload = {
        "type": "ContactActivityWebhook",
        "locationId": "loc_ghl_enterprise_01",
        "contactId": "cnt_984210a",
        "opportunityId": "opp_55190b",
        "fullName": "Jordan Vance",
        "email": "jordan.vance@example-enterprise.io",
        "phone": "+15550192834",
        "activities": [
            "Booked Appointment",
            "Clicked SMS Link",
        ],
        "tags": ["Inbound Funnel", "Q4 Campaign"],
        "customFields": [
            {
                "id": "lead_score",
                "key": "contact.lead_score",
                "field_value": 25,
            },
            {
                "id": "cf_budget_tier",
                "key": "contact.budget",
                "field_value": "Budget > $5k",
            },
        ],
    }

    return {
        "resource": "/webhooks/ghl/lead-score",
        "path": "/webhooks/ghl/lead-score",
        "httpMethod": "POST",
        "headers": {
            "Content-Type": "application/json",
            "User-Agent": "GoHighLevel-Webhook/2.0",
        },
        "queryStringParameters": None,
        "pathParameters": None,
        "isBase64Encoded": False,
        "body": json.dumps(webhook_payload),
        "requestContext": {
            "requestId": "cli-mock-req-001",
            "stage": "local",
            "http": {
                "method": "POST",
                "path": "/webhooks/ghl/lead-score",
            },
        },
    }


if __name__ == "__main__":
    print("=" * 72)
    print("GoHighLevel Automated Lead Scoring Engine - Local CLI Simulation")
    print("=" * 72)
    mock_event = build_mock_api_gateway_event()
    result = lambda_handler(mock_event, None)
    parsed_body = json.loads(result["body"])
    print(f"HTTP Status Code : {result['statusCode']}")
    print(f"Contact ID       : {parsed_body['contactId']}")
    print(f"Previous Score   : {parsed_body['previousScore']}")
    print(f"Points Delta     : {parsed_body['pointsDelta']:+d}")
    print(f"New Total Score  : {parsed_body['newScore']}")
    print(f"Threshold Status : {parsed_body['thresholdStatus']}")
    print(f"Tags Added       : {parsed_body['tagsAdded']}")
    print(f"Stage Moved To   : {parsed_body['opportunityStageMoved']}")
    print("-" * 72)
    print("Contact Note Appended:")
    print(parsed_body["noteBody"])
    print("-" * 72)
    print("Full Lambda Response Payload:")
    print(json.dumps(parsed_body, indent=2))
