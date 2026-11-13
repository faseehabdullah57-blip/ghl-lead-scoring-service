"""GoHighLevel (GHL API v2) client for contact score updates, pipeline stages, tags, and notes."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from src.models.lead_event import (
    LeadEvent,
    PipelineActionResult,
    ScoringResult,
    normalize_custom_fields,
)

logger = logging.getLogger(__name__)


class GHLAPIError(Exception):
    """Raised when a GoHighLevel API v2 request fails after retries."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_body: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class GHLPipelineManager:
    """Lightweight GoHighLevel API v2 client with retry/backoff and dry-run mode."""

    DEFAULT_BASE_URL = "https://services.leadconnectorhq.com"
    DEFAULT_API_VERSION = "2021-07-28"

    HOT_LEAD_THRESHOLD = 50
    DISQUALIFIED_THRESHOLD = 0

    HOT_LEAD_TAG = "Hot Lead"
    DISQUALIFIED_TAG = "Disqualified"
    PRIORITY_OUTREACH_STAGE = "Priority Outreach"

    RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        location_id: Optional[str] = None,
        lead_score_field_id: Optional[str] = None,
        priority_stage_id: Optional[str] = None,
        dry_run: Optional[bool] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        timeout_seconds: int = 10,
        session: Optional[requests.Session] = None,
    ) -> None:
        resolved_key = (
            api_key
            if api_key is not None
            else os.environ.get("GHL_API_KEY", "").strip()
        )
        self.api_key: str = resolved_key
        self.base_url: str = (
            base_url
            or os.environ.get("GHL_API_BASE_URL", self.DEFAULT_BASE_URL)
        ).rstrip("/")
        self.location_id: str = (
            location_id or os.environ.get("GHL_LOCATION_ID", "loc_default")
        )
        self.lead_score_field_id: str = (
            lead_score_field_id
            or os.environ.get("GHL_LEAD_SCORE_FIELD_ID", "lead_score")
        )
        self.priority_stage_id: str = (
            priority_stage_id
            or os.environ.get("GHL_PRIORITY_STAGE_ID", "stage_priority_outreach")
        )

        if dry_run is not None:
            self.dry_run = bool(dry_run)
        else:
            env_dry_run = os.environ.get("GHL_DRY_RUN", "").strip().lower()
            if env_dry_run in ("1", "true", "yes"):
                self.dry_run = True
            elif env_dry_run in ("0", "false", "no") and bool(self.api_key):
                self.dry_run = False
            else:
                self.dry_run = not bool(self.api_key)

        self.max_retries = max(0, int(max_retries))
        self.backoff_factor = max(0.0, float(backoff_factor))
        self.timeout_seconds = timeout_seconds
        self._session = session or requests.Session()

        # In-memory store for dry-run state verification and testing
        self._mock_contacts: Dict[str, Dict[str, Any]] = {}
        self._recorded_operations: List[Dict[str, Any]] = []

    @property
    def headers(self) -> Dict[str, str]:
        """Build standard GoHighLevel API v2 headers."""
        token = self.api_key or "DRY_RUN_MOCK_TOKEN"
        return {
            "Authorization": f"Bearer {token}",
            "Version": self.DEFAULT_API_VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def seed_mock_contact(
        self,
        contact_id: str,
        lead_score: int = 0,
        tags: Optional[List[str]] = None,
        opportunity_id: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Seed mock contact state for local dry-run or unit testing."""
        fields = dict(custom_fields or {})
        fields["lead_score"] = lead_score
        self._mock_contacts[contact_id] = {
            "id": contact_id,
            "tags": list(tags or []),
            "opportunityId": opportunity_id or f"opp_{contact_id}",
            "customFields": [
                {"id": self.lead_score_field_id, "key": k, "field_value": v}
                for k, v in fields.items()
            ],
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute an HTTP request against GHL API v2 with exponential backoff or dry-run simulation."""
        path = endpoint if endpoint.startswith("/") else f"/{endpoint}"
        url = f"{self.base_url}{path}"

        operation_record: Dict[str, Any] = {
            "method": method.upper(),
            "endpoint": path,
            "url": url,
            "payload": payload,
            "params": params,
            "dryRun": self.dry_run,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self._recorded_operations.append(operation_record)

        if self.dry_run:
            logger.info(
                "[DRY-RUN] %s %s | payload=%s params=%s",
                method.upper(),
                path,
                payload,
                params,
            )
            return self._simulate_dry_run_response(method.upper(), path, payload, params)

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.request(
                    method=method.upper(),
                    url=url,
                    headers=self.headers,
                    json=payload,
                    params=params,
                    timeout=self.timeout_seconds,
                )

                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    if attempt < self.max_retries:
                        sleep_time = self.backoff_factor * (2 ** attempt)
                        logger.warning(
                            "GHL API returned %s on %s %s (attempt %d/%d). Retrying in %.2fs...",
                            response.status_code,
                            method.upper(),
                            path,
                            attempt + 1,
                            self.max_retries + 1,
                            sleep_time,
                        )
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                        continue
                    raise GHLAPIError(
                        f"GHL API request failed after {self.max_retries + 1} attempts with status {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text,
                    )

                if response.status_code >= 400:
                    raise GHLAPIError(
                        f"GHL API error {response.status_code} on {method.upper()} {path}: {response.text}",
                        status_code=response.status_code,
                        response_body=response.text,
                    )

                if not response.text:
                    return {}
                return response.json()

            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.max_retries:
                    sleep_time = self.backoff_factor * (2 ** attempt)
                    logger.warning(
                        "Network error on %s %s (attempt %d/%d): %s. Retrying in %.2fs...",
                        method.upper(),
                        path,
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                        sleep_time,
                    )
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    continue
                raise GHLAPIError(
                    f"Network failure communicating with GHL API v2: {exc}"
                ) from exc

        raise GHLAPIError(f"GHL API request failed: {last_error}")

    def _simulate_dry_run_response(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]],
        params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Generate realistic GHL API v2 mock responses during dry-run execution."""
        parts = [p for p in path.split("/") if p]

        # GET /contacts/{contactId}
        if method == "GET" and len(parts) == 2 and parts[0] == "contacts":
            contact_id = parts[1]
            contact = self._mock_contacts.get(
                contact_id,
                {
                    "id": contact_id,
                    "tags": [],
                    "customFields": [],
                },
            )
            return {"contact": contact}

        # PUT /contacts/{contactId}
        if method == "PUT" and len(parts) == 2 and parts[0] == "contacts":
            contact_id = parts[1]
            contact = self._mock_contacts.setdefault(
                contact_id,
                {"id": contact_id, "tags": [], "customFields": []},
            )
            if payload and "customFields" in payload:
                contact["customFields"] = payload["customFields"]
            return {"contact": contact, "succeded": True}

        # POST /contacts/{contactId}/tags
        if method == "POST" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "tags":
            contact_id = parts[1]
            new_tags = (payload or {}).get("tags", [])
            contact = self._mock_contacts.setdefault(
                contact_id,
                {"id": contact_id, "tags": [], "customFields": []},
            )
            for tag in new_tags:
                if tag not in contact["tags"]:
                    contact["tags"].append(tag)
            return {"tags": contact["tags"]}

        # POST /contacts/{contactId}/notes
        if method == "POST" and len(parts) == 3 and parts[0] == "contacts" and parts[2] == "notes":
            contact_id = parts[1]
            return {
                "note": {
                    "id": f"note_{contact_id}",
                    "contactId": contact_id,
                    "body": (payload or {}).get("body", ""),
                    "dateAdded": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            }

        # GET /opportunities/search
        if method == "GET" and path == "/opportunities/search":
            contact_id = (params or {}).get("contact_id", "unknown")
            return {
                "opportunities": [
                    {
                        "id": f"opp_{contact_id}",
                        "contactId": contact_id,
                        "status": "open",
                        "pipelineStageId": "stage_initial",
                    }
                ]
            }

        # PUT /opportunities/{opportunityId}
        if method == "PUT" and len(parts) == 2 and parts[0] == "opportunities":
            opp_id = parts[1]
            return {
                "opportunity": {
                    "id": opp_id,
                    **(payload or {}),
                }
            }

        return {"status": "ok"}

    def get_contact(self, contact_id: str) -> Dict[str, Any]:
        """Fetch a contact record from GHL API v2 via GET /contacts/{contactId}."""
        response = self._request("GET", f"/contacts/{contact_id}")
        if isinstance(response.get("contact"), dict):
            return response["contact"]
        return response

    def get_current_score(
        self, contact_id: str, fallback_score: Optional[int] = None
    ) -> int:
        """Fetch the contact's current lead_score from custom fields, defaulting to fallback or 0."""
        contact_data = self.get_contact(contact_id)
        raw_fields = (
            contact_data.get("customFields")
            if "customFields" in contact_data
            else contact_data.get("custom_fields", [])
        )
        normalized = normalize_custom_fields(raw_fields)

        for key in (
            self.lead_score_field_id,
            "lead_score",
            "leadScore",
            "score",
            "Lead Score",
        ):
            if key in normalized and normalized[key] is not None:
                try:
                    return int(float(str(normalized[key]).strip()))
                except (ValueError, TypeError):
                    continue

        if fallback_score is not None:
            return int(fallback_score)
        return 0

    def update_contact_score(self, contact_id: str, new_score: int) -> Dict[str, Any]:
        """Update the contact's lead_score custom field via PUT /contacts/{contactId}."""
        payload = {
            "customFields": [
                {
                    "id": self.lead_score_field_id,
                    "key": "lead_score",
                    "field_value": int(new_score),
                }
            ]
        }
        return self._request("PUT", f"/contacts/{contact_id}", payload=payload)

    def add_contact_tags(self, contact_id: str, tags: List[str]) -> Dict[str, Any]:
        """Attach one or more tags to a contact via POST /contacts/{contactId}/tags."""
        payload = {"tags": list(tags)}
        return self._request("POST", f"/contacts/{contact_id}/tags", payload=payload)

    def resolve_active_opportunity_id(
        self, contact_id: str, hint_opportunity_id: Optional[str] = None
    ) -> str:
        """Resolve the contact's active opportunity ID or search GHL opportunities."""
        if hint_opportunity_id:
            return hint_opportunity_id

        search_res = self._request(
            "GET",
            "/opportunities/search",
            params={"location_id": self.location_id, "contact_id": contact_id},
        )
        opportunities = search_res.get("opportunities", [])
        if isinstance(opportunities, list) and opportunities:
            first_opp = opportunities[0]
            if isinstance(first_opp, dict) and first_opp.get("id"):
                return str(first_opp["id"])

        return f"opp_{contact_id}"

    def move_opportunity_to_stage(
        self,
        contact_id: str,
        stage_name: str = PRIORITY_OUTREACH_STAGE,
        stage_id: Optional[str] = None,
        opportunity_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Move the contact's active opportunity to the target stage via PUT /opportunities/{opportunityId}."""
        resolved_opp_id = self.resolve_active_opportunity_id(
            contact_id=contact_id,
            hint_opportunity_id=opportunity_id,
        )
        payload = {
            "pipelineStageId": stage_id or self.priority_stage_id,
            "stageName": stage_name,
            "status": "open",
        }
        return self._request("PUT", f"/opportunities/{resolved_opp_id}", payload=payload)

    def create_contact_note(self, contact_id: str, body: str) -> Dict[str, Any]:
        """Append a timestamped audit note to the contact via POST /contacts/{contactId}/notes."""
        payload = {"body": body}
        return self._request("POST", f"/contacts/{contact_id}/notes", payload=payload)

    @staticmethod
    def format_scoring_note(
        scoring_result: ScoringResult,
        previous_score: int,
        new_score: int,
        threshold_status: str,
        tags_added: List[str],
        stage_moved: Optional[str],
    ) -> str:
        """Format a clear, timestamped contact note summarizing point breakdown and score totals."""
        delta_str = (
            f"+{scoring_result.points_delta}"
            if scoring_result.points_delta >= 0
            else str(scoring_result.points_delta)
        )
        lines = [
            f"[{scoring_result.evaluated_at}] Automated Lead Scoring Update",
            f"Previous Score: {previous_score} | Change: {delta_str} | New Total: {new_score}",
            f"Status: {threshold_status}",
            "Point Breakdown:",
        ]

        if scoring_result.matched_rules:
            for match in scoring_result.matched_rules:
                pts_str = f"+{match.points}" if match.points >= 0 else str(match.points)
                lines.append(
                    f"  - {match.rule_name}: {pts_str} pts ({match.reason})"
                )
        else:
            lines.append("  - No scoring rules triggered (+0 pts)")

        actions: List[str] = []
        if tags_added:
            actions.append(f"Added tag(s): {', '.join(tags_added)}")
        if stage_moved:
            actions.append(f"Moved opportunity to stage: {stage_moved}")
        if actions:
            lines.append(f"Actions Triggered: {'; '.join(actions)}")

        return "\n".join(lines)

    def process_scoring_result(
        self,
        event: LeadEvent,
        scoring_result: ScoringResult,
    ) -> PipelineActionResult:
        """Execute atomic score update, evaluate thresholds, and log audit note in GoHighLevel."""
        start_op_idx = len(self._recorded_operations)
        contact_id = event.contact_id

        # 1. Fetch current lead score from contact custom fields (falling back to webhook hint or 0)
        previous_score = self.get_current_score(
            contact_id=contact_id,
            fallback_score=event.get_initial_score_hint(),
        )
        new_score = previous_score + scoring_result.points_delta

        # 2. Update contact custom field `lead_score` via PUT /contacts/{contactId}
        self.update_contact_score(contact_id=contact_id, new_score=new_score)

        # 3. Evaluate thresholds and execute corresponding CRM pipeline actions
        tags_added: List[str] = []
        opportunity_stage_moved: Optional[str] = None
        threshold_status = "STANDARD"

        if new_score >= self.HOT_LEAD_THRESHOLD:
            threshold_status = "HOT_LEAD"
            tags_added.append(self.HOT_LEAD_TAG)
            self.add_contact_tags(contact_id=contact_id, tags=[self.HOT_LEAD_TAG])
            self.move_opportunity_to_stage(
                contact_id=contact_id,
                stage_name=self.PRIORITY_OUTREACH_STAGE,
                opportunity_id=event.opportunity_id,
            )
            opportunity_stage_moved = self.PRIORITY_OUTREACH_STAGE
        elif new_score < self.DISQUALIFIED_THRESHOLD:
            threshold_status = "DISQUALIFIED"
            tags_added.append(self.DISQUALIFIED_TAG)
            self.add_contact_tags(contact_id=contact_id, tags=[self.DISQUALIFIED_TAG])

        # 4. Append timestamped contact note summarizing breakdown, previous score, and new total
        note_body = self.format_scoring_note(
            scoring_result=scoring_result,
            previous_score=previous_score,
            new_score=new_score,
            threshold_status=threshold_status,
            tags_added=tags_added,
            stage_moved=opportunity_stage_moved,
        )
        self.create_contact_note(contact_id=contact_id, body=note_body)

        operations_for_turn = self._recorded_operations[start_op_idx:]

        return PipelineActionResult(
            contact_id=contact_id,
            previous_score=previous_score,
            points_delta=scoring_result.points_delta,
            new_score=new_score,
            threshold_status=threshold_status,
            tags_added=tags_added,
            opportunity_stage_moved=opportunity_stage_moved,
            note_body=note_body,
            dry_run=self.dry_run,
            api_operations=operations_for_turn,
        )
