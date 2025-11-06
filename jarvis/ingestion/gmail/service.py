"""Gmail service implementation for fetching messages."""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from jarvis.ingestion.common.models import Attachment, Message

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GmailService:
    def __init__(self, credentials_path: str, token_path: str, user_id: str = "me") -> None:
        self.credentials_path = Path(credentials_path)
        self.token_path = Path(token_path)
        self.user_id = user_id
        self._service = None

    @property
    def service(self):
        if self._service is None:
            self._service = self._authorize()
        return self._service

    def _authorize(self):
        creds = None
        if self.token_path.exists():
            creds = Credentials.from_authorized_user_file(str(self.token_path), SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_path), SCOPES
                )
                creds = flow.run_local_server(port=0)
            self.token_path.parent.mkdir(parents=True, exist_ok=True)
            self.token_path.write_text(creds.to_json(), encoding="utf-8")
        return build("gmail", "v1", credentials=creds)

    def search(
        self,
        query: str,
        *,
        limit: int | None = None,
        page_size: int | None = None,
    ) -> Iterable[Message]:
        remaining = limit if (limit is not None and limit >= 0) else None
        max_results = page_size or 100
        if max_results <= 0 or max_results > 500:
            raise ValueError("page_size must be between 1 and 500")
        try:
            request = self.service.users().messages().list(
                userId=self.user_id,
                q=query,
                maxResults=max_results,
            )
            while request is not None:
                response = request.execute()
                for message_ref in response.get("messages", []):
                    if remaining is not None and remaining <= 0:
                        return
                    yield self._get_message(message_ref["id"])
                    if remaining is not None:
                        remaining -= 1
                        if remaining <= 0:
                            return
                request = self.service.users().messages().list_next(request, response)
        except HttpError as exc:  # pragma: no cover
            raise RuntimeError("Failed to query Gmail API") from exc

    def _get_message(self, message_id: str) -> Message:
        message_data = (
            self.service.users()
            .messages()
            .get(userId=self.user_id, id=message_id, format="full")
            .execute()
        )
        payload = message_data.get("payload", {})
        headers = {h["name"].lower(): h["value"] for h in payload.get("headers", [])}
        subject = headers.get("subject", "")
        sender = headers.get("from", "")
        recipients = [
            addr.strip()
            for addr in headers.get("to", "").split(",")
            if addr.strip()
        ]
        snippet = message_data.get("snippet", "")
        body = self._extract_body(payload)
        received_at = datetime.fromtimestamp(
            int(message_data.get("internalDate", "0")) / 1000, tz=timezone.utc
        )
        attachments = self._extract_attachments(message_id, payload)
        return Message(
            id=message_id,
            subject=subject,
            sender=sender,
            recipients=recipients,
            snippet=snippet,
            body=body,
            received_at=received_at,
            attachments=attachments,
            metadata={"labelIds": message_data.get("labelIds", [])},
            thread_id=message_data.get("threadId"),
        )

    def _extract_body(self, payload) -> str:
        if "body" in payload and payload["body"].get("data"):
            return self._decode_body(payload["body"]["data"])

        for part in payload.get("parts", []):
            mime = part.get("mimeType", "")
            data = part.get("body", {}).get("data")
            if mime.startswith("text/plain") and data:
                return self._decode_body(data)
        return ""

    def _extract_attachments(self, message_id: str, payload) -> List[Attachment]:
        attachments: List[Attachment] = []
        for part in payload.get("parts", []):
            filename = part.get("filename")
            body = part.get("body", {})
            if filename and body.get("attachmentId"):
                attachment_id = body["attachmentId"]
                attachment_data = (
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId=self.user_id, messageId=message_id, id=attachment_id)
                    .execute()
                )
                data = self._decode_bytes(attachment_data.get("data", ""))
                attachments.append(
                    Attachment(
                        id=attachment_id,
                        filename=filename,
                        mime_type=part.get("mimeType", "application/octet-stream"),
                        data=data,
                        metadata={"size": body.get("size")},
                    )
                )
        return attachments

    @staticmethod
    def _decode_body(data: str) -> str:
        return GmailService._decode_bytes(data).decode("utf-8", errors="replace")

    @staticmethod
    def _decode_bytes(data: str) -> bytes:
        return base64.urlsafe_b64decode(data + "==")
