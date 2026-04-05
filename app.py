import json
import logging
import os
import signal
import sys
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


STOP = False


def handle_signal(signum, frame):
    global STOP
    STOP = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_INTERVAL_SECONDS = env_int("POLL_INTERVAL_SECONDS", 300)
HTTP_TIMEOUT_SECONDS = env_int("HTTP_TIMEOUT_SECONDS", 20)
DRY_RUN = env_bool("DRY_RUN", False)
DELETE_STALE_ALIASES = env_bool("DELETE_STALE_ALIASES", True)
DELETE_STALE_MAILBOXES = env_bool("DELETE_STALE_MAILBOXES", False)
SKIP_INACTIVE_USERS = env_bool("SKIP_INACTIVE_USERS", True)

AUTHENTIK_URL = os.environ["AUTHENTIK_URL"].rstrip("/") + "/"
AUTHENTIK_TOKEN = os.environ["AUTHENTIK_TOKEN"]

MAILCOW_URL = os.environ["MAILCOW_URL"].rstrip("/")
MAILCOW_API_KEY = os.environ["MAILCOW_API_KEY"]

MAILBOX_QUOTA_MB = os.getenv("MAILBOX_QUOTA_MB", "2048")
MAILBOX_PASSWORD_LENGTH = env_int("MAILBOX_PASSWORD_LENGTH", 32)
MANAGED_DOMAINS = [
    d.strip().lower()
    for d in os.getenv(
        "MANAGED_DOMAINS",
        "keofamily.net,keofamily.email,rueeger.email",
    ).split(",")
    if d.strip()
]

# If true, create only primary mailboxes under these domains.
# Helps prevent accidental mailbox creation in unexpected domains.
PRIMARY_MAILBOX_ALLOWED_DOMAINS = [
    d.strip().lower()
    for d in os.getenv("PRIMARY_MAILBOX_ALLOWED_DOMAINS", "keofamily.net").split(",")
    if d.strip()
]

# Only aliases with this marker are deleted as "managed" stale aliases.
# Strongly recommended to avoid touching manually-created aliases.
MANAGED_ALIAS_COMMENT = os.getenv("MANAGED_ALIAS_COMMENT", "managed-by-authentik-sync")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("mailcow-sync")


class SyncError(Exception):
    pass


def build_session() -> Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


SESSION = build_session()


def request_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    json_body: Optional[Any] = None,
    data_body: Optional[Any] = None,
) -> Any:
    response = SESSION.request(
        method=method,
        url=url,
        headers=headers,
        json=json_body,
        data=data_body,
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    if response.status_code >= 400:
        raise SyncError(
            f"HTTP {response.status_code} for {method} {url}: {response.text[:1000]}"
        )
    if not response.text:
        return None
    try:
        return response.json()
    except ValueError:
        return response.text


def authentik_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {AUTHENTIK_TOKEN}",
        "Accept": "application/json",
    }


def mailcow_headers() -> Dict[str, str]:
    return {
        "X-API-Key": MAILCOW_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def domain_of(address: str) -> str:
    return address.rsplit("@", 1)[1].lower()


def is_managed_domain(address: str) -> bool:
    try:
        domain = domain_of(address)
    except Exception:
        return False
    return domain in MANAGED_DOMAINS


def is_allowed_primary_mailbox(email: str) -> bool:
    try:
        return domain_of(email) in PRIMARY_MAILBOX_ALLOWED_DOMAINS
    except Exception:
        return False


def normalize_alias_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = [value]

    cleaned: List[str] = []
    seen: Set[str] = set()

    for item in raw:
        if not isinstance(item, str):
            continue
        alias = item.strip().lower()
        if not alias or "@" not in alias:
            continue
        if alias not in seen:
            seen.add(alias)
            cleaned.append(alias)

    return cleaned


def generate_random_password(length: int = 32) -> str:
    import secrets
    import string

    alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def get_authentik_users() -> List[Dict[str, Any]]:
    users: List[Dict[str, Any]] = []
    url = AUTHENTIK_URL

    while url:
        data = request_json("GET", url, headers=authentik_headers())
        if not isinstance(data, dict):
            raise SyncError("Unexpected Authentik response format.")
        results = data.get("results", [])
        if not isinstance(results, list):
            raise SyncError("Unexpected Authentik results format.")
        users.extend(results)
        url = data.get("next")

    return users


def get_mailcow_mailboxes() -> List[Dict[str, Any]]:
    data = request_json(
        "GET",
        f"{MAILCOW_URL}/api/v1/get/mailbox/all",
        headers=mailcow_headers(),
    )
    if not isinstance(data, list):
        raise SyncError("Unexpected Mailcow mailbox response format.")
    return data


def get_mailcow_aliases() -> List[Dict[str, Any]]:
    data = request_json(
        "GET",
        f"{MAILCOW_URL}/api/v1/get/alias/all",
        headers=mailcow_headers(),
    )
    if not isinstance(data, list):
        raise SyncError("Unexpected Mailcow alias response format.")
    return data


def create_mailbox(email: str, display_name: str) -> None:
    if DRY_RUN:
        log.info("DRY_RUN create mailbox %s", email)
        return

    local_part, domain = email.split("@", 1)
    payload = {
        "local_part": local_part,
        "domain": domain,
        "name": display_name or email,
        "quota": MAILBOX_QUOTA_MB,
        "password": generate_random_password(MAILBOX_PASSWORD_LENGTH),
        "active": "1",
    }
    result = request_json(
        "POST",
        f"{MAILCOW_URL}/api/v1/add/mailbox",
        headers=mailcow_headers(),
        json_body=payload,
    )
    log.info("Created mailbox %s result=%s", email, truncate_for_log(result))


def delete_mailbox(email: str) -> None:
    if DRY_RUN:
        log.info("DRY_RUN delete mailbox %s", email)
        return

    result = request_json(
        "POST",
        f"{MAILCOW_URL}/api/v1/delete/mailbox",
        headers=mailcow_headers(),
        json_body=[email],
    )
    log.info("Deleted mailbox %s result=%s", email, truncate_for_log(result))


def create_alias(address: str, goto: str) -> None:
    if DRY_RUN:
        log.info("DRY_RUN create alias %s -> %s", address, goto)
        return

    payload = {
        "address": address,
        "goto": goto,
        "active": "1",
        "comment": MANAGED_ALIAS_COMMENT,
    }
    result = request_json(
        "POST",
        f"{MAILCOW_URL}/api/v1/add/alias",
        headers=mailcow_headers(),
        json_body=payload,
    )
    log.info("Created alias %s -> %s result=%s", address, goto, truncate_for_log(result))


def delete_alias(address: str) -> None:
    if DRY_RUN:
        log.info("DRY_RUN delete alias %s", address)
        return

    result = request_json(
        "POST",
        f"{MAILCOW_URL}/api/v1/delete/alias",
        headers=mailcow_headers(),
        json_body=[address],
    )
    log.info("Deleted alias %s result=%s", address, truncate_for_log(result))


def truncate_for_log(value: Any, max_len: int = 300) -> str:
    text = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    if len(text) <= max_len:
        return text
    return text[:max_len] + "...(truncated)"


def mailbox_map(mailboxes: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for mailbox in mailboxes:
        username = str(mailbox.get("username", "")).strip().lower()
        if username:
            result[username] = mailbox
    return result


def alias_map(aliases: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict[str, Any]] = {}
    for alias in aliases:
        address = str(alias.get("address", "")).strip().lower()
        if address:
            result[address] = alias
    return result


def alias_is_managed(alias_obj: Dict[str, Any]) -> bool:
    address = str(alias_obj.get("address", "")).strip().lower()
    comment = str(alias_obj.get("comment", "")).strip()
    return is_managed_domain(address) and comment == MANAGED_ALIAS_COMMENT


def build_desired_state(
    users: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    desired_mailboxes: Dict[str, Dict[str, Any]] = {}
    desired_aliases: Dict[str, str] = {}

    for user in users:
        email = str(user.get("email", "")).strip().lower()
        username = str(user.get("username", "")).strip()
        display_name = str(user.get("name", "")).strip() or username or email
        is_active = bool(user.get("is_active", False))
        attributes = user.get("attributes", {}) or {}

        if SKIP_INACTIVE_USERS and not is_active:
            continue
        if not email or "@" not in email:
            log.warning("Skipping user without valid email: username=%s", username)
            continue
        if not is_allowed_primary_mailbox(email):
            log.warning(
                "Skipping mailbox creation for %s because domain is not allowed", email
            )
            continue

        desired_mailboxes[email] = {
            "display_name": display_name,
            "username": username,
        }

        aliases = normalize_alias_list(attributes.get("aliases"))
        for alias in aliases:
            if alias == email:
                continue
            if not is_managed_domain(alias):
                log.warning("Skipping unmanaged alias domain: %s", alias)
                continue
            if alias in desired_aliases and desired_aliases[alias] != email:
                raise SyncError(
                    f"Alias conflict detected: {alias} maps to both "
                    f"{desired_aliases[alias]} and {email}"
                )
            desired_aliases[alias] = email

    return desired_mailboxes, desired_aliases


def sync_once() -> None:
    log.info("Starting sync cycle")

    users = get_authentik_users()
    mailboxes = get_mailcow_mailboxes()
    aliases = get_mailcow_aliases()

    existing_mailboxes = mailbox_map(mailboxes)
    existing_aliases = alias_map(aliases)

    desired_mailboxes, desired_aliases = build_desired_state(users)

    # Create missing mailboxes
    for email, meta in sorted(desired_mailboxes.items()):
        if email not in existing_mailboxes:
            create_mailbox(email, meta["display_name"])

    # Create or repair aliases
    for address, target in sorted(desired_aliases.items()):
        existing = existing_aliases.get(address)
        if existing is None:
            create_alias(address, target)
            continue

        current_target = str(existing.get("goto", "")).strip().lower()
        if current_target != target:
            log.info(
                "Alias target drift detected for %s: current=%s desired=%s",
                address,
                current_target,
                target,
            )
            delete_alias(address)
            create_alias(address, target)

    # Delete stale aliases that this service owns
    if DELETE_STALE_ALIASES:
        for address, alias_obj in sorted(existing_aliases.items()):
            if not alias_is_managed(alias_obj):
                continue
            if address not in desired_aliases:
                delete_alias(address)

    # Optional mailbox cleanup
    if DELETE_STALE_MAILBOXES:
        for email in sorted(existing_mailboxes.keys()):
            if not is_allowed_primary_mailbox(email):
                continue
            if email not in desired_mailboxes:
                delete_mailbox(email)

    log.info(
        "Sync cycle completed: users=%d desired_mailboxes=%d desired_aliases=%d",
        len(users),
        len(desired_mailboxes),
        len(desired_aliases),
    )


def main() -> int:
    run_once = env_bool("RUN_ONCE", False)

    while not STOP:
        try:
            sync_once()
        except Exception as exc:
            log.exception("Sync cycle failed: %s", exc)

        if run_once:
            break

        slept = 0
        while slept < POLL_INTERVAL_SECONDS and not STOP:
            time.sleep(1)
            slept += 1

    log.info("Shutting down")
    return 0


if __name__ == "__main__":
    sys.exit(main())
