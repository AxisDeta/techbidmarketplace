import base64
import hashlib
import hmac
import json
import logging
import os
import queue
import re
import secrets
import smtplib
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import wraps
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import mysql.connector
import mysql.connector.pooling
import requests
from dotenv import load_dotenv
from flask import (
    Flask, flash, g, jsonify, make_response, redirect,
    render_template, request, session, url_for
)

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# ── Hardcoded connects packages (USD prices shown in UI, KES charged) ──────────
CONNECTS_PACKAGES = [
    {"id": "starter", "name": "Starter",   "connects": 200,  "price_usd": 6.99},
    {"id": "pro",     "name": "Pro",        "connects": 1000, "price_usd": 9.99},
    {"id": "power",   "name": "Power",      "connects": 2500, "price_usd": 20.00},
]
EMPLOYER_SUB_USD   = 5.00      # $5/month employer subscription
WORKER_SIGNUP_CONNECTS = 10    # Free connects on signup
MIN_JOB_CONNECTS   = 10        # Minimum connects to apply for any job

# ── Freelance job categories ───────────────────────────────────────────────────
JOB_CATEGORIES = [
    "Web Development", "Mobile Development", "UI/UX Design", "Graphic Design",
    "Data Science & Analytics", "Machine Learning / AI", "DevOps & Cloud",
    "Cybersecurity", "Database Administration", "Game Development",
    "SEO & Digital Marketing", "Content Writing & Copywriting", "Video Editing",
    "Animation & Motion Graphics", "Virtual Assistance", "Customer Support",
    "Project Management", "Business Analysis", "Accounting & Finance",
    "Legal Services", "Translation & Localization", "Photo Editing",
    "WordPress Development", "E-commerce & Shopify", "Blockchain & Web3",
    "Quality Assurance & Testing", "Technical Writing", "Social Media Management",
    "Email Marketing", "Sales & Lead Generation",
]

JOB_TYPES   = ["hourly", "daily", "fixed"]
USER_ROLES  = ["worker", "employer"]


# ═══════════════════════════════════════════════════════════════════════════════
# Settings
# ═══════════════════════════════════════════════════════════════════════════════
def _env(key: str, default: str = "") -> str:
    v = os.getenv(key, default)
    return v.strip() if v else ""

def _env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k, "")
        if v and v.strip():
            return v.strip()
    return default

def _env_bool(key: str, default: bool = False) -> bool:
    return _env(key, "true" if default else "false").lower() in {"1", "true", "yes", "on"}

def _env_int(key: str, default: int = 0) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default

def _env_float(key: str, default: float = 0.0) -> float:
    try:
        return float(_env(key, str(default)))
    except ValueError:
        return default

def sanitize_prefix(p: str) -> str:
    c = re.sub(r"[^a-zA-Z0-9_]", "", p or "")
    if not c:
        c = "tbm_"
    if not c.endswith("_"):
        c += "_"
    return c.lower()


@dataclass(slots=True)
class Settings:
    secret_key:     str = field(default_factory=lambda: _env("FLASK_SECRET", secrets.token_hex(16)))
    app_base_url:   str = field(default_factory=lambda: _env("APP_BASE_URL", "http://127.0.0.1:5000"))

    mysql_host:     str = field(default_factory=lambda: _env_first("MYSQL_HOST", "DB_HOST"))
    mysql_port:     int = field(default_factory=lambda: _env_int("MYSQL_PORT", 3306))
    mysql_database: str = field(default_factory=lambda: _env_first("MYSQL_DATABASE", "DB_NAME"))
    mysql_user:     str = field(default_factory=lambda: _env_first("MYSQL_USER", "DB_USER"))
    mysql_password: str = field(default_factory=lambda: _env_first("MYSQL_PASSWORD", "DB_PASSWORD"))
    mysql_ssl_ca:   str = field(default_factory=lambda: _env("MYSQL_SSL_CA"))
    mysql_ssl_disabled: bool = field(default_factory=lambda: _env_bool("MYSQL_SSL_DISABLED", False))
    mysql_connect_timeout: int = field(default_factory=lambda: _env_int("MYSQL_CONNECT_TIMEOUT", 10))
    mysql_use_pool: bool = field(default_factory=lambda: _env_bool("MYSQL_USE_POOL", False))
    table_prefix:   str = field(default_factory=lambda: _env("DB_TABLE_PREFIX", "tbm_"))

    paystack_secret_key:    str = field(default_factory=lambda: _env("PAYSTACK_SECRET_KEY"))
    paystack_public_key:    str = field(default_factory=lambda: _env("PAYSTACK_PUBLIC_KEY"))
    paystack_webhook_secret: str = field(default_factory=lambda: _env("PAYSTACK_WEBHOOK_SECRET"))
    paystack_callback_url:  str = field(default_factory=lambda: _env("PAYSTACK_CALLBACK_URL"))
    paystack_currency:      str = field(default_factory=lambda: _env("PAYSTACK_CURRENCY", "KES"))

    pesapal_consumer_key:    str = field(default_factory=lambda: _env("PESAPAL_CONSUMER_KEY"))
    pesapal_consumer_secret: str = field(default_factory=lambda: _env("PESAPAL_CONSUMER_SECRET"))
    pesapal_callback_url:    str = field(default_factory=lambda: _env("PESAPAL_CALLBACK_URL"))
    pesapal_ipn_url:         str = field(default_factory=lambda: _env("PESAPAL_IPN_URL"))
    pesapal_currency:        str = field(default_factory=lambda: _env("PESAPAL_CURRENCY", "KES"))
    pesapal_api_url:         str = field(default_factory=lambda: _env("PESAPAL_API_URL", "https://pay.pesapal.com/v3"))

    usd_to_kes: float = field(default_factory=lambda: _env_float("USD_TO_KES", 130.0))

    gemini_api_key: str = field(default_factory=lambda: _env("GEMINI_API_KEY"))
    gemini_model:   str = field(default_factory=lambda: _env("GEMINI_MODEL", "gemini-1.5-flash"))
    ai_jobs_per_run: int = field(default_factory=lambda: _env_int("AI_JOBS_PER_RUN", 10))

    github_token:  str = field(default_factory=lambda: _env("GITHUB_TOKEN"))
    github_repo:   str = field(default_factory=lambda: _env("GITHUB_REPO"))
    github_branch: str = field(default_factory=lambda: _env("GITHUB_BRANCH", "main"))

    admin_username:      str = field(default_factory=lambda: _env("ADMIN_USERNAME", "admin"))
    admin_password:      str = field(default_factory=lambda: _env("ADMIN_PASSWORD", "change-me"))
    admin_google_emails: str = field(default_factory=lambda: _env("ADMIN_GOOGLE_EMAILS"))

    smtp_host:       str  = field(default_factory=lambda: _env("SMTP_HOST"))
    smtp_port:       int  = field(default_factory=lambda: _env_int("SMTP_PORT", 587))
    smtp_user:       str  = field(default_factory=lambda: _env("SMTP_USER"))
    smtp_password:   str  = field(default_factory=lambda: _env("SMTP_PASSWORD"))
    smtp_from_email: str  = field(default_factory=lambda: _env("SMTP_FROM_EMAIL"))
    smtp_use_tls:    bool = field(default_factory=lambda: _env_bool("SMTP_USE_TLS", True))

    @property
    def mysql_enabled(self) -> bool:
        return all([self.mysql_host, self.mysql_database, self.mysql_user])

    @property
    def smtp_enabled(self) -> bool:
        return bool(self.smtp_host and self.smtp_port and self.smtp_user and self.smtp_password)

    @property
    def admin_google_email_set(self) -> set[str]:
        return {e.strip().lower() for e in self.admin_google_emails.split(",") if e.strip()}

    def usd_to_kes_cents(self, usd: float) -> int:
        """Convert USD amount to KES in smallest unit (cents for Paystack = kobo/cents ×100)."""
        return int(round(usd * self.usd_to_kes * 100))

    def usd_to_kes_amount(self, usd: float) -> float:
        return round(usd * self.usd_to_kes, 2)


SETTINGS = Settings()


# ═══════════════════════════════════════════════════════════════════════════════
# MySQL Store
# ═══════════════════════════════════════════════════════════════════════════════
class MySQLStore:
    def __init__(self, settings: Settings) -> None:
        if not settings.mysql_enabled:
            raise RuntimeError("MySQL not configured. Set MYSQL_HOST, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD.")
        self.settings = settings
        self.prefix = sanitize_prefix(settings.table_prefix)
        self._kw: dict[str, Any] = {
            "host":               settings.mysql_host,
            "port":               settings.mysql_port,
            "database":           settings.mysql_database,
            "user":               settings.mysql_user,
            "password":           settings.mysql_password,
            "charset":            "utf8mb4",
            "autocommit":         False,
            "connection_timeout": settings.mysql_connect_timeout,
            "ssl_disabled":       settings.mysql_ssl_disabled,
        }
        if not settings.mysql_ssl_disabled and settings.mysql_ssl_ca:
            self._kw["ssl_ca"] = settings.mysql_ssl_ca
        self._pool = None
        if settings.mysql_use_pool:
            pk = dict(self._kw)
            pk.update({"pool_name": "tbm_pool", "pool_size": 10, "pool_reset_session": False})
            self._pool = mysql.connector.pooling.MySQLConnectionPool(**pk)

    def t(self, name: str) -> str:
        return f"{self.prefix}{re.sub(r'[^a-zA-Z0-9_]', '', name)}"

    def _connect(self):
        last = None
        for _ in range(2):
            conn = None
            try:
                conn = self._pool.get_connection() if self._pool else mysql.connector.connect(**self._kw)
                conn.ping(reconnect=True, attempts=2, delay=0)
                return conn
            except Exception as exc:
                last = exc
                if conn:
                    try: conn.close()
                    except: pass
                time.sleep(0.05)
        raise last or RuntimeError("Cannot get MySQL connection.")

    @staticmethod
    def _close(conn) -> None:
        try: conn.close()
        except: pass

    def query_all(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        last = None
        for _ in range(2):
            conn = self._connect()
            try:
                cur = conn.cursor(dictionary=True)
                cur.execute(sql, params)
                return list(cur.fetchall())
            except mysql.connector.Error as exc:
                last = exc; time.sleep(0.05)
            finally: self._close(conn)
        raise last or RuntimeError("query_all failed.")

    def query_one(self, sql: str, params: tuple = ()) -> dict[str, Any] | None:
        last = None
        for _ in range(2):
            conn = self._connect()
            try:
                cur = conn.cursor(dictionary=True)
                cur.execute(sql, params)
                return cur.fetchone()
            except mysql.connector.Error as exc:
                last = exc; time.sleep(0.05)
            finally: self._close(conn)
        raise last or RuntimeError("query_one failed.")

    def execute(self, sql: str, params: tuple = ()) -> int:
        last = None
        for _ in range(2):
            conn = self._connect()
            try:
                cur = conn.cursor()
                cur.execute(sql, params)
                conn.commit()
                return int(cur.lastrowid or 0)
            except mysql.connector.Error as exc:
                last = exc
                try: conn.rollback()
                except: pass
                time.sleep(0.05)
            finally: self._close(conn)
        raise last or RuntimeError("execute failed.")

    def ensure_schema(self) -> None:
        users   = self.t("users")
        emp     = self.t("employer_profiles")
        jobs    = self.t("jobs")
        winners = self.t("robot_winners")
        apps    = self.t("applications")
        pays    = self.t("payments")
        epays   = self.t("employer_payments")
        subs    = self.t("subscriptions")
        notifs  = self.t("notifications")
        p       = self.prefix

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {users} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    google_sub VARCHAR(191) UNIQUE,
                    password_hash TEXT,
                    role ENUM('worker','employer') NOT NULL,
                    full_name VARCHAR(180),
                    mobile VARCHAR(30),
                    country VARCHAR(80),
                    bio TEXT,
                    profile_pic_url TEXT,
                    skills JSON,
                    specialty VARCHAR(120),
                    connects_balance INT NOT NULL DEFAULT 0,
                    profile_complete TINYINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {emp} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL UNIQUE,
                    company_name VARCHAR(200),
                    website VARCHAR(300),
                    is_subscribed TINYINT NOT NULL DEFAULT 0,
                    subscription_expires_at DATETIME NULL,
                    CONSTRAINT fk_{p}ep_user FOREIGN KEY (user_id) REFERENCES {users}(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {jobs} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    employer_id BIGINT NULL,
                    title VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    category VARCHAR(120) NOT NULL,
                    job_type ENUM('hourly','daily','fixed') NOT NULL DEFAULT 'fixed',
                    budget_usd DECIMAL(12,2) NOT NULL DEFAULT 0,
                    duration VARCHAR(80),
                    connects_required INT NOT NULL DEFAULT 10,
                    is_robot TINYINT NOT NULL DEFAULT 0,
                    status ENUM('open','closed','filled') NOT NULL DEFAULT 'open',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_{p}jobs_cat (category),
                    INDEX idx_{p}jobs_robot (is_robot),
                    INDEX idx_{p}jobs_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {winners} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    job_id BIGINT NOT NULL UNIQUE,
                    robot_name VARCHAR(120) NOT NULL,
                    connects_shown INT NOT NULL DEFAULT 500,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_{p}win_job FOREIGN KEY (job_id) REFERENCES {jobs}(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {apps} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    job_id BIGINT NOT NULL,
                    connects_spent INT NOT NULL DEFAULT 10,
                    cover_letter TEXT,
                    status ENUM('pending','accepted','rejected') NOT NULL DEFAULT 'pending',
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_{p}app (user_id, job_id),
                    INDEX idx_{p}app_job (job_id),
                    CONSTRAINT fk_{p}app_user FOREIGN KEY (user_id) REFERENCES {users}(id) ON DELETE CASCADE,
                    CONSTRAINT fk_{p}app_job  FOREIGN KEY (job_id)  REFERENCES {jobs}(id)  ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {pays} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    provider VARCHAR(32) NOT NULL,
                    amount_usd DECIMAL(12,2) NOT NULL,
                    amount_kes DECIMAL(12,2) NOT NULL,
                    connects_awarded INT NOT NULL DEFAULT 0,
                    status VARCHAR(24) NOT NULL DEFAULT 'pending',
                    reference VARCHAR(191) NOT NULL UNIQUE,
                    provider_reference VARCHAR(191),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_{p}pays_user (user_id),
                    CONSTRAINT fk_{p}pays_user FOREIGN KEY (user_id) REFERENCES {users}(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {epays} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    employer_id BIGINT NOT NULL,
                    worker_user_id BIGINT NOT NULL,
                    job_id BIGINT NOT NULL,
                    amount_kes DECIMAL(12,2) NOT NULL,
                    status ENUM('pending','disbursed') NOT NULL DEFAULT 'pending',
                    admin_note TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {subs} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    employer_id BIGINT NOT NULL,
                    reference VARCHAR(191) NOT NULL UNIQUE,
                    amount_usd DECIMAL(12,2) NOT NULL DEFAULT 5.00,
                    status VARCHAR(24) NOT NULL DEFAULT 'pending',
                    expires_at DATETIME NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {notifs} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    message TEXT NOT NULL,
                    is_read TINYINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_{p}notif_user (user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            conn.commit()
            LOG.info("TechBid schema verified.")
        finally:
            self._close(conn)


# ═══════════════════════════════════════════════════════════════════════════════
# Payment Clients
# ═══════════════════════════════════════════════════════════════════════════════
class PaystackClient:
    _session = requests.Session()
    BASE = "https://api.paystack.co"

    def __init__(self, settings: Settings) -> None:
        self.s = settings

    def _h(self) -> dict:
        return {"Authorization": f"Bearer {self.s.paystack_secret_key}", "Content-Type": "application/json"}

    def initialize(self, *, email: str, amount_cents: int, reference: str,
                   callback_url: str, currency: str, metadata: dict | None = None) -> tuple[int, dict]:
        payload: dict = {"email": email, "amount": amount_cents, "reference": reference,
                          "callback_url": callback_url, "currency": currency}
        if metadata:
            payload["metadata"] = metadata
        try:
            r = self._session.post(f"{self.BASE}/transaction/initialize", headers=self._h(), json=payload, timeout=20)
            return r.status_code, r.json() if r.content else {}
        except Exception as exc:
            return 599, {"message": str(exc)}

    def verify(self, reference: str) -> tuple[int, dict]:
        try:
            r = self._session.get(f"{self.BASE}/transaction/verify/{reference}", headers=self._h(), timeout=20)
            return r.status_code, r.json() if r.content else {}
        except Exception as exc:
            return 599, {"message": str(exc)}

    def valid_sig(self, raw: bytes, sig: str | None) -> bool:
        secret = self.s.paystack_webhook_secret or self.s.paystack_secret_key or ""
        if not secret:
            return False
        expected = hmac.new(secret.encode(), raw, hashlib.sha512).hexdigest()
        return hmac.compare_digest(expected, sig or "")


class PesapalClient:
    _session = requests.Session()

    def __init__(self, settings: Settings) -> None:
        self.s = settings
        self.base = (settings.pesapal_api_url or "https://pay.pesapal.com/v3").rstrip("/")

    @staticmethod
    def _body(r: requests.Response) -> dict:
        if not r.content:
            return {}
        try:
            d = r.json()
            return d if isinstance(d, dict) else {"data": d}
        except Exception:
            return {"message": r.text}

    def get_token(self) -> tuple[str | None, dict | None]:
        try:
            r = self._session.post(f"{self.base}/api/Auth/RequestToken",
                json={"consumer_key": self.s.pesapal_consumer_key, "consumer_secret": self.s.pesapal_consumer_secret},
                headers={"Content-Type": "application/json", "Accept": "application/json"}, timeout=30)
            body = self._body(r)
        except Exception as exc:
            return None, {"message": str(exc)}
        token = body.get("token") if isinstance(body, dict) else None
        return (str(token), None) if token else (None, body)

    def register_ipn(self, token: str, ipn_url: str) -> tuple[str | None, dict | None]:
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            r = self._session.post(f"{self.base}/api/URLSetup/RegisterIPN",
                json={"url": ipn_url, "ipn_notification_type": "GET"}, headers=headers, timeout=30)
            body = self._body(r)
        except Exception as exc:
            return None, {"message": str(exc)}
        ipn_id = body.get("ipn_id") if isinstance(body, dict) else None
        return (str(ipn_id), None) if ipn_id else (None, body)

    def submit_order(self, token: str, ipn_id: str, reference: str, email: str,
                     amount: float, callback_url: str, currency: str, phone: str) -> tuple[int, dict]:
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": f"Bearer {token}"}
        payload = {
            "id": reference, "currency": currency, "amount": float(amount),
            "description": "TechBid connects purchase",
            "callback_url": callback_url, "notification_id": ipn_id,
            "billing_address": {"email_address": email or "user@techbid.io",
                                 "phone_number": phone or "254700000000",
                                 "first_name": "TechBid", "last_name": "User"},
        }
        try:
            r = self._session.post(f"{self.base}/api/Transactions/SubmitOrderRequest",
                json=payload, headers=headers, timeout=30)
            return r.status_code, self._body(r)
        except Exception as exc:
            return 599, {"message": str(exc)}

    def get_tx_status(self, token: str, order_tracking_id: str) -> tuple[int, dict]:
        headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": f"Bearer {token}"}
        try:
            r = self._session.get(f"{self.base}/api/Transactions/GetTransactionStatus",
                params={"orderTrackingId": order_tracking_id}, headers=headers, timeout=30)
            return r.status_code, self._body(r)
        except Exception as exc:
            return 599, {"message": str(exc)}


# ═══════════════════════════════════════════════════════════════════════════════
# Rate Limiter
# ═══════════════════════════════════════════════════════════════════════════════
class RateLimiter:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str, limit: int, window: float) -> bool:
        now = time.time()
        with self._lock:
            q = self._hits[key]
            while q and q[0] < now - window:
                q.popleft()
            if len(q) >= limit:
                return False
            q.append(now)
            return True


# ═══════════════════════════════════════════════════════════════════════════════
# SMTP Helper
# ═══════════════════════════════════════════════════════════════════════════════
_email_q: queue.Queue = queue.Queue()

def _email_worker() -> None:
    while True:
        item = _email_q.get()
        if item is None:
            break
        to_addr, subject, html_body = item
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = SETTINGS.smtp_from_email or SETTINGS.smtp_user
            msg["To"] = to_addr
            msg.attach(MIMEText(html_body, "html"))
            with smtplib.SMTP(SETTINGS.smtp_host, SETTINGS.smtp_port, timeout=15) as smtp:
                if SETTINGS.smtp_use_tls:
                    smtp.starttls()
                smtp.login(SETTINGS.smtp_user, SETTINGS.smtp_password)
                smtp.sendmail(msg["From"], [to_addr], msg.as_string())
        except Exception as exc:
            LOG.warning("Email send failed to %s: %s", to_addr, exc)
        finally:
            _email_q.task_done()

def send_email(to: str, subject: str, html: str) -> None:
    if SETTINGS.smtp_enabled and to:
        _email_q.put((to, subject, html))


# ═══════════════════════════════════════════════════════════════════════════════
# GitHub Profile Picture Upload
# ═══════════════════════════════════════════════════════════════════════════════
def upload_profile_pic(file_bytes: bytes, filename: str) -> str | None:
    """Upload profile picture to GitHub repo. Returns public URL or None."""
    if not SETTINGS.github_token or not SETTINGS.github_repo:
        return None
    try:
        from github import Github
        gh = Github(SETTINGS.github_token)
        repo = gh.get_repo(SETTINGS.github_repo)
        path = f"profile_pics/{filename}"
        encoded = base64.b64encode(file_bytes).decode()
        try:
            existing = repo.get_contents(path, ref=SETTINGS.github_branch)
            repo.update_file(path, f"Update {filename}", encoded, existing.sha, branch=SETTINGS.github_branch)
        except Exception:
            repo.create_file(path, f"Upload {filename}", encoded, branch=SETTINGS.github_branch)
        raw_url = f"https://raw.githubusercontent.com/{SETTINGS.github_repo}/{SETTINGS.github_branch}/{path}"
        return raw_url
    except Exception as exc:
        LOG.warning("GitHub upload failed: %s", exc)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# AI Job Generator (background thread)
# ═══════════════════════════════════════════════════════════════════════════════
_AI_ROBOT_NAMES = [
    "Alex M.", "Jordan T.", "Sam K.", "Riley C.", "Morgan P.", "Drew H.",
    "Casey B.", "Taylor W.", "Quinn R.", "Avery L.", "Blake N.", "Skyler F.",
    "Rowan D.", "Dakota S.", "Finley A.", "Reese E.", "Emery J.", "Sage G.",
]

def _generate_ai_jobs(store: MySQLStore) -> None:
    if not SETTINGS.gemini_api_key:
        LOG.info("AI job generator: no GEMINI_API_KEY set, skipping.")
        return
    try:
        import google.generativeai as genai
        genai.configure(api_key=SETTINGS.gemini_api_key)
        model = genai.GenerativeModel(SETTINGS.gemini_model)

        import random
        categories = random.sample(JOB_CATEGORIES, min(5, len(JOB_CATEGORIES)))
        count = 0
        for cat in categories:
            if count >= SETTINGS.ai_jobs_per_run:
                break
            prompt = (
                f"Generate {min(2, SETTINGS.ai_jobs_per_run - count)} realistic freelance job postings "
                f"in the '{cat}' category. Return a JSON array. Each item: "
                '{"title": "...", "description": "...(3-5 sentences)", "job_type": "hourly|daily|fixed", '
                '"budget_usd": <number>, "duration": "2 weeks|1 month|etc"}. '
                "Vary budget between 100 and 5000 USD. No markdown, pure JSON only."
            )
            try:
                resp = model.generate_content(prompt)
                raw = resp.text.strip()
                raw = raw.strip("```json").strip("```").strip()
                items = json.loads(raw)
                if not isinstance(items, list):
                    items = [items]
                for item in items:
                    connects_req = random.randint(10, 60)
                    winner_name  = random.choice(_AI_ROBOT_NAMES)
                    winner_conn  = random.randint(300, 2000)
                    jtype = item.get("job_type", "fixed")
                    if jtype not in JOB_TYPES:
                        jtype = "fixed"
                    job_id = store.execute(
                        f"INSERT INTO {store.t('jobs')} "
                        "(employer_id, title, description, category, job_type, budget_usd, duration, connects_required, is_robot, status) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,'open')",
                        (None, item.get("title", "Freelance Role")[:255],
                         item.get("description", "")[:2000],
                         cat, jtype,
                         float(item.get("budget_usd", 500)),
                         str(item.get("duration", "1 month"))[:80],
                         connects_req),
                    )
                    store.execute(
                        f"INSERT INTO {store.t('robot_winners')} (job_id, robot_name, connects_shown) VALUES (%s,%s,%s)",
                        (job_id, winner_name, winner_conn),
                    )
                    count += 1
            except Exception as exc:
                LOG.warning("AI job generation error for %s: %s", cat, exc)
        LOG.info("AI job generator: created %d jobs.", count)
    except Exception as exc:
        LOG.error("AI job generator fatal: %s", exc)


def _ai_job_thread(store: MySQLStore) -> None:
    time.sleep(10)  # wait for app to start
    while True:
        _generate_ai_jobs(store)
        time.sleep(86400)  # 24 hours


# ═══════════════════════════════════════════════════════════════════════════════
# Flask App
# ═══════════════════════════════════════════════════════════════════════════════
app = Flask(__name__)
app.secret_key = SETTINGS.secret_key
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5 MB upload limit

STORE = MySQLStore(SETTINGS)
PAYSTACK = PaystackClient(SETTINGS)
PESAPAL  = PesapalClient(SETTINGS)
LIMITER  = RateLimiter()


# ── Context processors ─────────────────────────────────────────────────────────
@app.context_processor
def inject_globals():
    uid = session.get("user_id")
    notif_count = 0
    if uid:
        row = STORE.query_one(
            f"SELECT COUNT(*) as c FROM {STORE.t('notifications')} WHERE user_id=%s AND is_read=0", (uid,))
        notif_count = row["c"] if row else 0
    return {
        "current_user_id": uid,
        "current_role": session.get("role"),
        "current_name": session.get("full_name"),
        "current_connects": session.get("connects", 0),
        "notif_count": notif_count,
        "categories": JOB_CATEGORIES,
        "packages": CONNECTS_PACKAGES,
    }


def _refresh_session(user_id: int) -> None:
    u = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE id=%s", (user_id,))
    if u:
        session["user_id"]    = u["id"]
        session["role"]       = u["role"]
        session["full_name"]  = u["full_name"] or u["email"]
        session["connects"]   = int(u["connects_balance"])
        session["profile_ok"] = bool(u["profile_complete"])
        session["email"]      = u["email"]


# ── Decorators ─────────────────────────────────────────────────────────────────
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def worker_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if session.get("role") != "worker":
            flash("Access restricted to workers.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return wrapper


def employer_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if session.get("role") != "employer":
            flash("Access restricted to employers.", "error")
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("is_admin"):
            flash("Admin access required.", "error")
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


def profile_required(f):
    """Redirect workers/employers to complete their profile first."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("profile_ok"):
            flash("Please complete your profile first.", "info")
            return redirect(url_for("complete_profile"))
        return f(*args, **kwargs)
    return wrapper


def csrf_token() -> str:
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_urlsafe(32)
    return session["csrf_token"]


def verify_csrf() -> bool:
    expected = session.get("csrf_token")
    provided = request.headers.get("X-CSRF-Token") or request.form.get("csrf_token")
    return bool(expected and provided and str(expected) == str(provided))


app.jinja_env.globals["csrf_token"] = csrf_token


def client_ip() -> str:
    fwd = request.headers.get("X-Forwarded-For", "").strip()
    return fwd.split(",")[0].strip() if fwd else (request.remote_addr or "unknown")


def push_notif(user_id: int, message: str) -> None:
    try:
        STORE.execute(
            f"INSERT INTO {STORE.t('notifications')} (user_id, message) VALUES (%s,%s)",
            (user_id, message),
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# Auth — Google OAuth
# ═══════════════════════════════════════════════════════════════════════════════
GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO  = "https://www.googleapis.com/oauth2/v3/userinfo"

_GOOGLE_CLIENT_ID     = _env("GOOGLE_CLIENT_ID")
_GOOGLE_CLIENT_SECRET = _env("GOOGLE_CLIENT_SECRET")
_GOOGLE_REDIRECT_URI  = _env("GOOGLE_REDIRECT_URI", f"{SETTINGS.app_base_url}/auth/google/callback")


@app.route("/auth/google")
def google_auth():
    role = request.args.get("role", "worker")
    session["pending_role"] = role if role in USER_ROLES else "worker"
    state = secrets.token_urlsafe(16)
    session["oauth_state"] = state
    params = urlencode({
        "client_id": _GOOGLE_CLIENT_ID,
        "redirect_uri": _GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
    })
    return redirect(f"{GOOGLE_AUTH_URL}?{params}")


@app.route("/auth/google/callback")
def google_callback():
    error = request.args.get("error")
    if error:
        flash(f"Google sign-in cancelled: {error}", "error")
        return redirect(url_for("login"))
    if request.args.get("state") != session.pop("oauth_state", None):
        flash("Invalid OAuth state. Please try again.", "error")
        return redirect(url_for("login"))
    code = request.args.get("code")
    try:
        token_resp = requests.post(GOOGLE_TOKEN_URL, data={
            "code": code, "client_id": _GOOGLE_CLIENT_ID,
            "client_secret": _GOOGLE_CLIENT_SECRET,
            "redirect_uri": _GOOGLE_REDIRECT_URI, "grant_type": "authorization_code",
        }, timeout=15).json()
        access_token = token_resp.get("access_token")
        userinfo = requests.get(GOOGLE_USERINFO,
            headers={"Authorization": f"Bearer {access_token}"}, timeout=10).json()
    except Exception as exc:
        LOG.warning("Google OAuth error: %s", exc)
        flash("Google sign-in failed. Please try again.", "error")
        return redirect(url_for("login"))

    email    = (userinfo.get("email") or "").lower().strip()
    sub      = userinfo.get("sub", "")
    name     = userinfo.get("name", "")
    role     = session.pop("pending_role", "worker")

    if not email:
        flash("Could not retrieve your email from Google.", "error")
        return redirect(url_for("login"))

    # Check if user exists
    user = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE email=%s", (email,))
    if user:
        # Update google_sub if not set
        if not user.get("google_sub"):
            STORE.execute(f"UPDATE {STORE.t('users')} SET google_sub=%s WHERE id=%s", (sub, user["id"]))
        # Check admin
        if email in SETTINGS.admin_google_email_set:
            session["is_admin"] = True
        _refresh_session(user["id"])
        return redirect(url_for("worker_dashboard") if user["role"] == "worker" else url_for("employer_dashboard"))
    else:
        # New user — create account
        uid = STORE.execute(
            f"INSERT INTO {STORE.t('users')} (email, google_sub, role, full_name, connects_balance) VALUES (%s,%s,%s,%s,%s)",
            (email, sub, role, name, WORKER_SIGNUP_CONNECTS if role == "worker" else 0),
        )
        if role == "employer":
            STORE.execute(f"INSERT INTO {STORE.t('employer_profiles')} (user_id) VALUES (%s)", (uid,))
        _refresh_session(uid)
        if email in SETTINGS.admin_google_email_set:
            session["is_admin"] = True
        send_email(email, "Welcome to TechBid Marketplace!",
            f"<h2>Welcome, {name}!</h2><p>Your account has been created as a <b>{role}</b>. "
            + (f"You've received <b>{WORKER_SIGNUP_CONNECTS} free connects</b> to get started!" if role == "worker" else "")
            + "</p>")
        flash("Account created! Please complete your profile.", "success")
        return redirect(url_for("complete_profile"))


# ═══════════════════════════════════════════════════════════════════════════════
# Auth — Register / Login / Logout
# ═══════════════════════════════════════════════════════════════════════════════
import hashlib as _hl

def _hash_pw(pw: str) -> str:
    return _hl.sha256(pw.encode()).hexdigest()


@app.route("/")
def index():
    if session.get("user_id"):
        role = session.get("role")
        return redirect(url_for("worker_dashboard") if role == "worker" else url_for("employer_dashboard"))
    return render_template("index.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("index"))
    if request.method == "POST":
        if not verify_csrf():
            flash("Invalid request.", "error")
            return redirect(url_for("register"))
        ip = client_ip()
        if not LIMITER.allow(f"reg:{ip}", 5, 3600):
            flash("Too many registration attempts. Try again later.", "error")
            return redirect(url_for("register"))
        email = request.form.get("email", "").lower().strip()
        pw    = request.form.get("password", "")
        role  = request.form.get("role", "worker")
        if not email or not pw or role not in USER_ROLES:
            flash("All fields are required.", "error")
            return redirect(url_for("register"))
        if len(pw) < 8:
            flash("Password must be at least 8 characters.", "error")
            return redirect(url_for("register"))
        existing = STORE.query_one(f"SELECT id FROM {STORE.t('users')} WHERE email=%s", (email,))
        if existing:
            flash("An account with that email already exists.", "error")
            return redirect(url_for("register"))
        uid = STORE.execute(
            f"INSERT INTO {STORE.t('users')} (email, password_hash, role, connects_balance) VALUES (%s,%s,%s,%s)",
            (email, _hash_pw(pw), role, WORKER_SIGNUP_CONNECTS if role == "worker" else 0),
        )
        if role == "employer":
            STORE.execute(f"INSERT INTO {STORE.t('employer_profiles')} (user_id) VALUES (%s)", (uid,))
        _refresh_session(uid)
        send_email(email, "Welcome to TechBid!", f"<p>Welcome! Your {role} account is ready.")
        flash("Account created! Please complete your profile.", "success")
        return redirect(url_for("complete_profile"))
    return render_template("auth/register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("index"))
    if request.method == "POST":
        if not verify_csrf():
            flash("Invalid request.", "error")
            return redirect(url_for("login"))
        ip = client_ip()
        if not LIMITER.allow(f"login:{ip}", 10, 600):
            flash("Too many login attempts. Please wait.", "error")
            return redirect(url_for("login"))
        email = request.form.get("email", "").lower().strip()
        pw    = request.form.get("password", "")
        user  = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE email=%s", (email,))
        if not user or user.get("password_hash") != _hash_pw(pw):
            flash("Invalid email or password.", "error")
            return redirect(url_for("login"))
        _refresh_session(user["id"])
        if email in SETTINGS.admin_google_email_set:
            session["is_admin"] = True
        dest = url_for("worker_dashboard") if user["role"] == "worker" else url_for("employer_dashboard")
        return redirect(dest)
    return render_template("auth/login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("index"))


@app.route("/profile/complete", methods=["GET", "POST"])
@login_required
def complete_profile():
    uid  = session["user_id"]
    role = session.get("role")
    user = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE id=%s", (uid,))
    if request.method == "POST":
        if not verify_csrf():
            flash("Invalid request.", "error")
            return redirect(url_for("complete_profile"))
        full_name = request.form.get("full_name", "").strip()
        mobile    = request.form.get("mobile", "").strip()
        country   = request.form.get("country", "").strip()
        bio       = request.form.get("bio", "").strip()
        skills_raw = request.form.get("skills", "")
        specialty  = request.form.get("specialty", "").strip()

        skills_list = [s.strip() for s in skills_raw.split(",") if s.strip()][:20]

        # Handle profile picture upload
        pic_url = user.get("profile_pic_url") if user else None
        pic_file = request.files.get("profile_pic")
        if pic_file and pic_file.filename:
            ext = pic_file.filename.rsplit(".", 1)[-1].lower()
            if ext in {"jpg", "jpeg", "png", "webp"}:
                file_bytes = pic_file.read()
                fname = f"{uid}_{int(time.time())}.{ext}"
                uploaded = upload_profile_pic(file_bytes, fname)
                if uploaded:
                    pic_url = uploaded

        # Employer extra fields
        if role == "employer":
            company = request.form.get("company_name", "").strip()
            website = request.form.get("website", "").strip()
            STORE.execute(
                f"UPDATE {STORE.t('employer_profiles')} SET company_name=%s, website=%s WHERE user_id=%s",
                (company, website, uid),
            )

        STORE.execute(
            f"UPDATE {STORE.t('users')} SET full_name=%s, mobile=%s, country=%s, bio=%s, "
            f"skills=%s, specialty=%s, profile_pic_url=%s, profile_complete=1 WHERE id=%s",
            (full_name, mobile, country, bio, json.dumps(skills_list), specialty, pic_url, uid),
        )
        _refresh_session(uid)
        flash("Profile updated!", "success")
        return redirect(url_for("worker_dashboard") if role == "worker" else url_for("employer_dashboard"))

    emp_profile = None
    if role == "employer":
        emp_profile = STORE.query_one(f"SELECT * FROM {STORE.t('employer_profiles')} WHERE user_id=%s", (uid,))
    return render_template("auth/complete_profile.html", user=user, emp_profile=emp_profile, categories=JOB_CATEGORIES)


# ═══════════════════════════════════════════════════════════════════════════════
# Worker Routes
# ═══════════════════════════════════════════════════════════════════════════════
@app.route("/worker/dashboard")
@worker_required
def worker_dashboard():
    uid = session["user_id"]
    _refresh_session(uid)
    user = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE id=%s", (uid,))
    recent_apps = STORE.query_all(
        f"SELECT a.*, j.title, j.category, j.budget_usd, j.job_type FROM {STORE.t('applications')} a "
        f"JOIN {STORE.t('jobs')} j ON j.id=a.job_id WHERE a.user_id=%s ORDER BY a.applied_at DESC LIMIT 5",
        (uid,),
    )
    notifs = STORE.query_all(
        f"SELECT * FROM {STORE.t('notifications')} WHERE user_id=%s ORDER BY created_at DESC LIMIT 10", (uid,))
    STORE.execute(f"UPDATE {STORE.t('notifications')} SET is_read=1 WHERE user_id=%s", (uid,))
    return render_template("worker/dashboard.html", user=user, recent_apps=recent_apps, notifs=notifs)


@app.route("/worker/jobs")
@worker_required
@profile_required
def worker_jobs():
    cat     = request.args.get("category", "")
    jtype   = request.args.get("type", "")
    search  = request.args.get("q", "").strip()
    page    = max(1, int(request.args.get("page", 1)))
    per_page = 20
    offset  = (page - 1) * per_page

    wheres = ["j.status='open'"]
    params: list = []
    if cat and cat in JOB_CATEGORIES:
        wheres.append("j.category=%s"); params.append(cat)
    if jtype and jtype in JOB_TYPES:
        wheres.append("j.job_type=%s"); params.append(jtype)
    if search:
        wheres.append("(j.title LIKE %s OR j.description LIKE %s)")
        params += [f"%{search}%", f"%{search}%"]
    where_sql = " AND ".join(wheres)

    total_row = STORE.query_one(
        f"SELECT COUNT(*) as c FROM {STORE.t('jobs')} j WHERE {where_sql}", tuple(params))
    total = total_row["c"] if total_row else 0

    jobs = STORE.query_all(
        f"SELECT j.*, rw.robot_name, rw.connects_shown FROM {STORE.t('jobs')} j "
        f"LEFT JOIN {STORE.t('robot_winners')} rw ON rw.job_id=j.id "
        f"WHERE {where_sql} ORDER BY j.created_at DESC LIMIT %s OFFSET %s",
        tuple(params) + (per_page, offset),
    )
    uid = session["user_id"]
    applied_ids = {r["job_id"] for r in STORE.query_all(
        f"SELECT job_id FROM {STORE.t('applications')} WHERE user_id=%s", (uid,))}

    pages = max(1, (total + per_page - 1) // per_page)
    return render_template("worker/jobs.html", jobs=jobs, applied_ids=applied_ids,
                           page=page, pages=pages, total=total,
                           cat=cat, jtype=jtype, search=search)


@app.route("/worker/jobs/<int:job_id>")
@worker_required
@profile_required
def worker_job_detail(job_id):
    uid = session["user_id"]
    job = STORE.query_one(
        f"SELECT j.*, rw.robot_name, rw.connects_shown FROM {STORE.t('jobs')} j "
        f"LEFT JOIN {STORE.t('robot_winners')} rw ON rw.job_id=j.id WHERE j.id=%s", (job_id,))
    if not job:
        flash("Job not found.", "error"); return redirect(url_for("worker_jobs"))
    already_applied = STORE.query_one(
        f"SELECT id FROM {STORE.t('applications')} WHERE user_id=%s AND job_id=%s", (uid, job_id))
    applicant_count = STORE.query_one(
        f"SELECT COUNT(*) as c FROM {STORE.t('applications')} WHERE job_id=%s", (job_id,))
    return render_template("worker/job_detail.html", job=job,
                           already_applied=bool(already_applied),
                           applicant_count=applicant_count["c"] if applicant_count else 0)


@app.route("/worker/jobs/<int:job_id>/apply", methods=["POST"])
@worker_required
@profile_required
def worker_apply(job_id):
    if not verify_csrf():
        flash("Invalid request.", "error"); return redirect(url_for("worker_job_detail", job_id=job_id))
    uid = session["user_id"]
    job = STORE.query_one(f"SELECT * FROM {STORE.t('jobs')} WHERE id=%s AND status='open'", (job_id,))
    if not job:
        flash("Job not available.", "error"); return redirect(url_for("worker_jobs"))
    already = STORE.query_one(
        f"SELECT id FROM {STORE.t('applications')} WHERE user_id=%s AND job_id=%s", (uid, job_id))
    if already:
        flash("You've already applied for this job.", "warning"); return redirect(url_for("worker_job_detail", job_id=job_id))
    connects_needed = job["connects_required"]
    user = STORE.query_one(f"SELECT connects_balance FROM {STORE.t('users')} WHERE id=%s", (uid,))
    if not user or user["connects_balance"] < connects_needed:
        flash(f"You need {connects_needed} connects to apply. Buy more connects.", "warning")
        return redirect(url_for("worker_buy_connects"))
    cover = request.form.get("cover_letter", "").strip()[:2000]

    # Deduct connects atomically
    affected = STORE.execute(
        f"UPDATE {STORE.t('users')} SET connects_balance=connects_balance-%s "
        f"WHERE id=%s AND connects_balance>=%s",
        (connects_needed, uid, connects_needed),
    )
    if not affected:
        flash("Insufficient connects.", "warning"); return redirect(url_for("worker_buy_connects"))
    STORE.execute(
        f"INSERT INTO {STORE.t('applications')} (user_id, job_id, connects_spent, cover_letter) VALUES (%s,%s,%s,%s)",
        (uid, job_id, connects_needed, cover),
    )
    _refresh_session(uid)
    flash(f"Application submitted! {connects_needed} connects used.", "success")
    return redirect(url_for("worker_job_detail", job_id=job_id))


@app.route("/worker/connects")
@worker_required
def worker_buy_connects():
    uid = session["user_id"]
    _refresh_session(uid)
    history = STORE.query_all(
        f"SELECT * FROM {STORE.t('payments')} WHERE user_id=%s ORDER BY created_at DESC LIMIT 20", (uid,))
    return render_template("worker/buy_connects.html",
                           packages=CONNECTS_PACKAGES,
                           paystack_pub=SETTINGS.paystack_public_key,
                           history=history)


@app.route("/worker/connects/checkout", methods=["POST"])
@worker_required
def worker_connects_checkout():
    if not verify_csrf():
        return jsonify({"error": "Invalid request"}), 403
    pkg_id   = request.json.get("package_id", "") if request.is_json else request.form.get("package_id", "")
    provider = request.json.get("provider", "paystack") if request.is_json else request.form.get("provider", "paystack")
    pkg = next((p for p in CONNECTS_PACKAGES if p["id"] == pkg_id), None)
    if not pkg:
        return jsonify({"error": "Invalid package"}), 400

    uid   = session["user_id"]
    email = session.get("email", "")
    ref   = f"tbm_{uid}_{pkg_id}_{secrets.token_hex(8)}"
    amount_usd = pkg["price_usd"]
    amount_kes = SETTINGS.usd_to_kes_amount(amount_usd)

    STORE.execute(
        f"INSERT INTO {STORE.t('payments')} (user_id, provider, amount_usd, amount_kes, connects_awarded, status, reference) "
        f"VALUES (%s,%s,%s,%s,%s,'pending',%s)",
        (uid, provider, amount_usd, amount_kes, pkg["connects"], ref),
    )

    if provider == "paystack":
        cents = SETTINGS.usd_to_kes_cents(amount_usd)
        status, data = PAYSTACK.initialize(
            email=email, amount_cents=cents, reference=ref,
            callback_url=SETTINGS.paystack_callback_url, currency=SETTINGS.paystack_currency,
            metadata={"user_id": uid, "package_id": pkg_id},
        )
        if status == 200 and data.get("status"):
            return jsonify({"redirect_url": data["data"]["authorization_url"]})
        return jsonify({"error": data.get("message", "Payment init failed")}), 400

    elif provider == "pesapal":
        token, err = PESAPAL.get_token()
        if err or not token:
            return jsonify({"error": "PesaPal auth failed"}), 500
        ipn_id, err2 = PESAPAL.register_ipn(token, SETTINGS.pesapal_ipn_url)
        if err2 or not ipn_id:
            return jsonify({"error": "PesaPal IPN registration failed"}), 500
        mobile = STORE.query_one(f"SELECT mobile FROM {STORE.t('users')} WHERE id=%s", (uid,))
        phone  = mobile["mobile"] if mobile else ""
        status, data = PESAPAL.submit_order(
            token=token, ipn_id=ipn_id, reference=ref, email=email,
            amount=amount_kes, callback_url=SETTINGS.pesapal_callback_url,
            currency=SETTINGS.pesapal_currency, phone=phone,
        )
        if status in (200, 201) and data.get("redirect_url"):
            return jsonify({"redirect_url": data["redirect_url"]})
        return jsonify({"error": data.get("message", "PesaPal order failed")}), 400

    return jsonify({"error": "Unknown provider"}), 400


@app.route("/worker/profile")
@worker_required
def worker_profile():
    uid  = session["user_id"]
    user = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE id=%s", (uid,))
    apps = STORE.query_all(
        f"SELECT a.*, j.title, j.category FROM {STORE.t('applications')} a "
        f"JOIN {STORE.t('jobs')} j ON j.id=a.job_id WHERE a.user_id=%s ORDER BY a.applied_at DESC",
        (uid,),
    )
    skills = json.loads(user["skills"] or "[]") if user and user.get("skills") else []
    return render_template("worker/profile.html", user=user, apps=apps, skills=skills)


# ═══════════════════════════════════════════════════════════════════════════════
# Employer Routes
# ═══════════════════════════════════════════════════════════════════════════════
def _employer_check_subscription(uid: int) -> bool:
    import datetime
    ep = STORE.query_one(f"SELECT * FROM {STORE.t('employer_profiles')} WHERE user_id=%s", (uid,))
    if not ep:
        return False
    if not ep["is_subscribed"]:
        return False
    exp = ep.get("subscription_expires_at")
    if exp and exp < datetime.datetime.utcnow():
        STORE.execute(f"UPDATE {STORE.t('employer_profiles')} SET is_subscribed=0 WHERE user_id=%s", (uid,))
        return False
    return True


@app.route("/employer/dashboard")
@employer_required
def employer_dashboard():
    uid  = session["user_id"]
    user = STORE.query_one(f"SELECT * FROM {STORE.t('users')} WHERE id=%s", (uid,))
    ep   = STORE.query_one(f"SELECT * FROM {STORE.t('employer_profiles')} WHERE user_id=%s", (uid,))
    is_sub = _employer_check_subscription(uid)
    my_jobs = STORE.query_all(
        f"SELECT j.*, (SELECT COUNT(*) FROM {STORE.t('applications')} a WHERE a.job_id=j.id) as app_count "
        f"FROM {STORE.t('jobs')} j WHERE j.employer_id=%s ORDER BY j.created_at DESC LIMIT 10", (uid,))
    pending_pays = STORE.query_all(
        f"SELECT * FROM {STORE.t('employer_payments')} WHERE employer_id=%s AND status='pending'", (uid,))
    return render_template("employer/dashboard.html", user=user, ep=ep,
                           is_subscribed=is_sub, my_jobs=my_jobs, pending_pays=pending_pays)


@app.route("/employer/subscribe", methods=["POST"])
@employer_required
def employer_subscribe():
    if not verify_csrf():
        return jsonify({"error": "Invalid request"}), 403
    uid   = session["user_id"]
    email = session.get("email", "")
    provider = request.json.get("provider", "paystack") if request.is_json else request.form.get("provider", "paystack")
    ref = f"sub_{uid}_{secrets.token_hex(8)}"
    amount_usd = EMPLOYER_SUB_USD
    amount_kes = SETTINGS.usd_to_kes_amount(amount_usd)

    STORE.execute(
        f"INSERT INTO {STORE.t('subscriptions')} (employer_id, reference, amount_usd, status) VALUES (%s,%s,%s,'pending')",
        (uid, ref, amount_usd),
    )
    if provider == "paystack":
        cents = SETTINGS.usd_to_kes_cents(amount_usd)
        status, data = PAYSTACK.initialize(
            email=email, amount_cents=cents, reference=ref,
            callback_url=SETTINGS.paystack_callback_url, currency=SETTINGS.paystack_currency,
            metadata={"type": "subscription", "employer_id": uid},
        )
        if status == 200 and data.get("status"):
            return jsonify({"redirect_url": data["data"]["authorization_url"]})
        return jsonify({"error": data.get("message", "Payment init failed")}), 400
    return jsonify({"error": "Use paystack for subscriptions"}), 400


@app.route("/employer/jobs/post", methods=["GET", "POST"])
@employer_required
@profile_required
def employer_post_job():
    uid = session["user_id"]
    if not _employer_check_subscription(uid):
        flash("You need an active subscription to post jobs. Subscribe for $5/month.", "warning")
        return redirect(url_for("employer_dashboard"))
    if request.method == "POST":
        if not verify_csrf():
            flash("Invalid request.", "error"); return redirect(url_for("employer_post_job"))
        title    = request.form.get("title", "").strip()[:255]
        desc     = request.form.get("description", "").strip()[:3000]
        category = request.form.get("category", "")
        jtype    = request.form.get("job_type", "fixed")
        budget   = float(request.form.get("budget_usd", 0) or 0)
        duration = request.form.get("duration", "").strip()[:80]
        if not title or category not in JOB_CATEGORIES or jtype not in JOB_TYPES:
            flash("Please fill in all required fields correctly.", "error")
            return redirect(url_for("employer_post_job"))
        job_id = STORE.execute(
            f"INSERT INTO {STORE.t('jobs')} (employer_id, title, description, category, job_type, budget_usd, duration, is_robot, status) "
            f"VALUES (%s,%s,%s,%s,%s,%s,%s,0,'open')",
            (uid, title, desc, category, jtype, budget, duration),
        )
        flash("Job posted successfully!", "success")
        return redirect(url_for("employer_applicants", job_id=job_id))
    return render_template("employer/post_job.html", categories=JOB_CATEGORIES, job_types=JOB_TYPES)


@app.route("/employer/jobs/<int:job_id>/applicants")
@employer_required
def employer_applicants(job_id):
    uid = session["user_id"]
    job = STORE.query_one(f"SELECT * FROM {STORE.t('jobs')} WHERE id=%s AND employer_id=%s", (job_id, uid))
    if not job:
        flash("Job not found.", "error"); return redirect(url_for("employer_dashboard"))
    applicants = STORE.query_all(
        f"SELECT a.*, u.full_name, u.country, u.specialty, u.connects_balance, u.profile_pic_url, u.skills, u.bio "
        f"FROM {STORE.t('applications')} a JOIN {STORE.t('users')} u ON u.id=a.user_id "
        f"WHERE a.job_id=%s ORDER BY u.connects_balance DESC", (job_id,))
    for ap in applicants:
        ap["skills_list"] = json.loads(ap["skills"] or "[]") if ap.get("skills") else []
    return render_template("employer/applicants.html", job=job, applicants=applicants)


@app.route("/employer/applicants/<int:app_id>/accept", methods=["POST"])
@employer_required
def employer_accept_applicant(app_id):
    if not verify_csrf():
        flash("Invalid request.", "error"); return redirect(url_for("employer_dashboard"))
    uid = session["user_id"]
    application = STORE.query_one(
        f"SELECT a.*, j.employer_id, j.id as job_id FROM {STORE.t('applications')} a "
        f"JOIN {STORE.t('jobs')} j ON j.id=a.job_id WHERE a.id=%s", (app_id,))
    if not application or application["employer_id"] != uid:
        flash("Not found.", "error"); return redirect(url_for("employer_dashboard"))
    STORE.execute(f"UPDATE {STORE.t('applications')} SET status='accepted' WHERE id=%s", (app_id,))
    push_notif(application["user_id"], f"Congratulations! Your application was accepted.")
    send_email(
        STORE.query_one(f"SELECT email FROM {STORE.t('users')} WHERE id=%s", (application["user_id"],))["email"],
        "Application Accepted — TechBid",
        "<h2>Great news!</h2><p>Your application has been accepted. The employer will contact you shortly.</p>"
    )
    flash("Applicant accepted.", "success")
    return redirect(url_for("employer_applicants", job_id=application["job_id"]))


@app.route("/employer/payments")
@employer_required
def employer_payments():
    uid = session["user_id"]
    pays = STORE.query_all(
        f"SELECT ep.*, u.full_name as worker_name, j.title as job_title "
        f"FROM {STORE.t('employer_payments')} ep "
        f"JOIN {STORE.t('users')} u ON u.id=ep.worker_user_id "
        f"JOIN {STORE.t('jobs')} j ON j.id=ep.job_id "
        f"WHERE ep.employer_id=%s ORDER BY ep.created_at DESC", (uid,))
    return render_template("employer/payments.html", pays=pays)


# ═══════════════════════════════════════════════════════════════════════════════
# Payment Webhooks & Callbacks
# ═══════════════════════════════════════════════════════════════════════════════
def _credit_connects_or_subscription(reference: str) -> None:
    """Shared post-payment logic for both Paystack and PesaPal."""
    pay = STORE.query_one(f"SELECT * FROM {STORE.t('payments')} WHERE reference=%s AND status='pending'", (reference,))
    if pay:
        STORE.execute(f"UPDATE {STORE.t('payments')} SET status='confirmed' WHERE id=%s", (pay["id"],))
        STORE.execute(
            f"UPDATE {STORE.t('users')} SET connects_balance=connects_balance+%s WHERE id=%s",
            (pay["connects_awarded"], pay["user_id"]),
        )
        push_notif(pay["user_id"], f"Payment confirmed! {pay['connects_awarded']} connects added to your account.")
        user = STORE.query_one(f"SELECT email,full_name FROM {STORE.t('users')} WHERE id=%s", (pay["user_id"],))
        if user:
            send_email(user["email"], "Connects Added — TechBid",
                f"<p>Hi {user['full_name']}, your payment was confirmed and "
                f"<b>{pay['connects_awarded']} connects</b> have been added to your account.</p>")
        return

    # Check if it's a subscription payment
    sub = STORE.query_one(f"SELECT * FROM {STORE.t('subscriptions')} WHERE reference=%s AND status='pending'", (reference,))
    if sub:
        import datetime
        expires = datetime.datetime.utcnow() + datetime.timedelta(days=30)
        STORE.execute(f"UPDATE {STORE.t('subscriptions')} SET status='confirmed', expires_at=%s WHERE id=%s",
                      (expires, sub["id"]))
        STORE.execute(
            f"UPDATE {STORE.t('employer_profiles')} SET is_subscribed=1, subscription_expires_at=%s WHERE user_id=%s",
            (expires, sub["employer_id"]),
        )
        push_notif(sub["employer_id"], "Your employer subscription is now active!")


@app.route("/billing/paystack/callback")
def paystack_callback():
    ref = request.args.get("reference") or request.args.get("trxref")
    if ref:
        status, data = PAYSTACK.verify(ref)
        if status == 200 and data.get("data", {}).get("status") == "success":
            _credit_connects_or_subscription(ref)
            flash("Payment successful! Your account has been updated.", "success")
        else:
            flash("Payment verification failed. Contact support if debited.", "error")
    return redirect(url_for("worker_buy_connects") if session.get("role") == "worker" else url_for("employer_dashboard"))


@app.route("/api/payments/paystack/webhook", methods=["POST"])
def paystack_webhook():
    raw = request.get_data()
    sig = request.headers.get("X-Paystack-Signature")
    if not PAYSTACK.valid_sig(raw, sig):
        return "Forbidden", 403
    try:
        event = json.loads(raw)
    except Exception:
        return "Bad Request", 400
    if event.get("event") == "charge.success":
        ref = event.get("data", {}).get("reference")
        if ref:
            _credit_connects_or_subscription(ref)
    return "OK", 200


@app.route("/billing/pesapal/callback")
def pesapal_callback():
    ref = request.args.get("OrderMerchantReference") or request.args.get("reference")
    tracking_id = request.args.get("OrderTrackingId")
    if ref and tracking_id:
        token, _ = PESAPAL.get_token()
        if token:
            status, data = PESAPAL.get_tx_status(token, tracking_id)
            if status == 200 and data.get("payment_status_description", "").lower() == "completed":
                _credit_connects_or_subscription(ref)
                flash("Payment successful! Your account has been updated.", "success")
            else:
                flash("Payment pending or failed. Contact support if debited.", "warning")
    return redirect(url_for("worker_buy_connects") if session.get("role") == "worker" else url_for("employer_dashboard"))


@app.route("/api/payments/pesapal/ipn")
def pesapal_ipn():
    ref = request.args.get("OrderMerchantReference")
    tracking_id = request.args.get("OrderTrackingId")
    if ref and tracking_id:
        token, _ = PESAPAL.get_token()
        if token:
            status, data = PESAPAL.get_tx_status(token, tracking_id)
            if status == 200 and data.get("payment_status_description", "").lower() == "completed":
                _credit_connects_or_subscription(ref)
    return jsonify({"orderNotificationType": "IPNCHANGE", "orderTrackingId": tracking_id,
                    "orderMerchantReference": ref, "status": 200})


# ═══════════════════════════════════════════════════════════════════════════════
# Contact Page
# ═══════════════════════════════════════════════════════════════════════════════
@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        if not verify_csrf():
            flash("Invalid request.", "error"); return redirect(url_for("contact"))
        name    = request.form.get("name", "").strip()
        email   = request.form.get("email", "").strip()
        message = request.form.get("message", "").strip()
        if name and email and message:
            send_email(SETTINGS.smtp_from_email or SETTINGS.smtp_user,
                f"Contact Form: {name} <{email}>",
                f"<p><b>From:</b> {name} ({email})</p><p>{message}</p>")
            flash("Thank you! We'll get back to you soon.", "success")
        else:
            flash("Please fill in all fields.", "error")
        return redirect(url_for("contact"))
    return render_template("contact.html")


# ═══════════════════════════════════════════════════════════════════════════════
# Admin Routes
# ═══════════════════════════════════════════════════════════════════════════════
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if session.get("is_admin"):
        return redirect(url_for("admin_dashboard"))
    if request.method == "POST":
        if not verify_csrf(): flash("Invalid request.", "error"); return redirect(url_for("admin_login"))
        u = request.form.get("username", "")
        p = request.form.get("password", "")
        if u == SETTINGS.admin_username and p == SETTINGS.admin_password:
            session["is_admin"] = True
            return redirect(url_for("admin_dashboard"))
        flash("Invalid admin credentials.", "error")
    return render_template("admin/login.html")


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect(url_for("index"))


@app.route("/admin")
@admin_required
def admin_dashboard():
    users_count   = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('users')}") or {}).get("c", 0)
    workers_count = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('users')} WHERE role='worker'") or {}).get("c", 0)
    emp_count     = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('users')} WHERE role='employer'") or {}).get("c", 0)
    jobs_count    = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('jobs')}") or {}).get("c", 0)
    robot_count   = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('jobs')} WHERE is_robot=1") or {}).get("c", 0)
    apps_count    = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('applications')}") or {}).get("c", 0)
    revenue_row   = STORE.query_one(f"SELECT COALESCE(SUM(amount_usd),0) as r FROM {STORE.t('payments')} WHERE status='confirmed'")
    revenue_usd   = float(revenue_row["r"]) if revenue_row else 0.0
    pending_disb  = (STORE.query_one(f"SELECT COUNT(*) as c FROM {STORE.t('employer_payments')} WHERE status='pending'") or {}).get("c", 0)
    recent_pays   = STORE.query_all(
        f"SELECT p.*, u.email FROM {STORE.t('payments')} p JOIN {STORE.t('users')} u ON u.id=p.user_id "
        f"ORDER BY p.created_at DESC LIMIT 10")
    return render_template("admin/dashboard.html", users_count=users_count, workers_count=workers_count,
                           emp_count=emp_count, jobs_count=jobs_count, robot_count=robot_count,
                           apps_count=apps_count, revenue_usd=revenue_usd, pending_disb=pending_disb,
                           recent_pays=recent_pays)


@app.route("/admin/users")
@admin_required
def admin_users():
    users = STORE.query_all(
        f"SELECT * FROM {STORE.t('users')} ORDER BY created_at DESC LIMIT 200")
    return render_template("admin/users.html", users=users)


@app.route("/admin/jobs")
@admin_required
def admin_jobs():
    jobs = STORE.query_all(
        f"SELECT j.*, rw.robot_name, rw.connects_shown FROM {STORE.t('jobs')} j "
        f"LEFT JOIN {STORE.t('robot_winners')} rw ON rw.job_id=j.id "
        f"ORDER BY j.created_at DESC LIMIT 200")
    return render_template("admin/jobs.html", jobs=jobs)


@app.route("/admin/jobs/create-robot", methods=["GET", "POST"])
@admin_required
def admin_create_robot_job():
    if request.method == "POST":
        if not verify_csrf(): flash("Invalid request.", "error"); return redirect(url_for("admin_create_robot_job"))
        import random
        title    = request.form.get("title", "").strip()[:255]
        desc     = request.form.get("description", "").strip()[:3000]
        category = request.form.get("category", "")
        jtype    = request.form.get("job_type", "fixed")
        budget   = float(request.form.get("budget_usd", 500) or 500)
        duration = request.form.get("duration", "1 month").strip()[:80]
        connects = int(request.form.get("connects_required", 20) or 20)
        robot_name  = request.form.get("robot_name", random.choice(_AI_ROBOT_NAMES)).strip()
        robot_conn  = int(request.form.get("robot_connects", random.randint(300, 1500)) or 500)
        if not title or category not in JOB_CATEGORIES or jtype not in JOB_TYPES:
            flash("Fill all fields correctly.", "error"); return redirect(url_for("admin_create_robot_job"))
        job_id = STORE.execute(
            f"INSERT INTO {STORE.t('jobs')} (employer_id, title, description, category, job_type, budget_usd, "
            f"duration, connects_required, is_robot, status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,'open')",
            (None, title, desc, category, jtype, budget, duration, connects),
        )
        STORE.execute(
            f"INSERT INTO {STORE.t('robot_winners')} (job_id, robot_name, connects_shown) VALUES (%s,%s,%s)",
            (job_id, robot_name, robot_conn),
        )
        flash("Robot job created!", "success"); return redirect(url_for("admin_jobs"))
    return render_template("admin/create_robot_job.html", categories=JOB_CATEGORIES, job_types=JOB_TYPES,
                           robot_names=_AI_ROBOT_NAMES)


@app.route("/admin/jobs/generate-ai", methods=["POST"])
@admin_required
def admin_generate_ai_jobs():
    if not verify_csrf():
        flash("Invalid request.", "error"); return redirect(url_for("admin_jobs"))
    t = threading.Thread(target=_generate_ai_jobs, args=(STORE,), daemon=True)
    t.start()
    flash("AI job generation started in the background. Check back in a moment.", "info")
    return redirect(url_for("admin_jobs"))


@app.route("/admin/jobs/<int:job_id>/delete", methods=["POST"])
@admin_required
def admin_delete_job(job_id):
    if not verify_csrf():
        flash("Invalid request.", "error"); return redirect(url_for("admin_jobs"))
    STORE.execute(f"DELETE FROM {STORE.t('jobs')} WHERE id=%s", (job_id,))
    flash("Job deleted.", "success"); return redirect(url_for("admin_jobs"))


@app.route("/admin/payments")
@admin_required
def admin_payments():
    pays = STORE.query_all(
        f"SELECT p.*, u.email, u.full_name FROM {STORE.t('payments')} p "
        f"JOIN {STORE.t('users')} u ON u.id=p.user_id ORDER BY p.created_at DESC LIMIT 200")
    emp_pays = STORE.query_all(
        f"SELECT ep.*, eu.email as employer_email, wu.full_name as worker_name, wu.email as worker_email, j.title "
        f"FROM {STORE.t('employer_payments')} ep "
        f"JOIN {STORE.t('users')} eu ON eu.id=ep.employer_id "
        f"JOIN {STORE.t('users')} wu ON wu.id=ep.worker_user_id "
        f"JOIN {STORE.t('jobs')} j ON j.id=ep.job_id "
        f"ORDER BY ep.created_at DESC LIMIT 200")
    return render_template("admin/payments.html", pays=pays, emp_pays=emp_pays)


@app.route("/admin/payments/<int:pay_id>/disburse", methods=["POST"])
@admin_required
def admin_disburse(pay_id):
    if not verify_csrf():
        flash("Invalid request.", "error"); return redirect(url_for("admin_payments"))
    note = request.form.get("admin_note", "").strip()
    STORE.execute(
        f"UPDATE {STORE.t('employer_payments')} SET status='disbursed', admin_note=%s WHERE id=%s",
        (note, pay_id),
    )
    ep = STORE.query_one(f"SELECT * FROM {STORE.t('employer_payments')} WHERE id=%s", (pay_id,))
    if ep:
        push_notif(ep["worker_user_id"], "Payment disbursed to you by admin. Check your account.")
        push_notif(ep["employer_id"], "Your payment has been processed and disbursed to the worker.")
    flash("Payment marked as disbursed.", "success")
    return redirect(url_for("admin_payments"))


# ═══════════════════════════════════════════════════════════════════════════════
# Error Handlers
# ═══════════════════════════════════════════════════════════════════════════════
@app.errorhandler(404)
def not_found(e):
    return render_template("errors/404.html"), 404

@app.errorhandler(500)
def server_error(e):
    return render_template("errors/500.html"), 500

@app.errorhandler(413)
def too_large(e):
    flash("File too large. Maximum upload size is 5MB.", "error")
    return redirect(request.referrer or url_for("index"))


# ═══════════════════════════════════════════════════════════════════════════════
# App Startup
# ═══════════════════════════════════════════════════════════════════════════════
def _startup() -> None:
    STORE.ensure_schema()
    # Start background email worker
    t_email = threading.Thread(target=_email_worker, daemon=True)
    t_email.start()
    # Start AI job generator (runs daily)
    t_ai = threading.Thread(target=_ai_job_thread, args=(STORE,), daemon=True)
    t_ai.start()
    LOG.info("TechBid Marketplace started.")


_startup()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    LOG.info("Starting on http://%s:%d", "127.0.0.1", port)
    app.run(host=host, port=port, debug=False)
