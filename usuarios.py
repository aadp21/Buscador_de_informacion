# usuarios.py
from __future__ import annotations
import os, secrets, time
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone
from passlib.hash import bcrypt_sha256
from conector_sheets import leer_hoja, escribir_hoja_stream
import os
from passlib.hash import bcrypt_sha256 as _bcrypt_sha256, bcrypt as _bcrypt


bcrypt_sha256 = _bcrypt_sha256.using(truncate_error=False)


# === Config ===
SHEET_ID = os.getenv("SHEET_ID", "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI")
USERS_SHEET = os.getenv("USERS_SHEET", "Usuarios")

COLUMNS = [
    "email", "name", "password_hash", "role",
    "is_active", "created_at", "reset_token", "reset_expires"
]

# === Cache lectura corta (para no golpear cuotas) ===
_CACHE_ROWS: List[Dict] | None = None
_CACHE_TS: float | None = None
CACHE_TTL = int(os.getenv("USUARIOS_CACHE_TTL", "60"))  # seg

def create_user():
    print("USING HASHER: bcrypt_sha256 (create_user)")

def authenticate(email: str, password: str) -> bool:
    u = get_user(email)
    if not u or (u.get("is_active", "TRUE") not in ("TRUE", "True", "true", "1")):
        return False

    h = (u.get("password_hash") or "").strip()

    # 1) Intento principal: bcrypt_sha256 (tu formato actual)
    try:
        return bcrypt_sha256.verify(password, h)
    except Exception:
        # 2) Fallback opcional: por si el hash fuera bcrypt puro ($2b$...), permite login
        try:
            return _bcrypt.verify(password, h)
        except Exception:
            return False



def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _norm_email(e: str) -> str:
    return (e or "").strip().lower()

def _rows_to_list(df) -> List[Dict]:
    return [] if df is None or df.empty else df.fillna("").to_dict(orient="records")

def _load_rows(force: bool = False) -> List[Dict]:
    global _CACHE_ROWS, _CACHE_TS
    now = time.time()
    if not force and _CACHE_ROWS is not None and _CACHE_TS and (now - _CACHE_TS) < CACHE_TTL:
        return _CACHE_ROWS
    df = leer_hoja(SHEET_ID, USERS_SHEET)
    rows = _rows_to_list(df)
    # normaliza columnas/valores básicos
    normd = []
    seen = set()
    for r in rows:
        item = {c: str(r.get(c, "")).strip() for c in COLUMNS}
        em = _norm_email(item.get("email", ""))
        if not em or em in seen:
            continue
        item["email"] = em
        seen.add(em)
        normd.append(item)
    _CACHE_ROWS, _CACHE_TS = normd, now
    return normd

def _save_rows(rows: List[Dict]):
    """Vuelca todas las filas; backoff simple ante 429."""
    def iter_rows():
        yield COLUMNS
        for r in rows:
            yield [r.get(c, "") for c in COLUMNS]
    # invalidar cache
    global _CACHE_ROWS, _CACHE_TS
    _CACHE_ROWS, _CACHE_TS = None, None

    for delay in (0, 1, 2, 4):  # 4 reintentos (0/1/2/4s)
        try:
            escribir_hoja_stream(SHEET_ID, USERS_SHEET, iter_rows(), batch_rows=5000)
            return
        except Exception as e:
            msg = str(e)
            if "429" in msg or "RATE_LIMIT" in msg:
                time.sleep(delay)
                continue
            raise

def get_user(email: str) -> Optional[Dict]:
    em = _norm_email(email)
    for r in _load_rows():
        if r.get("email") == em:
            return r.copy()
    return None

def list_users() -> List[Dict]:
    return [r.copy() for r in _load_rows()]

def create_user(email: str, password: str, name: str = "", role: str = "customer") -> Dict:
    email = _norm_email(email)
    if not email or "@" not in email:
        raise ValueError("Email inválido")
    if get_user(email):
        raise ValueError("Ya existe un usuario con ese email")
    if len(password) < 8:
        raise ValueError("La clave debe tener al menos 8 caracteres")

    rows = _load_rows()
    rows.append({
        "email": email,
        "name": (name or "").strip(),
        "password_hash": bcrypt_sha256.hash(password),
        "role": role if role in ("admin", "customer") else "customer",
        "is_active": "TRUE",
        "created_at": _utcnow().isoformat(),
        "reset_token": "",
        "reset_expires": ""
    })
    _save_rows(rows)
    return get_user(email)  # post-escritura

def update_user(email: str, **fields) -> Dict:
    email = _norm_email(email)
    rows = _load_rows()
    found = False
    for r in rows:
        if r.get("email") == email:
            found = True
            for k, v in fields.items():
                if k in COLUMNS and k not in ("email", "password_hash"):  # no cambiar email/ hash aquí
                    r[k] = str(v).strip()
            break
    if not found:
        raise ValueError("Usuario inexistente")
    _save_rows(rows)
    return get_user(email)

def deactivate_user(email: str):
    update_user(email, is_active="FALSE")

def set_password(email: str, new_password: str):
    if len(new_password) < 8:
        raise ValueError("La clave debe tener al menos 8 caracteres")
    email = _norm_email(email)
    rows = _load_rows()
    for r in rows:
        if r.get("email") == email:
            r["password_hash"] = bcrypt_sha256.hash(new_password)
            r["reset_token"] = ""
            r["reset_expires"] = ""
            _save_rows(rows)
            return
    raise ValueError("Usuario inexistente")

def authenticate(email: str, password: str) -> bool:
    u = get_user(email)
    if not u or (u.get("is_active", "TRUE") not in ("TRUE", "True", "true", "1")):
        return False
    return bcrypt_sha256.verify(password, u.get("password_hash",""))

def start_password_reset(email: str) -> str:
    u = get_user(email)
    if not u:
        # respondemos siempre igual desde el caller, pero aquí devolvemos token vacío
        return ""
    token = secrets.token_urlsafe(32)
    expires = (_utcnow() + timedelta(hours=2)).isoformat()
    update_user(email, reset_token=token, reset_expires=expires)
    return token

def complete_password_reset(token: str, new_password: str) -> str:
    if len(new_password) < 8:
        raise ValueError("La clave debe tener al menos 8 caracteres")
    rows = _load_rows()
    now = _utcnow()
    for r in rows:
        if r.get("reset_token") == token:
            exp = r.get("reset_expires", "")
            try:
                if not exp or now > datetime.fromisoformat(exp):
                    raise ValueError("Token expirado")
            except Exception:
                raise ValueError("Token inválido o expirado")
            r["password_hash"] = bcrypt_sha256.hash(new_password)
            r["reset_token"] = ""
            r["reset_expires"] = ""
            _save_rows(rows)
            return r["email"]
    raise ValueError("Token inválido")
