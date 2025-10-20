# auth.py
from __future__ import annotations
import secrets
from fastapi import APIRouter, Request, Form, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import usuarios  # nuestro módulo
import time

templates = Jinja2Templates(directory="templates")
router = APIRouter(prefix="/auth", tags=["auth"])

def _csrf_new(request: Request) -> str:
    tok = secrets.token_urlsafe(16)
    request.session["csrf"] = tok
    return tok

def _csrf_check(request: Request, token: str):
    sess = request.session.get("csrf")
    if not sess or not token or not secrets.compare_digest(str(sess), str(token)):
        raise HTTPException(status_code=400, detail="CSRF inválido")


def current_user(request: Request) -> str | None:
    if not hasattr(request, "session"):
        return None
    return request.session.get("user_email")

def require_login(request: Request):
    if not request.session.get("user_email"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    return request.session["user_email"]

@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request, next: str = "/buscar"):
    return templates.TemplateResponse("auth_login.html", {"request": request, "next": next, "csrf": _csrf_new(request)})

@router.post("/login", response_class=HTMLResponse)
def login_do(request: Request, email: str = Form(...), password: str = Form(...),
             next: str = Form("/buscar"), csrf: str = Form(...)):
    _csrf_check(request, csrf)
    ok = usuarios.authenticate(email, password)
    if not ok:
        return templates.TemplateResponse("auth_login.html", {
            "request": request,
            "error": "Credenciales inválidas",
            "next": next,
            "csrf": _csrf_new(request)
        })
    request.session["user_email"] = usuarios._norm_email(email)
    request.session["login_ts"] = time.time()   # ← IMPORTANTE para TTL
    return RedirectResponse(next or "/buscar", status_code=302)

@router.post("/logout")
def logout(request: Request, csrf: str = Form(...)):
    _csrf_check(request, csrf)
    request.session.clear()
    return RedirectResponse("/auth/login", status_code=302)

@router.get("/register", response_class=HTMLResponse)
def register_form(request: Request):
    return templates.TemplateResponse("auth_register.html", {"request": request, "csrf": _csrf_new(request)})

@router.post("/register", response_class=HTMLResponse)
def register_do(request: Request, email: str = Form(...), name: str = Form(""), password: str = Form(...), csrf: str = Form(...)):
    _csrf_check(request, csrf)
    try:
        usuarios.create_user(email=email, password=password, name=name)
    except Exception as e:
        return templates.TemplateResponse("auth_register.html", {"request": request, "error": str(e), "csrf": _csrf_new(request)})
    request.session["user_email"] = usuarios._norm_email(email)
    return RedirectResponse("/buscar", status_code=302)

@router.get("/forgot", response_class=HTMLResponse)
def forgot_form(request: Request):
    return templates.TemplateResponse("auth_forgot.html", {"request": request, "csrf": _csrf_new(request)})

@router.post("/forgot", response_class=HTMLResponse)
def forgot_do(request: Request, email: str = Form(...), csrf: str = Form(...)):
    _csrf_check(request, csrf)
    token = usuarios.start_password_reset(email)
    # Enviamos siempre mensaje genérico
    try:
        from main import send_mail
        reset_link = f"{request.url_for('reset_form')}?token={token}" if token else ""
        if reset_link:
            send_mail("Reset de contraseña", f"<p>Para restablecer tu clave haz clic: <a href='{reset_link}'>{reset_link}</a> (válido por 2 horas).</p>", to_addrs=[usuarios._norm_email(email)])
    except Exception:
        pass
    return templates.TemplateResponse("auth_forgot.html", {"request": request, "info": "Si el correo existe, te enviaremos instrucciones.", "csrf": _csrf_new(request)})

@router.get("/reset", response_class=HTMLResponse, name="reset_form")
def reset_form(request: Request, token: str):
    return templates.TemplateResponse("auth_reset.html", {"request": request, "token": token, "csrf": _csrf_new(request)})

@router.post("/reset", response_class=HTMLResponse)
def reset_do(request: Request, token: str = Form(...), password: str = Form(...), csrf: str = Form(...)):
    _csrf_check(request, csrf)
    try:
        usuarios.complete_password_reset(token, password)
    except Exception as e:
        return templates.TemplateResponse("auth_reset.html", {"request": request, "error": str(e), "token": token, "csrf": _csrf_new(request)})
    return RedirectResponse("/auth/login", status_code=302)
