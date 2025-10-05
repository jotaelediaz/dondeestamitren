# app/core/user_prefs.py

from app.services.routes_repo import get_repo as get_routes_repo

COOKIE_NAME = "demt.nucleus"
COOKIE_MAX_AGE = 60 * 60 * 24 * 365  # 1 año


def _catalog_slugs() -> set[str]:
    repo = get_routes_repo()
    try:
        nuclei = repo.list_nuclei() or []
        return {
            (n.get("slug") or "").strip().lower() for n in nuclei if (n.get("slug") or "").strip()
        }
    except Exception:
        return set()


def sanitize_slug(slug: str | None) -> str | None:
    if not slug:
        return None
    s = (slug or "").strip().lower()
    return s if s in _catalog_slugs() else None


def get_cookie(request) -> str | None:
    return sanitize_slug(request.cookies.get(COOKIE_NAME))


def get_from_header(request) -> str | None:
    return sanitize_slug(request.headers.get("X-User-Nucleus"))


def get_current_nucleus(request) -> str | None:
    return get_cookie(request) or get_from_header(request)


def set_cookie(response, slug: str):
    response.set_cookie(
        COOKIE_NAME,
        slug,
        max_age=COOKIE_MAX_AGE,
        path="/",
        samesite="Lax",
        secure=False,
        httponly=False,
    )


def clear_cookie(response):
    response.delete_cookie(COOKIE_NAME, path="/")
