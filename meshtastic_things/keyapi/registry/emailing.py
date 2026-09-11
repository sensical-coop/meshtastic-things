import structlog
from django.conf import settings
from django.core.mail import send_mail
from django.template.loader import render_to_string

log = structlog.get_logger()


def send_templated_email(to_email: str, subject: str, template_name: str, context: dict) -> None:
    """Renders registry/templates/registry/emails/<template_name>.txt with context and
    sends it plaintext-only via the configured EMAIL_BACKEND"""

    body = render_to_string(f"registry/emails/{template_name}.txt", context)
    send_mail(subject, body, settings.DEFAULT_FROM_EMAIL, [to_email], fail_silently=False)


def send_verification_email(owner, token: str) -> None:

    verify_link = f"{settings.PUBLIC_BASE_URL}/owners/verify-email?token={token}"
    send_templated_email(
        to_email=owner.email,
        subject="Verify your email - Meshtastic Key Management API",
        template_name="verify_email",
        context={"name": owner.name, "verify_link": verify_link, "ttl_hours": settings.EMAIL_VERIFICATION_TTL_HOURS},
    )


def send_password_reset_email(owner, token: str) -> None:

    reset_link = f"{settings.PUBLIC_BASE_URL}/owners/reset-password?token={token}"
    send_templated_email(
        to_email=owner.email,
        subject="Reset your password - Meshtastic Key Management API",
        template_name="password_reset",
        context={"name": owner.name, "reset_link": reset_link, "ttl_hours": settings.PASSWORD_RESET_TTL_HOURS},
    )
