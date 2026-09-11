"""keyapi's createsuperuser replacing Django's interactive username/password
flow

This creates the bearer API key Owners

  docker compose exec key_management_api python manage.py createsuperuser \\
    --email you@example.com --name "You"

If the owner already exists, it just grants superuser (--name is ignored) and
creates an API key if they don't already have one.
Re-running against an existing superuser with a key does nothing.

A superuser also gets a Django admin-panel login. Never touched on a re-run unless
--password is passed explicitly.
"""
import secrets

from django.core.management.base import BaseCommand, CommandError

from registry.auth import generate_api_key, hash_api_key
from registry.models import Owner


class Command(BaseCommand):
    help = "Create or promote an Owner to superuser, minting an API key and admin-panel password."

    def add_arguments(self, parser):
        parser.add_argument("--email", required=True)
        parser.add_argument("--name", default=None, help="required only if the owner doesn't exist yet")
        parser.add_argument("--password", default=None, help="admin-panel login password; random if omitted")

    def handle(self, *args, **options):
        email = options["email"]
        owner = Owner.objects.filter(email=email).first()
        created = False

        if owner is None:
            if not options["name"]:
                raise CommandError(f"No owner with email {email} yet - pass --name to create one")
            owner = Owner.objects.create_superuser(email=email, name=options["name"])
            created = True
            self.stdout.write(self.style.SUCCESS(f"Created owner {email} and granted superuser."))
        elif owner.is_superuser:
            self.stdout.write(f"{email} is already a superuser.")
        else:
            owner.is_superuser = True
            owner.is_staff = True
            owner.save(update_fields=["is_superuser", "is_staff"])
            self.stdout.write(self.style.SUCCESS(f"Granted superuser to {email}."))

        if not owner.api_key_hash:
            api_key = generate_api_key()
            owner.api_key_hash = hash_api_key(api_key)
            self.stdout.write("Save this API key - it is never shown again:")
            self.stdout.write(f"  export OWNER_API_KEY={api_key}")

        if options["password"] or created or not owner.has_usable_password():
            password = options["password"] or secrets.token_urlsafe(16)
            owner.set_password(password)
            if not options["password"]:
                self.stdout.write(f"Admin-panel login password (also never shown again): {password}")

        owner.save()
