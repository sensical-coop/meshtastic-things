"""Optional to create one owner + mesh + gateway from
KEYAPI__SEED_* env vars (all five required)."""
import os

import structlog
from django.core.management.base import BaseCommand

from registry.auth import generate_api_key, hash_api_key
from registry.kafka_publisher import get_publisher
from registry.models import Device, Mesh, Owner

log = structlog.get_logger()


class Command(BaseCommand):
    help = "Create one bootstrap owner + mesh + gateway from KEYAPI__SEED_* env vars, if set."

    def handle(self, *args, **options):
        seed_owner_name = os.environ.get("KEYAPI__SEED_OWNER_NAME")
        seed_owner_email = os.environ.get("KEYAPI__SEED_OWNER_EMAIL")
        seed_channel_id = os.environ.get("KEYAPI__SEED_CHANNEL_ID")
        seed_psk_b64 = os.environ.get("KEYAPI__SEED_PSK_B64")
        seed_gateway_device_id = os.environ.get("KEYAPI__SEED_GATEWAY_DEVICE_ID")
        if not (seed_owner_name and seed_owner_email and seed_channel_id and seed_psk_b64 and seed_gateway_device_id):
            self.stdout.write("KEYAPI__SEED_* not fully set - nothing to seed.")
            return

        owner = Owner.objects.filter(email=seed_owner_email).first()
        if owner is None:
            api_key = generate_api_key()
            owner = Owner.objects.create_user(
                email=seed_owner_email, name=seed_owner_name, api_key_hash=hash_api_key(api_key)
            )
            # Only chance to ever see this key in plaintext - it's not stored.
            self.stdout.write(f"Seeded bootstrap owner {seed_owner_email} - api_key={api_key}")

        mesh = Mesh.objects.filter(owner=owner, channel_id=seed_channel_id).first()
        if mesh is None:
            mesh = Mesh.objects.create(owner=owner, channel_id=seed_channel_id, psk_b64=seed_psk_b64, label="seeded from env")
            self.stdout.write(f"Seeded initial mesh {mesh.id} (channel_id={seed_channel_id})")

        device_id = int(seed_gateway_device_id)
        if not Device.objects.filter(mesh=mesh, device_id=device_id, is_gateway=True).exists():
            gateway = Device.objects.create(
                mesh=mesh, is_gateway=True, device_id=device_id, label="seeded from env"
            )
            get_publisher().publish_gateway_upsert(gateway, mesh)
            self.stdout.write(f"Seeded initial gateway {gateway.id} (device_id={device_id})")
