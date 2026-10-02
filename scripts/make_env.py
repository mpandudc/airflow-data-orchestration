"""Print a fresh .env with random local secrets."""

import base64
import secrets

print(f"AIRFLOW_FERNET_KEY={base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()}")
print(f"AIRFLOW_WEBSERVER_SECRET_KEY={secrets.token_urlsafe(32)}")
print(f"AIRFLOW_ADMIN_PASSWORD={secrets.token_urlsafe(16)}")
print(f"AIRFLOW_DB_PASSWORD={secrets.token_urlsafe(16)}")
