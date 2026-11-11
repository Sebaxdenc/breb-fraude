"""Nombres de los buckets del lakehouse.

En S3 los nombres de bucket son únicos en todo AWS, así que `lakehouse-bronze` ya puede tener dueño.
BREB_BUCKET_PREFIJO los distingue: en la EC2, infra/aws/preparar_ec2.sh lo fija en "breb-<cuenta>-".
En local queda vacío y los nombres no cambian.
"""
import os

PREFIJO = os.environ.get("BREB_BUCKET_PREFIJO", "")
CAPAS = ("bronze", "silver", "gold")


def bucket(capa: str) -> str:
    """Nombre del bucket de una capa: bronze, silver o gold."""
    return f"{PREFIJO}lakehouse-{capa}"


def ruta(capa: str, tabla: str) -> str:
    """Ruta s3a:// de una tabla dentro del bucket de su capa."""
    return f"s3a://{bucket(capa)}/{tabla}"
