"""Cliente mínimo de la API REST de Trino (/v1/statement), sin dependencias extra."""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

URL = os.environ.get("TRINO_URL", "http://trino:8080")
ADMIN = "breb_admin"


class ErrorTrino(RuntimeError):
    def __init__(self, mensaje: str, tipo: str = ""):
        super().__init__(mensaje)
        self.tipo = tipo


def consulta(sql: str, usuario: str = ADMIN, catalogo: str = "lakehouse") -> tuple[list[str], list[list]]:
    """Ejecuta `sql` como `usuario` y devuelve (columnas, filas)."""
    cab = {"X-Trino-User": usuario, "X-Trino-Source": "breb", "X-Trino-Catalog": catalogo}
    r = json.load(urllib.request.urlopen(
        urllib.request.Request(f"{URL}/v1/statement", data=sql.encode(), headers=cab, method="POST"), timeout=60))
    columnas, filas = [], []
    while True:
        if r.get("error"):
            e = r["error"]
            raise ErrorTrino(e.get("message", str(e)), e.get("errorName", ""))
        if r.get("columns") and not columnas:
            columnas = [c["name"] for c in r["columns"]]
        filas += r.get("data", [])
        if "nextUri" not in r:
            return columnas, filas
        r = json.load(urllib.request.urlopen(urllib.request.Request(r["nextUri"], headers=cab), timeout=60))


def esperar(segundos: int = 120) -> None:
    limite = time.monotonic() + segundos
    while True:
        try:
            if consulta("SELECT 1")[1] == [[1]]:
                return
        except (urllib.error.URLError, ConnectionError, ErrorTrino) as e:
            if time.monotonic() > limite:
                raise SystemExit(f"Trino no responde en {URL} después de {segundos} s: {e}. "
                                 "¿Está levantado el perfil consumo? docker compose --profile consumo up -d")
        time.sleep(3)


def tabla_texto(columnas: list[str], filas: list[list], maximo: int = 20) -> str:
    filas = [["" if v is None else str(v) for v in f] for f in filas[:maximo]]
    anchos = [max([len(c)] + [len(f[i]) for f in filas]) for i, c in enumerate(columnas)]
    lineas = ["  ".join(c.ljust(a) for c, a in zip(columnas, anchos))]
    lineas += ["  ".join(v.ljust(a) for v, a in zip(f, anchos)) for f in filas]
    return "\n".join(lineas)
