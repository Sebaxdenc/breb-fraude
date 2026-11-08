"""Contratos de integración: esquema del evento, de la alerta y validación (RF2)."""
from __future__ import annotations

import json
import pathlib
import uuid
from datetime import datetime
from functools import lru_cache

from jsonschema import Draft202012Validator, FormatChecker

DIR = pathlib.Path(__file__).parent
NAMESPACE_EVENTOS = uuid.UUID("2b0c8a3e-4f7d-5a61-9c2e-8d1f0b7a6e55")


@lru_cache(maxsize=None)
def _validador(nombre: str) -> Draft202012Validator:
    esquema = json.loads((DIR / f"{nombre}.schema.json").read_text())
    Draft202012Validator.check_schema(esquema)
    return Draft202012Validator(esquema, format_checker=FormatChecker())


def esquema(nombre: str) -> dict:
    return json.loads((DIR / f"{nombre}.schema.json").read_text())


def errores_evento(evento: dict) -> list[str]:
    """Lista vacía si el evento cumple el contrato. Los mensajes van a Bronze en validation_errors."""
    errores = [f"{'/'.join(map(str, e.path)) or '(raíz)'}: {e.message}"
               for e in _validador("evento_transferencia").iter_errors(evento)]
    ts = evento.get("event_ts")
    if isinstance(ts, str) and not errores:
        try:
            datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            errores.append("event_ts: fecha inexistente")
    return errores


def errores_alerta(alerta: dict) -> list[str]:
    return [f"{'/'.join(map(str, e.path)) or '(raíz)'}: {e.message}" for e in _validador("alerta_fraude").iter_errors(alerta)]


def uuid_transaccion(fuente: str, seq: int) -> str:
    """UUID v5 determinista: el mismo evento reenviado conserva su identificador."""
    return str(uuid.uuid5(NAMESPACE_EVENTOS, f"{fuente}:{seq}"))
