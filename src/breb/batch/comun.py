"""Conexiones comunes de los procesos batch."""
from __future__ import annotations

import os


def base_mongo():
    """Base `breb` de MongoDB con w:majority, o None si no hay MONGO_MIEMBROS (pruebas sin MongoDB)."""
    if not os.environ.get("MONGO_MIEMBROS"):
        return None
    from pymongo import MongoClient
    from pymongo.write_concern import WriteConcern
    cliente = MongoClient(f"mongodb://{os.environ['MONGO_MIEMBROS']}/?replicaSet=rs0&authSource=admin",
                          username=os.environ["MONGO_APP_USUARIO"], password=os.environ["MONGO_APP_CLAVE"],
                          serverSelectionTimeoutMS=20000)
    return cliente.get_database("breb", write_concern=WriteConcern(w="majority"))
