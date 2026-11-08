"""Carga del modelo XGBoost y puntuación (RF14).

- Se usa `xgboost.Booster`, sin scikit-learn.
- El one-hot de canal y monedas se reconstruye con `model_input_columns` de model_meta.json
  (no se carga ohe.joblib, que es un pickle creado con NumPy 2). Una categoría desconocida
  queda en ceros, igual que OneHotEncoder(handle_unknown="ignore") en el entrenamiento.
- Se puntúa con los árboles hasta `best_iteration`, como hizo `predict_proba` en el notebook.
"""
from __future__ import annotations

import json
import pathlib

import numpy as np
import pandas as pd
import xgboost as xgb


class Puntuador:
    def __init__(self, dir_modelo: str | pathlib.Path):
        d = pathlib.Path(dir_modelo)
        self.meta = json.loads((d / "model_meta.json").read_text())
        self.booster = xgb.Booster()
        self.booster.load_model(str(d / "aml_xgb.json"))
        self.columnas = self.meta["model_input_columns"]
        self.num = self.meta["num_features"]
        self.cat = self.meta["cat_features"]
        self.one_hot = {c: [col[len(c) + 1:] for col in self.columnas if col.startswith(c + "_")] for c in self.cat}
        self.iteraciones = (0, int(self.meta["best_iteration"]) + 1)
        self.umbral = float(self.meta["threshold"])
        self.version = f"aml_xgb_v1_it{self.meta['best_iteration']}"
        if xgb.__version__ != self.meta["versions"]["xgboost"]:
            raise RuntimeError(f"xgboost {xgb.__version__} instalado; el modelo se entrenó con "
                               f"{self.meta['versions']['xgboost']}. Fije la versión en la imagen.")

    def matriz(self, variables: pd.DataFrame, categoricas: pd.DataFrame) -> pd.DataFrame:
        X = variables[self.num].copy()
        for cat, valores in self.one_hot.items():
            for v in valores:
                X[f"{cat}_{v}"] = (categoricas[cat].to_numpy() == v).astype(np.float32)
        return X[self.columnas]

    def puntuar(self, variables: pd.DataFrame, categoricas: pd.DataFrame) -> np.ndarray:
        if len(variables) == 0:
            return np.empty(0, dtype=np.float32)
        X = self.matriz(variables, categoricas)
        return self.booster.predict(xgb.DMatrix(X), iteration_range=self.iteraciones)

    def explicar(self, variables: pd.DataFrame, categoricas: pd.DataFrame) -> np.ndarray:
        """Aporte de cada columna al score en log-odds (la última columna es el sesgo)."""
        X = self.matriz(variables, categoricas)
        return self.booster.predict(xgb.DMatrix(X), pred_contribs=True, iteration_range=self.iteraciones)
