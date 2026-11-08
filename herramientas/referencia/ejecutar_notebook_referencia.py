"""Ejecuta el código EXACTO del notebook de entrenamiento (versión 1 en Kaggle) sobre un
dataset con el esquema de LI-Small y guarda la referencia para la prueba de paridad.

Debe correr en un entorno igual al de Kaggle (Python 3.12, numpy 2.0.2, pandas 2.3.3,
numexpr 2.14.2, scikit-learn 1.6.1, xgboost 3.2.0). Solo cambia rutas y reduce el número de
árboles para que termine rápido: el cálculo de variables queda intacto.

En Kaggle el notebook procesa unos 5 millones de filas, y con más de 1.000.000 de elementos pandas
calcula la aritmética con numexpr, que redondea distinto a NumPy. Para que un dataset chico produzca
los mismos números que Kaggle, este script baja ese umbral a 0.

Además de lo que exporta el notebook (modelo, metadatos, muestra de referencia), guarda
todas las filas del test con sus variables y su score, para comparar fila por fila.

Uso: python ejecutar_notebook_referencia.py DIR_DATASET DIR_SALIDA
"""
import os
import pathlib
import sys

import matplotlib

matplotlib.use("Agg")

RAIZ = pathlib.Path(__file__).resolve().parents[2]
CODIGO = RAIZ / "modelo" / "entrenamiento" / "aml_xgb_breb_v1_codigo.py"


def main(dir_dataset: str, dir_salida: str) -> None:
    entrada = pathlib.Path(dir_dataset).resolve()
    salida = pathlib.Path(dir_salida).resolve()
    salida.mkdir(parents=True, exist_ok=True)
    src = CODIGO.read_text()
    reemplazos = {
        '"/kaggle/input/**/LI-Small_Trans.csv"': f'"{entrada}/**/LI-Small_Trans.csv"',
        '"/kaggle/working"': f'"{salida}"',
        "n_estimators=2000": "n_estimators=60",
        "early_stopping_rounds=200": "early_stopping_rounds=20",
    }
    for viejo, nuevo in reemplazos.items():
        assert src.count(viejo) == 1, f"no encuentro {viejo!r} exactamente una vez"
        src = src.replace(viejo, nuevo)
    src = src.replace("plt.show()", "plt.close('all')")

    import numexpr  # noqa: F401,E402  (si falta, pandas usaría NumPy y la referencia no sería la de Kaggle)
    from pandas.core.computation import expressions  # noqa: E402
    assert expressions.USE_NUMEXPR, "pandas no está usando numexpr (compute.use_numexpr)"
    expressions._MIN_ELEMENTS = 0
    g = {"__name__": "__main__"}
    exec(compile(src, str(CODIGO), "exec"), g)

    import numpy as np  # noqa: E402
    test_df, num, cat = g["test_df"], g["num_features"], g["cat_features"]
    cols = ["timestamp", "from_bank", "from_account", "to_bank", "to_account", "amount_received",
            "receiving_currency", "amount_paid", "payment_currency", "payment_format", "is_laundering",
            "from_node_key", "to_node_key"] + num
    ref = test_df[cols].copy()
    ref["score"] = np.asarray(g["test_preds_proba"], dtype=np.float64)
    ref.to_parquet(salida / "test_completo_referencia.parquet", index=False)
    print(f"[referencia] filas de test: {len(ref):,} | lavado: {int(ref.is_laundering.sum())}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
