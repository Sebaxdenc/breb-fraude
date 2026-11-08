"""Dataset sintético con el mismo esquema que IBM AML LI-Small_Trans.csv.

No intenta parecerse en distribución al dataset real: sirve para probar que el
motor de variables del Job 2 calcula exactamente lo mismo que el notebook de
entrenamiento. Por eso fuerza los casos difíciles: varias transacciones de la
misma cuenta en el mismo minuto, pares repetidos y recíprocos, autotransferencias
(Reinvestment), cambio de moneda, cuentas con el mismo número en bancos distintos,
filas duplicadas, CSV parcialmente desordenado y una cola posterior al rango primario.

Es determinista: la misma semilla produce el mismo archivo con cualquier versión de NumPy
(usa numpy.random.default_rng / PCG64).

Uso: python generar_dataset_sintetico.py SALIDA_DIR [--filas 180000] [--semilla 7]
"""
import argparse
import csv
import pathlib
from datetime import datetime, timedelta

import numpy as np

MONEDAS = ["US Dollar", "Euro", "Yuan", "Rupee", "UK Pound", "Yen", "Canadian Dollar", "Australian Dollar",
           "Swiss Franc", "Mexican Peso", "Brazil Real", "Ruble", "Saudi Riyal", "Shekel", "Bitcoin"]
TASA = {"US Dollar": 1.0, "Euro": 0.85, "Yuan": 6.7, "Rupee": 77.0, "UK Pound": 0.73, "Yen": 105.0,
        "Canadian Dollar": 1.3, "Australian Dollar": 1.4, "Swiss Franc": 0.92, "Mexican Peso": 20.0,
        "Brazil Real": 5.3, "Ruble": 75.0, "Saudi Riyal": 3.75, "Shekel": 3.3, "Bitcoin": 0.00009}
FORMATOS = ["Cheque", "Credit Card", "ACH", "Cash", "Wire", "Bitcoin", "Reinvestment"]
P_FORMATOS = [0.36, 0.25, 0.12, 0.10, 0.04, 0.03, 0.10]
INICIO = datetime(2022, 9, 1)
FIN_PRIMARIO = datetime(2022, 9, 11)
ENCABEZADO = ["Timestamp", "From Bank", "Account", "To Bank", "Account", "Amount Received",
              "Receiving Currency", "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]


def generar(salida: pathlib.Path, filas: int = 180_000, semilla: int = 7) -> pathlib.Path:
    rng = np.random.default_rng(semilla)
    bancos = rng.choice(np.arange(1, 4000), size=40, replace=False)
    n_cuentas = max(filas // 30, 500)
    cuentas = []
    for i in range(n_cuentas):
        banco = int(rng.choice(bancos))
        numero = f"{int(rng.integers(0, 16 ** 9)):09X}"
        cuentas.append((banco, numero))
    # mismo número de cuenta en otro banco: el nodo es banco + cuenta
    for i in range(min(60, n_cuentas // 10)):
        banco, numero = cuentas[i]
        otro = int(rng.choice([b for b in bancos if b != banco]))
        cuentas.append((otro, numero))
    n_cuentas = len(cuentas)
    moneda_cuenta = rng.choice(MONEDAS, size=n_cuentas, p=[0.5] + [0.5 / 14] * 14)
    actividad = rng.pareto(1.5, n_cuentas) + 0.2
    actividad /= actividad.sum()
    favoritos = [rng.choice(n_cuentas, size=int(rng.integers(1, 6)), replace=False) for _ in range(n_cuentas)]

    minutos_total = int((FIN_PRIMARIO - INICIO).total_seconds() // 60)
    hora_peso = np.array([0.3, 0.2, 0.2, 0.2, 0.3, 0.5, 0.8, 1.0, 1.2, 1.3, 1.3, 1.3,
                          1.2, 1.2, 1.3, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.6, 0.5, 0.4])
    filas_out = []

    def minuto_aleatorio(n):
        dia = rng.integers(0, 10, n)
        hora = rng.choice(24, size=n, p=hora_peso / hora_peso.sum())
        minuto = rng.integers(0, 60, n)
        m = dia * 1440 + hora * 60 + minuto
        # parte del tráfico cae en minutos "redondos" (00:00, cada hora): muchos empates
        redondo = rng.random(n) < 0.08
        m[redondo] = (m[redondo] // 60) * 60
        return np.minimum(m, minutos_total - 1)

    def monto():
        r = rng.random()
        if r < 0.05:
            return float(rng.integers(1, 30) * 1000)
        if r < 0.07:
            return round(float(rng.uniform(9000, 9999)), 2)
        if r < 0.09:
            return round(float(rng.uniform(4500, 4999)), 2)
        if r < 0.12:
            return float(rng.integers(1, 90) * 100)
        return round(float(rng.lognormal(6.5, 1.8)), 2)

    def agregar(minuto, o, d, formato, lavado, m=None):
        bo, co = cuentas[o]
        bd, cd = cuentas[d]
        mp = monto() if m is None else m
        cp = str(moneda_cuenta[o])
        if formato == "Reinvestment":
            bd, cd, cr, mr = bo, co, cp, mp
        elif formato == "Bitcoin":
            cp = cr = "Bitcoin"
            mp = round(mp * TASA["Bitcoin"], 6)
            mr = mp
        elif rng.random() < 0.04:
            cr = str(rng.choice(MONEDAS[:14]))
            mr = round(mp / TASA[cp] * TASA[cr], 2)
        else:
            cr, mr = cp, mp
        filas_out.append((int(minuto), bo, co, bd, cd, mr, cr, mp, cp, formato, int(lavado)))

    # 1) tráfico normal
    n_normal = int(filas * 0.97)
    origenes = rng.choice(n_cuentas, size=n_normal, p=actividad)
    minutos = minuto_aleatorio(n_normal)
    formatos = rng.choice(FORMATOS, size=n_normal, p=P_FORMATOS)
    for o, minuto, formato in zip(origenes, minutos, formatos):
        r = rng.random()
        if r < 0.65:
            d = int(rng.choice(favoritos[o]))
        elif r < 0.72 and favoritos[o].size:
            d = int(favoritos[o][0])                      # contraparte principal
        else:
            d = int(rng.choice(n_cuentas, p=actividad))
        agregar(minuto, int(o), d, str(formato), 0)
        if rng.random() < 0.03:                           # respuesta del destino: par recíproco
            agregar(minuto + int(rng.integers(0, 90)), d, int(o), "ACH", 0)

    # 2) ráfagas: misma cuenta, mismo minuto, y varias entradas a una cuenta en el mismo minuto
    for _ in range(filas // 400):
        o = int(rng.choice(n_cuentas, p=actividad))
        minuto = int(minuto_aleatorio(1)[0])
        for _ in range(int(rng.integers(2, 6))):
            agregar(minuto, o, int(rng.choice(n_cuentas)), str(rng.choice(FORMATOS[:5])), 0)
        d = int(rng.choice(n_cuentas))
        for _ in range(int(rng.integers(2, 5))):
            agregar(minuto, int(rng.choice(n_cuentas)), d, "Cheque", 0)

    # 3) lavado: fan-out, fan-in, gather-scatter y casos aislados
    n_lavado_obj = int(filas * 0.004)
    contador = 0
    while contador < n_lavado_obj:
        tipo = rng.random()
        minuto = int(minuto_aleatorio(1)[0])
        mula = int(rng.integers(0, n_cuentas))
        if tipo < 0.3:            # fan-out
            for k in range(int(rng.integers(4, 10))):
                agregar(minuto + k * int(rng.integers(0, 40)), mula, int(rng.integers(0, n_cuentas)), "ACH", 1,
                        round(float(rng.uniform(1000, 9500)), 2))
                contador += 1
        elif tipo < 0.55:         # fan-in
            for k in range(int(rng.integers(4, 10))):
                agregar(minuto + k * int(rng.integers(0, 40)), int(rng.integers(0, n_cuentas)), mula, "ACH", 1,
                        round(float(rng.uniform(1000, 9500)), 2))
                contador += 1
        elif tipo < 0.7:          # gather-scatter: entran y salen rápido
            total = 0.0
            for k in range(int(rng.integers(3, 7))):
                m = round(float(rng.uniform(2000, 9000)), 2)
                total += m
                agregar(minuto + k, int(rng.integers(0, n_cuentas)), mula, "ACH", 1, m)
                contador += 1
            for k in range(int(rng.integers(2, 5))):
                agregar(minuto + 20 + k, mula, int(rng.integers(0, n_cuentas)), "Wire", 1,
                        round(total / 4, 2))
                contador += 1
        else:                     # aislado (sin patrón)
            agregar(minuto, int(rng.integers(0, n_cuentas)), int(rng.integers(0, n_cuentas)),
                    str(rng.choice(FORMATOS[:5])), 1)
            contador += 1

    # 4) duplicados exactos y cola posterior al rango primario (60 % lavado)
    for i in rng.choice(len(filas_out), size=25, replace=False):
        filas_out.append(filas_out[int(i)])
    for _ in range(300):
        o, d = rng.integers(0, n_cuentas, 2)
        agregar(minutos_total + int(rng.integers(0, 2 * 1440)), int(o), int(d), "ACH", int(rng.random() < 0.6))

    # orden del CSV: casi cronológico, con desorden local (la estabilidad del sort importa)
    m = np.array([f[0] for f in filas_out])
    clave = m + rng.integers(-3, 4, len(m))
    orden = np.argsort(clave, kind="stable")

    salida.mkdir(parents=True, exist_ok=True)
    ruta = salida / "LI-Small_Trans.csv"
    with open(ruta, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(ENCABEZADO)
        for i in orden:
            minuto, bo, co, bd, cd, mr, cr, mp, cp, formato, lav = filas_out[int(i)]
            ts = (INICIO + timedelta(minutes=minuto)).strftime("%Y/%m/%d %H:%M")
            w.writerow([ts, bo, co, bd, cd, mr, cr, mp, cp, formato, lav])
    (salida / "LI-Small_accounts.csv").write_text("Bank Name,Bank ID,Account Number,Entity ID,Entity Name\n")
    (salida / "LI-Small_Patterns.txt").write_text("")
    return ruta


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("salida")
    ap.add_argument("--filas", type=int, default=180_000)
    ap.add_argument("--semilla", type=int, default=7)
    a = ap.parse_args()
    ruta = generar(pathlib.Path(a.salida), a.filas, a.semilla)
    print("ok", ruta)
