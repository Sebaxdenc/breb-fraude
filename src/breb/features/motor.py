"""Motor de variables del Job 2: replica fila por fila las 58 variables numéricas del notebook
de entrenamiento (modelo/entrenamiento/aml_xgb_breb_v1_codigo.py) a partir de un estado
acumulado por cuenta, por par ordenante→destino y por banco.

El cálculo tiene dos pasos, igual que el notebook:
1. `MotorVariables.procesar`: recorre las transacciones en orden (seq) y produce los
   acumulados "crudos" de cada fila leyendo y actualizando el estado (conteos, sumas,
   últimas marcas de tiempo, contrapartes distintas). Las sumas usan la misma suma
   compensada (Kahan) que `groupby().cumsum()` de pandas, para que den idénticas.
2. `derivar`: calcula las variables finales con las mismas expresiones de pandas del
   notebook, incluidos los pasos intermedios en float32, y al final lo convierte todo a
   float32 como hace el notebook antes de entrenar.

Particularidades del notebook que se replican tal cual (no son errores de este motor):
- `is_reciprocal_loop` vale 1 cuando el MISMO par ordenante→destino ya ocurrió antes; el
  notebook agrega al conjunto la clave invertida y luego busca la clave invertida.
- `net_flow`, `flow_ratio` y `tx_count_ratio` combinan el acumulado del ordenante con el
  del destino (dos cuentas distintas).
- `sender_inflow_gap_sec` solo considera entradas de fondos de un minuto anterior
  (merge_asof con allow_exact_matches=False); entre varias entradas de ese minuto toma la última.

El notebook corrió sobre unos 5 millones de filas. Con más de 1.000.000 de elementos, pandas evalúa la
aritmética con numexpr (`pandas.core.computation.expressions._MIN_ELEMENTS`), que se comporta distinto a
NumPy en dos puntos. El Job 2 procesa lotes chicos, donde pandas usa NumPy, así que se replican a mano:
- float32 combinado con un escalar de Python (`s + 1e-5`, `1.0 - s`) da float64, no float32.
- `x ** 2` se calcula con pow() de la libm, que en cerca del 0,08 % de los montos no da igual a x * x.

Hay una diferencia que depende de la máquina y no del código: NumPy elige al importar la implementación
de `log1p` en float32 según la CPU. Con AVX-512 usa su versión vectorizada; sin AVX-512, la de la libm.
Entre las dos hay hasta 2 ULP en cerca del 3 % de las filas de las variables de `VARIABLES_LOG1P_F32`.
La referencia sintética de las pruebas se generó en una CPU sin AVX-512; en una con AVX-512 el score
sigue igual (diferencia < 1e-6, mismas decisiones), así que las pruebas toleran esos 2 ULP solo ahí.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

SIN_TIEMPO_FROM_TO = 86400.0   # fillna del notebook para seconds_since_last_tx_from/_to
SIN_TIEMPO_CADENA = 999999.0   # fillna para chain_turnaround_sec y sender_inflow_gap_sec
# Variables que pasan por np.log1p sobre float32 (su último bit depende de la CPU, ver arriba).
VARIABLES_LOG1P_F32 = ("pass_through_velocity", "passthrough_velocity_true")


def cuenta_vacia() -> dict:
    return {
        # como ordenante
        "out_n": 0, "out_acc": 0.0, "out_comp": 0.0, "out2_acc": 0.0, "out2_comp": 0.0,
        "out_last_ts": None, "out_dist": 0,
        # como destino
        "in_n": 0, "in_acc": 0.0, "in_comp": 0.0, "in_last_ts": None, "in_last_paid": None,
        "in_last_recv": None, "in_prev_ts": None, "in_prev_recv": None, "in_dist": 0,
    }


def par_vacio() -> dict:
    return {"n": 0, "acc": 0.0, "comp": 0.0}


def banco_vacio() -> dict:
    return {"out_n": 0, "in_n": 0}


def clave_par(origen: str, destino: str) -> str:
    return f"{origen}|{destino}"


def _f64(s: pd.Series) -> pd.Series:
    """Promoción de numexpr: float32 operado con un escalar se calcula en float64."""
    return s.astype(np.float64)


def _cuadrado(s: pd.Series) -> pd.Series:
    """`s ** 2` como lo calcula numexpr: pow() de la libm, elemento por elemento."""
    v = s.to_numpy(dtype=np.float64)
    return pd.Series(np.fromiter((math.pow(x, 2.0) for x in v), dtype=np.float64, count=len(v)), index=s.index)


def _kahan(estado: dict, acc: str, comp: str, valor: float) -> float:
    """Suma compensada idéntica a pandas group_cumsum; devuelve el acumulado incluyendo `valor`."""
    a, c = estado[acc], estado[comp]
    y = valor - c
    t = a + y
    estado[comp] = t - a - y
    estado[acc] = t
    return t


CRUDAS = ["seconds_since_last_tx_from", "seconds_since_last_tx_to", "tx_count_from", "tx_count_to",
          "amt_sum_from", "amt_sum_to", "from_bank_n", "to_bank_n", "is_first_sender_pair",
          "dist_recip_prev", "sender_prior_count", "sumsq_cum", "recipient_prev_tx_count",
          "prev_received_ts", "prev_received_amt", "pair_n_prev", "pair_cum_paid",
          "dist_from_prev", "matched_ts", "matched_amt"]


class MotorVariables:
    """Recorre transacciones en orden y mantiene el estado en tres diccionarios.

    `cuentas`, `pares` y `bancos` pueden venir de MongoDB (Job 2) o empezar vacíos (estado
    inicial). Las claves que no existan se crean vacías al usarse.
    """

    def __init__(self, cuentas: dict | None = None, pares: dict | None = None, bancos: dict | None = None):
        self.cuentas = cuentas if cuentas is not None else {}
        self.pares = pares if pares is not None else {}
        self.bancos = bancos if bancos is not None else {}
        self.tocadas = (set(), set(), set())

    def _get(self, tabla: dict, clave, vacio, idx: int):
        e = tabla.get(clave)
        if e is None:
            e = vacio()
            tabla[clave] = e
        self.tocadas[idx].add(clave)
        return e

    def procesar(self, df: pd.DataFrame) -> pd.DataFrame:
        """`df` debe venir ordenado por seq. Columnas: ts (segundos epoch), from_node_key,
        to_node_key, from_bank, to_bank, amount_paid, amount_received."""
        n = len(df)
        out = {k: np.empty(n, dtype=np.float64) for k in CRUDAS}
        ts_arr = df["ts"].to_numpy(dtype=np.int64)
        f_arr = df["from_node_key"].to_numpy()
        t_arr = df["to_node_key"].to_numpy()
        bf_arr = df["from_bank"].to_numpy()
        bt_arr = df["to_bank"].to_numpy()
        paid_arr = df["amount_paid"].to_numpy(dtype=np.float64)
        recv_arr = df["amount_received"].to_numpy(dtype=np.float64)
        nan = np.nan
        for i in range(n):
            ts = int(ts_arr[i])
            paid = float(paid_arr[i])
            recv = float(recv_arr[i])
            F = self._get(self.cuentas, f_arr[i], cuenta_vacia, 0)
            T = self._get(self.cuentas, t_arr[i], cuenta_vacia, 0)
            P = self._get(self.pares, clave_par(f_arr[i], t_arr[i]), par_vacio, 1)
            BF = self._get(self.bancos, str(bf_arr[i]), banco_vacio, 2)
            BT = self._get(self.bancos, str(bt_arr[i]), banco_vacio, 2)

            # ---- lecturas (todo con el estado previo a esta fila)
            primera = 1 if P["n"] == 0 else 0
            out["seconds_since_last_tx_from"][i] = (ts - F["out_last_ts"]) if F["out_last_ts"] is not None else SIN_TIEMPO_FROM_TO
            out["seconds_since_last_tx_to"][i] = (ts - T["in_last_ts"]) if T["in_last_ts"] is not None else SIN_TIEMPO_FROM_TO
            out["tx_count_from"][i] = F["out_n"] + 1
            out["tx_count_to"][i] = T["in_n"] + 1
            out["sender_prior_count"][i] = F["out_n"]
            out["recipient_prev_tx_count"][i] = T["in_n"]
            out["from_bank_n"][i] = BF["out_n"] + 1
            out["to_bank_n"][i] = BT["in_n"] + 1
            out["is_first_sender_pair"][i] = primera
            out["dist_recip_prev"][i] = T["in_dist"]
            out["dist_from_prev"][i] = F["out_dist"]
            out["pair_n_prev"][i] = P["n"]
            out["prev_received_ts"][i] = T["in_last_ts"] if T["in_last_ts"] is not None else nan
            out["prev_received_amt"][i] = T["in_last_paid"] if T["in_last_paid"] is not None else nan
            # última entrada de fondos al ORDENANTE en un minuto anterior (merge_asof estricto)
            if F["in_last_ts"] is not None and F["in_last_ts"] < ts:
                out["matched_ts"][i], out["matched_amt"][i] = F["in_last_ts"], F["in_last_recv"]
            elif F["in_last_ts"] == ts and F["in_prev_ts"] is not None:
                out["matched_ts"][i], out["matched_amt"][i] = F["in_prev_ts"], F["in_prev_recv"]
            else:
                out["matched_ts"][i] = out["matched_amt"][i] = nan

            # ---- actualizaciones
            out["amt_sum_from"][i] = _kahan(F, "out_acc", "out_comp", paid)
            out["sumsq_cum"][i] = _kahan(F, "out2_acc", "out2_comp", math.pow(paid, 2.0))
            F["out_n"] += 1
            F["out_last_ts"] = ts
            F["out_dist"] += primera

            out["amt_sum_to"][i] = _kahan(T, "in_acc", "in_comp", recv)
            if T["in_last_ts"] != ts:
                T["in_prev_ts"], T["in_prev_recv"] = T["in_last_ts"], T["in_last_recv"]
            T["in_last_ts"], T["in_last_recv"], T["in_last_paid"] = ts, recv, paid
            T["in_n"] += 1
            T["in_dist"] += primera

            out["pair_cum_paid"][i] = _kahan(P, "acc", "comp", paid)
            P["n"] += 1
            BF["out_n"] += 1
            BT["in_n"] += 1
        crudas = pd.DataFrame(out, index=df.index)
        return crudas


def derivar(df: pd.DataFrame, crudas: pd.DataFrame, num_features: list[str]) -> pd.DataFrame:
    """Variables finales con las mismas expresiones (y tipos) del notebook."""
    d = pd.DataFrame(index=df.index)
    paid = df["amount_paid"].astype(np.float64)
    recv = df["amount_received"].astype(np.float64)
    ts_dt = df["timestamp"]
    ts_sec = df["ts"].astype(np.int64)
    c = crudas

    d["log_amount_paid"] = np.log1p(paid.clip(lower=0))
    d["log_amount_received"] = np.log1p(recv.clip(lower=0))
    d["amount_difference"] = (paid - recv).abs()
    d["log_amt_diff"] = np.log1p(d["amount_difference"])
    d["same_bank"] = (df["from_bank"] == df["to_bank"]).astype(int)
    d["same_currency"] = (df["receiving_currency"] == df["payment_currency"]).astype(int)
    d["hour"] = ts_dt.dt.hour
    d["day_of_week"] = ts_dt.dt.dayofweek
    d["hour_sin"] = np.sin(2 * np.pi * d["hour"] / 24.0)
    d["hour_cos"] = np.cos(2 * np.pi * d["hour"] / 24.0)
    d["day_sin"] = np.sin(2 * np.pi * d["day_of_week"] / 7.0)
    d["day_cos"] = np.cos(2 * np.pi * d["day_of_week"] / 7.0)
    d["seconds_since_last_tx_from"] = c["seconds_since_last_tx_from"]
    d["seconds_since_last_tx_to"] = c["seconds_since_last_tx_to"]
    d["tx_count_from"] = c["tx_count_from"]
    d["tx_count_to"] = c["tx_count_to"]
    d["amt_sum_from"] = c["amt_sum_from"]
    d["amt_sum_to"] = c["amt_sum_to"]
    d["net_flow"] = d["amt_sum_to"] - d["amt_sum_from"]
    d["tx_count_ratio"] = (d["tx_count_from"] + 1) / (d["tx_count_to"] + 1)
    d["flow_ratio"] = (d["amt_sum_from"] + 1.0) / (d["amt_sum_to"] + 1.0)
    d["amt_to_avg_ratio_from"] = paid / ((d["amt_sum_from"] / d["tx_count_from"]) + 1e-5)
    d["is_just_under_10k"] = ((paid >= 9000) & (paid < 10000)).astype(int)
    d["is_round_1k"] = (paid % 1000 == 0).astype(int)
    d["amount_mismatch"] = (paid != recv).astype(np.float32)
    d["is_cross_currency"] = (df["payment_currency"] != df["receiving_currency"]).astype(np.float32)
    d["from_bank_freq"] = c["from_bank_n"].astype(np.float32)
    d["to_bank_freq"] = c["to_bank_n"].astype(np.float32)
    d["is_first_sender_pair"] = c["is_first_sender_pair"].astype(int)
    d["distinct_users_on_recipient"] = (c["dist_recip_prev"] + c["is_first_sender_pair"]).astype(np.float32)

    prior_count = c["sender_prior_count"].astype(np.int64)
    prior_sum = d["amt_sum_from"] - paid
    prior_sumsq = c["sumsq_cum"] - _cuadrado(paid)
    d["sender_avg_amt"] = (prior_sum / prior_count.replace(0, np.nan)).fillna(0.0).astype(np.float32)
    var = prior_sumsq / prior_count.replace(0, np.nan) - _cuadrado(d["sender_avg_amt"].astype(np.float64))
    d["sender_std_amt"] = np.sqrt(var.clip(lower=0)).fillna(0.0).astype(np.float32)
    d["z_score_amount_sender"] = ((paid - d["sender_avg_amt"]) / (_f64(d["sender_std_amt"]) + 1e-5)).astype(np.float32)
    d["is_just_under_5k"] = ((paid >= 4500) & (paid < 5000)).astype(np.float32)
    d["is_round_100"] = (paid % 100 == 0).astype(np.float32)
    d["amount_cents_fraction"] = (paid % 1).astype(np.float32)
    d["is_night_tx"] = ((d["hour"] >= 0) & (d["hour"] <= 5)).astype(np.float32)
    d["is_weekend"] = (d["day_of_week"] >= 5).astype(np.float32)

    prev_count = c["recipient_prev_tx_count"].astype(np.int64)
    d["recipient_prev_tx_count"] = prev_count
    d["recipient_prev_received"] = d["amt_sum_to"] - recv
    d["recipient_hist_mean"] = (d["recipient_prev_received"] / prev_count.replace(0, np.nan)).fillna(0)
    d["recipient_amount_ratio"] = recv / (d["recipient_hist_mean"] + 1)
    d["recipient_unique_senders"] = c["dist_recip_prev"].astype(np.int64)

    mule = np.abs((d["amt_sum_from"] - d["amt_sum_to"]) / (d["amt_sum_from"] + d["amt_sum_to"] + 1e-5)).astype(np.float32)
    d["chain_turnaround_sec"] = (ts_sec - c["prev_received_ts"]).fillna(SIN_TIEMPO_CADENA).astype(np.float32)
    d["chain_amount_preservation_ratio"] = (paid / (c["prev_received_amt"] + 1e-5)).fillna(0.0).astype(np.float32)
    d["is_rapid_forwarding"] = (d["chain_turnaround_sec"] < 3600).astype(np.float32)
    d["pass_through_velocity"] = ((1.0 - _f64(mule)) / (_f64(np.log1p(d["chain_turnaround_sec"])) + 1.0)).astype(np.float32)
    d["counterparty_revisit_count"] = c["pair_n_prev"].astype(np.float32)
    d["is_reciprocal_loop"] = (c["pair_n_prev"] > 0).astype(np.float32)

    d["distinct_recipients_from"] = (c["dist_from_prev"] + c["is_first_sender_pair"]).astype(np.float32)
    d["recipient_churn_from"] = (d["distinct_recipients_from"] / (d["tx_count_from"] + 1.0)).astype(np.float32)
    has = c["matched_ts"].notna().to_numpy()
    ts_np = ts_sec.to_numpy()
    gap = np.where(has, ts_np - np.nan_to_num(c["matched_ts"].to_numpy()), SIN_TIEMPO_CADENA)
    d["sender_inflow_gap_sec"] = gap.astype(np.float32)
    d["sender_has_prior_inflow"] = has.astype(np.float32)
    ratio = np.where(has, paid.to_numpy() / (np.nan_to_num(c["matched_amt"].to_numpy()) + 1e-5), 0.0)
    d["passthrough_amount_ratio"] = ratio.astype(np.float32)
    d["passthrough_velocity_true"] = (d["passthrough_amount_ratio"].clip(upper=2.0)
                                      / (_f64(np.log1p(d["sender_inflow_gap_sec"])) + 1.0)).astype(np.float32)
    pair_rank = _f64(d["counterparty_revisit_count"]) + 1.0
    d["pair_amt_share_from"] = (c["pair_cum_paid"] / (d["amt_sum_from"] + 1e-5)).astype(np.float32)
    d["pair_tx_share_from"] = (pair_rank / (d["tx_count_from"] + 1e-5)).astype(np.float32)
    d["pair_tx_share_to"] = (pair_rank / (d["tx_count_to"] + 1e-5)).astype(np.float32)

    # el notebook convierte todo float64 a float32 antes de entrenar
    for col in d.columns:
        if d[col].dtype == np.float64:
            d[col] = d[col].astype(np.float32)
    faltan = [f for f in num_features if f not in d.columns]
    assert not faltan, f"variables sin calcular: {faltan}"
    return d[num_features]


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega la columna ts (segundos epoch) con la misma fórmula del notebook."""
    df = df.copy()
    df["ts"] = (df["timestamp"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")
    return df
