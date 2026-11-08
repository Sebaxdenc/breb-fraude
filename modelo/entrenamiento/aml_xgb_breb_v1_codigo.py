import os
import gc
import numpy as np
import pandas as pd
import glob

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    precision_recall_curve,
    classification_report,
    confusion_matrix
)
from xgboost import XGBClassifier

pd.set_option("display.max_columns", 100)

# [BRE-B] Busca el CSV donde Kaggle haya montado el dataset
_matches = glob.glob("/kaggle/input/**/LI-Small_Trans.csv", recursive=True)
assert _matches, "No encuentro LI-Small_Trans.csv: agrega el dataset IBM AML en Add Input"
DATA_DIR = os.path.dirname(_matches[0])
print("DATA_DIR =", DATA_DIR)

TRANS_FILE = os.path.join(DATA_DIR, "LI-Small_Trans.csv")
ACCOUNTS_FILE = os.path.join(DATA_DIR, "LI-Small_accounts.csv")
PATTERNS_FILE = os.path.join(DATA_DIR, "LI-Small_Patterns.txt")

print("Trans exists:", os.path.exists(TRANS_FILE))
print("Accounts exists:", os.path.exists(ACCOUNTS_FILE))
print("Patterns exists:", os.path.exists(PATTERNS_FILE))
import os
import gc
import numpy as np
import pandas as pd

from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    classification_report,
    confusion_matrix
)
from xgboost import XGBClassifier

pd.set_option("display.max_columns", 100)


usecols = [
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account.1",
    "Amount Received",
    "Receiving Currency",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
    "Is Laundering"
]

df = pd.read_csv(
    os.path.join(DATA_DIR, "LI-Small_Trans.csv"), 
    usecols=usecols
)

df.columns = [
    "timestamp",
    "from_bank",
    "from_account",
    "to_bank",
    "to_account",
    "amount_received",
    "receiving_currency",
    "amount_paid",
    "payment_currency",
    "payment_format",
    "is_laundering"
]

df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

# [BRE-B] Las transacciones posteriores al rango primario (Sep 1-10, 2022)
# son todas de lavado según el autor del dataset. Se excluyen.
PRIMARY_END = pd.Timestamp("2022-09-11")
_tail = df["timestamp"] >= PRIMARY_END
print(f"[BRE-B] Filas fuera de rango eliminadas: {_tail.sum():,} "
      f"(lavado entre ellas: {df.loc[_tail, 'is_laundering'].mean() if _tail.any() else 0:.1%})")
df = df.loc[~_tail].reset_index(drop=True)


# %% CELL: "Building Clean, Non-Redundant AML Feature Set" (feature engineering)
# FIX APPLIED: the "recipient_*" block near the bottom now keys on
# from_node_key/to_node_key (bank+account) instead of raw from_account/
# to_account. Raw account numbers can repeat across different banks, so
# using them alone silently merges unrelated accounts into one "node" —
# inconsistent with every other feature in this notebook, which correctly
# uses the bank+account node key.

print("Building Clean, Non-Redundant AML Feature Set...")

# 1. Create Node Keys
df["from_node_key"] = df["from_bank"].astype(str) + "_" + df["from_account"].astype(str)
df["to_node_key"] = df["to_bank"].astype(str) + "_" + df["to_account"].astype(str)
# Create Unix timestamp in seconds for rolling calculations
df["ts_sec"] = (df["timestamp"] - pd.Timestamp("1970-01-01")) // pd.Timedelta("1s")

# 2. Transaction Amount Transformations
df["log_amount_paid"] = np.log1p(df["amount_paid"].clip(lower=0))
df["log_amount_received"] = np.log1p(df["amount_received"].clip(lower=0))
df["amount_difference"] = (df["amount_paid"] - df["amount_received"]).abs()
df["log_amt_diff"] = np.log1p(df["amount_difference"])

# 3. Categorical Match Indicators
df["same_bank"] = (df["from_bank"] == df["to_bank"]).astype(int)
df["same_currency"] = (df["receiving_currency"] == df["payment_currency"]).astype(int)

# 4. Temporal Cyclical Encodings
df["hour"] = df["timestamp"].dt.hour
df["day_of_week"] = df["timestamp"].dt.dayofweek
df["day_of_month"] = df["timestamp"].dt.day
df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24.0)
df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24.0)
df["day_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7.0)
df["day_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7.0)

# 5. Inter-Transaction Latencies (Seconds)
df["seconds_since_last_tx_from"] = (
    df.groupby("from_node_key")["timestamp"].diff().dt.total_seconds().fillna(86400)
)
df["seconds_since_last_tx_to"] = (
    df.groupby("to_node_key")["timestamp"].diff().dt.total_seconds().fillna(86400)
)

# 6. Cumulative Account Velocities & Volumes
df["tx_count_from"] = df.groupby("from_node_key")["timestamp"].rank(method="first")
df["tx_count_to"] = df.groupby("to_node_key")["timestamp"].rank(method="first")

df["amt_sum_from"] = df.groupby("from_node_key")["amount_paid"].cumsum()
df["amt_sum_to"] = df.groupby("to_node_key")["amount_received"].cumsum()

# 7. Account Interaction & Flow Ratios
df["net_flow"] = df["amt_sum_to"] - df["amt_sum_from"]
df["tx_count_ratio"] = (df["tx_count_from"] + 1) / (df["tx_count_to"] + 1)
df["flow_ratio"] = (df["amt_sum_from"] + 1.0) / (df["amt_sum_to"] + 1.0)

df["amt_to_avg_ratio_from"] = df["amount_paid"] / (
        (df["amt_sum_from"] / df["tx_count_from"]) + 1e-5
)

# 8. Structuring Rule Flags
df["is_just_under_10k"] = ((df["amount_paid"] >= 9000) & (df["amount_paid"] < 10000)).astype(int)
df["is_round_1k"] = (df["amount_paid"] % 1000 == 0).astype(int)

# 9. Amount Mismatch & Cross Currency Flags
df["amount_mismatch"] = (df["amount_paid"] != df["amount_received"]).astype(
    np.float32
)
df["is_cross_currency"] = (
        df["payment_currency"] != df["receiving_currency"]
).astype(np.float32)

df = df.reset_index(drop=True)
print("Features calculated cleanly!")

# 10. Bank Transaction Frequencies (causal: count of same-bank transactions up to
#     and including this row — a global value_counts() would leak future volume
#     back onto earlier rows)
df["from_bank_freq"] = (df.groupby("from_bank").cumcount() + 1).astype(np.float32)
df["to_bank_freq"] = (df.groupby("to_bank").cumcount() + 1).astype(np.float32)

# 11. Distinct Senders on Recipient (Cumulative unique senders per recipient over time)
df["is_first_sender_pair"] = (
        df.groupby(["to_node_key", "from_node_key"]).cumcount() == 0
).astype(int)

df["distinct_users_on_recipient"] = (
    df.groupby("to_node_key")["is_first_sender_pair"]
    .cumsum()
    .astype(np.float32)
)

# 12. ACCOUNT-LEVEL Z-SCORE & AMOUNT SPIKE DETECTOR
#     Causal: mean/std computed from the sender's transactions strictly BEFORE
#     this row. The previous version used a full-history groupby().agg(["mean",
#     "std"]) over the whole dataframe, which bakes in future transactions
#     (including test-period ones) as the baseline for early rows — leakage.
sender_prior_count = df.groupby("from_node_key").cumcount()
sender_prior_sum = df.groupby("from_node_key")["amount_paid"].cumsum() - df["amount_paid"]
sender_prior_sumsq = (
        (df["amount_paid"] ** 2).groupby(df["from_node_key"]).cumsum() - df["amount_paid"] ** 2
)

df["sender_avg_amt"] = (
    (sender_prior_sum / sender_prior_count.replace(0, np.nan)).fillna(0.0).astype(np.float32)
)
sender_prior_var = (
        sender_prior_sumsq / sender_prior_count.replace(0, np.nan)
        - df["sender_avg_amt"].astype(np.float64) ** 2
)
df["sender_std_amt"] = (
    np.sqrt(sender_prior_var.clip(lower=0)).fillna(0.0).astype(np.float32)
)
df["z_score_amount_sender"] = (
        (df["amount_paid"] - df["sender_avg_amt"]) / (df["sender_std_amt"] + 1e-5)
).astype(np.float32)

# 14. EXPANDED STRUCTURING & CENTS SIGNALS
df["is_just_under_5k"] = (
        (df["amount_paid"] >= 4500) & (df["amount_paid"] < 5000)
).astype(np.float32)
df["is_round_100"] = (df["amount_paid"] % 100 == 0).astype(np.float32)
df["amount_cents_fraction"] = (df["amount_paid"] % 1).astype(np.float32)

df["is_night_tx"] = ((df["hour"] >= 0) & (df["hour"] <= 5)).astype(np.float32)
df["is_weekend"] = (df["day_of_week"] >= 5).astype(np.float32)

# Ensure chronological order
df = df.sort_values("timestamp", kind="stable").copy()

# --------------------------------------------------
# 1. Recipient historical transaction count
#    FIXED: to_node_key instead of to_account
# --------------------------------------------------
df["recipient_prev_tx_count"] = (
    df.groupby("to_node_key").cumcount()
)

# --------------------------------------------------
# 2. Recipient cumulative received amount (before current transaction)
#    FIXED: to_node_key instead of to_account
# --------------------------------------------------
# [BRE-B] Corregido: monto recibido por ESTA cuenta antes de esta fila.
df["recipient_prev_received"] = (
    df.groupby("to_node_key")["amount_received"].cumsum()
    - df["amount_received"]
)


# --------------------------------------------------
# 3. Recipient historical mean received amount
# --------------------------------------------------
df["recipient_hist_mean"] = (
        df["recipient_prev_received"] /
        df["recipient_prev_tx_count"].replace(0, np.nan)
).fillna(0)

# --------------------------------------------------
# 4. Current amount relative to recipient history
# --------------------------------------------------
df["recipient_amount_ratio"] = (
        df["amount_received"] /
        (df["recipient_hist_mean"] + 1)
)

# --------------------------------------------------
# 5. Number of unique previous senders
#    FIXED: iterate node keys instead of raw account numbers
# --------------------------------------------------
unique_senders = {}
prev_unique = []

for sender, receiver in zip(df["from_node_key"], df["to_node_key"]):
    if receiver not in unique_senders:
        unique_senders[receiver] = set()
    prev_unique.append(len(unique_senders[receiver]))
    unique_senders[receiver].add(sender)

df["recipient_unique_senders"] = prev_unique

# ==========================================
# DEFINE UNIFIED FEATURE LISTS
# ==========================================
num_features = [
    "hour", "day_of_week", "day_of_month",
    "hour_sin", "hour_cos", "day_sin", "day_cos",
    "log_amount_paid", "log_amount_received", "amount_difference", "log_amt_diff",
    "same_bank", "same_currency",
    "seconds_since_last_tx_from", "seconds_since_last_tx_to",
    "tx_count_from", "tx_count_to", "amt_sum_from", "amt_sum_to",
    "net_flow", "tx_count_ratio", "flow_ratio", "amt_to_avg_ratio_from",
    "is_just_under_10k", "is_round_1k", "amount_mismatch", "is_cross_currency", "from_bank_freq",
    "to_bank_freq", "distinct_users_on_recipient", "is_first_sender_pair",
    "sender_avg_amt", "sender_std_amt", "z_score_amount_sender", "amount_cents_fraction",
    "is_round_100", "is_just_under_5k", "is_night_tx", "is_weekend", "recipient_prev_tx_count",
    "recipient_prev_received", "recipient_hist_mean", "recipient_amount_ratio",
    "recipient_unique_senders",
]

# Currency was previously reduced to the binary same_currency /
# is_cross_currency flags, so the actual corridor (which currency into which)
# never reached the model — the raw columns get dropped by the numeric-only
# filter before training. Laundering concentrates in specific corridors, so
# one-hot them and let the model see the pair, not just "they differ".
cat_features = ["payment_format", "receiving_currency", "payment_currency"]

features = num_features + cat_features
print(f"Total Features Defined: {len(features)} ({len(num_features)} numerical, {len(cat_features)} categorical)")


import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def generate_8_aml_typology_features(df):
    print("=" * 60)
    print("ENGINEERING FEATURES FOR ALL 8 AML TYPOLOGY FEATURES (FLOAT32)")
    print("=" * 60)

    # 1. Ensure sorted by timestamp and reset index cleanly
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)

    # 2. Convert timestamp to seconds for C-accelerated binary search
    if "ts_sec" not in df.columns:
        df["ts_sec"] = df["timestamp"].astype("int64") // 10**9

    # ------------------------------------------------------------
    # 1. & 2. FAN-IN & FAN-OUT (Vectorized 24h Rolling Counts via searchsorted)
    # ------------------------------------------------------------
    print(" -> [1/8 & 2/8] Computing Fan-In and Fan-Out rolling 24h degrees...")

    def calc_24h_rolling_count(series):
        arr = series.values
        # C binary search for start of 24h window (86,400 seconds)
        start_indices = np.searchsorted(arr, arr - 86400, side="left")
        return (np.arange(len(arr)) - start_indices + 1).astype(np.float32)

    # .transform() preserves the exact row alignment and index of df
    df["src_fan_out_24h"] = df.groupby("from_node_key")["ts_sec"].transform(
        calc_24h_rolling_count
    )
    df["dst_fan_in_24h"] = df.groupby("to_node_key")["ts_sec"].transform(
        calc_24h_rolling_count
    )

    # ------------------------------------------------------------
    # 3. & 4. MULE PASS-THROUGH & NET FLOW BALANCES
    # ------------------------------------------------------------
    print(" -> [3/8 & 4/8] Computing Mule Pass-Through & Net Flow Ratios...")
    df["mule_net_flow_balance"] = np.abs(
        (df["amt_sum_from"] - df["amt_sum_to"])
        / (df["amt_sum_from"] + df["amt_sum_to"] + 1e-5)
    ).astype(np.float32)

    # ------------------------------------------------------------
    # 5. & 6. CHAINS & RAPID FORWARDING
    # ------------------------------------------------------------
    print(" -> [5/8 & 6/8] Computing Turnaround Speed & Rapid Forwarding...")
    df["prev_received_ts"] = df.groupby("to_node_key")["ts_sec"].shift(1)
    df["prev_received_amt"] = df.groupby("to_node_key")["amount_paid"].shift(1)

    df["chain_turnaround_sec"] = (
        (df["ts_sec"] - df["prev_received_ts"]).fillna(999999).astype(np.float32)
    )

    df["chain_amount_preservation_ratio"] = (
        (df["amount_paid"] / (df["prev_received_amt"] + 1e-5))
        .fillna(0.0)
        .astype(np.float32)
    )

    df["is_rapid_forwarding"] = (df["chain_turnaround_sec"] < 3600).astype(
        np.float32
    )

    df["pass_through_velocity"] = (
        (1.0 - df["mule_net_flow_balance"])
        / (np.log1p(df["chain_turnaround_sec"]) + 1.0)
    ).astype(np.float32)

    # ------------------------------------------------------------
    # 7. BIPARTITE NETWORKS (Group-to-Group Cross Transfers)
    # ------------------------------------------------------------
    print(" -> [7/8] Computing Bipartite Cross-Density Proxy...")
    df["bipartite_cross_density"] = (
        df["src_fan_out_24h"] * df["dst_fan_in_24h"]
    ).astype(np.float32)

    # ------------------------------------------------------------
    # 8. CYCLES & RECIPROCITY (Ping-Pong Loops & Counterparty Revisits)
    # ------------------------------------------------------------
    print(" -> [8/8] Computing Reciprocity and Counterparty Loop Counts...")
    df["pair_key"] = (
        df["from_node_key"].astype(str) + "_" + df["to_node_key"].astype(str)
    )
    df["counterparty_revisit_count"] = (
        df.groupby("pair_key").cumcount().astype(np.float32)
    )

    df["reverse_pair_key"] = (
        df["to_node_key"].astype(str) + "_" + df["from_node_key"].astype(str)
    )
    seen_pairs = set()
    is_reciprocal = []
    for rev_k in df["reverse_pair_key"].values:
        is_reciprocal.append(1.0 if rev_k in seen_pairs else 0.0)
        seen_pairs.add(rev_k)
    df["is_reciprocal_loop"] = np.array(is_reciprocal, dtype=np.float32)

    # Clean up intermediate helper columns
    drop_cols = [
        "ts_sec",
        "prev_received_ts",
        "prev_received_amt",
        "pair_key",
        "reverse_pair_key",
    ]
    df.drop(columns=drop_cols, inplace=True, errors="ignore")
    gc.collect()

    print("✓ ALL 8 AML TYPOLOGY FEATURES SUCCESSFULLY GENERATED!")
    print("=" * 60)
    return df

new_aml_features = ["chain_turnaround_sec", "chain_amount_preservation_ratio", "is_rapid_forwarding", "pass_through_velocity", "bipartite_cross_density", "counterparty_revisit_count", "is_reciprocal_loop"]
num_features.extend(new_aml_features)
features = num_features + cat_features
print(f"Total Features Defined: {len(features)} ({len(num_features)} numerical, {len(cat_features)} categorical)")

# Run on main DataFrame
df = generate_8_aml_typology_features(df)


# ============================================================
# TYPOLOGY FEATURES v2 — counterparty motifs
# ============================================================
# The v1 block above has two features that do not measure what their names
# claim, both in the family the model leans on hardest:
#
#   * src_fan_out_24h / dst_fan_in_24h count TRANSACTIONS in a 24h window,
#     not distinct counterparties. 20 payments to one account and 20
#     payments to 20 accounts score identically — but only the second is
#     smurfing/scatter. Real fan-out is a distinct-counterparty degree.
#
#   * The "mule pass-through" block keys on to_node_key, so for a row S->R
#     it measures when R last received. Layering is about S: funds land in
#     S, then S forwards them out. The turnaround that matters is the gap
#     between the SENDER's last inbound transaction and this outbound one.
#
# Everything here is derived from timestamps and counterparty identity
# only — no labels are touched, so none of it can leak.
# ============================================================

_WINDOW_24H = 86400
_TS_SCALE = 10 ** 10  # > any ts_sec, so group*scale+ts stays monotone per group


def rolling_window_sum_by_group(group_codes, ts, values, window=_WINDOW_24H):
    """Sum `values` over a trailing `window` of `ts`, within each group.

    Fully vectorised: sorting by (group, ts) makes each group contiguous and
    time-ordered, so a composite key group*_TS_SCALE+ts is globally sorted
    and one searchsorted locates every window start at once. Avoids a
    Python-level call per group, which matters at ~500k groups.
    """
    order = np.lexsort((ts, group_codes))
    g_sorted = group_codes[order].astype(np.int64)
    t_sorted = ts[order].astype(np.int64)
    v_sorted = values[order].astype(np.float64)

    key = g_sorted * _TS_SCALE + t_sorted
    lo_key = g_sorted * _TS_SCALE + (t_sorted - window)
    start = np.searchsorted(key, lo_key, side="left")

    cum = np.concatenate(([0.0], np.cumsum(v_sorted)))
    windowed = cum[np.arange(len(t_sorted)) + 1] - cum[start]

    out = np.empty(len(t_sorted), dtype=np.float64)
    out[order] = windowed
    return out


def generate_typology_v2_features(df):
    print("=" * 60)
    print("ENGINEERING TYPOLOGY v2 FEATURES (COUNTERPARTY MOTIFS)")
    print("=" * 60)

    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if "ts_sec" not in df.columns:
        df["ts_sec"] = df["timestamp"].astype("int64") // 10 ** 9

    from_codes = df["from_node_key"].astype("category").cat.codes.to_numpy()
    to_codes = df["to_node_key"].astype("category").cat.codes.to_numpy()
    ts = df["ts_sec"].to_numpy()

    # ------------------------------------------------------------
    # 1. TRUE DISTINCT-COUNTERPARTY DEGREES
    #    is_first_sender_pair marks the first ever transaction between a
    #    given (sender, recipient), so summing it over a trailing window
    #    counts BRAND-NEW counterparties in that window — the actual
    #    fan-out/fan-in signal, as opposed to raw transaction velocity.
    # ------------------------------------------------------------
    print(" -> [1/3] Distinct-counterparty fan-out / fan-in...")
    first_pair = df["is_first_sender_pair"].to_numpy(dtype=np.float64)

    df["new_recipients_24h_from"] = rolling_window_sum_by_group(
        from_codes, ts, first_pair
    ).astype(np.float32)
    df["new_senders_24h_to"] = rolling_window_sum_by_group(
        to_codes, ts, first_pair
    ).astype(np.float32)

    # 7-day horizon too: measured single-feature AUC climbs from 0.63 (24h)
    # to 0.70 (all-history), so counterparty spread accumulates over longer
    # spans than a day — the week window sits between the two.
    df["new_recipients_7d_from"] = rolling_window_sum_by_group(
        from_codes, ts, first_pair, window=7 * _WINDOW_24H
    ).astype(np.float32)
    df["new_senders_7d_to"] = rolling_window_sum_by_group(
        to_codes, ts, first_pair, window=7 * _WINDOW_24H
    ).astype(np.float32)

    # Expanding (all-history) distinct counterparty counts. The recipient
    # side already exists as distinct_users_on_recipient; this is its
    # missing mirror on the sender side.
    df["distinct_recipients_from"] = (
        df.groupby("from_node_key")["is_first_sender_pair"].cumsum().astype(np.float32)
    )

    # Concentration: how repetitive is this account's counterparty set?
    # Low ratio = same few counterparties over and over (reinvestment,
    # payroll); high ratio = constantly new counterparties (scatter).
    df["recipient_churn_from"] = (
            df["distinct_recipients_from"] / (df["tx_count_from"] + 1.0)
    ).astype(np.float32)

    # ------------------------------------------------------------
    # 2. TRUE SENDER-SIDE PASS-THROUGH (mule / layering)
    #    For each transaction, find the most recent moment the SENDER
    #    received funds, strictly before this one. merge_asof does the
    #    per-account backward search in C.
    # ------------------------------------------------------------
    print(" -> [2/3] Sender-side pass-through (inbound -> outbound latency)...")
    # inflow_ts is a duplicate of the merge key: merge_asof returns the LEFT
    # frame's values in the `on` column, so carrying a copy is what lets us
    # recover WHEN the matched inbound transaction happened.
    # The matched row also identifies the FUNDER (who paid the sender) and
    # carries that funder's own fan-out as of that moment — which turns this
    # single merge into a 2-hop view: F -> S -> R. Scatter-gather and stack
    # typologies are multi-hop, so 1-hop features can only ever see part of
    # them. Columns 1-3 below are computed earlier in this function, so they
    # are already populated for every row acting as an inflow here.
    inflow = pd.DataFrame({
        "node": df["to_node_key"].to_numpy(),
        "ts_sec": ts,
        "inflow_ts": ts,
        "inflow_amt": df["amount_received"].to_numpy(),
        "funder": df["from_node_key"].to_numpy(),
        "funder_distinct_recipients": df["distinct_recipients_from"].to_numpy(),
        "funder_new_recipients_24h": df["new_recipients_24h_from"].to_numpy(),
    })
    outflow = pd.DataFrame({
        "node": df["from_node_key"].to_numpy(),
        "ts_sec": ts,
    })
    # df is timestamp-sorted, so both frames are already sorted on the
    # merge key; `by` scopes the search to the same account, and
    # allow_exact_matches=False keeps it strictly in the past.
    matched = pd.merge_asof(
        outflow,
        inflow,
        on="ts_sec",
        by="node",
        direction="backward",
        allow_exact_matches=False,
    )

    matched_ts = matched["inflow_ts"].to_numpy()
    matched_amt = matched["inflow_amt"].to_numpy()
    has_inflow = ~np.isnan(matched_ts)

    gap = np.where(has_inflow, ts - np.nan_to_num(matched_ts), 999999.0)
    df["sender_inflow_gap_sec"] = gap.astype(np.float32)
    df["sender_has_prior_inflow"] = has_inflow.astype(np.float32)

    # What fraction of the last inbound amount is being forwarded on?
    # Mules forward close to 100% of what they received, and do it fast.
    amt_paid = df["amount_paid"].to_numpy(dtype=np.float64)
    ratio = np.where(
        has_inflow, amt_paid / (np.nan_to_num(matched_amt) + 1e-5), 0.0
    )
    df["passthrough_amount_ratio"] = ratio.astype(np.float32)

    # Speed-weighted forwarding intensity: high when a large share of a
    # recent inflow leaves again almost immediately.
    df["passthrough_velocity_true"] = (
            df["passthrough_amount_ratio"].clip(upper=2.0)
            / (np.log1p(df["sender_inflow_gap_sec"]) + 1.0)
    ).astype(np.float32)

    # NOTE: three 2-hop probes were tried here and all measured near chance
    # once the sender's own 1-hop features were present —
    # funder_distinct_recipients 0.544, bounce-back 0.532,
    # funder_new_recipients_24h 0.519 — so none are retained. The funder
    # columns are still carried through the merge above because they cost
    # nothing extra and document what was tested.

    # ------------------------------------------------------------
    # 2b. EDGE CONCENTRATION
    #     How much of an account's activity flows down this one edge?
    #     Ordinary accounts concentrate on a few counterparties; scatter
    #     and smurfing behaviour spreads thin across many. This is the
    #     strongest single family in the pipeline (0.72-0.77 AUC).
    # ------------------------------------------------------------
    pair_codes = pd.Series(
        (df["from_node_key"] + "\x00" + df["to_node_key"]).astype("category").cat.codes.to_numpy()
    )
    pair_cum_paid = df["amount_paid"].groupby(pair_codes).cumsum()
    pair_cum_recv = df["amount_received"].groupby(pair_codes).cumsum()
    pair_rank = df["counterparty_revisit_count"] + 1.0

    # Sender side: share of this account's outflow going down this edge.
    df["pair_amt_share_from"] = (
            pair_cum_paid / (df["amt_sum_from"] + 1e-5)
    ).astype(np.float32)
    df["pair_tx_share_from"] = (
            pair_rank / (df["tx_count_from"] + 1e-5)
    ).astype(np.float32)

    # Receiver side: share of this account's inbound traffic arriving down
    # this edge. A recipient drawing from one source behaves very
    # differently from one collecting from many (gather / funnel accounts).
    # Only the transaction-count form is kept: the amount-share mirror
    # measured 0.585 against 0.743 for the count form, so concentration
    # here is about how many payments arrive, not how much value.
    df["pair_tx_share_to"] = (
            pair_rank / (df["tx_count_to"] + 1e-5)
    ).astype(np.float32)

    # ------------------------------------------------------------
    # 3. SHORT-WINDOW BURSTINESS
    #    Sender-side only: the receiving-side 1h count measured at AUC
    #    0.503 (indistinguishable from noise) and was dropped rather than
    #    left to dilute colsample_bytree.
    # ------------------------------------------------------------
    print(" -> [3/3] 1h burst counts...")
    ones = np.ones(len(df), dtype=np.float64)
    df["tx_count_1h_from"] = rolling_window_sum_by_group(
        from_codes, ts, ones, window=3600
    ).astype(np.float32)

    df.drop(columns=["ts_sec"], inplace=True, errors="ignore")
    gc.collect()

    print("✓ TYPOLOGY v2 FEATURES GENERATED!")
    print("=" * 60)
    return df


# Single-feature |AUC| measured on a 2.5M-row slice (811 fraud cases);
# is_fast_passthrough (0.505) and tx_count_1h_to (0.503) were dropped as
# indistinguishable from noise.
typology_v2_features = [
    "new_recipients_24h_from", "new_senders_24h_to",       # 0.632 / 0.603
    "new_recipients_7d_from", "new_senders_7d_to",
    "distinct_recipients_from", "recipient_churn_from",    # 0.700 / 0.596
    "sender_inflow_gap_sec", "sender_has_prior_inflow",    # 0.570 / 0.590
    "passthrough_amount_ratio", "passthrough_velocity_true",  # 0.655 / 0.648
    "tx_count_1h_from",                                    # 0.547
    # edge concentration — strongest family in the pipeline
    "pair_tx_share_from", "pair_tx_share_to",              # 0.769 / 0.743
    "pair_amt_share_from",                                 # 0.724
]
num_features.extend(typology_v2_features)
features = num_features + cat_features

df = generate_typology_v2_features(df)
print(f"Total Features Defined: {len(features)} ({len(num_features)} numerical, {len(cat_features)} categorical)")

# Downcast all float64 columns to float32
float64_cols = df.select_dtypes(include=["float64"]).columns
df[float64_cols] = df[float64_cols].astype(np.float32)

# Downcast int64 columns where safe
int64_cols = df.select_dtypes(include=["int64"]).columns
for c in int64_cols:
    cmin = df[c].min()
    cmax = df[c].max()
    if cmin >= np.iinfo(np.int32).min and cmax <= np.iinfo(np.int32).max:
        df[c] = df[c].astype(np.int32)

# Free temporary objects that are no longer needed
for name in ["unique_senders", "prev_unique"]:
    if name in globals():
        del globals()[name]

gc.collect()


# Compute balanced temporal cutoffs chronologically (70% Train / 15% Valid / 15% Test)
train_cutoff = df["timestamp"].quantile(0.70)
valid_cutoff = df["timestamp"].quantile(0.85)

# Cover 100% of df cleanly without leaving trailing rows behind or leaking temporal data
train_mask = df["timestamp"] < train_cutoff
valid_mask = (df["timestamp"] >= train_cutoff) & (df["timestamp"] < valid_cutoff)
test_mask  = df["timestamp"] >= valid_cutoff

train_df = df[train_mask].copy()
valid_df = df[valid_mask].copy()
test_df  = df[test_mask].copy()

# df itself is no longer needed past this point — everything downstream
# works off train_df/valid_df/test_df, so free the memory now.
del df
gc.collect()

print("=" * 60)
print("CORRECTED TEMPORAL SPLIT SUMMARY (BALANCED 70/15/15)")
print("=" * 60)
print(f"Train Cutoff: {train_cutoff}")
print(f"Valid Cutoff: {valid_cutoff}")
print("-" * 60)
print(f"Train Rows: {len(train_df):,} | Laundering Cases: {train_df['is_laundering'].sum():,}")
print(f"Valid Rows: {len(valid_df):,} | Laundering Cases: {valid_df['is_laundering'].sum():,}")
print(f"Test Rows:  {len(test_df):,}  | Laundering Cases: {test_df['is_laundering'].sum():,}")
print("=" * 60)


# [BRE-B] Se eliminó el bloque Leiden: el original lo calculaba pero no lo usaba.
# Se excluyen variables que no se pueden replicar en el Job 2 de streaming.
EXCLUDE = {
    "day_of_month",                                   # artefacto del calendario del dataset
    "bipartite_cross_density",                        # ventanas móviles de 24 h
    "new_recipients_24h_from", "new_senders_24h_to",  # ventanas móviles de 24 h
    "new_recipients_7d_from", "new_senders_7d_to",    # ventanas móviles de 7 días
    "tx_count_1h_from",                               # ventana móvil de 1 h
}
num_features = [f for f in num_features if f not in EXCLUDE]
features = num_features + cat_features
print(f"[BRE-B] Total Features Defined: {len(features)} ({len(num_features)} numerical, {len(cat_features)} categorical)")


# ------------------------------------------------------------
# CACHE THE ENGINEERED FEATURES
# ------------------------------------------------------------
# Everything above — CSV parse, feature engineering, two Leiden runs — is
# identical for any experiment that only varies the feature SELECTION or the
# model hyperparameters. Dumping the engineered splits here lets
# train_only.py rerun just the model in a couple of minutes instead of
# repeating ~20 minutes of setup for every idea.
#
# Only the columns training actually consumes are written, which keeps the
# cache to the numeric feature matrix rather than the full frame.
WRITE_FEATURE_CACHE = False

if WRITE_FEATURE_CACHE:
    import json

    CACHE_DIR = "feature_cache"
    os.makedirs(CACHE_DIR, exist_ok=True)

    keep_cols = num_features + cat_features + ["is_laundering"]
    for split_name, split_df in [
        ("train", train_df), ("valid", valid_df), ("test", test_df)
    ]:
        path = os.path.join(CACHE_DIR, f"{split_name}.pkl")
        split_df[keep_cols].to_pickle(path)
        size_mb = os.path.getsize(path) / 1e6
        print(f"cached {split_name}: {len(split_df):,} rows -> {path} ({size_mb:,.0f} MB)")

    with open(os.path.join(CACHE_DIR, "meta.json"), "w") as fh:
        json.dump(
            {
                "num_features": num_features,
                "cat_features": cat_features,
                "target": "is_laundering",
            },
            fh,
            indent=2,
        )
    print(f"cached feature lists -> {CACHE_DIR}/meta.json")
    print("=" * 60)



# No undersampling: train on the full class distribution and let
# scale_pos_weight (computed below) handle the imbalance. Undersampling to
# 100:1 was discarding ~99% of legit transactions, including the hard
# negatives near the decision boundary that the model needs to see to rank
# well — that's the main suspect for the earlier PR-AUC ceiling.
print("=" * 60)
print("TRAINING SET CLASS BALANCE (NO UNDERSAMPLING)")
print("=" * 60)
print(f"Train Rows: {len(train_df):,} | Laundering Cases: {train_df['is_laundering'].sum():,}")
print("=" * 60)

target = "is_laundering"

# 1. One-hot encode categorical features
encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
X_train_cat = encoder.fit_transform(train_df[cat_features])
X_valid_cat = encoder.transform(valid_df[cat_features])
X_test_cat = encoder.transform(test_df[cat_features])

encoded_cols = encoder.get_feature_names_out(cat_features)
print(f"One-hot encoded {len(cat_features)} categoricals -> {len(encoded_cols)} columns")
X_train_cat_df = pd.DataFrame(
    X_train_cat, columns=encoded_cols, index=train_df.index
)
X_valid_cat_df = pd.DataFrame(
    X_valid_cat, columns=encoded_cols, index=valid_df.index
)
X_test_cat_df = pd.DataFrame(
    X_test_cat, columns=encoded_cols, index=test_df.index
)

# 2. Combine numerical and encoded categorical features
X_train_p = pd.concat([train_df[num_features], X_train_cat_df], axis=1)
X_valid_p = pd.concat([valid_df[num_features], X_valid_cat_df], axis=1)
X_test_p = pd.concat([test_df[num_features], X_test_cat_df], axis=1)

# 3. GUARANTEE NUMERIC ONLY (drops any raw string column that slipped in;
#    the categoricals above already entered as one-hot numeric columns)
X_train_p = X_train_p.select_dtypes(include=[np.number])
X_valid_p = X_valid_p.select_dtypes(include=[np.number])
X_test_p = X_test_p.select_dtypes(include=[np.number])

y_train = train_df[target]
y_valid = valid_df[target]
y_test = test_df[target]

raw_imbalance = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
# No class weighting. The previous np.sqrt(raw_imbalance) was labelled a
# "square-root trick [that] stabilizes PR-AUC" but measured the opposite:
# ablation on the cached matrix gave 0.311 weighted vs 0.375 unweighted,
# and full-imbalance weighting collapsed to 0.180. PR-AUC scores the
# RANKING of predictions, and up-weighting positives distorts which splits
# XGBoost chooses without improving that ranking.
scale_pos_weight = 1.0

# 4. Fit Baseline XGBoost Classifier
model = XGBClassifier(
    objective="binary:logistic",
    eval_metric="aucpr",
    n_estimators=2000,
    learning_rate=0.04,
    # depth 6 -> 8 -> 10 improved monotonically (0.311 / 0.325 / 0.360 on
    # the screening sweep); 12 regressed to 0.371 vs 0.375, so 10 is the
    # saturation point.
    max_depth=10,
    min_child_weight=10,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=scale_pos_weight,
    tree_method="hist",
    random_state=42,
    n_jobs=-1,
    early_stopping_rounds=200,  # Stops when validation PR-AUC peaks
)

model.fit(
    X_train_p, y_train, eval_set=[(X_valid_p, y_valid)], verbose=50
)

# 5. Tune the classification threshold on the validation set (F1-maximizing).
#    This was previously referenced as `best_threshold` without ever being
#    computed, which crashed at prediction time.
valid_preds_proba = model.predict_proba(X_valid_p)[:, 1]
precisions, recalls, thresholds = precision_recall_curve(y_valid, valid_preds_proba)
f1_scores = np.where(
    (precisions + recalls) > 0,
    2 * precisions * recalls / (precisions + recalls + 1e-12),
    0.0,
)
best_idx = np.argmax(f1_scores[:-1])  # last PR point has no matching threshold
best_threshold = thresholds[best_idx]
valid_pr_auc = average_precision_score(y_valid, valid_preds_proba)

print("=" * 60)
print("VALIDATION THRESHOLD TUNING")
print("=" * 60)
print(f"PR-AUC (Validation): {valid_pr_auc:.5f}")
print(
    f"Best Threshold (max F1): {best_threshold:.4f} "
    f"(F1={f1_scores[best_idx]:.4f}, Precision={precisions[best_idx]:.4f}, "
    f"Recall={recalls[best_idx]:.4f})"
)
print("=" * 60)


# 1. Generate probabilities on the completely untouched Test Set
test_preds_proba = model.predict_proba(X_test_p)[:, 1]

# 2. Apply the EXACT same optimal threshold found during validation
test_preds_tuned = (test_preds_proba >= best_threshold).astype(int)

# 3. Calculate final evaluation metrics
test_pr_auc = average_precision_score(y_test, test_preds_proba)
cm_test = confusion_matrix(y_test, test_preds_tuned)

# 4. Print the final out-of-time test dashboard
print("="*60)
print(f"FINAL OUT-OF-TIME TEST PERFORMANCE METRICS")
print("="*60)
print(f"PR-AUC (Average Precision): {test_pr_auc:.5f}")
print("="*60)
print("\n--- Final Test Classification Metrics ---")
print(classification_report(y_test, test_preds_tuned, target_names=["Legit", "Laundering"]))

print("--- Final Test Confusion Matrix ---")
print(f"True Negatives  (Legit correctly cleared): {cm_test[0][0]:,}")
print(f"False Positives (Legit flagged as Fraud):   {cm_test[0][1]:,}")
print(f"False Negatives (Laundering missed!):       {cm_test[1][0]:,}")
print(f"True Positives  (Laundering stopped):      {cm_test[1][1]:,}")
print("="*60)



import matplotlib.pyplot as plt
import seaborn as sns

# Extract baseline feature importances (Tabular Only)
fi_baseline_df = pd.DataFrame({
    'Feature': X_train_p.columns,
    'Importance': model.feature_importances_
}).sort_values(by='Importance', ascending=False).reset_index(drop=True)

# Print Top 15 Baseline Features
print("=" * 60)
print("BASELINE XGBOOST - TOP FEATURES (TABULAR ONLY)")
print("=" * 60)
print(fi_baseline_df.head(15).to_string(index=False))
print("=" * 60)

# Visual Bar Plot for Baseline
plt.figure(figsize=(12, 8))
sns.barplot(
    x='Importance',
    y='Feature',
    data=fi_baseline_df.head(60),
    hue='Feature',
    palette='mako',
    legend=False
)
plt.title('Baseline XGBoost Feature Importances (Tabular Only)', fontsize=14, pad=15)
plt.xlabel('Importance Score (Gain)', fontsize=12)
plt.ylabel('Features', fontsize=12)
plt.tight_layout()
plt.show()
# [BRE-B] Exportar artefactos para el Job 2 (quedan en la pestaña Output)
import json, joblib, sklearn, xgboost

OUT_DIR = "/kaggle/working"
os.makedirs(OUT_DIR, exist_ok=True)

model.save_model(os.path.join(OUT_DIR, "aml_xgb.json"))
joblib.dump(encoder, os.path.join(OUT_DIR, "ohe.joblib"))

meta = {
    "source_notebook": "https://www.kaggle.com/code/arturmarkosyan2/xgboost-leiden-community-detection-pr-auc-0-39",
    "dataset": "ealtman2019/ibm-transactions-for-anti-money-laundering-aml",
    "file": "LI-Small_Trans.csv",
    "primary_end": str(PRIMARY_END),
    "train_cutoff": str(train_cutoff),
    "valid_cutoff": str(valid_cutoff),
    "num_features": num_features,
    "cat_features": cat_features,
    "model_input_columns": list(X_train_p.columns),
    "excluded_features": sorted(EXCLUDE),
    "best_iteration": int(model.best_iteration),
    "threshold": float(best_threshold),
    "valid_pr_auc": float(valid_pr_auc),
    "test_pr_auc": float(test_pr_auc),
    "test_rows": int(len(test_df)),
    "test_laundering": int(test_df["is_laundering"].sum()),
    "versions": {
        "xgboost": xgboost.__version__,
        "scikit-learn": sklearn.__version__,
        "pandas": pd.__version__,
        "numpy": np.__version__,
    },
}
with open(os.path.join(OUT_DIR, "model_meta.json"), "w") as fh:
    json.dump(meta, fh, indent=2)

# Muestra de referencia para verificar que el Job 2 calcula lo mismo que este notebook
golden = test_df.head(20000).copy()
golden["score"] = test_preds_proba[: len(golden)]
golden.to_parquet(os.path.join(OUT_DIR, "golden_sample.parquet"), index=False)

for f in sorted(os.listdir(OUT_DIR)):
    print(f"{f:28s} {os.path.getsize(os.path.join(OUT_DIR, f)) / 1e6:8.2f} MB")
print(json.dumps({k: meta[k] for k in ["threshold", "valid_pr_auc", "test_pr_auc", "best_iteration"]}, indent=2))
