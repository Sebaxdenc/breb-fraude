"""Gráfico de latencia del Job 2 para las láminas (datos de PLAN.md, decisión D6).

Uso: python generar_graficos.py   (escribe latencia.png en esta carpeta; necesita matplotlib)
"""
import os
import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

NUM = FuncFormatter(lambda v, _: f"{v:g}".replace(".", ","))

AQUI = pathlib.Path(__file__).parent
if os.environ.get("BREB_FUENTES"):   # carpeta con LiberationSans-*.ttf si el sistema no tiene Arial
    for ttf in pathlib.Path(os.environ["BREB_FUENTES"]).glob("*.ttf"):
        font_manager.fontManager.addfont(str(ttf))
plt.rcParams.update({"font.family": ["Liberation Sans", "Arial", "DejaVu Sans"], "font.size": 16,
                     "axes.spines.top": False, "axes.spines.right": False})

fig, (a, b) = plt.subplots(1, 2, figsize=(13, 5.6), dpi=150, gridspec_kw={"width_ratios": [1, 1.25]})

corridas = ["1.ª corrida\n100–500 ev/s", "2.ª corrida\n250 ev/s", "Tramo\n50 ev/s"]
p99 = [285, 99, 53]
barras = a.bar(corridas, p99, color=["#C00000", "#E8710A", "#E8710A"], width=0.6)
a.set_yscale("log")
a.set_ylim(1, 600)
a.axhline(3, color="#2E9E44", lw=2.2, ls="--", label="Objetivo RNF-02: p99 < 3 s")
a.legend(loc="upper right", frameon=False, fontsize=15, labelcolor="#2E9E44")
a.yaxis.set_major_formatter(NUM)
for r, v in zip(barras, p99):
    a.text(r.get_x() + r.get_width() / 2, v * 1.12, f"{v} s", ha="center", va="bottom", fontsize=17, fontweight="bold")
a.set_ylabel("p99 Kafka → decisión (s, log)")
a.set_title("p99 medido en Docker", fontsize=18, fontweight="bold", loc="left")

tramos = ["Variables\n+ modelo", "MongoDB:\nescribir", "MongoDB:\nleer", "Kafka + validar\nen Python"]
rangos = [(0.05, 0.5), (2.5, 12.4), (0.9, 6.9), (2.7, 62)]
colores = ["#2E9E44", "#C925D1", "#C925D1", "#C00000"]
for i, ((lo, hi), c) in enumerate(zip(rangos, colores)):
    b.barh(i, hi - lo, left=lo, color=c, height=0.5, alpha=0.9)
    b.text(hi * 1.12, i, "< 0,5 s" if hi == 0.5 else f"{lo:g}–{hi:g} s".replace(".", ","), va="center",
           fontsize=16, fontweight="bold")
b.set_yticks(range(len(tramos)), tramos)
b.set_xscale("log")
b.set_xlim(0.04, 300)
b.xaxis.set_major_formatter(NUM)
b.set_xlabel("segundos por micro-batch (mín.–máx., log)")
b.set_title("Dónde se va el tiempo (2.ª corrida)", fontsize=18, fontweight="bold",
            loc="left")

fig.tight_layout()
fig.savefig(AQUI / "latencia.png", facecolor="white")
print("ok", AQUI / "latencia.png")
