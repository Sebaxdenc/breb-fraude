"""Figura de los tres DAG de Airflow tal como están en dags/ (actualiza la Figura 2 del Entregable 1).

Uso: python generar_dags.py dags_airflow.drawio
"""
import sys
from xml.sax.saxutils import escape

OUT = sys.argv[1]
esc = lambda s: escape(s, {'"': "&quot;"})
cells = []


def v(cid, value, style, x, y, w, h):
    cells.append(f'<mxCell id="{cid}" value="{esc(value)}" style="{esc(style)}" vertex="1" parent="1">'
                 f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')


def e(cid, s, t, exit=(1, 0.5), entry=(0, 0.5)):
    st = ("edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;endArrow=block;endFill=1;endSize=7;strokeColor=#5F6B7A;"
          f"strokeWidth=1.6;exitX={exit[0]};exitY={exit[1]};exitDx=0;exitDy=0;entryX={entry[0]};entryY={entry[1]};"
          "entryDx=0;entryDy=0;")
    cells.append(f'<mxCell id="{cid}" value="" style="{esc(st)}" edge="1" parent="1" source="{s}" target="{t}">'
                 '<mxGeometry relative="1" as="geometry"/></mxCell>')


BASE = "rounded=1;arcSize=12;whiteSpace=wrap;html=1;fontFamily=Courier New;fontSize=14;fontColor=#232F3E;strokeWidth=1.5;"
SENSOR = BASE + "fillColor=#DAE8FC;strokeColor=#6C8EBF;"
TASK = BASE + "fillColor=#D5E8D4;strokeColor=#82B366;"
MAINT = BASE + "fillColor=#FFE6CC;strokeColor=#D79B00;"
T_B = "text;html=1;align=left;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#232F3E;fontFamily=Helvetica;"
T_N = "text;html=1;align=left;verticalAlign=middle;fontSize=13;fontColor=#5F6B7A;fontFamily=Helvetica;whiteSpace=wrap;"
W, H = 230, 44

# DAG 1
v("t1", "DAG 1 · breb_lakehouse_pipeline", T_B, 20, 10, 330, 30)
v("t1s", "cada 15 min · catchup=False · max_active_runs=1 · max_active_tasks=1 · retries=3 con retroceso "
         "exponencial (máx. 5 min) · timeout 30 min por tarea", T_N, 360, 10, 1200, 30)
v("hlth", "check_streaming_health", SENSOR, 20, 60, W, H)
v("silv", "check_new_silver_data", SENSOR, 20, 130, W, H)
v("silvn", "PythonOperator · versión de Silver por XCom; sin datos nuevos: skipped", T_N, 20, 178, 270, 40)
v("dims", "silver_to_gold_dims", TASK, 300, 95, W, H)
v("fact", "silver_to_gold_facts", TASK, 570, 95, W, H)
v("dq", "data_quality_checks", TASK, 840, 95, W, H)
v("dqn", "retries=0 · registra la versión procesada", T_N, 840, 143, 270, 24)
v("sync", "sync_profiles_to_mongo", TASK, 1110, 95, W, H)
v("opt", "optimize", MAINT, 1380, 60, 160, H)
v("vac", "vacuum", MAINT, 1380, 130, 160, H)
v("tr", "trigger_rule = all_done", T_N, 1380, 178, 200, 24)
v("vn", "dims, facts y calidad: spark-submit con --version-silver &lt;XCom&gt; (time travel)", T_N, 300, 178, 520, 24)
e("a1", "hlth", "dims", entry=(0, 0.3))
e("a2", "silv", "dims", entry=(0, 0.7))
e("a3", "dims", "fact")
e("a4", "fact", "dq")
e("a5", "dq", "sync")
e("a6", "sync", "opt", exit=(1, 0.3))
e("a7", "sync", "vac", exit=(1, 0.7))

# DAG 2
v("t2", "DAG 2 · reprocess_quarantine", T_B, 20, 230, 330, 30)
v("t2s", "bajo demanda (schedule=None) · retries=2", T_N, 360, 230, 600, 30)
v("rq1", "reprocesar_cuarentena", TASK, 20, 272, W, H)
v("rq2", "disparar_pipeline", TASK, 300, 272, W, H)
v("rqn", "Corrige la cuarentena de Bronze sin inventar datos, MERGE a Silver (sin duplicados) y resultado por evento en "
         "silver.reprocesos_cuarentena. TriggerDagRunOperator lanza el DAG 1 para que lleguen a Gold.",
  T_N, 570, 266, 900, 56)
e("b1", "rq1", "rq2")

# DAG 3
v("t3", "DAG 3 · bootstrap_state", T_B, 20, 340, 330, 30)
v("t3s", "bajo demanda · parámetro inicio = tramo | corte · tareas que ya tienen su resultado salen con exit 99 "
         "(skipped) y las siguientes usan none_failed", T_N, 360, 340, 1200, 30)
x = 20
ids = ["comprobar_streaming_detenido", "preparar_eventos", "elegir_tramo", "cargar_estado", "verificar_estado"]
for i, nombre in enumerate(ids):
    v(f"bs{i}", nombre, TASK if i else SENSOR, x, 382, 290 if i == 0 else 230, H)
    x += (290 if i == 0 else 230) + 40
for i in range(4):
    e(f"c{i}", f"bs{i}", f"bs{i + 1}")

v("l1", "", SENSOR.replace("fontSize=14", "fontSize=11"), 20, 452, 26, 18)
v("l1t", "Comprobación / sensor", T_N, 54, 450, 170, 22)
v("l2", "", TASK, 230, 452, 26, 18)
v("l2t", "Tarea Spark / Python", T_N, 264, 450, 170, 22)
v("l3", "", MAINT, 440, 452, 26, 18)
v("l3t", "Mantenimiento Delta", T_N, 474, 450, 170, 22)

xml = ('<mxfile host="app.diagrams.net" type="device"><diagram id="dags" name="DAG">'
       '<mxGraphModel dx="1600" dy="500" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" '
       'page="1" pageScale="1" pageWidth="1600" pageHeight="490" math="0" shadow="0"><root><mxCell id="0"/>'
       '<mxCell id="1" parent="0"/>' + "".join(cells) + '</root></mxGraphModel></diagram></mxfile>')
open(OUT, "w", encoding="utf-8").write(xml)
print("ok", len(cells), "celdas")
