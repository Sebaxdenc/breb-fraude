"""Diagrama draw.io de la arquitectura implementada en el Entregable 2 (stack local con Docker Compose).

Uso: python generar_arquitectura.py arquitectura_e2.drawio
La Figura 1 del Entregable 1 (arquitectura_e1_aws.png) muestra el objetivo en AWS. Este diagrama muestra lo que
corre en el repositorio, con cada job de Spark como un componente propio y el equivalente en AWS de cada pieza.
"""
import sys
from xml.sax.saxutils import escape

OUT = sys.argv[1]
esc = lambda s: escape(s, {'"': "&quot;"})
cells, origin = [], {"1": (0, 0)}


def vertex(cid, value, style, x, y, w, h, parent="1"):
    px, py = origin[parent]
    origin[cid] = (x, y)
    cells.append(f'<mxCell id="{cid}" value="{esc(value)}" style="{esc(style)}" vertex="1" parent="{parent}">'
                 f'<mxGeometry x="{x - px}" y="{y - py}" width="{w}" height="{h}" as="geometry"/></mxCell>')


def edge(cid, s, t, style, exit, entry, pts=()):
    st = style + (f"exitX={exit[0]};exitY={exit[1]};exitDx=0;exitDy=0;"
                  f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;")
    geo = '<mxGeometry relative="1" as="geometry">'
    if pts:
        geo += '<Array as="points">' + "".join(f'<mxPoint x="{x}" y="{y}"/>' for x, y in pts) + '</Array>'
    cells.append(f'<mxCell id="{cid}" value="" style="{esc(st)}" edge="1" parent="1" source="{s}" target="{t}">'
                 f'{geo}</mxGeometry></mxCell>')


def free_line(cid, style, x1, y1, x2, y2):
    cells.append(f'<mxCell id="{cid}" value="" style="{esc(style)}" edge="1" parent="1"><mxGeometry relative="1" as="geometry">'
                 f'<mxPoint x="{x1}" y="{y1}" as="sourcePoint"/><mxPoint x="{x2}" y="{y2}" as="targetPoint"/>'
                 '</mxGeometry></mxCell>')


FONT = "fontFamily=Helvetica;"
BOX = "rounded=0;whiteSpace=wrap;html=1;fillColor=#FFFFFF;strokeColor=#879196;strokeWidth=1.2;"
TAG = FONT + ("rounded=0;whiteSpace=wrap;html=1;fillColor=#FFFFFF;strokeColor=#232F3E;strokeWidth=1.2;fontStyle=1;"
              "fontSize=15;fontColor=#232F3E;align=center;verticalAlign=middle;")
BADGE = FONT + ("ellipse;whiteSpace=wrap;html=1;aspect=fixed;fillColor=#C00000;strokeColor=#FFFFFF;strokeWidth=2;"
                "fontColor=#FFFFFF;fontStyle=1;fontSize=13;align=center;verticalAlign=middle;")
TXT = FONT + "text;html=1;align=left;verticalAlign=middle;fontSize=12.5;fontColor=#232F3E;whiteSpace=wrap;"
COLORES = {  # relleno, borde (colores de categoría de AWS)
    "fuente": ("#F2F3F3", "#232F3E"), "flujo": ("#EFE7FF", "#8C4FFF"), "bd": ("#FBE9FB", "#C925D1"),
    "datos": ("#EEF5E3", "#7AA116"), "integ": ("#FDE7F1", "#E7157B"), "app": ("#FDEFE3", "#ED7100"),
    "persona": ("#FFFFFF", "#232F3E"), "nota": ("#F8F9FA", "#879196"),
}
EB = "edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;endArrow=block;endFill=1;endSize=7;"
E_RT = EB + "strokeColor=#1E6FD9;strokeWidth=2;"                          # tiempo real
E_OP = EB + "strokeColor=#E8710A;strokeWidth=2.5;"                        # escritura operativa
E_BT = EB + "strokeColor=#5F6B7A;strokeWidth=1.8;dashed=1;dashPattern=6 4;"  # batch (Airflow)
E_RD = EB + "strokeColor=#2E9E44;strokeWidth=2;"                          # lectura / consulta


def box(cid, title, x, y, w, h):
    vertex(cid, "", BOX, x, y, w, h)
    tw = int(len(title) * 8.6 + 26)
    vertex(cid + "_t", title, TAG, x + 10, y + 10, tw, 28)


def card(cid, x, y, w, h, titulo, detalle, tipo, aws=None, badge=None):
    relleno, borde = COLORES[tipo]
    html = f"<b>{titulo}</b>"
    if detalle:
        html += f"<br><span style='font-size:11.5px'>{detalle}</span>"
    if aws:
        html += f"<br><span style='font-size:11px;color:#5F6B7A'>AWS: {aws}</span>"
    st = FONT + (f"rounded=1;arcSize=7;whiteSpace=wrap;html=1;fillColor={relleno};strokeColor={borde};strokeWidth=1.6;"
                 "fontSize=13;fontColor=#232F3E;align=center;verticalAlign=middle;spacing=4;")
    vertex(cid, html, st, x, y, w, h)
    if badge is not None:
        vertex(cid + "_b", str(badge), BADGE, x + w - 16, y - 14, 28, 28)


# ------------------------------------------------------------------ título
vertex("titulo", "Bre-B · Detección de fraude en tiempo real · arquitectura implementada (Entregable 2, Docker Compose)",
       FONT + "text;html=1;align=center;verticalAlign=middle;fontSize=20;fontStyle=1;fontColor=#232F3E;", 15, 4, 1890, 34)

# ------------------------------------------------------------------ fila A: tiempo real
box("b_src", "Fuente", 15, 48, 190, 412)
card("prod", 28, 160, 164, 120, "Productor", "LI-Small · 1.038.880 tx<br>orden de seq · UUID v5<br>acks=all · idempotente",
     "fuente", badge=1)

box("b_ing", "Ingestión", 225, 48, 200, 412)
card("kafka", 238, 160, 174, 120, "Kafka 4.3.2 (KRaft)", "breb.transacciones.raw<br>6 particiones · 7 días<br>clave source_account",
     "flujo", "MSK", badge=2)

box("b_spk", "Procesamiento · Spark 3.5.9 Structured Streaming", 445, 48, 560, 412)
card("job2", 640, 110, 220, 104, "Job 2 · riesgo + XGBoost", "trigger 1 s · dedup · 58 variables<br>score y alerta por transacción",
     "flujo", "EMR Serverless", badge=3)
card("job2v", 640, 230, 220, 70, "Job 2 · ventanas (RF3)", "5 min por ordenante · 10 min por receptora", "flujo", badge=3)
card("job1", 460, 352, 175, 92, "Job 1 · validación", "contrato 1.0 · trigger 30 s", "flujo", "EMR Serverless", badge=7)
card("job3", 815, 352, 175, 92, "Job 3 · Silver", "lee Bronze · trigger 30 s<br>MERGE por transaction_uuid", "flujo",
     "EMR Serverless", badge=8)

box("b_ope", "Estado operacional", 1025, 48, 260, 412)
card("mongo", 1040, 150, 230, 140, "MongoDB 7.0 · replica set",
     "cuentas · pares · bancos<br>w y readConcern majority<br>idempotente por batch_id", "bd", "MongoDB Atlas", badge=4)

box("b_alr", "Alertas", 1305, 48, 390, 412)
card("sns", 1320, 150, 172, 110, "SNS (moto)", "fraud-alerts-critical<br>atributo entidades", "integ", "SNS", badge=5)
card("sqs", 1510, 150, 172, 110, "SQS (moto)", "breb-entidad-&lt;id&gt; con filtro<br>breb-analistas-fraude", "integ", "SQS", badge=5)

box("b_pub", "Consumidores", 1715, 48, 190, 1012)
card("ent", 1726, 150, 168, 120, "Servicio de la entidad", "retiene fondos o bloquea<br>la ordenante: estado_retencion",
     "app", badge=6)
card("ana", 1726, 560, 168, 92, "Analistas de fraude", "bandeja de alertas<br>tablero de Metabase", "persona", badge=11)
card("cum", 1726, 700, 168, 92, "Cumplimiento", "linaje de Kafka a Gold<br>con su rol en Trino", "persona", badge=12)

# ------------------------------------------------------------------ fila B: lakehouse y consumo
box("b_lak", "Lakehouse · Delta Lake 3.3.3", 225, 480, 1060, 320)
card("ddb", 240, 548, 190, 84, "DynamoDB Local", "log de commits de Delta<br>(varios escritores por tabla)", "bd",
     "DynamoDB")
card("swfs", 240, 660, 190, 84, "SeaweedFS", "3 buckets: bronze, silver, gold", "datos", "S3")
card("brz", 460, 540, 175, 92, "Bronze", "append-only · 100 % de eventos<br>coordenadas de Kafka", "datos", badge=7)
card("qua", 460, 680, 175, 80, "Cuarentena", "Bronze con is_valid = false", "datos", badge=7)
card("slv", 760, 540, 175, 92, "Silver", "UTC · sin duplicados", "datos", badge=8)
card("rsk", 1060, 540, 205, 92, "risk_decisions", "una fila por decisión<br>variables · score · latencia", "datos", badge=3)
card("gld", 1060, 680, 205, 100, "Gold · estrella", "fact_transferencias<br>dim_cuenta (SCD 2) · entidad<br>canal · tiempo · etiquetas",
     "datos", badge=9)

box("b_con", "Consumo analítico", 1305, 480, 390, 320)
card("trino", 1320, 548, 172, 132, "Trino 484", "+ Hive Metastore 4.2.1<br>reglas de acceso por rol", "flujo",
     "Athena + Glue + Lake Formation", badge=10)
card("mtb", 1510, 548, 172, 132, "Metabase 0.65", "tablero de 9 preguntas<br>usuario metabase: solo Gold", "app",
     "ECS Fargate", badge=11)

# ------------------------------------------------------------------ fila C: orquestación
box("b_orq", "Orquestación · Airflow 3.3.2", 225, 820, 1060, 240)
card("af", 240, 868, 190, 172, "Airflow 3.3.2", "LocalExecutor · metastore en PostgreSQL<br>tareas Spark en local[2]<br>sobre la imagen de Spark<br>vigila la salud de los Jobs 1, 2 y 3",
     "integ", "MWAA")
card("dag2", 450, 868, 190, 82, "reprocess_quarantine", "bajo demanda<br>cuarentena corregida a Silver", "flujo", badge=9)
card("dag3", 450, 962, 190, 78, "bootstrap_state", "bajo demanda<br>estado inicial en MongoDB", "flujo", badge=9)
vertex("dag1", "<b>breb_lakehouse_pipeline</b> · cada 15 min · <b>Job 4</b>"
               "<div style='text-align:left;font-size:12px;margin-top:4px'>1. salud del streaming y versión nueva de Silver (sensores)<br>"
               "2. dimensiones (dim_cuenta SCD 2) y hechos, leyendo Silver en esa versión<br>"
               "3. controles de calidad: si fallan, no se toca MongoDB<br>"
               "4. perfiles históricos de las cuentas a MongoDB<br>5. OPTIMIZE y VACUUM (all_done)</div>",
       FONT + "rounded=1;arcSize=5;whiteSpace=wrap;html=1;fillColor=#EFE7FF;strokeColor=#8C4FFF;strokeWidth=1.6;"
              "fontSize=13;fontColor=#232F3E;align=center;verticalAlign=middle;spacing=6;", 660, 868, 610, 172)
vertex("dag1_b", "9", BADGE, 1254, 854, 28, 28)

box("b_nol", "No implementado en local", 1305, 820, 390, 240)
vertex("nol_t", "• TLS, KMS, IAM y Secrets Manager (RNF-08): credenciales locales en infra/credenciales<br>"
                "• CI/CD con CodePipeline y CodeBuild<br>"
                "• CloudWatch: lo reemplazan la tabla progreso_streaming y el tablero<br>"
                "• Correo a los analistas: cola SQS breb-analistas-fraude",
       TXT, 1320, 860, 365, 190)

# ------------------------------------------------------------------ leyenda
box("b_leg", "Leyenda", 15, 480, 190, 580)
for i, (txt, st) in enumerate([("Tiempo real", "strokeColor=#1E6FD9;strokeWidth=2;"),
                               ("Escritura operativa", "strokeColor=#E8710A;strokeWidth=2.5;"),
                               ("Batch (Airflow)", "strokeColor=#5F6B7A;strokeWidth=1.8;dashed=1;dashPattern=6 4;"),
                               ("Lectura / consulta", "strokeColor=#2E9E44;strokeWidth=2;")]):
    y = 545 + i * 36
    free_line(f"leg_l{i}", "html=1;endArrow=block;endFill=1;endSize=6;" + st, 28, y, 68, y)
    vertex(f"leg_x{i}", txt, TXT, 76, y - 12, 125, 24)
vertex("leg_b", "n", BADGE, 34, 682, 28, 28)
vertex("leg_bx", "Paso del flujo", TXT, 76, 684, 125, 24)
vertex("leg_aws", "<span style='color:#5F6B7A'>AWS: …</span> equivalente en la arquitectura objetivo del E1",
       TXT, 28, 722, 172, 48)
vertex("leg_per", "<b>Perfiles de Compose</b><br>dev: Kafka, MongoDB, SeaweedFS, DynamoDB, Spark, moto<br>"
                  "streaming: Jobs 1, 2 y 3<br>consumo: PostgreSQL, Hive Metastore, Trino, Metabase<br>"
                  "orquestacion: PostgreSQL, Airflow<br>herramientas: productor, entidad",
       TXT + "verticalAlign=top;fontSize=11.5;", 28, 790, 172, 250)

# ------------------------------------------------------------------ flujos
edge("f1", "prod", "kafka", E_RT, (1, 0.5), (0, 0.5))
edge("f2", "kafka", "job2", E_RT, (1, 0.2), (0, 0.5), [(530, 184), (530, 162)])
edge("f3", "kafka", "job2v", E_RT, (1, 0.45), (0, 0.5), [(560, 214), (560, 265)])
edge("f4", "kafka", "job1", E_RT, (0.5, 1), (0, 0.5), [(325, 398)])
edge("f5", "job1", "brz", E_RT, (0.5, 1), (0.5, 0))
edge("f6", "brz", "qua", E_RT, (0.5, 1), (0.5, 0))
edge("f7", "brz", "job3", E_RT, (1, 0.3), (0, 0.5), [(720, 568), (720, 398)])
edge("f8", "job3", "slv", E_RT, (0.5, 1), (0.5, 0), [(902, 470), (847, 470)])
edge("f9", "job2", "mongo", E_OP, (1, 0.3), (0, 0.3), [(950, 141), (950, 192)])
edge("f10", "mongo", "job2", E_RD, (0, 0.6), (1, 0.75), [(980, 234), (980, 188)])
edge("f11", "job2v", "mongo", E_OP, (1, 0.5), (0, 0.85), [(1010, 265), (1010, 269)])
edge("f12", "job2", "sns", E_RT, (0.5, 0), (0.5, 0), [(750, 100), (1406, 100)])
edge("f13", "sns", "sqs", E_RT, (1, 0.5), (0, 0.5))
edge("f14", "sqs", "ent", E_RD, (1, 0.3), (0, 0.3), [(1700, 183), (1700, 186)])
edge("f15", "sqs", "ana", E_RD, (0.5, 1), (0, 0.5), [(1596, 420), (1705, 420), (1705, 606)])
edge("f16", "ent", "mongo", E_OP, (0.5, 0), (0.5, 0), [(1810, 122), (1155, 122)])
edge("f17", "job2", "rsk", E_RT, (1, 0.9), (0.5, 0), [(1000, 204), (1000, 470), (1162, 470)])
edge("f18", "slv", "gld", E_BT, (1, 0.75), (0, 0.5), [(1000, 609), (1000, 730)])
edge("f19", "rsk", "gld", E_BT, (0.5, 1), (0.5, 0))
edge("f20", "qua", "slv", E_BT, (1, 0.5), (0.5, 1), [(847, 720)])
edge("f21", "dag1", "gld", E_BT, (0.82, 0), (0.5, 1))
edge("f22", "dag1", "mongo", E_BT, (1, 0.5), (1, 0.75), [(1295, 954), (1295, 255)])
edge("f24", "dag2", "qua", E_BT, (0.5, 0), (0.5, 1), [(545, 820), (547, 820)])
edge("f26", "b_lak", "trino", E_RD, (1, 0.3), (0, 0.3))
edge("f27", "trino", "mtb", E_RD, (1, 0.5), (0, 0.5))
edge("f28", "mtb", "ana", E_RD, (1, 0.25), (0, 0.75), [(1705, 581), (1705, 629)])
edge("f29", "trino", "cum", E_RD, (0.5, 1), (0, 0.5), [(1406, 746)])

xml = ('<mxfile host="app.diagrams.net" type="device"><diagram id="e2" name="Arquitectura E2">'
       '<mxGraphModel dx="2000" dy="1100" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" '
       'page="1" pageScale="1" pageWidth="1920" pageHeight="1080" math="0" shadow="0"><root><mxCell id="0"/>'
       '<mxCell id="1" parent="0"/>' + "".join(cells) + '</root></mxGraphModel></diagram></mxfile>')
open(OUT, "w", encoding="utf-8").write(xml)
print("ok", len(cells), "celdas")
