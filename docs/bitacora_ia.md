# Bitácora de delegación a agentes de IA

**Herramientas:**
- **Kiro**, un agente de IA de programación (sesiones en Kiro Web, con modelos Claude de Anthropic). Trabajó sobre este repositorio y una copia local del Entregable 1, sin acceso al portátil de la demo ni a la cuenta de Kaggle del equipo. Filas 1 a 28.
- **Claude** (Anthropic), en claude.ai, con lectura y escritura sobre este repositorio en GitHub. No corrió nada en el portátil de la demo: un integrante corrió los comandos y pegó las salidas en la conversación. Filas 29 a 39.

**Niveles de delegación:**
- **Completa:** el agente hizo la acción de punta a punta; el equipo revisó el resultado y lo aceptó, o lo verificó corriéndolo.
- **Parcial:** el agente y el equipo hicieron partes distintas y necesarias. Por ejemplo, el agente propuso alternativas y el equipo decidió, o el agente escribió el código y el equipo lo ejecutó, midió y reportó las fallas.
- **Nada:** lo hizo el equipo, sin el agente.

**Alcance:** esta bitácora cubre las sesiones con Kiro, desde la revisión del Entregable 1 v1.0 hasta el material de la sustentación, y las sesiones con Claude del 9 y 10 de noviembre de 2026. Lo que el equipo hizo fuera de esas sesiones (por ejemplo, la versión 1.0 del Entregable 1) debe agregarlo el equipo con su nivel de delegación.

| # | Acción | Delegación | Justificación |
|---|---|---|---|
| 1 | Plan para atender las 7 correcciones del profesor al Entregable 1 | Parcial | Las correcciones vinieron del profesor. El agente propuso cómo atender cada una (cuarentena en lugar de DLQ, SNS y SQS en lugar de FastAPI, Trino con catálogo en lugar de Spark SQL); el equipo las aprobó. |
| 2 | Redacción del Entregable 1 v2.0 (requisitos, ADR, riesgos, plan) | Completa | El agente reescribió el documento a partir de la v1.0 del equipo y de las correcciones, y generó el `.docx` y el `.pdf` con un script. El equipo lo revisó antes de entregarlo. |
| 3 | Diagrama de arquitectura en AWS y figura de los DAG (draw.io) | Completa | Generados por script con los íconos oficiales de AWS. Los scripts se quitaron del repositorio en #22 (fila 32); los diagramas se editan en draw.io. |
| 4 | Búsqueda y comparación de modelos de Kaggle | Parcial | El agente revisó cuatro notebooks públicos con IBM AML y documentó sus fallas (fugas de información, SMOTE antes de dividir). Elegir el modelo de Kaggle fue una exigencia del profesor (corrección 2) y el equipo aprobó el candidato. |
| 5 | Ajustes al notebook de entrenamiento (seis cambios documentados en el ADR 07) | Completa | El agente escribió el notebook con los ajustes y la exportación del contrato de variables. |
| 6 | Entrenamiento del modelo en Kaggle | Nada | El equipo ejecutó el notebook en su cuenta de Kaggle (versión 1 de `aml-xgboost-bre-b-fraud`) y descargó los artefactos. El agente no tiene acceso a esa cuenta. |
| 7 | Estructura del repositorio, contratos en JSON Schema y validación | Completa | Código y pruebas del agente. Congelar los contratos con aprobación del equipo sigue pendiente (`docs/plan_e2.md`, fase 0). |
| 8 | Motor de las 58 variables y prueba de paridad con datos sintéticos | Completa | El agente escribió el motor y la referencia sintética; la prueba exige igualdad bit a bit con el notebook. |
| 9 | Verificación de paridad con LI-Small real en Kaggle | Parcial | El agente escribió el notebook de verificación; el equipo lo corrió en Kaggle y reportó 54 de 58 variables. Con esa salida, el agente encontró la causa (la aritmética de numexpr en pandas) y la corrigió; el equipo volvió a correrlo y obtuvo **PARIDAD OK** (#3). |
| 10 | Decisiones de diseño durante la implementación (D1 SeaweedFS, D3 productor directo, D5 perfiles, postergar D6 latencia) | Parcial | El agente presentó las opciones con sus costos; el equipo decidió. Ejemplos: "SeaweedFS sí", el productor publica directo en Kafka, y "si no da la latencia, la dejamos registrada y priorizamos la demo de punta a punta". |
| 11 | Infraestructura: `docker-compose.yml`, imágenes, perfiles y scripts de inicio | Completa | Código del agente. Funcionó en el portátil de la demo después de las correcciones de la fila 13. |
| 12 | Corridas y mediciones en el portátil de la demo (Windows 11, WSL 2) | Nada | El equipo corrió todos los comandos de Docker y copió las salidas: STACK OK, PRUEBA DELTA OK, RAM por perfil, RF1, latencia, FASE 4, JOB 4 OK, PERMISOS OK y el tablero. El agente no tiene acceso a esa máquina. |
| 13 | Diagnóstico y corrección de las fallas encontradas en esas corridas | Parcial | El equipo encontró las fallas y entregó la evidencia (errores de DNS, imagen duplicada al construir, Hive Metastore usando Derby, SeaweedFS sin volúmenes, Job 4 sin infraestructura tras apagar un perfil). El agente diagnosticó y corrigió; el equipo verificó corriendo de nuevo. |
| 14 | Pruebas largas | Parcial | Por decisión del equipo, las pruebas que tardan más de unos minutos las corre el equipo y el agente analiza la salida. El agente se limita a pruebas cortas en su entorno. |
| 15 | Productor y medición fuente → Kafka (RF1) | Completa | Código del agente; la medición en Docker es parte de la fila 12. |
| 16 | Jobs 1, 2 y 3 de Spark Structured Streaming | Completa | Código del agente, verificado en Docker por el equipo: 71.910 de 71.910 eventos y 20.000 de 20.000 decisiones idénticas al notebook. |
| 17 | Análisis de la latencia (D6) y cambios para bajarla | Parcial | El agente instrumentó cada tramo del micro-batch y propuso los cambios; el equipo corrió las dos mediciones (285 s y 99 s de p99) y decidió posponer el resto. |
| 18 | Alertas: SNS y SQS en moto y servicio de la entidad | Completa | Código del agente; el equipo verificó en Docker que los analistas recibieran las 25 alertas del tramo. |
| 19 | Tramo de la demo con alertas | Completa | Código del agente; el equipo lo calculó con LI-Small real (PR-AUC igual al de Kaggle, 25 alertas). |
| 20 | Job 4 (Gold en estrella, SCD 2, controles de calidad) | Completa | Código del agente; el equipo lo corrió en Docker (JOB 4 OK, precisión y recall de 0,52). |
| 21 | Capa de consumo: registro en Trino, permisos, linaje y tablero de Metabase | Completa | Código del agente; el equipo lo corrió en Docker y envió capturas del tablero, con las que se corrigieron dos preguntas (#19). |
| 22 | Airflow 3 con los tres DAG | Completa | Código del agente, verificado sin Docker (los DAG cargan en Airflow 3.3.2 y las etapas batch corren con Spark local). La corrida en Docker la hace el equipo. |
| 23 | Pruebas automáticas (14) | Completa | Paridad, MongoDB con reintento tras falla parcial y contratos. El equipo las puede correr con `pytest`. |
| 24 | README, plan de implementación (`docs/plan_e2.md`) y documentación de los contratos | Completa | El agente los mantuvo al día en cada pull request. |
| 25 | Commits y pull requests | Parcial | El agente escribió los commits y abrió los pull requests a nombre de un integrante; el equipo los revisó y los fusionó (#1 a #21). Los de Claude están en la fila 38. |
| 26 | Material de la sustentación: guion, láminas, guía de la demo y preguntas de la defensa | Completa | Borrador del agente a partir del Entregable 1, el plan (`docs/plan_e2.md`) y el código. El equipo lo adapta, ensaya y decide el reparto. |
| 27 | Esta bitácora | Completa | Borrador del agente a partir del historial de las sesiones. El equipo la revisa y completa lo que hizo fuera de ellas. |
| 28 | Ensayo, presentación, demo en vivo y defensa | Nada | Las hace el equipo. Cada integrante debe poder explicar cualquier parte del sistema, incluido el código que escribió el agente. |

## Sesiones con Claude (9 y 10 de noviembre de 2026)

| # | Acción | Delegación | Justificación |
|---|---|---|---|
| 29 | Revisión de la guía del proyecto final del profesor | Completa | El agente señaló contradicciones y ambigüedades de la guía (calendario de asesorías, penalización individual de 30 % o 40 %, peso real de la defensa en el E2, canal de entrega del E1). Fue análisis para el equipo; no cambió el repositorio. |
| 30 | Consultas sobre la plataforma: demo local o en AWS, y qué tan viable es migrar | Parcial | El agente respondió con lo que dice la guía (sección 3.2 y checklist del E2) y separó lo que en AWS es solo configuración (S3, DynamoDB, SNS, SQS, Atlas) de lo que exige código (EMR Serverless, MSK con IAM, operadores de Airflow, Lake Formation). El integrante decidió no migrar antes de la sustentación y conservar los dos diagramas: el objetivo del E1 y el implementado del E2. |
| 31 | Revisión de credenciales en el repositorio público | Completa | El agente comprobó que los archivos de `infra/credenciales/` están versionados, que todos los puertos se publican solo en 127.0.0.1 y que no hay llaves reales de AWS ni de otros servicios. El integrante decidió dejarlos como están. |
| 32 | Diagrama de la arquitectura implementada (`docs/arquitectura/arquitectura_e2.*`) | Completa | El integrante entregó el diagrama anterior y la guía de estilo de diagramas del curso. El agente lo rehízo en draw.io y lo contrastó con el código; corrigió cinco diferencias: `risk_decisions` está en Gold y no en Silver, el Job 2 corre en dos aplicaciones (riesgo y ventanas), los nombres de las tablas de Gold, las tareas reales del DAG, y el formato y la frecuencia del productor. A pedido del integrante se quitaron `generar_arquitectura.py` y `generar_dags.py`. Revisado y fusionado por un integrante (#22). |
| 33 | README y `contratos/README.md` contrastados con el código | Completa | Seis correcciones al README: Metabase se configura con `breb.consumo tablero`, los artefactos del modelo ya están en el repositorio, faltaban los perfiles `streaming` y `herramientas`, las carpetas `jobs/` y `contratos/`, las opciones `--tramo` y `--solo-invalidos`, y el enlace al `.drawio`. En los contratos se documentó el subdocumento `ventanas` de `cuentas`. Revisado y fusionado por un integrante (#22). |
| 34 | `PLAN.md` | Parcial | El integrante pidió quitarlo de la raíz. El agente propuso moverlo a `docs/plan_e2.md` en lugar de borrarlo, porque lo citan el README, las láminas, el guion, las preguntas y esta bitácora; el integrante lo aceptó al fusionar #23. |
| 35 | Material de la sustentación contrastado con la rúbrica y con la primera corrida de Airflow en Docker | Parcial | El integrante corrió `breb_lakehouse_pipeline` en Docker y pegó la salida (11 min 39 s). El agente actualizó la guía de la demo, el guion, las preguntas y las láminas 6 y 10, y comprobó que los 31 enlaces al código de las preguntas apuntan a líneas que existen (#23). |
| 36 | OPTIMIZE y VACUUM en un DAG diario (`breb_lakehouse_mantenimiento`) | Parcial | Con los tiempos por tarea que pegó el integrante (VACUUM 6 min 46 s, OPTIMIZE 1 min 16 s), el agente propuso sacar el mantenimiento del pipeline, escribió el DAG nuevo y comprobó que los cuatro DAG cargan en Airflow 3.3.2. El integrante decidió hacerlo, reconstruyó la imagen con los comandos que dio el agente y midió el pipeline nuevo en Docker: 6 tareas en `success`, 3 min 33 s, con los hechos en Gold a los 2 min 21 s (#24). |
| 37 | RF1 durante la demo | Parcial | En la medición del integrante, el productor cerró en "PUBLICACIÓN CON PROBLEMAS" (p99 de 4,6 s y 0 errores). El agente explicó la causa (los jobs de streaming comparten el portátil y Kafka acababa de arrancar) y cambió el guion del minuto 3:15, la guía de la demo y las preguntas para no prometer que el RF1 se cumple en vivo (#24). |
| 38 | Commits y pull requests #22, #23, #24 y el de esta actualización | Parcial | El agente escribió los commits y abrió los pull requests a nombre de un integrante (Sebas_MC). El commit de #22 lleva la línea de coautoría de Claude; desde #23 no la lleva, por pedido del integrante. Por eso queda constancia aquí: los escribió el agente. El integrante los revisó y los fusionó. |
| 39 | Respuestas sugeridas para la defensa (por qué local y no AWS) y esta actualización de la bitácora | Completa | Borradores del agente a partir del ADR 06, del código y de la conversación. Las respuestas son una guía de estudio: cada integrante debe poder sostenerlas con sus palabras. El equipo revisa esta bitácora. |

## Cómo se verificó lo delegado
- **Con medidas, no con revisión de estilo:** paridad bit a bit con el notebook (58 de 58 variables, 20.000 de 20.000 decisiones), conteos sin duplicados de Kafka a `risk_decisions`, controles de calidad del Job 4 y 10 casos de permisos.
- **En el equipo real de la demo:** cada fase se aceptó cuando el equipo la corrió en el portátil de 16 GB y pegó la salida (por ejemplo, `FASE 4 OK`, `JOB 4 OK`, `PERMISOS OK`).
- **Con pruebas automáticas:** 14 pruebas que corren sin Docker, incluida la de reintento tras una falla a mitad de escritura.
- **Sesiones con Claude:** el diagrama y el README se contrastaron con el código; el DAG nuevo se aceptó cuando el integrante lo corrió en Docker y pegó la salida (6 tareas en `success`, 3 min 33 s).

## Errores del agente que detectó el equipo
- La primera versión del Job 2 escribía `risk_decisions` dentro de la decisión; en Docker eso costaba de 6 a 19 s por micro-batch. Se sacó del camino crítico.
- Limitar los offsets por micro-batch cambiaba las variables frente al notebook; se quitó el tope (D2).
- La instrucción de apagar los jobs con `--profile streaming stop` apagaba toda la infraestructura y el Job 4 falló 19 minutos después; se cambió por apagar los jobs por nombre.
- Varias imágenes compartían la misma sección `build` y Docker fallaba con "already exists"; solo `spark-master` construye la imagen.
- Pruebas que el agente lanzó en su entorno tardaron unos 30 minutos y el equipo las detuvo; desde entonces el equipo corre las pruebas largas (fila 14).

### Errores de Claude que se corrigieron en la conversación
- La primera versión del diagrama E2 se hizo solo a partir de la imagen anterior y tenía cinco diferencias con el código (fila 32). Se corrigieron al leer el repositorio.
- Antes de leer el código, el agente advirtió que el Job 2 puntuaba eventos sin validar. Al leer `job2_riesgo.py` vio que sí valida el contrato, y retiró la advertencia.
- En la medición del DAG nuevo, las instrucciones del agente decían apagar los jobs cuando los logs dejaran de imprimir `batch`. Silver todavía no tenía los datos y la corrida salió con todas las tareas en `skipped`. Se repitió comprobando antes la versión de Silver (`version_silver_pendiente`).
