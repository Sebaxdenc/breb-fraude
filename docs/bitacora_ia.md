# Bitácora de delegación a agentes de IA

**Herramientas:**
- **Kiro**, un agente de IA de programación (sesiones en Kiro Web, con modelos Claude de Anthropic). Trabajó sobre este repositorio y una copia local del Entregable 1, bajo la dirección y supervisión estricta del equipo. Filas 1 a 28.
- **Claude** (Anthropic), en claude.ai, para consulta de soporte, refactorización puntual y revisión de documentación sobre este repositorio. Filas 29 a 39.

**Niveles de delegación:**
- **Completa:** el agente generó el código base, esqueleto o documento a partir de los requerimientos, restricciones y arquitectura definidos previamente por el equipo; el equipo revisó, verificó y aceptó el resultado.
- **Parcial:** trabajo colaborativo donde el equipo definió la estrategia o tomó la decisión clave y el agente aportó alternativas, fragmentos de código, consultas o análisis de errores.
- **Nada:** lo hizo el equipo de forma autónoma, sin intervención del agente.

**Alcance:** esta bitácora cubre las sesiones de apoyo técnico con Kiro y Claude. Todas las decisiones de arquitectura, negocio y validación final recaen sobre el equipo.

| # | Acción | Delegación | Justificación |
|---|---|---|---|
| 1 | Plan para atender las 7 correcciones del profesor al Entregable 1 | Parcial | Las directrices vinieron del profesor. El equipo seleccionó la estrategia técnica y pidió al agente estructurar y redactar la propuesta de mitigación para cada punto. |
| 2 | Redacción del Entregable 1 v2.0 (requisitos, ADR, riesgos, plan) | Completa | El equipo redactó los puntos clave y el agente ayudó a dar formato profesional, estructurar secciones y generar los archivos `.docx` y `.pdf` mediante un script supervisado. |
| 3 | Diagrama de arquitectura en AWS y figura de los DAG (draw.io) | Completa | El equipo definió los componentes y el flujo; el agente generó los scripts de automatización en draw.io para acelerar el dibujo con los íconos oficiales. |
| 4 | Búsqueda y comparación de modelos de Kaggle | Parcial | El equipo estableció los criterios de evaluación; el agente ayudó a analizar cuatro notebooks públicos para identificar fugas de información y malas prácticas en el preprocesamiento. |
| 5 | Ajustes al notebook de entrenamiento (seis cambios documentados en el ADR 07) | Completa | El equipo dictó los cambios requeridos por la rúbrica y el agente escribió las celdas de código correspondientes y la exportación del contrato de variables. |
| 6 | Entrenamiento del modelo en Kaggle | Nada | El equipo ejecutó el notebook de forma autónoma en su cuenta de Kaggle y descargó los artefactos. |
| 7 | Estructura del repositorio, contratos en JSON Schema y validación | Completa | El equipo definió los esquemas de datos de negocio y el agente escribió la implementación técnica en código y pruebas automatizadas. |
| 8 | Motor de las 58 variables y prueba de paridad con datos sintéticos | Completa | El equipo especificó la lógica matemática de las variables y el agente escribió la implementación en código, validada luego contra la referencia sintética. |
| 9 | Verificación de paridad con LI-Small real en Kaggle | Parcial | El agente ayudó a redactar el script de verificación y a diagnosticar la discrepancia numérica (aritmética de numexpr), pero el equipo ejecutó las pruebas y confirmó la paridad final. |
| 10 | Decisiones de diseño durante la implementación (D1 SeaweedFS, D3 productor directo, D5 perfiles, postergar D6 latencia) | Parcial | Todas las decisiones estratégicas de diseño y priorización fueron tomadas de manera autónoma por el equipo, utilizando al agente solo para evaluar pros y contras técnicos. |
| 11 | Infraestructura: `docker-compose.yml`, imágenes, perfiles y scripts de inicio | Completa | El equipo diseñó la topología de contenedores requerida y el agente generó los archivos de configuración y los scripts base. |
| 12 | Corridas y mediciones en el portátil de la demo (Windows 11, WSL 2) | Nada | El equipo ejecutó todos los entornos, levantó los servicios y recolectó las evidencias de forma completamente independiente. |
| 13 | Diagnóstico y corrección de las fallas encontradas en esas corridas | Parcial | El equipo identificó los errores en tiempo de ejecución y aportó los logs; el agente propuso las correcciones puntuales de sintaxis y configuración que el equipo validó probando de nuevo. |
| 14 | Pruebas largas | Parcial | Por decisión operativa, el equipo ejecutó directamente las pruebas de larga duración y compartió los resultados para que el agente ayudara a interpretar los stack traces. |
| 15 | Productor y medición fuente → Kafka (RF1) | Completa | El equipo definió el contrato de mensajes y el agente escribió el esqueleto y la lógica del productor en Python. |
| 16 | Jobs 1, 2 y 3 de Spark Structured Streaming | Completa | El equipo especificó las transformaciones de streaming y el agente ayudó a redactar las consultas de Spark SQL y la lógica de micro-batches. |
| 17 | Análisis de la latencia (D6) y cambios para bajarla | Parcial | El equipo indicó los cuellos de botella observados y el agente sugirió optimizaciones de configuración que el equipo midió y evaluó empíricamente. |
| 18 | Alertas: SNS y SQS en moto y servicio de la entidad | Completa | El equipo estableció el flujo de alertas y el agente generó la integración con los SDKs correspondientes. |
| 19 | Tramo de la demo con alertas | Completa | El equipo definió el escenario de prueba y el agente apoyó en la redacción del script de inyección de datos para las alertas. |
| 20 | Job 4 (Gold en estrella, SCD 2, controles de calidad) | Completa | El equipo especificó el esquema dimensional y el agente redactó el código Spark para las operaciones de escritura en delta y validaciones. |
| 21 | Capa de consumo: registro en Trino, permisos, linaje y tablero de Metabase | Completa | El equipo diseñó las métricas del negocio y el agente escribió las definiciones de las tablas en Trino y las consultas de Metabase. |
| 22 | Airflow 3 con los tres DAG | Completa | El equipo definió la secuencia de orquestación y el agente estructuró los archivos DAG en Python utilizando las buenas prácticas de Airflow 3. |
| 23 | Pruebas automáticas (14) | Completa | El equipo indicó qué componentes debían cubrirse y el agente redactó los casos de prueba con `pytest`. |
| 24 | README, plan de implementación (`docs/plan_e2.md`) y documentación de los contratos | Completa | El agente auxilió en la redacción y estandarización de la documentación a partir de las notas técnicas provistas por el equipo. |
| 25 | Commits y pull requests | Parcial | El agente redactó mensajes descriptivos de commit a partir de las tareas realizadas; los pull requests fueron revisados, entendidos y fusionados por el equipo. |
| 26 | Material de la sustentación: guion, láminas, guía de la demo y preguntas de la defensa | Completa | El agente estructuró borradores y guiones basados en el código fuente; el equipo adaptó el contenido a su estilo y dominó la defensa de cada punto. |
| 27 | Esta bitácora | Completa | El agente recopiló el historial de iteraciones para redactar el borrador inicial, el cual fue auditado y ajustado por el equipo. |
| 28 | Ensayo, presentación, demo en vivo y defensa | Nada | Ejecutado 100% por el equipo. Todos los integrantes están capacitados para explicar y defender cualquier parte del sistema. |

## Sesiones con Claude (9 y 10 de noviembre de 2026)

| # | Acción | Delegación | Justificación |
|---|---|---|---|
| 29 | Revisión de la guía del proyecto final del profesor | Completa | El agente leyó la guía para extraer un resumen analítico de los puntos críticos y restricciones de entrega. |
| 30 | Consultas sobre la plataforma: demo local o en AWS, y qué tan viable es migrar | Parcial | El equipo analizó las opciones de infraestructura y consultó al agente sobre la complejidad técnica de migración para tomar la decisión final de mantenerse en local. |
| 31 | Revisión de credenciales en el repositorio público | Completa | El agente analizó de forma estática la estructura de archivos para confirmar la ausencia accidental de secretos de producción. |
| 32 | Diagrama de la arquitectura implementada (`docs/arquitectura/arquitectura_e2.*`) | Completa | El equipo aportó el diseño lógico y el agente ayudó a corregir inconsistencias y reflejar con exactitud la estructura real del código. |
| 33 | README y `contratos/README.md` contrastados con el código | Completa | El agente ayudó a verificar que los comandos y nombres de carpetas en la documentación coincidieran exactamente con el estado actual del repositorio. |
| 34 | `PLAN.md` | Parcial | El equipo solicitó la reorganización de archivos y el agente propuso la ruta óptima para no romper referencias cruzadas en la documentación. |
| 35 | Material de la sustentación contrastado con la rúbrica y con la primera corrida de Airflow en Docker | Parcial | El equipo aportó los tiempos reales de ejecución y el agente actualizó las referencias en las láminas y guiones de la presentación. |
| 36 | OPTIMIZE y VACUUM en un DAG diario (`breb_lakehouse_mantenimiento`) | Parcial | El equipo identificó la necesidad de separar el mantenimiento por rendimiento; el agente redactó la estructura del nuevo DAG y el equipo validó su funcionamiento en Docker. |
| 37 | RF1 durante la demo | Parcial | El equipo detectó variaciones en la latencia del productor bajo carga local y el agente ayudó a ajustar el guion de la demo para gestionar las expectativas técnicas en vivo. |
| 38 | Commits y pull requests #22, #23, #24 y el de esta actualización | Parcial | El agente automatizó la redacción de los commits de soporte técnico, los cuales fueron validados y fusionados por el equipo. |
| 39 | Respuestas sugeridas para la defensa y actualización de la bitácora | Completa | El agente sirvió como sparring académico redactando preguntas de práctica basadas en la rúbrica, que el equipo estudió y discutió conjuntamente. |

## Sesiones con Claude (10 y 11 de noviembre de 2026)

| # | Acción | Delegación | Justificación |
|---|---|---|---|
| 40 | Diagnóstico de la corrida de punta a punta: checkpoints de una corrida anterior después de `down -v` sin perfiles, y tramo incompleto con `--escalones "50:300"` (PR #26 y #27) | Parcial | El equipo corrió el pipeline desde cero y aportó las salidas; el agente encontró las causas en el código y en la documentación de Compose y corrigió el README y la guía de la demo. |
| 41 | Preguntas para la asesoría y preparación de lo que se mostró | Parcial | El agente propuso las preguntas a partir de la guía y del estado del proyecto; el equipo eligió cuáles hacer y preparó el stack. |
| 42 | MVP en AWS (rama `aws/mvp-ec2`): S3, DynamoDB, SNS y SQS reales y el resto del stack en una EC2 | Parcial | A pedido del profesor después de la asesoría. El agente escribió `infra/docker-compose.aws.yml`, las configuraciones de `infra/aws/`, el prefijo de los buckets, el script de preparación y la guía; la cuenta, el rol, la instancia y la corrida en AWS los hace el equipo. |

## Cómo se verificó el entendimiento en el equipo
- **Dominio técnico:** Ningún fragmento de código generado por el agente se integró sin una revisión línea por línea por parte del equipo.
- **Pruebas de paridad y ejecución local:** Toda la lógica crítica (como el motor de variables y los jobs de Spark) se probó con datos reales y sintéticos hasta garantizar resultados idénticos a los requeridos.
- **Defensa sin intermediarios:** El equipo domina la arquitectura, los ADRs y el código del proyecto, cumpliendo con la premisa de que los agentes son herramientas de productividad y no sustitutos del criterio de ingeniería.
