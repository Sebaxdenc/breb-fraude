# Contratos de integración

Estas cuatro interfaces se congelan en S13 (sección 5.1 del documento). Cambiarlas requiere un pull request aprobado por los cuatro integrantes y subir la versión.

| Contrato | Archivo | Productor → consumidor |
|---|---|---|
| Evento de transferencia 1.0 | [`src/breb/contratos/evento_transferencia.schema.json`](../src/breb/contratos/evento_transferencia.schema.json) | Productor / API de ingesta → Kafka `breb.transacciones.raw` → Jobs 1 y 2 |
| Alerta de fraude 1.0 | [`src/breb/contratos/alerta_fraude.schema.json`](../src/breb/contratos/alerta_fraude.schema.json) | Job 2 → SNS `fraud-alerts-critical` → colas SQS |
| Variables del modelo | [`modelo/aml_xgb/model_meta.json`](../modelo/aml_xgb/model_meta.json) | Notebook de entrenamiento → Job 2 |
| Documentos de MongoDB | [este archivo, sección siguiente](#documentos-de-mongodb) | Job 2 (riesgo y ventanas), Airflow y servicio de la entidad → MongoDB |

Los esquemas JSON están dentro del paquete `breb` para que el productor, el Job 1, el Job 2 y las pruebas usen exactamente el mismo archivo. `breb.contratos.errores_evento(evento)` devuelve la lista de errores que el Job 1 guarda en `validation_errors`.

## Decisiones del contrato del evento
- `transaction_uuid` es un **UUID v5** calculado a partir de `seq`, no un UUID v4 aleatorio: si el productor reintenta un envío, el evento conserva su identificador y la deduplicación por `transaction_uuid` funciona (RNF-03).
- `seq` es la posición de la transacción en el orden del entrenamiento (timestamp con sort estable). El Job 2 ordena cada micro-batch por `seq`, porque las variables acumuladas dependen del orden.
- `event_ts` es la marca de tiempo original, sin zona horaria. La hora de llegada no entra al modelo; la latencia se mide con el timestamp `LogAppendTime` del bróker.
- La etiqueta `Is Laundering` no viaja en el evento. Se carga aparte en Gold para medir precisión y recall.

## Documentos de MongoDB
Tres colecciones. Cada escritor actualiza solo su subdocumento con `$set` (RF8, riesgo R6).

| Colección | `_id` | `streaming` (Job 2) | `ventanas` (Job 2 · ventanas) | `historico` (Airflow) | `estado_retencion` (entidad) |
|---|---|---|---|---|---|
| `cuentas` | `"{banco}_{cuenta}"` | Acumulados como ordenante y como destino | Ventana más reciente: 5 min como ordenante y 10 min como receptora | Perfil desde Gold | Retención de fondos vigente |
| `pares` | `"{origen}\|{destino}"` | Conteo y monto acumulado del par | — | — | — |
| `bancos` | `"{banco}"` | Conteo como banco de origen y de destino | — | — | — |

### Subdocumento `streaming` de `cuentas`
| Campo | Significado | Variables que alimenta |
|---|---|---|
| `out_n` | Envíos previos de la cuenta | `tx_count_from`, `sender_avg_amt` |
| `out_acc`, `out_comp` | Suma compensada (Kahan) de montos enviados | `amt_sum_from`, `sender_avg_amt` |
| `out2_acc`, `out2_comp` | Suma compensada de montos enviados al cuadrado | `sender_std_amt` |
| `out_last_ts` | Último envío (segundos epoch) | `seconds_since_last_tx_from` |
| `out_dist` | Destinatarios distintos | `distinct_recipients_from`, `recipient_churn_from` |
| `in_n` | Entradas previas | `tx_count_to`, `recipient_prev_tx_count` |
| `in_acc`, `in_comp` | Suma compensada de montos recibidos | `amt_sum_to`, `recipient_prev_received` |
| `in_last_ts`, `in_last_paid`, `in_last_recv` | Última entrada: minuto, monto pagado y monto recibido | `seconds_since_last_tx_to`, `chain_*`, `sender_inflow_gap_sec` |
| `in_prev_ts`, `in_prev_recv` | Última entrada de un minuto anterior al de `in_last_ts` | `sender_inflow_gap_sec`, `passthrough_*` |
| `in_dist` | Ordenantes distintos | `distinct_users_on_recipient`, `recipient_unique_senders` |
| `batch_id` | Último micro-batch aplicado (`-1` = estado inicial) | Idempotencia |

`streaming_prev` guarda el subdocumento anterior al último micro-batch. Si Spark reintenta ese micro-batch, el Job 2 parte de `streaming_prev` y el resultado es el mismo aunque la falla haya ocurrido a mitad de la escritura (`src/breb/estado/almacen.py`).

Las sumas usan compensación de Kahan porque `groupby().cumsum()` de pandas la usa; sin ella, las variables no saldrían idénticas a las del entrenamiento.

### Subdocumento `ventanas` de `cuentas`
Lo escribe solo la aplicación `job2-ventanas` (RF3, `main_ventanas` en `src/breb/streaming/job2_riesgo.py`): ventanas deslizantes con salto de 1 minuto, watermark de 2 minutos y trigger de 30 s, leídas directo de Kafka. De cada cuenta guarda únicamente la ventana de los últimos minutos.

| Campo | Ventana | Contenido |
|---|---|---|
| `ventanas.envio_5m` | 5 min por cuenta ordenante (`source_account`) | `inicio`, `fin`, `batch_id`, `transferencias`, `monto` (suma de `amount_paid`) y `destinatarios_distintos` |
| `ventanas.recepcion_10m` | 10 min por cuenta receptora (`destination_account`) | `inicio`, `fin`, `batch_id`, `transferencias`, `monto_recibido` (suma de `amount_received`) y `ordenantes_distintos` |

- Los conteos de distintos son aproximados (`approx_count_distinct`).
- Solo reemplaza la ventana guardada si la nueva termina igual o más tarde (`fin`), así un micro-batch reintentado o atrasado no la pisa con una más vieja.
- Usa `upsert`: si la cuenta todavía no existe en `cuentas`, crea el documento solo con este subdocumento.
- El modelo del Job 2 no lo lee: ninguna de las 58 variables sale de aquí. El tablero de Metabase calcula sus preguntas de ventanas con SQL sobre Gold.
