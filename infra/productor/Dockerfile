# Productor de la demo (fase 3): reproduce LI-Small en Kafka. Representa a las entidades que envían
# transferencias a Bre-B, así que va en su propia imagen, sin Spark.
# Contexto de build: la raíz del repositorio (ver infra/docker-compose.yml).

ARG PYTHON_IMAGE=python:3.11.18-slim-bookworm
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 \
    BREB_DATOS=/datos \
    BREB_MODEL_META=/opt/breb/modelo/aml_xgb/model_meta.json

COPY infra/productor/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY pyproject.toml /opt/breb/
COPY src/ /opt/breb/src/
RUN pip install --no-cache-dir --no-deps /opt/breb && rm -rf /opt/breb/build /opt/breb/src/*.egg-info
# Solo los metadatos del modelo (corte de validación y fin del rango); el modelo no hace falta aquí.
COPY modelo/aml_xgb/model_meta.json /opt/breb/modelo/aml_xgb/model_meta.json

RUN useradd --uid 1000 --create-home productor
USER productor
WORKDIR /datos
ENTRYPOINT ["python", "-m", "breb.productor"]
CMD ["--help"]
