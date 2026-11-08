from breb.contratos import errores_evento, esquema, uuid_transaccion
from breb.productor.eventos import eventos_invalidos


def test_eventos_invalidos_se_detectan():
    for e in eventos_invalidos(50, semilla=3):
        assert errores_evento(e), e


def test_fecha_inexistente_es_invalida():
    e = eventos_invalidos(1)[0]
    e.update(seq=5, event_ts="2022-02-30T10:00:00", source_account="1_ABC", amount_paid=1.0,
             payment_currency="US Dollar", payment_format="ACH")
    e.pop("campo_extra", None)
    assert errores_evento(e) == ["event_ts: fecha inexistente"]


def test_uuid_determinista():
    assert uuid_transaccion("x", 7) == uuid_transaccion("x", 7) != uuid_transaccion("x", 8)


def test_monedas_del_contrato_coinciden_con_el_modelo():
    import json
    import pathlib
    meta = json.loads((pathlib.Path(__file__).parents[1] / "modelo/aml_xgb/model_meta.json").read_text())
    del_modelo = {c.split("_", 2)[2] for c in meta["model_input_columns"] if c.startswith("payment_currency_")}
    formatos = {c.split("_", 2)[2] for c in meta["model_input_columns"] if c.startswith("payment_format_")}
    s = esquema("evento_transferencia")
    assert set(s["$defs"]["moneda"]["enum"]) == del_modelo
    assert set(s["properties"]["payment_format"]["enum"]) == formatos
