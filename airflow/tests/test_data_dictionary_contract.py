"""Contrato entre os nomes de coluna publicados pela API e os que o pipeline realmente lê.

`contracts/data_dictionary.v1.json` é a resposta de `GET /api/v1/data-dictionary`. Ao mudar o
catálogo em qualquer um dos dois repositórios, atualize o arquivo a partir da API e ajuste o outro.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts_python.ucs_pipeline import UC_SOURCE_COLUMNS
from scripts_python.za_buffer_pipeline import ZA_SOURCE_COLUMNS

CONTRACT = json.loads((Path(__file__).parent / "contracts" / "data_dictionary.v1.json").read_text(encoding="utf-8"))


def _columns(base_id: str) -> dict[str, set[str]]:
    base = next(item for item in CONTRACT["bases"] if item["id"] == base_id)
    return {column["name"]: {column["name"], *column["aliases"]} for column in base["columns"]}


@pytest.mark.parametrize("base_id", ["uc-create", "uc-update", "uc-replace-point"])
def test_uc_columns_match_the_published_dictionary(base_id: str) -> None:
    published = _columns(base_id)
    pipeline = {name: set(candidates) for name, candidates in UC_SOURCE_COLUMNS.items()}
    assert published == pipeline


@pytest.mark.parametrize("base_id", ["uc-za-batch", "za-replace-buffer"])
def test_official_zone_columns_match_the_published_dictionary(base_id: str) -> None:
    published = _columns(base_id)
    pipeline = {name: set(candidates) for name, candidates in ZA_SOURCE_COLUMNS.items()}
    assert published == pipeline
