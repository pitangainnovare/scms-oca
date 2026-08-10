"""
Serviço de transformação de dados raw para bronze no OpenSearch.
Utiliza scripts Painless configurados via interface.
"""
import json
import logging
from collections import defaultdict

from django.db.models import Exists, OuterRef
from django.utils import timezone

from etl.models import EtlItemProcess, EtlPipelineConfig
from etl.services import enqueue_etl_item
from search_gateway.client import get_opensearch_client

from .models import HarvestStatus, IndexStatus, TransformationScript
from .utils import source_hash

logger = logging.getLogger(__name__)

DEFAULT_TRANSFORM_PAGE_SIZE = 500

client = get_opensearch_client()


def index_exists(index_name):
    return bool(client.indices.exists(index=index_name))


def _ensure_indices_exist(source_index, dest_index):
    if not index_exists(source_index) or not index_exists(dest_index):
        raise RuntimeError(
            f"O indice source ou destino: {source_index} / {dest_index} "
            "não existe no opensearch"
        )


def _parse_query_script(query_script):
    if query_script is None or query_script == "":
        raise ValueError("query_script vazio")

    if isinstance(query_script, dict):
        query = query_script
    elif isinstance(query_script, str):
        query = json.loads(query_script)
    else:
        raise TypeError("query_script deve ser str (JSON) ou dict")

    if not isinstance(query, dict):
        raise ValueError("query_script precisa ser um JSON object (dict)")
    return query


def _build_reindex_body(
    source_index,
    dest_index,
    transform_script,
    query_script=None,
    identifiers=None,
):
    body = {
        "source": {"index": source_index},
        "dest": {"index": dest_index},
        "script": {"lang": "painless", "source": transform_script},
    }
    if identifiers:
        body["source"]["query"] = {"ids": {"values": list(identifiers)}}
    elif query_script:
        body["source"]["query"] = _parse_query_script(query_script)
    return body


def _build_transform_result(
    status,
    index,
    dest,
    total=0,
    created=0,
    updated=0,
    error=None,
):
    return {
        "status": status,
        "total": total,
        "created": created,
        "updated": updated,
        "index": index,
        "dest": dest,
        "error": error,
    }


def _run_reindex(body):
    index = body["source"]["index"]
    dest = body["dest"]["index"]
    try:
        _ensure_indices_exist(index, dest)
        response = client.reindex(body=body)
        return _build_transform_result(
            "success",
            index=index,
            dest=dest,
            total=int(response.get("total", 0) or 0),
            created=int(response.get("created", 0) or 0),
            updated=int(response.get("updated", 0) or 0),
        )
    except Exception as exc:
        message = f"Erro ao transformar {index} → {dest}: {exc}"
        logger.error(f"{message}. Detalhe OpenSearch: {getattr(exc, 'info', None)}")
        return _build_transform_result(
            "error",
            index=index,
            dest=dest,
            error=message,
        )


def get_active_transformation_scripts(model_name, type_data=None):
    """
    Scripts ativos do modelo.
    SciELO Data: chave `model_type`, ou todos os variantes se type_data omitido.
    """
    qs = TransformationScript.objects.filter(is_active=True)
    if model_name == "HarvestedSciELOData":
        if type_data:
            return qs.filter(harvest_model=f"{model_name}_{type_data}")
        return qs.filter(harvest_model__startswith=f"{model_name}_")
    return qs.filter(harvest_model=model_name)


def _refresh_source_for_page(script, identifiers):
    try:
        client.indices.refresh(index=script.source_index)
    except Exception as exc:
        message = (
            f"Falha ao refresh do índice fonte {script.source_index} "
            f"antes da transformação de {len(identifiers)} documento(s): {exc}"
        )
        return _build_transform_result(
            "error",
            index=script.source_index,
            dest=script.dest_index,
            error=message,
        )


def _reindex_page(script, identifiers):
    body = _build_reindex_body(
        source_index=script.source_index,
        dest_index=script.dest_index,
        transform_script=script.transform_script,
        identifiers=identifiers,
    )

    logger.info(
        f"Transformando página com {len(identifiers)} documento(s) de "
        f"{script.source_index} para {script.dest_index}"
    )
    result = _run_reindex(body)
    if result.get("status") != "success":
        return result

    if result.get("total", 0) == 0:
        message = (
            f"Nenhum documento encontrado em {script.source_index} "
            f"para {len(identifiers)} identifier(s)."
        )
        return _build_transform_result(
            "error",
            index=script.source_index,
            dest=script.dest_index,
            total=0,
            error=message,
        )

    return result


def transform_documents_page(script, identifiers):
    """
    Transforma uma página raw → bronze:
    1 refresh do índice fonte + 1 reindex por lista de IDs.
    """
    if not identifiers:
        return _build_transform_result(
            "skip",
            index=script.source_index,
            dest=script.dest_index,
            error="Nenhum identifier informado para transformação paginada.",
        )

    if error_result := _refresh_source_for_page(script, identifiers):
        return error_result

    result = _reindex_page(script, identifiers)
    if result.get("status") != "success":
        return result

    result["enqueued"] = sum(
        1
        for identifier in identifiers
        if _enqueue_transformed_bronze(script.dest_index, identifier)
    )
    return result


def transform_indexed_page(model_name, identifiers, type_data=None):
    """
    Resolve o script ativo e transforma a página de identifiers.

    Retorna None quando não há script ativo (catch-up cobre depois).
    """
    if not identifiers:
        return None

    script = get_active_transformation_scripts(model_name, type_data=type_data).first()
    if not script:
        logger.info(
            f"Nenhum script ativo encontrado para {model_name}"
            + (f"_{type_data}" if type_data else "")
        )
        return None

    result = transform_documents_page(script, identifiers)
    if result.get("status") == "error":
        logger.error(
            f"Falha ao transformar página {model_name}"
            + (f" type={type_data}" if type_data else "")
            + f" ({len(identifiers)} docs): {result.get('error')}"
        )
    return result


def transform_documents_batch(script):
    """Reindex completo (botão admin). Não é o caminho da coleta contínua."""
    try:
        body = _build_reindex_body(
            source_index=script.source_index,
            dest_index=script.dest_index,
            transform_script=script.transform_script,
            query_script=getattr(script, "query_script", None) or None,
        )
    except Exception as exc:
        return _build_transform_result(
            "error",
            index=script.source_index,
            dest=script.dest_index,
            error=f"Query JSON inválida: {exc}",
        )

    logger.info(
        f"Transformando em lote de {script.source_index} para {script.dest_index}"
    )
    return _run_reindex(body)


def _enqueue_transformed_bronze(index_name, identifier):
    try:
        response = client.get(index=index_name, id=identifier)
        source = response.get("_source") or {}
        if not EtlPipelineConfig.objects.select_for_source(index_name, source):
            return False
        payload_hash = source_hash(source)
        client.update(
            index=index_name,
            id=identifier,
            body={
                "doc": {
                    "oca_indexed_at": timezone.now().isoformat(),
                    "oca_source_hash": payload_hash,
                }
            },
            refresh=False,
        )
        enqueue_etl_item(
            source_index=index_name,
            external_id=identifier,
            source_payload=source,
        )
        return True
    except Exception as exc:
        logger.warning(
            f"Falha ao enfileirar ETL silver para {index_name}/{identifier}: {exc}"
        )
        return False


def _transform_batches_by_type(model_name, batches_by_type):
    """Dispara transform_indexed_page para cada lote não vazio."""
    for type_data, identifiers in batches_by_type.items():
        if identifiers:
            transform_indexed_page(model_name, identifiers, type_data=type_data)


def reconcile_missing_bronze_etl(document_model, page_size=DEFAULT_TRANSFORM_PAGE_SIZE):
    model_name = document_model.__name__
    bronze_indices = set(
        get_active_transformation_scripts(model_name).values_list(
            "dest_index", flat=True
        )
    )
    if not bronze_indices:
        logger.info(
            f"Reconciliação. Fase 2. Não há script para transformar raw em bronze para {model_name}."
        )
        return

    indexed_qs = document_model.objects.filter(
        harvest_status=HarvestStatus.SUCCESS,
        index_status=IndexStatus.SUCCESS,
    ).exclude(raw_data={})

    has_etl = EtlItemProcess.objects.filter(
        source_index__in=bronze_indices,
        external_id=OuterRef("identifier"),
    )

    missing_qs = indexed_qs.filter(~Exists(has_etl))
    missing_count = missing_qs.count()
    logger.info(f"Reconciliação. Fase 2. {model_name}: {missing_count} sem ETL")
    if missing_count == 0:
        return

    batches_by_type = defaultdict(list)
    for obj in missing_qs.iterator():
        type_data = getattr(obj, "type_data", None)
        batches_by_type[type_data].append(obj.identifier)
        if len(batches_by_type[type_data]) >= page_size:
            transform_indexed_page(
                model_name,
                batches_by_type[type_data],
                type_data=type_data,
            )
            batches_by_type[type_data] = []

    _transform_batches_by_type(model_name, batches_by_type)
