"""
Serviço de transformação de dados raw para bronze no OpenSearch.
Utiliza scripts Painless configurados via interface.
"""
import json
import logging

from django.db.models import Exists, OuterRef
from django.utils import timezone

from etl.models import EtlItemProcess, EtlPipelineConfig
from etl.services import enqueue_etl_item
from search_gateway.client import get_opensearch_client

from .models import HarvestStatus, IndexStatus, TransformationScript
from .utils import source_hash

logger = logging.getLogger(__name__)


client = get_opensearch_client()

def index_exists(index_name):
    """
    Verifica se um índice existe no OpenSearch.

    Mantém o comportamento simples (True/False) para facilitar reuso.
    """
    return bool(client.indices.exists(index=index_name))


def _missing_index_error(source_index, dest_index):
    return {
        "status": "error",
        "message": (
            "O indice source ou destino: "
            f"{source_index} / {dest_index} não existe no opensearch"
        ),
    }


def _ensure_indices_exist(source_index, dest_index):
    if not index_exists(source_index) or not index_exists(dest_index):
        return _missing_index_error(source_index, dest_index)
    return None


def _parse_query_script(query_script, identifier=None):
    """
    Retorna um dict de query (para `source.query` do reindex).

    - `query_script` pode ser `str` (JSON) ou `dict`.
    - Se `identifier` for informado, substitui placeholder `{{identifier}}`.
    """
    if query_script is None or query_script == "":
        raise ValueError("query_script vazio")

    if isinstance(query_script, dict):
        query = query_script
    elif isinstance(query_script, str):
        raw = query_script
        if identifier:
            raw = raw.replace("{{identifier}}", identifier).replace(
                "{{ identifier }}", identifier
            )
        query = json.loads(raw)
    else:
        raise TypeError("query_script deve ser str (JSON) ou dict")

    if not isinstance(query, dict):
        raise ValueError("query_script precisa ser um JSON object (dict)")
    return query


def _base_reindex_body(source_index, dest_index, transform_script):
    return {
        "source": {"index": source_index},
        "dest": {"index": dest_index},
        "script": {"lang": "painless", "source": transform_script},
    }


def _build_document_reindex_body(
    source_index,
    dest_index,
    transform_script,
    identifier,
    query_script=None,
):
    body = _base_reindex_body(
        source_index=source_index,
        dest_index=dest_index,
        transform_script=transform_script,
    )
    if query_script:
        body["source"]["query"] = _parse_query_script(
            query_script=query_script,
            identifier=identifier,
        )
    else:
        body["source"]["query"] = {"ids": {"values": [identifier]}}
    return body


def _build_batch_reindex_body(
    source_index,
    dest_index,
    transform_script,
    query_script=None,
):
    body = _base_reindex_body(
        source_index=source_index,
        dest_index=dest_index,
        transform_script=transform_script,
    )
    if query_script:
        body["source"]["query"] = _parse_query_script(query_script=query_script)
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
    index = (body.get("source") or {}).get("index")
    dest = (body.get("dest") or {}).get("index")
    try:
        missing = _ensure_indices_exist(index, dest)
        if missing:
            raise RuntimeError(missing["message"])
        response = client.reindex(body=body, refresh=True)
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


def transform_document(script, identifier):
    """
    Transforma um único documento raw → bronze.

    Usado no fluxo automático após indexação (signals / reconciliação).
    """
    if not identifier:
        return _build_transform_result(
            "error",
            index=script.source_index,
            dest=script.dest_index,
            error="identifier é obrigatório para transformação unitária.",
        )

    try:
        client.indices.refresh(index=script.source_index)
    except Exception as exc:
        logger.error(
            f"Falha ao refresh do índice fonte {script.source_index} "
            f"antes da transformação de {identifier}: {exc}"
        )
        return _build_transform_result(
            "error",
            index=script.source_index,
            dest=script.dest_index,
            error=str(exc),
        )

    body = _build_document_reindex_body(
        source_index=script.source_index,
        dest_index=script.dest_index,
        transform_script=script.transform_script,
        query_script=getattr(script, "query_script", None),
        identifier=identifier,
    )

    logger.info(
        f"Transformando documento {identifier} de {script.source_index} para {script.dest_index}"
    )
    result = _run_reindex(body)
    if result.get("status") != "success":
        return result
    if (result.get("created", 0) + result.get("updated", 0)) > 0:
        _enqueue_transformed_bronze(script.dest_index, identifier)
    return result


def transform_documents_batch(script):
    """
    Transforma documentos raw → bronze em lote.

    Usado pelo botão de execução na interface administrativa.
    """
    try:
        body = _build_batch_reindex_body(
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
            return
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
    except Exception as exc:
        logger.warning(
            f"Falha ao enfileirar ETL silver para {index_name}/{identifier}: {exc}"
        )


def transform_after_indexing(instance, model_name):
    """
    Função auxiliar para ser chamada após indexação.
    Busca o TransformationScript pelo harvest_model e executa a transformação.

    Args:
        instance: Instância do modelo (HarvestedBooks, HarvestedPreprint, etc)
        model_name: Nome da classe do modelo

    Returns:
        Dict com status e mensagem da operação
    """
    harvest_model_key = model_name
    if model_name == "HarvestedSciELOData":
        type_data = getattr(instance, "type_data", None)
        if type_data:
            harvest_model_key = f"{model_name}_{type_data}"

    script = TransformationScript.objects.filter(
        harvest_model=harvest_model_key,
        is_active=True
    ).first()

    if not script:
        logger.info(
            f"Nenhum script de transformação ativo encontrado para {harvest_model_key}"
        )
        return {"status": "skip", "message": f"Nenhum script ativo encontrado para {harvest_model_key}"}

    identifier = getattr(instance, "identifier", None)
    if not identifier:
        return {"status": "error", "message": "Instância em identifiesr; não é possível transformar."}

    return transform_document(script, identifier)


def reconcile_missing_bronze_etl(document_model):
    model_name = document_model.__name__

    bronze_indices = set(
        TransformationScript.objects.filter(
            harvest_model=model_name,
            is_active=True,
        ).values_list("dest_index", flat=True)
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

    logger.info(f"Reconciliação. Fase 2. {model_name}: {missing_qs.count()} sem ETL")
    for obj in missing_qs.iterator():
        try:
            transform_after_indexing(instance=obj, model_name=model_name)
        except Exception as exc:
            logger.warning(
                f"Reconcialiação. Fase 2. Falha ao criar ETL para documento do tipo "
                f"{model_name} ({obj.identifier}): {exc}"
            )
