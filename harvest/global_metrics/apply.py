import logging
from pathlib import Path

from django.conf import settings
from django.db import close_old_connections

from harvest.global_metrics.opensearch import (
    iter_harvest_metric_groups,
    update_silver_group_by_query,
    wait_for_update_task,
)
from harvest.models import GlobalMetricsUploadFile
from search_gateway.client import get_opensearch_client


def apply_global_metrics_upload_to_silver(
    upload_file_id,
    harvest_index=None,
    silver_index=None
):

    upload_file = GlobalMetricsUploadFile.objects.get(pk=upload_file_id)
    if not upload_file.status:
        raise RuntimeError(
            "O arquivo de métricas globais ainda não foi processado."
        )
    client = get_opensearch_client()
    if client is None:
        raise RuntimeError("Cliente OpenSearch não configurado.")

    harvest_index = harvest_index or settings.GLOBAL_METRICS_FILE_UPLOAD_OPENSEARCH_INDEX
    silver_index = silver_index or getattr(
        settings,
        "ETL_SILVER_INDEX_PATTERN",
        "silver_scientific_production",
    )
    source_file = Path(upload_file.file.name).name
    logging.info(
        f"Arquivo de métricas globais {upload_file.pk} ({source_file}) já foi processado; "
        f"iniciando update no índice silver {silver_index}."
    )

    stats = {
        "upload_file_id": upload_file_id,
        "source_file": source_file,
        "harvest_rows": 0,
        "groups_processed": 0,
        "matches_found": 0,
        "updated": 0,
        "version_conflicts": 0,
        "unresolved_countries": [],
        "errors": [],
    }
    unresolved_countries = set()

    try:
        groups = iter_harvest_metric_groups(
            client=client,
            harvest_index=harvest_index,
            source_file=source_file,
        )
        for group in groups:
            stats["harvest_rows"] += group.pop("metric_rows", 0)
            unresolved_countries.update(group.pop("unresolved_countries", []))
            submission = update_silver_group_by_query(
                client=client,
                silver_index=silver_index,
                group=group,
            )
            task_id = submission.get("task")
            if not task_id:
                raise RuntimeError(
                    "OpenSearch não retornou o ID da task de update_by_query."
                )
            response = wait_for_update_task(client, task_id)
            stats["groups_processed"] += 1
            stats["matches_found"] += response.get("total", 0)
            stats["updated"] += response.get("updated", 0)
            stats["version_conflicts"] += response.get("version_conflicts", 0)
            if failures := response.get("failures"):
                stats["errors"].extend(failures)
    finally:
        close_old_connections()

    stats["unresolved_countries"] = sorted(unresolved_countries)
    upload_file.save_stats(stats)
    logging.info(
        f"Métricas globais do upload {upload_file.pk} aplicadas em {silver_index}: "
        f"{stats['groups_processed']} grupos, {stats['matches_found']} matches, "
        f"{stats['updated']} atualizações."
    )
    return stats
