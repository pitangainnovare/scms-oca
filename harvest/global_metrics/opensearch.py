from etl.transform.normalizers import normalize_issn
from etl.world_regions import world_region_for_country
from harvest.global_metrics.parsing import (
    append_unique,
    global_metric_row_from_hit,
)
from search_gateway.option_normalization import clean_text

def iter_harvest_metric_groups(client, harvest_index, source_file):
    """Percorre o harvest do arquivo de upload e devolve grupos ISSN + ano.

    Linhas com o mesmo ano e ISSN canônico são unidas: ``indexed_in``,
    ``country_codes`` e variantes de ISSN (print/e-ISSN).
    """
    groups = {}
    body = {
        "query": source_file_query(source_file),
        "_source": [
            "raw_data.issns",
            "raw_data.year",
            "raw_data.country",
            "raw_data.scopus_active_in_the_year",
            "raw_data.wos_active_in_the_year",
            "raw_data.scielo_active_and_valid_in_the_year",
        ],
        "size": 10000,
    }
    for hit in scroll_hits(
        client,
        harvest_index,
        body
    ):
        row = global_metric_row_from_hit(hit)
        if not row:
            continue
        _merge_harvest_row_into_groups(groups, row)

    yield from groups.values()


def source_file_query(source_file):
    """Filtro que restringe o harvest ao arquivo de upload informado."""
    return {
        "bool": {
            "should": [
                {"term": {"source_file.keyword": source_file}},
                {"term": {"source_file": source_file}},
                {"match_phrase": {"source_file": source_file}},
            ],
            "minimum_should_match": 1,
        }
    }


def _merge_harvest_row_into_groups(groups, row):
    year = row["year"]
    seen_canonical = set()
    for issn in row["issns"]:
        canonical_issn = normalize_issn(issn) or clean_text(issn)
        if not canonical_issn or canonical_issn in seen_canonical:
            continue
        seen_canonical.add(canonical_issn)

        group = groups.get((year, canonical_issn))
        if group is None:
            group = {
                "year": year,
                "issns": [],
                "indexed_in": set(),
                "country_codes": [],
                "countries": [],
                "metric_rows": 0,
                "unresolved_countries": [],
            }
            groups[(year, canonical_issn)] = group

        group["metric_rows"] += 1
        group["indexed_in"].update(row["indexed_in"])
        append_unique(group["issns"], issn)
        append_unique(group["issns"], canonical_issn)
        append_unique(group["country_codes"], row.get("country_code"))
        append_unique(group["countries"], row.get("country"))
        if row.get("country") and not row.get("country_code"):
            append_unique(group["unresolved_countries"], row["country"])


def update_silver_group_by_query(client, silver_index, group):
    """Aplica as métricas de um grupo no silver via ``update_by_query``.

    Dispara a atualização em background (``wait_for_completion=False``)
    para evitar o timeout HTTP de 40s.
    """
    return client.update_by_query(
        index=silver_index,
        body=build_global_metrics_update_by_query_body(group),
        conflicts="proceed",
        refresh=False,
        wait_for_completion=False,
    )


def build_global_metrics_update_by_query_body(group):
    """Corpo do ``update_by_query`` para um único grupo ISSN + ano.

    A query filtra só ``publication_year`` e os ISSNs daquele grupo. Os
    params incluem ``indexed_in``, ``country_codes`` e a ``world_region``
    derivada do primeiro código de país.
    """
    country_code = group["country_codes"][0] if group["country_codes"] else None

    return {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"publication_year": group["year"]}},
                    {"terms": {"source.issns": group["issns"]}},
                ]
            }
        },
        "script": {
            "lang": "painless",
            "source": global_metrics_update_script(),
            "params": {
                "indexed_in": sorted(group["indexed_in"]),
                "country_codes": group["country_codes"],
                "world_region": world_region_for_country(country_code),
            },
        },
    }


def global_metrics_update_script():
    """Script Painless que grava métricas globais em ``oca_data.scielo.source``.

    Atualiza ``indexed_in`` sem duplicar valores já presentes, define
    ``country_code`` com o primeiro código resolvido e preenche ou remove
    ``world_region`` conforme o parâmetro recebido.
    """
    return """
        if (ctx._source.oca_data == null) {
            ctx._source.oca_data = new HashMap();
        }
        if (ctx._source.oca_data.scielo == null) {
            ctx._source.oca_data.scielo = new HashMap();
        }
        if (ctx._source.oca_data.scielo.source == null) {
            ctx._source.oca_data.scielo.source = new HashMap();
        }
        if (params.indexed_in != null && params.indexed_in.size() > 0) {
            def current = ctx._source.oca_data.scielo.source.indexed_in;
            if (current == null) {
                ctx._source.oca_data.scielo.source.indexed_in = new ArrayList();
            } else if (!(current instanceof List)) {
                def values = new ArrayList();
                values.add(current);
                ctx._source.oca_data.scielo.source.indexed_in = values;
            }
            for (def value : params.indexed_in) {
                if (!ctx._source.oca_data.scielo.source.indexed_in.contains(value)) {
                    ctx._source.oca_data.scielo.source.indexed_in.add(value);
                }
            }
        }

    """


def scroll_hits(client, index, body, scroll="20m"):
    """Itera todos os hits de uma search com scroll e libera o contexto ao fim."""
    response = client.search(index=index, body=body, scroll=scroll)
    scroll_id = response.get("_scroll_id")
    try:
        while True:
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                break
            yield from hits
            response = client.scroll(scroll_id=scroll_id, scroll=scroll)
            scroll_id = response.get("_scroll_id")
    finally:
        if scroll_id:
            client.clear_scroll(scroll_id=scroll_id)
