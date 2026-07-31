from functools import lru_cache

import country_converter
import pycountry

WORLD_REGIONS_UPDATE_SCRIPT = """
    boolean changed = false;
    def ocaData = ctx._source.oca_data;
    def scielo = ocaData != null ? ocaData.scielo : null;
    def source = scielo != null ? scielo.source : null;

    if (source != null) {
        def countryCode = source.country_code;
        def expectedRegion =
            countryCode != null ? params.mapping[countryCode] : null;
        def currentRegion = source.world_region;

        if (expectedRegion == null && currentRegion != null) {
            source.remove('world_region');
            changed = true;
        } else if (
            expectedRegion != null && !expectedRegion.equals(currentRegion)
        ) {
            source.world_region = expectedRegion;
            changed = true;
        }
    }

    def expectedRegions = new ArrayList();
    def uniqueRegions = new HashSet();

    for (def countryCode : ctx._source.author_country_codes ?: []) {
        def region = params.mapping[countryCode];

        if (region != null) {
            uniqueRegions.add(region);
        }
    }

    expectedRegions.addAll(uniqueRegions);
    Collections.sort(expectedRegions);

    def openalex = ocaData != null ? ocaData.openalex : null;
    def affiliations = openalex != null ? openalex.affiliations : null;
    def currentRegions =
        affiliations != null ? affiliations.world_regions : null;

    if (expectedRegions.isEmpty()) {
        if (currentRegions != null) {
            affiliations.remove('world_regions');
            changed = true;
        }
    } else if (!expectedRegions.equals(currentRegions)) {
        if (ocaData == null) {
            ocaData = new HashMap();
            ctx._source.oca_data = ocaData;
        }
        if (ocaData.openalex == null) {
            ocaData.openalex = new HashMap();
        }
        if (ocaData.openalex.affiliations == null) {
            ocaData.openalex.affiliations = new HashMap();
        }

        ocaData.openalex.affiliations.world_regions = expectedRegions;
        changed = true;
    }

    if (!changed) {
        ctx.op = 'noop';
    }
"""


@lru_cache(maxsize=1)
def country_world_regions():
    country_codes = sorted(country.alpha_2 for country in pycountry.countries)
    regions = country_converter.CountryConverter().convert(
        names=country_codes,
        src="ISO2",
        to="UNregion",
    )

    return {
        country_code: region
        for country_code, region in zip(country_codes, regions)
        if isinstance(region, str) and region != "not found"
    }


def world_region_for_country(country_code):
    if not isinstance(country_code, str):
        return None

    return country_world_regions().get(country_code.strip().upper())


def add_source_world_region(document):
    source = document["oca_data"].get("scielo", {}).get("source")

    if source is None:
        return

    region = world_region_for_country(source.get("country_code"))

    if region:
        source["world_region"] = region
    else:
        source.pop("world_region", None)


def add_affiliation_world_regions(document):
    regions = sorted(
        {
            region
            for country_code in document.get("author_country_codes") or []
            if (region := world_region_for_country(country_code))
        }
    )
    oca_data = document["oca_data"]
    affiliations = oca_data.get("openalex", {}).get("affiliations")

    if regions:
        affiliations = oca_data.setdefault("openalex", {}).setdefault(
            "affiliations",
            {},
        )
        affiliations["world_regions"] = regions
    elif affiliations is not None:
        affiliations.pop("world_regions", None)


def apply_world_regions(
    client,
    index_name,
    slices,
    batch_size,
    requests_per_second,
    repair,
):
    required_fields = {
        "oca_data.scielo.source.world_region": "world_region",
        "oca_data.openalex.affiliations.world_regions": "world_regions",
    }
    mappings = client.indices.get_field_mapping(
        index=index_name,
        fields=",".join(required_fields),
    )
    invalid_fields = []

    for concrete_index, index_mapping in mappings.items():
        fields = index_mapping.get("mappings") or {}

        for field_name, leaf_name in required_fields.items():
            field_type = (
                fields.get(field_name, {})
                .get("mapping", {})
                .get(leaf_name, {})
                .get("type")
            )

            if field_type != "keyword":
                invalid_fields.append(f"{concrete_index}:{field_name}")

    if not mappings:
        invalid_fields.append(index_name)

    if invalid_fields:
        fields = ", ".join(invalid_fields)

        raise RuntimeError(
            f"Mapping de regiões ausente ou incompatível em {fields}. "
            "Aplique docs/Regioes-do-mundo.md antes do backfill."
        )

    if repair:
        query = {
            "bool": {
                "should": [
                    {"exists": {"field": "oca_data.scielo.source.country_code"}},
                    {"exists": {"field": "author_country_codes"}},
                    {"exists": {"field": "oca_data.scielo.source.world_region"}},
                    {
                        "exists": {
                            "field": "oca_data.openalex.affiliations.world_regions"
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }
    else:
        query = {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "filter": {
                                "exists": {
                                    "field": "oca_data.scielo.source.country_code"
                                }
                            },
                            "must_not": {
                                "exists": {
                                    "field": "oca_data.scielo.source.world_region"
                                }
                            },
                        }
                    },
                    {
                        "bool": {
                            "filter": {
                                "exists": {
                                    "field": "author_country_codes"
                                }
                            },
                            "must_not": {
                                "exists": {
                                    "field": (
                                        "oca_data.openalex.affiliations.world_regions"
                                    )
                                }
                            },
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }

    body = {
        "query": query,
        "script": {
            "lang": "painless",
            "source": WORLD_REGIONS_UPDATE_SCRIPT,
            "params": {"mapping": country_world_regions()},
        },
    }

    return client.update_by_query(
        index=index_name,
        body=body,
        params={
            "conflicts": "proceed",
            "refresh": "false",
            "wait_for_completion": "false",
            "slices": slices,
            "scroll_size": batch_size,
            "requests_per_second": requests_per_second,
        },
    )
