from unittest.mock import Mock

from django.test import SimpleTestCase

from etl.world_regions import (
    add_affiliation_world_regions,
    add_source_world_region,
    apply_world_regions,
    world_region_for_country,
)


class WorldRegionsTests(SimpleTestCase):
    def setUp(self):
        self.client = Mock()
        self.client.indices.get_field_mapping.return_value = {
            "silver": {
                "mappings": {
                    "oca_data.scielo.source.world_region": {
                        "mapping": {
                            "world_region": {
                                "type": "keyword",
                            },
                        },
                    },
                    "oca_data.openalex.affiliations.world_regions": {
                        "mapping": {
                            "world_regions": {
                                "type": "keyword",
                            },
                        },
                    },
                },
            },
        }

    def test_maps_canonical_iso_country_codes(self):
        self.assertEqual(world_region_for_country("GB"), "Northern Europe")
        self.assertEqual(world_region_for_country("GR"), "Southern Europe")

    def test_returns_none_for_invalid_country_codes(self):
        for country_code in ("", "BRA", "INVALID", None):
            with self.subTest(country_code=country_code):
                self.assertIsNone(world_region_for_country(country_code))

    def test_ignores_document_without_source(self):
        document = {"oca_data": {}}

        add_source_world_region(document)

        self.assertEqual(document, {"oca_data": {}})

    def test_adds_and_removes_unique_affiliation_world_regions(self):
        document = {
            "author_country_codes": ["BR", "JP", "BR"],
            "oca_data": {},
        }

        add_affiliation_world_regions(document)

        affiliations = document["oca_data"]["openalex"]["affiliations"]
        self.assertEqual(
            affiliations["world_regions"],
            ["Eastern Asia", "South America"],
        )

        document.pop("author_country_codes")

        add_affiliation_world_regions(document)

        self.assertNotIn("world_regions", affiliations)

    def test_starts_incremental_async_backfill(self):
        self.client.update_by_query.return_value = {"task": "node:1"}

        result = apply_world_regions(
            self.client,
            "silver",
            "auto",
            1000,
            500,
            False,
        )

        self.assertEqual(result, {"task": "node:1"})
        self.client.indices.get_field_mapping.assert_called_once_with(
            index="silver",
            fields=(
                "oca_data.scielo.source.world_region,"
                "oca_data.openalex.affiliations.world_regions"
            ),
        )
        request = self.client.update_by_query.call_args.kwargs
        self.assertEqual(
            request["params"],
            {
                "conflicts": "proceed",
                "refresh": "false",
                "wait_for_completion": "false",
                "slices": "auto",
                "scroll_size": 1000,
                "requests_per_second": 500,
            },
        )
        self.assertIn(
            "must_not",
            request["body"]["query"]["bool"]["should"][0]["bool"],
        )

    def test_rejects_index_without_world_region_mapping(self):
        self.client.indices.get_field_mapping.return_value = {
            "silver": {
                "mappings": {},
            },
        }

        with self.assertRaisesMessage(
            RuntimeError,
            "Aplique docs/Regioes-do-mundo.md antes do backfill.",
        ):
            apply_world_regions(
                self.client,
                "silver",
                "auto",
                1000,
                500,
                False,
            )

        self.client.update_by_query.assert_not_called()
