from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from search_gateway.client import get_opensearch_client

RAW_INDEX_BODY = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        "properties": {
            "oca_indexed_at": {"type": "date"},
            "oca_source_hash": {"type": "text"},
            "raw_data": {"type": "object", "enabled": False},
        }
    },
}

RAW_INDICES = {
    "article": ("HarvestedArticle", "OS_INDEX_RAW_ARTICLE"),
    "preprint": ("HarvestedPreprint", "OS_INDEX_RAW_PREPRINT"),
    "books": ("HarvestedBooks", "OS_INDEX_RAW_BOOK"),
    "dataset": ("HarvestedSciELOData(dataset)", "OS_INDEX_RAW_SCIELO_DATA_DATASET"),
    "dataverse": (
        "HarvestedSciELOData(dataverse)",
        "OS_INDEX_RAW_SCIELO_DATA_DATAVERSE",
    ),
}


class Command(BaseCommand):
    help = (
        "Cria os índices raw no OpenSearch (article, preprint, books, scielo data). "
        "Use --index article para criar só o índice de artigos."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--index",
            choices=list(RAW_INDICES.keys()),
            help="Cria o índice raw para o tipo escolhido. Padrão: todos.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Se o índice já existir, remove e recria.",
        )

    def handle(self, *args, **options):
        client = get_opensearch_client()
        if client is None:
            raise CommandError("OpenSearch client não configurado.")

        index_choice = options["index"]
        selected = (
            {index_choice: RAW_INDICES[index_choice]}
            if index_choice
            else RAW_INDICES
        )
        force = options["force"]

        for key, (model_name, setting_name) in selected.items():
            index_name = getattr(settings, setting_name, None)
            if not index_name:
                self.stdout.write(
                    self.style.WARNING(
                        f"{model_name}: índice não configurado ({setting_name}), pulando."
                    )
                )
                continue

            exists = client.indices.exists(index=index_name)
            if exists and force:
                self.stdout.write(
                    self.style.WARNING(f"{model_name}: removendo índice {index_name}.")
                )
                client.indices.delete(index=index_name)
                exists = False

            if exists:
                self.stdout.write(
                    self.style.NOTICE(f"{model_name}: índice já existe: {index_name}.")
                )
                continue

            client.indices.create(index=index_name, body=RAW_INDEX_BODY)
            self.stdout.write(
                self.style.SUCCESS(f"{model_name}: índice criado: {index_name}.")
            )
