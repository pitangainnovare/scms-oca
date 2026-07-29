import json

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from etl.world_regions import apply_world_regions
from search_gateway.client import get_opensearch_client


class Command(BaseCommand):
    help = "Aplica as sub-regiões geográficas UN M49 aos documentos silver."

    def add_arguments(self, parser):
        parser.add_argument(
            "--index",
            default=settings.ETL_PUBLIC_ALIAS,
            help="Índice ou alias silver de destino.",
        )
        parser.add_argument(
            "--slices",
            default="auto",
            help="Quantidade de slices paralelos ou 'auto'.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Documentos processados por lote.",
        )
        parser.add_argument(
            "--requests-per-second",
            type=float,
            default=500,
            help="Limite global de documentos processados por segundo.",
        )
        parser.add_argument(
            "--repair",
            action="store_true",
            help="Recalcula também documentos que já possuem região.",
        )
        parser.add_argument(
            "--task-id",
            help="Consulta uma execução existente.",
        )
        parser.add_argument(
            "--cancel",
            action="store_true",
            help="Cancela a execução indicada por --task-id.",
        )

    def handle(self, *args, **options):
        client = get_opensearch_client()

        if client is None:
            raise CommandError("Cliente OpenSearch não configurado.")

        task_id = options["task_id"]

        if options["cancel"]:
            if not task_id:
                raise CommandError("--cancel exige --task-id.")

            result = client.tasks.cancel(task_id=task_id)
            self.stdout.write(json.dumps(result, indent=2))
            return

        if task_id:
            result = client.tasks.get(task_id=task_id)
            self.stdout.write(json.dumps(result, indent=2))
            return

        slices = options["slices"]

        if slices != "auto":
            try:
                slices = int(slices)
            except ValueError as error:
                raise CommandError("--slices deve ser 'auto' ou um inteiro.") from error

            if slices <= 0:
                raise CommandError("--slices deve ser maior que zero.")

        if options["batch_size"] <= 0:
            raise CommandError("--batch-size deve ser maior que zero.")

        if options["requests_per_second"] <= 0:
            raise CommandError("--requests-per-second deve ser maior que zero.")

        try:
            result = apply_world_regions(
                client,
                options["index"],
                slices,
                options["batch_size"],
                options["requests_per_second"],
                options["repair"],
            )
        except Exception as error:
            raise CommandError(str(error)) from error

        task_id = result.get("task")

        if not task_id:
            raise CommandError("O OpenSearch não retornou o identificador da task.")

        self.stdout.write(
            self.style.SUCCESS(
                f"Backfill iniciado. Task: {task_id}\n"
                "Consulte com: "
                "python manage.py apply_world_regions_to_silver "
                f"--task-id {task_id}"
            )
        )
