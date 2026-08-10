from django.db import migrations, models


def update_transformation_script_harvest_model(apps, schema_editor):
    TransformationScript = apps.get_model("harvest", "TransformationScript")
    TransformationScript.objects.filter(harvest_model="HarvestedBooks").update(
        harvest_model="HarvestedBook"
    )


def revert_transformation_script_harvest_model(apps, schema_editor):
    TransformationScript = apps.get_model("harvest", "TransformationScript")
    TransformationScript.objects.filter(harvest_model="HarvestedBook").update(
        harvest_model="HarvestedBooks"
    )


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0004_harvestedarticle_harvesterrorlogarticle_and_more"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="HarvestedBooks",
            new_name="HarvestedBook",
        ),
        migrations.RenameModel(
            old_name="HarvestErrorLogBooks",
            new_name="HarvestErrorLogBook",
        ),
        migrations.AlterModelOptions(
            name="harvestedbook",
            options={
                "verbose_name": "Dados de Scielo Book",
                "verbose_name_plural": "Dados de Scielo Books",
            },
        ),
        migrations.RunPython(
            update_transformation_script_harvest_model,
            revert_transformation_script_harvest_model,
        ),
        migrations.AlterField(
            model_name="transformationscript",
            name="harvest_model",
            field=models.CharField(
                blank=True,
                choices=[
                    ("HarvestedArticle", "Article"),
                    ("HarvestedPreprint", "Preprint"),
                    ("HarvestedBook", "Book"),
                    ("HarvestedSciELOData_dataset", "SciELO Data - Dataset"),
                    ("HarvestedSciELOData_dataverse", "SciELO Data - Dataverse"),
                ],
                db_index=True,
                help_text="Modelo de coleta associado a este script para transformação automática",
                max_length=50,
                null=True,
                verbose_name="Modelo de Coleta",
            ),
        ),
    ]
