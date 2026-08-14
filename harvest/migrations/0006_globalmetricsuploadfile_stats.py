from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("harvest", "0005_rename_harvestedbooks_to_harvestedbook"),
    ]

    operations = [
        migrations.AddField(
            model_name="globalmetricsuploadfile",
            name="stats",
            field=models.JSONField(
                blank=True,
                default=dict,
                help_text="Estatísticas da última aplicação das métricas no índice silver.",
                verbose_name="Estatísticas",
            ),
        ),
    ]
