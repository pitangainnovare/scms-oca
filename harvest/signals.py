import logging

from django.db.models.signals import post_delete, post_save, pre_save
from django.dispatch import receiver

from .indexing import (
    get_index_name,
    delete_harvested_document,
    index_harvested_instance,
)
from .models import HarvestedArticle, HarvestedBook, HarvestedPreprint, HarvestedSciELOData, IndexStatus

logger = logging.getLogger(__name__)


def _store_previous_raw_data(instance):
    if not instance.pk:
        instance._raw_data_before = None
        return

    instance._raw_data_before = (
        instance.__class__.objects.filter(pk=instance.pk)
        .values_list("raw_data", flat=True)
        .first()
    )


def _should_index_raw_data(instance, created, update_fields):
    if not instance.raw_data:
        if created:
            logger.warning(
                f"Registro criado sem raw_data, pulando indexação ({instance.__class__.__name__}:{instance.identifier})."
            )
        return False

    if created:
        return True

    if update_fields is not None:
        return "raw_data" in update_fields

    if hasattr(instance, "_raw_data_before"):
        if instance._raw_data_before != instance.raw_data:
            return True

    if hasattr(instance, "index_status") and instance.index_status in (IndexStatus.PENDING, IndexStatus.FAILED):
        return True

    return False


def _index_if_raw_data_saved(instance, created, update_fields):
    """
    Indexa raw no OpenSearch. A transformação bronze é orquestrada por página
    na coleta / reconciliação, não no signal.
    """
    if not _should_index_raw_data(instance, created, update_fields):
        return

    index_name = get_index_name(
        model_name=instance.__class__.__name__,
        instance=instance,
    )
    index_harvested_instance(instance=instance, index_name=index_name)


@receiver(post_save, sender=HarvestedPreprint)
def index_preprint_on_save(sender, instance, created, update_fields=None, **kwargs):
    _index_if_raw_data_saved(instance, created, update_fields)


@receiver(post_save, sender=HarvestedArticle)
def index_article_on_save(sender, instance, created, update_fields=None, **kwargs):
    _index_if_raw_data_saved(instance, created, update_fields)


@receiver(post_save, sender=HarvestedBook)
def index_books_on_save(sender, instance, created, update_fields=None, **kwargs):
    _index_if_raw_data_saved(instance, created, update_fields)


@receiver(post_save, sender=HarvestedSciELOData)
def index_scielo_data_on_save(sender, instance, created, update_fields=None, **kwargs):
    _index_if_raw_data_saved(instance, created, update_fields)


@receiver(pre_save, sender=HarvestedPreprint)
def track_preprint_raw_data(sender, instance, **kwargs):
    _store_previous_raw_data(instance)


@receiver(pre_save, sender=HarvestedArticle)
def track_article_raw_data(sender, instance, **kwargs):
    _store_previous_raw_data(instance)


@receiver(pre_save, sender=HarvestedBook)
def track_books_raw_data(sender, instance, **kwargs):
    _store_previous_raw_data(instance)


@receiver(post_delete, sender=HarvestedBook)
def delete_books_on_delete(sender, instance, **kwargs):
    index_name = get_index_name(model_name=instance.__class__.__name__)
    if not index_name:
        return
    delete_harvested_document(
        model_name=instance.__class__.__name__,
        identifier=instance.identifier,
    )


@receiver(pre_save, sender=HarvestedSciELOData)
def track_scielo_data_raw_data(sender, instance, **kwargs):
    _store_previous_raw_data(instance)
