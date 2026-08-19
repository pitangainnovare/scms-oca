import logging

from harvest.bronze_transform import transform_indexed_page
from harvest.exception_logs import ExceptionContext
from harvest.models import HarvestedPreprint, HarvestErrorLogPreprint
from harvest.parse_info_oai_pmh import get_info_article

NODES = [
    "title",
    "subject",
    "identifier",
    "rights",
    "publisher",
    "description",
    "publisher",
    "relation",
    "type",
]


def harvest_preprint(recs, user):
    page_ids = []
    for rec in recs:
        if not rec.header.identifier:
            continue
        logging.info(
            f"Colletando preprint: {rec.header.identifier}. "
            f"datestamp: {rec.header.datestamp}"
        )
        harvested_obj, _ = HarvestedPreprint.objects.get_or_create(
            identifier=rec.header.identifier,
            creator=user,
        )
        harvested_obj.mark_as_in_progress()
        exc_context = ExceptionContext(
            harvest_object=harvested_obj,
            log_model=HarvestErrorLogPreprint,
            fk_field="preprint",
        )
        article_info = get_info_article(rec, exc_context, nodes=NODES)
        harvested_obj.set_attrs_from_article_info(
            article_info=article_info,
            datestamp=rec.header.datestamp,
        )
        exc_context.save_to_db()
        exc_context.mark_status_harvest()
        if harvested_obj.is_indexed():
            page_ids.append(harvested_obj.identifier)

    if page_ids:
        transform_indexed_page("HarvestedPreprint", page_ids)
