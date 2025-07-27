import os

from django.db import models
from django.conf import settings
from django.utils.translation import gettext as _
from django.db.models import Count
from taggit.managers import TaggableManager
from wagtail.admin.panels import FieldPanel, HelpPanel
from wagtailautocomplete.edit_handlers import AutocompletePanel

from core.models import CommonControlField
from institution.models import Institution
from location.models import Location
from usefulmodels.models import Action, Practice, ThematicArea, State

from . import choices
from .forms import EventDirectoryFileForm, EventDirectoryForm
from .permission_helper import MUST_BE_MODERATE
from core import help_fields


def get_default_action():
    try:
        return Action.objects.get(name__icontains="disseminação")
    except Action.DoesNotExist:
        return None


class EventDirectory(CommonControlField):
    class Meta:
        verbose_name_plural = _("Dissemination Data")
        verbose_name = _("Dissemination Data")
        permissions = (
            (MUST_BE_MODERATE, _("Must be moderated")),
            ("can_edit_title", _("Can edit title")),
            ("can_edit_link", _("Can edit link")),
            ("can_edit_description", _("Can edit description")),
            ("can_edit_start_date", _("Can edit start_date")),
            ("can_edit_end_date", _("Can edit end_date")),
            ("can_edit_start_time", _("Can edit start_time")),
            ("can_edit_end_time", _("Can edit end_time")),
            ("can_edit_locations", _("Can edit locations")),
            ("can_edit_institutions", _("Can edit institutions")),
            ("can_edit_thematic_areas", _("Can edit thematic_areas")),
            ("can_edit_practice", _("Can edit practice")),
            ("can_edit_classification", _("Can edit classification")),
            ("can_edit_keywords", _("Can edit keywords")),
            ("can_edit_attendance", _("Can edit attendance")),
            ("can_edit_record_status", _("Can edit record_status")),
            ("can_edit_source", _("Can edit source")),
            (
                "can_edit_institutional_contribution",
                _("Can edit institutional_contribution"),
            ),
            ("can_edit_notes", _("Can edit notes")),
        )

    title = models.CharField(
        _("Title"),
        max_length=255,
        null=False,
        blank=False,
        help_text=help_fields.DIRECTORY_TITLE_HELP,
    )
    link = models.URLField(
        _("Link"), null=False, blank=False, help_text=help_fields.DIRECTORY_LINK_HELP
    )
    description = models.TextField(
        _("Description"),
        max_length=1000,
        null=True,
        blank=True,
        help_text=help_fields.DIRECTORY_DESCRIPTION_HELP,
    )
    start_date = models.DateField(
        _("Start Date"), max_length=255, null=True, blank=True
    )
    end_date = models.DateField(_("End Date"), max_length=255, null=True, blank=True)
    start_time = models.TimeField(
        _("Start Time"), max_length=255, null=True, blank=True
    )
    end_time = models.TimeField(_("End Time"), max_length=255, null=True, blank=True)

    locations = models.ManyToManyField(
        Location, verbose_name=_("Location"), blank=False
    )
    organization = models.ManyToManyField(
        Institution,
        verbose_name=_("Instituição"),
        blank=True,
        help_text=_("Instituições responsáveis pela organização do evento."),
    )
    thematic_areas = models.ManyToManyField(
        ThematicArea,
        verbose_name=_("Thematic Area"),
        blank=True,
        help_text=help_fields.DIRECTORY_THEMATIC_AREA_HELP,
    )

    practice = models.ForeignKey(
        Practice,
        verbose_name=_("Practice"),
        null=True,
        blank=False,
        on_delete=models.SET_NULL,
        help_text=help_fields.DIRECTORY_PRACTICE_HELP,
    )

    action = models.ForeignKey(
        Action,
        verbose_name=_("Action"),
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        default=get_default_action,
        help_text=help_fields.DIRECTORY_ACTION_HELP,
    )
    classification = models.CharField(
        _("Classification"),
        choices=choices.classification,
        max_length=255,
        null=True,
        blank=True,
        help_text=help_fields.DIRECTORY_CLASSIFICATION_HELP,
    )
    keywords = TaggableManager(
        _("Keywords"), blank=True, help_text=help_fields.DIRECTORY_KEYWORDS_AREA_HELP
    )

    attendance = models.CharField(
        _("Attendance"),
        choices=choices.attendance_type,
        max_length=255,
        null=True,
        blank=False,
    )

    record_status = models.CharField(
        _("Record status"),
        choices=choices.status,
        max_length=255,
        null=True,
        blank=True,
        help_text=help_fields.DIRECTORY_RECORD_STATUS_HELP,
    )

    source = models.CharField(
        _("Source"),
        max_length=255,
        null=True,
        blank=True,
        help_text=help_fields.DIRECTORY_SOURCE_HELP,
    )

    institutional_contribution = models.CharField(
        _("Institutional Contribution"),
        max_length=255,
        default=settings.DIRECTORY_DEFAULT_CONTRIBUTOR,
        help_text=help_fields.DIRECTORY_INSTITUTIONAL_CONTRIBUTION_HELP,
    )

    notes = models.TextField(
        _("Notes"),
        max_length=1000,
        null=True,
        blank=True,
        help_text=help_fields.DIRECTORY_NOTES_HELP,
    )

    panels = [
        HelpPanel(
            "Encontros, congressos, workshops, seminários realizados no Brasil (presenciais, virtuais ou híbridos) cujo tema principal seja a promoção da Ciência Aberta"
        ),
        FieldPanel("title", permission="event_directory.can_edit_title"),
        FieldPanel("link", permission="event_directory.can_edit_link"),
        FieldPanel("source", permission="event_directory.can_edit_source"),
        FieldPanel("description", permission="event_directory.can_edit_description"),
        FieldPanel("institutional_contribution", permission="event_directory.can_edit_institutional_contribution"),
        AutocompletePanel("organization", permission="event_directory.can_edit_organization"),
        FieldPanel("start_date", permission="event_directory.can_edit_start_date"),
        FieldPanel("end_date", permission="event_directory.can_edit_end_date"),
        FieldPanel("start_time", permission="event_directory.can_edit_start_time"),
        FieldPanel("end_time", permission="event_directory.can_edit_end_time"),
        FieldPanel("attendance", permission="event_directory.can_edit_attendance"),
        AutocompletePanel("locations", permission="event_directory.can_edit_locations"),
        AutocompletePanel("thematic_areas", permission="event_directory.can_edit_thematic_areas"),
        FieldPanel("keywords", permission="event_directory.can_edit_keywords"),
        FieldPanel("classification", permission="event_directory.can_edit_classification"),
        FieldPanel("practice", permission="event_directory.can_edit_practice"),
        FieldPanel("record_status", permission="event_directory.can_edit_record_status"),
        FieldPanel("notes", permission="event_directory.can_edit_notes"),
    ]

    def __unicode__(self):
        return "%s" % self.title

    def __str__(self):
        return "%s" % self.title

    def get_absolute_edit_url(self):
        return f"/event_directory/eventdirectory/edit/{self.id}/"

    @property
    def data(self):
        d = {
            "event__title": self.title,
            "event__link": self.link,
            "event__description": self.description,
            "event__start_date": self.start_date.isoformat()
            if self.start_date
            else None,
            "event__end_date": self.end_date.isoformat() if self.end_date else None,
            "event__start_time": self.start_time.isoformat()
            if self.start_time
            else None,
            "event__end_time": self.end_time.isoformat() if self.end_time else None,
            "event__classification": self.classification,
            "event__keywords": [keyword for keyword in self.keywords.names()],
            "event__attendance": self.attendance,
            "event__record_status": self.record_status,
            "event__source": self.source,
            "event__institutional_contribution": self.institutional_contribution,
            "event__action": self.action.name,
            "event__practice": self.practice.name,
        }
        if self.locations:
            loc = []
            for location in self.locations.iterator():
                loc.append(location.data)
            d.update({"event__locations": loc})

        if self.organization:
            org_list = []
            for org in self.organization.iterator():
                org_list.append(org.data)
            d.update({"event__organization": org_list})

        if self.thematic_areas:
            area = []
            for thematic_area in self.thematic_areas.iterator():
                area.append(thematic_area.data)
            d.update({"event__thematic_areas": area})

        if self.practice:
            d.update(self.practice.data)

        if self.action:
            d.update(self.action.data)

        return d

    @classmethod
    def filter_items_to_generate_indicators(
        cls,
        action__name=None,
        practice__code=None,
        practice__name=None,
        classification=None,
        institution__name=None,
        thematic_area__level0=None,
        thematic_area__level1=None,
        location__state__code=None,
        location__state__region=None,
    ):
        params = dict(
            action__name=action__name,
            practice__code=practice__code,
            practice__name=practice__name,
            classification=classification,
            organization__name=institution__name,
            organization__location__state__acronym=location__state__code,
            organization__location__state__region=location__state__region,
            thematic_areas__level0=thematic_area__level0,
            thematic_areas__level1=thematic_area__level1,
            locations__state__acronym=location__state__code,
            locations__state__region=location__state__region,
        )
        params = {k: v for k, v in params.items() if v}
        return cls.objects.filter(record_status="PUBLISHED", **params)

    @classmethod
    def parameters_for_values(
        cls,
        by_practice=False,
        by_classification=False,
        by_institution=False,
        by_thematic_area_level0=False,
        by_thematic_area_level1=False,
        by_state=False,
        by_region=False,
    ):
        selected_attributes = Action.parameters_for_values("action")
        if by_classification:
            selected_attributes += ["classification"]
        if by_practice:
            selected_attributes += Practice.parameters_for_values("practice")
        if by_institution:
            selected_attributes += Institution.parameters_for_values("organization")
        if by_state or by_region:
            selected_attributes += State.parameters_for_values(
                "locations__state", by_state, by_state, by_region
            )
            selected_attributes += State.parameters_for_values(
                "organization__location__state", by_state, by_state, by_region
            )
        if by_thematic_area_level0 or by_thematic_area_level1:
            selected_attributes += ThematicArea.parameters_for_values(
                "thematic_areas", by_thematic_area_level0, by_thematic_area_level1
            )
        return selected_attributes

    @classmethod
    def group(
        cls,
        query_result,
        selected_attributes,
    ):
        return (
            query_result.values(*selected_attributes)
            .annotate(count=Count("id"))
            .order_by("count")
            .iterator()
        )

    def get_title(self):
        return self.title
    get_title.short_description = "Title"

    def get_link(self):
        return self.link
    get_link.short_description = "Link"

    def get_description(self):
        return self.description
    get_description.short_description = "Description"

    def get_start_date(self):
        return self.start_date
    get_start_date.short_start_date = "Start Date"

    def get_end_date(self):
        return self.end_date
    get_end_date.short_end_date = "End Date"

    def get_start_time(self):
        return self.start_time
    get_start_time.short_start_time = "Start Time"

    def get_end_time(self):
        return self.end_time
    get_end_time.short_end_time = "End Time"

    def get_thematic_areas_level0(self):
        return "| ".join([t.level0 for t in self.thematic_areas.all()])
    get_thematic_areas_level0.short_description = "Thematic Area Level0"
    
    def get_thematic_areas_level1(self):
        return "| ".join([t.level1 for t in self.thematic_areas.all()])
    get_thematic_areas_level1.short_description = "Thematic Area Level1"
    
    def get_thematic_areas_level2(self):
        return "| ".join([t.level2 for t in self.thematic_areas.all()])
    get_thematic_areas_level2.short_description = "Thematic Area Level2"

    def get_keywords(self):
        return "| ".join([t.name for t in self.keywords.all()])
    get_keywords.short_description = "Keywords"
    
    def get_classification(self):
        return self.classification 
    get_classification.short_description = "Classification"
    
    def get_practice(self):
        return self.practice
    get_practice.short_description = "Practice"
    
    def get_action(self):
        return self.action
    get_action.short_description = "Action"
    
    def get_source(self):
        return self.source
    get_source.short_description = "Source"


    base_form_class = EventDirectoryForm


class EventDirectoryFile(CommonControlField):
    class Meta:
        verbose_name_plural = _("Dissemination Data Upload")

    attachment = models.ForeignKey(
        "wagtaildocs.Document",
        verbose_name=_("Attachement"),
        null=True,
        blank=False,
        on_delete=models.SET_NULL,
        related_name="+",
    )
    is_valid = models.BooleanField(_("Is valid?"), default=False, blank=True, null=True)
    line_count = models.IntegerField(
        _("Number of lines"), default=0, blank=True, null=True
    )

    def filename(self):
        return os.path.basename(self.attachment.name)

    panels = [FieldPanel("attachment")]
    base_form_class = EventDirectoryFileForm
