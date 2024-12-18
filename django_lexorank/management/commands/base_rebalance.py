import typing
from django.utils import timezone
from django.core.management.base import BaseCommand, CommandError

from django_lexorank.models import ScheduledRebalancing, RankedModel

class Command(BaseCommand):
    help = "Scan the ScheduledRebalancing in Database to run"
    model: typing.Optional[RankedModel] = None

    def resolve(self):
        now = timezone.now()
        scheduled = ScheduledRebalancing.objects.filter(scheduled_at__lte=now)
        if not scheduled.exists():
            return
        for item in scheduled:
            self.model.rebalance_by_scheduled(item.with_respect_to)

    def handle(self, *args, **options):
        self.resolve()
