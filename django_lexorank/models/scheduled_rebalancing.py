from django.db import models

class ScheduledRebalancing(models.Model):
    model = models.CharField(max_length=255)
    with_respect_to = models.CharField(
        default="", max_length=255, blank=True, help_text="_with_respect_to_value_key of the respected object."
    )
    scheduled_at = models.DateTimeField(auto_now_add=True)
