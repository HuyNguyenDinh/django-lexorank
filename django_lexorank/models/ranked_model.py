from typing import Optional, Type, List, Dict

from django.contrib import admin
from django.db import models, transaction
from django.db.models import CharField
from django.db.models.functions import Length
from django.forms.models import model_to_dict

from ..fields import RankField
from ..lexorank import LexoRank, RebalancingRequiredException
from ..managers import RankedModelManager
from .scheduled_rebalancing import ScheduledRebalancing

CharField.register_lookup(Length, "length")


class RankedModel(models.Model):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__initial_values = model_to_dict(self)

    objects = RankedModelManager()

    rank = RankField()
    order_with_respect_to: Optional[List[str]] = None
    SEPARATOR = ":"

    class Meta:
        abstract = True
        ordering = ["rank"]

    @classmethod
    def from_db(cls, db, field_names, values):
        instance = super().from_db(db, field_names, values)
        instance._state.adding = False
        instance._state.db = db
        instance._initial_values = dict(zip(field_names, values))
        return instance

    def field_value_has_changed(self, field: str) -> bool:
        if not self.pk or not self.__initial_values:
            return False

        if getattr(self, field) != self.__initial_values[field]:
            return True

        return False

    @transaction.atomic
    def save(self, *args, **kwargs) -> None:
        # if self.order_with_respect_to:
        #     if self.field_value_has_changed(self.order_with_respect_to):
        #         self.rank = None  # type: ignore[assignment]

        super().save(*args, **kwargs)

        if self.rebalancing_required():
            self.schedule_rebalancing()
            # self.rebalance()

    def _model(self) -> Type[models.Model]:
        return self._meta.model

    @property
    def _with_respect_to_kwargs(self) -> dict:
        if not self.order_with_respect_to:
            return {}
        result = {}
        for field in self.order_with_respect_to:
            result.update({field: getattr(self, field)})
        return result

    @property
    def _with_respect_to_value(self) -> str:
        if self.order_with_respect_to:
            result = tuple(list(map(lambda x: str(getattr(self, x).pk), self.order_with_respect_to)))
            return result
        return tuple()
    
    @property   
    def _with_respect_to_value_key(self) -> str:
        """
            Convert dictionary to string for hashable
        """
        return self.SEPARATOR.join(self._with_respect_to_value)
    
    @classmethod
    def reverse_with_respect_to_value_key(cls, key: str) -> Dict:
        """
            Re-Convert hashable string to dictionary
        """
        values = key.split(cls.SEPARATOR)
        result = {}
        idx = 0
        for respect_to_field in cls.order_with_respect_to:
            result[respect_to_field] = values[idx]
            idx += 1
        return result

    @property
    def _objects_count(self):
        return self._model.objects.filter(**self._with_respect_to_kwargs).count()

    def _move_to(self, rank: str) -> "RankedModel":
        self.rank = rank  # type: ignore[assignment]
        self.save(update_fields=["rank"])
        return self

    def place_on_top(self) -> "RankedModel":
        """Place object at the top of the list."""
        first_object = self.get_first_object()
        _, rank, _ = self._wrapper_get_lexorank_in_between(before_obj=None, after_obj=first_object)

        return self._move_to(rank)

    def place_on_bottom(self) -> "RankedModel":
        """Place object at the bottom of the list."""
        last_object = self.get_last_object()
        _, rank, _ = self._wrapper_get_lexorank_in_between(before_obj=last_object, after_obj=None)

        return self._move_to(rank)

    def place_after(self, after_obj: "RankedModel") -> "RankedModel":
        """Place object after selected one."""
        next_obj = after_obj.get_next_object()
        _, rank, after_obj = self._wrapper_get_lexorank_in_between(before_obj=after_obj, after_obj=next_obj)
        return self._move_to(rank)

    def place_before(self, before_obj: "RankedModel") -> "RankedModel":
        """Place object before selected one."""
        previous_obj = before_obj.get_previous_object()
        before_obj, rank, _ = self._wrapper_get_lexorank_in_between(before_obj=previous_obj, after_obj=before_obj)

        return self._move_to(rank)
    
    def place_between(self, before_obj: Optional["RankedModel"], after_obj: Optional["RankedModel"]):
        before_obj, rank, after_obj = self._wrapper_get_lexorank_in_between(before_obj=before_obj, after_obj=after_obj)
        previous_rank = getattr(before_obj, "rank", None)
        next_rank = getattr(after_obj, "rank", None)
        filter_kwargs = {**self._with_respect_to_kwargs}
        if previous_rank:
            filter_kwargs.update(rank__gt=previous_rank)
        if next_rank:
            filter_kwargs.update(rank__lt=next_rank)
        if self.queryset_wrapper(**filter_kwargs).exists():
            raise Exception("Not valid rank")
        return self._move_to(rank)
    
    def _wrapper_get_lexorank_in_between(self, before_obj: Optional["RankedModel"], after_obj: Optional["RankedModel"]):
        previous_rank = getattr(before_obj, "rank", None)
        next_rank = getattr(after_obj, "rank", None)
        try:
            rank = LexoRank.get_lexorank_in_between(
                previous_rank=previous_rank,
                next_rank=next_rank,
                objects_count=self._objects_count,
            )
        except RebalancingRequiredException:
            self.rebalance()
            if before_obj:
                before_obj.refresh_from_db()
                previous_rank = getattr(before_obj, "rank", None)
            if after_obj:
                after_obj.refresh_from_db()
                next_rank = getattr(after_obj, "rank", None)
            rank = LexoRank.get_lexorank_in_between(
                previous_rank=previous_rank,
                next_rank=next_rank,
                objects_count=self._objects_count,
            )
        except Exception as e:
            raise e
        else:
            return before_obj, rank, after_obj

    @classmethod
    def queryset_wrapper(cls, **kwargs):
        return cls.objects.filter(**kwargs)

    def get_previous_object(self) -> Optional["RankedModel"]:
        """
        Return object that precedes provided object,
        or None if provided object is the first.
        """
        return (
            self._model.objects.filter(
                rank__lt=self.rank, **self._with_respect_to_kwargs
            )
            .order_by("-rank")
            .first()
        )

    def get_previous_object_rank(self) -> Optional[str]:
        """
        Return object rank that precedes provided object,
        or None if provided object is the first.
        """
        previous_object = self.get_previous_object()
        return previous_object.rank if previous_object else None

    def get_next_object(self) -> Optional["RankedModel"]:
        """
        Return object that follows provided object,
        or None if provided object is the last.
        """
        return (
            self._model.objects.filter(
                rank__gt=self.rank, **self._with_respect_to_kwargs
            )
            .order_by("rank")
            .first()
        )

    def get_next_object_rank(self) -> Optional[str]:
        """
        Return object rank that follows provided object,
        or None if provided object is the last.
        """
        next_object = self.get_next_object()
        return next_object.rank if next_object else None
    
    @classmethod
    def rebalance_by_scheduled(cls, with_respect_to_value_key):
        """
            Convenient method to run rebalance from ScheduledRebalancing
        """
        filter_kwargs = cls.reverse_with_respect_to_value_key(with_respect_to_value_key)
        object_count = cls.objects.filter(**filter_kwargs).count()
        with transaction.atomic():
            qs = (
                cls.objects.filter(**filter_kwargs)
                .order_by("rank")
                .select_for_update()
            )

            objects_to_update = []

            rank = LexoRank.get_min_rank(
                objects_count=object_count,
            )
            for obj in qs:
                rank = LexoRank.increment_rank(
                    rank=rank,
                    objects_count=object_count,
                )
                obj.rank = rank
                objects_to_update.append(obj)
            cls.objects.bulk_update(objects_to_update, ["rank"])
        cls.post_rebalance_by_scheduled(objects_to_update)

    def rebalance(self) -> "RankedModel":
        """Rebalance ranks of all objects."""
        with transaction.atomic():
            qs = (
                self._model.objects.filter(**self._with_respect_to_kwargs)
                .order_by("rank")
                .select_for_update()
            )

            objects_to_update = []

            rank = LexoRank.get_min_rank(
                objects_count=self._objects_count,
            )
            for obj in qs:
                rank = LexoRank.increment_rank(
                    rank=rank,
                    objects_count=self._objects_count,
                )
                obj.rank = rank
                objects_to_update.append(obj)

            self._model.objects.bulk_update(objects_to_update, ["rank"])

        self.refresh_from_db()
        self.post_rebalance()
        return self
    
    def post_rebalance(self):
        """
            This method run at the signal after rebalance
        """
        pass

    @classmethod
    def post_rebalance_by_scheduled(cls, list_object_updated):
        """
            This method run at the signal after rebalance by SchduledRebalancing
        """
        pass

    def rebalancing_required(self) -> bool:
        """
        Return `True` if any object has rank length greater than 128, `False` otherwise.
        """
        return self._model.objects.filter(
            rank__length__gte=LexoRank.rebalancing_length,
            **self._with_respect_to_kwargs
        ).exists()

    def rebalancing_scheduled(self) -> bool:
        """
        Return `True` if rebalancing was scheduled for a list that includes that object,
        `False` otherwise.
        """
        return ScheduledRebalancing.objects.filter(
            model=self._meta.model_name,
            with_respect_to=self._with_respect_to_value_key,
        ).exists()

    @classmethod
    def get_first_object(cls, with_respect_to_kwargs: dict) -> Optional["RankedModel"]:
        """Return the first object if exists.."""
        if cls.order_with_respect_to and not with_respect_to_kwargs:
            raise ValueError("with_respect_to_kwargs must be provided")

        return cls.objects.filter(**with_respect_to_kwargs).order_by("rank").first()

    @classmethod
    def get_first_object_rank(cls, with_respect_to_kwargs: dict) -> Optional[str]:
        """Return the rank of the first object or None if no objects exist."""
        if cls.order_with_respect_to and not with_respect_to_kwargs:
            raise ValueError("with_respect_to_kwargs must be provided")

        first_object = cls.get_first_object(
            with_respect_to_kwargs=with_respect_to_kwargs
        )
        return first_object.rank if first_object else None

    @classmethod
    def get_last_object(cls, with_respect_to_kwargs: dict) -> Optional["RankedModel"]:
        """Return the last object if exists."""
        if cls.order_with_respect_to and not with_respect_to_kwargs:
            raise ValueError("with_respect_to_kwargs must be provided")

        return cls.objects.filter(**with_respect_to_kwargs).order_by("-rank").first()

    @classmethod
    def get_last_object_rank(cls, with_respect_to_kwargs: dict) -> Optional[str]:
        """Return the rank of the last object or None if no objects exist."""
        if cls.order_with_respect_to and not with_respect_to_kwargs:
            raise ValueError("with_respect_to_kwargs must be provided")

        last_object = cls.get_last_object(with_respect_to_kwargs=with_respect_to_kwargs)
        return last_object.rank if last_object else None

    def schedule_rebalancing(self):
        ScheduledRebalancing.objects.update_or_create(
            model=self._meta.model_name,
            with_respect_to=self._with_respect_to_value_key,
        )
