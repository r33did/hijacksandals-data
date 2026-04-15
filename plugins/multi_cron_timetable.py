# This Plugin Created by munawarimam
# Date: 2026-02-03 13:38:00
# This plugin can help to set multiple cron on single dag

from __future__ import annotations

from datetime import datetime as dt_datetime
from typing import Any

import pendulum
from croniter import croniter
from pendulum import DateTime

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable


class MultiCronTimetable(Timetable):

    def __init__(self, cron_defs: list[str], timezone: str = "UTC") -> None:
        self.cron_defs = cron_defs
        self.timezone = timezone

    def serialize(self) -> dict[str, Any]:
        return {"cron_defs": self.cron_defs, "timezone": self.timezone}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> MultiCronTimetable:
        return cls(cron_defs=data["cron_defs"], timezone=data["timezone"])

    @property
    def summary(self) -> str:
        return " | ".join(self.cron_defs)

    def _get_next(self, current: DateTime) -> DateTime:
        tz = pendulum.timezone(self.timezone)
        ref = current.in_tz(tz)
        candidates = []
        for expr in self.cron_defs:
            nxt = croniter(expr, ref).get_next(dt_datetime)
            candidates.append(pendulum.instance(nxt, tz=tz))
        return min(candidates)

    def _get_prev(self, current: DateTime) -> DateTime:
        tz = pendulum.timezone(self.timezone)
        ref = current.in_tz(tz)
        candidates = []
        for expr in self.cron_defs:
            prev = croniter(expr, ref).get_prev(dt_datetime)
            candidates.append(pendulum.instance(prev, tz=tz))
        return max(candidates)

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        prev = self._get_prev(run_after)
        return DataInterval(start=prev, end=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is None:
            earliest = restriction.earliest
            if earliest is None:
                return None
            start = earliest
        else:
            start = last_automated_data_interval.end

        end = self._get_next(start)

        if restriction.latest is not None and end > restriction.latest:
            return None

        return DagRunInfo.interval(start=start, end=end)


class MultiCronTimetablePlugin(AirflowPlugin):
    name = "multi_cron_timetable"
    timetables = [MultiCronTimetable]