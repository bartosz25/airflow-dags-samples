from __future__ import annotations

import logging
from datetime import timedelta

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable, DataInterval, DagRunInfo, TimeRestriction
from pendulum import DateTime, UTC, Time


class EndOfScheduleTimetable(Timetable):

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        start_day = run_after.at(0, 0, 0)
        end_day = start_day + timedelta(days=1)
        return DataInterval(start=start_day, end=end_day)

    def next_dagrun_info(self, *, last_automated_data_interval: DataInterval | None, restriction: TimeRestriction,) \
            -> DagRunInfo | None:
        # Let's first handle the case of the first DAG run
        logging.info(f'last_automated_data_interval={last_automated_data_interval}')
        if last_automated_data_interval is None:
            # I'm simplifying here. Ideally, you should deal with the missing start_date or disabled catch-up
            # otherwise than raising an exception
            if restriction.earliest is None or not restriction.catchup:
                raise Exception("EndOfScheduleTimetable doesn't support missing start_date and disabled catch-up")

            start_date_time = DateTime.combine(restriction.earliest.date(), Time.min).replace(tzinfo=UTC)
        else:
            next_schedule = last_automated_data_interval.start + timedelta(days=1)
            start_date_time = DateTime.combine(next_schedule, Time.min).replace(tzinfo=UTC)

        end_date_time = DateTime.combine(start_date_time, Time.min).replace(tzinfo=UTC)
        return DagRunInfo.interval(start=start_date_time, end=end_date_time.replace(minute=1))



class EndOfScheduleTimetablePlugin(AirflowPlugin):
    name = "end_of_schedule_timetable_plugin"
    timetables = [EndOfScheduleTimetable]

