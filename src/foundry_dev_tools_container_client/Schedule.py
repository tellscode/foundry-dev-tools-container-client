from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import pytz
import re

class Schedule:

    def __init__(self, refreshs: list[dict], buffer: timedelta = timedelta(minutes=120)):
        """
        Class for representing a refresh schedule for a dataset.
        Provides methods to calculate last and next pull dates based on
        the current time and the defined refresh points.

        #### Usage Example:

        ```python
        refreshs = [{
            "cycle": "monthly",  # Monthly refresh
            "day": 1,  # Day of the month to refresh
            "time": "02:00:00"
        },{
            "cycle": "weekly",
            "day": "Monday",  # Weekday to refresh on
            "time": "02:00:00"
        }],
        buffer = timedelta(minutes=60)  # Buffer time in minutes

        Schedule(refreshs, buffer)
        ```
        """
        self.refreshs: List[Refresh] = []
        self.buffer = buffer

        for refresh in refreshs:
            self.refreshs.append(Refresh.init_by_object(refresh))


    def get_latest_refresh(self, date: datetime = datetime.now(pytz.UTC), buffer_overwrite: timedelta = None) -> datetime:
        """
        Get the latest refresh datetime based on the current time.
        
        Args:
            date: Current datetime (default: now)
            buffer_overwrite: Optional timedelta to overwrite self.buffer
            
        Returns:
            Latest refresh datetime
        """
        if not self.refreshs:
            raise ValueError("No valid refresh points provided in the schedule.")

        buffer = buffer_overwrite if buffer_overwrite else self.buffer

        return max([refresh.get_latest_refresh(date) for refresh in self.refreshs]) + buffer


    def get_next_refresh(self, date: datetime = datetime.now(pytz.UTC), buffer_overwrite: timedelta = None) -> datetime:
        """
        Get the next refresh datetime based on the current time.
        
        Args:
            date: Current datetime (default: now)
            buffer_overwrite: Optional timedelta to overwrite self.buffer
            
        Returns:
            Next refresh datetime
        """
        if not self.refreshs:
            raise ValueError("No valid refresh points provided in the schedule.")
        
        buffer = buffer_overwrite if buffer_overwrite else self.buffer

        return min([refresh.get_next_refresh(date) for refresh in self.refreshs]) + buffer


class Refresh:

    padding = {  # ± for next refresh calculation, also used for checking if self.cycle is valid
        "weekly": timedelta(days=10),
        "monthly": timedelta(days=40)
    }

    weekday_mapping = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6
    }

    def __init__(self, cycle: str, day: str|int, time: str):
        """
        Class for representing a single refresh point in the schedule.
        
        Args:
            cycle: "weekly" | "monthly" - TODO: add "daily", "quarterly", "yearly"
            day: Day of the month (1 to 31, -1 to -31) for monthly, weekday name for weekly
            time: Time of day in "HH:MM:SS" format
        """
        self.cycle = cycle
        self.day = day
        self.time = time

        # CYCLE CHECK
        if self.cycle not in Refresh.padding:  # check for valid cycle
            supported_cycles = "', '".join(Refresh.padding.keys())
            raise ValueError(f"Unsupported cycle '{self.cycle}', supported cycles are: '{supported_cycles}'")
        
        # DAY CHECK
        if self.day == 0:
            raise ValueError("Day cannot be 0, please use a positive or negative integer resembling the n-th or -n-th day of the month.")

        if self.cycle == "weekly":
            if not isinstance(self.day, str):  # check for valid type for cycle weekly
                raise ValueError(f"Unsupported type {type(self.day)} for 'day', expects <class 'str'> for weekly cycles.")

            if self.day.lower() not in Refresh.weekday_mapping:  # check for valid weekday name
                supported_weekdays = "', '".join(Refresh.weekday_mapping.keys())
                raise ValueError(f"Unsupported weekday '{self.day}', supported weekdays are: '{supported_weekdays}'")
        
        elif not isinstance(self.day, int):  # check for valid type for cycle not weekly
            raise ValueError(f"Unsupported type {type(self.day)} for 'day', expects <class 'int'> for {self.cycle} cycles.")
        
        # TIME CHECK
        if re.match(r"^\d{2}:\d{2}:\d{2}$", self.time) is None:  # currently not checking for correct numbers in HH:MM:SS format
            raise ValueError(f"Invalid time format '{self.time}', expected 'HH:MM:SS' format.")


    def init_by_object(obj: dict[str, Any]) -> Any:
        """
        Initialize the Refresh object from a dictionary.
        
        Args:
            obj: Dictionary containing 'cycle', 'day', and 'time' keys

        Returns:
            self: Returns the initialized Refresh object
        """
        cycle = obj.get("cycle")
        day = obj.get("day")
        time = obj.get("time")

        if not all([cycle, day, time]):
            raise ValueError("Incomplete refresh object, must contain 'cycle', 'day', and 'time' keys.")

        # Re-validate after initialization
        return Refresh(cycle, day, time)


    def get_latest_refresh(self, date: datetime = datetime.now(pytz.UTC)) -> datetime:
        """
        Get the latest refresh datetime based on the current time.
        
        Args:
            date: Current datetime (default: now)
            
        Returns:
            Latest refresh datetime
        """
        refresh_datetimes = self.get_refresh_datetimes(date)
        
        # Return the most recent refresh time before now
        return max(filter(lambda dt: dt <= date, refresh_datetimes))
    

    def get_next_refresh(self, date: datetime = datetime.now(pytz.UTC)) -> datetime:
        """
        Get the next refresh datetime based on the current time.
        
        Args:
            date: Current datetime (default: now)
            
        Returns:
            Next refresh datetime
        """
        refresh_datetimes = self.get_refresh_datetimes(date)
        
        # Return the next refresh time after now
        return min(filter(lambda dt: dt > date, refresh_datetimes))


    def get_refresh_datetimes(self, date: datetime = datetime.now(pytz.UTC)) -> list[datetime]:
        """
        Get all refresh datetimes within a specified time range.
        
        Args:
            from_dt: Start datetime (default: now)
            to_dt: End datetime (default: from_dt + 30 days)
            
        Returns:
            List of UTC datetimes when refreshes should occur
        """
        padding = Refresh.padding[self.cycle]
        from_date = date - padding
        to_date = date + padding
        
        get_datetimes = {
            "monthly": self.get_monthly_refresh_datetimes,
            "weekly": self.get_weekly_refresh_datetimes,
        }

        return get_datetimes[self.cycle](from_date, to_date)


    def get_monthly_refresh_datetimes(self, from_dt: datetime, to_dt: datetime) -> list[datetime]:
        """ Calculate monthly refresh datetimes within the given range """
        datetimes = []

        time_str = self.time
        hours, minutes, seconds = map(int, time_str.split(":"))

        current = from_dt.replace(day=1)
        while current <= to_dt:

            days_in_current_month = Refresh.amount_of_days_in_month(current)

            if abs(self.day) > days_in_current_month:  # If the day is invalid (e.g., February 30), use the last valid day
                self.day = days_in_current_month * self.day // abs(self.day)

            if self.day > 0:  # positive day -> n-th day of the month
                dt = current.replace(day=self.day)
            else:             # negative day -> -n-th day of the month
                dt = current.replace(day=days_in_current_month + self.day + 1)

            dt = dt.replace(hour=hours, minute=minutes, second=seconds, tzinfo=pytz.UTC)
            
            if from_dt <= dt <= to_dt:
                datetimes.append(dt)

            if current.month != 12:  # jump to the next month
                current = current.replace(month=current.month + 1)
            else:
                current = current.replace(year=current.year + 1, month=1)
        
        return datetimes


    def get_weekly_refresh_datetimes(self, from_dt: datetime, to_dt: datetime) -> list[datetime]:
        """ Calculate weekly refresh datetimes within the given range """
        datetimes = []

        day_of_week = Refresh.weekday_mapping[self.day]
        
        time_str = self.time
        hours, minutes, seconds = map(int, time_str.split(":"))

        current = from_dt

        while current <= to_dt:
            # Move to the next occurrence of the specified day and time
            dt = current + timedelta(days=(day_of_week - current.weekday()) % 7)
            dt = dt.replace(hour=hours, minute=minutes, second=seconds, tzinfo=pytz.UTC)

            if from_dt <= dt <= to_dt:
                datetimes.append(dt)

            # Move to the next week
            current = current + timedelta(weeks=1)

        return datetimes
    
    # - - - Utility - - -

    @staticmethod
    def amount_of_days_in_month(date: datetime) -> int:
        """
        Get the number of days in the month of the given date.
        
        Args:
            date: A datetime object

        Returns:
            The number of days in the month
        """
        if date.month != 12:
            next_month = date.replace(month=date.month + 1, day=1)
        else:
            next_month = date.replace(year=date.year + 1, month=1, day=1)
        
        return (next_month - timedelta(days=1)).day
