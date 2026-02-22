from __future__ import annotations

from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
from typing import List, Set
from config import settings


def _parse_holidays(dates: List[str]) -> List[date]:
    out: List[date] = []
    for d in dates:
        try:
            out.append(datetime.strptime(d, "%Y-%m-%d").date())
        except Exception:
            continue
    return out


def _easter_sunday(year: int) -> date:
    """
    Compute Western (Gregorian) Easter Sunday using Anonymous Gregorian algorithm (Meeus/Jones/Butcher).
    """
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """
    Return the date for the nth weekday of a month.
    weekday: Monday=0 ... Sunday=6
    n: 1..5 (5 used for 'last' when combined with logic elsewhere)
    """
    try:
        first = date(year, month, 1)
        first_wd = first.weekday()
        delta = (weekday - first_wd) % 7
        day = 1 + delta + (n - 1) * 7
        
        result = date(year, month, day)
        
        # If the result is in the next month, we went too far
        if result.month != month:
            # Go back one week to get the last occurrence in this month
            result = result - timedelta(days=7)
        
        return result
    except ValueError:
        # If date creation fails, calculate manually
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        
        # Start from the last day and work backwards
        for day in range(last_day, 0, -1):
            try:
                candidate = date(year, month, day)
                if candidate.weekday() == weekday:
                    return candidate
            except ValueError:
                continue
        
        # This should never happen, but fallback to first day of month
        return date(year, month, 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """
    Return the date for the last weekday of a month.
    """
    import calendar
    
    # Get the last day of the month
    last_day = calendar.monthrange(year, month)[1]
    
    # Start from the last day and work backwards
    for day in range(last_day, 0, -1):
        try:
            candidate = date(year, month, day)
            if candidate.weekday() == weekday:
                return candidate
        except ValueError:
            continue
    
    # This should never happen, but fallback
    return date(year, month, 1)


def _observed(dt: date) -> date:
    """
    Apply standard US observed rules:
    - If holiday falls on Saturday, observed on Friday prior.
    - If holiday falls on Sunday, observed on Monday after.
    """
    if dt.weekday() == 5:  # Saturday
        return dt - timedelta(days=1)
    if dt.weekday() == 6:  # Sunday
        return dt + timedelta(days=1)
    return dt


def _generate_us_market_holidays(year: int) -> Set[date]:
    """
    Generate a set of US market holiday dates for a given year, using typical
    equity/futures closures. This is a pragmatic baseline; brokers may vary.
    Includes observed rules and Good Friday.
    """
    holidays: Set[date] = set()

    # New Year's Day (observed)
    holidays.add(_observed(date(year, 1, 1)))
    # Martin Luther King Jr. Day (3rd Monday in January)
    holidays.add(_nth_weekday(year, 1, 0, 3))
    # Presidents' Day (3rd Monday in February)
    holidays.add(_nth_weekday(year, 2, 0, 3))
    # Good Friday (2 days before Easter Sunday)
    easter = _easter_sunday(year)
    good_friday = easter - timedelta(days=2)
    holidays.add(good_friday)
    # Memorial Day (last Monday in May)
    holidays.add(_last_weekday(year, 5, 0))
    # Juneteenth (June 19, observed)
    holidays.add(_observed(date(year, 6, 19)))
    # Independence Day (July 4, observed)
    holidays.add(_observed(date(year, 7, 4)))
    # Labor Day (1st Monday in September)
    holidays.add(_nth_weekday(year, 9, 0, 1))
    # Thanksgiving Day (4th Thursday in November)
    holidays.add(_nth_weekday(year, 11, 3, 4))
    # Christmas Day (Dec 25, observed)
    holidays.add(_observed(date(year, 12, 25)))

    return holidays


def _generate_us_low_volume_days(year: int) -> Set[date]:
    """
    Generate discretionary 'low-volume' days often skipped by strategies:
    - Halloween (weekday)
    - Indigenous Peoples'/Columbus Day (2nd Monday in Oct)
    - Veterans Day (Nov 11, weekday)
    - Day before Thanksgiving (Wed)
    - Black Friday (Fri after Thanksgiving)
    - Christmas Eve (Dec 24, weekday)
    - New Year's Eve (Dec 31, weekday)
    - Day after Christmas (Dec 26, weekday if Dec 25 was weekday)
    - Day after New Year's (Jan 2, weekday if Jan 1 was weekday)
    """
    lv: Set[date] = set()
    # Halloween
    halloween = date(year, 10, 31)
    if halloween.weekday() < 5:
        lv.add(halloween)
    # Indigenous Peoples'/Columbus Day (2nd Monday Oct)
    lv.add(_nth_weekday(year, 10, 0, 2))
    # Veterans Day
    veterans = date(year, 11, 11)
    if veterans.weekday() < 5:
        lv.add(veterans)
    # Thanksgiving
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    # Day before Thanksgiving (Wed)
    day_before_thanksgiving = thanksgiving - timedelta(days=1)
    if day_before_thanksgiving.weekday() < 5:
        lv.add(day_before_thanksgiving)
    # Black Friday
    black_friday = thanksgiving + timedelta(days=1)
    if black_friday.weekday() < 5:
        lv.add(black_friday)
    # Christmas Eve
    christmas_eve = date(year, 12, 24)
    if christmas_eve.weekday() < 5:
        lv.add(christmas_eve)
    # New Year's Eve
    nye = date(year, 12, 31)
    if nye.weekday() < 5:
        lv.add(nye)
    # Day after Christmas (if both weekdays)
    christmas = date(year, 12, 25)
    day_after_christmas = date(year, 12, 26)
    if christmas.weekday() < 5 and day_after_christmas.weekday() < 5:
        lv.add(day_after_christmas)
    # Day after New Year's (if both weekdays)
    new_years = date(year, 1, 1)
    day_after_ny = date(year, 1, 2)
    if new_years.weekday() < 5 and day_after_ny.weekday() < 5:
        lv.add(day_after_ny)
    return lv


def _merged_holiday_set(now_local: datetime, tz: ZoneInfo) -> Set[date]:
    """
    Merge config-provided holidays with generated US market holidays
    when enabled. Returns a set of local-date holidays.
    """
    manual = set(_parse_holidays(settings.CALENDAR_HOLIDAYS))

    if not getattr(settings, "CALENDAR_RULES_US_MARKETS", True):
        return manual

    years_back = int(getattr(settings, "CALENDAR_YEARS_BACK", 0))
    years_ahead = int(getattr(settings, "CALENDAR_YEARS_AHEAD", 3))
    base_year = now_local.year
    generated: Set[date] = set()
    for y in range(base_year - years_back, base_year + years_ahead + 1):
        generated |= _generate_us_market_holidays(y)

    # Optionally include discretionary low-volume days (E*TRADE-style)
    if getattr(settings, "CALENDAR_INCLUDE_LOW_VOLUME", True):
        for y in range(base_year - years_back, base_year + years_ahead + 1):
            generated |= _generate_us_low_volume_days(y)

    return manual | generated


def is_prepost_holiday_block_now(now_utc: datetime | None = None) -> bool:
    """
    Returns True if current time is within the pre/post holiday block window.
    Window: [holiday 00:00 - pre_hours, holiday 23:59:59 + post_hours]
    All times evaluated in CALENDAR_TZ (default America/New_York).
    """
    if not settings.CALENDAR_ENABLED:
        return False

    tz = ZoneInfo(settings.CALENDAR_TZ)
    now = (now_utc or datetime.utcnow()).astypezone(tz) if hasattr(datetime, "astypezone") else (now_utc or datetime.utcnow()).astimezone(tz)

    holidays = sorted(_merged_holiday_set(now, tz))
    pre_delta = timedelta(hours=int(settings.CALENDAR_PRE_HOURS))
    post_delta = timedelta(hours=int(settings.CALENDAR_POST_HOURS))

    for h in holidays:
        start_local = datetime.combine(h, time(0, 0, 0), tzinfo=tz)
        end_local = datetime.combine(h, time(23, 59, 59), tzinfo=tz)
        window_start = start_local - pre_delta
        window_end = end_local + post_delta
        if window_start <= now <= window_end:
            return True
    return False


def get_current_holiday_info(now_utc: datetime | None = None) -> tuple[bool, str, str]:
    """
    Get detailed information about current holiday status.
    
    Returns:
        Tuple of (is_holiday_period, holiday_name, holiday_type)
        - is_holiday_period: True if within holiday block window
        - holiday_name: Name of the holiday (e.g., "Thanksgiving Day")
        - holiday_type: "MARKET_CLOSED" or "LOW_VOLUME"
    """
    if not settings.CALENDAR_ENABLED:
        return False, "", ""

    tz = ZoneInfo(settings.CALENDAR_TZ)
    now = (now_utc or datetime.utcnow()).astypezone(tz) if hasattr(datetime, "astypezone") else (now_utc or datetime.utcnow()).astimezone(tz)
    today = now.date()
    
    # Get all holidays for current year
    us_market_holidays = _generate_us_market_holidays(now.year)
    us_low_volume_days = _generate_us_low_volume_days(now.year)
    manual_holidays = set(_parse_holidays(settings.CALENDAR_HOLIDAYS))
    
    pre_delta = timedelta(hours=int(settings.CALENDAR_PRE_HOURS))
    post_delta = timedelta(hours=int(settings.CALENDAR_POST_HOURS))
    
    all_holidays = us_market_holidays | manual_holidays
    if getattr(settings, "CALENDAR_INCLUDE_LOW_VOLUME", True):
        all_holidays |= us_low_volume_days
    
    # PRIORITY 1: Check if TODAY is a holiday (exact match)
    if today in all_holidays:
        holiday_name = _get_holiday_name(today, now.year)
        
        # Determine if it's a market closure or low volume day
        if today in us_market_holidays or today in manual_holidays:
            holiday_type = "MARKET_CLOSED"
        else:
            holiday_type = "LOW_VOLUME"
        
        return True, holiday_name, holiday_type
    
    # PRIORITY 2: Check if we're in a holiday window (pre/post period)
    for holiday_date in sorted(all_holidays):
        # Skip today since we already checked it above
        if holiday_date == today:
            continue
            
        start_local = datetime.combine(holiday_date, time(0, 0, 0), tzinfo=tz)
        end_local = datetime.combine(holiday_date, time(23, 59, 59), tzinfo=tz)
        window_start = start_local - pre_delta
        window_end = end_local + post_delta
        
        if window_start <= now <= window_end:
            # We're in a pre/post holiday window
            holiday_name = _get_holiday_name(holiday_date, now.year)
            
            # Determine if it's a market closure or low volume day
            if holiday_date in us_market_holidays or holiday_date in manual_holidays:
                holiday_type = "MARKET_CLOSED"
            else:
                holiday_type = "LOW_VOLUME"
            
            # Add context about pre/post period
            if holiday_date > today:
                holiday_name = f"Pre-{holiday_name}"
            else:
                holiday_name = f"Post-{holiday_name}"
            
            return True, holiday_name, holiday_type
    
    return False, "", ""


def _get_holiday_name(holiday_date: date, year: int) -> str:
    """
    Get the name of a holiday based on its date.
    """
    # Map specific dates to holiday names
    holiday_names = {
        date(year, 1, 1): "New Year's Day",
        date(year, 7, 4): "Independence Day", 
        date(year, 12, 25): "Christmas Day",
        date(year, 6, 19): "Juneteenth",
        date(year, 11, 11): "Veterans Day",
        date(year, 10, 31): "Halloween",
        date(year, 12, 24): "Christmas Eve",
        date(year, 12, 31): "New Year's Eve",
        date(year, 1, 2): "Day After New Year's",
        date(year, 12, 26): "Day After Christmas"
    }
    
    # Check observed holidays (Saturday -> Friday, Sunday -> Monday)
    if holiday_date.weekday() == 4:  # Friday
        # Could be Saturday holiday observed on Friday
        saturday = holiday_date + timedelta(days=1)
        if saturday in holiday_names:
            return holiday_names[saturday] + " (Observed)"
    elif holiday_date.weekday() == 0:  # Monday
        # Could be Sunday holiday observed on Monday
        sunday = holiday_date - timedelta(days=1)
        if sunday in holiday_names:
            return holiday_names[sunday] + " (Observed)"
    
    # Check direct match
    if holiday_date in holiday_names:
        return holiday_names[holiday_date]
    
    # Calculate dynamic holidays
    # Martin Luther King Jr. Day (3rd Monday in January)
    mlk_day = _nth_weekday(year, 1, 0, 3)
    if holiday_date == mlk_day:
        return "Martin Luther King Jr. Day"
    
    # Presidents' Day (3rd Monday in February)
    presidents_day = _nth_weekday(year, 2, 0, 3)
    if holiday_date == presidents_day:
        return "Presidents' Day"
    
    # Good Friday (2 days before Easter)
    easter = _easter_sunday(year)
    good_friday = easter - timedelta(days=2)
    if holiday_date == good_friday:
        return "Good Friday"
    
    # Memorial Day (last Monday in May)
    memorial_day = _last_weekday(year, 5, 0)
    if holiday_date == memorial_day:
        return "Memorial Day"
    
    # Labor Day (1st Monday in September)
    labor_day = _nth_weekday(year, 9, 0, 1)
    if holiday_date == labor_day:
        return "Labor Day"
    
    # Columbus Day (2nd Monday in October)
    columbus_day = _nth_weekday(year, 10, 0, 2)
    if holiday_date == columbus_day:
        return "Columbus Day"
    
    # Thanksgiving (4th Thursday in November)
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    if holiday_date == thanksgiving:
        return "Thanksgiving Day"
    
    # Day before Thanksgiving
    day_before_thanksgiving = thanksgiving - timedelta(days=1)
    if holiday_date == day_before_thanksgiving:
        return "Day Before Thanksgiving"
    
    # Black Friday
    black_friday = thanksgiving + timedelta(days=1)
    if holiday_date == black_friday:
        return "Black Friday"
    
    # Fallback
    return f"Holiday ({holiday_date.strftime('%B %d')})"


