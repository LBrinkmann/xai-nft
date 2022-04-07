import datetime


def to_midnight_ts(ds):
    midnight = datetime.datetime.combine(ds, datetime.datetime.min.time())
    return midnight.replace(tzinfo=datetime.timezone.utc)


def to_str(ds):
    return str(ds.date())


def create_timeline(n_days):
    base = datetime.datetime.utcnow()
    date_list = [base - datetime.timedelta(days=x) for x in range(n_days)]
    date_list_ts = list(map(to_midnight_ts, date_list))
    date_list_str = list(map(to_str, date_list))
    return [
        {'before': before, 'after': after, 'day': date_str}
        for before, after, date_str in zip(date_list_ts, date_list_ts[1:], date_list_str[1:])
    ]
