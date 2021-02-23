import logging # pylint: disable=C0302
import functools
from datetime import datetime
import redis
from flask.globals import request
from src.models import DailyUniqueUsersMetrics, DailyTotalUsersMetrics, MonthlyUniqueUsersMetrics, MonthlyTotalUsersMetrics, DailyAppNameMetrics, MonthlyAppNameMetrics
from src.utils.config import shared_config
from src.utils.query_params import stringify_query_params, app_name_param
logger = logging.getLogger(__name__)

REDIS_URL = shared_config["redis"]["url"]
REDIS = redis.Redis.from_url(url=REDIS_URL)

METRICS_INTERVAL = 5 # should this be a shared config?

# Redis Key Convention:
# API_METRICS:routes:<date>:<hour>
# API_METRICS:application:<date>:<hour>

metrics_prefix = "API_METRICS"
metrics_routes = "routes"
metrics_applications = "applications"
metrics_visited_nodes = "visited_nodes"
frequent_route_metrics = "frequent_route_metrics"
daily_route_metrics = "daily_route_metrics"
monthly_route_metrics = "monthly_route_metrics"
frequent_app_metrics = "frequent_app_metrics"
daily_app_metrics = "daily_app_metrics"
monthly_app_metrics = "monthly_app_metrics"

'''
NOTE: if you want to change the time interval to recording metrics,
change the `datetime_format` and func `get_rounded_date_time` to reflect the interval
ie. If you wanted to record metrics per minute:
datetime_format = "%Y/%m/%d:%H-%M" # Add the minute template
def get_rounded_date_time():
    return datetime.utcnow().replace(second=0, microsecond=0) # Remove rounding min.
'''
datetime_format = "%Y/%m/%d:%H"
def get_rounded_date_time():
    return datetime.utcnow().replace(minute=0, second=0, microsecond=0)

def format_ip(ip):
    # Replace the `:` character with an `_`  because we use : as the redis key delimiter
    return ip.strip().replace(":", "_")

def get_request_ip(request_obj):
    header_ips = request_obj.headers.get('X-Forwarded-For', request_obj.remote_addr)
    # Use the left most IP, representing the user's IP
    first_ip = header_ips.split(',')[0]
    return format_ip(first_ip)

def parse_metrics_key(key):
    """
    Validates that a key is correctly formatted and returns
    the source: (routes|applications), ip address, and date of key
    """
    if not metrics_prefix.startswith(metrics_prefix): # ??? do we mean key.startswith(metrics_prefix)
        logger.warning(f"Bad redis key inserted w/out metrics prefix {key}")
        return None

    fragments = key.split(':')
    if len(fragments) != 5:
        logger.warning(f"Bad redis key inserted: must have 5 parts {key}")
        return None

    _, source, ip, date, time = fragments
    # Replace the ipv6 _ delimiter back to :
    ip = ip.replace("_", ":")
    if source not in (metrics_routes, metrics_applications):
        logger.warning(f"Bad redis key inserted: must be routes or application {key}")
        return None
    date_time = datetime.strptime(f"{date}:{time}", datetime_format)

    return source, ip, date_time

def persist_route_metrics(db, day, month, count, unique_daily_count, unique_monthly_count):
    with db.scoped_session() as session:
        day_unique_record = session.query(DailyUniqueUsersMetrics).filter(DailyUniqueUsersMetrics.timestamp === day)
        if len(day_unique_record) > 0:
            day_unique_record.update({"count": DailyUniqueUsersMetrics.count + unique_daily_count})
        else:
            session.add(DailyUniqueUsersMetrics(
                timestamp = day,
                count = unique_daily_count
            ))
        
        day_total_record = session.query(DailyTotalUsersMetrics).filter(DailyTotalUsersMetrics.timestamp === day)
        if len(day_total_record) > 0:
            day_total_record.update({"count": DailyTotalUsersMetrics.count + count})
        else:
            session.add(DailyTotalUsersMetrics(
                timestamp = day,
                count = count
            ))
        
        month_unique_record = session.query(MonthlyUniqueUsersMetrics).filter(MonthlyUniqueUsersMetrics.timestamp === month)
        if len(month_unique_record) > 0:
            month_unique_record.update({"count": MonthlyUniqueUsersMetrics.count + unique_monthly_count})
        else:
            session.add(MonthlyUniqueUsersMetrics(
                timestamp = month,
                count = unique_monthly_count
            ))
        
        month_total_record = session.query(MonthlyTotalUsersMetrics).filter(MonthlyTotalUsersMetrics.timestamp === month)
        if len(month_total_record) > 0:
            month_total_record.update({"count": MonthlyTotalUsersMetrics.count + count})
        else:
            session.add(MonthlyTotalUsersMetrics(
                timestamp = month,
                count = count
            ))

def persist_app_metrics(db, day, month, app_count):
    with db.scoped_session() as session:
        for app_name, count in app_count.items():
            day_record = session.query(DailyAppNameMetrics)
                .filter(DailyAppNameMetrics.timestamp === day and DailyAppNameMetrics.application_name == app_name)
            if len(day_record) > 0:
                day_record.update({"count": DailyAppNameMetrics.count + count)
            else:
                session.add(DailyAppNameMetrics(
                    timestamp = day,
                    app_name = app_name,
                    count = count
                ))
        
            month_record = session.query(MonthlyAppNameMetrics)
                .filter(MonthlyAppNameMetrics.timestamp === month and MonthlyAppNameMetrics.application_name == app_name)
            if len(month_record) > 0:
                month_record.update({"count": MonthlyAppNameMetrics.count + count})
            else:
                session.add(MonthlyAppNameMetrics(
                    timestamp = month,
                    app_name = app_name,
                    count = count
                ))

def merge_metrics(metrics, end_time, metric_type, db):
    day = end_time.split(':')[0]
    month = f"{day[:7]}/01"
    
    frequent_key = frequent_route_metrics if metric_type == 'route' else frequent_app_metrics
    daily_key = daily_route_metrics if metric_type == 'route' else daily_app_metrics
    monthly_key = monthly_route_metrics if metric_type == 'route' else monthly_app_metrics
    
    frequent_metrics = REDIS.hgetall(frequent_key) or {}
    daily_metrics = REDIS.hgetall(daily_key) or {}
    monthly_metrics = REDIS.hgetall(monthly_key) or {}

    # only relevant for unique users and total api calls
    unique_daily_count = 0
    unique_monthly_count = 0

    # only relevant for app metrics
    app_count = {}
    
    for new_value, new_count in metrics.items():
        if metric_type == 'route' and new_value not in daily_metrics[day]:
            unique_daily_count += 1
        if metric_type == 'route' and new_value not in monthly_metrics[month]:
            unique_monthly_count += 1
        if metric_type == 'app':
            app_count[new_value] = new_count
        frequent_metrics[end_time][new_value] = frequent_metrics[end_time][new_value] + new_count if new_value in frequent_metrics[end_time] else new_count
        daily_metrics[day][new_value] = daily_metrics[day][new_value] + new_count if new_value in daily_metrics[day] else new_count
        monthly_metrics[month][new_value] = monthly_metrics[month][new_value] + new_count if new_value in monthly_metrics[month] else new_count
    
    REDIS.hmset(frequent_key, frequent_metrics)
    REDIS.hmset(daily_key, daily_metrics)
    REDIS.hmset(monthly_key, monthly_metrics)

    # since we persist on metrics read from other nodes, we may not need daily and monthly metrics in redis
    if metric_type == 'route':
        persist_route_metrics(db, day, month, sum(metric.values()), unique_daily_count, unique_monthly_count)
    else:
        persist_app_metrics(db, day, month, app_count)

def merge_route_metrics(metrics, end_time, db):
    merge_metrics(metrics, end_time, 'route', db)

def merge_app_metrics(metrics, end_time, db):
    merge_metrics(metrics, end_time, 'app', db)

def get_redis_metrics(args, metric_type):
    start_time = args.get("start_time")
    metrics = REDIS.hgetall(metric_type)
    if not metrics:
        return {}
    
    result = {}
    for date, value_counts in metrics.items():
        if date > start_time:
            for value, count in value_counts.items():
                result[value] = result[value] + count if value in result else count

    return result

def get_redis_route_metrics(args):
    return get_redis_metrics(args, frequent_route_metrics)

def get_redis_app_metrics(args):
    return get_redis_metrics(args, frequent_app_metrics)


def extract_app_name_key():
    """
    Extracts the application name redis key and hash from the request
    The key should be of format:
        <metrics_prefix>:<metrics_application>:<ip>:<rounded_date_time_format>
        ie: "API_METRICS:applications:192.168.0.1:2020/08/04:14"
    The hash should be of format:
        <app_name>
        ie: "audius_dapp"
    """
    application_name = request.args.get(app_name_param, type=str, default=None)
    ip = get_request_ip(request)
    date_time = get_rounded_date_time().strftime(datetime_format)

    application_key = f"{metrics_prefix}:{metrics_applications}:{ip}:{date_time}"
    return (application_key, application_name)

def extract_route_key():
    """
    Extracts the route redis key and hash from the request
    The key should be of format:
        <metrics_prefix>:<metrics_routes>:<ip>:<rounded_date_time_format>
        ie: "API_METRICS:routes:192.168.0.1:2020/08/04:14"
    The hash should be of format:
        <path><sorted_query_params>
        ie: "/v1/tracks/search?genre=rap&query=best"
    """
    path = request.path
    req_args = request.args.items()
    req_args = stringify_query_params(req_args)
    route = f"{path}?{req_args}" if req_args else path
    ip = get_request_ip(request)
    date_time = get_rounded_date_time().strftime(datetime_format)

    route_key = f"{metrics_prefix}:{metrics_routes}:{ip}:{date_time}"
    return (route_key, route)

# Metrics decorator.
def record_metrics(func):
    """
    The metrics decorator records each time a route is hit in redis
    The number of times a route is hit and an app_name query param are used are recorded.
    A redis a redis hash map is used to store each of these values.

    NOTE: This must be placed before the cache decorator in order for the redis incr to occur
    """
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        try:
            application_key, application_name = extract_app_name_key()
            route_key, route = extract_route_key()
            REDIS.hincrby(route_key, route, 1)
            if application_name:
                REDIS.hincrby(application_key, application_name, 1)
        except Exception as e:
            logger.error('Error while recording metrics: %s', e.message)

        return func(*args, **kwargs)
    return wrap
