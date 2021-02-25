import logging # pylint: disable=C0302
import functools
from datetime import datetime, timedelta
import redis
import json
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
datetime_format_secondary = "%Y/%m/%d:%H:%M"
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
        day_unique_record = (
            session.query(DailyUniqueUsersMetrics)
            .filter(DailyUniqueUsersMetrics.timestamp == day)
            .first()
        )
        if day_unique_record:
            day_unique_record.count += unique_daily_count
        else:
            day_unique_record = DailyUniqueUsersMetrics(
                timestamp = day,
                count = unique_daily_count
            )
        session.add(day_unique_record)
        
        day_total_record = (
            session.query(DailyTotalUsersMetrics)
            .filter(DailyTotalUsersMetrics.timestamp == day)
            .first()
        )
        if day_total_record:
            day_total_record.count += count
        else:
            day_total_record = DailyTotalUsersMetrics(
                timestamp = day,
                count = count
            )
        session.add(day_total_record)
        
        month_unique_record = (
            session.query(MonthlyUniqueUsersMetrics)
            .filter(MonthlyUniqueUsersMetrics.timestamp == month)
            .first()
        )
        if month_unique_record:
            month_unique_record.count += unique_monthly_count
        else:
            monnth_unique_record = MonthlyUniqueUsersMetrics(
                timestamp = month,
                count = unique_monthly_count
            )
        session.add(month_unique_record)
        
        month_total_record = (
            session.query(MonthlyTotalUsersMetrics)
            .filter(MonthlyTotalUsersMetrics.timestamp == month)
            .first()
        )
        if month_total_record:
            month_total_record.count += count
        else:
            MonthlyTotalUsersMetrics(
                timestamp = month,
                count = count
            )
        session.add(month_total_record)

def persist_app_metrics(db, day, month, app_count):
    with db.scoped_session() as session:
        for application_name, count in app_count.items():
            day_record = (
                session.query(DailyAppNameMetrics)
                .filter(DailyAppNameMetrics.timestamp == day and DailyAppNameMetrics.application_name == application_name)
                .first()
            )
            if day_record:
                day_record.count += count
            else:
                day_record = DailyAppNameMetrics(
                    timestamp = day,
                    application_name = application_name,
                    count = count
                )
            session.add(day_record)
        
            month_record = (
                session.query(MonthlyAppNameMetrics)
                .filter(MonthlyAppNameMetrics.timestamp == month and MonthlyAppNameMetrics.application_name == application_name)
                .first()
            )
            if month_record:
                month_record.count += count
            else:
                month_record = MonthlyAppNameMetrics(
                    timestamp = month,
                    application_name = application_name,
                    count = count
                )
            session.add(month_record)

def merge_metrics(metrics, end_time, metric_type, db):
    logger.info(f"about to merge {metric_type} metrics: {metrics}")
    day = end_time.split(':')[0]
    month = f"{day[:7]}/01"
    
    frequent_key = frequent_route_metrics if metric_type == 'route' else frequent_app_metrics
    frequent_metrics_str = REDIS.get(frequent_key)
    frequent_metrics = json.loads(frequent_metrics_str) if frequent_metrics_str else {}
    
    daily_key = daily_route_metrics if metric_type == 'route' else daily_app_metrics
    daily_metrics_str = REDIS.get(daily_key)
    daily_metrics = json.loads(daily_metrics_str) if daily_metrics_str else {}
    
    monthly_key = monthly_route_metrics if metric_type == 'route' else monthly_app_metrics
    monthly_metrics_str = REDIS.get(monthly_key)
    monthly_metrics = json.loads(monthly_metrics_str) if monthly_metrics_str else {}
    
    if end_time not in frequent_metrics:
        frequent_metrics[end_time] = {}
    if day not in daily_metrics:
        daily_metrics[day] = {}
    if month not in monthly_metrics:
        monthly_metrics[month] = {}
    
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
    
    # remove metrics older than METRICS_INTERVAL * 10 from frequent_metrics
    old_time_str = (datetime.utcnow() - timedelta(minutes=METRICS_INTERVAL * 10)).strftime(datetime_format_secondary)
    frequent_metrics = {timestamp: metrics for timestamp, metrics in frequent_metrics.items() if timestamp > old_time_str}
    logger.info(f"updated cached frequent metrics: {frequent_metrics}")
    if frequent_metrics:
        REDIS.set(frequent_key, json.dumps(frequent_metrics))
    
    # remove metrics METRICS_INTERVAL after the end of the day from daily_metrics
    yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime(datetime_format_secondary)
    daily_metrics = {timestamp: metrics for timestamp, metrics in daily_metrics.items() if timestamp > yesterday_str}
    logger.info(f"updated cached daily metrics: {daily_metrics}")
    if daily_metrics:
        REDIS.set(daily_key, json.dumps(daily_metrics))
    
    # remove metrics METRICS_INTERVAL after the end of the month from monthly_metrics
    thirty_days_ago = (datetime.utcnow() - timedelta(days=30)).strftime(datetime_format_secondary)
    monthly_metrics = {timestamp: metrics for timestamp, metrics in monthly_metrics.items() if timestamp > thirty_days_ago}
    logger.info(f"updated cached monthly metrics: {monthly_metrics}")
    if monthly_metrics:
        REDIS.set(monthly_key, json.dumps(monthly_metrics))

    day_format = datetime_format_secondary.split(':')[0]
    day_obj = datetime.strptime(day, day_format)
    month_obj = datetime.strptime(month, day_format)
    if metric_type == 'route':
        persist_route_metrics(db, day_obj, month_obj, sum(metrics.values()), unique_daily_count, unique_monthly_count)
    else:
        persist_app_metrics(db, day_obj, month_obj, app_count)

def merge_route_metrics(metrics, end_time, db):
    merge_metrics(metrics, end_time, 'route', db)

def merge_app_metrics(metrics, end_time, db):
    merge_metrics(metrics, end_time, 'app', db)

def get_redis_metrics(start_time, metric_type):
    redis_metrics_str = REDIS.get(metric_type)
    metrics = json.loads(redis_metrics_str) if redis_metrics_str else {}
    if not metrics:
        return {}
    
    result = {}
    for datetime_str, value_counts in metrics.items():
        datetime_obj = datetime.strptime(datetime_str, datetime_format_secondary)
        if datetime_obj > start_time:
            for value, count in value_counts.items():
                result[value] = result[value] + count if value in result else count

    return result

def get_redis_route_metrics(start_time):
    return get_redis_metrics(start_time, frequent_route_metrics)

def get_redis_app_metrics(start_time):
    return get_redis_metrics(start_time, frequent_app_metrics)

def get_aggregate_metrics_info():
    info_str = REDIS.get(metrics_visited_nodes)
    return json.loads(info_str) if info_str else {}

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
