import logging
import random
from src.models import RouteMetrics, AppNameMetrics
from src.tasks.celery_app import celery

from src.utils.redis_metrics import metrics_prefix, metrics_application, \
    metrics_routes, metrics_visited_nodes, merge_metrics, parse_metrics_key, get_rounded_date_time

logger = logging.getLogger(__name__)


def process_route_keys(session, redis, key, ip, date):
    """
    For a redis hset storing a mapping of routes to the number of times they are hit,
    parse each key out into the version, path, and query string.
    Create a new entry in the DB for the each route.
    """
    try:
        route_metrics = []
        routes = redis.hgetall(key)
        for key_bstr in routes:
            route = key_bstr.decode('utf-8').strip('/')
            val = int(routes[key_bstr].decode('utf-8'))

            version = "0" # default value if version is not present
            path = route
            query_string = None

            route_subpaths = route.split('/')

            # Extract the version out of the path
            if route_subpaths[0].startswith('v') and len(route_subpaths[0]) > 1:
                version = route_subpaths[0][1:]
                path = '/'.join(route_subpaths[1:])

            # Extract the query string out of the path
            route_query = path.split('?')
            if len(route_query) > 1:
                path = route_query[0]
                query_string = route_query[1]
            route_metrics.append(
                RouteMetrics(
                    version=version,
                    route_path=path,
                    query_string=query_string,
                    count=val,
                    ip=ip,
                    timestamp=date
                )
            )

        if route_metrics:
            session.bulk_save_objects(route_metrics)
        redis.delete(key)
    except Exception as e:
        raise Exception("Error processing route key %s with error %s" % (key, e))

def process_app_name_keys(session, redis, key, ip, date):
    """
    For a redis hset storing a mapping of app_name usage in request parameters to count,
    Create a new entry in the DB for each app_name.
    """
    try:
        app_name_metrics = []
        routes = redis.hgetall(key)
        for key_bstr in routes:
            app_name = key_bstr.decode('utf-8')
            val = int(routes[key_bstr].decode('utf-8'))

            app_name_metrics.append(
                AppNameMetrics(
                    application_name=app_name,
                    count=val,
                    ip=ip,
                    timestamp=date
                )
            )
        if app_name_metrics:
            session.bulk_save_objects(app_name_metrics)
        redis.delete(key)

    except Exception as e:
        raise Exception("Error processing app name key %s with error %s" % (key, e))

def sweep_metrics(db, redis):
    """
    Move the metrics values from redis to the DB.

    Get all the redis keys with the metrics prefix,
    parse the key to get the timestamp in the key.
    If it is before the current time, then process the redis hset.
    """
    with db.scoped_session() as session:
        for key_byte in redis.scan_iter(f"{metrics_prefix}:*"):
            key = key_byte.decode("utf-8")
            try:
                parsed_key = parse_metrics_key(key)

                if parsed_key is None:
                    raise KeyError(f"index_metrics.py | Unable to parse key {key} | Skipping process key")
                source, ip, key_date = parsed_key

                current_date_time = get_rounded_date_time()

                if key_date < current_date_time:
                    if source == metrics_routes:
                        process_route_keys(session, redis, key, ip, key_date)
                    elif source == metrics_application:
                        process_app_name_keys(session, redis, key, ip, key_date)
            except KeyError as e:
                logger.warning(e)
                redis.delete(key)
            except Exception as e:
                logger.error(e)
                redis.delete(key)

def refresh_metrics_matviews(db):
    with db.scoped_session() as session:
        logger.info('index_metrics.py | refreshing metrics matviews')
        session.execute('REFRESH MATERIALIZED VIEW route_metrics_day_bucket')
        session.execute('REFRESH MATERIALIZED VIEW route_metrics_month_bucket')
        session.execute('REFRESH MATERIALIZED VIEW route_metrics_trailing_week')
        session.execute('REFRESH MATERIALIZED VIEW route_metrics_trailing_month')
        session.execute('REFRESH MATERIALIZED VIEW route_metrics_all_time')
        session.execute('REFRESH MATERIALIZED VIEW app_name_metrics_trailing_week')
        session.execute('REFRESH MATERIALIZED VIEW app_name_metrics_trailing_month')
        session.execute('REFRESH MATERIALIZED VIEW app_name_metrics_all_time')
        logger.info('index_metrics.py | refreshed metrics matviews')

# should this be cached?
def get_all_nodes():
    return []

def get_metrics(node):
    # for trailing api calls (count) and unique users (unique_count) for month:
    # make a call to {node.endpoint}/v1/metrics/routes/trailing/month
    # fallback to {node.endpoint}/v1/metrics/routes?bucket_size=century&start_time=${startTime} with start_time being 1 month from now

    # for time series for api calls and unique users
    # bucket size and start time combos: (month,now-year), (day,now-month), (day,now-week), (hour,now-day)
    # make a call to {node.endpoint}/v1/metrics/${route}?bucket_size=${bucket_size}&start_time=${startTime} with the different bucket sizes

    # for top apps; limit is 8; bucketPath can be week, month, all_time with fallback startTime corresponding to now-week, now-month, now-year
    # make a call to {node.endpoint}/v1/metrics/app_name/trailing/${bucketPath}?limit=8
    # fallback to {node.endpoint}/v1/metrics/app_name?start_time=${startTime}&limit=8&include_unknown=true
    return []

# test this function
def consolidate_metrics_from_other_nodes(redis):
    # read this list directly from chain, there is some contract available for this
    all_nodes = get_all_nodes()
    # this should include the node and the last timestamp we got data from this node so we can optimize deduping
    visited_nodes = redis.scan_iter(metrics_visited_nodes) or []
    # for filtering unvisited nodes, do we compare node hashes, names, etc?
    # check identifiers for filtering, getting from cache, setting cache, etc.
    unvisited_nodes = list(filter(lambda node: node not in visited_nodes, all_nodes))
    # expose above as endpoint for visibility?
    if not len(unvisited_nodes):
        redis.set(metrics_visited_nodes, [])
        unvisited_nodes = all_nodes
    next_node_to_visit = unvisited_nodes[random.randrange(len(unvisited_nodes))]
    redis.set(metrics_visited_nodes, redis.get(metrics_visited_nodes) + [next_node_to_visit])
    next_node_metrics = get_metrics(next_node_to_visit)
    merge_metrics(next_node_metrics)
    # do I need try/catch above?


######## CELERY TASKs ########


@celery.task(name="update_metrics", bind=True)
def update_metrics(self):
    # Cache custom task class properties
    # Details regarding custom task context can be found in wiki
    # Custom Task definition can be found in src/__init__.py
    db = update_metrics.db
    redis = update_metrics.redis

    # Define lock acquired boolean
    have_lock = False

    # Define redis lock object
    update_lock = redis.lock("update_metrics_lock", blocking_timeout=25)
    try:
        # Attempt to acquire lock - do not block if unable to acquire
        have_lock = update_lock.acquire(blocking=False)
        if have_lock:
            logger.info(
                f"index_metrics.py | update_metrics | {self.request.id} | Acquired update_metrics_lock")
            sweep_metrics(db, redis)
            refresh_metrics_matviews(db)
            logger.info(
                f"index_metrics.py | update_metrics | {self.request.id} | Processing complete within session")
        else:
            logger.error(
                f"index_metrics.py | update_metrics | {self.request.id} | Failed to acquire update_metrics_lock")
    except Exception as e:
        logger.error(
            "Fatal error in main loop of update_metrics: %s", e, exc_info=True)
        raise e
    finally:
        if have_lock:
            update_lock.release()


@celery.task(name="aggregate_metrics", bind=True)
def aggregate_metrics(self):
    # Cache custom task class properties
    # Details regarding custom task context can be found in wiki
    # Custom Task definition can be found in src/__init__.py
    redis = aggregate_metrics.redis

    # Define lock acquired boolean
    have_lock = False

    # Define redis lock object
    update_lock = redis.lock("aggregate_metrics_lock", blocking_timeout=25)
    try:
        # Attempt to acquire lock - do not block if unable to acquire
        have_lock = update_lock.acquire(blocking=False)
        if have_lock:
            logger.info(
                f"index_metrics.py | aggregate_metrics | {self.request.id} | Acquired aggregate_metrics_lock")
            consolidate_metrics_from_other_nodes(redis)
            logger.info(
                f"index_metrics.py | aggregate_metrics | {self.request.id} | Processing complete within session")
        else:
            logger.error(
                f"index_metrics.py | aggregate_metrics | {self.request.id} | Failed to acquire aggregate_metrics_lock")
    except Exception as e:
        logger.error(
            "Fatal error in main loop of aggregate_metrics: %s", e, exc_info=True)
        raise e
    finally:
        if have_lock:
            update_lock.release()
