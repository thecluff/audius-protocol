import logging
import requests
from datetime import datetime, timedelta
from src import eth_abi_values
from src.models import RouteMetrics, AppNameMetrics
from src.tasks.celery_app import celery

from src.utils.redis_metrics import metrics_prefix, metrics_applications, \
    metrics_routes, metrics_visited_nodes, merge_route_metrics, merge_app_metrics, parse_metrics_key, get_rounded_date_time, METRICS_INTERVAL

logger = logging.getLogger(__name__)

sp_factory_registry_key = bytes("ServiceProviderFactory", "utf-8")
discovery_node_service_type = bytes("discovery-node", "utf-8")

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
        app_names = redis.hgetall(key)
        for key_bstr in app_names:
            app_name = key_bstr.decode('utf-8')
            val = int(app_names[key_bstr].decode('utf-8'))

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
                    elif source == metrics_applications:
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
    shared_config = aggregate_metrics.shared_config
    eth_web3 = aggregate_metrics.eth_web3
    eth_registry_address = aggregate_metrics.eth_web3.toChecksumAddress(
        shared_config["eth_contracts"]["registry"]
    )
    eth_registry_instance = eth_web3.eth.contract(
        address=eth_registry_address, abi=eth_abi_values["Registry"]["abi"]
    )
    sp_factory_address = eth_registry_instance.functions.getContract(
        sp_factory_registry_key
    ).call()
    sp_factory_inst = eth_web3.eth.contract(
        address=sp_factory_address, abi=eth_abi_values["ServiceProviderFactory"]["abi"]
    )
    discovery_providers = sp_factory_inst.functions.getServiceProviderList(discovery_node_service_type).call()
    return discovery_providers

def get_metrics(endpoint, start_time):
    try:
        route_metrics_request = requests.get(f"{endpoint}/routes/cached?start_time={start_time}", timeout=3)
        if route_metrics_request.status_code != 200:
            raise Exception(f"Query to cached route metrics endpoint failed with status code {r.status_code}")
        
        app_metrics_request = requests.get(f"{endpoint}/app_name/cached?start_time={start_time}", timeout=3)
        if app_metrics_request.status_code != 200:
            raise Exception(f"Query to cached app metrics endpoint failed with status code {r.status_code}")

        return route_metrics_request.json(), app_metrics_request.json()
    except Exception as e:
        logger.warning(f"Could not get metrics from node ${endpoint} with start_time {start_time}")
        logger.error(e)
        return None, None

# test this function
# <metrics_prefix>:<metrics_application>:<ip>:<rounded_date_time_format>
#         ie: "API_METRICS:applications:192.168.0.1:2020/08/04:14"
# <metrics_prefix>:<metrics_routes>:<ip>:<rounded_date_time_format>
#         ie: "API_METRICS:routes:192.168.0.1:2020/08/04:14"
def consolidate_metrics_from_other_nodes(self, db, redis):
    # make sure to exclude this node (itself)
    all_other_nodes = [node.endpoint for node in get_all_nodes(self)]
    logger.info(all_other_nodes)

    # expose as endpoint for visibility?
    visited_node_timestamps = redis.hgetall(metrics_visited_nodes) or {}

    for node in all_other_nodes:
        # what should we do if the visited_node_timestamps does not include a given node e.g. new node, or when it's empty the first time: datetime.utcnow() - timedelta(minutes=5)?
        start_time = visited_node_timestamps[node]
        new_route_metrics, new_app_metrics = get_metrics(node, start_time)
        if new_route_metrics and new_app_metrics: # do we want to separate route and app, i.e. one call succees other fails we merge the one that succeeds, also may mean 2 visited maps
            end_time = datetime.utcnow().strftime("%Y/%m/%d:%H:%M")
            visited_node_timestamps[node] = end_time
            merge_route_metrics(new_route_metrics, end_time, db)
            merge_app_metrics(new_app_metrics, end_time, db)
    redis.hmset(metrics_visited_nodes, visited_node_timestamps)

    # add logic to invalidate redis cache after end of day or end of month?
    


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
    db = aggregate_metrics.db
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
            consolidate_metrics_from_other_nodes(self, db, redis)
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
