import logging
from src.queries.get_genre_metrics import get_genre_metrics
from src.queries.get_plays_metrics import get_plays_metrics
from flask_restx import Resource, Namespace, fields, reqparse, inputs
from src.api.v1.helpers import make_response, success_response, to_dict, \
    parse_bool_param, parse_unix_epoch_param, abort_bad_request_param, \
    abort_bad_path_param, format_limit
from .models.metrics import route_metric, app_name_metric, app_name, plays_metric, \
    genre_metric, route_trailing_metric, app_name_trailing_metric
from src.queries.get_route_metrics import get_route_metrics, get_aggregate_route_metrics
from src.queries.get_app_name_metrics import get_app_name_metrics, get_aggregate_app_metrics
from src.queries.get_app_names import get_app_names
from src.queries.get_trailing_metrics import get_monthly_trailing_route_metrics, \
    get_trailing_app_metrics, get_aggregate_route_metrics_trailing_month
from src.utils.redis_cache import cache
from src.utils.redis_metrics import get_redis_route_metrics, get_redis_app_metrics, \
    get_aggregate_metrics_info
logger = logging.getLogger(__name__)


ns = Namespace('metrics', description='Metrics related operations')

route_metrics_response = make_response("metrics_reponse", ns, fields.List(fields.Nested(route_metric)))
route_metrics_trailing_month_response = make_response("route_metrics_trailing_month_response", ns, fields.Nested(route_trailing_metric))
app_name_response = make_response("app_name_response", ns, fields.List(fields.Nested(app_name)))
app_name_trailing_response = make_response("app_name_trailing_response", ns, fields.List(fields.Nested(app_name_trailing_metric)))
app_name_metrics_response = make_response("app_name_metrics_response", ns, fields.List(fields.Nested(app_name_metric)))
plays_metrics_response = make_response("plays_metrics", ns, fields.List(fields.Nested(plays_metric)))
genre_metrics_response = make_response("genre_metrics", ns, fields.List(fields.Nested(genre_metric)))

metrics_route_parser = reqparse.RequestParser()
metrics_route_parser.add_argument('path', required=False)
metrics_route_parser.add_argument('query_string', required=False)
metrics_route_parser.add_argument('start_time', required=False, type=int)
metrics_route_parser.add_argument('exact', required=False)
metrics_route_parser.add_argument('limit', required=False, type=int)
metrics_route_parser.add_argument('bucket_size', required=False)
metrics_route_parser.add_argument('version', required=False, action='append')

valid_date_buckets = ['hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century']
valid_bucket_sizes = {
    'week': ['day'],
    'month': ['day', 'week'],
    'all_time': ['month', 'week']
}

@ns.route("/aggregates/info", doc=False)
class MetricsAggregateInfo(Resource):
    @cache(ttl_sec=5)
    def get(self):
        """Gets aggregate metrics information"""
        metrics_info = get_aggregate_metrics_info()
        logger.info(f"metrics info: {metrics_info}")
        response = success_response(metrics_info)
        return response

@ns.route("/aggregates/routes/cached", doc=False)
class CachedMetrics(Resource):
    @cache(ttl_sec=5)
    def get(self):
        args = metrics_route_parser.parse_args()
        start_time = parse_unix_epoch_param(args.get("start_time"))
        metrics = get_redis_route_metrics(start_time)
        # if not metrics:
        #     metrics = {
        #         "1.2.3.4": 5,
        #         "5.6.7.8": 10
        #     }
        response = success_response(metrics)
        return response

@ns.route("/aggregates/routes/trailing/month", doc=False)
class RouteAggregateMetricsTrailingMonth(Resource):
    # @cache(ttl_sec=30 * 60)
    def get(self):
        """Gets aggregated route metrics based on time range and bucket size"""
        metrics = get_aggregate_route_metrics_trailing_month()
        response = success_response(metrics)
        return response

aggregate_metrics_route_parser = reqparse.RequestParser()
aggregate_metrics_route_parser.add_argument('bucket_size', required=False)

@ns.route("/aggregates/routes/<string:time_range>", doc=False)
class RouteAggregateMetrics(Resource):
    # @cache(ttl_sec=30 * 60)
    def get(self, time_range):
        """Gets aggregated route metrics based on time range and bucket size"""
        if time_range not in valid_bucket_sizes:
            abort_bad_path_param('time_range', ns)
        
        args = aggregate_metrics_route_parser.parse_args()
        valid_buckets = valid_bucket_sizes[time_range]
        bucket_size = args.get("bucket_size") or valid_buckets[0]
        
        if bucket_size not in valid_buckets:
            abort_bad_request_param('bucket_size', ns)
            
        metrics = get_aggregate_route_metrics(time_range, bucket_size)
        response = success_response(metrics)
        return response

@ns.route("/aggregates/apps/cached", doc=False)
class CachedMetrics(Resource):
    @cache(ttl_sec=5)
    def get(self):
        args = metrics_route_parser.parse_args()
        start_time = parse_unix_epoch_param(args.get("start_time"))
        metrics = get_redis_app_metrics(start_time)
        # if not metrics:
        #     metrics = {
        #         "best-app": 5,
        #         "top-app": 10
        #     }
        response = success_response(metrics)
        return response

aggregate_metrics_app_name_parser = reqparse.RequestParser()
aggregate_metrics_app_name_parser.add_argument('limit', required=False)

@ns.route("/aggregates/apps/<string:time_range>", doc=False)
class AppNameAggregateMetricsTrailing(Resource):
    # @cache(ttl_sec=30 * 60)
    def get(self, time_range):
        """Gets aggregated app metrics based on time range and bucket size"""
        if time_range not in valid_bucket_sizes:
            abort_bad_path_param('time_range', ns)
        
        args = aggregate_metrics_app_name_parser.parse_args()
        limit = format_limit(args, max_limit=100)
        metrics = get_aggregate_app_metrics(time_range, limit)
        response = success_response(metrics)
        return response

@ns.route("/routes", doc=False)
class RouteMetrics(Resource):
    @ns.expect(metrics_route_parser)
    @ns.doc(
        id="""Route Metrics""",
        params={
            'path': 'Request Path',
            'query_string': 'Query String',
            'start_time': 'Start Time in Unix Epoch',
            'exact': 'Exact Path Query Match',
            'limit': 'Limit',
            'bucket_size': 'Bucket Size',
            'version': 'API Version Query Filter'
        },
        responses={
            200: 'Success',
            400: 'Bad request',
            500: 'Server error'
        }
    )
    @ns.marshal_with(route_metrics_response)
    @cache(ttl_sec=3 * 60 * 60)
    def get(self):
        """Get the route metrics"""
        args = metrics_route_parser.parse_args()
        if args.get('limit') is None:
            args['limit'] = 168
        else:
            args['limit'] = min(args.get('limit'), 168)

        if args.get('bucket_size') is None:
            args['bucket_size'] = 'hour'
        if args.get('bucket_size') not in valid_date_buckets:
            abort_bad_request_param('bucket_size', ns)

        try:
            args['start_time'] = parse_unix_epoch_param(args.get('start_time'), 0)
        except:
            abort_bad_request_param('start_time', ns)

        if args.get('exact') is not None:
            args['exact'] = parse_bool_param(args.get('exact'))
            if args.get('exact') is None:
                abort_bad_request_param('exact', ns)
        else:
            args['exact'] = False

        args['path'] = args.get('path') if args.get('path') is not None else ''
        route_metrics = get_route_metrics(args)
        response = success_response(route_metrics)
        return response


metrics_app_name_list_parser = reqparse.RequestParser()
metrics_app_name_list_parser.add_argument('start_time', required=False, type=int)
metrics_app_name_list_parser.add_argument('limit', required=False, type=int)
metrics_app_name_list_parser.add_argument('offset', required=False, type=int)
metrics_app_name_list_parser.add_argument('include_unknown', required=False, type=inputs.boolean)

@ns.route("/routes/trailing/month", doc=False)
class RouteMetricsTrailingMonth(Resource):
    @ns.marshal_with(route_metrics_trailing_month_response)
    @cache(ttl_sec=30 * 60)
    def get(self):
        """Gets trailing month route metrics from matview"""
        metrics = get_monthly_trailing_route_metrics()
        response = success_response(metrics)
        return response

@ns.route("/app_name", doc=False)
class AppNameListMetrics(Resource):
    @ns.doc(
        id="""Get App Names""",
        params={
            'offset': 'Offset',
            'limit': 'Limit',
            'start_time': 'Start Time in Unix Epoch',
            'include_unknown': 'Whether or not to include unknown apps'
        },
        responses={
            200: 'Success',
            400: 'Bad request',
            500: 'Server error'
        }
    )
    @ns.expect(metrics_app_name_list_parser)
    @ns.marshal_with(app_name_response)
    @cache(ttl_sec=30 * 60)
    def get(self):
        """List all the app names"""
        args = metrics_app_name_list_parser.parse_args()
        if args.get('limit') is None:
            args['limit'] = 100
        else:
            args['limit'] = min(args.get('limit'), 100)
        if args.get('offset') is None:
            args['offset'] = 0
        try:
            args['start_time'] = parse_unix_epoch_param(args.get('start_time'), 0)
        except:
            abort_bad_request_param('start_time', ns)

        app_names = get_app_names(args)
        response = success_response(app_names)
        return response


valid_trailing_time_periods = ["week", "month", "all_time"]
trailing_app_name_parser = reqparse.RequestParser()
trailing_app_name_parser.add_argument('limit', required=False, type=int)

@ns.route("/app_name/trailing/<string:time_range>")
class TrailingAppNameMetrics(Resource):
    @ns.marshal_with(app_name_trailing_response)
    @cache(ttl_sec=3 * 60 * 60)
    def get(self, time_range):
        """Gets trailing app name metrics from matview"""
        if time_range not in valid_trailing_time_periods:
            abort_bad_request_param('time_range', ns)
        parsed = trailing_app_name_parser.parse_args()
        args = {
            "limit": parsed.get("limit", 10),
            "time_range": time_range
        }
        metrics = get_trailing_app_metrics(args)
        response = success_response(metrics)
        return response

metrics_app_name_parser = reqparse.RequestParser()
metrics_app_name_parser.add_argument('start_time', required=False, type=int)
metrics_app_name_parser.add_argument('limit', required=False, type=int)
metrics_app_name_parser.add_argument('bucket_size', required=False)

@ns.route("/app_name/<string:app_name>", doc=False)
class AppNameMetrics(Resource):
    @ns.doc(
        id="""Get Metrics by App Name""",
        params={
            'start_time': 'Start Time in Unix Epoch',
            'limit': 'Limit',
            'bucket_size': 'Bucket Size',
        },
        responses={
            200: 'Success',
            400: 'Bad request',
            500: 'Server error'
        }
    )
    @ns.expect(metrics_app_name_parser)
    @ns.marshal_with(app_name_metrics_response)
    @cache(ttl_sec=30 * 60)
    def get(self, app_name):
        """Get the app name metrics"""
        args = metrics_app_name_parser.parse_args()

        if args.get('limit') is None:
            args['limit'] = 168
        else:
            args['limit'] = min(args.get('limit'), 168)

        try:
            args['start_time'] = parse_unix_epoch_param(args.get('start_time'), 0)
        except:
            abort_bad_request_param('start_time', ns)

        if args.get('bucket_size') is None:
            args['bucket_size'] = 'hour'
        if args.get('bucket_size') not in valid_date_buckets:
            abort_bad_request_param('bucket_size', ns)

        app_name_metrics = get_app_name_metrics(app_name, args)
        response = success_response(app_name_metrics)
        return response


metrics_plays_parser = reqparse.RequestParser()
metrics_plays_parser.add_argument('start_time', required=False, type=int)
metrics_plays_parser.add_argument('limit', required=False, type=int)
metrics_plays_parser.add_argument('bucket_size', required=False)

@ns.route("/plays", doc=False)
class PlaysMetrics(Resource):
    @ns.doc(
        id="""Get Plays Metrics""",
        params={
            'start_time': 'Start Time in Unix Epoch',
            'limit': 'Limit',
            'bucket_size': 'Bucket Size',
        },
        responses={
            200: 'Success',
            400: 'Bad request',
            500: 'Server error'
        }
    )
    @ns.expect(metrics_plays_parser)
    @ns.marshal_with(plays_metrics_response)
    @cache(ttl_sec=3 * 60 * 60)
    def get(self):
        args = metrics_plays_parser.parse_args()

        if args.get('limit') is None:
            args['limit'] = 168
        else:
            args['limit'] = min(args.get('limit'), 168)

        try:
            args['start_time'] = parse_unix_epoch_param(args.get('start_time'), 0)
        except:
            abort_bad_request_param('start_time', ns)

        if args.get('bucket_size') is None:
            args['bucket_size'] = 'hour'
        if args.get('bucket_size') not in valid_date_buckets:
            abort_bad_request_param('bucket_size', ns)

        plays_metrics = get_plays_metrics(args)
        response = success_response(plays_metrics)
        return response


metrics_genres_parser = reqparse.RequestParser()
metrics_genres_parser.add_argument('start_time', required=False, type=int)
metrics_genres_parser.add_argument('limit', required=False, type=int)
metrics_genres_parser.add_argument('offset', required=False, type=int)

@ns.route("/genres", doc=False)
class GenreMetrics(Resource):
    @ns.doc(
        id="""Get Genre Metrics""",
        params={
            'offset': 'Offset',
            'limit': 'Limit',
            'start_time': 'Start Time in Unix Epoch'
        },
        responses={
            200: 'Success',
            400: 'Bad request',
            500: 'Server error'
        }
    )
    @ns.expect(metrics_genres_parser)
    @ns.marshal_with(genre_metrics_response)
    @cache(ttl_sec=3 * 60 * 60)
    def get(self):
        args = metrics_genres_parser.parse_args()

        if args.get('limit') is None:
            args['limit'] = 100
        else:
            args['limit'] = min(args.get('limit'), 100)
        if args.get('offset') is None:
            args['offset'] = 0
        try:
            args['start_time'] = parse_unix_epoch_param(args.get('start_time'), 0)
        except:
            abort_bad_request_param('start_time', ns)

        genre_metrics = get_genre_metrics(args)
        response = success_response(genre_metrics)
        return response
