from contextlib import contextmanager
import logging  # pylint: disable=C0302
from sqlalchemy import create_engine
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker
from src.queries.search_config import set_search_similarity

logger = logging.getLogger(__name__)


class SessionManager:
    def __init__(self, db_url, db_engine_args):
        self._engine = create_engine(db_url, **db_engine_args)
        self._session_factory = sessionmaker(bind=self._engine)
        # Attach a listener for new engine connection.
        # See https://docs.sqlalchemy.org/en/14/core/event.html
        listen(self._engine, 'connect', self.on_connect)

    def on_connect(self, dbapi_conn, connection_record):
        """
        Callback invoked with a raw DBAPI connection every time the engine assigns a new
        connection to the session manager.

        Actions that should be fired on new connection should be performed here.
        For example, pg_trgm.similarity_threshold needs to be set once for each connection,
        but not if that connection is recycled and used in another session.
        """
        logger.debug("Using new DBAPI connection")
        cursor = dbapi_conn.cursor()
        set_search_similarity(cursor)
        cursor.close()

    def session(self):
        """
        Get a session for direct management/use. Use not recommended unless absolutely
        necessary.
        """
        return self._session_factory()

    @contextmanager
    def scoped_session(self, expire_on_commit=True):
        """
        Usage:
            with scoped_session() as session:
                use the session ...

        Session commits when leaving the block normally, or rolls back if an exception
        is thrown.

        Taken from: http://docs.sqlalchemy.org/en/latest/orm/session_basics.html
        """
        session = self._session_factory()
        session.expire_on_commit = expire_on_commit
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
