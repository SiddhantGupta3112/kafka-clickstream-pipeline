from db import get_db
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLOSE_QUERY = """
    DELETE FROM active_sessions
    WHERE last_event_at < NOW() - INTERVAL '30 minutes'
    RETURNING session_id, user_id, session_start, last_event_at, event_count
"""

INSERT_CLOSED = """
    INSERT INTO closed_sessions (session_id, user_id, session_start, session_end, event_count)
    VALUES (%s, %s, %s, %s, %s)
"""

while True:
    try:
        with get_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute(CLOSE_QUERY)
                closed_sessions = cursor.fetchall()

                if closed_sessions:
                    logger.info(f"Closing {len(closed_sessions)} sessions.")
                    for session in closed_sessions:
                        cursor.execute(
                            INSERT_CLOSED,
                            (
                                session[0],  
                                session[1],  
                                session[2], 
                                session[3],  
                                session[4],  
                            ),
                        )
                    conn.commit()
                else:
                    logger.info("No sessions to close.")

    except Exception as e:
        logger.error(f"Error while closing sessions: {e}")

    time.sleep(60)