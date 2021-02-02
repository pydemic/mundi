import logging

log = logging.getLogger("mundi")
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
