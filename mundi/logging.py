import logging
import os

log = logging.getLogger("mundi")
logging.basicConfig()

if os.environ.get("MUNDI_DEBUG", "").lower() == "true":
    logging.getLogger("mundi").setLevel(logging.DEBUG)
else:
    logging.getLogger("mundi").setLevel(logging.WARNING)
