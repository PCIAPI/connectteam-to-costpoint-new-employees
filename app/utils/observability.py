import logging
import os

import structlog
from aws_lambda_powertools import Tracer

aws_xray_tracer = Tracer()

# Setup stdlib logging with structlog formatter
logging_handler = logging.StreamHandler()
logging_handler.setFormatter(
    structlog.stdlib.ProcessorFormatter(processor=structlog.dev.ConsoleRenderer())
)
logging_logger = logging.getLogger()
logging_logger.setLevel(logging.INFO)
logging_logger.addHandler(logging_handler)

# Setup structlog
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)
