import logging
import sys


def setup_logging(level: str = "INFO", json_format: bool = False) -> None:
    """Настраивает логирование для всего проекта."""
    root_logger = logging.getLogger()

    # Сброс существующих хендлеров
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    log_level = getattr(logging, level.upper(), logging.INFO)
    root_logger.setLevel(log_level)

    handler = logging.StreamHandler(sys.stdout)

    if json_format:
        from pythonjsonlogger.jsonlogger import JsonFormatter

        formatter = JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Настройка уровней для сторонних библиотек
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
