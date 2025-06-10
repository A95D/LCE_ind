import logging
import os

def setup_logger(log_file='etl.log'):
    # Создаём папку logs, если нужно
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger('etl_logger')
    logger.setLevel(logging.INFO)

    # Чтобы не дублировать хендлеры при повторной инициализации
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger