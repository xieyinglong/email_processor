import logging


def setup_logging(log_file='app.log'):
    """配置日志系统"""

    # 创建 logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # 设置最低日志级别

    # 创建文件 handler 并设置级别为 ERROR（只记录错误）
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.ERROR)

    # 创建控制台 handler 并设置级别为 INFO
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 创建日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 将 handler 添加到 logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
