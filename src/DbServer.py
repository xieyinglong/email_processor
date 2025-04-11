# 读取配置
import pathlib
from configparser import ConfigParser
from urllib.parse import quote_plus

config = ConfigParser()
config_path = pathlib.Path(__file__).parent / "config.cfg"
config.read(config_path, encoding="utf-8")


class DataBaseServer:
    def __init__(self, date_str):
        self.file_patterns = {}
        self.primary_column_map = {}
        self.date = date_str
        # 基础信息
        self.host = config.get("DATABASE", "host")
        self.database = config.get("DATABASE", "database")
        self.username = config.get("DATABASE", "user")
        self.password = config.get("DATABASE", "password")
        self.folder_path = config.get("DATABASE", "email_save_folder")
        self.port = config.get("DATABASE", "port")
        self.table_name = config.get("DATABASE", "trigger_table_name")
        # 额外字段
        # 密码含特殊字符进行重编码
        self.encoded_password = quote_plus(self.password)
        self.folder_path = f"{self.folder_path}\\{self.date}"
        self.db_connection_string = self.base_login()

    def base_login(self):
        return f"mysql+pymysql://{self.username}:{self.encoded_password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"
