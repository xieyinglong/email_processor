from typing import Dict

import pymysql


def get_key_value_map_from_mysql(
        table_name: str
) -> Dict[str, str]:
    """
    从MySQL表中读取key-value对并返回字典
    :param table_name: 表名
    :return: 包含key-value对的字典
    """
    result_map = {}

    try:
        # 建立数据库连接
        connection = pymysql.connect(
            host="10.6.135.34",
            user="root",
            password="root@mysql",
            database="excel_to_database",
            port=3306,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        with connection:
            with connection.cursor() as cursor:
                # 执行SQL查询
                sql = (
                    f"SELECT key_column, value_column FROM table_map_config WHERE table_name = %s")
                cursor.execute(sql, table_name)

                # 获取所有结果并构建字典
                for row in cursor.fetchall():
                    key = row['key_column']
                    value = row['value_column']
                    if key is not None:  # 确保key不为None
                        result_map[str(key)] = value if value is not None else None

    except pymysql.Error as e:
        print(f"数据库错误: {e}")
        raise

    return result_map

