import threading
import time as time_module
from datetime import time as date_time, datetime, timedelta
from typing import List, Optional

import pymysql  # 以MySQL为例，可根据实际数据库更换
import schedule

from src import DbServer
from src.DbServer import DataBaseServer
from src.EmailServer import download_emails
from src.excel_loader import xlsx_to_database


class EmailDbTrigger:
    def __init__(self, id: str, trigger_at: Optional[date_time], trigger_interval: int,
                 trigger_interval_remain: int, email_time_range: int, update_status: int, subject_keyword: str,
                 table_value: str, flag: int, mark: str, primary_column: str):
        self.id = id
        self.trigger_at = trigger_at
        self.trigger_interval = trigger_interval
        self.trigger_interval_remain = trigger_interval_remain
        self.email_time_range = email_time_range
        self.update_status = update_status
        self.subject_keyword = subject_keyword
        self.table_value = table_value
        self.flag = flag
        self.mark = mark
        self.primary_column = primary_column


def query_database(host: str, user: str, password: str, database: str, port: int, table_name: str) \
        -> List[EmailDbTrigger]:
    """查询定时任务表中所有行数据"""
    try:
        # 数据库连接配置（根据实际情况修改）
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset='utf8mb4'  # 默认为utf8mb4，可根据实际需求更换
        )

        print(f"\n执行查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        with connection.cursor() as cursor:
            # 执行SQL查询（指定表名 email_db_trigger）
            sql: str = (
                f"SELECT id, trigger_at, trigger_interval, trigger_interval_remain,"
                f"email_time_range, update_status, subject_keyword, table_value, flag, "
                f"remark, primary_column FROM email_db_trigger where flag = 1")
            cursor.execute(sql)

            # 获取所有行数据
            records = []
            rows = cursor.fetchall()

            # 打印结果
            print(f"共查询到 {len(rows)} 条记录:")
            for row in rows:
                print(row)
                record = EmailDbTrigger(id=row[0], trigger_at=row[1], trigger_interval=row[2],
                                        trigger_interval_remain=row[3], email_time_range=row[4], update_status=row[5],
                                        subject_keyword=row[6], table_value=row[7], flag=row[8], mark=row[9],
                                        primary_column=row[10])
                records.append(record)
            return records
    except Exception as e:
        print(f"数据库查询出错: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


# 一段时间间隔后更新
def update_trigger_intervals(host: str, user: str, password: str, database: str, port: int, table_name: str,
                             records: List[EmailDbTrigger]):
    # 设置参数
    date_str = datetime.now().strftime("%Y%m%d")  # 文件夹名称
    # date_str = '20250401'
    folder_path = DbServer.DataBaseServer(date_str).folder_path
    db_connection_string = DbServer.DataBaseServer(date_str).db_connection_string
    file_patterns = DbServer.DataBaseServer(date_str).file_patterns
    primary_column_map = DbServer.DataBaseServer(date_str).primary_column_map

    if not records:
        return
    try:
        # 数据库连接配置（根据实际情况修改）
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset='utf8mb4',  # 默认为utf8mb4，可根据实际需求更换
        )
        with connection.cursor() as cursor:
            # 执行SQL查询（指定表名）
            update_sql: str = f"UPDATE email_db_trigger SET trigger_interval_remain = %s WHERE id = %s"

            update_params = []
            update_mark = []
            update_lists = []
            for record in records:
                if record.trigger_at is None:
                    if record.trigger_interval is not None:
                        if record.trigger_interval_remain > 1:
                            # 计数减一
                            update_params.append((record.trigger_interval_remain - 1, record.id))
                            update_mark.append(record.mark)
                        else:
                            # 计数复原
                            update_lists.append(record)
                            update_params.append((record.trigger_interval, record.id))
                            update_mark.append(record.mark)
                            # 新增数据映射规则（文件关键字-数据表名）
                            file_patterns[record.subject_keyword] = record.table_value
                            primary_column_map[record.table_value] = record.primary_column
                            print(f"{record.mark} 触发")
                            # 下载邮件
                            download_emails(date_str, record.email_time_range, record.subject_keyword,
                                            mulprocess=False)
                            # 存入数据库
                            xlsx_to_database(folder_path, db_connection_string, file_patterns, primary_column_map,
                                             record.update_status)

            if update_params:
                print(update_params)
                cursor.executemany(update_sql, update_params)
                connection.commit()
                print(f"  已更新备注为 {update_mark} ，共计 {len(update_params)} 行数据!")
    except Exception as e:
        print(f"更新失败:{e}")
        if connection:
            connection.rollback()
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


def should_trigger_now(trigger_time: date_time) -> bool:
    """判断给定时间是否在当前时间的一分钟内（过去或未来）"""
    now = datetime.now()
    hh_mm = now.strftime("%H:%M")
    hh_mm_t = timedelta_to_hhmm_compact(trigger_time)
    return hh_mm == hh_mm_t


def timedelta_to_hhmm_compact(td: timedelta) -> str:
    total_minutes = int(td.total_seconds()) // 60
    return f"{total_minutes // 60:02d}:{abs(total_minutes) % 60:02d}"


# 指定时间更新指定表
def update_trigger_table(host: str, user: str, password: str, database: str, port: int, table_name: str,
                         record_id: str):
    # 设置参数
    date_str = datetime.now().strftime("%Y%m%d")  # 文件夹名称
    # date_str = '20250401'
    folder_path = DbServer.DataBaseServer(date_str).folder_path
    db_connection_string = DbServer.DataBaseServer(date_str).db_connection_string
    file_patterns = DbServer.DataBaseServer(date_str).file_patterns
    primary_column_map = DbServer.DataBaseServer(date_str).primary_column_map

    try:
        # 数据库连接配置（根据实际情况修改）
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            charset='utf8mb4',  # 默认为utf8mb4，可根据实际需求更换
        )

        print(f"\n执行查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        with connection.cursor() as cursor:

            # 执行SQL查询（指定表名）
            sql: str = (
                f"SELECT id, trigger_at, email_time_range, update_status, subject_keyword, table_value, flag, remark, primary_column"
                f"FROM email_db_trigger where id = %s")
            cursor.execute(sql, record_id)

            # 获取一行数据
            row = cursor.fetchone()
            if row is not None:
                # 新增数据映射规则（文件关键字-数据表名）
                file_patterns.append({row[4]: row[5]})
                primary_column_map[row[5]] = row[8]
                # 下载邮件(row[2]row[4]分别为邮件选择时间范围，邮件主题关键字段)
                download_emails(date_str, minutes=row[2], subject_keyword=row[4], mulprocess=False)
                # 存入数据库
                xlsx_to_database(folder_path, db_connection_string, file_patterns, primary_column_map,
                                 update_status=row[3])

    except Exception as e:
        print(f"数据库查询出错: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()


# 运行主体
def process_triggers():
    # 设置参数
    host = DataBaseServer("").host
    user = DataBaseServer("").username
    password = DataBaseServer("").password
    database = DataBaseServer("").database
    port = int(DataBaseServer("").port)
    table_name = DataBaseServer("").table_name
    # 立即执行一次查询
    records = query_database(host, user, password, database, port, table_name)
    # 数据分组
    interval_records = [record for record in records if record.trigger_interval is not None]
    uninterval_records = [record for record in records if record.trigger_at is not None]

    # 处理触发时间间隔不为空的数据
    update_trigger_intervals(host, user, password, database, port, table_name, interval_records)
    # 处理触发时间不为空的数据
    for record in uninterval_records:
        if should_trigger_now(record.trigger_at):
            update_trigger_table(host, user, password, database, port, table_name, record.id)


def schedule_checker():
    """检查定时任务"""
    while True:
        schedule.run_pending()
        time_module.sleep(1)


# 定时器启动
def start_schedule():
    # schedule.every(1).minutes.do(process_triggers)  # 每分钟执行一次process_triggers函数
    schedule.every(1).seconds.do(process_triggers)  # 每秒执行一次process_triggers函数
    schedule_checker()  # 执行schedule_checker函数


if __name__ == "__main__":
    print("程序启动，立即开始定时执行...")
    schedule_thread = threading.Thread(target=start_schedule, daemon=True)
    schedule_thread.start()
    try:
        while True:
            time_module.sleep(1)
    except KeyboardInterrupt:
        print("进程终止...")