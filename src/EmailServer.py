import email
import pathlib
import time
from configparser import ConfigParser
from datetime import datetime, timedelta
from email.header import Header
from email.utils import parsedate_to_datetime
from multiprocessing import Pool

import imaplib
import poplib
from imapclient import IMAPClient
from rich.progress import Progress

from src.log import setup_logging

# 读取配置
config = ConfigParser()
config_path = pathlib.Path(__file__).parent / "config.cfg"
config.read(config_path, encoding="utf-8")
# 日志记录
logger = setup_logging()


class EmailServer:
    def __init__(self, date_str, minutes):
        self.date = date_str
        self.minutes = minutes
        # 基础信息
        self.host = config.get("EMAIL", "host")
        self.port = int(config.get("EMAIL", "port"))
        self.username = config.get("EMAIL", "username")
        self.password = config.get("EMAIL", "password")
        self.save_folder = config.get("EMAIL", "email_save_folder")
        # 额外字段
        self.server = None
        self.save_folder = f"{self.save_folder}\\{self.date}"
        self.base_login()

    def __del__(self):
        if self.server:
            try:
                self.server.quit()
            except:
                pass

    @staticmethod
    def get_parse_byte(obj):
        """解析加密字段"""
        if not obj:
            return ""
        if isinstance(obj, bytes):
            s = obj.decode()
        elif isinstance(obj, str):
            s = obj
        else:
            return str(obj)

        try:
            b, charset = email.header.decode_header(s)[0]
            if isinstance(b, bytes):
                if charset:
                    s = b.decode(charset)
                else:
                    s = b.decode()
        except Exception as e:
            logger.warning(f"解码失败: {e}")
        return s

    def base_login(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # self.server = poplib.POP3_SSL(host=self.host, port=self.port) if self.port == 995 else poplib.POP3(
                #     host=self.host, port=self.port)
                self.server = poplib.POP3_SSL(host=self.host, port=995)
                self.server.user(self.username)
                self.server.pass_(self.password)
                logger.info(f"登录成功：{self.username}")
                return
            except Exception as e:
                logger.error(f"登录失败（尝试 {attempt + 1}/{max_retries}）：{e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    raise

    def get_need_emails(self, subject_keyword: str):
        """
        筛选出给定时间范围内含指定关键字的邮件列表
        """
        need_list = []
        now = datetime.now()
        try:
            # 获取邮件总数
            mail_count = len(self.server.list()[1])
            logger.info(f"共有 {mail_count} 封邮件")

            # 从最新的邮件开始检查
            for i in range(mail_count, 0, -1):
                try:
                    # 获取邮件头部信息
                    response, lines, octets = self.server.top(i, 0)
                    msg_content = b'\r\n'.join(lines)
                    msg = email.message_from_bytes(msg_content)

                    # 获取邮件日期
                    date_str = msg.get('Date')
                    if date_str:
                        try:
                            # 解析邮件日期
                            msg_time = parsedate_to_datetime(date_str)
                            if msg_time.tzinfo is not None:
                                msg_time = msg_time.replace(tzinfo=None)
                            # 检查时间范围（）
                            if (now - timedelta(minutes=self.minutes)) <= msg_time and (now - timedelta(days=1)) <= msg_time :
                                # 检查主题关键词
                                subject = self.get_parse_byte(msg.get('Subject', ''))
                                if subject_keyword in subject:
                                    need_list.append(i)
                                    # logger.info(f"找到匹配邮件 #{i}: {subject}")
                            else:
                                break
                        except Exception as e:
                            logger.warning(f"解析邮件 #{i} 日期时出错: {e}")
                            continue
                except Exception as e:
                    logger.warning(f"检查邮件 #{i} 时出错: {e}")
                    continue

        except Exception as e:
            logger.error(f"获取邮件列表失败：{e}")
        return need_list

    def download_email(self, need_id: int, echo=False):
        """
        下载邮件中的所有附件，返回成功下载的附件路径列表
        """
        downloaded_files = []
        max_retries = 3

        for attempt in range(max_retries):
            try:
                # 保持连接活跃
                self.server.noop()

                # 获取完整邮件内容
                response, lines, octets = self.server.retr(need_id)
                msg_content = b'\r\n'.join(lines)
                email_message = email.message_from_bytes(msg_content)

                # 处理邮件附件
                for part in email_message.walk():
                    if part.get_content_disposition() == "attachment":
                        filename = self.get_parse_byte(part.get_filename())
                        if not filename:
                            continue

                        # 创建保存目录
                        save_path = pathlib.Path(self.save_folder)
                        save_path.mkdir(parents=True, exist_ok=True)

                        # 处理重复文件名
                        filepath = save_path / filename
                        counter = 1
                        while filepath.exists():
                            stem = filepath.stem
                            suffix = filepath.suffix
                            filepath = save_path / f"{stem}_{counter}{suffix}"
                            counter += 1

                        # 保存附件
                        try:
                            payload = part.get_payload(decode=True)
                            with open(filepath, 'wb') as f:
                                f.write(payload)
                            downloaded_files.append(str(filepath))
                            if echo:
                                logger.info(f"附件已保存到: {filepath}")
                        except Exception as e:
                            logger.error(f"保存附件 {filename} 失败: {e}")

                break  # 成功则退出重试循环

            except Exception as e:
                if "socket error" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"连接中断，正在重试... (尝试 {attempt + 1})")
                    time.sleep(2 ** attempt)
                    self.base_login()  # 重新登录
                else:
                    logger.error(f"下载邮件 {need_id} 失败: {e}")
                    raise

        return downloaded_files if downloaded_files else None


def download_emails(date_str, minutes, subject_keyword, mulprocess=False):
    emailServer = EmailServer(date_str, minutes)
    need_list = emailServer.get_need_emails(subject_keyword)
    if mulprocess:
        need_list = [(date_str, id) for id in need_list]
        # with Pool(5) as p:
        # p.starmap(_download_email, need_list)  # 无用代码
    else:
        with Progress() as progress:
            task = progress.add_task("[red]邮件下载中...", total=len(need_list))
            for need_id in need_list:
                file_path = emailServer.download_email(need_id, echo=False)
                progress.update(task, advance=1, description=f"[red]{file_path}下载完毕...", refresh=True)
