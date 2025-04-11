# 无法异步执行，需要换 aioimaplib 库 执行
# https://blog.csdn.net/gitblog_00783/article/details/142075459
# https://github.com/bamthomas/aioimaplib
# 这里采用多进程计算，多进程中由于_io.BufferedReader 无法序列化server，所以需要单独写一个函数
import email
import pathlib
from configparser import ConfigParser
from datetime import datetime, timedelta
from email.header import Header
from multiprocessing import Pool

from imapclient import IMAPClient
from rich.progress import Progress

# 读取配置
config = ConfigParser()
config_path = pathlib.Path(__file__).parent / "config.cfg"
config.read(config_path, encoding="utf-8")


class EmailServer:
    def __init__(self, date_str, minutes):
        self.date = date_str
        self.minutes = minutes
        # 基础信息
        self.host = config.get("EMAIL", "host")
        self.port = config.get("EMAIL", "port")
        self.username = config.get("EMAIL", "username")
        self.password = config.get("EMAIL", "password")
        self.save_folder = config.get("EMAIL", "email_save_folder")
        # 额外字段
        self.server = None
        self.save_folder = f"{self.save_folder}\\{self.date}"
        self.base_login()

    def __del__(self):
        self.server.logout()

    @staticmethod
    def get_parse_byte(obj):
        """解析加密字段
        """
        if isinstance(obj, bytes):
            s = obj.decode()
        elif isinstance(obj, str):
            s = obj
        b, charset = email.header.decode_header(s)[0]
        s = b.decode(charset)
        return s

    def base_login(self):
        """
        登入基础设置：
            1. imapclient 连接文档
            2. 用户名密码 登入 client
            3. 选择收件文件夹
            4. 筛选今天的邮件
        """
        self.server = IMAPClient(host=self.host, ssl=False)
        self.server.login(username=self.username, password=self.password)
        self.server.select_folder('INBOX')

    def get_need_emails(self, subject_keyword: str):
        """
        筛选出给定时间范围内含指定关键字的邮件列表
        """
        need_list = []
        now = datetime.now()
        try:
            # 从昨天开始查询
            since_date = (now - timedelta(days=1)).strftime('%d-%b-%Y')

            # 构造搜索条件
            criteria = ['SINCE', since_date]
            messages = self.server.search(criteria)

            # 筛选符合邮件
            for msgid, data in self.server.fetch(messages, ['ENVELOPE']).items():
                envelope = data[b'ENVELOPE']
                msg_time = envelope.date
                if (now - timedelta(minutes=self.minutes)) <= msg_time:
                    subject = self.get_parse_byte(envelope.subject)
                    if subject_keyword in subject:
                        need_list.append(msgid)

            return need_list

        except Exception as e:
            print(f"邮件筛选出错：{e}")
        return []

    def download_email(self, need_id: int, echo=False):
        """
        下载邮件中的所有附件，返回成功下载的附件路径列表
        """
        downloaded_files = []
        for uid, message_data in self.server.fetch(need_id, "RFC822").items():
            # 解析邮件内容
            email_message = email.message_from_bytes(message_data[b"RFC822"])

            for part in email_message.walk():
                # 附件检测
                if part.get_content_disposition() == "attachment" or (
                        part.get_filename() and part.get_content_maintype() != "multipart"
                ):
                    # 获取附件名
                    filename = self.get_parse_byte(part.get_filename())
                    if not filename:
                        continue

                    # 创建保存目录（如果不存在）
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
                    with open(filepath, 'wb') as f:
                        f.write(part.get_payload(decode=True))

                    downloaded_files.append(str(filepath))

                    if echo:
                        print(f"附件已保存到: {filepath}")

        return downloaded_files if downloaded_files else None


def _download_email(id):
    email_server = EmailServer()
    file_path = email_server.download_email(id, echo=True)
    print(f"下载完成：{file_path}")


def download_emails(date_str, minutes, subject_keyword, mulprocess=False):
    need_list = EmailServer(date_str, minutes).get_need_emails(subject_keyword)
    if mulprocess:
        need_list = [(date_str, id) for id in need_list]
        with Pool(5) as p:
            p.starmap(_download_email, need_list)  # 无用代码
    else:
        with Progress() as progress:
            task = progress.add_task("[red]邮件下载中...", total=len(need_list))
            for id in need_list:
                file_path = EmailServer(date_str, minutes).download_email(id, echo=False)
                progress.update(task, advance=1, description=f"[red]{file_path}下载完毕...")
