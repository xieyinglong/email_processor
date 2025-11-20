import email
import logging
import pathlib
from email.policy import default

from tqdm import tqdm

logger = logging.getLogger(__name__)


class StreamAttachmentDownloader:
    def __init__(self, save_folder):
        self.save_folder = pathlib.Path(save_folder)
        self.save_folder.mkdir(parents=True, exist_ok=True)
        self.chunk_size = 1024 * 1024  # 1MB

    def _generate_unique_path(self, filename):
        """生成唯一文件名"""
        counter = 0
        stem = filename.stem
        suffix = filename.suffix
        while filename.exists():
            counter += 1
            filename = filename.parent / f"{stem}_{counter}{suffix}"
        return filename

    def save_large_attachment(self, part, filename):
        """流式保存大附件"""
        filepath = self.save_folder / filename
        filepath = self._generate_unique_path(filepath)

        try:
            # 获取附件大小（如果可用）
            size = int(part.get('Content-Length', 0))

            with open(filepath, 'wb') as f, \
                    tqdm(unit='B', unit_scale=True, unit_divisor=1024,
                         total=size, desc=f"下载 {filename}") as pbar:

                for chunk in part.iter_content(self.chunk_size):
                    if chunk:  # 过滤掉保持连接的空块
                        f.write(chunk)
                        pbar.update(len(chunk))

            return filepath
        except Exception as e:
            logger.error(f"保存附件失败: {str(e)}")
            if filepath.exists():
                filepath.unlink()  # 删除不完整的文件
            return None

    def process_email(self, raw_email):
        """处理邮件并下载附件"""
        msg = email.message_from_bytes(raw_email, policy=default)
        downloaded = []

        for part in msg.walk():
            if part.get_filename() and part.get_content_disposition() == 'attachment':
                filename = part.get_filename()
                saved_path = self.save_attachment(part, filename)
                if saved_path:
                    downloaded.append(saved_path)
        return downloaded

