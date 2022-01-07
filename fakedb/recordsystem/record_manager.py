

from ..filesystem.file_manager import FileManager
from .utils import get_record_capacity, get_bitmap_len
from .header import Header
from .rid import RID
from .record import Record
from ..config import PAGE_SIZE, NEXT_AVAILABLE_PAGE_OFFSET, NEXT_AVAILABLE_PAGE_SIZE, BITMAP_START_OFFSET
import numpy as np


class RecordManager:

    def __init__(self, file_manager: FileManager) -> None:
        self.file_manager = file_manager
        self.fd = None

    def init_info(self, fd) -> None:
        '''
        打开文件时调用，获取页头等基本信息
        '''
        self.fd = fd
        header_page = self.file_manager.read_page(fd, 0)
        # print(f'header page:{header_page}')
        self.header = Header.deserialize(header_page)
        # print('self.header = ', self.header)

    def create_file(self, filename, record_len):
        '''
        创建一张表
        filename: 待创建的文件名(数据库名+表名)
        record_len: 一条记录的长度
        '''
        # print(f'record manager create file:{filename}')
        # 创建文件
        self.file_manager.create_file(filename)

        # 添加头页
        fd = self.file_manager.open_file(filename)  # 打开文件
        record_capacity = get_record_capacity(record_len)  # 计算每页可存储记录的最大条数
        bitmap_len = get_bitmap_len(record_capacity)
        header = Header(
            record_len=record_len,
            record_capacity=record_capacity,
            record_num=0,
            page_num=1,
            filename=filename,
            bitmap_len=bitmap_len,
            next_available_page=0)
        header_data = header.serialize()
        self.file_manager.write_page(fd, 0, header_data)  # 写在第一页

        # 关闭文件
        self.file_manager.close_file(fd)

    def remove_file(self, filename):
        '''删除文件'''
        self.file_manager.remove_file(filename)

    def open_file(self, filename):
        '''
        打开文件, 获得其句柄
        '''
        fd = self.file_manager.open_file(filename)
        if fd != self.fd:
            self.init_info(fd)
            # print(f'filename:{filename}, fd:{fd}')
        return fd

    def close_file(self):
        '''
        关闭文件
        '''
        self.file_manager.close_file(self.fd)
        temp_fd = self.fd
        self.fd = None
        return temp_fd

    def get_byte_offset_by_slotid(self, slotid):
        return BITMAP_START_OFFSET + self.header.bitmap_len + self.header.record_len * slotid

    def get_page_and_offset(self, rid: RID):
        page = self.file_manager.read_page(self.fd, rid.page_id)
        byte_offset = self.get_byte_offset_by_slotid(rid.slot_id)
        return page, byte_offset

    def get_next_available(self, page):
        offset = NEXT_AVAILABLE_PAGE_OFFSET
        siz = NEXT_AVAILABLE_PAGE_SIZE
        return int.from_bytes(page[offset:offset + siz].tobytes(), 'big')

    def set_next_available(self, page, next_page_id):
        offset = NEXT_AVAILABLE_PAGE_OFFSET
        siz = NEXT_AVAILABLE_PAGE_SIZE
        page[offset:offset +
             siz] = np.frombuffer(next_page_id.to_bytes(siz, 'big'), dtype=np.uint8)

    def write_header_back(self):
        self.file_manager.write_page(self.fd, 0, self.header.serialize())

    def append_page(self):
        data = np.full(PAGE_SIZE, -1, dtype=np.uint8)
        data[NEXT_AVAILABLE_PAGE_OFFSET: NEXT_AVAILABLE_PAGE_OFFSET +
             NEXT_AVAILABLE_PAGE_SIZE] = 0
        self.set_next_available(data, 0)
        page_id = self.file_manager.new_page(self.fd, data)
        self.header.page_num += 1
        self.header.next_available_page = page_id
        self.write_header_back()
        return page_id

    def get_bitmap(self, page):
        offset = BITMAP_START_OFFSET
        l = self.header.bitmap_len
        record_capacity = self.header.record_capacity
        return np.unpackbits(page[offset: offset+l])[:record_capacity]

    def set_bitmap(self, page, bitmap):
        offset = BITMAP_START_OFFSET
        l = self.header.bitmap_len
        page[offset: offset+l] = np.packbits(bitmap)  # 不足一个byte的话会先补0再pack

    def get_record(self, rid: RID):
        page, byte_offset = self.get_page_and_offset(rid)
        record = Record(rid, page[byte_offset: byte_offset + self.header.record_len])
        return record

    def update_record(self, rid: RID, data):
        page, byte_offset = self.get_page_and_offset(rid)
        page[byte_offset: byte_offset + self.header.record_len] = data
        self.file_manager.write_page(self.fd, rid.page_id, page)

    def get_page(self, page_id):
        return self.file_manager.read_page(self.fd, page_id)

    def insert_record(self, data):
        page_id = self.header.next_available_page
        if page_id == 0:
            page_id = self.append_page()
            # print(f'append page:{page_id}')
            # header_page = self.file_manager.read_page(self.fd, 0)
            # print(f'header_page', header_page)

        # if page_id == 1:
        #     print(f'insert data:{data.tobytes()}, page:{page_id}')

        page = self.get_page(page_id)
        bitmap = self.get_bitmap(page)
        availabel_slots, = np.where(bitmap)

        slot_id = availabel_slots[0]
        offset = self.get_byte_offset_by_slotid(slot_id)
        record_len = self.header.record_len
        page[offset: offset+record_len] = data
        bitmap[slot_id] = 0

        self.set_bitmap(page, bitmap)

        self.header.record_num += 1

        if len(availabel_slots) == 1:
            # this page is full now
            self.header.next_available_page = self.get_next_available(page)

        self.file_manager.write_page(self.fd, page_id, page)
        self.write_header_back()
        return RID(page_id, slot_id)

    def delete_record(self, rid: RID):
        page_id = rid.page_id
        slot_id = rid.slot_id

        page = self.file_manager.read_page(self.fd, page_id)
        bitmap = self.get_bitmap(page)

        bitmap[slot_id] = 1
        self.header.record_num -= 1

        self.set_bitmap(page, bitmap)
        if self.get_next_available(page) == 0:
            self.set_next_available(
                page, self.header.next_available_page)
            self.header.next_availabel_page = page_id

        self.write_header_back()
        self.file_manager.write_page(self.fd, page_id, page)

    def shutdown(self):
        pass
