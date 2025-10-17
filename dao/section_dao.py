from typing import List, Any
import mysql.connector

from commons.base_logger import BaseLogger
from gugu2.commons.base_db import BaseDB
from gugu2.mydataclass.section import SectionInfo
from tools.retry_on_exception import retry_on_exception
from gugu2.dao.gongpeng_dao import GongpengDao


class SectionDao(BaseDB):
    """
    SectionDao 提供 SectionInfo 对象的数据库操作，包括插入、批量 upsert、查询等。
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.gongpeng_dao = GongpengDao(**kwargs)
        self.logger = BaseLogger(__name__)


    def ensure_table(self):
        """
        确保 section 表存在，不存在则创建。
        """
        create_table_sql = """
                           CREATE TABLE IF NOT EXISTS section_info
                           (
                               id                   BIGINT PRIMARY KEY,
                               auctiontype          VARCHAR(32),
                               auctionid            INT,
                               name                 VARCHAR(255),
                               organizername        VARCHAR(255),
                               organizerphone       VARCHAR(64),
                               customerservicephone VARCHAR(64),
                               matchid              BIGINT,
                               startranking         INT,
                               endranking           INT,
                               count                INT,
                               sorttype             VARCHAR(32),
                               startprice           DECIMAL(20, 2),
                               sort                 INT,
                               createadminid        BIGINT,
                               createtime           BIGINT,
                               status_name          VARCHAR(64),
                               CONSTRAINT fk_auctionid FOREIGN KEY (auctionid) REFERENCES gongpeng_info (id)
                                   ON UPDATE CASCADE
                                   ON DELETE SET NULL
                           ) ENGINE = InnoDB
                             DEFAULT CHARSET = utf8mb4; 
                           """


        with self.connection_ctx() as conn:
            cursor = conn.cursor()
        try:
            cursor.execute(create_table_sql)
            cursor.close()
            self.logger.log_info("已确保 section 表存在")
        except mysql.connector.Error as e:
            self.logger.log_error(f"创建 section 表失败: {e}", exc_info=True)





    def insert_section(self, info: SectionInfo):
        """
        插入单条的拍卖列表，联查同步公棚拍卖状态
        """
        info.status_name = self.gongpeng_dao.get_status_name_by_auctionid(info.auctionid)   #获取公棚拍卖完成状态
        insert_sql = """
                     INSERT INTO section_info (id, auctiontype, auctionid, name, organizername, organizerphone,
                                               customerservicephone, matchid, startranking, endranking, count,
                                               sorttype, startprice, sort, createadminid, createtime, status_name)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
                     """
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(insert_sql, (
                    info.id,
                    info.auctiontype,
                    info.auctionid,
                    info.name,
                    info.organizername,
                    info.organizerphone,
                    info.customerservicephone,
                    info.matchid,
                    info.startranking,
                    info.endranking,
                    info.count,
                    info.sorttype,
                    info.startprice,
                    info.sort,
                    info.createadminid,
                    info.createtime,
                    info.status_name #公棚表联查
                ))
                conn.commit()
                self.logger.log_info(f"插入 section 成功，ID={info.id}")
            except mysql.connector.Error as e:
                self.logger.log_error(f"插入 section 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    @retry_on_exception()
    def batch_upsert(self, items: List[SectionInfo]):
        """
        批量插入或更新 SectionInfo 数据（自动 upsert），并同步 status_name 字段
        """
        if not items:
            return

        auctionid_list = list(set(item.auctionid for item in items))
        status_map = self.gongpeng_dao.get_status_name_map(auctionid_list)
        upsert_sql = """
                     INSERT INTO section_info (id, auctiontype, auctionid, name, organizername, organizerphone,
                                               customerservicephone, matchid, startranking, endranking, count,
                                               sorttype, startprice, sort, createadminid, createtime, status_name)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                     ON DUPLICATE KEY UPDATE auctiontype=VALUES(auctiontype),
                                             auctionid=VALUES(auctionid),
                                             name=VALUES(name),
                                             organizername=VALUES(organizername),
                                             organizerphone=VALUES(organizerphone),
                                             customerservicephone=VALUES(customerservicephone),
                                             matchid=VALUES(matchid),
                                             startranking=VALUES(startranking),
                                             endranking=VALUES(endranking),
                                             count=VALUES(count),
                                             sorttype=VALUES(sorttype),
                                             startprice=VALUES(startprice),
                                             sort=VALUES(sort),
                                             createadminid=VALUES(createadminid),
                                             createtime=VALUES(createtime),
                                             status_name=VALUES(status_name) \
                     """
        values = [
            (
                item.id, item.auctiontype, item.auctionid, item.name, item.organizername, item.organizerphone,
                item.customerservicephone, item.matchid, item.startranking, item.endranking, item.count,
                item.sorttype, item.startprice, item.sort, item.createadminid, item.createtime,
                status_map.get(item.auctionid, "")  # 保证为字符串
            )
            for item in items
        ]
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(upsert_sql, values)
                conn.commit()
                self.logger.log_info(f"批量 upsert section 成功，共 {len(items)} 条记录")
            except mysql.connector.Error as e:
                self.logger.log_error(f"批量 upsert section 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()
    def get_by_id(self, id: int) -> SectionInfo | None:
        """
    根据ID获取 SectionInfo
    """
        select_sql = """
                 SELECT id,
                        auctiontype,
                        auctionid,
                        name,
                        organizername,
                        organizerphone,
                        customerservicephone,
                        matchid,
                        startranking,
                        endranking,
                        count,
                        sorttype,
                        startprice,
                        sort,
                        createadminid,
                        createtime,
                        status_name
                 FROM section_info
                 WHERE id = %s
                 """
        with self.connection_ctx() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(select_sql, (id,))
            row = cursor.fetchone()
            cursor.close()
        if not row:
            return None
        else:
            return SectionInfo(**row)

    def get_all_ids(self) -> list[Any] | None:
        """
        获取所有 section 的 id 列表
        """
        sql = "SELECT id FROM section_info"
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                return [row[0] for row in result]
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取所有 section id 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def get_all_ids_with_nodone(self) -> list[Any] | None:
        """
        获取所有 section 的 id 列表
        """
        sql = "SELECT id FROM section_info Where status_name = '进行中';"
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                return [row[0] for row in result]
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取所有 section id 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def get_id_and_sectionid(self) -> List[dict]:
        """
        获取所有 section 的 id 和 sectionid 列表
        """
        sql = "SELECT id, auctionid FROM section_info"  # 假设 section_info 表中有 sectionid 字段
        result_list = []

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                # 将结果存储在列表中，每个元素是一个字典
                for row in result:
                    result_list.append({"gongpeng_id": row[1], "section_id": row[0]})  # row[0] 是 id，row[1] 是 sectionid
                return result_list
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取所有 section id 和 sectionid 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()
    def get_id_and_sectionid_nodone(self) -> List[dict]:
        """
        获取所有未完成的 section 的 id 和 sectionid 列表
        """
        sql = "SELECT id, auctionid FROM section_info WHERE status_name = '进行中'"  # 假设 section_info 表中有 sectionid 字段
        result_list = []

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                # 将结果存储在列表中，每个元素是一个字典
                for row in result:
                    result_list.append({"gongpeng_id": row[1], "section_id": row[0]})  # row[0] 是 id，row[1] 是 sectionid
                return result_list
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取所有 section id 和 sectionid 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()