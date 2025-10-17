
from typing import List, Any
import mysql.connector
import json  # 导入 json 模块
from gugu2.commons.base_db import BaseDB
from gugu2.mydataclass.gongpeng import GongpengInfo  # 修正导入路径

class GongpengDao(BaseDB):
    """
    GongpengDAO 类提供了对 GongpengInfo 对象进行数据库操作的方法。
    包括插入、更新、删除和查询拍卖信息等操作。
    """

    def __init__(self, **kwargs):
        """
        初始化 公棚DAO 并建立数据库连接。
        """
        super().__init__(**kwargs)



    def ensure_table(self):
        """
        确保数据库中存在 gongpeng_info 表，若不存在则创建。
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS gongpeng_info (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            organizer_name VARCHAR(255),
            organizer_phone VARCHAR(50),
            customer_service_phone VARCHAR(50),
            start_time BIGINT,
            end_time BIGINT,
            status_name VARCHAR(50),
            live_status_name VARCHAR(50)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """

        with self.connection_ctx() as connection:#使用父类上下文管理器进行数据库操作
            cursor = connection.cursor(dictionary=True)#创建数据库操作对象，开启游标（建表没必要奥）
            try:
                cursor.execute(create_table_sql)
                self.logger.log_info("已确保 gongpeng_info 表存在")
            except mysql.connector.Error as e:
                self.logger.log_error(f"创建 gongpeng_info 表失败: {e}", exc_info=True)
            finally:
                cursor.close()



    def insert_gongpeng(self, info: GongpengInfo):
        """
        插入一条公棚拍卖信息
        """
        insert_sql = """
                     INSERT INTO gongpeng_info (id, name, organizer_name, organizer_phone, customer_service_phone,
                                                start_time, end_time, status_name, live_status_name
                                                )
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(insert_sql, (
                    info.id,
                    info.name,
                    info.organizername,
                    info.organizerphone,
                    info.customerservicephone,
                    info.starttime,
                    info.endtime,
                    info.statusname,
                    info.livestatusname
                ))
                conn.commit()
                self.logger.log_info(f"插入拍卖信息成功，ID={info.id}")
            except mysql.connector.Error as e:
                self.logger.log_error(f"插入拍卖信息失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def get_by_id(self, gongpeng_id: int):
        """
        根据ID获取拍卖信息
        """
        select_sql = "SELECT id, name, organizer_name, organizer_phone, customer_service_phone, start_time, end_time, status_name, live_status_name FROM gongpeng_info WHERE id = %s"

        with self.connection_ctx() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(select_sql, (gongpeng_id,))
            row = cursor.fetchone()
            cursor.close()

        if not row:
            return None

        return GongpengInfo(
            id=row['id'],
            name=row['name'],
            organizername=row['organizer_name'],
            organizerphone=row['organizer_phone'],
            customerservicephone=row['customer_service_phone'],
            starttime=row['start_time'],
            endtime=row['end_time'],
            statusname=row['status_name'],
            livestatusname=row['live_status_name']

        )

    def delete_by_id(self, gongpeng_id: int):
        """
        根据ID删除拍卖信息
        """
        delete_sql = "DELETE FROM gongpeng_info WHERE id = %s"
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(delete_sql, (gongpeng_id,))
                conn.commit()
                self.logger.log_info(f"删除拍卖信息成功，ID={gongpeng_id}")
            except mysql.connector.Error as e:
                self.logger.log_error(f"删除拍卖信息失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()


    def update_gongpeng(self, info: GongpengInfo):
        """
        更新拍卖信息（不包含鸽子图片字段）
        """
        update_sql = """
        UPDATE gongpeng_info SET
            name=%s, organizer_name=%s, organizer_phone=%s, customer_service_phone=%s,
            start_time=%s, end_time=%s, status_name=%s, live_status_name=%s

        WHERE id=%s
        """

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(update_sql, (
                    info.name,
                    info.organizername,
                    info.organizerphone,
                    info.customerservicephone,
                    info.starttime,
                    info.endtime,
                    info.statusname,
                    info.livestatusname,
                    info.id
                ))
                conn.commit()
                self.logger.log_info(f"更新拍卖信息成功，ID={info.id}")
            except mysql.connector.Error as e:
                self.logger.log_error(f"更新拍卖信息失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def batch_upsert(self, items: List[GongpengInfo]):
        """
        批量插入或更新拍卖信息数据（自动 upsert）

        功能：
        - 对于每条数据，如果主键 `id` 不存在，则执行插入
        - 如果已存在，则执行更新操作（覆盖其字段值）

        使用 MySQL 的 INSERT ... ON DUPLICATE KEY UPDATE 实现，保证效率和原子性

        参数:
            items (List[GongpengInfo]): 需要插入或更新的拍卖信息列表
        """
        if not items:
            return

        # SQL模板：主键冲突（duplicate）时更新字段
        upsert_sql = """
                     INSERT INTO gongpeng_info (id, name, organizer_name, organizer_phone, customer_service_phone,
                                                start_time, end_time, status_name, live_status_name
                                                )
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                     ON DUPLICATE KEY UPDATE name=VALUES(name),
                                             organizer_name=VALUES(organizer_name),
                                             organizer_phone=VALUES(organizer_phone),
                                             customer_service_phone=VALUES(customer_service_phone),
                                             start_time=VALUES(start_time),
                                             end_time=VALUES(end_time),
                                             status_name=VALUES(status_name),
                                             live_status_name=VALUES(live_status_name)
                                             
                     """

        # 提取所有值，顺序需与 SQL 字段完全一致
        values = [
            (
                item.id,
                item.name,
                item.organizername,
                item.organizerphone,
                item.customerservicephone,
                item.starttime,
                item.endtime,
                item.statusname,
                item.livestatusname,

            )
            for item in items
        ]

        # 数据库执行上下文：自动获取并释放连接
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                # 一次性执行所有 upsert 操作
                cursor.executemany(upsert_sql, values)
                conn.commit()
                self.logger.log_info(f"批量 upsert 成功，共 {len(items)} 条记录")
            except mysql.connector.Error as e:
                self.logger.log_error(f"批量 upsert 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def batch_upsert_and_status(self, items: List[GongpengInfo]):
        """
        批量插入或更新拍卖信息数据（自动 upsert）

        同时标记未在本次 upsert 中的记录为“已完成”
        """
        if not items:
            return

        upsert_sql = """
                     INSERT INTO gongpeng_info (id, name, organizer_name, organizer_phone, customer_service_phone, \
                                                start_time, end_time, status_name, live_status_name) \
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                     ON DUPLICATE KEY UPDATE name=VALUES(name), \
                                             organizer_name=VALUES(organizer_name), \
                                             organizer_phone=VALUES(organizer_phone), \
                                             customer_service_phone=VALUES(customer_service_phone), \
                                             start_time=VALUES(start_time), \
                                             end_time=VALUES(end_time), \
                                             status_name=VALUES(status_name), \
                                             live_status_name=VALUES(live_status_name) \
                     """

        values = [
            (
                item.id,
                item.name,
                item.organizername,
                item.organizerphone,
                item.customerservicephone,
                item.starttime,
                item.endtime,
                item.statusname,
                item.livestatusname,
            )
            for item in items
        ]

        ids = [item.id for item in items]  # 当前同步的所有 ID

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                # 执行批量 upsert
                cursor.executemany(upsert_sql, values)

                # 更新未出现在本次 upsert 中的记录状态
                if ids:
                    update_sql = f"""
                        UPDATE gongpeng_info
                        SET status_name = '已完成'
                        WHERE id NOT IN ({','.join(['%s'] * len(ids))})
                    """
                    cursor.execute(update_sql, ids)

                conn.commit()
                self.logger.log_info(f"批量 upsert 成功，共 {len(items)} 条记录")
            except mysql.connector.Error as e:
                self.logger.log_error(f"批量 upsert 失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()


    def make_status_as_finished(self, current_ids:List[int]):
        """
            将数据库中不在当前抓取结果中的记录标记为“已完成”。

            参数:
                current_ids (List[int]): 本次抓取结果中所有的拍卖活动 ID 列表
            """
        if not current_ids:
            self.logger.log_info("当前 ID 列表为空，跳过更新")
            return
        sql = """
        UPDATE gongpeng_info 
        SET status_name='已完成'
        WHERE id NOT IN ({})    
        """.format(','.join(['%s'] * len(current_ids)))


        try:
            with self.connection_ctx() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, current_ids)
                conn.commit()
                self.logger.log_info(f"更新 {len(current_ids)} 条公棚拍卖状态")
                cursor.close()
        except mysql.connector.Error as e:
            self.logger.log_error(f"更新公棚缺失记录失败 {e}", exc_info=True)

    def get_all_ids(self) -> list[Any] | None:
        """
        获取数据库中所有拍卖活动的 ID 列表

        返回:
            List[int]: 所有已存在于数据库中的拍卖 ID 列表
        """
        sql = "SELECT id FROM gongpeng_info"

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                return [row[0] for row in result]
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取所有拍卖ID失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def get_unfinished_ids(self) -> list[Any] | None:
        """
        获取状态为“未完成”的拍卖活动 ID 列表

        返回:
            List[int]: 所有状态为“已完成”的拍卖 ID 列表
        """
        sql = "SELECT id FROM gongpeng_info WHERE status_name != '已完成'"

        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchall()
                return [row[0] for row in result]
            except mysql.connector.Error as e:
                self.logger.log_error(f"获取未完成拍卖ID失败: {e}", exc_info=True)
                raise
            finally:
                cursor.close()

    def get_status_name_by_auctionid(self, auctionid: int) -> str:
        """
        获取拍卖完成状态

        auctionid:公棚的拍卖id
        返回：
            状态列（完成进行）
        """
        sql = "SELECT status_name FROM gongpeng_info WHERE id = %s"
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (auctionid,))
            row = cursor.fetchone()
            cursor.close()
        return row[0] if row else ""

    def get_status_name_map(self, auctionid_list: list[int]) -> dict:
        """
        批量获取 {auctionid: status_name} 映射
        """
        if not auctionid_list:
            return {}
        sql = f"SELECT id, status_name FROM gongpeng_info WHERE id IN ({','.join(['%s'] * len(auctionid_list))})"
        with self.connection_ctx() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, auctionid_list)
            result = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
        return result