# -*- coding: utf-8 -*-
"""
PigeonDao (优化版)
-----------------
针对你给出的 PigeonDao 做了如下增强与修复：
1) **修复 with 作用域错误**：`ensure_table_pigeon_info` 中 `cursor.execute` 被放在 with 块之外，可能在连接已关闭后执行导致失败；已修正。
2) **批量 UPSERT 字段缺失**：`insert_or_update_pigeon_info_batch` 的列清单缺少 `image` 字段（单条插入包含，批量不包含），已补齐并同步到 `ON DUPLICATE KEY UPDATE`。
3) **类型标注/返回 Optional**：`get_pigeon_info_by_id` 可能返回 None，应标注 `Optional[PigeonInfo]`。
4) **DDL 更健壮**：显式 `ENGINE / CHARSET`，执行后 `commit()`；并建议性索引（可按需打开）。
5) **游标/连接安全**：所有 DB 操作均在 with 作用域内执行，异常路径也不会泄露资源。
6) **结构去重与常量化**：列名集中在常量，避免单条/批量 SQL 列不同步；插入数据统一由辅助方法 `_row_from_model` 生成，避免重复代码与漏字段。
7) **日志更一致**：捕获异常时附带上下文。
8) **时间格式化**：`format_datetime` 增强类型标注；仅当值为 datetime 时才格式化。

说明：
- 下面保留了你的字段设计（部分时间类字段仍为 VARCHAR，若可迁移建议改为 DATETIME/TIMESTAMP）。
- BaseLogger 假定提供 `log_info/log_error` 方法；如果你 prefer 原生 `logging.Logger`，可在此处替换为 `self.logger = BaseLogger(__name__).logger` 并改用 `self.logger.info/error`。
"""
from __future__ import annotations

import time
from dataclasses import asdict, is_dataclass
from pprint import pprint
from typing import Dict, Any, List, Optional, Tuple, Sequence
from datetime import datetime

from commons.base_db import BaseDB
from commons.base_logger import BaseLogger
from mydataclass.pigeon import PigeonInfo


class PigeonDao(BaseDB):
    """PigeonDao 提供 PigeonInfo 对象的数据库操作。"""

    # 统一维护列顺序，避免单条/批量列不一致导致出错
    _COLUMNS: List[str] = [
        "id", "code", "auction_id", "auction_type", "margin_ratio", "section_id",
        "name", "ranking", "competition_id", "competition_name", "match_id", "match_name",
        "gugu_pigeon_id", "foot_ring", "feather_color", "matcher_name", "start_price",
        "image",  # 注意：批量之前缺失该字段，已补齐
        "sort", "client_sort", "is_current", "status", "create_time", "status_time",
        "view_count", "start_time", "end_time", "status_name", "organizer_name",
        "organizer_phone", "order_status", "order_status_name", "is_watched", "remark",
        "ws_remark", "bid_id", "quote", "bid_type", "bid_time", "bid_user_id",
        "bid_user_code", "bid_user_nickname", "bid_user_avatar", "bid_count", "order_id",
        "create_admin_id", "specified_count", "specified_sync",
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = BaseLogger(__name__)

    # ------------------------------
    # Schema 管理
    # ------------------------------
    def ensure_table_pigeon_info(self) -> None:
        """确保 pigeon_info 表存在；若无则创建，并检查常用索引（MySQL）。"""
        try:
            with self.connection_ctx() as conn:
                with conn.cursor() as cur:
                    # --- 检查表是否已存在 ---
                    cur.execute("SHOW TABLES LIKE 'pigeon_info'")
                    table_exists = cur.fetchone() is not None

                    # --- 创建表 ---
                    if not table_exists:
                        create_table_sql = """
                        CREATE TABLE pigeon_info (
                        id INT PRIMARY KEY,
                        code VARCHAR(255),
                        auction_id INT,
                        auction_type VARCHAR(255),
                        margin_ratio DECIMAL(10, 2),
                        section_id INT,
                        name VARCHAR(255),
                        ranking INT,
                        competition_id INT,
                        competition_name VARCHAR(255),
                        match_id INT,
                        match_name VARCHAR(255),
                        gugu_pigeon_id VARCHAR(255),
                        foot_ring VARCHAR(255),
                        feather_color VARCHAR(255),
                        matcher_name VARCHAR(255),
                        start_price DECIMAL(10, 2),
                        image VARCHAR(255),
                        sort INT,
                        client_sort INT,
                        is_current BOOLEAN,
                        status VARCHAR(255),
                        create_time VARCHAR(255),
                        status_time VARCHAR(255),
                        view_count INT,
                        start_time VARCHAR(255),
                        end_time VARCHAR(255),
                        status_name VARCHAR(255),
                        organizer_name VARCHAR(255),
                        organizer_phone VARCHAR(255),
                        order_status VARCHAR(255),
                        order_status_name VARCHAR(255),
                        is_watched BOOLEAN,
                        remark TEXT,
                        ws_remark TEXT,
                        bid_id INT,
                        quote DECIMAL(10, 2),
                        bid_type VARCHAR(255),
                        bid_time VARCHAR(255),
                        bid_user_id INT,
                        bid_user_code VARCHAR(255),
                        bid_user_nickname VARCHAR(255),
                        bid_user_avatar VARCHAR(255),
                        bid_count INT,
                        order_id INT,
                        create_admin_id INT,
                        specified_count INT,
                        specified_sync BOOLEAN
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                        cur.execute(create_table_sql)
                        self.logger.log_info("已创建 pigeon_info 表")
                    else:
                        self.logger.log_info("表 pigeon_info 已存在，跳过创建")

                    # --- 索引检测并按需创建 ---
                    indexes = [
                        ("idx_pigeon_info_user_id", "bid_user_id"),
                        # 可按需扩展：
                        # ("idx_pigeon_info_auction_id", "auction_id"),
                        # ("idx_pigeon_info_match_id", "match_id"),
                        # ("idx_pigeon_info_status", "status"),
                        ]

                    for name, col in indexes:
                        if not self._mysql_index_exists(cur, "pigeon_info", name):
                            cur.execute(f"CREATE INDEX {name} ON pigeon_info({col})")
                            self.logger.log_info(f"已创建索引 {name}")
                        else:
                            self.logger.log_info(f"索引 {name} 已存在，跳过")

                conn.commit()

        except Exception as e:
            self.logger.log_error(f"检查/创建 pigeon_info 表或索引失败: {e}", exc_info=True)

    def _mysql_index_exists(self, cursor, table_name: str, index_name: str) -> bool:
        """检查 MySQL 表中索引是否存在。"""
        sql = f"SHOW INDEX FROM {table_name} WHERE Key_name = %s"
        cursor.execute(sql, (index_name,))
        return cursor.fetchone() is not None

    # ------------------------------
    # 写入：单条
    # ------------------------------
    def insert_pigeon_info(self, pigeon_info: PigeonInfo) -> None:
        """插入一条鸽子信息到 pigeon_info 表。"""
        placeholders = ", ".join([f"%({c})s" for c in self._COLUMNS])
        col_list = ", ".join(self._COLUMNS)
        insert_sql = f"""
            INSERT INTO pigeon_info ({col_list})
            VALUES ({placeholders});
        """
        data = self._row_from_model(pigeon_info)
        try:
            with self.connection_ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, data)
                conn.commit()
            self.logger.log_info("成功插入鸽子信息到 pigeon_info 表")
        except Exception as e:
            # 如果是主键冲突，这里只记录错误。需要 Upsert 行为请调用批量接口或单独实现 ON DUPLICATE KEY 版本
            self.logger.log_error(f"插入鸽子信息失败: {e}", exc_info=True)

    # ------------------------------
    # 读取：单条
    # ------------------------------
    def get_pigeon_info_by_id(self, pigeon_id: int) -> Optional[PigeonInfo]:
        """根据 ID 查询，返回 PigeonInfo 或 None。"""
        sql = "SELECT * FROM pigeon_info WHERE id = %s"
        try:
            with self.connection_ctx() as conn:
                with conn.cursor(dictionary=True) as cur:
                    cur.execute(sql, (pigeon_id,))
                    row = cur.fetchone()
            if row:
                return PigeonInfo(**row)
            return None
        except Exception as e:
            self.logger.log_error(f"根据 ID 查询鸽子信息失败: {e}", exc_info=True)
            return None

    # ------------------------------
    # 写入：批量（带 UPSERT）
    # ------------------------------
    def insert_or_update_pigeon_info_batch(
            self,
            pigeon_info_list: List[PigeonInfo],
            *,
            batch_size: int = 1000,
            ) -> None:
        """
        高吞吐批量 UPSERT（复用同一连接/游标 + 多值插入 + 重试，MySQL 8.0.20+ 友好）。
        """
        if not pigeon_info_list:
            return

        # 建议按主键排序，降低页分裂/锁冲突
        try:
            pigeon_info_list.sort(key=lambda x: x.id)
        except Exception:
            pass

        cols = self._COLUMNS
        col_list = ", ".join(cols)
        # 避免 VALUES() 弃用：使用 VALUES ... AS NEW，再在 UPDATE 中引用 NEW.col
        update_clause = ", ".join([f"{c} = NEW.{c}" for c in cols if c != "id"])

        def _chunks(seq, size):
            for i in range(0, len(seq), size):
                yield seq[i: i + size]

        try:
            with self.connection_ctx() as conn:
                with conn.cursor() as cur:
                    for chunk in _chunks(pigeon_info_list, batch_size):
                        rows = [self._row_from_model(p) for p in chunk]
                        placeholders_one = "(" + ", ".join(["%s"] * len(cols)) + ")"
                        placeholders_all = ", ".join([placeholders_one] * len(rows))
                        params = [r[c] for r in rows for c in cols]
                        sql = f"""
    INSERT INTO pigeon_info ({col_list})
    VALUES {placeholders_all} AS NEW
    ON DUPLICATE KEY UPDATE {update_clause};
    """
                        self._exec_on_cursor_with_retry(conn, cur, sql, params)
                conn.commit()
            self.logger.log_info(f"批量插入或更新鸽子信息成功，共 {len(pigeon_info_list)} 条")
        except Exception:
            self.logger.log_error("批量 upsert 失败", exc_info=True)
            raise

    def _exec_on_cursor_with_retry(
            self,
            conn,
            cur,
            sql: str,
            params: List,
            *,
            max_retries: int = 3,
            base_sleep: float = 0.2,
            ) -> None:
        """在给定 cursor 上执行 SQL，遇死锁/锁等待回滚并指数退避重试。"""
        attempt = 0
        while True:
            try:
                cur.execute(sql, params)
                return
            except Exception as e:
                err_no = getattr(e, "errno", None)
                if err_no in (1205, 1213) and attempt < max_retries:
                    sleep = base_sleep * (2 ** attempt)
                    self.logger.log_error(
                        f"写入冲突（{err_no}），第 {attempt + 1} 次重试，等待 {sleep:.2f}s",
                        exc_info=True,
                        )
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    import time as _t

                    _t.sleep(sleep)
                    attempt += 1
                    continue
                self.logger.log_error(f"批量执行失败（已重试{attempt}次）: {e}", exc_info=True)
                raise

    def _exec_with_retry(self, sql: str, params: List, *, max_retries: int = 3, base_sleep: float = 0.2) -> None:
        """执行 SQL，遇到死锁/锁等待超时重试（指数退避）"""
        attempt = 0
        while True:
            try:
                with self.connection_ctx() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params)
                    conn.commit()
                return
            except Exception as e:
                # 识别 MySQL 死锁/锁等待错误码（根据你的驱动调整）
                err_no = getattr(e, "errno", None)
                if err_no in (1205, 1213) and attempt < max_retries:  # 1205 Lock wait timeout, 1213 Deadlock found
                    sleep = base_sleep * (2 ** attempt)
                    self.logger.log_error(
                        f"写入冲突（{err_no}），第 {attempt + 1} 次重试，等待 {sleep:.2f}s", exc_info=True
                        )
                    time.sleep(sleep)
                    attempt += 1
                    continue
                # 其他错误或重试耗尽
                self.logger.log_error(f"批量执行失败（已重试{attempt}次）: {e}", exc_info=True)
                raise
    # ------------------------------
    # 辅助：模型转数据行
    # ------------------------------
    @staticmethod
    def format_datetime(dt: Any) -> Any:
        """仅当值为 datetime 时格式化为字符串（数据库目前使用 VARCHAR 存储）。"""
        if isinstance(dt, datetime):
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt

    def _row_from_model(self, pigeon_info: PigeonInfo) -> Dict[str, Any]:
        """将 PigeonInfo 转换为与表列对齐的 dict。避免单条/批量重复书写映射。"""
        # 如果 PigeonInfo 继承了 BaseDataClass，也可以用 pigeon_info.to_dict(drop_none=False)
        # 这里使用 asdict 以最大兼容 dataclass；若非 dataclass 则回退到属性访问。
        row: Dict[str, Any]
        if is_dataclass(pigeon_info):
            row = asdict(pigeon_info)
        else:
            row = {k: getattr(pigeon_info, k) for k in self._COLUMNS if hasattr(pigeon_info, k)}

        # 统一保证列齐全，缺失字段用 None；并格式化时间类字段
        result: Dict[str, Any] = {}
        for col in self._COLUMNS:
            val = row.get(col)
            if col in ("start_time", "end_time"):
                val = self.format_datetime(val)
            result[col] = val if col not in ("remark", "ws_remark") else (val or None)
        return result

    def query_bid_statistics_and_deals(
            self,
            user_codes: Sequence[str],
            auction_id: int,
            *,
            status_whitelist: Tuple[str, ...] = ("已完成", "已结拍"),  # 默认过滤“已完成类”状态
            chunk_size: int = 100,
            ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        """
        查询指定用户（bid_user_code）在所有拍卖场的“已完成”记录，
        并在这些完成记录中，统计：
          1. 当前拍卖场（auction_id）的成交数据；
          2. 所有拍卖场的整体成交数据。

        -----
        【返回值】
        :return: Tuple (statistics, completed_deals)
            - statistics: {
                  user_code: {
                      # 当前拍卖场
                      "deal_count": int,
                      "total_price": float,
                      "highest_price": float,
                      "second_highest_price": float,

                      # 所有拍卖场
                      "deal_count_all": int,
                      "total_price_all": float,
                      "highest_price_all": float,
                      "second_highest_price_all": float,
                  }
              }
            - completed_deals: {user_code: [ {matcher_name, name, foot_ring, quote, auction_id, status_name}, ... ]}
        """

        # --- 参数预处理 ---
        if not user_codes:
            return {}, {}
        if chunk_size <= 0:
            chunk_size = 100

        # 清洗 code：去掉空白字符串
        user_codes = [uc.strip() for uc in user_codes if uc and uc.strip()]
        if not user_codes:
            return {}, {}

        filter_status = bool(status_whitelist)

        # --- 结果容器 ---
        statistics: Dict[str, Dict[str, Any]] = {}
        completed_deals: Dict[str, List[Dict[str, Any]]] = {}

        # --- 查询执行 ---
        with self.connection_ctx() as conn:
            with conn.cursor(dictionary=True) as cursor:
                for i in range(0, len(user_codes), chunk_size):
                    chunk = user_codes[i:i + chunk_size]
                    ph_users = ", ".join(["%s"] * len(chunk))

                    # SQL：按 code + 状态过滤，不限制 auction_id
                    if filter_status:
                        ph_status = ", ".join(["%s"] * len(status_whitelist))
                        sql = f"""
                            SELECT
                                bid_user_code,
                                matcher_name,
                                name,
                                foot_ring,
                                quote,
                                auction_id,
                                status_name
                            FROM pigeon_info
                            WHERE bid_user_code IN ({ph_users})
                              AND status_name IN ({ph_status})
                            ORDER BY bid_user_code, quote DESC
                        """
                        params = [*chunk, *status_whitelist]
                    else:
                        sql = f"""
                            SELECT
                                bid_user_code,
                                matcher_name,
                                name,
                                foot_ring,
                                quote,
                                auction_id,
                                status_name
                            FROM pigeon_info
                            WHERE bid_user_code IN ({ph_users})
                            ORDER BY bid_user_code, quote DESC
                        """
                        params = [*chunk]

                    cursor.execute(sql, params)

                    # --- 遍历结果 ---
                    for row in cursor.fetchall():
                        uc = row["bid_user_code"]
                        q = float(row["quote"]) if row["quote"] is not None else 0.0

                        # 收集所有“已完成类”成交记录（跨全部拍卖场）
                        completed_deals.setdefault(uc, []).append(
                            {
                                "matcher_name": row["matcher_name"],
                                "name": row["name"],
                                "foot_ring": row["foot_ring"],
                                "quote": q,
                                "auction_id": row["auction_id"],
                                "status_name": row["status_name"],
                                }
                            )

                        # 初始化该用户的统计数据（当前拍卖场 + 全部拍卖场）
                        user_data = statistics.setdefault(
                            uc,
                            {
                                # 当前拍卖场
                                "deal_count": 0,
                                "total_price": 0.0,
                                "highest_price": 0.0,
                                "second_highest_price": 0.0,

                                # 所有拍卖场
                                "deal_count_all": 0,
                                "total_price_all": 0.0,
                                "highest_price_all": 0.0,
                                "second_highest_price_all": 0.0,
                                },
                            )

                        # --- 先更新【全部拍卖场】统计 ---
                        user_data["deal_count_all"] += 1
                        user_data["total_price_all"] += q
                        if q > user_data["highest_price_all"]:
                            user_data["second_highest_price_all"] = user_data["highest_price_all"]
                            user_data["highest_price_all"] = q
                        elif q > user_data["second_highest_price_all"]:
                            user_data["second_highest_price_all"] = q

                        # --- 再更新【当前拍卖场】统计 ---
                        if row["auction_id"] == auction_id:
                            user_data["deal_count"] += 1
                            user_data["total_price"] += q
                            if q > user_data["highest_price"]:
                                user_data["second_highest_price"] = user_data["highest_price"]
                                user_data["highest_price"] = q
                            elif q > user_data["second_highest_price"]:
                                user_data["second_highest_price"] = q

        return statistics, completed_deals


if __name__ == "__main__":
    pgdao = PigeonDao()
    deals,s = pgdao.query_bid_statistics_and_deals(
        user_codes=["GUGU008CQT367"], status_whitelist=("已完成", "已结拍"), chunk_size=100, auction_id=343
        )
    pprint(deals.items())
    pprint(s.items())
