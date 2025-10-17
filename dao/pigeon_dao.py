from datetime import datetime

from commons.base_db import BaseDB
from commons.base_logger import BaseLogger
from mydataclass.pigeon import PigeonInfo





class PigeonDao(BaseDB):
    """
    PigeonDao提供 PigeonInfo 对象的数据库操作。
    """

    def __init__(self,**kwargs):
        """
        构造函数
        """
        super().__init__(**kwargs)
        self.logger = BaseLogger(__name__)

    def ensure_table_pigeon_info(self):
        """
        确保 PigeonInfo 表存在
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS pigeon_info (
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
    ) ENGINE = InnoDB 
      DEFAULT CHARSET = utf8mb4;
        """
        with self.connection_ctx() as conn:
            cur = conn.cursor()
        try:
            cur.execute(create_table_sql)
            self.logger.log_info("创建pigeon_info表成功")
        except Exception as e:
            self.logger.log_error(f"创建pigeon_info表失败{e}",exc_info=True)


    def insert_pigeon_info(self, pigeon_info: PigeonInfo):
        """
        插入一条鸽子信息到 pigeon_info 表
        :param pigeon_info: PigeonInfo 数据类实例
        """
        insert_sql = """
        INSERT INTO pigeon_info (
            id, code, auction_id, auction_type, margin_ratio, section_id, name, ranking, competition_id, 
            competition_name, match_id, match_name, gugu_pigeon_id, foot_ring, feather_color, matcher_name, 
            start_price, image, sort, client_sort, is_current, status, create_time, status_time, view_count, 
            start_time, end_time, status_name, organizer_name, organizer_phone, order_status, order_status_name, 
            is_watched, remark, ws_remark, bid_id, quote, bid_type, bid_time, bid_user_id, bid_user_code, 
            bid_user_nickname, bid_user_avatar, bid_count, order_id, create_admin_id, specified_count, specified_sync
        ) VALUES (
            %(id)s, %(code)s, %(auction_id)s, %(auction_type)s, %(margin_ratio)s, %(section_id)s, %(name)s, %(ranking)s,
            %(competition_id)s, %(competition_name)s, %(match_id)s, %(match_name)s, %(gugu_pigeon_id)s, %(foot_ring)s,
            %(feather_color)s, %(matcher_name)s, %(start_price)s, %(image)s, %(sort)s, %(client_sort)s, %(is_current)s,
            %(status)s, %(create_time)s, %(status_time)s, %(view_count)s, %(start_time)s, %(end_time)s, %(status_name)s,
            %(organizer_name)s, %(organizer_phone)s, %(order_status)s, %(order_status_name)s, %(is_watched)s, %(remark)s,
            %(ws_remark)s, %(bid_id)s, %(quote)s, %(bid_type)s, %(bid_time)s, %(bid_user_id)s, %(bid_user_code)s, 
            %(bid_user_nickname)s, %(bid_user_avatar)s, %(bid_count)s, %(order_id)s, %(create_admin_id)s, 
            %(specified_count)s, %(specified_sync)s
        );
        """
        data = {
            'id': pigeon_info.id,
            'code': pigeon_info.code,
            'auction_id': pigeon_info.auction_id,
            'auction_type': pigeon_info.auction_type,
            'margin_ratio': pigeon_info.margin_ratio,
            'section_id': pigeon_info.section_id,
            'name': pigeon_info.name,
            'ranking': pigeon_info.ranking,
            'competition_id': pigeon_info.competition_id,
            'competition_name': pigeon_info.competition_name,
            'match_id': pigeon_info.match_id,
            'match_name': pigeon_info.match_name,
            'gugu_pigeon_id': pigeon_info.gugu_pigeon_id,
            'foot_ring': pigeon_info.foot_ring,
            'feather_color': pigeon_info.feather_color,
            'matcher_name': pigeon_info.matcher_name,
            'start_price': pigeon_info.start_price,
            'image': pigeon_info.image,
            'sort': pigeon_info.sort,
            'client_sort': pigeon_info.client_sort,
            'is_current': pigeon_info.is_current,
            'status': pigeon_info.status,
            'create_time': pigeon_info.create_time,
            'status_time': pigeon_info.status_time,
            'view_count': pigeon_info.view_count,
            'start_time': self.format_datetime(pigeon_info.start_time),
            'end_time': self.format_datetime(pigeon_info.end_time),
            'status_name': pigeon_info.status_name,
            'organizer_name': pigeon_info.organizer_name,
            'organizer_phone': pigeon_info.organizer_phone,
            'order_status': pigeon_info.order_status,
            'order_status_name': pigeon_info.order_status_name,
            'is_watched': pigeon_info.is_watched,
            'remark': pigeon_info.remark or None,
            'ws_remark': pigeon_info.ws_remark or None,
            'bid_id': pigeon_info.bid_id,
            'quote': pigeon_info.quote,
            'bid_type': pigeon_info.bid_type,
            'bid_time': pigeon_info.bid_time,
            'bid_user_id': pigeon_info.bid_user_id,
            'bid_user_code': pigeon_info.bid_user_code,
            'bid_user_nickname': pigeon_info.bid_user_nickname,
            'bid_user_avatar': pigeon_info.bid_user_avatar,
            'bid_count': pigeon_info.bid_count,
            'order_id': pigeon_info.order_id,
            'create_admin_id': pigeon_info.create_admin_id,
            'specified_count': pigeon_info.specified_count,
            'specified_sync': pigeon_info.specified_sync,
        }
        with self.connection_ctx() as conn:
            cur = conn.cursor()
            try:
                cur.execute(insert_sql, data)
                conn.commit()
                self.logger.log_info("成功插入鸽子信息到 pigeon_info 表")
            except Exception as e:
                self.logger.log_error(f"插入鸽子信息失败: {e}", exc_info=True)

    def get_pigeon_info_by_id(self, pigeon_id: int) -> PigeonInfo:
        """
        根据ID查询 pigeon_info 表，返回 PigeonInfo 实例或 None
        :param pigeon_id: 鸽子ID
        :return: PigeonInfo对象或None
        """
        sql = "SELECT * FROM pigeon_info WHERE id = %s"
        with self.connection_ctx() as conn:
            cur = conn.cursor(dictionary=True)
            try:
                cur.execute(sql, (pigeon_id,))
                row = cur.fetchone()
                if row:
                    return PigeonInfo(**row)
                else:
                    return None
            except Exception as e:
                self.logger.log_error(f"根据ID查询鸽子信息失败: {e}", exc_info=True)
                return None

    def insert_or_update_pigeon_info_batch(self, pigeon_info_list: list):
        """
        批量插入鸽子信息，如果存在相同的 id，则更新数据
        :param pigeon_info_list: List[PigeonInfo] 数据类实例列表
        """
        if not pigeon_info_list:
            return

        # 生成批量插入并更新的 SQL 语句
        insert_or_update_sql = """
                                   INSERT INTO pigeon_info (id, code, auction_id, auction_type, margin_ratio, \
                                                            section_id, name, ranking, competition_id, \
                                                            competition_name, match_id, match_name, gugu_pigeon_id, \
                                                            foot_ring, feather_color, matcher_name, \
                                                            start_price, sort, client_sort, is_current, status, \
                                                            create_time, status_time, view_count, \
                                                            start_time, end_time, status_name, organizer_name, \
                                                            organizer_phone, order_status, order_status_name, \
                                                            is_watched, remark, ws_remark, bid_id, quote, bid_type, \
                                                            bid_time, bid_user_id, bid_user_code, \
                                                            bid_user_nickname, bid_user_avatar, bid_count, order_id, \
                                                            create_admin_id, specified_count, specified_sync) \
                                   VALUES (%(id)s, %(code)s, %(auction_id)s, %(auction_type)s, %(margin_ratio)s, \
                                           %(section_id)s, %(name)s, %(ranking)s, \
                                           %(competition_id)s, %(competition_name)s, %(match_id)s, %(match_name)s, \
                                           %(gugu_pigeon_id)s, %(foot_ring)s, \
                                           %(feather_color)s, %(matcher_name)s, %(start_price)s, %(sort)s, \
                                           %(client_sort)s, %(is_current)s, \
                                           %(status)s, %(create_time)s, %(status_time)s, %(view_count)s, %(start_time)s, \
                                           %(end_time)s, %(status_name)s, \
                                           %(organizer_name)s, %(organizer_phone)s, %(order_status)s, \
                                           %(order_status_name)s, %(is_watched)s, %(remark)s, \
                                           %(ws_remark)s, %(bid_id)s, %(quote)s, %(bid_type)s, %(bid_time)s, \
                                           %(bid_user_id)s, %(bid_user_code)s, \
                                           %(bid_user_nickname)s, %(bid_user_avatar)s, %(bid_count)s, %(order_id)s, \
                                           %(create_admin_id)s, \
                                           %(specified_count)s, %(specified_sync)s) \
                                   ON DUPLICATE KEY UPDATE code              = VALUES(code), \
                                                           auction_id        = VALUES(auction_id), \
                                                           auction_type      = VALUES(auction_type), \
                                                           margin_ratio      = VALUES(margin_ratio), \
                                                           section_id        = VALUES(section_id), \
                                                           name              = VALUES(name), \
                                                           ranking           = VALUES(ranking), \
                                                           competition_id    = VALUES(competition_id), \
                                                           competition_name  = VALUES(competition_name), \
                                                           match_id          = VALUES(match_id), \
                                                           match_name        = VALUES(match_name), \
                                                           gugu_pigeon_id    = VALUES(gugu_pigeon_id), \
                                                           foot_ring         = VALUES(foot_ring), \
                                                           feather_color     = VALUES(feather_color), \
                                                           matcher_name      = VALUES(matcher_name), \
                                                           start_price       = VALUES(start_price), \
                                                           sort              = VALUES(sort), \
                                                           client_sort       = VALUES(client_sort), \
                                                           is_current        = VALUES(is_current), \
                                                           status            = VALUES(status), \
                                                           create_time       = VALUES(create_time), \
                                                           status_time       = VALUES(status_time), \
                                                           view_count        = VALUES(view_count), \
                                                           start_time        = VALUES(start_time), \
                                                           end_time          = VALUES(end_time), \
                                                           status_name       = VALUES(status_name), \
                                                           organizer_name    = VALUES(organizer_name), \
                                                           organizer_phone   = VALUES(organizer_phone), \
                                                           order_status      = VALUES(order_status), \
                                                           order_status_name = VALUES(order_status_name), \
                                                           is_watched        = VALUES(is_watched), \
                                                           remark            = VALUES(remark), \
                                                           ws_remark         = VALUES(ws_remark), \
                                                           bid_id            = VALUES(bid_id), \
                                                           quote             = VALUES(quote), \
                                                           bid_type          = VALUES(bid_type), \
                                                           bid_time          = VALUES(bid_time), \
                                                           bid_user_id       = VALUES(bid_user_id), \
                                                           bid_user_code     = VALUES(bid_user_code), \
                                                           bid_user_nickname = VALUES(bid_user_nickname), \
                                                           bid_user_avatar   = VALUES(bid_user_avatar), \
                                                           bid_count         = VALUES(bid_count), \
                                                           order_id          = VALUES(order_id), \
                                                           create_admin_id   = VALUES(create_admin_id), \
                                                           specified_count   = VALUES(specified_count), \
                                                           specified_sync    = VALUES(specified_sync); \
                               """

        data_list = []
        for pigeon_info in pigeon_info_list:
            # 将 PigeonInfo 对象转换为字典
            data = {
                'id': pigeon_info.id,
                'code': pigeon_info.code,
                'auction_id': pigeon_info.auction_id,
                'auction_type': pigeon_info.auction_type,
                'margin_ratio': pigeon_info.margin_ratio,
                'section_id': pigeon_info.section_id,
                'name': pigeon_info.name,
                'ranking': pigeon_info.ranking,
                'competition_id': pigeon_info.competition_id,
                'competition_name': pigeon_info.competition_name,
                'match_id': pigeon_info.match_id,
                'match_name': pigeon_info.match_name,
                'gugu_pigeon_id': pigeon_info.gugu_pigeon_id,
                'foot_ring': pigeon_info.foot_ring,
                'feather_color': pigeon_info.feather_color,
                'matcher_name': pigeon_info.matcher_name,
                'start_price': pigeon_info.start_price,
                'image': pigeon_info.image,
                'sort': pigeon_info.sort,
                'client_sort': pigeon_info.client_sort,
                'is_current': pigeon_info.is_current,
                'status': pigeon_info.status,
                'create_time': pigeon_info.create_time,
                'status_time': pigeon_info.status_time,
                'view_count': pigeon_info.view_count,
                'start_time': self.format_datetime(pigeon_info.start_time),
                'end_time': self.format_datetime(pigeon_info.end_time),
                'status_name': pigeon_info.status_name,
                'organizer_name': pigeon_info.organizer_name,
                'organizer_phone': pigeon_info.organizer_phone,
                'order_status': pigeon_info.order_status,
                'order_status_name': pigeon_info.order_status_name,
                'is_watched': pigeon_info.is_watched,
                'remark': pigeon_info.remark or None,
                'ws_remark': pigeon_info.ws_remark or None,
                'bid_id': pigeon_info.bid_id,
                'quote': pigeon_info.quote,
                'bid_type': pigeon_info.bid_type,
                'bid_time': pigeon_info.bid_time,
                'bid_user_id': pigeon_info.bid_user_id,
                'bid_user_code': pigeon_info.bid_user_code,
                'bid_user_nickname': pigeon_info.bid_user_nickname,
                'bid_user_avatar': pigeon_info.bid_user_avatar,
                'bid_count': pigeon_info.bid_count,
                'order_id': pigeon_info.order_id,
                'create_admin_id': pigeon_info.create_admin_id,
                'specified_count': pigeon_info.specified_count,
                'specified_sync': pigeon_info.specified_sync,
                }
            data_list.append(data)

        with self.connection_ctx() as conn:
            cur = conn.cursor()
            try:
                cur.executemany(insert_or_update_sql, data_list)
                conn.commit()
                self.logger.log_info("批量插入或更新鸽子信息成功")
            except Exception as e:
                self.logger.log_error(f"批量插入或更新鸽子信息失败: {e}", exc_info=True)

    @staticmethod
    def format_datetime(dt):
        """ 格式化 datetime 对象为字符串 """
        if isinstance(dt, datetime):
            return dt.strftime('%Y-%m-%d %H:%M:%S')  # 返回符合数据库存储的格式
        return dt  # 如果不是 datetime 对象，则直接返回原值