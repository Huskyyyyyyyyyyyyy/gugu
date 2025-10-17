import time

from crawlers.gongpeng_crawler import GongPengCrawler
from crawlers.pigeon_crawler import PigeonCrawler
from dao.pigeon_dao import PigeonDao
from dao.gongpeng_dao import GongpengDao
from dao.section_dao import SectionDao
from tools.config_loader import load_config
from crawlers.sections_clawler import SectionCrawler


mysqlconfig=load_config('mysqlconfig','config\\db_config.yaml')



def all_gugu():
    """
    总流程
    """

    # 初始化数据库和表
    gongpeng_dao = GongpengDao(**mysqlconfig)
    section_dao = SectionDao(**mysqlconfig)
    pigeons_dao = PigeonDao(**mysqlconfig)
    gongpeng_dao.ensure_table()
    section_dao.ensure_table()
    pigeons_dao.ensure_table_pigeon_info()

    #初始化网络访问
    gongpeng_clawler = GongPengCrawler()
    section_crawler = SectionCrawler()
    pigeons_crawler = PigeonCrawler()
    #循环抓取
    while True:
        #获取更新公棚表
        gongpeng = gongpeng_clawler.crawl()
        gongpeng_dao.batch_upsert_and_status(gongpeng)
        #查询未完成的拍卖公棚
        unfinished_gp_ids = gongpeng_dao.get_unfinished_ids()
        #并发抓取未完成的拍卖列并更新section表
        unfinished_sections = section_crawler.fetchall_sections(unfinished_gp_ids)
        section_dao.batch_upsert(unfinished_sections)
        #查询未完成的section项，更新鸽子表
        id_and_sectionid_nodone = section_dao.get_id_and_sectionid_nodone()
        pigeons = pigeons_crawler.fetchall_pigeons(id_and_sectionid_nodone)
        pigeons_dao.insert_or_update_pigeon_info_batch(pigeons)

        time.sleep(5)


if __name__ == "__main__":
 all_gugu()