import time

from crawlers.gongpeng_crawler import GongpengCrawler
from crawlers.pigeon_crawler import PigeonCrawler
from dao.pigeon_dao import PigeonDao

from tools.config_loader import load_config
from crawlers.sections_clawler import SectionCrawler


mysqlconfig=load_config('mysqlconfig','config\\db_config.yaml')



def all_gugu():
     # 1) 拿拍卖列表
    with GongpengCrawler() as gp_crawler:
        gp_list = gp_crawler.crawl_all()
        ids = [info.id for info in gp_list]

    # 2) 并发抓各拍卖的 section
    with SectionCrawler() as sc:
        sections = sc.fetchall_sections(ids)

    section_id_and_gongpeng_id = [
        {"section_id": s.id, "gongpeng_id": s.auction_id}
        for s in sections
        ]
    pc = PigeonCrawler()
    pigeons = pc.fetchall_pigeons(section_id_and_gongpeng_id)
    # pprint(pigeons)
    from dao.pigeon_dao import PigeonDao
    pgDao = PigeonDao(**mysqlconfig)
    pgDao.ensure_table_pigeon_info()
    pgDao.insert_or_update_pigeon_info_batch(pigeons)

def one_gugu():
    with SectionCrawler() as sc:
        sections = sc.fetchall_sections([345])

    section_id_and_gongpeng_id = [
        {"section_id": s.id, "gongpeng_id": s.auction_id}
        for s in sections
        ]
    pc = PigeonCrawler()
    pigeons = pc.fetchall_pigeons(section_id_and_gongpeng_id)
    # pprint(pigeons)
    from dao.pigeon_dao import PigeonDao
    pgDao = PigeonDao(**mysqlconfig)
    # pgDao.ensure_table_pigeon_info()
    pgDao.insert_or_update_pigeon_info_batch(pigeons)

if __name__ == "__main__":
 while True:
     all_gugu()
     time.sleep(60*60)