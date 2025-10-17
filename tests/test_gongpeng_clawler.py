from pprint import pprint

from dao.pigeon_dao import PigeonDao
from gugu2.clawlers.pigeon_crawler import PigeonCrawler
from crawlers.gongpeng_crawler import GongPengCrawler
import unittest
from dao.gongpeng_dao import GongpengDao
from dao.section_dao import SectionDao
from tools.config_loader import load_config
from crawlers.sections_clawler import SectionCrawler

mysqlconfig=load_config('mysqlconfig','config\\db_config.yaml')



def gongpeng_crawler():
    # gongpeng_dao = GongpengDAO(mysqlconfig)
    # gongpeng_dao.ensure_table()
    pprint(mysqlconfig)
    section_dao = SectionDao(**mysqlconfig)
    section_dao.ensure_table()
    pigeon_dao = PigeonDao(**mysqlconfig)
    pigeon_dao.ensure_table_pigeon_info()


    # ids = gongpeng_dao.get_all_ids()
    # gongpeng = GongPengCrawler()
    # gongpeng_info = gongpeng.crawl()
    # gongpeng_dao.batch_upsert(gongpeng_info)
    gid_and_sid = (section_dao.get_id_and_sectionid_nodone())


    pigeonclawer = PigeonCrawler()
    pigeons = pigeonclawer.fetchall_pigeons(gid_and_sid)
    pigeon_dao.insert_or_update_pigeon_info_batch(pigeons)



    #
    # secton = SectionCrawler()
    # ids = gongpeng_dao.get_all_ids()
    # sections = secton.crawl_sections(ids)
    # section_dao.batch_upsert(sections)

    # pprint(sections)





    # gongpeng_dao.batch_upsert(crawl)
    # gongpeng_dao.make_status_as_finished(ids)

if __name__ == '__main__':
    gongpeng_crawler()