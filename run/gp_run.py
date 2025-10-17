from crawlers.gongpeng_crawler import GongPengCrawler
from dao.gongpeng_dao import GongpengDAO
from tools.config_loader import load_config

mysqlconfig=load_config('mysqlconfig','config\\db_config.yaml')
def main():
    gongpeng_crawler=GongPengCrawler()
    gongpeng_dao=GongpengDAO(mysqlconfig)
    fetch = gongpeng_crawler.crawl()
    gongpeng_dao.batch_upsert_and_status(fetch)



if __name__ == '__main__':
    main()








