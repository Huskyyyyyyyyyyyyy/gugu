# demo_quickstart.py
from crawlers.gongpeng_crawler import GongpengCrawler  # 你的类文件名自行替换

def main():
    # 如果 config\spider.yaml 里已经配置了 gongpeng 段落（见下文 YAML 示例），这行就够了
    crawler = GongpengCrawler(
        config_path=r"config\spider.yaml",
        config_section="gongpeng",
        )
    items = crawler.crawl_all()
    print("总条数：", len(items))
    print("预览：", items[0].__dict__ if items else None)

if __name__ == "__main__":
    main()
