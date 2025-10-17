import unittest
from crawlers.sections_clawler import SectionCrawler



def test_crawler():
    secton = SectionCrawler()
    secton.fetch_sections(263)