import scrapy


class BankinformSpider(scrapy.Spider):
    name = "bankinform"
    allowed_domains = ["bankinform.ru"]
    start_urls = ["https://bankinform.ru"]

    def parse(self, response):
        pass
