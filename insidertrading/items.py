# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CompaniesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    company_name=scrapy.Field()
    path=scrapy.Field()
    insider_trades_weblink=scrapy.Field()

class InsiderTradesItem(scrapy.Item):
    company_id=scrapy.Field()
    date=scrapy.Field()
    trader=scrapy.Field()
    quantity=scrapy.Field()
    short_val=scrapy.Field()
    type=scrapy.Field()

