import scrapy
from ..items import CompaniesItem, InsiderTradesItem
import mysql.connector



class InsiderSpiderSpider(scrapy.Spider):
    name = 'companies_spider'
    allowed_domains = ['finanzen.net']
    start_urls = ['https://www.finanzen.net/index/dax/40-werte/']

    
    
    def parse(self, response):
        # run the code once and get list of all companies in DAX40
        # take the name and attach it against https://www.finanzen.net/insidertrades/<company_name>

        i_companies = CompaniesItem()
        
        companies = response.css(".table-hover td:nth-child(1)")

        for company in companies:
            #print(company)
            i_companies['company_name'] = company.css("a::text").extract_first()
            i_companies['path'] = company.css("a::attr(href)").extract_first()
            modified_path = i_companies['path'].replace('/aktien/','').replace('-aktie','')
            i_companies['insider_trades_weblink'] = "https://www.finanzen.net/insidertrades/"+ modified_path

            
            #yield scrapy.Request( i_companies['insider_trades_weblink'],callback=self.insider_data,meta={'company_name':i_companies['company_name']})
            #yield scrapy.Request('https://www.finanzen.net/insidertrades/airbus',callback=self.insider_data)
            yield i_companies
    
