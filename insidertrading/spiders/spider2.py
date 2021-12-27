import scrapy
from ..items import CompaniesItem, InsiderTradesItem
import mysql.connector

class InsiderSpiderSpider2(scrapy.Spider):
    name='trade_spider'
    allowed_domains = ['finanzen.net']

    def start_requests(self):
        self.create_connection()
        self.read_data()
        for company in self.companies:
            #print('Company dict=',company)
            yield scrapy.Request(company['weblink'],callback=self.insider_data,meta={'company_id':company['company_id']})
    
    def read_data(self):
        self.curr.execute("select company_id,insider_trades_weblink from insider_trades.companies")
        results = self.curr.fetchall()
        self.companies = []
        for x in results:
            self.companies.append( {'company_id':x[0],'weblink':x[1]})


    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user = 'root',
            passwd= 'helloworld123',
            database = 'insider_trades'
        )
        self.curr = self.conn.cursor()
    
    def insider_data(self,response):
        #
        #x=response.css(".col-sm-8 td:nth-child(1)::text").extract()
        i_inside_trade =InsiderTradesItem()

        rows=response.xpath('//*[@class="col-sm-8"]//tr')
        l=[]
        for row in rows:
            
            i_inside_trade['company_id']=response.meta.get('company_id')
            i_inside_trade['date']=row.xpath('td[1]//text()').extract_first()
            i_inside_trade['trader']=row.xpath('td[2]//text()').extract_first()
            i_inside_trade['quantity']=row.xpath('td[3]//text()').extract_first()
            i_inside_trade['short_val']=row.xpath('td[4]//text()').extract_first()
            i_inside_trade['type']=row.xpath('td[5]//text()').extract_first()
            #print('i_inside_trade=',i_inside_trade)
            yield i_inside_trade
