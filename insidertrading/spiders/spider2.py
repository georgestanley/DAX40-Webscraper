import scrapy
from ..items import CompaniesItem, InsiderTradesItem
import mysql.connector
from datetime import datetime

class InsiderSpiderSpider2(scrapy.Spider):
    name='trade_spider'
    allowed_domains = ['finanzen.net']

    def start_requests(self):
        
        self.create_connection()
        self.read_data()
        for company in self.companies:
            #print('Company dict=',company)
            self.xxx = None
            yield scrapy.Request(company['weblink'],
                                callback=self.insider_data,
                                meta={'company_id':company['company_id'], 'weblink':company['weblink']})
            
            #if (company['company_id']==2):
                #return
            #yield scrapy.Request('https://www.finanzen.net/insidertrades/adidas',callback=self.insider_data,meta={'company_id':1,
            #'weblink':'https://www.finanzen.net/insidertrades/adidas'})
    
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
        #rows = response.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "col-sm-8", " " ))]')
        #rows = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "table-responsive", " " ))]//tr')
        l=[]
        for row in rows:
            try:
            #print(row)
                i_inside_trade['company_id']=response.meta.get('company_id')
                i_inside_trade['date']=datetime.strptime(row.xpath('td[1]//text()').extract_first(), '%d.%m.%y')
                i_inside_trade['trader']=row.xpath('td[2]//text()').extract_first()
                i_inside_trade['quantity']=row.xpath('td[3]//text()').extract_first()
                i_inside_trade['short_val']=row.xpath('td[4]//text()').extract_first()
                i_inside_trade['type']=row.xpath('td[5]//text()').extract_first()
                #print('i_inside_trade=',i_inside_trade)
                yield i_inside_trade
            except Exception as e:
                print('Error encountered. Pls check')
                print(e)
        if self.xxx is None:    
            self.xxx = response.css('div.paging.clear-block.clearfix a::text').extract()

        """
        print('xxx=',self.xxx)
        if len(self.xxx)>0:
            next_page = response.meta.get('weblink')+'@intpagenr_'+self.xxx[0]
            print('next_page=',next_page)
            yield response.follow(next_page,callback=self.insider_data,meta={'company_id':company['company_id'], 'weblink':company['weblink']})
            self.xxx.pop(0)
        """

        next_page= response.css('div.paging.clear-block.clearfix a.imageButtonRight::attr(href)').extract_first()
        print('next_page=',next_page)        
        if next_page is not None:
            yield response.follow(next_page,callback=self.insider_data, meta= {'company_id':response.meta.get('company_id')})


#https://www.finanzen.net/insidertrades/adidas@intpagenr_2
#https://www.finanzen.net/insidertrades/volkswagen_vz
 #response.css('div.paging.clear-block.clearfix a::text').extract()   