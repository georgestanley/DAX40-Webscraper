# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from .items import CompaniesItem, InsiderTradesItem


class InsidertradingPipeline:

    def __init__(self) -> None:
        self.create_connection()
        self.create_table()
        self.post_processing()

        
    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user = 'root',
            passwd= 'helloworld123',
            database = 'insider_trades'
        )
        self.curr = self.conn.cursor()

    def create_table(self):
        #self.curr.execute("""DROP TABLE IF EXISTS companies""")
        self.curr.execute("""create table IF NOT EXISTS companies(
            company_id INT AUTO_INCREMENT PRIMARY KEY,
            company_name text,
            path text,
            insider_trades_weblink text,
            created_at timestamp)""")
        #self.curr.execute("""DROP TABLE IF EXISTS trades""")
        self.curr.execute("""create table IF NOT EXISTS trades(
            tranx_id INT AUTO_INCREMENT PRIMARY KEY,
            company_id INT,
            date text,
            trader text,
            quantity INT,
            short_val FLOAT,
            type text,
            created_at timestamp)""")    

    def process_item(self, item, spider):
        if isinstance(item,CompaniesItem):
            self.store_db_companies(item)
            #print('Pipeline:'+ item['company_name'][0] )
            return item
        elif isinstance(item,InsiderTradesItem):
            self.store_db_trades(item)
            #print('Pipeline:'+ item['company_name'][0] )
            return item

    def store_db_companies(self, item):
        self.curr.execute("""insert into companies(company_name,path,insider_trades_weblink,created_at) values (%s,%s,%s, now())""",
        (
            item['company_name'],
            item['path'],
            item['insider_trades_weblink'],

        ) )
        self.conn.commit()
    
    def store_db_trades(self, item):
        print("""insert into trades (company_id,
            date,
            trader ,
            quantity ,
            short_val,
            type,
            created_at)values (%s,%s,%s,%s,%s,%s,now())""",(
            item['company_id'],
            item['date'],
            item['trader'],
            item['quantity'].replace('.',''),
            item['short_val'].replace(',','.'),
            item['type']
        ))
        self.curr.execute("""insert into trades (company_id,
            date,
            trader ,
            quantity ,
            short_val,
            type,
            created_at)values (%s,%s,%s,%s,%s,%s,now())""",
        (
            item['company_id'],
            item['date'],
            item['trader'],
            item['quantity'].replace('.',''),
            item['short_val'].replace(',','.'),
            item['type']
        ) )

  
        self.conn.commit()
    
    def post_processing(self):
        # use this code block for cleaning up null or junk data
        self.curr.execute("""insert into insider_trades.script_executions(last_executed_at,spider) 
        values (now(),'full_load')""")
        self.curr.execute("""delete from insider_trades.trades where date is null; """)
        print('post processing complete')
        self.conn.commit()
