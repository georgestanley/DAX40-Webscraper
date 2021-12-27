import mysql.connector

conn = mysql.connector.connect(
            host='localhost',
            user = 'root',
            passwd= 'helloworld123',
            database = 'insider_trades'
        )
curr = conn.cursor()
curr.execute("select last_executed_at from insider_trades.script_executions;")
last_executed_at = curr.fetchall()
print(last_executed_at)

