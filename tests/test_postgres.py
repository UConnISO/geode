from geode.database import Database

db = Database()
sql = """SELECT * FROM sediment ORDER BY stop DESC LIMIT 2;"""

res = db.cursor.execute(sql)
print res.fetchall()
