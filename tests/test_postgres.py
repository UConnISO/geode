from geode.database import Database

db = Database()
sql = """SELECT * FROM sediment ORDER BY stop DESC LIMIT 2;"""

db.cursor.execute(sql)
res = db.cursor.fetchall()
print res[0][0]
