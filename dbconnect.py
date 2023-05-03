import psycopg2
from psycopg2 import sql
from config import config

conn = psycopg2.connect(
    host="localhost",
    database="hrdb",
    user="horse_crawler",
    password="tophorses")

class DBCon:

    def __init__(self) -> None:
        self.conn = self.connect()
        self.cur = self.conn.cursor()
    
    def disconnect(self):
        if self.cur:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
        print('Database connection closed.') 

    def insert_horse(self, id, name, sex):
        self.cur.execute("INSERT INTO horse.horses (id, horse_name, sex) VALUES (%s, %s, %s)", (id, name, sex))
        self.conn.commit()

    def insert_horse_with_placeholder(self, id):
        self.cur.execute("INSERT INTO horse.horses (id) VALUES (%s)", (id,))
        self.conn.commit()
    
    def insert_horse_attribute(self, id, attribute_column, attribute):
        if attribute_column == "born_date":
            attribute = attribute + "0101"
        query = sql.SQL("UPDATE horse.horses SET {} = {} WHERE id = {}").format(sql.Identifier(attribute_column), sql.Literal(attribute), sql.Literal(id))
        self.cur.execute(query)
    
    def commit_change(self):
        self.conn.commit()
    
    def rollback_change(self):
        self.conn.rollback()

    def horse_exists(self, id):
        self.cur.execute("SELECT id FROM horse.horses WHERE id = %s", (id,))
        return self.cur.fetchone() is not None

    def horse_populated(self, id):
        self.cur.execute("SELECT horse_name FROM horse.horses WHERE id = %s", (id,))
        res = self.cur.fetchone()
        if res == None:
            return False
        return res[0] is not None


    def get_horse_by_id(self, id):
        self.cur.execute("SELECT * FROM horse.horses WHERE id = %s", (id,))
        return self.cur.fetchone()

    def connect(self):
        """ Connect to the PostgreSQL database server """
        conn = None
        try:
            # read connection parameters
            params = config()

            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        return conn
        # finally:
        #     if conn is not None:
        #         conn.close()
        #         print('Database connection closed.')


if __name__ == '__main__':
    test_db_conn = DBCon()
    print(test_db_conn.horse_populated("000a00b09e"))
    test_db_conn.disconnect()