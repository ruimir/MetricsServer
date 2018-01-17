from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
import cx_Oracle
from simplejson import dumps
from flask_jsonpify import jsonify

db_connect = create_engine('oracle+cx_oracle://STATS_AGENT:aebd@localhost:1521/?service_name=orcl')

app = Flask(__name__)
api = Api(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


# TODO
# Get Current System Stats
# Get Current Datafiles Stats
# Get Current Tablespaces Stats


class Tablespaces(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("SELECT * FROM TABLESPACE ORDER BY TABLESPACE.ID")
        result = {'tablespaces': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class Datafiles(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("SELECT * FROM DATAFILE ORDER BY  DATAFILE.ID")
        result = {'datafiles': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class System_Stats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute(
            "SELECT to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime,CPUUSAGE,PGAUSED,SGAUSED FROM STATS ORDER BY capturetime DESC")
        result = {'system_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class Tablespace_Stats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute(
            "SELECT id,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime,TABLESPACE FROM TABLESPACESTATS ORDER BY capturetime DESC")
        result = {'tablespace_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class Datafile_Stats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute(
            "SELECT id,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime,DATAFILE FROM DATAFILESTATS ORDER BY capturetime DESC")
        result = {'datafile_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class CurrentStats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("""
       SELECT  * FROM (SELECT *
       FROM STATS
       ORDER
       BY STATS.CAPTURETIME DESC)
       WHERE ROWNUM = 1""")
        result = [dict(zip(tuple(query.keys()), i)) for i in query.cursor][0]
        return jsonify(result)


class CurrentTablespaceStats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("""
        SELECT id,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime,TABLESPACE  FROM TABLESPACESTATS
        WHERE TABLESPACESTATS.CAPTURETIME=(
        SELECT  CAPTURETIME FROM (SELECT *
        FROM STATS
        ORDER
        BY STATS.CAPTURETIME DESC)
        WHERE ROWNUM = 1)""")
        result = {'tablespace_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class CurrentDatafileStats(Resource):
    def get(self):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("""
        SELECT id,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime,DATAFILE FROM DATAFILESTATS
        WHERE DATAFILESTATS.CAPTURETIME=(
        SELECT  CAPTURETIME FROM (SELECT *
        FROM STATS
        ORDER
        BY STATS.CAPTURETIME DESC)
        WHERE ROWNUM = 1)""")
        result = {'datafile_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class DatafileEvolution(Resource):
    def get(self, datafileid):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("""
        SELECT ID,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime FROM DATAFILESTATS
        WHERE DATAFILESTATS.DATAFILE=:datafile
        """, datafile=datafileid)
        result = {'datafile_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


class TablespaceEvolution(Resource):
    def get(self, tablespaceid):
        conn = db_connect.connect()  # connect to database
        query = conn.execute("""
        SELECT ID,TOTALSPACE,OCCUPIEDSPACE,FREESPACE,PERCENTAGEFREE,to_char(CAPTURETIME,'dd/mm/yyyy hh24:mi:ss') capturetime FROM TABLESPACESTATS
        WHERE TABLESPACESTATS.TABLESPACE=:tablespace
        """, tablespace=tablespaceid)
        result = {'tablespace_stats': [dict(zip(tuple(query.keys()), i)) for i in query.cursor]}
        return jsonify(result)


api.add_resource(Tablespaces, '/tablespaces')
api.add_resource(Datafiles, '/datafiles')
api.add_resource(System_Stats, '/stats')
api.add_resource(Datafile_Stats, '/datastats')
api.add_resource(Tablespace_Stats, '/tablestats')
api.add_resource(CurrentStats, '/currentstats')
api.add_resource(CurrentTablespaceStats, '/currenttablespacestats')
api.add_resource(CurrentDatafileStats, '/currentdatafilestats')
api.add_resource(DatafileEvolution, '/datafileevolution/<datafileid>')
api.add_resource(TablespaceEvolution, '/tablespaceevolution/<tablespaceid>')

if __name__ == '__main__':
    app.run(port=5002)
