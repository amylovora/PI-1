from lib2to3.pgen2.token import CIRCUMFLEXEQUAL
from typing import Union
from fastapi import FastAPI
import mysql.connector 
import pandas as pd 



def database_connector():
    connection = mysql.connector.connect(
        user="root", password="root", host="127.0.0.1", port="3306", database="db"
    )
    print("DB connected")
    return connection

def data_load():
    ### DATA ###
    circuits = pd.read_csv('/Users/vr/fuchi-homework/data/circuits.csv', sep=',')
    circuits = pd.DataFrame(circuits)
    #print(circuits)
    constructors = pd.read_json('/Users/vr/fuchi-homework/data/constructors.json', lines=True)
    constructors = pd.DataFrame(constructors)
    #print(constructors)
    pit_stop = pd.read_json('/Users/vr/fuchi-homework/data/pit_stops.json', orient=str)
    pit_stop = pd.DataFrame(pit_stop)
    pit_stop = pit_stop.replace("/N", 0 )
    #print(pit_stop)
    races = pd.read_csv('/Users/vr/fuchi-homework/data/races.csv', sep=',')
    races = pd.DataFrame(races)
    races = races.replace("/N", 0 )
    races = races.replace("\\N", 0 )
    
    #print(races)
    results = pd.read_json('/Users/vr/fuchi-homework/data/results.json', lines=True)
    results = pd.DataFrame(results)
    results = results.replace("/N", 0 )
    results = results.replace("\\N", 0 )
    #print(results)

    drivers = pd.read_json('/Users/vr/fuchi-homework/data/drivers.json', lines=True)
    drivers_name = pd.json_normalize(drivers['name'])
    drivers_test = drivers.join(drivers_name)
    del drivers_test['name']
    drivers_test
    drivers = pd.DataFrame(drivers_test)
    drivers = drivers.replace("\\N", 0 )
    #print(drivers_test)
    return circuits,constructors,drivers,pit_stop,races,results

def drop_table(connection):
    cursor = connection.cursor()
    sql1 = "DROP TABLE IF EXISTS `circuits`;"
    cursor.execute(sql1)
    sql2 = "DROP TABLE IF EXISTS `constructors`;"
    cursor.execute(sql2)
    sql3 = "DROP TABLE IF EXISTS `drivers`;"
    cursor.execute(sql3)
    sql4 = "DROP TABLE IF EXISTS `pit_stops`;"
    cursor.execute(sql4)
    sql5 = "DROP TABLE IF EXISTS `races`;"
    cursor.execute(sql5)
    sql6 = "DROP TABLE IF EXISTS `results`;"
    cursor.execute(sql6)
    return
    
    

def create_table(connection):
    cursor = connection.cursor()
    sql1 = """
            CREATE TABLE IF NOT EXISTS `circuits` (
            `circuitId`		    INTEGER,
            `circuitRef` 		VARCHAR(255),
            `name`		    	VARCHAR(255),
            `location`		    VARCHAR(255),
            `country`			VARCHAR(255),
            `lat`				DECIMAL(10,2),
            `lng`				DECIMAL(10,2),
            `alt`				DECIMAL(10,2),
            `url`				VARCHAR(255)
            ) """
    cursor.execute(sql1)
    sql2 = """
            CREATE TABLE IF NOT EXISTS `constructors` (
            `constructorId`	    INTEGER,
            `constructorRef` 	VARCHAR(255),
            `name`			    VARCHAR(255),
            `nationality`		VARCHAR(255),
            `url`				VARCHAR(255)
            )"""
    cursor.execute(sql2)
    sql3 = """
            CREATE TABLE IF NOT EXISTS `drivers` (
            `driverId`		    INTEGER,
            `driverRef` 		VARCHAR(255),
            `number`			INTEGER,
            `code`		    	VARCHAR(255),
            `forename`	    	VARCHAR(255),
            `surname`			VARCHAR(255),
            `dob`				DATE,
            `nationality`		VARCHAR(255),
            `url`				VARCHAR(255)
            ) """
    cursor.execute(sql3)
    sql4 = """
        CREATE TABLE IF NOT EXISTS `pit_stops` (
        `raceId`		   	INTEGER,
        `driverId` 	    	INTEGER,
        `stop`		    	INTEGER,
        `lap`				INTEGER,
        `time`		    	TIME,
        `duration`	    	VARCHAR(255),
        `milliseconds`  	DECIMAL(10,2)
        ) """
    cursor.execute(sql4)
    sql5 = """
        CREATE TABLE IF NOT EXISTS `races` (
        `raceId`			INTEGER,
        `year` 			    YEAR,
        `round`		    	INTEGER,
        `circuitId`	    	INTEGER,
        `name`		    	VARCHAR(255),
        `date`		    	DATE,
        `time`		    	TIME,
        `url`				VARCHAR(255)
        ) """
    cursor.execute(sql5)
    sql6 = """
        CREATE TABLE IF NOT EXISTS `results` (
        `resultId`				INTEGER,
        `raceId` 				INTEGER,
        `driverId`				INTEGER,
        `constructorId`			INTEGER,
        `number`				INTEGER,
        `grid`					INTEGER,
        `position`				VARCHAR(255),
        `positionText`			VARCHAR(255),
        `positionOrder`			INTEGER,
        `points`				DECIMAL(10,2),
        `laps`					INTEGER,
        `time`					VARCHAR(255),
        `milliseconds`			VARCHAR(255),
        `fastestLap`			INTEGER,
        `rank`					INTEGER,
        `fastestLapTime`		TIME,
        `fastestLapSpeed`		DECIMAL(10,2),
        `statusId`				INTEGER
        ) """
    cursor.execute(sql6)
    return

def insert_data(circuits,constructors,drivers,pit_stops,races,results,connection):
    
    cols1 = "`,`".join([str(i) for i in circuits.columns.tolist()])
    cursor = connection.cursor()
    for i1,row1 in circuits.iterrows():
        sql1 = "INSERT INTO `circuits` (`" +cols1 + "`) VALUES (" + "%s,"*(len(row1)-1) + "%s)"
        cursor.execute(sql1, tuple(row1))
   
    cols2 = "`,`".join([str(i) for i in constructors.columns.tolist()])
    cursor = connection.cursor()
    for i2,row2 in constructors.iterrows():
        sql2 = "INSERT INTO `constructors` (`" +cols2 + "`) VALUES (" + "%s,"*(len(row2)-1) + "%s)"
        cursor.execute(sql2, tuple(row2))
     
    cols3 = "`,`".join([str(i) for i in drivers.columns.tolist()])
    cursor = connection.cursor()
    for i3,row3 in drivers.iterrows():
        sql3 = "INSERT INTO `drivers` (`" +cols3 + "`) VALUES (" + "%s,"*(len(row3)-1) + "%s)"
        cursor.execute(sql3, tuple(row3))
    
    cols4 = "`,`".join([str(i) for i in pit_stops.columns.tolist()])
    cursor = connection.cursor()
    for i4,row4 in pit_stop.iterrows():
        sql4 = "INSERT INTO `pit_stops` (`" +cols4 + "`) VALUES (" + "%s,"*(len(row4)-1) + "%s)"
        cursor.execute(sql4, tuple(row4))
    
    cols5 = "`,`".join([str(i) for i in races.columns.tolist()])
    cursor = connection.cursor()
    for i5,row5 in races.iterrows():
        sql5 = "INSERT INTO `races` (`" +cols5 + "`) VALUES (" + "%s,"*(len(row5)-1) + "%s)"
        cursor.execute(sql5, tuple(row5))
    
    cols6 = "`,`".join([str(i) for i in results.columns.tolist()])
    cursor = connection.cursor()
    for i6,row6 in results.iterrows():
        sql6 = "INSERT INTO `results` (`" +cols6 + "`) VALUES (" + "%s,"*(len(row6)-1) + "%s)"
        cursor.execute(sql6, tuple(row6))        
    
    # the connection is not autocommitted by default, so we must commit to save our changes
    connection.commit()
    return
    
app = FastAPI()
circuits,constructors,drivers,pit_stop,races,results=data_load()
connection=database_connector()
drop_table(connection)
create_table(connection)
insert_data(circuits,constructors,drivers,pit_stop,races,results,connection)
@app.get("/best_year")
def read_root():
    try:
        cursor = connection.cursor()
        cursor.execute("""
                        SELECT year 
                        FROM(
                        SELECT COUNT(year), year
                        FROM races r
                        GROUP BY year
                        ORDER BY 1 desc 
                        )a limit 1
                       """)
        circuits = cursor.fetchall()
        print(circuits)
        # Asegurar que la llama funciona aun cuando no hay datos
        return circuits
    except Exception as e:
        print(repr(e))

        
@app.get("/best_driver")
def read_root():
    try:
        cursor = connection.cursor()
        cursor.execute("""
                        SELECT 
                        Q.forename,
                        Q.surname,
                        count(Q.`position`) as cant_pos_first
                        from (
                        SELECT 
                        r.`position`,
                        r.driverId,
                        d.forename,
                        d.surname
                        FROM results r, drivers d 
                        WHERE r.driverId = d.driverId
                        AND (r.`position` <> 0 and r.`position` = 1)
                        )Q 
                        group by 
                        Q.forename,
                        Q.surname
                        ORDER BY 3 DESC
                        LIMIT 1
                       """)
        circuits = cursor.fetchall()
        print(circuits)
        return circuits
    except Exception as e:
        print(repr(e))


@app.get("/best_circuit")
def read_root():
    try:
        cursor = connection.cursor()
        cursor.execute("""
                        SELECT 
                        COUNT(q.circuitId) as cant_cir, 
                        name
                        FROM(
                        SELECT 
                        r.raceid, 
                        c.circuitId, 
                        c.name
                        FROM 
                        circuits c, 
                        races r 
                        WHERE c.circuitId = r.circuitId 
                        )q
                        GROUP BY name
                        ORDER BY 1 DESC
                        LIMIT 1
                       """)
        circuits = cursor.fetchall()
        print(circuits)
        return circuits
    except Exception as e:
        print(repr(e))

@app.get("/best_points_driver")
def read_root():
    try:
        cursor = connection.cursor()
        cursor.execute("""
                        SELECT
                        SUM(q.points), q.forename, q.surname, q.nationality
                        FROM (
                        SELECT 
                        d.driverId, 
                        d.forename, 
                        d.surname, 
                        r.points, 
                        c.nationality 
                        FROM drivers d , 
                        constructors c , 
                        results r 
                        WHERE d.driverId = r.driverId 
                        AND c.constructorId  = r.constructorId 
                        AND (c.nationality = 'American' OR c.nationality =	'British')
                        )q
                        GROUP BY q.forename, q.surname, q.nationality
                        ORDER BY 1 DESC
                        LIMIT 1
                       """)
        circuits = cursor.fetchall()
        print(circuits)
        return circuits
    except Exception as e:
        print(repr(e))