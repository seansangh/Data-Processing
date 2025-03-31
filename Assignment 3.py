#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2
from itertools import islice
import csv



def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur= openconnection.cursor()
    cur.execute("create table " + ratingstablename + "(userid int, movieid int, rating float);")

    maxI= 500    
    i= 0

    
    with open(ratingsfilepath, 'r') as f:
        for line in f:
            if i >= maxI:
                break
                
            pStrip= line.strip().split('::')
            
            if len(pStrip) == 4:
                userid, movieid, rating, _ = pStrip
                cur.execute(f"insert into {ratingstablename} (userid, movieid, rating) values (%s, %s, %s)", (int(userid), int(movieid), float(rating)) )
                
                i+= 1
    openconnection.commit()
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur= openconnection.cursor()
    step= 5.0 / numberofpartitions
    
    t1= 'CREATE TABLE range_part{0} AS SELECT * FROM ratings WHERE rating>= {1} and rating<={2} '
    t2= 'CREATE TABLE range_part{0} AS SELECT * FROM ratings WHERE rating> {1} and rating<={2} '
    
    for x in range(numberofpartitions):
        if x == 0:
            cur.execute(t1.format(
                x, x* step, (x+ 1) * step) )
                
        else:
            cur.execute(t2.format(
                x, x* step, (x+ 1) * step) )
    
    cur.close()
    
    


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    rrp= 'CREATE TABLE rrobin_part{0} AS SELECT userid, movieid,rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid FROM {1} ) AS temp WHERE mod(temp.rowid - 1, {2}) = {3}'
    cur= openconnection.cursor()
    for x in range(numberofpartitions):
    
        cur.execute(rrp.format(x, ratingstablename, numberofpartitions, x) )
            
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur= openconnection.cursor()

    cur.execute('INSERT INTO {0} VALUES ({1},{2},{3})'.format(ratingstablename, userid, itemid, rating) )
    cur.execute('SELECT * FROM {0} '.format(ratingstablename) )
    recTotal= len(cur.fetchall())

    cur.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%' ")
    totParts= len(cur.fetchall())

    tTot= (recTotal- 1) % totParts
    cur.execute('INSERT INTO rrobin_part{0} VALUES ({1},{2},{3})'.format(tTot,userid, itemid, rating) )
        
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    curr= openconnection.cursor()
    
    curr.execute('INSERT INTO {0} VALUES ({1},{2},{3})'.format(ratingstablename, userid, itemid, rating))
    curr.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%' ")
    totParts= len(curr.fetchall())

    inserT = 'INSERT INTO range_part{0} VALUES ({1},{2},{3})'

    step= 5.0/ totParts


    for x in range(totParts):
        if x == 0:
            if rating>= x* step and rating<= (x+1)* step:
                curr.execute(inserT.format(x, userid, itemid,rating) )
                
        else:
            if rating> x* step and rating<= (x+1)* step:
                curr.execute(inserT.format(x, userid,itemid, rating) )
                
    curr.close()
    
    

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
            
#if __name__ == "__main__":
    #createDB()
    #loadRatings("ratings", "ratings.dat" ,getOpenConnection())
    
