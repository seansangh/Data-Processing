#!/usr/bin/python3


import psycopg2
import os
import sys


DATABASE_NAME='dds_assignment'
RATINGS_TABLE_NAME='ratings'
RANGE_TABLE_PREFIX='range_part'
RROBIN_TABLE_PREFIX='rrobin_part'
RANGE_QUERY_OUTPUT_FILE='RangeQueryOut.txt'
PONT_QUERY_OUTPUT_FILE='PointQueryOut.txt'
RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    rrobinquery1= "select partitionnum from {tName};".format(
        tName= RROBIN_RATINGS_METADATA_TABLE
    )

    rrobinquery2= "select * from {RROBIN_TABLE_PREFIX}{pNum} where rating>= {rmin} and rating<= {rmax}; "
    
    finalRatings= []
    with openconnection.cursor() as cur:
        cur.execute(rrobinquery1 )
        rrp= cur.fetchone()[0]

        for pNum in range(0, rrp):
            cur.execute(
                rrobinquery2.format(
                    RROBIN_TABLE_PREFIX= RROBIN_TABLE_PREFIX,
                    pNum= pNum,
                    rmin= ratingMinValue,
                    rmax= ratingMaxValue
                )
            )
            
            rs= cur.fetchall()
            for rValue in rs:
                rValue= list(rValue)
                rValue.insert(
                    0,
                    "{RROBIN_TABLE_PREFIX}{pNum}".format(
                        RROBIN_TABLE_PREFIX= RROBIN_TABLE_PREFIX,
                        pNum= pNum
                    )
                )
                finalRatings.append(rValue)




    rquery= "select partitionnum from {tName} where maxrating>= {rmin} and minrating<= {rmax};".format(
        tName= RANGE_RATINGS_METADATA_TABLE,
        rmin= ratingMinValue,
        rmax= ratingMaxValue
    )

    rquery2= "select * from {RANGE_TABLE_PREFIX}{pNum} where rating>= {rmin} and rating<= {rmax};"

    with openconnection.cursor() as cur:
        cur.execute(rquery)
        r= cur.fetchall()
        mList2= []
        
        for val in r:
            mList2.append(val[0])
            
        for pNum in mList2:
            cur.execute(
                rquery2.format(
                    RANGE_TABLE_PREFIX= RANGE_TABLE_PREFIX,
                    pNum= pNum,
                    rmin= ratingMinValue,
                    rmax= ratingMaxValue
                )
            )
            
            rs2 = cur.fetchall()
            for rVal in rs2:
                rVal= list(rVal)
                rVal.insert(
                    0,
                    "{RANGE_TABLE_PREFIX}{pNum}".format(
                        RANGE_TABLE_PREFIX= RANGE_TABLE_PREFIX,
                        pNum= pNum
                    )
                )
                finalRatings.append(rVal)

    writeToFile("RangeQueryOut.txt", finalRatings)



def PointQuery(ratingsTableName, ratingValue, openconnection):

    rrobinquery1= "select partitionnum from {tName};".format(
        tName= RROBIN_RATINGS_METADATA_TABLE
    )

    rrobinquery2= "select * from {RROBIN_TABLE_PREFIX}{pValue} where rating={rValue}; "
    
    finalRatings= [] 
    with openconnection.cursor() as cur:
        cur.execute(rrobinquery1 )
        
        rrp= cur.fetchone()[0]

        for pValue in range(0, rrp):
            cur.execute(
                rrobinquery2.format(
                
                    RROBIN_TABLE_PREFIX= RROBIN_TABLE_PREFIX,
                    pValue= pValue,
                    rValue= ratingValue
                ) 
            )
            
            rs3= cur.fetchall()
            for rVal in rs3:
                rVal= list(rVal)
                rVal.insert(
                    0,
                    "{RROBIN_TABLE_PREFIX}{pValue}".format(
                        RROBIN_TABLE_PREFIX= RROBIN_TABLE_PREFIX,
                        pValue= pValue
                    )
                ) 
                finalRatings.append(rVal)



    rquery= "select partitionnum from {tName} where maxrating>={rValue} and minrating<={rValue}; ".format(
        tName= RANGE_RATINGS_METADATA_TABLE,
        rValue= ratingValue
    )

    rquery2= "select * from {RANGE_TABLE_PREFIX}{pValue} where rating={rValue}; "

    with openconnection.cursor() as cur:
        cur.execute(rquery )
        items= cur.fetchall()
        
        numberOfP= []
        
        for val in items:
            numberOfP.append(val[0])
            
        for pValue in numberOfP:
            cur.execute(
                rquery2.format(
                    RANGE_TABLE_PREFIX= RANGE_TABLE_PREFIX,
                    pValue= pValue,
                    rValue= ratingValue
                )
            )
            
            rs4= cur.fetchall()
            for rVal in rs4:
                rVal= list(rVal)
                
                rVal.insert(
                    0,
                    "{RANGE_TABLE_PREFIX}{pValue}".format(
                        RANGE_TABLE_PREFIX= RANGE_TABLE_PREFIX,
                        pValue= pValue
                        
                    )
                )
                finalRatings.append(rVal)

    writeToFile("PointQueryOut.txt", finalRatings)
                


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
