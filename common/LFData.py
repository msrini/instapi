import requests
import sys
from datetime import date
import json
from itertools import repeat

from bson.json_util import dumps

from common import ElasticClass

import pandas as pd
import numpy as np
import logging
'''
Get all Least fares for a city pair or a set of city pairs
Returns a list of city pair objects
'''
class LFData:
    def __init__(self, ds="ES-local"):      #datasource can be ES-local, ES-remote, M-Lab
        self.db = None
        self.cpObjs = None
        self.source = ds.lower()

        if self.source == "es-local":
            self.db = ElasticClass.ESInterface('local')
        if self.source == "es-remote":
            self.db = ElasticClass.ESInterface('remote')
    def fetchLFData(self, inpCP=None, index="leastfares", type="ssref"):     #ES-local, ES-remote, M-Lab
        #expected array if input city pairs. This method will transform as per ES or Mongo
        filt = None
        if inpCP is not None:
            if self.source == "es-local" or self.source == "es-remote":
                if isinstance(inpCP, list):
                    filt = {"citypair": (' '.join(inpCP))}
                else:
                    filt = {"citypair": inpCP}
            else:
                filt = {"citypair": {"$in": inpCP}}
        self.cpObjs = self.db.readFull(filt, index, type)
        #TODO take care of error....
        return 0
    def fetchES(self, filt=None, index="dp", type="bookings"):
        return self.db.readFull(filt, index, type)

    def writeLF(self, key, data, index, type):
        res = self.db.writeObj(key, data, index, type)
        return res
    def local2Remote(self, target, index, type):
        remES = LFData(target)
        def replicate(obj):
            print (obj["citypair"])
            remES.writeLF(obj["citypair"], obj,index, type)
            return
        outp = self.db.readChunk(None, index, type, replicate)

        return outp
    '''
    Given a list of City Pair Objects, filter out traveldate that does not fall within the given from and to dates
    '''
    def filterDates(self, fromDate, toDate):
        #check presence of args in the form of a json
        fromDt = date(fromDate["year"], fromDate["month"], fromDate["day"])
        toDt = date(toDate["year"], toDate["month"], toDate["day"])
        #Check validity of dates, flag error if invalid
        newcpObjs = []
        for cpObj in self.cpObjs:
            newObj = {"citypair":cpObj["citypair"], "quotes":[], "_id":cpObj["_id"]}
            for qObj in cpObj["quotes"]:
                tempdt = qObj["traveldate"]
                tdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                if tdt < fromDt or tdt > toDt:
                    continue
                newObj["quotes"].append(qObj)
            newcpObjs.append(newObj)
        self.cpObjs = newcpObjs
    '''
    Transform a list of CityPair objects to return a list of panda datagrid objects.
    Each datagrid object consists of lists of data
    '''
    def multiGrids(self):
        dfs = []
        #Segregate based on City Pairs
        for cpObj in self.cpObjs:
            gridObj = {"citypair": [], "traveldate": [], "quotedate": [], "lfare": [], "diff": []}
            quotes = cpObj["quotes"]
            cp = cpObj["citypair"]
            for qObj in quotes:
                tempdt = qObj["traveldate"]
                tdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                tempdt = qObj["quotedate"]
                qdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                gridObj["citypair"].append(cp)
                gridObj["traveldate"].append(str(tdt))
                gridObj["quotedate"].append(str(qdt))
                gridObj["lfare"].append(qObj["minprice"])
                gridObj["diff"].append((tdt - qdt).days)
            dfs.append(pd.DataFrame(gridObj))
        return(dfs)
    '''
    return a data frame
    '''
    def tabulateLF(self):
        gridObj = {"citypair":[], "traveldate":[], "quotedate":[], "price":[], "diff":[]}
        gridObj = {"citypair": [], "price": []}
        #quotedate is the booking date (date in which the ticket is booked for the traveldate)
        for cpObj in self.cpObjs:
            quotes = cpObj["quotes"]
            cp = cpObj["citypair"]
            for qObj in quotes:
                #tempdt = qObj["traveldate"]
                #tdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                #tempdt = qObj["quotedate"]
                #qdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                gridObj["citypair"].append(cp)
                #gridObj["traveldate"].append(str(tdt))
                #gridObj["quotedate"].append(str(qdt))
                gridObj["price"].append(qObj["minprice"])   #price is the least fare
                #gridObj["diff"].append((tdt-qdt).days)
                #obj = {"citypair":cp, "traveldate": tdt, "quotedate":qdt, "price":qObj["minprice"], "diff":(tdt-qdt).days}
        self.gridObj = gridObj
        df = pd.DataFrame(gridObj)
        return (df)
    def trendDF(self):
        grids = []
        cps = []
        for cpObj in self.cpObjs:
            cp = cpObj["citypair"]
            cps.append(cp)
            gridObj = {cp: list(repeat(0,32)), "travelmonth": [], "diff": list(range(0,32)), "counts": list(repeat(0,32))}
            for qObj in cpObj["quotes"]:
                tempdt = qObj["traveldate"]
                tdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                travelMon = tempdt[:4] + "-" + tempdt[5:7]
                tempdt = qObj["quotedate"]
                qdt = date(int(tempdt[:4]), int(tempdt[5:7]), int(tempdt[8:]))
                diff = (tdt - qdt).days
                if diff > 31:
                    continue
                if diff in gridObj["diff"]:
                    gridObj[cp][diff] += qObj["minprice"]
                    gridObj["counts"][diff] += 1
                    #gridObj["travelmonth"][diff] = travelMon
                else:
                    gridObj[cp].append(qObj["minprice"])
                    #gridObj["travelmonth"].append(travelMon)
                    gridObj["diff"].append(diff)
                    gridObj["counts"].append(1)
            nRows = len(gridObj["counts"])
            for idx in range(0,nRows):
                if gridObj[cp][idx] == 0:       #means no entry, so fill it up
                    copyIdx = 0
                    for idx2 in range(idx, nRows):
                        if gridObj[cp][idx2] > 0:
                            copyIdx = idx2
                            break
                    #Fill with the next available days' price. if none is filled up, then it is 0 all the time
                    gridObj[cp][idx] = gridObj[cp][copyIdx]
                    gridObj["counts"][idx] = gridObj["counts"][copyIdx]
                    #gridObj["travelmonth"][idx] = gridObj["travelmonth"][copyIdx]

                if (gridObj["counts"][idx] > 1):
                    gridObj[cp][idx] = gridObj[cp][idx] / gridObj["counts"][idx]
                    gridObj["counts"][idx] = 1
            grids.append(gridObj)
        newgrid = {}
        for cp in cps:
            grid = getGrid(grids,cp)
            if grid is None:
                continue
            newgrid.update({cp:grid[cp]})
            newgrid.update({"diff":grid["diff"]})
        return pd.DataFrame(newgrid)
def getGrid(grids, cp):
    for grid in grids:
        if cp in grid:
            return grid
    return None