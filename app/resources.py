from flask import Flask, request, make_response, jsonify
from flask_restful import Resource, Api

from flask.ext.httpauth import HTTPBasicAuth
from common.dateRoutines import Dates
#from common.LFData import LFData
from common.dateRoutines import Dates

from common.ElasticClass import ESInterface
app = Flask(__name__, static_url_path="")
api = Api(app)
class Constants:
    def __init__(self, schema="ES"):
        self.FO = "flightoptions"
        #### HEADER DATA ####
        self.DP = "dp"  # reflects the data platform with index as dp
        self.ORGS = "organizations"  # reflects the name of the organization type
        self.SRC = "searches"
        self.SC = "searchcriteria"
        self.OR = "origin"  # SC.OR
        self.DE = "destination"
        self.AR = "airresultset"  # []
        self.FR = "flightresults"  # AR[].FR
        self.FRINDEX = "flightresultindex"
        self.PRICING = "pricing"
        self.AIRCLASS = "class"
        self.PRICE = "price"

        self.DESTAIRPORT = "destinationairport"
        self.TRAVELDATETIME = "traveldatetime"
        self.TRAVELDTISO = "traveldateiso"
        self.REQDATETIME = "reqdatetime"
        self.REQDATETIMEISO = "reqdatetimeiso"
        self.BOOKDATETIMEISO = "bookdatetimeiso"
        self.RETURNDATE = "returndatetime"
        self.RETURNDATEISO = "returndatetimeiso"
        self.DIRECTION = "direction"
        self.ORIAIRPCODE = "originairport"
        self.DEPTS = "departments"
        self.SELECTIONS = "selections"
        self.BOOKINGS = "bookings"
        self.NAME = "name"
        self.CLIENTID = "clientid"
        self.SEARCHID = "searchid"
        self.SELECTIONID = "selectionid"
        self.BOOKINGID = "bookingid"
        self.FLIGHTBOUNDS = "flightbounds"

        self.CITYPAIR = "citypair"

class InvalidUsage(Exception):
    status_code = 400
    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv
def validateDS(essource):
    if essource == 'local':
        return 'local'
    else:
        if essource == 'remote':
            return 'remote'
        else:
            raise InvalidUsage('Invalid Data Source - use local or remote only', status_code=410)
class leastfares(Resource):
    #decorators = [auth.login_required]
    def get(self, essource, cp):
        s = validateDS(essource)
        self.dh = ESInterface(s)

        filt = {"citypair": cp}
        x = self.dh.readFull(filt)
        return {cp: x}
class orgs(Resource):
    def get(self, essource):
        s = validateDS(essource)
        eh = ESInterface(s)
        resp = eh.readFull(None, "app", "clients")
        return {"clients": resp}
class counts(Resource):
    def get(self, essource):
        C = Constants("ES")
        s = validateDS(essource)
        eh = ESInterface(s)
        orgObjs = eh.ff([C.CLIENTID], "app", "clients")
        orgcnt = eh.count("app","clients")
        clientIDS = list(map(lambda x: x[C.CLIENTID], orgObjs))
        orgs = {"clients": orgcnt, "clientids":clientIDS}
        #for each client fetch client counts
        allCI = {}
        for clientid in clientIDS:
            itemDetails = {C.SRC: {"count": 0}, C.SELECTIONS: {"count": 0}, C.BOOKINGS: {"count": 0}}
            for item in itemDetails.keys():
                cntobj = {"count":eh.count(clientid, item)}
                itemDetails.update({item:cntobj})
            allCI.update({clientid:itemDetails})
        return {"orgs":orgs, "txns":allCI}
class bookings(Resource):
    def get(self,essource,clientID):
        import numpy as np

        C = Constants("ES")
        dateObject = Dates()
        s = validateDS(essource)
        eh = ESInterface(s)
        if clientID == 'all':  # means all clients
            clientIDs = map(lambda x: x[C.CLIENTID], eh.readFull(None, "app", "clients"))
        else:
            clientIDs = [clientID]
        rows = []
        rows.append([C.CITYPAIR, C.REQDATETIMEISO, C.BOOKDATETIMEISO, C.TRAVELDTISO, C.RETURNDATEISO, C.AIRCLASS,
                     C.CLIENTID, C.PRICE, "advancedays", "req2bookdays", "book2traveldays"])
        for esindex in clientIDs:
            bkgs = eh.readFull(None, esindex, C.BOOKINGS)
            for bkg in bkgs:
                row = []
                selObj = eh.readFull({C.SELECTIONID: bkg[C.SELECTIONID]}, esindex, C.SELECTIONS)[0]
                arIdx = selObj[C.FR][0]["airresultindex"]
                frIdx = selObj[C.FR][0][C.FRINDEX]
                prIdx = selObj[C.FR][0]["pricingindex"]
                searchObj = eh.readFull({C.SEARCHID: selObj[C.SEARCHID]}, esindex, C.SRC)[0]
                frObj = searchObj[C.AR][arIdx][C.FR][frIdx]

                row.append(searchObj[C.SC][C.OR] + searchObj[C.SC][C.DE])  # citypair
                row.append(searchObj["header"][C.REQDATETIMEISO])
                row.append(bkg[C.BOOKDATETIMEISO])
                row.append(searchObj[C.SC][C.TRAVELDTISO])
                row.append(searchObj[C.SC][C.RETURNDATEISO])
                row.append(searchObj[C.SC][C.AIRCLASS])
                row.append(searchObj["header"][C.CLIENTID])
                row.append(frObj["pricing"][prIdx][C.PRICE])

                diff = dateObject.diffDays(searchObj[C.SC][C.TRAVELDATETIME], searchObj["header"][C.REQDATETIME])
                row.append(diff)
                diff = dateObject.diffDays(bkg["bookdatetime"], searchObj["header"][C.REQDATETIME])
                row.append(diff)
                diff = dateObject.diffDays(searchObj[C.SC][C.TRAVELDATETIME], bkg["bookdatetime"])
                row.append(diff)

                rows.append(row)
        return {'bookings':rows}