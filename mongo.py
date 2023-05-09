
from pymongo import MongoClient
from threading import Thread


# Utils

# Esta función parsea las expresiones lógicas utilizando pattern matching
def parse(args):
    match args:
        # case 2
        case ["not", e]: return {"$not": parse(e)}
        # case 3
        case [e1, "==", e2]: return {e1: parse(e2)}
        case [e1, "!=", e2]: return {e1: {"$ne": parse(e2)}}
        case [e1, "<", e2]: return {e1: {"$lt": parse(e2)}}
        case [e1, ">", e2]: return {e1: {"$gt": parse(e2)}}
        case [e1, "<=", e2]: return {e1: {"$lte": parse(e2)}}
        case [e1, ">=", e2]: return {e1: {"$gte": parse(e2)}}
        case [e1, "in", e2]: return {e1: {"$in": parse(e2)}}
        case [e1, "not-in", e2]: return {e1: {"$nin": parse(e2)}}
        # case _
        case ["and", *expr]: return {"$and": [parse(e) for e in expr]}
        case ["nor", *expr]: return {"$nor": [parse(e) for e in expr]}
        case ["or", *expr]: return {"$or": [parse(e) for e in expr]}
        case _: return args

def _thread_change(change_stream, on_snapshot):
    for change in change_stream:
        match on_snapshot.__code__.co_argcount:
            case 1:
                on_snapshot(change)
            case 3:
                # Mongo a veces no retorna el fullDocument
                fullDocument = [change.get("fullDocument", {})]
                changes = {"documentKey": change["documentKey"], "operationType": change["operationType"], "ns": change["ns"]}
                #if "updateDescription" in change:
                #    changes.update({"updateDescription": change.get("updateDescription")})
                read_time = change.get("clusterTime", {})
                on_snapshot(fullDocument, changes, read_time)
            case _:
                raise Exception("callback function must have 1 or 3 parameters")


# Clases

class MongoService():
    def __init__(self, host, port, **kwargs):
        self.client = MongoClient(host, port, **kwargs)
        self.cfg = {}

    def database(self, name: str):
        database = self.client[name]
        return MongoDatabase(database=database)

class MongoDatabase():
    def __init__(self, database):
        self.database = database

    def collection(self, name: str):
        collection = self.database[name]
        return MongoCollection(collection=collection)

class MongoCollection():
    def __init__(self, collection):
        self.collection = collection
        self.pipeline = []

    def _reset(self):
        self.pipeline = []

    def on_snapshot(self, on_snapshot=None):
        pipeline = []
        change_stream = self.collection.watch(pipeline)
        if on_snapshot:
            return Thread(target=_thread_change, args=(change_stream, on_snapshot)).start()
        return change_stream

    def document(self, docId: str):
        return MongoReference(collection=self.collection, docId=docId)
    
    def get(self):
        result = self.collection.aggregate(self.pipeline)
        self._reset()
        return list(result)

    # Agregation
    def where(self, *args):
        args = args[0] if len(args) == 1 else args
        self.pipeline.append({"$match": parse(args)})
        return self
    
    def order_by(self, name: int, order=1):
        self.pipeline.append({"$sort": {name: order}})
    
    def limit(self, num: int):
        self.pipeline.append({"$limit": num})

class MongoReference():
    def __init__(self, collection, docId):
        self.collection = collection
        self.docId = docId

    def _docId(self):
        return {"_id": self.docId}
    
    def on_snapshot(self, on_snapshot=None):
        pipeline = [{"$match": {"documentKey": self._docId()}}]
        change_stream = self.collection.watch(pipeline)
        if on_snapshot:
            return Thread(target=_thread_change, args=(change_stream, on_snapshot)).start()
        return change_stream

    def get(self):
        return self.collection.find_one(self._docId())
    
    def delete(self):
        self.collection.delete_one(self._docId())
    
    def set(self, data: dict):
        data.pop("_id", None)
        self.collection.replace_one(self._docId(), data, upsert=True)

    def update(self, data: dict):
        data.pop("_id", None)
        self.collection.update_one(self._docId(), {"$set": data}, upsert=True)

    def push(self, data:dict):
        data.pop("_id", None)
        self.collection.update_one(self._docId(), {"$push": data}, upsert=True)