from threading import Thread
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.command_cursor import CommandCursor
from pymongo.change_stream import CollectionChangeStream


# Utils

def parse(args: list) -> dict:
    """Parse logic expresions using pattern matching"""
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

def _thread_change(change_stream: CollectionChangeStream, on_snapshot: callable) -> None:
    """Thread function for on_snapshot method in MongoCollection"""
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
    """Represents a mongodb connetion"""
    def __init__(self, host: str, port: int, **kwargs: dict) -> None:
        self.client: MongoClient = MongoClient(host, port, **kwargs)

    def database(self, name: str) -> 'MongoDatabase':
        """Get a database from MongoService"""
        database = self.client[name]
        return MongoDatabase(database=database)

class MongoDatabase():
    """Represents a mongodb database"""
    def __init__(self, database: Database) -> None:
        self.database: Database = database
        self.name: str = database.name

    # Query
    def list(self) -> list:
        """Get a list of the collections in the database"""
        return self.database.list_collection_names()

    def collection(self, name: str) -> 'MongoCollection':
        """Get a collection from a MongoDatabase"""
        collection = self.database.get_collection(name)
        return MongoCollection(collection=collection)

class MongoCollection():
    """Represents a mongodb collection"""
    def __init__(self, collection: Collection) -> None:
        self.collection: Collection = collection
        self.database: Database = collection.database
        self.name: str = collection.name

        self.pipeline: list = []

    def _reset(self) -> None:
        """Reset the agregation pipeline"""
        self.pipeline = []
    
    def _autoId(self) -> None:
        """Create a new document and get his docId"""
        result = self.collection.insert_one({})
        #self.document(result.inserted_id).set({"_id": result.inserted_id})
        return result.inserted_id

    # Query
    def on_snapshot(self, on_snapshot: callable=None):
        """Watch changes in a collection"""
        pipeline = []
        change_stream = self.collection.watch(pipeline)
        if on_snapshot:
            return Thread(target=_thread_change, args=(change_stream, on_snapshot)).start()
        return change_stream
    
    def count(self) -> str:
        """Get the number of documents in the collection"""
        return self.collection.count_documents()

    def delete(self) -> None:
        """Drop the collection"""
        self.collection.drop()

    def document(self, docId: str=None) -> 'MongoReference':
        """Get a document reference from a MongoCollection"""
        if not docId:
            docId = self._autoId()
        return MongoReference(collection=self.collection, docId=docId)
    
    def get(self) -> None:
        """Get a list with all collection documents"""
        return list(self.aggregate())
    
    def to_dict(self) -> dict:
        """Get a dict with all collection documents"""
        data = {}
        for document in self.aggregate():
            data.update({document.pop("_id", None), document})
        return data

    # Agregation
    def aggregate(self) -> CommandCursor:
        """Get a dictionary by applying a agregation pipeline"""
        result = self.collection.aggregate(self.pipeline)
        self._reset()
        return result

    def where(self, *args: list) -> 'MongoCollection':
        """Add a match aggregation to pipeline"""
        args = args[0] if len(args) == 1 else args
        self.pipeline.append({"$match": parse(args)})
        return self
    
    def order_by(self, name: int, order: int=1) -> 'MongoCollection':
        """Add a sort aggregation to pipeline"""
        self.pipeline.append({"$sort": {name: order}})
        return self
    
    def limit(self, num: int) -> 'MongoCollection':
        """Add a limit aggregation to pipeline"""
        self.pipeline.append({"$limit": num})
        return self

class MongoReference():
    """Represents a mongodb document reference"""
    def __init__(self, collection: Collection, docId: str) -> None:
        self.collection: Collection = collection
        self.docId: str = docId
    
    def _docId(self) -> dict:
        """Get a dict with the mongo document id"""
        return {"_id": self.docId}
    
    def _clean(self, data) -> dict:
        """Clean data before insert them"""
        data.pop("_id", None)
        return data

    # Query
    def on_snapshot(self, on_snapshot: callable=None):
        """Watch changes in a document"""
        pipeline = [{"$match": {"documentKey": self._docId()}}]
        change_stream = self.collection.watch(pipeline)
        if on_snapshot:
            return Thread(target=_thread_change, args=(change_stream, on_snapshot)).start()
        return change_stream
    
    def delete(self) -> None:
        """Delete a mongodb doument"""
        self.collection.delete_one(self._docId())

    def get(self) -> dict:
        """Get the document data as a dict"""
        return self.collection.find_one(self._docId())

    def get_snapshot(self) -> 'MongoDocument':
        """Get a document snapshot from a MongoReference"""
        return MongoDocument(self.collection.find_one(self._docId()), self.docId)
    
    def set(self, data: dict) -> None:
        """Overwrite data in a mongodb doument"""
        data = self._clean(data)
        self.collection.replace_one(self._docId(), data, upsert=True)

    def update(self, data: dict) -> None:
        """Update data in a mongodb doument"""
        data = self._clean(data)
        self.collection.update_one(self._docId(), {"$set": data}, upsert=True)

    def push(self, data: dict) -> None:
        """Push values in a mongodb aray"""
        data = self._clean(data)
        self.collection.update_one(self._docId(), {"$push": data}, upsert=True)

class MongoDocument():
    """Represents a mongodb document snapshot"""
    def __init__(self, document: dict, docId: str) -> None:
        self.document: dict = document
        self.exists: bool = (document != None)
        self.docId: str = docId

    def to_dict(self) -> dict:
        """Get the document data as a dict"""
        return self.document

    def get(self, query) -> dict:
        """Make a query on the document snapshot"""
        raise Exception("Not implemented")
