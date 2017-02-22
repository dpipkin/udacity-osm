# -*- coding: utf-8 -*-

from collections import defaultdict
from pprint import pprint
import re
from pymongo import MongoClient

def audit_postal_codes(db):
    pipeline = [{'$group': {'_id': '$address.postcode',
                            'count': {'$sum': 1}}},
                {'$sort': {'count': 1}}]

    for doc in db.dirty_nodes.aggregate(pipeline):
        db.postcode_counts.update(doc, {
                '$setOnInsert': doc
            }, upsert=True)

def audit_colons(db):
    multiple_colons = []
    for doc in db.dirty_nodes.find():
        multi_colon_fields = [k for k in doc if len(k.split(':')) > 2]
        multiple_colons += multi_colon_fields
    pprint(multiple_colons)
    return multiple_colons

def get_key_counts(db):
    key_counts = defaultdict(int)
    for doc in db.dirty_nodes.find():
        for k in doc:
            key_counts[k] += 1
    return key_counts

def update_buildings(db):
    db.nested.update({'building': {'$type': 'string'}},
                     {'$set': {'building': {'val': '$building'}}},
                     multi=True)

def nest_colons(db):
    for doc in db.dirty_nodes.find():
        colon_fields = [k for k in doc if len(k.split(':')) > 1]
        for colon in colon_fields:
            parent, child = colon.split(':')[0], colon.split(':')[1:]
            child = ':'.join(child)

            if parent in doc:
                if type(doc[parent]) == str:
                    doc[parent] = {'val':doc[parent]}
                doc[parent][child] = doc[colon]
            else:
                doc[parent] = {child: doc[colon]}
            del doc[colon]
        db.nested.insert(doc)

if __name__ == '__main__':

    client = MongoClient()
    db = client.utahosm

    # audit_postal_codes(db)
    # audit_colons(db)
    # nest_colons(db)
