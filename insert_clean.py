import re
import sys
import xml.etree.cElementTree as ET

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

CREATED = ['version', 'changeset', 'timestamp', 'user', 'uid']
FIELDS = ['building', 'name', 'tiger:county', 'amenity', 'service', 'highway']

def shape_element(elem):
    node = {}
    if elem.tag == 'node' or elem.tag == 'way':
        node['type'] = elem.tag
        node_id = 0
        for k, v in elem.attrib.items():
            if k in CREATED:
                if 'created' not in node:
                    node['created'] = {}
                node['created'][k] = v
            elif k == 'id':
                node_id = v
            else:
                node[k] = v
        node['_id'] = node_id

        for tag in elem.iter('tag'):
            if tag.attrib['k'] in FIELDS:
                if tag.attrib['k'] == 'tiger:county':
                    node['county'] =  tag.attrib['v']
                else:
                    node[tag.attrib['k']] = tag.attrib['v']
        for nd in elem.iter('nd'):
            if 'node_refs' not in node:
                node['node_refs'] = []
            node['node_refs'].append(nd.attrib['ref'])
        return node
    else:
        return None

if __name__ == '__main__':
    collection_name = sys.argv[1]
    client = MongoClient()
    db = client.utahosm

    with open('utah.osm', 'r', encoding='utf-8') as f:
        for _, elem in ET.iterparse(f):
            el = shape_element(elem)
            if el:
                try:
                    db[collection_name].insert_one(el)
                except DuplicateKeyError:
                    print("Error duplicate key on {}".format(el['_id']))
