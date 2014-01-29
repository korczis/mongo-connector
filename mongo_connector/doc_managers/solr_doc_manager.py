# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

"""Receives documents from the oplog worker threads and indexes them
into the backend.

This file is a document manager for the Solr search engine, but the intent
is that this file can be used as an example to add on different backends.
To extend this to other systems, simply implement the exact same class and
replace the method definitions with API calls for the desired backend.
"""
import re
import json
import logging
import os
import sys

from pysolr import Solr, SolrError
from threading import Timer
import collections

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'mongo_connector'))

from util import retry_until_ok
ADMIN_URL = 'admin/luke?show=Schema&wt=json'

decoder = json.JSONDecoder()

class DocManager():
    """The DocManager class creates a connection to the backend engine and
    adds/removes documents, and in the case of rollback, searches for them.

    The reason for storing id/doc pairs as opposed to doc's is so that multiple
    updates to the same doc reflect the most up to date version as opposed to
    multiple, slightly different versions of a doc.
    """

    def __init__(self, url, auto_commit=False, unique_key='_id'):
        """Verify Solr URL and establish a connection.
        """
        self.solr = Solr(url)
        self.unique_key = unique_key
        self.auto_commit = auto_commit
        self.field_list = []
        self.dynamic_field_list = []
        self.build_fields()

        self.commit_bulk_max = 1000
        self.commit_bulk_num = 0  

        self.bulk_docs_send = 10   
        self.bulk_docs_counter = 0

        self.docs = []

        if auto_commit:
            self.run_auto_commit()

    def _parse_fields(self, result, field_name):
        """ If Schema access, parse fields and build respective lists
        """
        field_list = []
        for key, value in result.get('schema', {}).get(field_name, {}).items():
            if key not in field_list:
                field_list.append(key)
        return field_list

    def build_fields(self):
        """ Builds a list of valid fields
        """
        declared_fields = self.solr._send_request('get', ADMIN_URL)
        result = decoder.decode(declared_fields)
        self.field_list = self._parse_fields(result, 'fields'),
        self.dynamic_field_list = self._parse_fields(result, 'dynamicFields')

    def clean_doc(self, doc):
        """ Cleans a document passed in to be compliant with the Solr as
        used by Solr. This WILL remove fields that aren't in the schema, so
        the document may actually get altered.
        """
        if not self.field_list:
            return doc

        fixed_doc = {}
        for key, value in doc.items():
            if key in self.field_list[0]:
                fixed_doc[key] = value

            # Dynamic strings. * can occur only at beginning and at end
            else:
                for field in self.dynamic_field_list:
                    if field[0] == '*':
                        regex = re.compile(r'\w%s\b' % (field))
                    else:
                        regex = re.compile(r'\b%s\w' % (field))
                    if regex.match(key):
                        fixed_doc[key] = value

        return fixed_doc

    def stop(self):
        """ Stops the instance
        """
        self.auto_commit = False

    def transform_doc_old(self, doc):
        new_doc = dict()
        new_doc['_id'] = doc['_id']
        new_doc['GlossaryId'] = doc['GlossaryId']
        new_doc['SourceTerm'] = doc['SourceTerm']

        if 'Notes' in doc.keys(): 
            new_doc['Notes'] = doc['Notes']     
               
        if 'Definition' in doc.keys():
            new_doc['Definition'] = doc['Definition']

        if 'IsProductName' in doc.keys():
            new_doc['IsProductName'] = doc['IsProductName']

        if 'DoNotTranslate' in doc.keys():
            new_doc['DoNotTranslate'] = doc['DoNotTranslate']
            

        for translation in doc['TargetTerms']:
            name = translation['LangCode'] + "_target_term"
            name = re.sub('[^0-9a-zA-Z]+', '_', name).lower()
            new_doc[name] = translation['Terms'][0]

        # print(new_doc)
        return new_doc

    def transform_doc(self, doc):
        new_doc = dict()
        new_doc['_id'] = doc['_id']
        new_doc['id'] = doc['_id']

        if 'address' in doc['value']['data'].keys():
            if 'street' in doc['value']['data']['address'].keys():
                new_doc['cs_address_street'] = doc['value']['data']['address']['street']
            
            if 'city' in doc['value']['data']['address'].keys():
                new_doc['cs_address_city'] = doc['value']['data']['address']['city']
            
            if 'postalCode' in doc['value']['data']['address'].keys():
                new_doc['cs_address_postal_code'] = doc['value']['data']['address']['postalCode']

        if 'description' in doc['value']['data'].keys():
            new_doc['cs_description'] = doc['value']['data']['description']
        
        if 'email' in doc['value']['data'].keys():
            new_doc['cs_email'] = doc['value']['data']['email']
        
        if 'fax' in doc['value']['data'].keys():
            new_doc['fax'] = doc['value']['data']['fax']
        
        if 'name' in doc['value']['data'].keys():
            new_doc['cs_name'] = doc['value']['data']['name']

        if 'phone' in doc['value']['data'].keys():
            new_doc['phone'] = doc['value']['data']['phone']

        # print(new_doc)
        return new_doc

    def upsert(self, doc):
        """Update or insert a document into Solr

        This method should call whatever add/insert/update method exists for
        the backend engine and add the document in there. The input will
        always be one mongo document, represented as a Python dictionary.
        """               
        #docs = [self.clean_doc(self.transform_doc(doc))]
        
        doc = self.clean_doc(self.transform_doc(doc))

        self.docs.append(doc)
        self.commit_bulk_num += 1

        if self.commit_bulk_num >= self.commit_bulk_max:            
            self.commit()              

    def sendDocsBulk(self):       

        if self.docs:
            #c = collections.Counter(self.docs)

            try:
                self.solr.add(self.docs, commit=True)
                #retry_until_ok(self.solr.commit)
                self.docs.clear()                
                logging.info("Bulk add: (%s) ", self.commit_bulk_num)
                self.commit_bulk_num = 0
            except SolrError:
                logging.error("Could not insert %r into Solr" % (self.docs))
            except:
                logging.error(sys.exc_info())
                raise             


    def remove(self, doc):
        """Removes documents from Solr

        The input is a python dictionary that represents a mongo document.
        """
        self.solr.delete(id=str(doc[self.unique_key]), commit=True)

    def _remove(self):
        """Removes everything
        """
        self.solr.delete(q='*:*')

    def search(self, start_ts, end_ts):
        """Called to query Solr for documents in a time range.
        """
        query = '_ts: [%s TO %s]' % (start_ts, end_ts)
        return self.solr.search(query, rows=100000000)

    def _search(self, query):
        """For test purposes only. Performs search on Solr with given query
            Does not have to be implemented.
        """
        return self.solr.search(query, rows=200)

    def commit(self):
        """This function is used to force a commit.
        """
        #self.sendDocsBulk()
        if self.docs:
            self.sendDocsBulk()
        #else:
            #retry_until_ok(self.solr.commit)

    def run_auto_commit(self):
        """Periodically commits to the Solr server.
        """
        self.solr.commit()
        if self.auto_commit:
            Timer(1, self.run_auto_commit).start()

    def get_last_doc(self):
        """Returns the last document stored in the Solr engine.
        """
        #search everything, sort by descending timestamp, return 1 row
        try:
            result = self.solr.search('*:*', sort='_ts desc', rows=1)
        except ValueError:
            return None

        if len(result) == 0:
            return None

        return result.docs[0]
