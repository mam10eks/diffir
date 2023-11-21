from ir_datasets.formats import GenericQrel, BaseDocs, BaseQueries, BaseQrels, GenericDoc, GenericQuery
import json
from ir_datasets.datasets.base import Dataset
from typing import NamedTuple, List, Mapping, Optional, Iterator
from ir_datasets.indices import Docstore
from ir_datasets import registry
from copy import deepcopy
import gzip

class GenericDocFromDict:
    _fields = []
    def __init__(self, data):
        self.data = data['original_document']
        self.doc_id = self.data['doc_id']
        self.text = data['text']
        GenericDocFromDict._fields = ['doc_id', 'text'] + sorted([i for i in set(GenericDocFromDict._fields + list(self.data.keys())) if i != 'doc_id' and i != 'text'])

    def default_text(self):
        return self.text
    
    def _asdict(self):
        ret = deepcopy(self.data)
        ret['doc_id'] = self.doc_id
        ret['text'] = self.text
        
        return ret

    def __getattr__(self, attr):
        if attr in self.data and attr != 'text' and attr != 'doc_id':
            return self.data[attr]

        return self.__getattribute__(attr)


class DummyDocsStore(NamedTuple):
    docs: dict

    def get(self, i):
        return self.docs.get(i, GenericDocFromDict({'original_document': {'doc_id': i}, 'text': 'COULD NOT LOAD DOCUMENT.'}))
    
    def get_many_iter(self, doc_ids):
        return [self.get(i) for i in doc_ids]

class GenericFromDictDocs(BaseDocs):
    def __init__(self, docs):
        self.docs = docs

    def docs_iter(self) -> Iterator[GenericDocFromDict]:
        return self.docs.values()

    def docs_count(self) -> int:
        return len(self.docs)

    def docs_cls(self):
        return GenericDocFromDict

    def docs_store(self) -> Docstore:
        return DummyDocsStore(docs=self.docs)


def register_irds_from_files(data_files):
    name = str(data_files)
    if name in registry:
        return name

    queries = []
    documents = {}
    qrels = []
    qrels_defs = {}
    for r in data_files:
        if r.endswith('.gz'):
            r = json.load(gzip.open(r, 'rt'))
        else:
            r = json.load(open(r))
        for docno, doc in r['documents'].items():
           documents[docno] = GenericDocFromDict(doc)
        for qid, query in r['queries'].items():
           queries += [GenericQuery(query_id=qid, text=query['query'])]
        for qid in r['qrels']:
            for docno, rel in r['qrels'][qid].items():
                qrels_defs[rel] = str(rel)
                qrels += [GenericQrel(query_id=qid, doc_id=docno, relevance=rel)]
    irds_docs = GenericFromDictDocs(documents)
    irds_qrels = BaseQrels()
    irds_qrels.qrels_iter = lambda: qrels
    irds_qrels.qrels_defs = lambda: qrels_defs
    irds_queries = BaseQueries()
    irds_queries.queries_iter = lambda: queries
    dataset = Dataset(irds_docs, irds_queries, irds_qrels)
    registry.register(name, dataset)
    return name
