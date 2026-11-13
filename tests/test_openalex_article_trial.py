import json
import tempfile
import unittest
from pathlib import Path
from app.openalex_catalog import OpenAlexCatalog, connect
from app.openalex_article_trial import apply
from app.openalex_snapshot import SnapshotError

class ArticleTrialTests(unittest.TestCase):
    def test_insert_only_and_idempotent_with_fts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); db=root/'base.sqlite'; sample=root/'sample.json'
            catalog=OpenAlexCatalog(db)
            with connect(db) as c:
                c.execute("INSERT INTO openalex_snapshot_state(singleton,dataset_version,state,complete,expected_records,expected_files,imported_works) VALUES(1,'2026-09-23','complete',1,1,1,1)")
                c.execute("INSERT INTO openalex_works(dataset_version,openalex_id,title) VALUES('2026-09-23','W1','Original')")
            rows=[{'id':f'https://openalex.org/W{i}','type':'article','publication_date':'2026-10-01','title':'Trial quantum'} for i in (1,2)]
            sample.write_text(json.dumps({'results':rows}))
            result=apply(db,sample,'2026-09-24','2026-10-14')
            self.assertEqual(result['ids'],['W2'])
            self.assertEqual(catalog.by_id('W1')['work']['title'],'Original')
            self.assertEqual(len(catalog.search('quantum',None,10)['results']),1)
            self.assertEqual(apply(db,sample,'2026-09-24','2026-10-14')['inserted'],0)
            self.assertEqual(catalog.status()['imported_works'],2)
            rows[0]['publication_date']='2000-01-01';sample.write_text(json.dumps({'results':rows}))
            with self.assertRaises(SnapshotError):apply(db,sample,'2026-09-24','2026-10-14')
