import json
import pytest,redis as redis_lib
from fastapi.testclient import TestClient
from conftest import add_readers,add_segment,get_segment
from src import worker
from src.services import publishing
from src.services.analysis import cluster
from src.services.business import dump
from src.config.database import Session
from src.models.entities import User,Reader
from src.utils.security import hash_password
FEATURES=['weekly_reading','completion','night_share']
@pytest.fixture()
def users(clean):
 with Session() as s:
  for role in ('admin','analyst','viewer'):s.add(User(email=role+'@t.local',role=role,password=hash_password('Demo123!')))
  s.commit()
@pytest.fixture()
def client(queue,monkeypatch):
 monkeypatch.setattr(redis_lib.Redis,'from_url',staticmethod(lambda *a,**k:queue))
 from src.main import app
 with TestClient(app) as c:yield c
def auth(client,email):
 r=client.post('/auth/login',json={'email':email,'password':'Demo123!'})
 assert r.status_code==200,r.text
 return {'Authorization':'Bearer '+r.json()['token']}
def submit(client,h,name='11月会员阅读分群',k=3):
 r=client.post('/segments',json={'name':name,'k':k,'features':FEATURES},headers=h)
 assert r.status_code==200,r.text
 return r.json()['id']
def test_export_matches_member_file_and_archive_count(client,users,queue):
 h=auth(client,'analyst@t.local')
 add_readers(36)
 sid=submit(client,h)
 assert worker.process_one(queue)
 assert get_segment(sid)['state']=='done'
 r=client.get(f'/segments/{sid}/export',headers=h)
 assert r.status_code==200
 res=r.json()['result']
 assert res['n']==36==len(res['snapshot'])
 assert sum(p['size'] for p in res['profiles'])==36
 on_disk=json.loads((publishing.dir()/res['artifact']).read_text(encoding='utf-8'))
 assert on_disk['checksum']==res['checksum'] and on_disk['members']==res['members']
 sizes={p['cluster_id']:p['size'] for p in res['profiles']}
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'低频读者轻阅读试点','note':'每周一次短篇精选，不增发'},headers=h)
 assert r.status_code==200,r.text
 assert len(r.json()['members'])==sizes[0]
def test_incomplete_publish_blocked_then_auto_repaired(client,users,queue):
 h=auth(client,'analyst@t.local')
 add_readers(30)
 sid=submit(client,h)
 worker.process_one(queue)
 (publishing.dir()/get_segment(sid)['result']['artifact']).unlink()   # 成员文件丢失 → 未完整发布
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==409
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'轻阅读','note':'每周一次短篇精选'},headers=h)
 assert r.status_code==409
 worker.sweep(queue)                                                   # 巡检发现缺失，按冻结快照重新发布
 worker.process_one(queue)
 assert get_segment(sid)['state']=='done'
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==200
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'轻阅读','note':'每周一次短篇精选'},headers=h)
 assert r.status_code==200
def test_not_done_blocked(client,users,queue):
 h=auth(client,'analyst@t.local')
 add_readers(30)
 sid=submit(client,h)                                                  # 仍在排队，未消费
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==409
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'测试','note':'未完成任务不可用'},headers=h)
 assert r.status_code==409
def test_completed_version_and_frozen_list_preserved(client,users,queue):
 h=auth(client,'analyst@t.local')
 add_readers(30)
 sid=submit(client,h,'冻结版本')
 worker.process_one(queue)
 before=get_segment(sid)['result']
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':1,'title':'保留名单','note':'冻结名单不随档案漂移'},headers=h)
 assert r.status_code==200,r.text
 members_before=r.json()['members']
 first=client.get('/readers',headers=h).json()[0]
 r=client.put(f"/readers/{first['id']}",json={'code':first['code'],'weekly_reading':99,'completion':0.9,'night_share':0.1,'note':'修订'},headers=h)
 assert r.status_code==200
 add_readers(3,prefix='N')
 worker.sweep(queue)                                                   # 已完成版本不被回收或重算
 after=get_segment(sid)
 assert after['state']=='done'
 assert after['result']['checksum']==before['checksum'] and after['result']['n']==30
 current=[o for o in client.get('/outreaches',headers=h).json() if o['segment_id']==sid][0]
 assert current['members']==members_before
def test_legacy_done_version_still_usable(client,users):
 h=auth(client,'analyst@t.local')
 add_readers(30)
 with Session() as s:rows=[dump(r) for r in s.query(Reader).order_by(Reader.id).all()]
 payload=cluster(rows,FEATURES,3)
 sid=add_segment(state='done',result={**payload,'snapshot':rows})      # 老版本：无run_id/artifact记录
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==200
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'历史版本','note':'已完成老版本仍可用于触达'},headers=h)
 assert r.status_code==200
 (publishing.dir()/publishing.legacy_name(sid)).write_text(json.dumps({'checksum':'bogus'}))
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==409  # 老成员文件与版本状态不一致 → 拒绝
def test_corrupt_member_file_blocked(client,users,queue):
 h=auth(client,'analyst@t.local')
 add_readers(30)
 sid=submit(client,h)
 worker.process_one(queue)
 path=publishing.dir()/get_segment(sid)['result']['artifact']
 path.write_text(json.dumps({'checksum':'tampered','members':[],'profiles':[],'n':0}))
 assert client.get(f'/segments/{sid}/export',headers=h).status_code==409
 r=client.post('/outreaches',json={'segment_id':sid,'cluster_id':0,'title':'篡改','note':'被篡改的结果不可触达'},headers=h)
 assert r.status_code==409
