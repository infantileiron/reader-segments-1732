import os,sys,tempfile,time,threading
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TMP=tempfile.mkdtemp(prefix='segtest-')
os.environ['DATABASE_URL']='sqlite:///'+os.path.join(TMP,'test.db')
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['JWT_SECRET']='test-secret'
os.environ['ARTIFACT_DIR']=os.path.join(TMP,'artifacts')
os.environ['SEGMENT_LEASE_SECONDS']='0.2'
os.environ['SEGMENT_SWEEP_SECONDS']='0.1'
os.environ['SEGMENT_ORPHAN_GRACE']='0'
import pytest
from sqlalchemy import update
from src.config.database import Session,ensure_schema
from src.models.entities import User,Reader,Segment,Outreach,Audit
from src.services.business import dump
from src.services import publishing
from src import worker
class FakeRedis:
 def __init__(self):self.lists={};self.lock=threading.Lock()
 def rpush(self,k,v):
  with self.lock:self.lists.setdefault(k,[]).append(str(v))
 def blpop(self,k,timeout=0):
  end=time.time()+min(float(timeout),0.05)
  while True:
   with self.lock:
    q=self.lists.get(k)
    if q:return (k,q.pop(0))
   if time.time()>=end:return None
   time.sleep(0.005)
@pytest.fixture()
def queue():return FakeRedis()
@pytest.fixture(autouse=True)
def clean():
 ensure_schema()
 with Session() as s:
  for t in (Audit,Outreach,Segment,Reader,User):s.query(t).delete()
  s.commit()
 d=publishing.dir()
 if d.exists():
  for p in d.iterdir():p.unlink()
 yield
def add_readers(n=30,prefix='T'):
 with Session() as s:
  for i in range(n):
   mode=i%3
   s.add(Reader(code=f'{prefix}-{i:03}',weekly_reading=round([4.,14.,26.][mode]+i*0.01,2),completion=round(min(1.,[.3,.8,.6][mode]+i*0.001),4),night_share=[.2,.25,.7][mode],note=''))
  s.commit()
def add_segment(k=3,state='queued',result=None,name='测试分群'):
 with Session() as s:
  if result is None:result={'snapshot':[dump(r) for r in s.query(Reader).order_by(Reader.id).all()]}
  seg=Segment(name=name,k=k,features=['weekly_reading','completion','night_share'],state=state,result=result)
  s.add(seg);s.commit();return seg.id
def get_segment(sid):
 with Session() as s:return dump(s.get(Segment,sid))
def expire_claim(sid):
 with Session() as s:
  s.execute(update(Segment).where(Segment.id==sid).values(claimed_at=0));s.commit()
