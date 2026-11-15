import os,time,json,logging,uuid
from pathlib import Path
from datetime import datetime,timezone
import redis
from sqlalchemy import update,or_,func
from src.config.database import Session,ensure_schema
from src.models.entities import Segment
from src.services.analysis import cluster
from src.services import publishing
QUEUE='reader:segments'
LEASE=float(os.environ.get('SEGMENT_LEASE_SECONDS','120'))   # running租约：超时认定执行者已中断
SWEEP=float(os.environ.get('SEGMENT_SWEEP_SECONDS','15'))    # 周期巡检：重投丢失的queued、回收过期running、清理孤儿文件
def claim(sid):
 # 原子认领：只有state仍是queued才能抢到；重复投递/多进程并发只有一个执行者拿到run_id。
 rid=uuid.uuid4().hex
 with Session() as s:
  res=s.execute(update(Segment).where(Segment.id==sid,Segment.state=='queued').values(state='running',run_id=rid,claimed_at=time.time(),attempts=func.coalesce(Segment.attempts,0)+1))
  s.commit()
 return rid if res.rowcount==1 else None
def _finish(sid,rid,**values):
 # 带围栏的收尾：run_id或state不匹配说明已被新执行者接管，过期执行者不能覆盖新结果。
 with Session() as s:
  res=s.execute(update(Segment).where(Segment.id==sid,Segment.run_id==rid,Segment.state=='running').values(**values))
  s.commit()
 return res.rowcount==1
def execute(sid,rid):
 with Session() as s:
  row=s.get(Segment,sid)
  if not row or row.run_id!=rid or row.state!='running':return 'skipped'
  snapshot=(row.result or {}).get('snapshot') or [];features,k=row.features,row.k
 try:
  payload=cluster(snapshot,features,k)          # 始终按发起时冻结的档案快照重算
  publishing.validate_payload(payload,len(snapshot))
 except Exception as e:
  _finish(sid,rid,state='failed',error=str(e)[:1500]);return 'failed'
 name=publishing.artifact_name(sid,rid)
 result={**payload,'snapshot':snapshot,'run_id':rid,'artifact':name,'published_at':datetime.now(timezone.utc).isoformat()}
 try:
  publishing.write_atomic(name,result)          # 先完整落盘成员文件，再提交完成状态
 except Exception:
  logging.exception('artifact write failed segment=%s',sid);return 'retry'  # 保持running，租约到期后自动重算
 if _finish(sid,rid,state='done',result=result,error=''):return 'done'
 publishing.discard(name)                        # 发布被拒：本执行者已过期，丢弃自己的产物
 logging.warning('stale executor discarded segment=%s run=%s',sid,rid);return 'stale'
def recover(r):
 # 中断恢复：过期running回到queued，所有queued重新入队（重复投递由claim去重）；
 # done版本成员文件缺失时按冻结快照重新发布，保证完成状态与成员文件最终一致。
 cutoff=time.time()-LEASE
 with Session() as s:
  s.execute(update(Segment).where(Segment.state=='running',or_(Segment.claimed_at.is_(None),Segment.claimed_at<cutoff)).values(state='queued'))
  for row in s.query(Segment).filter(Segment.state=='done').all():
   name=(row.result or {}).get('artifact')
   if name and not (publishing.dir()/name).exists():row.state='queued'
  s.commit()
  ids=[i for (i,) in s.query(Segment.id).filter(Segment.state=='queued').all()]
 for i in ids:r.rpush(QUEUE,i)
 return ids
def sweep(r):
 ids=recover(r)
 with Session() as s:
  active={row.result.get('artifact') for row in s.query(Segment).filter(Segment.state=='done') if row.result and row.result.get('artifact')}
  active|={publishing.artifact_name(row.id,row.run_id) for row in s.query(Segment).filter(Segment.state=='running') if row.run_id}
 publishing.sweep_orphans(active)
 return ids
def process_one(r,timeout=3):
 item=r.blpop(QUEUE,timeout=timeout)
 if not item:return False
 sid=int(item[1]);rid=claim(sid)
 if not rid:return True                          # 重复投递或已被并发执行者认领
 execute(sid,rid)
 return True
def main():
 logging.basicConfig(level=logging.INFO)
 ensure_schema()
 r=redis.Redis.from_url(os.environ['REDIS_URL'])
 last=0.0                                        # 首轮sweep即完成启动恢复
 while True:
  Path('/tmp/heartbeat').touch()
  try:
   if time.time()-last>SWEEP:sweep(r);last=time.time()
   process_one(r,timeout=3)
  except redis.RedisError:logging.exception('redis');time.sleep(2)
  except Exception:logging.exception('worker');time.sleep(2)
if __name__=='__main__':main()
