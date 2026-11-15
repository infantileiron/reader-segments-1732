import json,threading,multiprocessing as mp
from conftest import add_readers,add_segment,get_segment,expire_claim
from src.config.database import Session
from src.models.entities import Reader
from src.services import publishing
from src.services.analysis import cluster
from src import worker
def test_claim_fencing_single_winner():
 # 多线程抢同一版本：条件更新保证只有一个执行者拿到run_id
 add_readers();sid=add_segment()
 barrier=threading.Barrier(8);wins=[]
 def t():
  barrier.wait()
  rid=worker.claim(sid)
  if rid:wins.append(rid)
 ts=[threading.Thread(target=t) for _ in range(8)]
 [x.start() for x in ts];[x.join() for x in ts]
 assert len(wins)==1
 assert get_segment(sid)['run_id']==wins[0]
def _claim_child(sid,q):
 from src.config.database import engine
 engine.dispose()
 from src import worker as w
 q.put(bool(w.claim(sid)))
def test_multiprocess_single_claim():
 # 多进程并发认领同一版本同样只有一个赢家
 add_readers();sid=add_segment()
 ctx=mp.get_context('fork');q=ctx.Queue()
 ps=[ctx.Process(target=_claim_child,args=(sid,q)) for _ in range(4)]
 [p.start() for p in ps];[p.join(30) for p in ps]
 assert all(p.exitcode==0 for p in ps)
 assert sum(q.get() for _ in ps)==1
def test_duplicate_delivery_publishes_once(queue):
 add_readers();sid=add_segment()
 queue.rpush(worker.QUEUE,sid);queue.rpush(worker.QUEUE,sid)
 assert worker.process_one(queue);assert worker.process_one(queue)
 seg=get_segment(sid)
 assert seg['state']=='done' and seg['attempts']==1
def test_interrupt_before_file_write_recovers(queue):
 # 认领后、写文件前中断：恢复后按冻结快照重算，人数合计等于档案数
 add_readers(30);sid=add_segment()
 assert worker.claim(sid)
 assert get_segment(sid)['state']=='running'
 expire_claim(sid)
 worker.recover(queue)
 assert get_segment(sid)['state']=='queued'
 assert worker.process_one(queue)
 seg=get_segment(sid);res=seg['result']
 assert seg['state']=='done'
 assert res['n']==30==len(res['snapshot'])==len(res['members'])
 assert sum(p['size'] for p in res['profiles'])==30
 assert (publishing.dir()/res['artifact']).exists()
def test_interrupt_after_file_write_recovers(queue):
 # 成员文件已落盘、完成状态未提交时中断：恢复后重新发布，状态与文件一致，孤儿文件被清理
 add_readers(30);sid=add_segment()
 rid=worker.claim(sid)
 seg=get_segment(sid)
 payload=cluster(seg['result']['snapshot'],seg['features'],seg['k'])
 crashed=publishing.artifact_name(sid,rid)
 publishing.write_atomic(crashed,{**payload,'snapshot':seg['result']['snapshot'],'run_id':rid,'artifact':crashed})
 expire_claim(sid)
 worker.recover(queue);worker.process_one(queue)
 seg=get_segment(sid);res=seg['result']
 assert seg['state']=='done' and res['run_id']!=rid and res['artifact']!=crashed
 on_disk=json.loads((publishing.dir()/res['artifact']).read_text(encoding='utf-8'))
 assert on_disk['checksum']==res['checksum']
 worker.sweep(queue)
 assert not (publishing.dir()/crashed).exists()
 assert len(list(publishing.dir().glob('segment-*-*.json')))==1
def test_partial_tmp_file_ignored(queue):
 # 写文件过程中中断：只剩.tmp残片，不影响恢复与最终发布
 add_readers(30);sid=add_segment()
 rid=worker.claim(sid)
 publishing.dir().mkdir(parents=True,exist_ok=True)
 (publishing.dir()/('.'+publishing.artifact_name(sid,rid)+'.tmp')).write_text('{"partial":')
 expire_claim(sid)
 worker.recover(queue);worker.process_one(queue)
 seg=get_segment(sid)
 assert seg['state']=='done'
 assert (publishing.dir()/seg['result']['artifact']).exists()
def test_stale_executor_cannot_overwrite(queue,monkeypatch):
 # A计算期间租约过期，B接管并发布；A收尾时提交必须被拒并丢弃产物
 add_readers(30);sid=add_segment()
 ridA=worker.claim(sid)
 real_cluster=worker.cluster
 entered=threading.Event();release=threading.Event();calls=[0]
 def slow_cluster(snapshot,features,k):
  calls[0]+=1
  if calls[0]==1:entered.set();release.wait(5)
  return real_cluster(snapshot,features,k)
 monkeypatch.setattr(worker,'cluster',slow_cluster)
 out={}
 t=threading.Thread(target=lambda:out.setdefault('A',worker.execute(sid,ridA)))
 t.start();assert entered.wait(5)
 expire_claim(sid);worker.recover(queue)
 ridB=worker.claim(sid)
 assert ridA and ridB and ridA!=ridB
 assert worker.execute(sid,ridB)=='done'
 published=get_segment(sid)['result']
 release.set();t.join()
 assert out['A']=='stale'
 seg=get_segment(sid)
 assert seg['result']['run_id']==ridB
 assert seg['result']['artifact']==published['artifact']
 assert not (publishing.dir()/publishing.artifact_name(sid,ridA)).exists()
 assert json.loads((publishing.dir()/published['artifact']).read_text(encoding='utf-8'))['checksum']==published['checksum']
 assert worker.execute(sid,ridA)=='skipped'   # 已易主的版本不再重复计算
def test_concurrent_publish_single_winner(queue):
 # 两个执行者同时收尾：只发布一份结果，只留一份成员文件
 add_readers(30);sid=add_segment()
 ridA=worker.claim(sid)
 expire_claim(sid);worker.recover(queue)
 ridB=worker.claim(sid)
 barrier=threading.Barrier(2);out={}
 def run(tag,rid):
  barrier.wait();out[tag]=worker.execute(sid,rid)
 t1=threading.Thread(target=run,args=('A',ridA));t2=threading.Thread(target=run,args=('B',ridB))
 t1.start();t2.start();t1.join();t2.join()
 done=[v for v in out.values() if v=='done']
 assert len(done)==1 and set(out.values())<={'done','stale','skipped'}   # 只发布一份结果
 assert get_segment(sid)['state']=='done'
 assert len(list(publishing.dir().glob('segment-*-*.json')))==1
def test_member_counts_match_archive(queue):
 add_readers(40);sid=add_segment(k=4)
 queue.rpush(worker.QUEUE,sid);worker.process_one(queue)
 seg=get_segment(sid);res=seg['result']
 assert seg['state']=='done'
 assert res['n']==40
 assert sum(p['size'] for p in res['profiles'])==40
 assert len({m['id'] for m in res['members']})==40
def test_recompute_uses_frozen_snapshot(queue):
 # 发起后档案新增，中断重算仍用发起时冻结的档案
 add_readers(30);sid=add_segment()
 add_readers(5,prefix='X')
 assert worker.claim(sid)
 expire_claim(sid)
 worker.recover(queue);worker.process_one(queue)
 res=get_segment(sid)['result']
 assert res['n']==30 and len(res['snapshot'])==30
def test_failed_is_terminal(queue):
 # 数据问题明确失败，不被自动重投
 add_readers(30)
 with Session() as s:
  for r in s.query(Reader):r.completion=0.5
  s.commit()
 sid=add_segment()
 queue.rpush(worker.QUEUE,sid);worker.process_one(queue)
 seg=get_segment(sid)
 assert seg['state']=='failed' and seg['error']
 assert worker.recover(queue)==[]
def test_write_error_keeps_running_for_retry(queue,monkeypatch):
 # 写文件失败保持running，租约到期后自动重算并成功发布
 add_readers(30);sid=add_segment()
 rid=worker.claim(sid)
 real_write=publishing.write_atomic;calls=[0]
 def flaky(name,payload):
  calls[0]+=1
  if calls[0]==1:raise OSError('disk full')
  return real_write(name,payload)
 monkeypatch.setattr(publishing,'write_atomic',flaky)
 assert worker.execute(sid,rid)=='retry'
 assert get_segment(sid)['state']=='running'
 expire_claim(sid)
 worker.recover(queue);worker.process_one(queue)
 assert get_segment(sid)['state']=='done'
def test_orphan_sweep_keeps_active_and_legacy(queue):
 add_readers(30);sid=add_segment()
 queue.rpush(worker.QUEUE,sid);worker.process_one(queue)
 active=get_segment(sid)['result']['artifact']
 orphan=publishing.artifact_name(sid,'deadbeef')
 (publishing.dir()/orphan).write_text('{}')
 (publishing.dir()/publishing.legacy_name(sid)).write_text('{}')
 worker.sweep(queue)
 assert (publishing.dir()/active).exists()
 assert not (publishing.dir()/orphan).exists()
 assert (publishing.dir()/publishing.legacy_name(sid)).exists()
