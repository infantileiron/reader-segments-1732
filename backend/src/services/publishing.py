import json,os,time,logging
from pathlib import Path
# 成员文件（分群产物）的写入、校验与清理。worker发布和API导出/触达共用同一套一致性口径。
def dir():return Path(os.environ.get('ARTIFACT_DIR','/artifacts'))
def grace():return float(os.environ.get('SEGMENT_ORPHAN_GRACE','600'))
class PublishError(Exception):pass
def artifact_name(segment_id,run_id):return f'segment-{segment_id}-{run_id}.json'
def legacy_name(segment_id):return f'{segment_id}.json'
def write_atomic(name,payload):
 # 先写临时文件并fsync，再同目录rename：任何时刻读到的要么是旧版本，要么是完整新版本。
 d=dir();d.mkdir(parents=True,exist_ok=True)
 tmp=d/('.'+name+'.tmp');final=d/name
 with open(tmp,'w',encoding='utf-8') as f:
  json.dump(payload,f,ensure_ascii=False);f.flush();os.fsync(f.fileno())
 os.replace(tmp,final)
 try:
  fd=os.open(d,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
 except OSError:pass
 return final
def read(name):
 try:return json.loads((dir()/name).read_text(encoding='utf-8'))
 except FileNotFoundError:raise PublishError('分群结果文件缺失，等待自动恢复或重新计算')
 except (ValueError,OSError):raise PublishError('分群结果文件损坏，等待自动恢复或重新计算')
def discard(name):
 try:(dir()/name).unlink()
 except FileNotFoundError:pass
def validate_payload(payload,expected):
 # 群内人数合计必须等于发起时冻结的档案数，且每个会员恰好属于一个群。
 members=payload.get('members') or [];profiles=payload.get('profiles') or []
 if payload.get('n')!=expected or len(members)!=expected:raise ValueError(f'群成员合计{len(members)}与档案数{expected}不一致')
 if sum(int(p.get('size',0)) for p in profiles)!=expected:raise ValueError('各群人数合计与档案数不一致')
 ids=[m.get('id') for m in members]
 if len(set(ids))!=len(ids):raise ValueError('群成员存在重复会员')
 if not profiles or any(p.get('cluster_id') is None for p in profiles):raise ValueError('群画像不完整')
def verify_published(row):
 # 完成状态、成员文件与版本结果三者一致才视为已完整发布；老版本没有artifact记录时以库内结果为准。
 res=row.result or {}
 snapshot=res.get('snapshot') or []
 payload=res
 name=res.get('artifact')
 if name:
  payload=read(name)
  if payload.get('checksum')!=res.get('checksum'):raise PublishError('成员文件与版本状态不一致，等待自动恢复')
 else:
  legacy=dir()/legacy_name(row.id)
  if legacy.exists():
   try:old=json.loads(legacy.read_text(encoding='utf-8'))
   except ValueError:raise PublishError('成员文件损坏，等待自动恢复或重新计算')
   if old.get('checksum')!=res.get('checksum'):raise PublishError('成员文件与版本状态不一致，等待自动恢复')
 try:validate_payload(payload,len(snapshot))
 except ValueError as e:raise PublishError(str(e))
 return payload
def sweep_orphans(active_names):
 # 清理中断/过期执行者留下的孤儿文件；宽限期内和仍被引用的文件不动，历史<id>.json不动。
 d=dir()
 if not d.exists():return []
 removed=[];cutoff=time.time()-grace()
 for p in d.glob('segment-*-*.json'):
  if p.name in active_names:continue
  try:
   if p.stat().st_mtime<cutoff:p.unlink();removed.append(p.name)
  except FileNotFoundError:pass
 if removed:logging.info('orphan artifacts removed: %s',removed)
 return removed
