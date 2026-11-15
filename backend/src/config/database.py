import os
from sqlalchemy import create_engine,inspect,text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, declarative_base
_url=os.environ['DATABASE_URL']
_kw={'pool_pre_ping':True}
if _url.startswith('sqlite'):_kw['connect_args']={'timeout':30,'check_same_thread':False}
engine=create_engine(_url,**_kw)
Session=sessionmaker(bind=engine,expire_on_commit=False)
Base=declarative_base()
# 新增列的幂等补齐：create_all只建缺失的表，老库需要补列；并发启动时重复ALTER会报错，吞掉后统一校验。
_SEGMENT_COLUMNS=(('run_id','VARCHAR(40)'),('claimed_at','DOUBLE PRECISION'),('attempts','INTEGER'))
def ensure_schema():
 Base.metadata.create_all(engine)
 for name,ddl in _SEGMENT_COLUMNS:
  with engine.begin() as c:
   if name not in {col['name'] for col in inspect(c).get_columns('segments')}:
    try:c.execute(text(f'ALTER TABLE segments ADD COLUMN {name} {ddl}'))
    except SQLAlchemyError:pass
 with engine.connect() as c:have={col['name'] for col in inspect(c).get_columns('segments')}
 missing=[n for n,_ in _SEGMENT_COLUMNS if n not in have]
 if missing:raise RuntimeError('segments缺少列: '+','.join(missing))
def db():
 with Session() as s: yield s
