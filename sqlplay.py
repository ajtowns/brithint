import sqlalchemy
from sqlalchemy import *

engine = create_engine("sqlite:///brithint.sqlite")
m = MetaData(bind=engine)
m.reflect()
c = engine.connect()

ev = m.tables["events"]
b = m.tables["block"]
