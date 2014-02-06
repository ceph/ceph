from sqlalchemy import create_engine
from pecan import conf  # noqa
from db import Session, Base
import sys

def create_from_conf():
    configs = dict(conf.sqlalchemy)
    url = configs.pop('url')
    return create_engine(url, **configs)

def init_model():
    engine = create_from_conf()
    conf.sqlalchemy.engine = engine
    engine.connect()
    #create the tables if not existing
    Base.metadata.create_all(engine)

def start():
    print >> sys.stderr, " --- MODEL START"
    Session.bind = conf.sqlalchemy.engine

def commit():
    print >> sys.stderr, " --- MODEL COMMIT"
    Session.commit()

def rollback():
    print >> sys.stderr, " --- MODEL ROLLBACK"
    Session.rollback()

def clear():
    print >> sys.stderr, " --- MODEL CLEAR"
    Session.remove()

