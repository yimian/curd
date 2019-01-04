import time
import pytest
from .operations import (
    delete, normal_filter, filter_with_order_by, thread_pool, update
)
from phoenixdb.errors import NotSupportedError

from curd import Session
from .conf import hbase_conf

    
def create_test_table(session):
    session.execute('CREATE SCHEMA IF NOT EXISTS "curd"')
    session.execute('DROP TABLE IF EXISTS "curd"."test"')
    session.execute("""CREATE TABLE IF NOT EXISTS "curd"."test" ("id" bigint NOT NULL, "text" varchar, CONSTRAINT pk PRIMARY KEY ("id")) DATA_BLOCK_ENCODING='FAST_DIFF', COMPRESSION = 'GZ'""")
    return 'curd.test'


def create(session, create_test_table):
    collection = create_test_table(session)

    data = {'id': 100, 'text': 'test'}

    session.create(collection, data)
    # do test for habse
    # with pytest.raises(DuplicateKeyError):
    #     session.create(collection, data, mode='insert')

    assert data == session.get(collection, [('=', 'id', 100)])

    time.sleep(10)
    data2 = {'id': 100, 'text': 't2'}
    session.create(collection, data2, mode='replace')

    assert data != session.get(collection, [('=', 'id', 100)])


def update(session, create_test_table):
    collection = create_test_table(session)
    data = {'id': 100, 'text': 'test'}
    session.create(collection, data)
    with pytest.raises(NotSupportedError):
        session.update(collection, {'text': 't2'}, [('=', 'id', data['id'])])


def test_hbase():
    session = Session([hbase_conf])
    print('>>>>>>>>>>>>>> test create <<<<<<<<<<<<<<<<<')
    create(session, create_test_table)

    print('>>>>>>>>>>>>>> test update <<<<<<<<<<<<<<<<<')
    update(session, create_test_table)

    print('>>>>>>>>>>>>>> test delete <<<<<<<<<<<<<<<<<')
    delete(session, create_test_table)

    print('>>>>>>>>>>>>>> test normal filter <<<<<<<<<<<<<<<<<')
    normal_filter(session, create_test_table, size=50)

    print('>>>>>>>>>>>>>> test order by filter <<<<<<<<<<<<<<<<<')
    filter_with_order_by(session, create_test_table, size=50)

    print('>>>>>>>>>>>>>> test multi thread create <<<<<<<<<<<<<<<<<')
    thread_pool(session, create_test_table, size=100)

