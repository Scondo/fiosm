'''
Created on 01.04.2013

@author: Scondo
'''
from config import *
if use_osm:
    import psycopg2
    conn = psycopg2.connect(psy_dsn)
    conn.autocommit = True
    cur = conn.cursor()

    from sqlalchemy import create_engine
    from melt import Base, Statistic, BuildAssoc, StreetAssoc, PlaceAssoc
    engine = create_engine(al_dsn, echo=False)
    from argparse import ArgumentParser


def StatTableReCreate():
    Base.metadata.drop_all(engine, (Statistic.__table__, ))
    Statistic.__table__.create(engine)


def AssocTableReCreate():
    Base.metadata.drop_all(engine, [PlaceAssoc.__table__, StreetAssoc.__table__])
    PlaceAssoc.__table__.create(engine)
    StreetAssoc.__table__.create(engine)


def AssocBTableReCreate():
    Base.metadata.drop_all(engine, (BuildAssoc.__table__, ))
    Base.metadata.create_all(engine, (BuildAssoc.__table__, ))


def AssocIdxCreate():
    cur.execute("CREATE INDEX " + prefix + pl_aso_tbl + "_ao_id_idx ON " + \
                prefix + pl_aso_tbl + " USING btree (ao_id)")
    cur.execute("CREATE INDEX " + prefix + way_aso_tbl + "_ao_id_idx ON " + \
                prefix + way_aso_tbl + " USING btree (ao_id);")


def CleanupTableReCreate():
    cur.execute("DROP TABLE IF EXISTS fiosm_cleanup")
    cur.execute("CREATE TABLE fiosm_cleanup (tablename CHARACTER VARYING (120),  osmid   bigint);")


def CleanupTriggersReCreate():
    cur.execute("""CREATE OR REPLACE FUNCTION on_del_poly () RETURNS trigger AS
   $BODY$
   BEGIN
      INSERT INTO fiosm_cleanup (tablename, osmid) VALUES ('""" + prefix + poly_table + """', OLD.osm_id);
   END;
   $BODY$
   LANGUAGE plpgsql  VOLATILE  COST 100""")
    cur.execute("DROP TRIGGER IF EXISTS tr_del_poly ON public." + prefix + poly_table)
    cur.execute("""CREATE TRIGGER tr_del_poly
   BEFORE DELETE ON public.""" + prefix + poly_table + " FOR EACH ROW EXECUTE PROCEDURE on_del_poly();")

    cur.execute("""CREATE OR REPLACE FUNCTION on_del_way () RETURNS trigger AS
   $BODY$
   BEGIN
      INSERT INTO fiosm_cleanup (tablename, osmid) VALUES ('""" + prefix + ways_table + """', OLD.osm_id);
   END;
   $BODY$
   LANGUAGE plpgsql  VOLATILE  COST 100""")
    cur.execute("DROP TRIGGER IF EXISTS tr_del_way ON public." + prefix + ways_table)
    cur.execute("""CREATE TRIGGER tr_del_way
   BEFORE DELETE ON public.""" + prefix + ways_table + " FOR EACH ROW EXECUTE PROCEDURE on_del_way();")

if __name__ == '__main__' and use_osm:
    parser = ArgumentParser(description="Deploy FIOSM database part")
    parser.add_argument("--assocAO", action='store_true')
    parser.add_argument("--assocB", action='store_true')
    parser.add_argument("--assoc", action='store_true')
    parser.add_argument("--stat", action='store_true')
    parser.add_argument("--idx", action='store_true')

    args = parser.parse_args()
    if args.assocAO or args.assoc:
        AssocTableReCreate()
    if args.assocB or args.assoc:
        AssocBTableReCreate()
    if args.stat:
        StatTableReCreate()
    if args.idx:
        AssocIdxCreate()
