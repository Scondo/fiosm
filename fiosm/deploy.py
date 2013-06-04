'''
Created on 01.04.2013

@author: Scondo
'''
from config import *
import psycopg2
conn = psycopg2.connect(connstr)
conn.autocommit = True
cur = conn.cursor()
from argparse import ArgumentParser


def AssocTriggersReCreate():
    cur.execute("""CREATE OR REPLACE FUNCTION on_del_poly () RETURNS trigger AS
   $BODY$
   BEGIN
      --remove houses
      DELETE FROM """+prefix+bld_aso_tbl+""" WHERE osm_build = OLD.osm_id AND point = 0;
      --remove place
      DELETE FROM """+prefix+pl_aso_tbl+""" WHERE osm_admin = OLD.osm_id;
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql   VOLATILE   COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_poly ON public."""+prefix+poly_table)
    cur.execute("""CREATE TRIGGER tr_del_poly
   BEFORE DELETE ON public."""+prefix+poly_table+""" FOR EACH ROW
   EXECUTE PROCEDURE on_del_poly ();""")

    cur.execute("""CREATE OR REPLACE FUNCTION on_del_line () RETURNS trigger AS
   $BODY$
   BEGIN
      --remove street
      DELETE FROM """+prefix+way_aso_tbl+""" WHERE osm_way = OLD.osm_id;
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql   VOLATILE   COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_line ON public."""+prefix+ways_table)
    cur.execute("""CREATE TRIGGER tr_del_line
   BEFORE DELETE ON public."""+prefix+ways_table+""" FOR EACH ROW
   EXECUTE PROCEDURE on_del_line ();""")
    
    cur.execute("""CREATE OR REPLACE FUNCTION on_del_f_house () RETURNS trigger AS
   $BODY$
   BEGIN
      DELETE FROM """+prefix+bld_aso_tbl+""" WHERE aoguid = OLD.houseguid;
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql VOLATILE COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_f_house ON public.fias_house""")
    cur.execute("""CREATE TRIGGER tr_del_f_house
   BEFORE DELETE ON public.fias_house FOR EACH ROW
   EXECUTE PROCEDURE on_del_f_house ();""")

    cur.execute("""CREATE OR REPLACE FUNCTION on_del_f_ao () RETURNS trigger AS
   $BODY$
   BEGIN
      DELETE FROM """+prefix+way_aso_tbl+""" WHERE aoguid = OLD.aoguid;
      DELETE FROM """+prefix+pl_aso_tbl+""" WHERE aoguid = OLD.aoguid;
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql VOLATILE COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_f_ao ON public.fias_addr_obj""")
    cur.execute("""CREATE TRIGGER tr_del_f_ao
   BEFORE DELETE ON public.fias_addr_obj FOR EACH ROW
   EXECUTE PROCEDURE on_del_f_ao ();""")

    cur.execute("""CREATE OR REPLACE FUNCTION on_del_place () RETURNS trigger AS
   $BODY$
   BEGIN
      --remove houses
      DELETE FROM """+prefix+bld_aso_tbl+""" WHERE EXISTS
        (SELECT houseguid FROM fias_house WHERE 
        fias_house.houseguid = """+prefix+bld_aso_tbl+""".aoguid AND fias_house.aoguid = OLD.aoguid);
      --remove subs
      DELETE FROM """+prefix+pl_aso_tbl+""" WHERE EXISTS
        (SELECT aoguid FROM fias_addr_obj WHERE 
        fias_addr_obj.aoguid = """+prefix+pl_aso_tbl+""".aoguid AND fias_addr_obj.parentguid = OLD.aoguid);
      --remove streets
      DELETE FROM """+prefix+way_aso_tbl+""" WHERE EXISTS
        (SELECT aoguid FROM fias_addr_obj WHERE
        fias_addr_obj.aoguid = """+prefix+way_aso_tbl+""".aoguid AND fias_addr_obj.parentguid = OLD.aoguid);
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql   VOLATILE   COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_place ON public."""+prefix+pl_aso_tbl)
    cur.execute("""CREATE TRIGGER tr_del_place
   BEFORE DELETE ON public."""+prefix+pl_aso_tbl+""" FOR EACH ROW
   EXECUTE PROCEDURE on_del_place ();""")
    
    cur.execute("""CREATE OR REPLACE FUNCTION on_del_street () RETURNS trigger AS
   $BODY$
   BEGIN
      --remove houses
      DELETE FROM """+prefix+bld_aso_tbl+""" WHERE EXISTS
        (SELECT houseguid FROM fias_house WHERE 
        fias_house.houseguid = """+prefix+bld_aso_tbl+""".aoguid AND fias_house.aoguid = OLD.aoguid);
      RETURN OLD;
   END;
   $BODY$
   LANGUAGE plpgsql   VOLATILE   COST 100""")
    cur.execute("""DROP TRIGGER IF EXISTS tr_del_street ON public."""+prefix+way_aso_tbl)
    cur.execute("""CREATE TRIGGER tr_del_street
   BEFORE DELETE ON public."""+prefix+way_aso_tbl+""" FOR EACH ROW
   EXECUTE PROCEDURE on_del_street ();""")


def StatTableReCreate():
    cur.execute("DROP TABLE IF EXISTS fiosm_stat")
    cur.execute("""CREATE TABLE fiosm_stat(aoguid    uuid,
    ao_all  integer, found     integer, street    integer,
    all_b   integer, found_b   integer,
    all_r   integer, found_r   integer, street_r    integer,
    all_b_r integer, found_b_r   integer
);""")


def StatIdxCreate():
    cur.execute("CREATE INDEX fiosm_stat_aoguid_idx ON fiosm_stat USING btree (aoguid);")


def AssocTableReCreate():
    cur.execute("DROP TABLE IF EXISTS " + prefix + pl_aso_tbl)
    cur.execute("CREATE TABLE " + prefix + pl_aso_tbl + "(aoguid UUID,  osm_admin bigint);")
    cur.execute("DROP TABLE IF EXISTS " + prefix + way_aso_tbl)
    cur.execute("CREATE TABLE " + prefix + way_aso_tbl + "(aoguid UUID,  osm_way  bigint);")


def AssocBTableReCreate():
    cur.execute("DROP TABLE IF EXISTS " + prefix + bld_aso_tbl)
    cur.execute("CREATE TABLE " + prefix + bld_aso_tbl + "(f_id  int,  osm_build   bigint, point smallint);")


def AssocIdxCreate():
    cur.execute("CREATE INDEX " + prefix + pl_aso_tbl + "_aoguid_idx ON " + prefix + pl_aso_tbl + """ USING btree (aoguid);
CREATE INDEX """ + prefix + pl_aso_tbl + "_osm_admin_idx ON " + prefix + pl_aso_tbl + """ USING btree (osm_admin);""")
    cur.execute("CREATE INDEX " + prefix + way_aso_tbl + "_aoguid_idx ON " + prefix + way_aso_tbl + """ USING btree (aoguid);
CREATE INDEX """ + prefix + way_aso_tbl + "_osm_way_idx ON " + prefix + way_aso_tbl + """ USING btree (osm_way);""")


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

if __name__ == '__main__':
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
        StatIdxCreate()
    if args.idx:
        AssocIdxCreate()
