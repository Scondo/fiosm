use_osm = True

conn_par = {"db": "fiosm", "user": "fiosm", "host": "192.168.56.101", "pass": "fiosm"}
psy_dsn = "dbname={db} user={user} host={host} password={pass}".format(**conn_par)
al_dsn = "postgresql://{user}:{pass}@{host}/{db}".format(**conn_par)

prefix='planet_osm_'
poly_table='polygon'
ways_table='line'
point_table='point'

slim_node_tbl='nodes'
slim_way_tbl='ways'
slim_rel_tbl='rels'

pl_aso_tbl='fias_place'
way_aso_tbl='fias_street'
bld_aso_tbl='fias_build'

use_osm = True

