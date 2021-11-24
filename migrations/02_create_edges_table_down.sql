/*
 docker exec \
 -ti cockroachdb sh \
 -c "./cockroach sql --insecure < cockroach-data/migrations/02_create_edges_table_down.sql"
 */
DROP TABLE IF EXISTS edges;

