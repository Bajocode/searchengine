/*
 docker exec \
 -ti cockroachdb sh \
 -c "./cockroach sql --insecure < cockroach-data/migrations/01_create_links_table_down.sql"
 */
DROP TABLE IF EXISTS links;

