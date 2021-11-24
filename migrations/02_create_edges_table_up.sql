/* 
 docker exec \
 -ti cockroachdb sh \
 -c "./cockroach sql --insecure < cockroach-data/migrations/02_create_edges_table_up.sql" 
 */
CREATE TABLE IF NOT EXISTS edges (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    src uuid NOT NULL REFERENCES links (id) ON DELETE CASCADE,
    dst uuid NOT NULL REFERENCES links (id) ON DELETE CASCADE,
    updated_at timestamp,
    CONSTRAINT edge_links UNIQUE (src, dst)
);

