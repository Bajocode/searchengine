/* 
 docker exec \
 -ti cockroachdb sh \
 -c "./cockroach sql --insecure < cockroach-data/migrations/01_create_links_table_up.sql" 
 */
CREATE TABLE IF NOT EXISTS links (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid (),
    url STRING UNIQUE,
    retrieved_at timestamp
);

