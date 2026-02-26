-- Add unique constraint on (image_id, table_index) to prevent duplicate
-- table extractions when re-processing the same article.

-- First remove duplicates, keeping only the most recent extraction
DELETE FROM fcai_article_extracted_tables a
USING fcai_article_extracted_tables b
WHERE a.image_id = b.image_id
  AND a.table_index = b.table_index
  AND a.id < b.id;

-- Now add the constraint (idempotent)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_extracted_table_image_index'
    ) THEN
        ALTER TABLE fcai_article_extracted_tables
            ADD CONSTRAINT uq_extracted_table_image_index
            UNIQUE (image_id, table_index);
    END IF;
END
$$;
