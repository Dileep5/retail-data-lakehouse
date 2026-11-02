SELECT batch_id, window_start, window_end, status, finished_at
FROM meta.ingestion_batches
ORDER BY batch_id;
