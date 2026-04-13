-- ============================================================
-- V3 column additions — computed fields that were buried in raw_json
-- Run AFTER schema_v3.sql (posts/comments tables)
-- ============================================================

-- Total upvotes across all scraped posts for this signal
alter table signals_social add column if not exists total_upvotes int default 0;

-- Total comment count across all scraped posts
alter table signals_social add column if not exists total_comment_count int default 0;

-- Percentage of texts containing repeat purchase language (0.0-1.0)
alter table signals_social add column if not exists repeat_purchase_pct float default 0;

-- Number of raw texts/posts the signal was derived from
alter table signals_social add column if not exists sample_size int default 0;
