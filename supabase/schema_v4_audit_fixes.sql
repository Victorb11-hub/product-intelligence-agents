-- ============================================================
-- V4 — Audit fixes: zero data loss from Apify to Supabase
-- Run AFTER schema_v3.sql
-- ============================================================

-- 1. Add missing columns to posts table for every Apify field
alter table posts add column if not exists reddit_id text;
alter table posts add column if not exists upvote_ratio float;
alter table posts add column if not exists data_type text;       -- 'post' or 'comment'
alter table posts add column if not exists parent_id text;       -- null for posts, parent post id for comments
alter table posts add column if not exists is_nsfw boolean default false;
alter table posts add column if not exists media_urls jsonb;     -- array of image/video URLs
alter table posts add column if not exists raw_json jsonb;       -- full Apify item as backup

-- Index on reddit_id for dedup checks
create index if not exists idx_posts_reddit_id on posts (reddit_id);
-- Index on data_type for filtering posts vs comments
create index if not exists idx_posts_data_type on posts (data_type);

-- 2. Add missing columns to signals_social
alter table signals_social add column if not exists total_upvotes int default 0;
alter table signals_social add column if not exists total_comment_count int default 0;
alter table signals_social add column if not exists repeat_purchase_pct float default 0;
alter table signals_social add column if not exists sample_size int default 0;
alter table signals_social add column if not exists integrity_verified boolean default false;
alter table signals_social add column if not exists integrity_errors text;

-- 3. Add integrity fields to agent_runs
alter table agent_runs add column if not exists integrity_check_passed boolean;
alter table agent_runs add column if not exists integrity_errors jsonb;
