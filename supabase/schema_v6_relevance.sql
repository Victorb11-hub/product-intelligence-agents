-- V6 — Relevance scoring for posts + discarded count for agent_runs
alter table posts add column if not exists relevance_score float default 0;
create index if not exists idx_posts_relevance on posts (relevance_score desc);
alter table agent_runs add column if not exists irrelevant_posts_discarded int default 0;
