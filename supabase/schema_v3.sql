-- ============================================================
-- Product Intelligence System — V3 Schema
-- Posts & Comments tables for raw content storage
-- Run AFTER schema_v2.sql
-- ============================================================

-- ============================================================
-- 1. POSTS — individual scraped posts from all platforms
-- ============================================================
create table posts (
  id              uuid primary key default uuid_generate_v4(),
  product_id      uuid not null references products(id) on delete cascade,
  run_id          uuid,
  platform        text not null,
  post_title      text,
  post_body       text,
  post_url        text,
  subreddit       text,
  upvotes         int default 0,
  comment_count   int default 0,
  author          text,
  posted_at       timestamptz,
  scraped_date    date not null default current_date,
  intent_level    int check (intent_level between 1 and 5),
  sentiment_score float,
  anomaly_flag    boolean default false,
  created_at      timestamptz not null default now()
);

create index idx_posts_product on posts (product_id);
create index idx_posts_run on posts (run_id);
create index idx_posts_platform on posts (platform);
create index idx_posts_intent on posts (intent_level desc);
create index idx_posts_sentiment on posts (sentiment_score desc);
create index idx_posts_scraped on posts (scraped_date desc);

-- ============================================================
-- 2. COMMENTS — individual comments attached to posts
-- ============================================================
create table comments (
  id                    uuid primary key default uuid_generate_v4(),
  post_id               uuid not null references posts(id) on delete cascade,
  product_id            uuid not null references products(id) on delete cascade,
  platform              text not null,
  comment_body          text,
  author                text,
  upvotes               int default 0,
  intent_level          int check (intent_level between 1 and 5),
  sentiment_score       float,
  is_buy_intent         boolean default false,
  is_problem_language   boolean default false,
  is_repeat_purchase    boolean default false,
  posted_at             timestamptz,
  created_at            timestamptz not null default now()
);

create index idx_comments_post on comments (post_id);
create index idx_comments_product on comments (product_id);
create index idx_comments_platform on comments (platform);
create index idx_comments_intent on comments (intent_level desc);
create index idx_comments_buy_intent on comments (is_buy_intent) where is_buy_intent = true;

-- ============================================================
-- RLS policies (dev — open access)
-- ============================================================
alter table posts enable row level security;
alter table comments enable row level security;

do $$
declare
  t text;
begin
  for t in select unnest(array['posts','comments']) loop
    execute format('create policy "Allow anon select on %I" on %I for select using (true)', t, t);
    execute format('create policy "Allow anon insert on %I" on %I for insert with check (true)', t, t);
    execute format('create policy "Allow anon update on %I" on %I for update using (true) with check (true)', t, t);
    execute format('create policy "Allow anon delete on %I" on %I for delete using (true)', t, t);
  end loop;
end $$;

-- ============================================================
-- Enable realtime
-- ============================================================
alter publication supabase_realtime add table posts;
