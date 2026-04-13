-- ============================================================
-- Product Intelligence System — Supabase Schema
-- ============================================================

-- Enable required extensions
create extension if not exists "uuid-ossp";

-- ============================================================
-- 1. PRODUCTS
-- ============================================================
create table products (
  id            uuid primary key default uuid_generate_v4(),
  name          text not null,
  category      text not null,
  keywords      jsonb default '[]'::jsonb,
  first_seen_date date not null default current_date,
  current_verdict text not null default 'watch' check (current_verdict in ('buy','watch','pass')),
  current_score  float not null default 0,
  lifecycle_phase text not null default 'early' check (lifecycle_phase in ('early','buy_window','peak','plateau','declining')),
  fad_flag       boolean not null default false,
  active         boolean not null default true,
  created_at     timestamptz not null default now()
);

create index idx_products_active on products (active);
create index idx_products_verdict on products (current_verdict);
create index idx_products_category on products (category);
create index idx_products_score on products (current_score desc);

-- ============================================================
-- 2. SIGNALS — SOCIAL
-- ============================================================
create table signals_social (
  id               uuid primary key default uuid_generate_v4(),
  product_id       uuid not null references products(id) on delete cascade,
  scraped_date     date not null default current_date,
  platform         text not null check (platform in ('tiktok','instagram','x','facebook','reddit','youtube')),
  data_confidence  float not null default 1.0,
  mention_count    int,
  growth_rate_wow  float,
  sentiment_score  float,
  velocity_score   float,
  creator_tier_score float,
  buy_intent_comment_count int,
  problem_language_comment_count int,
  raw_json         jsonb
);

create index idx_signals_social_product on signals_social (product_id, scraped_date);
create index idx_signals_social_platform on signals_social (platform, scraped_date);

-- ============================================================
-- 3. SIGNALS — SEARCH
-- ============================================================
create table signals_search (
  id                  uuid primary key default uuid_generate_v4(),
  product_id          uuid not null references products(id) on delete cascade,
  scraped_date        date not null default current_date,
  platform            text not null default 'google_trends' check (platform in ('google_trends')),
  data_confidence     float not null default 1.0,
  slope_24m           float,
  breakout_flag       boolean default false,
  yoy_growth          float,
  seasonal_pattern    text,
  related_rising_queries jsonb,
  news_trigger_flag   boolean default false
);

create index idx_signals_search_product on signals_search (product_id, scraped_date);

-- ============================================================
-- 4. SIGNALS — RETAIL
-- ============================================================
create table signals_retail (
  id                    uuid primary key default uuid_generate_v4(),
  product_id            uuid not null references products(id) on delete cascade,
  scraped_date          date not null default current_date,
  platform              text not null check (platform in ('amazon','walmart','etsy')),
  data_confidence       float not null default 1.0,
  bestseller_rank       int,
  rank_change_wow       int,
  review_count          int,
  review_count_growth   float,
  review_sentiment      float,
  search_rank           int,
  out_of_stock_flag     boolean default false,
  price                 numeric(10,2),
  price_history         jsonb,
  handmade_vs_mass_market_ratio float
);

create index idx_signals_retail_product on signals_retail (product_id, scraped_date);
create index idx_signals_retail_platform on signals_retail (platform, scraped_date);

-- ============================================================
-- 5. SIGNALS — SUPPLY
-- ============================================================
create table signals_supply (
  id                     uuid primary key default uuid_generate_v4(),
  product_id             uuid not null references products(id) on delete cascade,
  scraped_date           date not null default current_date,
  platform               text not null default 'alibaba' check (platform in ('alibaba')),
  data_confidence        float not null default 1.0,
  supplier_listing_count int,
  supplier_count_change  int,
  moq_current            int,
  moq_trend              text,
  price_per_unit         numeric(10,2),
  competing_supplier_count int,
  new_category_flag      boolean default false
);

create index idx_signals_supply_product on signals_supply (product_id, scraped_date);

-- ============================================================
-- 6. SIGNALS — DISCOVERY
-- ============================================================
create table signals_discovery (
  id                   uuid primary key default uuid_generate_v4(),
  product_id           uuid not null references products(id) on delete cascade,
  scraped_date         date not null default current_date,
  platform             text not null default 'pinterest' check (platform in ('pinterest')),
  data_confidence      float not null default 1.0,
  pin_save_rate        float,
  save_rate_growth     float,
  board_creation_count int,
  keyword_search_volume int,
  trending_category_flag boolean default false,
  demographic_score    float
);

create index idx_signals_discovery_product on signals_discovery (product_id, scraped_date);

-- ============================================================
-- 7. SCORES HISTORY
-- ============================================================
create table scores_history (
  id                      uuid primary key default uuid_generate_v4(),
  product_id              uuid not null references products(id) on delete cascade,
  scored_date             date not null default current_date,
  composite_score         float not null,
  early_detection_score   float,
  demand_validation_score float,
  purchase_intent_score   float,
  supply_readiness_score  float,
  verdict                 text not null check (verdict in ('buy','watch','pass')),
  verdict_reasoning       text,
  score_change            float default 0,
  data_confidence         float not null default 1.0,
  platforms_used          jsonb default '[]'::jsonb
);

create index idx_scores_history_product on scores_history (product_id, scored_date desc);
create index idx_scores_history_date on scores_history (scored_date desc);

-- ============================================================
-- 8. COMPETITORS
-- ============================================================
create table competitors (
  id              uuid primary key default uuid_generate_v4(),
  competitor_name text not null,
  product_name    text not null,
  url             text,
  category        text,
  in_stock        boolean not null default true,
  current_price   numeric(10,2),
  price_history   jsonb default '[]'::jsonb,
  review_count    int default 0,
  review_score    float,
  is_new_sku      boolean default false,
  first_seen      date not null default current_date,
  last_checked    date not null default current_date
);

create index idx_competitors_category on competitors (category);
create index idx_competitors_in_stock on competitors (in_stock);

-- ============================================================
-- 9. ALERTS
-- ============================================================
create table alerts (
  id           uuid primary key default uuid_generate_v4(),
  product_id   uuid references products(id) on delete set null,
  alert_type   text not null check (alert_type in (
    'green_flag','competitor_oos','new_sku','score_acceleration','fad_warning','reddit_pushback'
  )),
  priority     text not null default 'medium' check (priority in ('high','medium','low')),
  message      text not null,
  triggered_at timestamptz not null default now(),
  actioned     boolean not null default false,
  actioned_by  text,
  outcome_note text
);

create index idx_alerts_priority on alerts (priority, triggered_at desc);
create index idx_alerts_actioned on alerts (actioned);
create index idx_alerts_product on alerts (product_id);

-- ============================================================
-- 10. SOURCING LOG
-- ============================================================
create table sourcing_log (
  id               uuid primary key default uuid_generate_v4(),
  product_id       uuid not null references products(id) on delete cascade,
  decision_date    date not null default current_date,
  decided_by       text not null,
  score_at_decision float,
  supplier         text,
  moq              int,
  unit_cost        numeric(10,2),
  import_method    text check (import_method in ('air_freight','sea_freight','domestic')),
  estimated_arrival date,
  actual_sell_price numeric(10,2),
  units_sold       int,
  outcome          text check (outcome in ('success','partial','dead_stock'))
);

create index idx_sourcing_log_product on sourcing_log (product_id);
create index idx_sourcing_log_outcome on sourcing_log (outcome);

-- ============================================================
-- 11. ROW LEVEL SECURITY — open read/write for anon key (dev)
-- In production, lock these down to authenticated users.
-- ============================================================

alter table products enable row level security;
alter table signals_social enable row level security;
alter table signals_search enable row level security;
alter table signals_retail enable row level security;
alter table signals_supply enable row level security;
alter table signals_discovery enable row level security;
alter table scores_history enable row level security;
alter table competitors enable row level security;
alter table alerts enable row level security;
alter table sourcing_log enable row level security;

-- Allow anon read/write on all tables (development policy)
do $$
declare
  t text;
begin
  for t in select unnest(array[
    'products','signals_social','signals_search','signals_retail',
    'signals_supply','signals_discovery','scores_history',
    'competitors','alerts','sourcing_log'
  ]) loop
    execute format('create policy "Allow anon select on %I" on %I for select using (true)', t, t);
    execute format('create policy "Allow anon insert on %I" on %I for insert with check (true)', t, t);
    execute format('create policy "Allow anon update on %I" on %I for update using (true) with check (true)', t, t);
    execute format('create policy "Allow anon delete on %I" on %I for delete using (true)', t, t);
  end loop;
end $$;

-- ============================================================
-- 12. ENABLE REALTIME on key tables
-- ============================================================
alter publication supabase_realtime add table products;
alter publication supabase_realtime add table scores_history;
alter publication supabase_realtime add table competitors;
alter publication supabase_realtime add table alerts;
