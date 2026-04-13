-- ============================================================
-- Product Intelligence System — V2 Schema (5 new tables)
-- Run AFTER schema.sql and seed.sql
-- ============================================================

-- ============================================================
-- 1. SCHEDULES
-- ============================================================
create table schedules (
  id           uuid primary key default uuid_generate_v4(),
  platform     text not null unique,
  frequency    text not null default 'daily' check (frequency in ('daily','weekly')),
  run_time     time not null default '02:00',
  day_of_week  int check (day_of_week between 0 and 6),
  enabled      boolean not null default true,
  last_run     timestamptz,
  next_run     timestamptz,
  created_at   timestamptz not null default now()
);

create index idx_schedules_enabled on schedules (enabled);
create index idx_schedules_next_run on schedules (next_run);

-- Seed default schedules for all 12 platforms
insert into schedules (platform, frequency, run_time, enabled) values
  ('reddit',        'daily', '02:00', true),
  ('tiktok',        'daily', '02:10', true),
  ('instagram',     'daily', '02:20', true),
  ('x',             'daily', '02:30', true),
  ('facebook',      'daily', '02:40', true),
  ('youtube',       'daily', '02:50', true),
  ('google_trends', 'daily', '03:00', true),
  ('amazon',        'daily', '03:10', true),
  ('walmart',       'daily', '03:20', true),
  ('etsy',          'daily', '03:30', true),
  ('alibaba',       'daily', '03:40', true),
  ('pinterest',     'daily', '03:50', true);

-- ============================================================
-- 2. AGENT RUNS
-- ============================================================
create table agent_runs (
  id                 uuid primary key default uuid_generate_v4(),
  run_id             uuid not null,
  platform           text not null,
  status             text not null default 'pending' check (status in ('pending','running','complete','failed','degraded')),
  products_processed int default 0,
  rows_written       int default 0,
  rows_rejected      int default 0,
  started_at         timestamptz,
  completed_at       timestamptz,
  error_message      text,
  duration_seconds   float,
  agent_run_summary  text,
  anomalies_detected int default 0,
  created_at         timestamptz not null default now()
);

create index idx_agent_runs_run_id on agent_runs (run_id);
create index idx_agent_runs_platform on agent_runs (platform);
create index idx_agent_runs_status on agent_runs (status);
create index idx_agent_runs_created on agent_runs (created_at desc);

-- ============================================================
-- 3. AGENT WEIGHTS (learned signal weights)
-- ============================================================
create table agent_weights (
  id               uuid primary key default uuid_generate_v4(),
  agent            text not null,
  signal_name      text not null,
  base_weight      float not null default 1.0,
  learned_weight   float not null default 1.0,
  adjustment_count int not null default 0,
  last_updated     timestamptz not null default now(),
  unique (agent, signal_name)
);

create index idx_agent_weights_agent on agent_weights (agent);

-- Seed base weights for all agents
insert into agent_weights (agent, signal_name, base_weight, learned_weight) values
  -- Social agents
  ('reddit',    'mention_count',    1.0, 1.0), ('reddit',    'sentiment_score',  1.0, 1.0),
  ('reddit',    'velocity_score',   1.0, 1.0), ('reddit',    'buy_intent_score', 1.0, 1.0),
  ('tiktok',    'mention_count',    1.0, 1.0), ('tiktok',    'sentiment_score',  1.0, 1.0),
  ('tiktok',    'velocity_score',   1.0, 1.0), ('tiktok',    'buy_intent_score', 1.0, 1.0),
  ('instagram', 'mention_count',    1.0, 1.0), ('instagram', 'sentiment_score',  1.0, 1.0),
  ('instagram', 'velocity_score',   1.0, 1.0), ('instagram', 'buy_intent_score', 1.0, 1.0),
  ('x',         'mention_count',    1.0, 1.0), ('x',         'sentiment_score',  1.0, 1.0),
  ('x',         'velocity_score',   1.0, 1.0), ('x',         'buy_intent_score', 1.0, 1.0),
  ('facebook',  'mention_count',    1.0, 1.0), ('facebook',  'sentiment_score',  1.0, 1.0),
  ('facebook',  'velocity_score',   1.0, 1.0), ('facebook',  'buy_intent_score', 1.0, 1.0),
  ('youtube',   'mention_count',    1.0, 1.0), ('youtube',   'sentiment_score',  1.0, 1.0),
  ('youtube',   'velocity_score',   1.0, 1.0), ('youtube',   'buy_intent_score', 1.0, 1.0),
  -- Search
  ('google_trends', 'slope_24m',    1.0, 1.0), ('google_trends', 'yoy_growth', 1.0, 1.0),
  ('google_trends', 'breakout_flag',1.0, 1.0),
  -- Retail
  ('amazon',  'bestseller_rank',  1.0, 1.0), ('amazon',  'review_sentiment', 1.0, 1.0),
  ('amazon',  'review_count',     1.0, 1.0), ('amazon',  'buy_intent_score', 1.0, 1.0),
  ('walmart', 'bestseller_rank',  1.0, 1.0), ('walmart', 'review_sentiment', 1.0, 1.0),
  ('etsy',    'review_count',     1.0, 1.0), ('etsy',    'review_sentiment', 1.0, 1.0),
  ('etsy',    'handmade_ratio',   1.0, 1.0),
  -- Supply
  ('alibaba', 'supplier_count',   1.0, 1.0), ('alibaba', 'price_per_unit',  1.0, 1.0),
  ('alibaba', 'moq_trend',        1.0, 1.0),
  -- Discovery
  ('pinterest', 'pin_save_rate',  1.0, 1.0), ('pinterest', 'save_rate_growth', 1.0, 1.0),
  ('pinterest', 'keyword_volume', 1.0, 1.0);

-- ============================================================
-- 4. CROSS REFERENCE RUNS
-- ============================================================
create table cross_reference_runs (
  id                   uuid primary key default uuid_generate_v4(),
  run_id               uuid not null,
  product_id           uuid not null references products(id) on delete cascade,
  platforms_positive   jsonb default '[]'::jsonb,
  platforms_negative   jsonb default '[]'::jsonb,
  platforms_neutral    jsonb default '[]'::jsonb,
  cross_platform_score float default 0,
  consensus_flag       boolean default false,
  divergence_flag      boolean default false,
  analysis_summary     text,
  created_at           timestamptz not null default now()
);

create index idx_cross_ref_run_id on cross_reference_runs (run_id);
create index idx_cross_ref_product on cross_reference_runs (product_id);
create index idx_cross_ref_consensus on cross_reference_runs (consensus_flag);

-- ============================================================
-- 5. SIGNALS LOW QUALITY (rejected signals)
-- ============================================================
create table signals_low_quality (
  id                 uuid primary key default uuid_generate_v4(),
  product_id         uuid not null references products(id) on delete cascade,
  platform           text not null,
  scraped_date       date not null default current_date,
  data_quality_score float not null,
  rejection_reason   text not null,
  raw_json           jsonb,
  created_at         timestamptz not null default now()
);

create index idx_low_quality_product on signals_low_quality (product_id);
create index idx_low_quality_platform on signals_low_quality (platform);
create index idx_low_quality_date on signals_low_quality (scraped_date desc);

-- ============================================================
-- Add new columns to existing signals tables for agent skills
-- ============================================================

-- Velocity & phase columns for all signal tables
do $$
declare
  t text;
begin
  for t in select unnest(array[
    'signals_social','signals_search','signals_retail',
    'signals_supply','signals_discovery'
  ]) loop
    execute format('alter table %I add column if not exists velocity float', t);
    execute format('alter table %I add column if not exists acceleration float', t);
    execute format('alter table %I add column if not exists projected_peak_days int', t);
    execute format('alter table %I add column if not exists phase text', t);
    execute format('alter table %I add column if not exists fad_score float', t);
    execute format('alter table %I add column if not exists lasting_score float', t);
    execute format('alter table %I add column if not exists industry_shift_score float', t);
    execute format('alter table %I add column if not exists avg_intent_score float', t);
    execute format('alter table %I add column if not exists intent_level_distribution jsonb', t);
    execute format('alter table %I add column if not exists high_intent_comment_count int default 0', t);
    execute format('alter table %I add column if not exists relative_strength float default 1.0', t);
    execute format('alter table %I add column if not exists above_category_average boolean default false', t);
    execute format('alter table %I add column if not exists anomaly_flag boolean default false', t);
    execute format('alter table %I add column if not exists anomaly_type text', t);
    execute format('alter table %I add column if not exists anomaly_description text', t);
    execute format('alter table %I add column if not exists agent_summary text', t);
    execute format('alter table %I add column if not exists run_id uuid', t);
    execute format('alter table %I add column if not exists sentiment_confidence float', t);
    execute format('alter table %I add column if not exists data_quality_score float default 1.0', t);
  end loop;
end $$;

-- Add cross_platform_summary to products table
alter table products add column if not exists cross_platform_summary text;
alter table products add column if not exists target_subreddits jsonb default '[]'::jsonb;

-- ============================================================
-- RLS policies for new tables (dev — open access)
-- ============================================================
alter table schedules enable row level security;
alter table agent_runs enable row level security;
alter table agent_weights enable row level security;
alter table cross_reference_runs enable row level security;
alter table signals_low_quality enable row level security;

do $$
declare
  t text;
begin
  for t in select unnest(array[
    'schedules','agent_runs','agent_weights',
    'cross_reference_runs','signals_low_quality'
  ]) loop
    execute format('create policy "Allow anon select on %I" on %I for select using (true)', t, t);
    execute format('create policy "Allow anon insert on %I" on %I for insert with check (true)', t, t);
    execute format('create policy "Allow anon update on %I" on %I for update using (true) with check (true)', t, t);
    execute format('create policy "Allow anon delete on %I" on %I for delete using (true)', t, t);
  end loop;
end $$;

-- ============================================================
-- Enable realtime on new tables
-- ============================================================
alter publication supabase_realtime add table agent_runs;
alter publication supabase_realtime add table schedules;
