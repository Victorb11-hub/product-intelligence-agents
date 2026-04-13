-- ============================================================
-- V5 — Apify usage tracking + schedule update
-- ============================================================

-- Add usage tracking columns to agent_runs
alter table agent_runs add column if not exists apify_results_count int default 0;
alter table agent_runs add column if not exists apify_estimated_cost float default 0;

-- Update Reddit schedule to 6am daily
update schedules set run_time = '06:00' where platform = 'reddit';

-- Create usage alerts table for monthly budget monitoring
create table if not exists usage_alerts (
  id              uuid primary key default uuid_generate_v4(),
  month           text not null,            -- '2026-03'
  platform        text not null,
  total_results   int default 0,
  total_cost      float default 0,
  budget_limit    float default 25.0,       -- $25/month default
  pct_used        float default 0,
  alert_triggered boolean default false,
  created_at      timestamptz not null default now()
);

create index if not exists idx_usage_alerts_month on usage_alerts (month, platform);

-- RLS for usage_alerts
alter table usage_alerts enable row level security;
create policy "Allow anon select on usage_alerts" on usage_alerts for select using (true);
create policy "Allow anon insert on usage_alerts" on usage_alerts for insert with check (true);
create policy "Allow anon update on usage_alerts" on usage_alerts for update using (true) with check (true);
