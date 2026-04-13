-- ============================================================
-- V7 — Learning infrastructure fixes
-- ============================================================

-- 1. Archived flag on posts (older than 90 days get archived, never deleted)
ALTER TABLE posts ADD COLUMN IF NOT EXISTS archived boolean DEFAULT false;
CREATE INDEX IF NOT EXISTS idx_posts_archived ON posts (archived);

-- 2. Smoothed velocity on signals_social
ALTER TABLE signals_social ADD COLUMN IF NOT EXISTS velocity_smoothed float;

-- 3. Product snapshots — clean daily time series for charts and backtesting
CREATE TABLE IF NOT EXISTS product_snapshots (
  id               uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  product_id       uuid NOT NULL REFERENCES products(id) ON DELETE CASCADE,
  snapshot_date    date NOT NULL DEFAULT current_date,
  composite_score  float,
  verdict          text,
  lifecycle_phase  text,
  reddit_mentions  int,
  reddit_sentiment float,
  reddit_intent    float,
  gt_slope         float,
  gt_yoy_growth    float,
  platforms_active int,
  data_confidence  float,
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (product_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_product ON product_snapshots (product_id, snapshot_date DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_date ON product_snapshots (snapshot_date DESC);

-- RLS
ALTER TABLE product_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow anon select on product_snapshots" ON product_snapshots FOR SELECT USING (true);
CREATE POLICY "Allow anon insert on product_snapshots" ON product_snapshots FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow anon update on product_snapshots" ON product_snapshots FOR UPDATE USING (true) WITH CHECK (true);

-- Realtime for dashboard score chart
ALTER PUBLICATION supabase_realtime ADD TABLE product_snapshots;
