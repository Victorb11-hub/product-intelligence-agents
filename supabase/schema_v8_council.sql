-- ============================================================
-- V8 — Research Council, Email Settings, Formula Recommendations
-- Run all SQL in one block in Supabase SQL Editor
-- ============================================================

-- ============================================================
-- 1. COUNCIL VERDICTS
-- ============================================================
CREATE TABLE IF NOT EXISTS council_verdicts (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  product_id uuid REFERENCES products(id) ON DELETE CASCADE,
  run_id uuid,
  verdict_date date DEFAULT current_date,
  -- Agent 1: Trend Archaeologist
  trend_archaeologist_vote text,
  trend_archaeologist_confidence float,
  trend_archaeologist_reasoning text,
  trend_archaeologist_round2_vote text,
  trend_archaeologist_round2_reasoning text,
  -- Agent 2: Demand Validator
  demand_validator_vote text,
  demand_validator_confidence float,
  demand_validator_reasoning text,
  demand_validator_round2_vote text,
  demand_validator_round2_reasoning text,
  -- Agent 3: Supply Chain Analyst
  supply_analyst_vote text,
  supply_analyst_confidence float,
  supply_analyst_reasoning text,
  supply_analyst_round2_vote text,
  supply_analyst_round2_reasoning text,
  -- Agent 4: Fad Detector
  fad_detector_vote text,
  fad_detector_confidence float,
  fad_detector_reasoning text,
  fad_detector_round2_vote text,
  fad_detector_round2_reasoning text,
  -- Agent 5: Category Strategist
  category_strategist_vote text,
  category_strategist_confidence float,
  category_strategist_reasoning text,
  category_strategist_round2_vote text,
  category_strategist_round2_reasoning text,
  -- Council result
  council_verdict text,
  council_confidence float,
  votes_for_buy int DEFAULT 0,
  votes_for_watch int DEFAULT 0,
  votes_for_pass int DEFAULT 0,
  -- Victor override
  victor_vote text,
  victor_reasoning text,
  victor_weight float DEFAULT 1.5,
  final_verdict text,
  -- Dissent tracking
  dissent_from_composite boolean DEFAULT false,
  dissent_reasoning text,
  -- Outcome tracking
  outcome_correct boolean,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_council_product ON council_verdicts (product_id, verdict_date DESC);
CREATE INDEX IF NOT EXISTS idx_council_run ON council_verdicts (run_id);
CREATE INDEX IF NOT EXISTS idx_council_date ON council_verdicts (verdict_date DESC);

-- ============================================================
-- 2. COUNCIL WEIGHTS
-- ============================================================
CREATE TABLE IF NOT EXISTS council_weights (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  agent_name text UNIQUE NOT NULL,
  current_weight float DEFAULT 1.0,
  base_weight float DEFAULT 1.0,
  total_decisions int DEFAULT 0,
  correct_decisions int DEFAULT 0,
  accuracy_rate float DEFAULT 0,
  last_adjusted timestamptz,
  adjustment_history jsonb DEFAULT '[]'::jsonb
);

INSERT INTO council_weights (agent_name, current_weight, base_weight) VALUES
  ('trend_archaeologist', 1.0, 1.0),
  ('demand_validator',    1.0, 1.0),
  ('supply_analyst',      1.0, 1.0),
  ('fad_detector',        0.8, 0.8),
  ('category_strategist', 1.0, 1.0)
ON CONFLICT (agent_name) DO NOTHING;

-- ============================================================
-- 3. FORMULA RECOMMENDATIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS formula_recommendations (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  product_id uuid REFERENCES products(id) ON DELETE CASCADE,
  run_id uuid,
  agent_name text NOT NULL,
  recommendation_type text NOT NULL,
  current_value text,
  recommended_value text,
  reasoning text,
  confidence float,
  status text DEFAULT 'pending',
  victor_decision text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_formula_rec_status ON formula_recommendations (status);
CREATE INDEX IF NOT EXISTS idx_formula_rec_agent ON formula_recommendations (agent_name);

-- ============================================================
-- 4. EMAIL SETTINGS
-- ============================================================
CREATE TABLE IF NOT EXISTS email_settings (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  email_address text UNIQUE NOT NULL,
  name text,
  active boolean DEFAULT true,
  receive_daily boolean DEFAULT true,
  receive_monthly boolean DEFAULT true,
  receive_quarterly boolean DEFAULT true,
  receive_alerts boolean DEFAULT true,
  added_at timestamptz DEFAULT now()
);

-- ============================================================
-- 5. PIPELINE RUNS — tracks nightly pipeline phases
-- ============================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  run_date date DEFAULT current_date,
  phase text NOT NULL,
  status text DEFAULT 'scheduled',
  started_at timestamptz,
  completed_at timestamptz,
  duration_seconds float,
  details jsonb,
  error_message text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_date ON pipeline_runs (run_date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_phase ON pipeline_runs (phase);

-- ============================================================
-- 6. RLS — open access for dev
-- ============================================================
ALTER TABLE council_verdicts ENABLE ROW LEVEL SECURITY;
ALTER TABLE council_weights ENABLE ROW LEVEL SECURITY;
ALTER TABLE formula_recommendations ENABLE ROW LEVEL SECURITY;
ALTER TABLE email_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline_runs ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE t text;
BEGIN
  FOR t IN SELECT unnest(array[
    'council_verdicts','council_weights','formula_recommendations',
    'email_settings','pipeline_runs'
  ]) LOOP
    EXECUTE format('CREATE POLICY "Allow anon select on %I" ON %I FOR SELECT USING (true)', t, t);
    EXECUTE format('CREATE POLICY "Allow anon insert on %I" ON %I FOR INSERT WITH CHECK (true)', t, t);
    EXECUTE format('CREATE POLICY "Allow anon update on %I" ON %I FOR UPDATE USING (true) WITH CHECK (true)', t, t);
    EXECUTE format('CREATE POLICY "Allow anon delete on %I" ON %I FOR DELETE USING (true)', t, t);
  END LOOP;
END $$;

-- ============================================================
-- 7. REALTIME
-- ============================================================
ALTER PUBLICATION supabase_realtime ADD TABLE council_verdicts;
ALTER PUBLICATION supabase_realtime ADD TABLE pipeline_runs;
ALTER PUBLICATION supabase_realtime ADD TABLE formula_recommendations;
