-- ============================================================
-- SEED DATA — 10 products, 3 categories, 30 days of history
-- ============================================================

-- ============================================================
-- PRODUCTS
-- ============================================================
insert into products (id, name, category, keywords, first_seen_date, current_verdict, current_score, lifecycle_phase, fad_flag, active) values
  ('a0000000-0000-0000-0000-000000000001', 'Sea Moss Gel',          'Supplements',  '["sea moss","irish moss","superfood","mineral supplement"]',   '2026-02-28', 'buy',   82.4, 'buy_window', false, true),
  ('a0000000-0000-0000-0000-000000000002', 'Shilajit Resin',        'Supplements',  '["shilajit","fulvic acid","ayurvedic","mineral resin"]',       '2026-03-01', 'buy',   78.1, 'early',      false, true),
  ('a0000000-0000-0000-0000-000000000003', 'Chlorophyll Drops',     'Supplements',  '["chlorophyll","liquid chlorophyll","detox","green supplement"]','2026-02-20', 'watch', 64.2, 'peak',       false, true),
  ('a0000000-0000-0000-0000-000000000004', 'Mushroom Coffee',       'Supplements',  '["mushroom coffee","lions mane","adaptogen","nootropic"]',     '2026-03-05', 'watch', 59.8, 'early',      false, true),
  ('a0000000-0000-0000-0000-000000000005', 'Gua Sha Stone Set',     'Beauty Tools', '["gua sha","facial tool","lymphatic","jade"]',                 '2026-02-15', 'buy',   76.5, 'buy_window', false, true),
  ('a0000000-0000-0000-0000-000000000006', 'Ice Roller',            'Beauty Tools', '["ice roller","facial roller","depuff","skincare tool"]',       '2026-03-10', 'watch', 61.3, 'early',      false, true),
  ('a0000000-0000-0000-0000-000000000007', 'Red Light Therapy Mask','Beauty Tools', '["red light","LED mask","anti aging","phototherapy"]',          '2026-02-25', 'watch', 68.9, 'buy_window', false, true),
  ('a0000000-0000-0000-0000-000000000008', 'Acupressure Mat',       'Fitness',      '["acupressure","shakti mat","pain relief","recovery"]',        '2026-03-02', 'pass',  48.2, 'declining',  true,  true),
  ('a0000000-0000-0000-0000-000000000009', 'Resistance Band Set',   'Fitness',      '["resistance bands","workout bands","home gym","elastic"]',    '2026-02-18', 'buy',   80.6, 'peak',       false, true),
  ('a0000000-0000-0000-0000-000000000010', 'Massage Gun Mini',      'Fitness',      '["massage gun","percussion","muscle recovery","portable"]',    '2026-03-08', 'watch', 57.4, 'early',      false, true);

-- ============================================================
-- HELPER: Generate 30 days of data using generate_series
-- We use day offsets 0..29 → dates from 2026-03-01 to 2026-03-30
-- ============================================================

-- ============================================================
-- SCORES HISTORY (30 days × 10 products = 300 rows)
-- Uses a deterministic formula based on product base score + day variation
-- ============================================================
insert into scores_history (product_id, scored_date, composite_score, early_detection_score, demand_validation_score, purchase_intent_score, supply_readiness_score, verdict, verdict_reasoning, score_change, data_confidence, platforms_used)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  -- composite score: base ± sine wave variation
  greatest(0, least(100, p.current_score + 8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - (29 - d.day_offset) * 0.15)),
  -- early detection
  greatest(0, least(100, p.current_score * 0.95 + 10 * sin(d.day_offset * 0.25))),
  -- demand validation
  greatest(0, least(100, p.current_score * 0.90 + 6 * cos(d.day_offset * 0.35))),
  -- purchase intent
  greatest(0, least(100, p.current_score * 0.85 + 7 * sin(d.day_offset * 0.4 + 1))),
  -- supply readiness
  greatest(0, least(100, p.current_score * 0.80 + 5 * cos(d.day_offset * 0.2 + 2))),
  -- verdict
  case
    when greatest(0, least(100, p.current_score + 8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - (29 - d.day_offset) * 0.15)) >= 75 then 'buy'
    when greatest(0, least(100, p.current_score + 8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - (29 - d.day_offset) * 0.15)) >= 55 then 'watch'
    else 'pass'
  end,
  -- reasoning
  case
    when greatest(0, least(100, p.current_score + 8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - (29 - d.day_offset) * 0.15)) >= 75
      then 'Strong multi-platform demand signals. Social velocity high, retail ranks improving, supply chain ready. Recommend immediate sourcing.'
    when greatest(0, least(100, p.current_score + 8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - (29 - d.day_offset) * 0.15)) >= 55
      then 'Moderate interest detected across platforms. Demand growing but not yet confirmed by purchase intent signals. Continue monitoring.'
    else 'Weak or declining signals. Insufficient evidence of sustained demand. Social mentions may be noise or fad-driven.'
  end,
  -- score change (difference from "yesterday")
  case when d.day_offset = 0 then 0 else round((8 * sin(d.day_offset * 0.3 + ascii(left(p.name,1)) * 0.1) - 8 * sin((d.day_offset-1) * 0.3 + ascii(left(p.name,1)) * 0.1))::numeric, 1) end,
  -- data confidence
  0.6 + 0.35 * random(),
  -- platforms used
  '["tiktok","instagram","google_trends","reddit","amazon","alibaba","pinterest"]'::jsonb
from products p
cross join generate_series(0, 29) as d(day_offset);

-- ============================================================
-- SIGNALS — SOCIAL (TikTok + Instagram for all products, 30 days)
-- ============================================================
insert into signals_social (product_id, scraped_date, platform, data_confidence, mention_count, growth_rate_wow, sentiment_score, velocity_score, creator_tier_score, buy_intent_comment_count, problem_language_comment_count)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  plat.name,
  0.7 + 0.25 * random(),
  (200 + p.current_score * 10 + d.day_offset * 5 + random() * 50)::int,
  0.05 + 0.15 * sin(d.day_offset * 0.3) + random() * 0.05,
  0.3 + 0.5 * (p.current_score / 100.0) + 0.1 * sin(d.day_offset * 0.2),
  0.2 + 0.6 * (p.current_score / 100.0) + 0.1 * cos(d.day_offset * 0.25),
  case plat.name when 'tiktok' then 0.4 + 0.4 * random() else 0.3 + 0.3 * random() end,
  (10 + p.current_score * 0.5 + random() * 10)::int,
  (2 + random() * 5)::int
from products p
cross join generate_series(0, 29) as d(day_offset)
cross join (values ('tiktok'), ('instagram')) as plat(name);

-- Reddit signals (for demand validation)
insert into signals_social (product_id, scraped_date, platform, data_confidence, mention_count, growth_rate_wow, sentiment_score, velocity_score, creator_tier_score, buy_intent_comment_count, problem_language_comment_count)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  'reddit',
  0.6 + 0.3 * random(),
  (50 + p.current_score * 3 + d.day_offset * 2 + random() * 20)::int,
  0.03 + 0.10 * sin(d.day_offset * 0.3),
  case when p.name = 'Acupressure Mat' then -0.1 + 0.15 * sin(d.day_offset * 0.2) else 0.4 + 0.4 * (p.current_score / 100.0) end,
  0.15 + 0.4 * (p.current_score / 100.0),
  0,
  (5 + random() * 8)::int,
  case when p.name = 'Acupressure Mat' then (8 + random() * 5)::int else (1 + random() * 3)::int end
from products p
cross join generate_series(0, 29) as d(day_offset);

-- ============================================================
-- SIGNALS — SEARCH (Google Trends, 30 days)
-- ============================================================
insert into signals_search (product_id, scraped_date, platform, data_confidence, slope_24m, breakout_flag, yoy_growth, seasonal_pattern, related_rising_queries, news_trigger_flag)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  'google_trends',
  0.8 + 0.15 * random(),
  case when p.name = 'Acupressure Mat' then -0.02 else 0.03 + 0.05 * (p.current_score / 100.0) end,
  p.fad_flag,
  0.1 + 0.5 * (p.current_score / 100.0),
  case when p.category = 'Fitness' then 'new_year_spike' when p.category = 'Beauty Tools' then 'holiday_gift' else 'steady' end,
  case p.category
    when 'Supplements' then '["best supplements 2026","natural health","wellness trends"]'::jsonb
    when 'Beauty Tools' then '["skincare routine","anti aging tools","beauty gadgets"]'::jsonb
    else '["home workout","recovery tools","fitness gear"]'::jsonb
  end,
  (random() < 0.1)
from products p
cross join generate_series(0, 29) as d(day_offset);

-- ============================================================
-- SIGNALS — RETAIL (Amazon + Etsy, 30 days)
-- ============================================================
insert into signals_retail (product_id, scraped_date, platform, data_confidence, bestseller_rank, rank_change_wow, review_count, review_count_growth, review_sentiment, search_rank, out_of_stock_flag, price, price_history, handmade_vs_mass_market_ratio)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  plat.name,
  0.7 + 0.2 * random(),
  greatest(1, (500 - p.current_score * 5 + d.day_offset * 2 + random() * 30)::int),
  (-10 + (p.current_score / 5.0) + random() * 5)::int,
  (100 + p.current_score * 20 + d.day_offset * 3)::int,
  0.02 + 0.05 * (p.current_score / 100.0),
  0.5 + 0.4 * (p.current_score / 100.0),
  greatest(1, (100 - p.current_score + random() * 20)::int),
  false,
  case plat.name when 'amazon' then 15.00 + random() * 30 else 20.00 + random() * 25 end,
  '[]'::jsonb,
  case plat.name when 'etsy' then 0.3 + 0.4 * random() else null end
from products p
cross join generate_series(0, 29) as d(day_offset)
cross join (values ('amazon'), ('etsy')) as plat(name);

-- ============================================================
-- SIGNALS — SUPPLY (Alibaba, 30 days)
-- ============================================================
insert into signals_supply (product_id, scraped_date, platform, data_confidence, supplier_listing_count, supplier_count_change, moq_current, moq_trend, price_per_unit, competing_supplier_count, new_category_flag)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  'alibaba',
  0.75 + 0.2 * random(),
  (20 + p.current_score * 0.5 + d.day_offset * 0.3)::int,
  (random() * 3)::int,
  case when p.category = 'Supplements' then 500 when p.category = 'Beauty Tools' then 200 else 300 end,
  case when d.day_offset > 20 then 'decreasing' when d.day_offset > 10 then 'stable' else 'increasing' end,
  case when p.category = 'Supplements' then 2.50 + random() * 3 when p.category = 'Beauty Tools' then 3.00 + random() * 5 else 5.00 + random() * 8 end,
  (5 + p.current_score * 0.2)::int,
  (d.day_offset < 3)
from products p
cross join generate_series(0, 29) as d(day_offset);

-- ============================================================
-- SIGNALS — DISCOVERY (Pinterest, 30 days)
-- ============================================================
insert into signals_discovery (product_id, scraped_date, platform, data_confidence, pin_save_rate, save_rate_growth, board_creation_count, keyword_search_volume, trending_category_flag, demographic_score)
select
  p.id,
  '2026-03-01'::date + d.day_offset,
  'pinterest',
  0.6 + 0.3 * random(),
  0.02 + 0.05 * (p.current_score / 100.0) + 0.01 * sin(d.day_offset * 0.3),
  0.05 + 0.10 * (p.current_score / 100.0),
  (10 + p.current_score * 0.3 + d.day_offset * 0.5)::int,
  (500 + p.current_score * 50 + d.day_offset * 10)::int,
  (p.current_score > 70),
  0.4 + 0.5 * (p.current_score / 100.0)
from products p
cross join generate_series(0, 29) as d(day_offset);

-- ============================================================
-- COMPETITORS
-- ============================================================
insert into competitors (competitor_name, product_name, category, in_stock, current_price, price_history, review_count, review_score, is_new_sku, first_seen, last_checked) values
  ('NaturePlus',    'Organic Sea Moss Capsules',  'Supplements',  true,   24.99, '[{"date":"2026-03-01","price":24.99},{"date":"2026-03-15","price":22.99}]', 1250, 4.3, false, '2026-01-15', '2026-03-30'),
  ('VitaRoots',     'Premium Shilajit Extract',   'Supplements',  false,  34.99, '[{"date":"2026-03-01","price":29.99},{"date":"2026-03-20","price":34.99}]', 890,  4.1, false, '2026-02-01', '2026-03-30'),
  ('GreenLife Co',  'Liquid Chlorophyll Mint',     'Supplements',  true,   18.50, '[{"date":"2026-03-01","price":18.50}]',                                    3200, 4.5, false, '2025-11-01', '2026-03-30'),
  ('BeautyStone',   'Rose Quartz Gua Sha',        'Beauty Tools', true,   12.99, '[{"date":"2026-03-01","price":14.99},{"date":"2026-03-15","price":12.99}]', 5600, 4.6, false, '2025-09-01', '2026-03-30'),
  ('CoolFace',      'Stainless Steel Ice Roller',  'Beauty Tools', true,   9.99,  '[{"date":"2026-03-01","price":9.99}]',                                     2100, 4.2, true,  '2026-03-10', '2026-03-30'),
  ('GlowTech',      'LED Therapy Panel',           'Beauty Tools', false,  89.99, '[{"date":"2026-03-01","price":99.99},{"date":"2026-03-15","price":89.99}]', 780,  3.9, false, '2026-01-20', '2026-03-30'),
  ('FlexFit',       'Acupressure Pillow Mat',      'Fitness',      true,   29.99, '[{"date":"2026-03-01","price":29.99}]',                                    4500, 4.4, false, '2025-06-01', '2026-03-30'),
  ('BandPro',       'Heavy Resistance Band Kit',   'Fitness',      true,   19.99, '[{"date":"2026-03-01","price":21.99},{"date":"2026-03-10","price":19.99}]', 8900, 4.7, false, '2025-03-01', '2026-03-30'),
  ('RecoverMax',    'Pocket Massage Gun',          'Fitness',      true,   49.99, '[{"date":"2026-03-01","price":49.99}]',                                    1600, 4.0, true,  '2026-03-05', '2026-03-30'),
  ('PureShroom',    'Chaga Mushroom Blend',        'Supplements',  false,  27.50, '[{"date":"2026-03-01","price":25.00},{"date":"2026-03-20","price":27.50}]', 650,  4.2, true,  '2026-03-15', '2026-03-30');

-- ============================================================
-- ALERTS
-- ============================================================
insert into alerts (product_id, alert_type, priority, message, triggered_at, actioned) values
  ('a0000000-0000-0000-0000-000000000001', 'green_flag',         'high',   'Sea Moss Gel crossed 80-point threshold with strong multi-platform corroboration. Buy window is open.',                '2026-03-29 08:00:00+00', false),
  ('a0000000-0000-0000-0000-000000000002', 'score_acceleration', 'high',   'Shilajit Resin score jumped +12 points in 3 days. TikTok velocity surging with creator endorsements.',               '2026-03-28 14:30:00+00', false),
  ('a0000000-0000-0000-0000-000000000002', 'competitor_oos',     'high',   'VitaRoots Premium Shilajit Extract is out of stock on Amazon. Opportunity window for Shilajit Resin.',                '2026-03-27 10:00:00+00', true),
  ('a0000000-0000-0000-0000-000000000003', 'fad_warning',        'medium', 'Chlorophyll Drops showing signs of plateau. Google Trends slope flattening and social velocity declining.',           '2026-03-28 09:15:00+00', false),
  ('a0000000-0000-0000-0000-000000000005', 'green_flag',         'high',   'Gua Sha Stone Set demand validated across Amazon, Etsy, and Pinterest. Multiple competitors lowering price.',        '2026-03-26 11:00:00+00', true),
  ('a0000000-0000-0000-0000-000000000007', 'competitor_oos',     'medium', 'GlowTech LED Therapy Panel out of stock. Red Light Therapy Mask could capture displaced demand.',                    '2026-03-29 07:00:00+00', false),
  ('a0000000-0000-0000-0000-000000000008', 'reddit_pushback',    'high',   'Acupressure Mat receiving negative Reddit sentiment (-0.2). Multiple threads questioning effectiveness.',            '2026-03-25 16:00:00+00', true),
  (null,                                    'new_sku',           'low',    'CoolFace launched Stainless Steel Ice Roller — new SKU in Beauty Tools category. Monitor for market impact.',         '2026-03-10 09:00:00+00', true),
  ('a0000000-0000-0000-0000-000000000009', 'green_flag',         'high',   'Resistance Band Set holding above 80 for 7 consecutive days. Sustained demand confirmed across all scoring jobs.',    '2026-03-29 06:00:00+00', false),
  ('a0000000-0000-0000-0000-000000000010', 'score_acceleration', 'medium', 'Massage Gun Mini trending upward — score increased from 51 to 57 in one week. Early detection signals strengthening.','2026-03-28 12:00:00+00', false),
  (null,                                    'new_sku',           'low',    'PureShroom entered Supplements category with Chaga Mushroom Blend. Already out of stock — high demand signal.',       '2026-03-15 10:00:00+00', false),
  ('a0000000-0000-0000-0000-000000000004', 'fad_warning',        'low',    'Mushroom Coffee social mentions plateauing. Watch for further decline before committing to sourcing.',                '2026-03-27 15:00:00+00', false);

-- ============================================================
-- SOURCING LOG
-- ============================================================
insert into sourcing_log (product_id, decision_date, decided_by, score_at_decision, supplier, moq, unit_cost, import_method, estimated_arrival, actual_sell_price, units_sold, outcome) values
  ('a0000000-0000-0000-0000-000000000001', '2026-03-15', 'Victor', 79.5, 'Fujian SeaHarvest Co.',  500,  3.20, 'sea_freight', '2026-04-20', 14.99, 480,  'success'),
  ('a0000000-0000-0000-0000-000000000005', '2026-03-10', 'Victor', 74.2, 'Xiamen Stone Arts Ltd.', 200,  2.80, 'air_freight', '2026-03-25', 12.99, 195,  'success'),
  ('a0000000-0000-0000-0000-000000000009', '2026-03-12', 'Victor', 77.8, 'Dongguan FitGear Inc.',  300,  1.50, 'sea_freight', '2026-04-15', 9.99,  220,  'partial'),
  ('a0000000-0000-0000-0000-000000000003', '2026-03-05', 'Victor', 71.0, 'Hangzhou GreenBio Ltd.', 500,  1.80, 'sea_freight', '2026-04-10', 8.99,  150,  'partial'),
  ('a0000000-0000-0000-0000-000000000008', '2026-03-01', 'Victor', 55.3, 'Ningbo Wellness Corp.',  300,  4.50, 'air_freight', '2026-03-15', 19.99, 45,   'dead_stock');
