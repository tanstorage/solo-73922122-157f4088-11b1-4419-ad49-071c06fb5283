-- 02_seed.sql  演示数据：3 个地区，共 260 条理赔
-- （华北 120 条 -> 至少两页；华东 100 条；华南 40 条）
TRUNCATE claims, policies RESTART IDENTITY CASCADE;

INSERT INTO policies (id, region) OVERRIDING SYSTEM VALUE VALUES
    (1, '华北'),
    (2, '华北'),
    (3, '华东'),
    (4, '华东'),
    (5, '华南');

-- 用 generate_series 批量造数，金额为整数（分）
INSERT INTO claims (policy_id, amount)
SELECT 1, (g * 7 % 5000) * 100 + 100
FROM   generate_series(1, 80) AS g;

INSERT INTO claims (policy_id, amount)
SELECT 2, (g * 13 % 5000) * 100 + 200
FROM   generate_series(1, 40) AS g;

INSERT INTO claims (policy_id, amount)
SELECT 3, (g * 17 % 5000) * 100 + 300
FROM   generate_series(1, 60) AS g;

INSERT INTO claims (policy_id, amount)
SELECT 4, (g * 19 % 5000) * 100 + 400
FROM   generate_series(1, 40) AS g;

INSERT INTO claims (policy_id, amount)
SELECT 5, (g * 23 % 5000) * 100 + 500
FROM   generate_series(1, 40) AS g;

SELECT setval(
    pg_get_serial_sequence('claims', 'id'),
    (SELECT COALESCE(MAX(id), 1) FROM claims)
);
SELECT setval(
    pg_get_serial_sequence('policies', 'id'),
    (SELECT COALESCE(MAX(id), 1) FROM policies)
);
