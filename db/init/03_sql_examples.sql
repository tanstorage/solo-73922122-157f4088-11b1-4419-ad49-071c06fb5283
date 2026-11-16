-- 03_sql_examples.sql  SQL 补录示例（源端写操作，独立事务自动提交，
-- 与任何快照会话互不影响；会话中的 REPEATABLE READ 只读事务看不到这些新提交）
-- 也可通过 API：POST /admin/policies、POST /admin/claims 等完成同样操作。

-- 1) 补录保单
INSERT INTO policies (region)
VALUES ('西南')
RETURNING id, region;

-- 2) 补录理赔（金额必须为非负整数，单位分）
INSERT INTO claims (policy_id, amount)
VALUES (1, 128000)
RETURNING id, policy_id, amount;

-- 3) 修改理赔金额
UPDATE claims
SET    amount = 99900
WHERE  id = 1
RETURNING id, policy_id, amount;

-- 4) 删除理赔
DELETE FROM claims
WHERE  id = 2
RETURNING id;

-- 5) 按地区核对明细总量（源端最新已提交数据）
SELECT p.region,
       COUNT(*)        AS claim_count,
       SUM(c.amount)   AS total_amount
FROM   claims c
JOIN   policies p ON p.id = c.policy_id
GROUP  BY p.region
ORDER  BY p.region;
