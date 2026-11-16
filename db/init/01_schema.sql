-- 01_schema.sql  表结构
-- 保单表：id、地区
CREATE TABLE IF NOT EXISTS policies (
    id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region VARCHAR(64) NOT NULL
);

-- 理赔表：id、保单id、整数金额（单位：分，避免浮点误差）
CREATE TABLE IF NOT EXISTS claims (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_id BIGINT NOT NULL REFERENCES policies(id),
    amount    BIGINT NOT NULL CHECK (amount >= 0)
);

-- 按地区分页查询的常用访问路径
CREATE INDEX IF NOT EXISTS idx_claims_policy
    ON claims (policy_id);
CREATE INDEX IF NOT EXISTS idx_policies_region
    ON policies (region);
