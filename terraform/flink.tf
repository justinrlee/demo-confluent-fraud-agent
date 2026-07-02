# ---------------------------------------------------------------------------
# Bedrock connection (IAM user creds, fixed region). The Claude model id is
# encoded in the endpoint URL (see locals in main.tf).
# ---------------------------------------------------------------------------
resource "confluent_flink_connection" "bedrock" {
  organization {
    id = data.confluent_organization.main.id
  }
  environment {
    id = confluent_environment.main.id
  }
  compute_pool {
    id = confluent_flink_compute_pool.main.id
  }
  principal {
    id = confluent_service_account.app_manager.id
  }
  rest_endpoint = data.confluent_flink_region.main.rest_endpoint
  credentials {
    key    = confluent_api_key.flink.id
    secret = confluent_api_key.flink.secret
  }

  display_name   = "bedrock-fraud-connection"
  type           = "BEDROCK"
  endpoint       = local.bedrock_endpoint
  aws_access_key = var.aws_access_key_id
  aws_secret_key = var.aws_secret_access_key

  depends_on = [
    confluent_api_key.flink,
    confluent_role_binding.app_manager_env_admin,
  ]
}

# ---------------------------------------------------------------------------
# Pre-built UDF tools JAR (committed to the repo) uploaded as a Flink artifact.
# Demo users need no Java/Maven toolchain — Terraform just uploads the binary.
# ---------------------------------------------------------------------------
resource "confluent_flink_artifact" "tools" {
  display_name   = "${local.prefix}-tools-${random_id.suffix.hex}"
  cloud          = local.cloud_provider
  region         = local.aws_region
  content_format = "JAR"
  artifact_file  = "${path.module}/../tools-udf/target/fraud-tools.jar"
  environment {
    id = confluent_environment.main.id
  }
}

# Shared args for every Flink statement (passed to the flink-statement module).
locals {
  flink_common = {
    organization_id  = data.confluent_organization.main.id
    environment_id   = confluent_environment.main.id
    compute_pool_id  = confluent_flink_compute_pool.main.id
    principal_id     = confluent_service_account.app_manager.id
    rest_endpoint    = data.confluent_flink_region.main.rest_endpoint
    flink_api_key    = confluent_api_key.flink.id
    flink_api_secret = confluent_api_key.flink.secret
    catalog          = local.catalog
    database         = local.database
  }
}

# ----------------------------- Source tables -------------------------------
# Created by Flink so the topics + Avro value schemas exist before the producer
# runs and before the detection statement is deployed. `timestamp` is epoch ms;
# event_time is a computed time attribute used for the session window.

module "tbl_transactions" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-transactions-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `transactions` (
      `user_id` STRING NOT NULL,
      `transaction_id` STRING NOT NULL,
      `amount` DOUBLE NOT NULL,
      `merchant` STRING NOT NULL,
      `merchant_category` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_user_logins" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-user-logins-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `user_logins` (
      `user_id` STRING NOT NULL,
      `ip_address` STRING NOT NULL,
      `device_id` STRING NOT NULL,
      `location` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

module "tbl_account_changes" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-account-changes-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `account_changes` (
      `user_id` STRING NOT NULL,
      `field_changed` STRING NOT NULL,
      `old_value` STRING NOT NULL,
      `new_value` STRING NOT NULL,
      `timestamp` BIGINT NOT NULL,
      `event_time` AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
      WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
    );
  EOT
}

# Sink table the detection statement writes alerts into; the dashboard consumes it.
module "tbl_fraud_alerts" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-fraud-alerts-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `fraud_alerts` (
      `user_id` STRING NOT NULL,
      `risk_score` INT NOT NULL,
      `reasoning` STRING NOT NULL,
      `actions_taken` STRING NOT NULL,
      `flagged_transaction_ids` STRING NOT NULL,
      `raw_response` STRING NOT NULL
    );
  EOT
}

# ----------------------- Anomaly Detection (ARIMA) ------------------------
# Detects anomalous spending patterns by windowing transactions into 15-second
# tumbling windows per user, aggregating spend metrics, and running ARIMA on
# the aggregates. This pattern matches the Confluent reference architecture.
module "tbl_anomalous_windows" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-anomalous-windows-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `anomalous_windows` AS
    WITH `windowed_spending` AS (
      SELECT
        `window_start`,
        `window_end`,
        `window_time`,
        `user_id`,
        COUNT(*) AS `txn_count`,
        SUM(CAST(`amount` AS DOUBLE)) AS `total_amount`,
        CAST(ROUND(AVG(CAST(`amount` AS DOUBLE)), 2) AS DOUBLE) AS `avg_amount`,
        MAX(CAST(`amount` AS DOUBLE)) AS `max_amount`
      FROM TABLE(
        TUMBLE(TABLE `transactions`, DESCRIPTOR(`event_time`), INTERVAL '15' SECOND)
      )
      GROUP BY `window_start`, `window_end`, `window_time`, `user_id`
    ),
    `anomaly_detection` AS (
      SELECT
        `user_id`,
        `window_start`,
        `window_end`,
        `window_time`,
        `txn_count`,
        `total_amount`,
        `avg_amount`,
        `max_amount`,
        ML_DETECT_ANOMALIES(
          `total_amount`,
          `window_time`,
          JSON_OBJECT(
            'minTrainingSize' VALUE 8,
            'maxTrainingSize' VALUE 100,
            'confidencePercentage' VALUE 95.0,
            'enableStl' VALUE FALSE
          )
        ) OVER (
          PARTITION BY `user_id`
          ORDER BY `window_time`
          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS `anomaly_result`
      FROM `windowed_spending`
    )
    SELECT
      `user_id`,
      `window_start`,
      `window_end`,
      `txn_count`,
      `total_amount`,
      `avg_amount`,
      `max_amount`,
      CAST(ROUND(`anomaly_result`.`forecast_value`, 2) AS DOUBLE) AS `expected_amount`,
      `anomaly_result`.`upper_bound`,
      `anomaly_result`.`lower_bound`,
      `anomaly_result`.`is_anomaly`
    FROM `anomaly_detection`
    WHERE `anomaly_result`.`is_anomaly` = TRUE
      AND `total_amount` > `anomaly_result`.`upper_bound`;
  EOT
  depends_on       = [module.tbl_transactions]
}

# Materialized view of transactions from anomalous windows (for dashboard/joins)
module "tbl_anomalous_transactions" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-table-anomalous-transactions-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TABLE `anomalous_transactions` AS
    SELECT
      t.`user_id`,
      t.`transaction_id`,
      t.`amount`,
      t.`merchant`,
      t.`merchant_category`,
      t.`location`,
      t.`timestamp`,
      t.`event_time`,
      w.`total_amount` AS `window_total_amount`,
      w.`expected_amount`,
      w.`upper_bound`,
      w.`lower_bound`
    FROM `transactions` t
    INNER JOIN `anomalous_windows` w
      ON t.`user_id` = w.`user_id`
      AND t.`event_time` >= w.`window_start`
      AND t.`event_time` < w.`window_end`;
  EOT
  depends_on       = [module.tbl_transactions, module.tbl_anomalous_windows]
}

# ------------------------------- Model -------------------------------------
module "model" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-model-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE MODEL `fraud_model`
    INPUT (`prompt` STRING)
    OUTPUT (`response` STRING)
    WITH (
      'provider' = 'bedrock',
      'task' = 'text_generation',
      'bedrock.connection' = '${confluent_flink_connection.bedrock.display_name}',
      'bedrock.params.max_tokens' = '8192'
    );
  EOT
  depends_on       = [confluent_flink_connection.bedrock]
}

# ------------------------ Functions (from JAR) -----------------------------
module "fn_flag" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-flag-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `flag_transaction`
    AS 'io.confluent.frauddemo.FlagTransaction'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

module "fn_freeze" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-freeze-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `freeze_account`
    AS 'io.confluent.frauddemo.FreezeAccount'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

module "fn_notify" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-fn-notify-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE FUNCTION `notify_user`
    AS 'io.confluent.frauddemo.NotifyUser'
    USING JAR 'confluent-artifact://${confluent_flink_artifact.tools.id}';
  EOT
  depends_on       = [confluent_flink_artifact.tools]
}

# --------------------------------- Tools -----------------------------------
module "tool_flag" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-flag-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `flag_transaction_tool`
    USING FUNCTION `flag_transaction`
    WITH (
      'type' = 'function',
      'description' = 'Flag a specific transaction as potentially fraudulent for manual review. Arguments: transaction_id, reason.'
    );
  EOT
  depends_on       = [module.fn_flag]
}

module "tool_freeze" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-freeze-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `freeze_account_tool`
    USING FUNCTION `freeze_account`
    WITH (
      'type' = 'function',
      'description' = 'Temporarily freeze a user account due to suspected fraud. Arguments: user_id, reason.'
    );
  EOT
  depends_on       = [module.fn_freeze]
}

module "tool_notify" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-tool-notify-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE TOOL `notify_user_tool`
    USING FUNCTION `notify_user`
    WITH (
      'type' = 'function',
      'description' = 'Send a fraud alert notification to the user. Arguments: user_id, message.'
    );
  EOT
  depends_on       = [module.fn_notify]
}

# --------------------------------- Agent -----------------------------------
module "agent" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "create-agent-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
    CREATE AGENT `fraud_detection_agent`
    USING MODEL `fraud_model`
    USING PROMPT 'You are a real-time fraud detection analyst receiving activity profiles PRE-FILTERED by windowed ARIMA anomaly detection.

    IMPORTANT: These profiles have already been flagged by a statistical ARIMA model that analyzes 15-second spending windows.
    ARIMA detected that the user''s total spending in a 15-second window was anomalously high compared to their historical baseline.
    Each transaction shows: [WINDOW ANOMALY: total=$X, expected=$Y] where total > expected indicates unusual spending velocity.

    Your job is CONTEXTUAL ANALYSIS. The ARIMA model only sees aggregate spending patterns — you see the full picture:
    - Does the spending burst make sense given the user''s other activity?
    - Are there supporting fraud signals beyond just the spending velocity?
    - Is this likely a legitimate high-value shopping spree or actual fraud?

    You receive a plain-text activity profile for ONE user over a short time window with:
    - Transactions from an ARIMA-flagged 15-second window (with window total and expected amount)
    - Recent logins (location, device, ip)
    - Recent account changes (field, old value, new value)

    Analyze for these fraud signals IN ADDITION TO the ARIMA spending anomaly:
    1. Geographic impossibility: login and transaction in distant cities within minutes
    2. Account takeover: email/password change followed by rapid high-value purchases
    3. Device/IP anomalies: new devices combined with other signals
    4. Merchant patterns: unusual merchant types or rapid merchant switching
    5. Contextual mismatch: ARIMA flagged it but the context suggests legitimate behavior (e.g. holiday shopping)

    SCORING GUIDE - use the FULL range:
    - 90-100: ARIMA spending anomaly + multiple strong fraud signals (e.g. geo-impossible + account takeover)
    - 70-89: ARIMA spending anomaly + one strong fraud signal (e.g. geo-impossible travel or account takeover)
    - 45-69: ARIMA spending anomaly with weak supporting signals (e.g. new device or unusual merchants)
    - 20-44: ARIMA spending anomaly but context suggests legitimate (e.g. planned shopping from usual location)
    - 0-19: ARIMA anomaly appears to be false positive (should be rare)

    TOOLS - DECIDE THE SCORE FIRST, THEN ACT. Determine risk_score before calling any tool, and
    only call the tools the score warrants below. Make every tool call up front. Once you call a
    tool, treat it as final: never reconsider it, apologize for it, or reverse it in text.
    - If risk_score >= 80: call freeze_account_tool(user_id, reason) and notify_user_tool(user_id, message)
    - If risk_score 50-79: call flag_transaction_tool(transaction_id, reason) for each suspicious transaction and notify_user_tool(user_id, message)
    - If risk_score 20-49: call notify_user_tool(user_id, message)
    - If risk_score < 20: do NOT call any tool.
    Record the tools you actually called in "actions_taken".

    CRITICAL RULES:
    - Copy the EXACT "user_id" string from the input. Do NOT change it.
    - Copy EXACT "transaction_id" strings from the input into "flagged_transaction_ids". Do NOT invent IDs.
    - If no transactions exist, set "flagged_transaction_ids" to an empty list.
    - Your FINAL message must be ONLY the single JSON object below: no preamble, no commentary, no
      self-corrections, no markdown, no code fences. Put all explanation inside "reasoning", nowhere else.
    {"user_id": "<copy from input>", "risk_score": <0-100 integer>, "reasoning": "<one or two sentences>", "actions_taken": ["freeze_account"|"flag_transaction"|"notify_user"], "flagged_transaction_ids": ["<copied transaction ids>"]}'
    USING TOOLS `flag_transaction_tool`, `freeze_account_tool`, `notify_user_tool`
    WITH (
      'max_iterations' = '6',
      'handle_exception' = 'continue',
      'max_consecutive_failures' = '5'
    );
  EOT
  depends_on = [
    module.model,
    module.tool_flag,
    module.tool_freeze,
    module.tool_notify,
  ]
}

# ----------------------- Activity profiles (windowing) ---------------------
# Unions the 3 differently-shaped streams into a common (user_id, event_type,
# event_time, line) shape, then builds one per-user activity profile per 3s
# event-time SESSION window. Materialized as its own table/topic so it's
# queryable (SELECT * FROM activity_profiles) and shows up as a node in Stream
# Lineage between the source topics and the agent.
module "profiles" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  # Pin a small fixed watermark idle-timeout. Confluent Cloud's default
  # "progressive idleness" grows the idle timeout with statement age (up to 5
  # min), which stalls the session windows over time when the producer leaves
  # some partitions idle between cycles. A fixed 5s keeps the watermark
  # advancing so windows close and profiles flow continuously.
  extra_properties = {
    "sql.tables.scan.idle-timeout" = "5 s"
  }
  statement_name = "create-activity-profiles-${random_id.suffix.hex}"
  statement      = <<-EOT
    CREATE TABLE `activity_profiles` AS
    WITH `unified` AS (
      SELECT a.`user_id`, 'transaction' AS `event_type`,
             a.`event_time`,
             CONCAT('- txn ', a.`transaction_id`, ': $', CAST(a.`amount` AS STRING),
                    ' at ', a.`merchant`, ' (', a.`merchant_category`, ') in ', a.`location`,
                    ' [WINDOW ANOMALY: total=$', CAST(a.`window_total_amount` AS STRING),
                    ', expected=$', CAST(a.`expected_amount` AS STRING), ']') AS `line`
      FROM `anomalous_transactions` a
      UNION ALL
      SELECT l.`user_id`, 'login' AS `event_type`,
             l.`event_time`,
             CONCAT('- login from ', l.`location`, ' via ', l.`device_id`, ' (ip ', l.`ip_address`, ')') AS `line`
      FROM `user_logins` l
      WHERE EXISTS (
        SELECT 1 FROM `anomalous_transactions` a
        WHERE a.`user_id` = l.`user_id`
          AND ABS(TIMESTAMPDIFF(SECOND, l.`event_time`, a.`event_time`)) < 10
      )
      UNION ALL
      SELECT c.`user_id`, 'account_change' AS `event_type`,
             c.`event_time`,
             CONCAT('- ', c.`field_changed`, ' changed from "', c.`old_value`, '" to "', c.`new_value`, '"') AS `line`
      FROM `account_changes` c
      WHERE EXISTS (
        SELECT 1 FROM `anomalous_transactions` a
        WHERE a.`user_id` = c.`user_id`
          AND ABS(TIMESTAMPDIFF(SECOND, c.`event_time`, a.`event_time`)) < 10
      )
    )
    SELECT
      `user_id`,
      `window_start`,
      `window_end`,
      CONCAT(
        'STATISTICAL ANOMALY DETECTED (ARIMA on 15s windowed spending)\n\n',
        'User: ', `user_id`, '\n\n',
        'Transactions:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'transaction' THEN `line` END, '\n'), '  (none)'), '\n\n',
        'Logins:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'login' THEN `line` END, '\n'), '  (none)'), '\n\n',
        'Account changes:\n', COALESCE(LISTAGG(CASE WHEN `event_type` = 'account_change' THEN `line` END, '\n'), '  (none)')
      ) AS `profile_text`
    FROM TABLE(
      SESSION(TABLE `unified` PARTITION BY `user_id`, DESCRIPTOR(`event_time`), INTERVAL '3' SECONDS)
    )
    GROUP BY `user_id`, `window_start`, `window_end`;
  EOT
  depends_on = [
    module.tbl_transactions,
    module.tbl_user_logins,
    module.tbl_account_changes,
    module.tbl_anomalous_transactions,
  ]
}

# ----------------------- Detection (agent) ---------------------------------
# Reads each per-user activity profile, runs the Streaming Agent on it, and
# parses the agent's JSON verdict into the fraud_alerts columns.
module "detect" {
  source           = "./modules/flink-statement"
  organization_id  = local.flink_common.organization_id
  environment_id   = local.flink_common.environment_id
  compute_pool_id  = local.flink_common.compute_pool_id
  principal_id     = local.flink_common.principal_id
  rest_endpoint    = local.flink_common.rest_endpoint
  flink_api_key    = local.flink_common.flink_api_key
  flink_api_secret = local.flink_common.flink_api_secret
  catalog          = local.flink_common.catalog
  database         = local.flink_common.database
  statement_name   = "detect-fraud-${random_id.suffix.hex}"
  statement        = <<-EOT
    INSERT INTO `fraud_alerts`
    WITH `scored` AS (
      SELECT
        `user_id`,
        CAST(`response` AS STRING) AS `raw_response`,
        REGEXP_EXTRACT(CAST(`response` AS STRING), '\{[\s\S]*\}', 0) AS `json_text`
      FROM `activity_profiles`,
      LATERAL TABLE(AI_RUN_AGENT(`fraud_detection_agent`, `profile_text`, `user_id`))
    )
    SELECT
      `user_id`,
      COALESCE(CAST(JSON_VALUE(`json_text`, '$.risk_score') AS INT), 0) AS `risk_score`,
      COALESCE(JSON_VALUE(`json_text`, '$.reasoning'), '') AS `reasoning`,
      COALESCE(JSON_QUERY(`json_text`, '$.actions_taken'), '[]') AS `actions_taken`,
      COALESCE(JSON_QUERY(`json_text`, '$.flagged_transaction_ids'), '[]') AS `flagged_transaction_ids`,
      `raw_response`
    FROM `scored`;
  EOT
  depends_on = [
    module.agent,
    module.profiles,
    module.tbl_fraud_alerts,
  ]
}
