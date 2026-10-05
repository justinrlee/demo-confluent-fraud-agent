# TODO

## Update demo screenshots

The screenshots in `images/demo/` (used by `demo.md`) and `images/fraud-detection-dashboard.png`
(used by the README) were captured on the earlier pipeline (`activity_profiles` → `fraud_alerts`).
Recapture them against a live deployment of the current pipeline
(`user_activity_scored` → `user_activity_anomalous` → `user_activity_anomalous_enriched` →
`fraud_analysis_results`). Run the producer with `--both` first so ARIMA has a baseline.

Must update (content changed):

- [ ] `01-stream-lineage-overview.png` — lineage graph now shows the four-table chain
- [ ] `06-flink-job-node.png` — now the materialized-table nodes, not two statements
- [ ] `07-flink-union.png` — SQL now lives in the `user_activity_scored` query
- [ ] `08-session-window-a.png` — query/result is now `user_activity_scored`
- [ ] `08-session-window-b.png` — `SELECT * FROM user_activity_scored` (adds ARIMA columns)
- [ ] `12-ai-run-agent.png` — now in the `fraud_analysis_results` query, reading `enriched_profile_text`
- [ ] `13-fraud-alerts-topic.png` — topic is now `fraud_analysis_results` (consider renaming the file)
- [ ] `14-select-fraud-alerts.png` — now `SELECT * FROM fraud_analysis_results ...` (consider renaming the file)
- [ ] `15-dashboard-overview.png` — dashboard v2 layout (ARIMA status, new sections)
- [ ] `16-dashboard-critical-alert.png` — now the Recent Fraud Analysis table
- [ ] `17-recap-lineage.png` — also used in the README intro
- [ ] `../fraud-detection-dashboard.png` — README header image (dashboard v2)

Check, may still be accurate:

- [ ] `02-producer-node.png`, `03-transactions-topic.png`, `04-user-logins-topic.png`, `05-account-changes-topic.png` — source side is unchanged, but the producer node name may differ
- [ ] `09-create-model.png` — model unchanged
- [ ] `10-create-tools.png` — tools unchanged
- [ ] `11-create-agent.png` — the agent prompt was rewritten for ARIMA-flagged input

New screenshots to consider adding:

- [ ] `SELECT * FROM user_activity_anomalous` result, for demo step 8b

When done, remove the screenshot note after step 1 in `demo.md` and the "screenshots predate it" remark in `AGENTS.md`.

## Add comments around ARIMA scoring

- [ ] Document the `ML_DETECT_ANOMALIES` call in `terraform/flink.tf` (`user_activity_scored` query): why `avg_amount` per session rather than totals, why `window_time` is the ordering column, and what each option does (`minTrainingSize` 50, `maxTrainingSize` 128, `confidencePercentage` 99.95, `enableStl` false) and why those values were chosen
- [ ] Explain in `flink.tf` why `user_activity_anomalous` filters on `avg_amount > upper_bound` (only spend spikes, not dips)
- [ ] Explain in `flink.tf` why windowing and ARIMA live in the same query (time attributes are lost through CTAS / materialized tables; see `ARIMA_ENHANCEMENT.md`)
- [ ] Comment the producer's spending profiles in `producer/generate_events.py` (`USER_PROFILES`): how each profile feeds the ARIMA baseline, and why `fraud_target` users have low baselines
- [ ] Comment where `arima_scoring_active` is set in `dashboard/app.py` and how the "ARIMA scoring pending" indicator is derived
- [ ] Reconcile `ARIMA_ENHANCEMENT.md` with the code: it still describes 15s tumbling windows, `minTrainingSize` 8 / `maxTrainingSize` 100 / 95% confidence, and a separate profiles/join pipeline
- [ ] Update the agent prompt in `flink.tf`: it still refers to "15-second window" and "[WINDOW ANOMALY: total=$X, expected=$Y]" per transaction, but the pipeline now scores 3s session averages and prepends a "STATISTICAL ANOMALY DETECTED" header
