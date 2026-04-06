CREATE TABLE IF NOT EXISTS workspace_inventory (
	captured_at TIMESTAMP,
	workspace_id STRING,
	item_id STRING,
	item_type STRING,
	item_name STRING,
	display_name STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS pipeline_run_log (
	run_id STRING,
	pipeline_name STRING,
	stage STRING,
	started_at TIMESTAMP,
	ended_at TIMESTAMP,
	status STRING,
	rows_in LONG,
	rows_out LONG,
	error_message STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ops_audit_log (
	event_id STRING,
	timestamp TIMESTAMP,
	triggered_by STRING,
	action STRING,
	target_item STRING,
	outcome STRING,
	error_detail STRING,
	fix_applied STRING,
	fix_outcome STRING
) USING DELTA;
