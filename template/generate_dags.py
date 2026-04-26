import os
import re
import sys
import importlib
import yaml
import json
from collections import OrderedDict
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
OUTPUT_DIR = os.path.join(ROOT_DIR, "dags")
PLUGINS_DIR = os.path.join(ROOT_DIR, "plugins")

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

DEFAULTS = {
    "conn_id": "hijack-conn",
    "owner": "Hijack Team",
    "timezone": "Asia/Jakarta",
    "start_date": "2026-01-01",
    "catchup": False,
}

REQUIRED_FIELDS = ["schedule", "tasks"]
VALID_TASK_TYPES = ("sql", "bash", "hiveSensor", "csvSensor", "externalTaskSensor", "triggerDagRun", "etl")

# Recognised partition date keys and their strftime format
PARTITION_DATE_KEYS = {
    "dt_id":  "%Y%m%d",
    "mth_id": "%Y%m",
    "prc_dt": "%Y%m%d",
    "TRANSACTION_DATE": "%Y%m%d",
    "transactiondate": "%Y%m%d",
    "process_dt": "%Y-%m-%d",
    "load_dt": "%Y%m%d",
    "prt_dt": "%Y%m%d",
    "prcdt": "%Y%m%d"
}

PARTITION_DATE_PATTERNS = {
    "%Y%m%d": r"^\d{8}$",
    "%Y%m": r"^\d{6}$",
    "%Y-%m-%d": r"^\d{4}-\d{2}-\d{2}$",
}


def build_default_date_expr(date_key, days_ago=1):
    fmt = PARTITION_DATE_KEYS[date_key]
    return f"(datetime.today() - timedelta(days={days_ago})).strftime('{fmt}')"


def build_param_pattern(date_key):
    fmt = PARTITION_DATE_KEYS[date_key]
    pattern = PARTITION_DATE_PATTERNS.get(fmt)
    if not pattern:
        raise ValueError(f"No validation pattern configured for format '{fmt}'")
    return pattern


def build_runtime_date_template(date_key):
    return "{{ dag_run.conf.get('" + date_key + "', params." + date_key + ") if dag_run and dag_run.conf else params." + date_key + " }}"


def collect_date_params(config):
    """Collect DAG-level date params so manual runs can override partition values."""
    date_params = OrderedDict()

    configured = config.get("date_params", {})
    if isinstance(configured, list):
        for key in configured:
            if key not in PARTITION_DATE_KEYS:
                raise ValueError(f"Unsupported date_params key '{key}'. Supported: {list(PARTITION_DATE_KEYS.keys())}")
            date_params[str(key)] = {"days_ago": 1}
    elif isinstance(configured, dict):
        for key, value in configured.items():
            if key not in PARTITION_DATE_KEYS:
                raise ValueError(f"Unsupported date_params key '{key}'. Supported: {list(PARTITION_DATE_KEYS.keys())}")
            if isinstance(value, dict):
                date_params[str(key)] = {"days_ago": int(value.get("days_ago", 1))}
            else:
                date_params[str(key)] = {"days_ago": int(value)}
    elif configured:
        raise ValueError("date_params must be either a list or a mapping")

    for task in config.get("tasks", []):
        for part in task.get("partitions", []):
            if isinstance(part, str) and part in PARTITION_DATE_KEYS and part not in date_params:
                date_params[part] = {"days_ago": int(task.get("daterange", 1))}

        date_var = task.get("params", {}).get("date_var")
        if date_var in PARTITION_DATE_KEYS and date_var not in date_params:
            date_params[date_var] = {"days_ago": 1}

        csv_date_key = task.get("date_key")
        if csv_date_key in PARTITION_DATE_KEYS and csv_date_key not in date_params:
            date_params[csv_date_key] = {"days_ago": int(task.get("daterange", 1))}

    return date_params


def build_task_params_entries(task, sql_dir_expr=None, include_sql_file=False):
    params_entries = []
    task_name = task["name"]
    if sql_dir_expr:
        params_entries.append(f'"sql_dir": {sql_dir_expr}')
    if include_sql_file:
        params_entries.append(f'"sql_file": {json.dumps(task.get("sql_file", f"{task_name}.sql"))}')
    for key, value in task.get("params", {}).items():
        params_entries.append(f'{json.dumps(str(key))}: {json.dumps(value)}')
    return params_entries


def validate_config(config, filepath):
    missing = [f for f in REQUIRED_FIELDS if f not in config]
    if missing:
        raise ValueError(f"Config {filepath} missing required fields: {missing}")
    for i, task in enumerate(config["tasks"]):
        if "name" not in task:
            raise ValueError(f"Config {filepath} task #{i} missing 'name' field")
        if "type" not in task:
            raise ValueError(f"Config {filepath} task '{task.get('name')}' missing 'type' field (sql or bash)")
        if task["type"] not in VALID_TASK_TYPES:
            raise ValueError(
                f"Config {filepath} task '{task['name']}' has invalid type '{task['type']}' "
                f"(must be {', '.join(VALID_TASK_TYPES)})"
            )
        if task["type"] == "etl":
            plugin_module = task.get("plugin_module")
            loader_key = task.get("loader_key", task.get("loader_header"))
            mode = task.get("mode", task.get("loader_type"))
            if not plugin_module:
                raise ValueError(f"Config {filepath} task '{task['name']}' of type etl is missing required field 'plugin_module'")
            if not loader_key:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' of type etl is missing required field "
                    f"'loader_key' (or legacy alias 'loader_header')"
                )
            if mode and mode not in ("full", "incremental", "dimension"):
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' of type etl has invalid mode '{mode}' "
                    f"(must be full, incremental, or dimension)"
                )
            plugin_path = os.path.join(PLUGINS_DIR, f"{plugin_module}.py")
            if not os.path.isfile(plugin_path):
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' references plugin_module '{plugin_module}', "
                    f"but file was not found at plugins/{plugin_module}.py"
                )
            plugin = load_etl_plugin_module(plugin_module)
            loader_configs = getattr(plugin, "LOADER_CONFIGS", None)
            run_loader = getattr(plugin, "run_loader", None)
            if not isinstance(loader_configs, dict):
                raise ValueError(
                    f"Plugin '{plugin_module}' must define LOADER_CONFIGS as a dict"
                )
            if not callable(run_loader):
                raise ValueError(
                    f"Plugin '{plugin_module}' must define callable run_loader(loader_key, ...)"
                )
            if loader_key not in loader_configs:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' references loader_key '{loader_key}' "
                    f"which is not defined in plugins/{plugin_module}.py"
                )
            if task.get("lookback_days") is not None and int(task["lookback_days"]) < 0:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' has invalid lookback_days '{task['lookback_days']}'"
                )
        if task["type"] == "hiveSensor":
            if "partitions" not in task:
                raise ValueError(f"Config {filepath} task '{task['name']}' of type hiveSensor is missing required field 'partitions'")
            if "hdfs_base_path" not in task:
                for field in ("db", "table"):
                    if field not in task:
                        raise ValueError(f"Config {filepath} task '{task['name']}' of type hiveSensor must have either 'hdfs_base_path' or both 'db' and 'table'")
        if task["type"] == "csvSensor":
            if "filepath" not in task:
                raise ValueError(f"Config {filepath} task '{task['name']}' of type csvSensor is missing required field 'filepath'")
        if task["type"] == "externalTaskSensor":
            if "external_task_id" not in task:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' of type externalTaskSensor is missing required field 'external_task_id'"
                )
            if "external_dag_id" not in task and "target_config" not in task:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' of type externalTaskSensor must have either "
                    f"'external_dag_id' or 'target_config'"
                )
        if task["type"] == "triggerDagRun":
            if "trigger_dag_id" not in task and "target_config" not in task:
                raise ValueError(
                    f"Config {filepath} task '{task['name']}' of type triggerDagRun must have either "
                    f"'trigger_dag_id' or 'target_config'"
                )


def name_from_filename(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def to_var_name(name):
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(name)) if name else "task"
    return sanitized if sanitized[0].isalpha() or sanitized[0] == '_' else f"_{sanitized}"


def to_sensor_var(name):
    """Return the Python variable name for a sensor, avoiding double sensor_ prefix."""
    var = to_var_name(name)
    return var if var.startswith("sensor_") else f"sensor_{var}"


def dag_id_from_name(name):
    return re.sub(r'[-\s]+', '_', name)


def resolve_dag_id_from_reference(task):
    """Resolve a referenced DAG id from an explicit dag_id or another YAML config."""
    if task.get("external_dag_id"):
        return task["external_dag_id"]
    if task.get("trigger_dag_id"):
        return task["trigger_dag_id"]

    target_config = task.get("target_config")
    if not target_config:
        raise ValueError(f"Task '{task.get('name')}' is missing target DAG reference")

    target_path = os.path.join(CONFIG_DIR, target_config)
    if not os.path.isfile(target_path):
        raise ValueError(
            f"Task '{task.get('name')}': target_config '{target_config}' was not found under config/"
        )

    with open(target_path, "r", encoding="utf-8-sig") as f:
        target_yaml = yaml.safe_load(f) or {}

    return target_yaml.get("dag_id", dag_id_from_name(name_from_filename(target_config)))


def description_from_name(name):
    title = re.sub(r'[-_]+', ' ', name).title()
    return f"Pipeline {title}"


def parse_start_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def load_etl_plugin_module(plugin_module):
    try:
        return importlib.import_module(f"plugins.{plugin_module}")
    except Exception as exc:
        raise ValueError(f"Failed to import plugin module 'plugins.{plugin_module}': {exc}") from exc


def normalize_etl_mode(task, loader_config):
    mode = task.get("mode", task.get("loader_type"))
    if mode == "dimension":
        return "full"
    if mode:
        return mode
    return "incremental" if loader_config.get("supports_incremental_window") else "full"


def build_etl_task_meta(task):
    plugin_module = task["plugin_module"]
    loader_key = task.get("loader_key", task.get("loader_header"))
    plugin = load_etl_plugin_module(plugin_module)
    loader_config = plugin.LOADER_CONFIGS[loader_key]
    mode = normalize_etl_mode(task, loader_config)
    dag_prefix = task.get("dag_prefix") or getattr(plugin, "DEFAULT_DAG_PREFIX", plugin_module)
    dag_id = task.get("dag_id", f"{dag_prefix}_{loader_key}_hourly")
    tags = list(OrderedDict.fromkeys(
        list(task.get("tags", [])) +
        list(loader_config.get("tags", [])) +
        [dag_prefix, "etl", loader_key]
    ))
    return {
        "plugin": plugin,
        "plugin_module": plugin_module,
        "loader_key": loader_key,
        "loader_config": loader_config,
        "mode": mode,
        "dag_prefix": dag_prefix,
        "dag_id": dag_id,
        "tags": tags,
        "lookback_days": int(task.get("lookback_days", 1)),
    }


def generate_etl_dags(config, filename):
    owner = config.get("owner", DEFAULTS["owner"])
    catchup = config.get("catchup", DEFAULTS["catchup"])
    start_date = config.get("start_date", DEFAULTS["start_date"])
    start_year, start_month, start_day = parse_start_date(start_date)
    tasks = config["tasks"]
    schedule = config["schedule"]

    if any(t.get("dag_group") for t in tasks):
        raise ValueError("ETL task generation does not support dag_group yet")

    outputs = {}
    for task in tasks:
        if task["type"] != "etl":
            raise ValueError("Configs that contain ETL tasks cannot mix them with non-ETL task types yet")

        meta = build_etl_task_meta(task)
        loader_name = meta["loader_config"]["name"]
        description = task.get("description", f"Hourly {meta['dag_prefix']} loader for {loader_name}")
        lines = []
        lines.append("from datetime import datetime, timedelta")
        lines.append("")
        lines.append("from airflow import DAG")
        lines.append("from airflow.operators.python import PythonOperator")
        lines.append("")
        lines.append("from plugins import " + meta["plugin_module"])
        lines.append("")
        lines.append("default_args = {")
        lines.append(f'    "owner": "{owner}",')
        lines.append('    "depends_on_past": False,')
        lines.append('    "retries": 1,')
        lines.append('    "retry_delay": timedelta(minutes=10),')
        lines.append("}")
        lines.append("")
        lines.append(f"LOOKBACK_DAYS = {meta['lookback_days']}")
        lines.append("")
        lines.append("def run_etl_loader(loader_key, **context):")
        lines.append(f'    loader_config = {meta["plugin_module"]}.LOADER_CONFIGS[loader_key]')
        if meta["mode"] == "incremental":
            lines.append("    start_date = None")
            lines.append("    end_date = None")
            lines.append('    if loader_config.get("supports_incremental_window", False):')
            lines.append('        interval_start = context["data_interval_start"] - timedelta(days=LOOKBACK_DAYS)')
            lines.append('        interval_end = context["data_interval_end"]')
            lines.append('        start_date = interval_start.strftime("%Y-%m-%d")')
            lines.append('        end_date = interval_end.strftime("%Y-%m-%d")')
            lines.append(f'    {meta["plugin_module"]}.run_loader(loader_key, start_date=start_date, end_date=end_date)')
        else:
            lines.append("    del context")
            lines.append(f'    {meta["plugin_module"]}.run_loader(loader_key)')
        lines.append("")
        lines.append("dag = DAG(")
        lines.append(f'    dag_id="{meta["dag_id"]}",')
        lines.append("    default_args=default_args,")
        lines.append(f'    description="{description}",')
        if isinstance(schedule, dict):
            if schedule.get("type") == "multi_cron":
                lines.append('    schedule=' + json.dumps(schedule.get("cron_defs", [])) + ",")
            else:
                raise ValueError(f"Unsupported ETL schedule mapping: {schedule}")
        elif schedule is None:
            lines.append("    schedule=None,")
        else:
            lines.append(f'    schedule="{schedule}",')
        lines.append(f"    start_date=datetime({start_year}, {start_month}, {start_day}),")
        lines.append(f"    catchup={catchup},")
        lines.append("    max_active_runs=1,")
        lines.append(f"    tags={meta['tags']},")
        lines.append(")")
        lines.append("")
        lines.append("with dag:")
        lines.append("    PythonOperator(")
        lines.append(f'        task_id="load_{meta["loader_key"]}",')
        lines.append("        python_callable=run_etl_loader,")
        lines.append(f'        op_kwargs={{"loader_key": "{meta["loader_key"]}"}},')
        lines.append("    )")
        lines.append("")
        lines.append(f'globals()["{meta["dag_id"]}"] = dag')
        outputs[f"{meta['dag_id']}.py"] = "\n".join(lines)

    return outputs




def build_hdfs_partition_path(db, table, partitions, dateranges=None, base_path=None):
    """Build an HDFS partition path emitted as a Python f-string evaluated at runtime.

    Each entry in `partitions` must be either:
      - "key=value"  — used verbatim as a static path segment
      - a plain key name from PARTITION_DATE_KEYS — rendered as a runtime
        expression: (datetime.today() - timedelta(days=N)).strftime(fmt)

    `dateranges` is a dict mapping partition key -> daterange int for date keys.
    `base_path`  overrides the default /user/hive/warehouse/{db}.db/{table} base.
    """
    if not partitions:
        raise ValueError(f"partitions list is empty for {db}.{table}")

    dateranges = dateranges or {}
    base = base_path.rstrip("/") if base_path else f"/user/hive/warehouse/{db}.db/{table}"
    segments = []

    for part in partitions:
        part = str(part).strip()
        if "=" in part:
            segments.append(part)
        elif part in PARTITION_DATE_KEYS:
            segments.append(f"{part}={build_runtime_date_template(part)}")
        else:
            raise ValueError(
                f"Partition key '{part}' is not recognised. "
                f"Use 'key=value' for static values, or one of the date keys: {list(PARTITION_DATE_KEYS.keys())}"
            )

    path = base + "/" + "/".join(segments)
    return json.dumps(path)



def _build_sensor_from_task(task, indent):
    """Build a HivePartitionSensor block with a fully runtime-evaluated HDFS path.

    The partition date is computed inside the generated DAG via timedelta so
    the sensor always checks the correct date regardless of when the DAG was generated.

    Returns (lines, sensor_var_names).
    """
    name       = task["name"]
    var        = to_var_name(name)
    partitions = list(task["partitions"])
    last_part  = partitions[-1]
    daterange  = task.get("daterange", 1)

    if last_part not in PARTITION_DATE_KEYS:
        raise ValueError(
            f"Task '{name}': last partition key '{last_part}' is not a recognised date key. "
            f"Supported: {list(PARTITION_DATE_KEYS.keys())}"
        )

    # Pass daterange only for the last (date) partition key; static segments are unaffected
    hdfs_path = build_hdfs_partition_path(
        task.get("db"),
        task.get("table"),
        partitions,
        dateranges={last_part: daterange},
        base_path=task.get("hdfs_base_path"),
    )

    sensor_name = to_sensor_var(name)
    mode      = task.get("mode", "reschedule")
    poke      = task.get("poke_interval", 120)
    timeout   = task.get("timeout", 7200)
    soft_fail = task.get("soft_fail", False)

    lines = []
    lines.append(f'{indent}{sensor_name} = HivePartitionSensor(')
    lines.append(f'{indent}    task_id="{sensor_name}",')
    lines.append(f'{indent}    hdfs_partition_path={hdfs_path},')
    lines.append(f'{indent}    mode="{mode}",')
    lines.append(f'{indent}    poke_interval={poke},')
    lines.append(f'{indent}    timeout={timeout},')
    lines.append(f'{indent}    soft_fail={soft_fail},')
    if "pool" in task:
        lines.append(f'{indent}    pool="{task["pool"]}",')
    if "pool_slots" in task:
        lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
    lines.append(f'{indent})')

    return lines, [sensor_name]


def render_hive_sensor_task(task, indent):
    """Render a standalone hiveSensor task (type: hiveSensor)."""
    name = task["name"]

    if "hdfs_partition_path" in task:
        # Explicit path override
        var = to_sensor_var(name)
        mode      = task.get("mode", "reschedule")
        poke      = task.get("poke_interval", 120)
        timeout   = task.get("timeout", 7200)
        soft_fail = task.get("soft_fail", False)
        lines = []
        lines.append(f'{indent}{var} = HivePartitionSensor(')
        lines.append(f'{indent}    task_id="{var}",')
        lines.append(f'{indent}    hdfs_partition_path="{task["hdfs_partition_path"]}",')
        lines.append(f'{indent}    mode="{mode}",')
        lines.append(f'{indent}    poke_interval={poke},')
        lines.append(f'{indent}    timeout={timeout},')
        lines.append(f'{indent}    soft_fail={soft_fail},')
        if "pool" in task:
            lines.append(f'{indent}    pool="{task["pool"]}",')
        if "pool_slots" in task:
            lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
        lines.append(f'{indent})')
        task["_sensor_vars"] = []
        return '\n'.join(lines)

    lines, sensor_vars = _build_sensor_from_task(task, indent)
    task["_hive_sensor_var"] = sensor_vars[0]
    task["_sensor_vars"] = []
    return '\n'.join(lines)


def render_csv_sensor_task(task, indent):
    """Render a standalone csvSensor task (type: csvSensor).

    Scans `filepath` for any file whose name contains any date string
    in the range [today, D-1, ..., D-daterange], all computed at runtime.
    Returns True as soon as ANY matching file is found.
    """
    name      = task["name"]
    var       = to_var_name(name)
    filepath  = task["filepath"].rstrip("/")
    daterange = task.get("daterange", 1)
    mode      = task.get("mode", "reschedule")
    poke      = task.get("poke_interval", 120)
    timeout   = task.get("timeout", 7200)
    soft_fail = task.get("soft_fail", False)
    date_key  = task.get("date_key")

    if date_key in PARTITION_DATE_KEYS:
        date_strings_expr = f'[{json.dumps(build_runtime_date_template(date_key))}]'
    else:
        # Build a runtime list expression: [today, D-1, ..., D-daterange]
        date_strings_expr = (
            "["
            + ", ".join(
                f"(datetime.today() - timedelta(days={d})).strftime('%Y%m%d')"
                for d in range(0, daterange + 1)
            )
            + "]"
        )

    sensor_name = to_sensor_var(name)
    lines = []
    lines.append(f'{indent}{sensor_name} = CsvFileSensor(')
    lines.append(f'{indent}    task_id="{sensor_name}",')
    lines.append(f'{indent}    filepath="{filepath}",')
    lines.append(f'{indent}    date_strings={date_strings_expr},')
    lines.append(f'{indent}    mode="{mode}",')
    lines.append(f'{indent}    poke_interval={poke},')
    lines.append(f'{indent}    timeout={timeout},')
    lines.append(f'{indent}    soft_fail={soft_fail},')
    if "pool" in task:
        lines.append(f'{indent}    pool="{task["pool"]}",')
    if "pool_slots" in task:
        lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
    lines.append(f'{indent})')

    task["_csv_sensor_var"] = sensor_name
    task["_sensor_vars"] = []
    return '\n'.join(lines)


def render_external_task_sensor_task(task, indent):
    """Render a standalone ExternalTaskSensor task."""
    name = task["name"]
    var = to_var_name(name)
    external_dag_id = resolve_dag_id_from_reference(task)
    mode = task.get("mode", "reschedule")
    poke = task.get("poke_interval", 60)
    timeout = task.get("timeout", 10800)
    check_existence = task.get("check_existence", True)
    allowed_states = task.get("allowed_states")
    failed_states = task.get("failed_states")
    skip_on_manual_run = task.get("skip_on_manual_run", False)
    deferrable = task.get("deferrable", True)
    sensor_class = "ManualSkippableExternalTaskSensor" if skip_on_manual_run else "ExternalTaskSensor"

    lines = []
    lines.append(f'{indent}{var} = {sensor_class}(')
    lines.append(f'{indent}    task_id="{name}",')
    lines.append(f'{indent}    external_dag_id="{external_dag_id}",')
    lines.append(f'{indent}    external_task_id="{task["external_task_id"]}",')
    lines.append(f'{indent}    mode="{mode}",')
    lines.append(f'{indent}    poke_interval={poke},')
    lines.append(f'{indent}    timeout={timeout},')
    lines.append(f'{indent}    check_existence={check_existence},')
    lines.append(f'{indent}    deferrable={deferrable},')
    if "pool" in task:
        lines.append(f'{indent}    pool="{task["pool"]}",')
    if "pool_slots" in task:
        lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
    if skip_on_manual_run:
        lines.append(f'{indent}    skip_on_manual_run=True,')
    if allowed_states is not None:
        lines.append(f'{indent}    allowed_states={allowed_states},')
    if failed_states is not None:
        lines.append(f'{indent}    failed_states={failed_states},')
    lines.append(f'{indent})')

    task["_sensor_vars"] = []
    return '\n'.join(lines)


def render_trigger_dag_task(task, indent, date_param_keys=None):
    """Render a TriggerDagRunOperator task."""
    name = task["name"]
    var = to_var_name(name)
    trigger_dag_id = resolve_dag_id_from_reference(task)
    wait_for_completion = task.get("wait_for_completion", False)
    poke_interval = task.get("poke_interval", 60)
    reset_dag_run = task.get("reset_dag_run", False)
    allowed_states = task.get("allowed_states")
    failed_states = task.get("failed_states")
    deferrable = task.get("deferrable", wait_for_completion)

    lines = []
    lines.append(f'{indent}{var} = TriggerDagRunOperator(')
    lines.append(f'{indent}    task_id="{name}",')
    lines.append(f'{indent}    trigger_dag_id="{trigger_dag_id}",')
    lines.append(f'{indent}    wait_for_completion={wait_for_completion},')
    lines.append(f'{indent}    poke_interval={poke_interval},')
    lines.append(f'{indent}    reset_dag_run={reset_dag_run},')
    lines.append(f'{indent}    deferrable={deferrable},')
    if date_param_keys:
        conf_entries = [f'{json.dumps(key)}: {json.dumps(build_runtime_date_template(key))}' for key in date_param_keys]
        lines.append(f'{indent}    conf={{{", ".join(conf_entries)}}},')
    if "pool" in task:
        lines.append(f'{indent}    pool="{task["pool"]}",')
    if "pool_slots" in task:
        lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
    if allowed_states is not None:
        lines.append(f'{indent}    allowed_states={allowed_states},')
    if failed_states is not None:
        lines.append(f'{indent}    failed_states={failed_states},')
    lines.append(f'{indent})')

    task["_sensor_vars"] = []
    return '\n'.join(lines)

def render_task(task, indent, sql_dir_expr=None, date_param_keys=None):
    """Render a sql or bash task, with an optional waitForPartition sensor prefix."""
    name = task["name"]
    var = to_var_name(name)
    task_type = task["type"]
    lines = []

    # Optional sensor prefix for bash/sql tasks
    if task.get("waitForPartition"):
        if "hdfs_partition_path" in task:
            sensor_var = to_sensor_var(name)
            mode      = task.get("mode", "reschedule")
            poke      = task.get("poke_interval", 120)
            timeout   = task.get("timeout", 7200)
            soft_fail = task.get("soft_fail", False)
            lines.append(f'{indent}{sensor_var} = HivePartitionSensor(')
            lines.append(f'{indent}    task_id="sensor_{name}",')
            lines.append(f'{indent}    hdfs_partition_path="{task["hdfs_partition_path"]}",')
            lines.append(f'{indent}    mode="{mode}",')
            lines.append(f'{indent}    poke_interval={poke},')
            lines.append(f'{indent}    timeout={timeout},')
            lines.append(f'{indent}    soft_fail={soft_fail},')
            if "pool" in task:
                lines.append(f'{indent}    pool="{task["pool"]}",')
            if "pool_slots" in task:
                lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
            lines.append(f'{indent})')
            lines.append("")
            task["_sensor_vars"] = [sensor_var]
        elif "db" in task and "table" in task and "partitions" in task:
            sensor_lines, sensor_vars = _build_sensor_from_task(task, indent)
            lines.extend(sensor_lines)
            lines.append("")
            task["_sensor_vars"] = sensor_vars
        else:
            raise ValueError(
                f"Task '{name}' has waitForPartition=true but is missing "
                f"either 'hdfs_partition_path' OR ('db' + 'table' + 'partitions')."
            )

    # Main operator block
    if task_type == "bash":
        lines.append(f'{indent}{var} = BashOperator(')
        lines.append(f'{indent}    task_id="{name}",')
        if "command" in task:
            cmd = task["command"].replace('"', '\\"')
            lines.append(f'{indent}    bash_command="{cmd}",')
        else:
            lines.append(f'{indent}    bash_command="{name}.sh",')
        if sql_dir_expr:
            params_entries = build_task_params_entries(
                task,
                sql_dir_expr=sql_dir_expr,
                include_sql_file=True,
            )
            lines.append(f'{indent}    params={{{", ".join(params_entries)}}},')
        if "pool" in task:
            lines.append(f'{indent}    pool="{task["pool"]}",')
        if "pool_slots" in task:
            lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
        lines.append(f'{indent})')
    else:
        lines.append(f'{indent}{var} = MultiStatementSQLExecuteQueryOperator(')
        lines.append(f'{indent}    task_id="{name}",')
        lines.append(f'{indent}    conn_id="{task["_conn_id"]}",')
        if "query" in task:
            sql_val = task["query"].replace('"', '\\"')
            lines.append(f'{indent}    sql="{sql_val}",')
        else:
            lines.append(f'{indent}    sql="{name}.sql",')
        lines.append(f'{indent}    split_statements=True,')
        params_entries = build_task_params_entries(task)
        if params_entries:
            lines.append(f'{indent}    params={{{", ".join(params_entries)}}},')
        if "database" in task:
            lines.append(f'{indent}    hook_params={{"schema": "{task["database"]}"}},')
        if "pool" in task:
            lines.append(f'{indent}    pool="{task["pool"]}",')
        if "pool_slots" in task:
            lines.append(f'{indent}    pool_slots={task["pool_slots"]},')
        lines.append(f'{indent})')

    return '\n'.join(lines)


def generate_dag_code(config, filename):
    name = name_from_filename(filename)

    dag_id = config.get("dag_id", dag_id_from_name(name))
    description = config.get("description", description_from_name(name))
    schedule = config["schedule"]
    project = config.get("project")
    default_conn_id = config.get("conn_id", DEFAULTS["conn_id"])
    owner = config.get("owner", DEFAULTS["owner"])
    tags = config.get("tags", [])
    start_date = config.get("start_date", DEFAULTS["start_date"])
    catchup = config.get("catchup", DEFAULTS["catchup"])
    timezone = config.get("timezone", DEFAULTS["timezone"])
    tasks = config["tasks"]
    date_params = collect_date_params(config)

    start_year, start_month, start_day = parse_start_date(start_date)
    multi_cron = isinstance(schedule, list)

    # Enrich tasks with resolved defaults
    for task in tasks:
        task["_conn_id"] = task.get("conn_id", default_conn_id)

    # Group by layer
    layers = OrderedDict()
    for task in tasks:
        layer = task.get("layer")
        if layer:
            layers.setdefault(layer, []).append(task)
    tasks_without_layer = [t for t in tasks if not t.get("layer")]

    # Flags
    use_sql    = any(t["type"] == "sql" for t in tasks)
    use_bash   = any(t["type"] == "bash" for t in tasks)
    use_sensor = any(t["type"] == "hiveSensor" or t.get("waitForPartition") for t in tasks)
    use_csv_sensor = any(t["type"] == "csvSensor" for t in tasks)
    use_external_sensor = any(t["type"] == "externalTaskSensor" for t in tasks)
    use_trigger_dag = any(t["type"] == "triggerDagRun" for t in tasks)
    needs_searchpath = use_sql or use_bash

    # Build searchpath entries
    searchpaths = []
    if use_sql:
        searchpaths.append(
            f'os.path.join(INCLUDE_DIR, "sql", "{project}")' if project
            else 'os.path.join(INCLUDE_DIR, "sql")'
        )
    if use_bash:
        searchpaths.append(
            f'os.path.join(INCLUDE_DIR, "scripts", "{project}")' if project
            else 'os.path.join(INCLUDE_DIR, "scripts")'
        )

    lines = []

    # Imports
    lines.append("from airflow import DAG")
    if use_sql:
        lines.append("from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator")
        lines.append("from airflow.providers.common.sql.hooks.sql import return_single_query_results")
    if date_params:
        lines.append("from airflow.models.param import Param")
    if use_bash:
        lines.append("from airflow.operators.bash import BashOperator")
    if use_external_sensor:
        lines.append("from airflow.sensors.external_task import ExternalTaskSensor")
    if use_trigger_dag:
        lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
    if use_sensor or use_csv_sensor:
        lines.append("from airflow.sensors.base import BaseSensorOperator")
        lines.append("from airflow.utils.decorators import apply_defaults")
        lines.append("import subprocess")

    if layers:
        lines.append("from airflow.utils.task_group import TaskGroup")
    if multi_cron:
        lines.append("from multi_cron_timetable import MultiCronTimetable")
    lines.append("from datetime import datetime, timedelta")
    if needs_searchpath:
        lines.append("import os")

    # Variables
    if needs_searchpath:
        lines.append("")
        lines.append("INCLUDE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'include')")

    if use_sql:
        lines.append("")
        lines.append("")
        lines.append("class MultiStatementSQLExecuteQueryOperator(SQLExecuteQueryOperator):")
        lines.append('    """Split templated SQL into separate statements and strip trailing semicolons."""')
        lines.append("")
        lines.append("    def execute(self, context):")
        lines.append('        self.log.info(\"Executing: %s\", self.sql)')
        lines.append("        hook = self.get_db_hook()")
        lines.append("        if self.split_statements and isinstance(self.sql, str):")
        lines.append("            sql = [hook.strip_sql_string(stmt) for stmt in hook.split_sql_string(self.sql)]")
        lines.append("        else:")
        lines.append("            sql = self.sql")
        lines.append("        output = hook.run(")
        lines.append("            sql=sql,")
        lines.append("            autocommit=self.autocommit,")
        lines.append("            parameters=self.parameters,")
        lines.append("            handler=self.handler if self._should_run_output_processing() else None,")
        lines.append("            return_last=self.return_last,")
        lines.append("        )")
        lines.append("        if not self._should_run_output_processing():")
        lines.append("            return None")
        lines.append("        if return_single_query_results(sql, self.return_last, False):")
        lines.append("            return self._process_output([output], hook.descriptions)[-1]")
        lines.append("        return self._process_output(output, hook.descriptions)")

    # HivePartitionSensor class (injected once if any task uses waitForPartition)
    if use_sensor or use_csv_sensor:
        lines.append("")
        lines.append("")
        lines.append("class HivePartitionSensor(BaseSensorOperator):")
        lines.append('    """Waits until an HDFS/Hive partition path exists."""')
        lines.append('    template_fields = ("hdfs_partition_path",)')
        lines.append("")
        lines.append("    @apply_defaults")
        lines.append("    def __init__(self, hdfs_partition_path: str, *args, **kwargs):")
        lines.append("        super().__init__(*args, **kwargs)")
        lines.append("        self.hdfs_partition_path = hdfs_partition_path")
        lines.append("")
        lines.append("    def poke(self, context):")
        lines.append('        self.log.info(f"Checking HDFS partition: {self.hdfs_partition_path}")')
        lines.append("        try:")
        lines.append('            kinit = subprocess.run(')
        lines.append('                ["kinit", "-kt", "/etc/keytabs/hdp-batch_user2.keytab", "hdp-batch_user2@OFFICE.CORP.INDOSAT.COM"],')
        lines.append('                capture_output=True')
        lines.append('            )')
        lines.append('            if kinit.returncode != 0:')
        lines.append('                raise RuntimeError(f"kinit failed: {kinit.stderr}")')
        lines.append('            result = subprocess.run(')
        lines.append('                ["hdfs", "dfs", "-test", "-e", self.hdfs_partition_path],')
        lines.append('                capture_output=True')
        lines.append('            )')
        lines.append("            exists = result.returncode == 0")
        lines.append('            self.log.info("Partition found." if exists else "Partition not ready yet...")')
        lines.append("            return exists")
        lines.append("        except Exception as e:")
        lines.append('            self.log.error(f"Error checking HDFS: {e}")')
        lines.append("            return False")

    # CsvFileSensor class (injected once if any task uses csvSensor)
    if use_csv_sensor:
        lines.append("")
        lines.append("")
        lines.append("class CsvFileSensor(BaseSensorOperator):")
        lines.append('    """Waits until a file whose name contains any of the target date strings exists in an HDFS filepath."""')
        lines.append('    template_fields = ("filepath", "date_strings")')
        lines.append("")
        lines.append("    @apply_defaults")
        lines.append("    def __init__(self, filepath: str, date_strings: list, *args, **kwargs):")
        lines.append("        super().__init__(*args, **kwargs)")
        lines.append("        self.filepath = filepath")
        lines.append("        self.date_strings = date_strings")
        lines.append("")
        lines.append("    def poke(self, context):")
        lines.append('        self.log.info(f"Scanning HDFS path {self.filepath} for files matching any of: {self.date_strings}")')
        lines.append("        try:")
        lines.append('            kinit = subprocess.run(')
        lines.append('                ["kinit", "-kt", "/etc/keytabs/hdp-batch_user2.keytab", "hdp-batch_user2@OFFICE.CORP.INDOSAT.COM"],')
        lines.append('                capture_output=True')
        lines.append('            )')
        lines.append('            if kinit.returncode != 0:')
        lines.append('                raise RuntimeError(f"kinit failed: {kinit.stderr}")')
        lines.append('            result = subprocess.run(')
        lines.append('                ["hdfs", "dfs", "-ls", self.filepath],')
        lines.append('                capture_output=True, text=True')
        lines.append('            )')
        lines.append('            if result.returncode != 0:')
        lines.append('                self.log.info("HDFS path not accessible yet...")')
        lines.append('                return False')
        lines.append('            for date_str in self.date_strings:')
        lines.append('                matches = [line for line in result.stdout.splitlines() if date_str in line.split("/")[-1]]')
        lines.append('                if matches:')
        lines.append('                    self.log.info(f"Found match for {date_str}: {matches}")')
        lines.append('                    return True')
        lines.append('            self.log.info("No matching file found yet...")')
        lines.append('            return False')
        lines.append("        except Exception as e:")
        lines.append('            self.log.error(f"Error scanning HDFS path: {e}")')
        lines.append("            return False")

    if use_external_sensor:
        lines.append("")
        lines.append("")
        lines.append("class ManualSkippableExternalTaskSensor(ExternalTaskSensor):")
        lines.append('    """Skips waiting for upstream tasks when the DAG is manually triggered."""')
        lines.append("")
        lines.append("    def __init__(self, skip_on_manual_run: bool = False, *args, **kwargs):")
        lines.append("        super().__init__(*args, **kwargs)")
        lines.append("        self.skip_on_manual_run = skip_on_manual_run")
        lines.append("")
        lines.append("    def poke(self, context):")
        lines.append('        dag_run = context.get("dag_run")')
        lines.append('        run_type = str(getattr(dag_run, "run_type", "")).lower() if dag_run else ""')
        lines.append('        if self.skip_on_manual_run and run_type.endswith("manual"):')
        lines.append('            self.log.info("Manual DAG run detected, skipping external dependency wait.")')
        lines.append("            return True")
        lines.append("        return super().poke(context)")

    # Timetable
    if multi_cron:
        lines.append("")
        lines.append(f'timetable = MultiCronTimetable(cron_defs={schedule}, timezone="{timezone}")')

    # DAG definition
    lines.append("")
    lines.append("with DAG(")
    lines.append(f'    dag_id="{dag_id}",')
    lines.append(f'    description="{description}",')
    if multi_cron:
        lines.append("    timetable=timetable,")
    elif schedule is None:
        lines.append("    schedule=None,")
    else:
        lines.append(f'    schedule="{schedule}",')
    lines.append(f"    start_date=datetime({start_year}, {start_month}, {start_day}),")
    lines.append(f"    catchup={catchup},")
    lines.append(f"    tags={tags},")
    lines.append(f'    default_args={{"owner": "{owner}"}},')
    if date_params:
        lines.append("    params={")
        for key, meta in date_params.items():
            lines.append(
                f'        "{key}": Param('
                f'{build_default_date_expr(key, meta["days_ago"])}, '
                f'type="string", '
                f'pattern=r"{build_param_pattern(key)}"'
                f'),'
            )
        lines.append("    },")
    if needs_searchpath:
        if len(searchpaths) == 1:
            lines.append(f'    template_searchpath=[{searchpaths[0]}],')
        else:
            lines.append('    template_searchpath=[')
            for sp in searchpaths:
                lines.append(f'        {sp},')
            lines.append('    ],')
    lines.append(") as dag:")

    if project:
        sql_dir_expr = f'os.path.join(INCLUDE_DIR, "sql", "{project}")'
    else:
        sql_dir_expr = 'os.path.join(INCLUDE_DIR, "sql")'

    def _render(task, indent):
        if task["type"] == "hiveSensor":
            return render_hive_sensor_task(task, indent)
        if task["type"] == "csvSensor":
            return render_csv_sensor_task(task, indent)
        if task["type"] == "externalTaskSensor":
            return render_external_task_sensor_task(task, indent)
        if task["type"] == "triggerDagRun":
            return render_trigger_dag_task(task, indent, date_param_keys=list(date_params.keys()))
        return render_task(task, indent, sql_dir_expr=sql_dir_expr, date_param_keys=list(date_params.keys()))

    # Tasks — renderers populate task["_sensor_vars"] / task["_hive_sensor_var"] as side-effects
    for layer_name, layer_tasks in layers.items():
        lines.append("")
        lines.append(f'    with TaskGroup("{layer_name}") as {layer_name}:')
        for task in layer_tasks:
            lines.append(_render(task, "        "))

    for task in tasks_without_layer:
        lines.append("")
        lines.append(_render(task, "    "))

    # Dependencies (after render so _sensor_vars / _hive_sensor_var are populated)
    dependencies = []
    # Build a lookup: task name -> the variable name that represents it in the DAG
    task_var_lookup = {}
    for task in tasks:
        if task["type"] == "hiveSensor":
            task_var_lookup[task["name"]] = to_sensor_var(task["name"])
        elif task["type"] == "csvSensor":
            task_var_lookup[task["name"]] = to_sensor_var(task["name"])
        else:
            task_var_lookup[task["name"]] = to_var_name(task["name"])

    for task in tasks:
        var = task_var_lookup[task["name"]]
        # sensor-prefix >> main operator (bash/sql only)
        for s in task.get("_sensor_vars", []):
            dependencies.append(f"{s} >> {var}")
        # explicit depends_on
        for dep in task.get("depends_on", []):
            dep_name = os.path.splitext(dep)[0]
            dep_var  = task_var_lookup.get(dep_name, to_var_name(dep_name))
            dependencies.append(f"{dep_var} >> {var}")

    if dependencies:
        lines.append("")
        for dep in dependencies:
            lines.append(f"    {dep}")

    lines.append("")
    return "\n".join(lines)


def _last_task_var(group_tasks, task_var_lookup):
    """Return the var name of the last non-sensor task in a group (used as ExternalTaskSensor anchor)."""
    # Prefer the last bash/sql task; fall back to last sensor
    for task in reversed(group_tasks):
        if task["type"] in ("bash", "sql"):
            return task_var_lookup[task["name"]], task["name"]
    # all sensors — use last one
    last = group_tasks[-1]
    return task_var_lookup[last["name"]], last["name"]


def generate_grouped_dags(config, filename):
    """
    When tasks have a `dag_group` field, split them into multiple DAG files.

    Rules:
    - One .py file per unique dag_group value.
    - The group whose name appears first in the YAML (or is explicitly marked
      `is_entry: true`) gets the real schedule; all others get schedule=None.
    - Cross-group depends_on → ExternalTaskSensor added at the top of the
      dependent DAG, waiting for the LAST task of the upstream group DAG.

    Returns: dict of {output_filename: dag_code_string}
    """
    base_name    = name_from_filename(filename)
    schedule     = config["schedule"]
    project      = config.get("project")
    owner        = config.get("owner", DEFAULTS["owner"])
    tags         = config.get("tags", [])
    start_date   = config.get("start_date", DEFAULTS["start_date"])
    catchup      = config.get("catchup", DEFAULTS["catchup"])
    timezone     = config.get("timezone", DEFAULTS["timezone"])
    default_conn_id = config.get("conn_id", DEFAULTS["conn_id"])
    date_params = collect_date_params(config)
    start_year, start_month, start_day = parse_start_date(start_date)
    multi_cron   = isinstance(schedule, list)
    all_tasks    = config["tasks"]

    # Enrich tasks
    for task in all_tasks:
        task["_conn_id"] = task.get("conn_id", default_conn_id)

    # Collect ordered unique groups
    seen_groups = []
    for task in all_tasks:
        g = task.get("dag_group")
        if g and g not in seen_groups:
            seen_groups.append(g)

    entry_group = seen_groups[0]  # first group gets the real schedule

    # Partition tasks by group
    groups = OrderedDict()
    for g in seen_groups:
        groups[g] = [t for t in all_tasks if t.get("dag_group") == g]

    # Build global task_var_lookup (across ALL groups, needed for cross-group dep resolution)
    task_var_lookup = {}
    for task in all_tasks:
        if task["type"] in ("hiveSensor", "csvSensor"):
            task_var_lookup[task["name"]] = to_sensor_var(task["name"])
        else:
            task_var_lookup[task["name"]] = to_var_name(task["name"])

    # Determine the "anchor" (last task var + task_id) for each group
    # downstream DAGs will ExternalTaskSensor on this task
    group_anchor = {}   # group_name -> (var_name, task_id_str, dag_id_str)
    for g, gtasks in groups.items():
        dag_id = f"{dag_id_from_name(base_name)}__{g}"
        var, tname = _last_task_var(gtasks, task_var_lookup)
        # task_id in the generated DAG equals var name for sensors, task name for bash/sql
        if all_tasks[next(i for i, t in enumerate(all_tasks) if t["name"] == tname)]["type"] in ("hiveSensor", "csvSensor"):
            task_id = var   # sensor var == task_id
        else:
            task_id = tname
        group_anchor[g] = (var, task_id, dag_id)

    outputs = {}

    for g, gtasks in groups.items():
        dag_id      = f"{dag_id_from_name(base_name)}__{g}"
        description = f"Pipeline {re.sub(r'[-_]+', ' ', base_name).title()} — {g.replace('_', ' ').title()}"
        is_entry    = (g == entry_group)

        use_sql        = any(t["type"] == "sql"        for t in gtasks)
        use_bash       = any(t["type"] == "bash"       for t in gtasks)
        use_sensor = any(t["type"] == "hiveSensor" or t.get("waitForPartition") for t in gtasks)
        use_csv_sensor = any(t["type"] == "csvSensor"  for t in gtasks)
        use_external_sensor = any(t["type"] == "externalTaskSensor" for t in gtasks)
        use_trigger_dag = any(t["type"] == "triggerDagRun" for t in gtasks)
        needs_searchpath = use_sql or use_bash

        # Cross-group deps: which upstream groups does THIS group depend on?
        upstream_groups = OrderedDict()   # group_name -> set of task names referenced
        for task in gtasks:
            for dep in task.get("depends_on", []):
                dep_name = os.path.splitext(dep)[0]
                dep_task = next((t for t in all_tasks if t["name"] == dep_name), None)
                if dep_task and dep_task.get("dag_group") and dep_task["dag_group"] != g:
                    ug = dep_task["dag_group"]
                    upstream_groups.setdefault(ug, set()).add(dep_name)

        # ── Imports ──────────────────────────────────────────────────────────
        lines = []
        lines.append("from airflow import DAG")
        if use_sql:
            lines.append("from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator")
            lines.append("from airflow.providers.common.sql.hooks.sql import return_single_query_results")
        if date_params:
            lines.append("from airflow.models.param import Param")
        if use_bash:
            lines.append("from airflow.operators.bash import BashOperator")
        if use_external_sensor or upstream_groups:
            lines.append("from airflow.sensors.external_task import ExternalTaskSensor")
        if use_trigger_dag:
            lines.append("from airflow.operators.trigger_dagrun import TriggerDagRunOperator")
        if use_sensor or use_csv_sensor:
            lines.append("from airflow.sensors.base import BaseSensorOperator")
            lines.append("from airflow.utils.decorators import apply_defaults")
            lines.append("import subprocess")
        if multi_cron and is_entry:
            lines.append("from multi_cron_timetable import MultiCronTimetable")
        lines.append("from datetime import datetime, timedelta")
        if needs_searchpath:
            lines.append("import os")

        # ── INCLUDE_DIR ───────────────────────────────────────────────────────
        if needs_searchpath:
            lines.append("")
            lines.append("INCLUDE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'include')")

        # ── Sensor classes ────────────────────────────────────────────────────
        if use_sql:
            lines.append("")
            lines.append("")
            lines.append("class MultiStatementSQLExecuteQueryOperator(SQLExecuteQueryOperator):")
            lines.append('    """Split templated SQL into separate statements and strip trailing semicolons."""')
            lines.append("")
            lines.append("    def execute(self, context):")
            lines.append('        self.log.info(\"Executing: %s\", self.sql)')
            lines.append("        hook = self.get_db_hook()")
            lines.append("        if self.split_statements and isinstance(self.sql, str):")
            lines.append("            sql = [hook.strip_sql_string(stmt) for stmt in hook.split_sql_string(self.sql)]")
            lines.append("        else:")
            lines.append("            sql = self.sql")
            lines.append("        output = hook.run(")
            lines.append("            sql=sql,")
            lines.append("            autocommit=self.autocommit,")
            lines.append("            parameters=self.parameters,")
            lines.append("            handler=self.handler if self._should_run_output_processing() else None,")
            lines.append("            return_last=self.return_last,")
            lines.append("        )")
            lines.append("        if not self._should_run_output_processing():")
            lines.append("            return None")
            lines.append("        if return_single_query_results(sql, self.return_last, False):")
            lines.append("            return self._process_output([output], hook.descriptions)[-1]")
            lines.append("        return self._process_output(output, hook.descriptions)")

        if use_sensor or use_csv_sensor:
            lines.append("")
            lines.append("")
            lines.append("class HivePartitionSensor(BaseSensorOperator):")
            lines.append('    """Waits until an HDFS/Hive partition path exists."""')
            lines.append('    template_fields = ("hdfs_partition_path",)')
            lines.append("")
            lines.append("    @apply_defaults")
            lines.append("    def __init__(self, hdfs_partition_path: str, *args, **kwargs):")
            lines.append("        super().__init__(*args, **kwargs)")
            lines.append("        self.hdfs_partition_path = hdfs_partition_path")
            lines.append("")
            lines.append("    def poke(self, context):")
            lines.append('        self.log.info(f"Checking HDFS partition: {self.hdfs_partition_path}")')
            lines.append("        try:")
            lines.append('            kinit = subprocess.run(')
            lines.append('                ["kinit", "-kt", "/etc/keytabs/hdp-batch_user2.keytab", "hdp-batch_user2@OFFICE.CORP.INDOSAT.COM"],')
            lines.append('                capture_output=True')
            lines.append('            )')
            lines.append('            if kinit.returncode != 0:')
            lines.append('                raise RuntimeError(f"kinit failed: {kinit.stderr}")')
            lines.append('            result = subprocess.run(')
            lines.append('                ["hdfs", "dfs", "-test", "-e", self.hdfs_partition_path],')
            lines.append('                capture_output=True')
            lines.append('            )')
            lines.append("            exists = result.returncode == 0")
            lines.append('            self.log.info("Partition found." if exists else "Partition not ready yet...")')
            lines.append("            return exists")
            lines.append("        except Exception as e:")
            lines.append('            self.log.error(f"Error checking HDFS: {e}")')
            lines.append("            return False")

        if use_csv_sensor:
            lines.append("")
            lines.append("")
            lines.append("class CsvFileSensor(BaseSensorOperator):")
            lines.append('    """Waits until a file whose name contains any of the target date strings exists in an HDFS filepath."""')
            lines.append('    template_fields = ("filepath", "date_strings")')
            lines.append("")
            lines.append("    @apply_defaults")
            lines.append("    def __init__(self, filepath: str, date_strings: list, *args, **kwargs):")
            lines.append("        super().__init__(*args, **kwargs)")
            lines.append("        self.filepath = filepath")
            lines.append("        self.date_strings = date_strings")
            lines.append("")
            lines.append("    def poke(self, context):")
            lines.append('        self.log.info(f"Scanning HDFS path {self.filepath} for files matching any of: {self.date_strings}")')
            lines.append("        try:")
            lines.append('            kinit = subprocess.run(')
            lines.append('                ["kinit", "-kt", "/etc/keytabs/hdp-batch_user2.keytab", "hdp-batch_user2@OFFICE.CORP.INDOSAT.COM"],')
            lines.append('                capture_output=True')
            lines.append('            )')
            lines.append('            if kinit.returncode != 0:')
            lines.append('                raise RuntimeError(f"kinit failed: {kinit.stderr}")')
            lines.append('            result = subprocess.run(')
            lines.append('                ["hdfs", "dfs", "-ls", self.filepath],')
            lines.append('                capture_output=True, text=True')
            lines.append('            )')
            lines.append('            if result.returncode != 0:')
            lines.append('                self.log.info("HDFS path not accessible yet...")')
            lines.append('                return False')
            lines.append('            for date_str in self.date_strings:')
            lines.append('                matches = [line for line in result.stdout.splitlines() if date_str in line.split("/")[-1]]')
            lines.append('                if matches:')
            lines.append('                    self.log.info(f"Found match for {date_str}: {matches}")')
            lines.append('                    return True')
            lines.append('            self.log.info("No matching file found yet...")')
            lines.append('            return False')
            lines.append("        except Exception as e:")
            lines.append('            self.log.error(f"Error scanning HDFS path: {e}")')
            lines.append("            return False")

        if use_external_sensor:
            lines.append("")
            lines.append("")
            lines.append("class ManualSkippableExternalTaskSensor(ExternalTaskSensor):")
            lines.append('    """Skips waiting for upstream tasks when the DAG is manually triggered."""')
            lines.append("")
            lines.append("    def __init__(self, skip_on_manual_run: bool = False, *args, **kwargs):")
            lines.append("        super().__init__(*args, **kwargs)")
            lines.append("        self.skip_on_manual_run = skip_on_manual_run")
            lines.append("")
            lines.append("    def poke(self, context):")
            lines.append('        dag_run = context.get("dag_run")')
            lines.append('        run_type = str(getattr(dag_run, "run_type", "")).lower() if dag_run else ""')
            lines.append('        if self.skip_on_manual_run and run_type.endswith("manual"):')
            lines.append('            self.log.info("Manual DAG run detected, skipping external dependency wait.")')
            lines.append("            return True")
            lines.append("        return super().poke(context)")

        # ── Timetable ─────────────────────────────────────────────────────────
        if multi_cron and is_entry:
            lines.append("")
            lines.append(f'timetable = MultiCronTimetable(cron_defs={schedule}, timezone="{timezone}")')

        # ── DAG definition ────────────────────────────────────────────────────
        lines.append("")
        lines.append("with DAG(")
        lines.append(f'    dag_id="{dag_id}",')
        lines.append(f'    description="{description}",')
        if is_entry:
            if multi_cron:
                lines.append("    timetable=timetable,")
            elif schedule is None:
                lines.append('    schedule=None,')
            else:
                lines.append(f'    schedule="{schedule}",')
        else:
            lines.append('    schedule=None,')
        lines.append(f"    start_date=datetime({start_year}, {start_month}, {start_day}),")
        lines.append(f"    catchup={catchup},")
        lines.append(f"    max_active_runs=1,")
        lines.append(f"    tags={tags},")
        lines.append(f'    default_args={{"owner": "{owner}"}},')
        if date_params:
            lines.append("    params={")
            for key, meta in date_params.items():
                lines.append(
                    f'        "{key}": Param('
                    f'{build_default_date_expr(key, meta["days_ago"])}, '
                    f'type="string", '
                    f'pattern=r"{build_param_pattern(key)}"'
                    f'),'
                )
            lines.append("    },")
        if needs_searchpath:
            searchpaths = []
            if use_sql:
                searchpaths.append(f'os.path.join(INCLUDE_DIR, "sql", "{project}")' if project else 'os.path.join(INCLUDE_DIR, "sql")')
            if use_bash:
                searchpaths.append(f'os.path.join(INCLUDE_DIR, "scripts", "{project}")' if project else 'os.path.join(INCLUDE_DIR, "scripts")')
            if len(searchpaths) == 1:
                lines.append(f'    template_searchpath=[{searchpaths[0]}],')
            else:
                lines.append('    template_searchpath=[')
                for sp in searchpaths:
                    lines.append(f'        {sp},')
                lines.append('    ],')
        lines.append(") as dag:")

        sql_dir_expr = f'os.path.join(INCLUDE_DIR, "sql", "{project}")' if project else 'os.path.join(INCLUDE_DIR, "sql")'

        def _render(task, indent):
            if task["type"] == "hiveSensor":
                return render_hive_sensor_task(task, indent)
            if task["type"] == "csvSensor":
                return render_csv_sensor_task(task, indent)
            if task["type"] == "externalTaskSensor":
                return render_external_task_sensor_task(task, indent)
            if task["type"] == "triggerDagRun":
                return render_trigger_dag_task(task, indent, date_param_keys=list(date_params.keys()))
            return render_task(task, indent, sql_dir_expr=sql_dir_expr, date_param_keys=list(date_params.keys()))

        # ── ExternalTaskSensors for upstream groups ───────────────────────────
        ext_sensor_vars = {}   # upstream_group -> ext_sensor_var_name
        for ug in upstream_groups:
            _, anchor_task_id, upstream_dag_id = group_anchor[ug]
            ext_var = f"wait_for_{ug}"
            ext_sensor_vars[ug] = ext_var
            lines.append("")
            lines.append(f'    {ext_var} = ExternalTaskSensor(')
            lines.append(f'        task_id="wait_for_{ug}",')
            lines.append(f'        external_dag_id="{upstream_dag_id}",')
            lines.append(f'        external_task_id="{anchor_task_id}",')
            lines.append(f'        mode="reschedule",')
            lines.append(f'        poke_interval=60,')
            lines.append(f'        timeout=10800,')
            lines.append(f'        check_existence=True,')
            lines.append(f'        deferrable=True,')
            lines.append(f'    )')

        # ── Tasks ─────────────────────────────────────────────────────────────
        for task in gtasks:
            lines.append("")
            lines.append(_render(task, "    "))

        # ── Dependencies ──────────────────────────────────────────────────────
        dependencies = []

        # Wire ExternalTaskSensor → every task in this group that has NO
        # same-group upstream (i.e. the "entry" tasks of this group)
        if ext_sensor_vars:
            # find tasks whose depends_on are ALL cross-group or empty
            for task in gtasks:
                if task["type"] in ("hiveSensor", "csvSensor"):
                    continue  # sensors run independently
                same_group_deps = [
                    d for d in task.get("depends_on", [])
                    if next((t for t in all_tasks if t["name"] == os.path.splitext(d)[0]
                             and t.get("dag_group") == g), None)
                ]
                if not same_group_deps:
                    # this task has no same-group upstream → wire all ext sensors to it
                    task_var = task_var_lookup[task["name"]]
                    for ext_var in ext_sensor_vars.values():
                        dependencies.append(f"{ext_var} >> {task_var}")

        # Same-group depends_on wiring (skip cross-group deps — handled by ExternalTaskSensor)
        for task in gtasks:
            task_var = task_var_lookup[task["name"]]
            # sensor_vars prefix (waitForPartition on bash/sql)
            for s in task.get("_sensor_vars", []):
                dependencies.append(f"{s} >> {task_var}")
            # explicit depends_on — same group only
            for dep in task.get("depends_on", []):
                dep_name = os.path.splitext(dep)[0]
                dep_task = next((t for t in all_tasks if t["name"] == dep_name), None)
                if dep_task and dep_task.get("dag_group") == g:
                    dep_var = task_var_lookup.get(dep_name, to_var_name(dep_name))
                    dependencies.append(f"{dep_var} >> {task_var}")

        if dependencies:
            lines.append("")
            for dep in sorted(set(dependencies)):
                lines.append(f"    {dep}")

        lines.append("")
        output_filename = f"gen_{dag_id}.py"
        outputs[output_filename] = "\n".join(lines)

    return outputs



def main():
    if not os.path.isdir(CONFIG_DIR):
        print(f"Error: config directory not found at {CONFIG_DIR}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    yaml_files = sorted([
        os.path.join(root, f)
        for root, _, files in os.walk(CONFIG_DIR)
        for f in files
        if f.endswith(('.yaml', '.yml'))
    ])

    if not yaml_files:
        print("No YAML config files found in config/")
        sys.exit(0)

    generated = 0
    for filepath in yaml_files:
        rel_path = os.path.relpath(filepath, CONFIG_DIR)
        filename = os.path.basename(filepath)
        print(f"Processing: {rel_path}")

        with open(filepath, 'r', encoding='utf-8-sig') as f:
            config = yaml.safe_load(f)

        if not config:
            print(f"  Skipping {rel_path}: empty config")
            continue

        validate_config(config, rel_path)

        # Route: grouped (multi-DAG) vs single DAG
        has_etl = any(t["type"] == "etl" for t in config["tasks"])
        has_groups = any(t.get("dag_group") for t in config["tasks"])

        if has_etl:
            outputs = generate_etl_dags(config, filename)
            for out_filename, dag_code in outputs.items():
                output_file = os.path.join(OUTPUT_DIR, out_filename)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(dag_code)
                print(f"  Generated: {output_file}")
                generated += 1
        elif has_groups:
            outputs = generate_grouped_dags(config, filename)
            for out_filename, dag_code in outputs.items():
                output_file = os.path.join(OUTPUT_DIR, out_filename)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(dag_code)
                print(f"  Generated: {output_file}")
                generated += 1
        else:
            dag_code = generate_dag_code(config, filename)
            dag_id = config.get("dag_id", dag_id_from_name(name_from_filename(filename)))
            output_file = os.path.join(OUTPUT_DIR, f"gen_{dag_id}.py")
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(dag_code)
            print(f"  Generated: {output_file}")
            generated += 1


if __name__ == "__main__":
    main()
