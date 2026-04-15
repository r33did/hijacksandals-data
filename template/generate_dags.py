import os
import re
import sys
import yaml
from collections import OrderedDict
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
CONFIG_DIR = os.path.join(ROOT_DIR, "config")
OUTPUT_DIR = os.path.join(ROOT_DIR, "dags")

DEFAULTS = {
    "conn_id": "ioh-hive",
    "owner": "IOH - DE",
    "timezone": "Asia/Jakarta",
    "start_date": "2026-01-01",
    "catchup": False,
}

REQUIRED_FIELDS = ["schedule", "tasks"]

# Recognised partition date keys and their strftime format
PARTITION_DATE_KEYS = {
    "dt_id":  "%Y%m%d",
    "mth_id": "%Y%m",
    "prc_dt": "%Y%m%d",
    "TRANSACTION_DATE": "%Y%m%d",
    "transactiondate": "%Y%m%d",
    "process_dt": "%Y%m%d",
    "load_dt": "%Y%m%d",
    "prt_dt": "%Y%m%d",
    "prcdt": "%Y%m%d"
}


def validate_config(config, filepath):
    missing = [f for f in REQUIRED_FIELDS if f not in config]
    if missing:
        raise ValueError(f"Config {filepath} missing required fields: {missing}")
    for i, task in enumerate(config["tasks"]):
        if "name" not in task:
            raise ValueError(f"Config {filepath} task #{i} missing 'name' field")
        if "type" not in task:
            raise ValueError(f"Config {filepath} task '{task.get('name')}' missing 'type' field (sql or bash)")
        if task["type"] not in ("sql", "bash", "hiveSensor", "csvSensor"):
            raise ValueError(f"Config {filepath} task '{task['name']}' has invalid type '{task['type']}' (must be sql, bash, hiveSensor, or csvSensor)")
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


def description_from_name(name):
    title = re.sub(r'[-_]+', ' ', name).title()
    return f"Pipeline {title}"


def parse_start_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day




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
            fmt = PARTITION_DATE_KEYS[part]
            days = dateranges.get(part, 1)
            segments.append(f"{part}={{(datetime.today() - timedelta(days={days})).strftime('{fmt}')}}")
        else:
            raise ValueError(
                f"Partition key '{part}' is not recognised. "
                f"Use 'key=value' for static values, or one of the date keys: {list(PARTITION_DATE_KEYS.keys())}"
            )

    path = base + "/" + "/".join(segments)
    return f'f"{path}"'



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
    lines.append(f'{indent})')

    task["_csv_sensor_var"] = sensor_name
    task["_sensor_vars"] = []
    return '\n'.join(lines)

def render_task(task, indent, sql_dir_expr=None):
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
            lines.append(f'{indent}    bash_command="{task["command"]}",')
        else:
            lines.append(f'{indent}    bash_command="{name}.sh",')
        if sql_dir_expr:
            lines.append(
                f'{indent}    params={{'
                f'"sql_dir": {sql_dir_expr}, '
                f'"sql_file": "{name}.sql"'
                f'}},'
            )
        lines.append(f'{indent})')
    else:
        lines.append(f'{indent}{var} = SQLExecuteQueryOperator(')
        lines.append(f'{indent}    task_id="{name}",')
        lines.append(f'{indent}    conn_id="{task["_conn_id"]}",')
        if "query" in task:
            sql_val = task["query"].replace('"', '\\"')
            lines.append(f'{indent}    sql="{sql_val}",')
        else:
            lines.append(f'{indent}    sql="{name}.sql",')
        if "database" in task:
            lines.append(f'{indent}    hook_params={{"schema": "{task["database"]}"}},')
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
    use_sensor     = any(t["type"] == "hiveSensor" or t.get("waitForPartition") for t in tasks)
    use_csv_sensor = any(t["type"] == "csvSensor" for t in tasks)
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
    if use_bash:
        lines.append("from airflow.operators.bash import BashOperator")
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

    # HivePartitionSensor class (injected once if any task uses waitForPartition)
    if use_sensor or use_csv_sensor:
        lines.append("")
        lines.append("")
        lines.append("class HivePartitionSensor(BaseSensorOperator):")
        lines.append('    """Waits until an HDFS/Hive partition path exists."""')
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
    else:
        lines.append(f'    schedule="{schedule}",')
    lines.append(f"    start_date=datetime({start_year}, {start_month}, {start_day}),")
    lines.append(f"    catchup={catchup},")
    lines.append(f"    tags={tags},")
    lines.append(f'    default_args={{"owner": "{owner}"}},')
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
        return render_task(task, indent, sql_dir_expr=sql_dir_expr)

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

        with open(filepath, 'r') as f:
            config = yaml.safe_load(f)

        if not config:
            print(f"  Skipping {rel_path}: empty config")
            continue

        validate_config(config, rel_path)

        dag_code = generate_dag_code(config, filename)
        dag_id = config.get("dag_id", dag_id_from_name(name_from_filename(filename)))
        output_file = os.path.join(OUTPUT_DIR, f"gen_{dag_id}.py")

        with open(output_file, 'w') as f:
            f.write(dag_code)

        print(f"  Generated: {output_file}")
        generated += 1


if __name__ == "__main__":
    main()