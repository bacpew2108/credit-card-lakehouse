"""Bộ demo 8 kịch bản tính năng Lakehouse (mục 11.1 Design Doc), chạy trên máy host với stack đang chạy.

    make demo                         # kịch bản 1–7
    make demo S="2 5"                 # chỉ chạy kịch bản 2 và 5
    make demo S=8 CUSTOMER_ID=1510    # quyền xóa dữ liệu: XÓA THẬT khách hàng 1510, không hoàn tác được

  1  ACID, cô lập snapshot   kill driver Spark giữa lúc ghi lại fact trong một commit, Trino truy vấn liên tục
  2  Time travel, rollback   nạp nhầm tháng 2019-10 (append thay vì ghi đè), FOR VERSION AS OF, rollback_to_snapshot
  3  Schema evolution        thêm cột mcc_group, đổi tên cột, int → bigint trên bản sao mcc_codes
  4  Partition evolution     bản sao fact: partition theo tháng → theo năm, rồi nạp thêm 4 tháng
  5  Hidden partitioning     lọc theo transaction_ts: Trino chỉ đọc partition của tháng (EXPLAIN, số dòng đọc)
  6  Bảo trì bảng            59 batch nhỏ theo ngày → rewrite_data_files, rewrite_manifests, expire_snapshots
  7  Idempotency, tái lập    chạy lại dag_monthly tháng 2019-06; dag_rebuild cả năm 2018 (clean)
  8  Quyền xóa dữ liệu       make erase rồi thử time travel về snapshot trước lúc xóa

Mỗi kịch bản in bằng chứng (bảng số liệu) và các tiêu chí ĐẠT/KHÔNG ĐẠT tự kiểm tra. Kịch bản 3, 4, 6
làm trên bản sao trong namespace ops.demo (xóa và tạo lại mỗi lần chạy), để không đổi schema, layout
của bảng thật. Kịch bản 1, 2 ghi vào fact thật nhưng để lại đúng dữ liệu cũ (1 không commit, 2 rollback);
kịch bản 7 chạy pipeline thật. Kịch bản 1 dừng container spark-connect: không chạy khi Airflow đang có
DAG chạy (script kiểm tra trước). Trino dùng user engineer (đọc mọi catalog, ghi được ops).
"""

import argparse
import json
import shlex
import statistics
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone

from run_queries import print_table

COMPOSE = ["docker", "compose"]
SPARK_DEMO = [*COMPOSE, "exec", "-T", "airflow-scheduler", "python", "/opt/airflow/spark/demo/iceberg_demo.py"]
AIRFLOW = [*COMPOSE, "exec", "-T", "airflow-scheduler", "airflow"]
DAGS = ("dag_reference", "dag_monthly", "dag_rebuild", "dag_maintenance")
FACT = "gold.card.fact_transaction"
SCENARIOS = {}
# Kill tiến trình client của job demo trong airflow-scheduler (image không có pkill): sau khi driver chết,
# client Spark Connect thử kết nối lại vài phút chứ không thoát ngay
KILL_CLIENT = """
import os, signal
for p in os.listdir('/proc'):
    if p.isdigit() and int(p) != os.getpid():
        try:
            cmd = open(f'/proc/{p}/cmdline', 'rb').read()
        except OSError:
            continue
        if b'iceberg_demo.py' in cmd and b'gold-rewrite' in cmd:
            os.kill(int(p), signal.SIGKILL)
"""


def scenario(n, title):
    def wrap(fn):
        SCENARIOS[n] = (title, fn)
        return fn
    return wrap


def ts_range(first, last, col="transaction_ts", ntz=False):
    """Điều kiện [đầu tháng first, đầu tháng sau last) cho Trino (TIMESTAMP) hoặc Spark (TIMESTAMP_NTZ)."""
    a = date.fromisoformat(f"{first}-01")
    b = date.fromisoformat(f"{last}-01")
    b = date(b.year + b.month // 12, b.month % 12 + 1, 1)
    kw = "TIMESTAMP_NTZ" if ntz else "TIMESTAMP"
    return f"{col} >= {kw} '{a} 00:00:00' AND {col} < {kw} '{b} 00:00:00'"


def meta(table, kind):
    """gold.card.fact_transaction, files → gold.card."fact_transaction$files" (bảng metadata của Trino)."""
    c, s, t = table.split(".")
    return f'{c}.{s}."{t}${kind}"'


class Demo:
    def __init__(self, host, port):
        import trino
        self.trino_mod = trino
        self.host, self.port = host, port
        self.conn = self.connect()
        self.results = []      # (kịch bản, tiêu chí, đạt?, chi tiết)
        self.current = None

    def connect(self):
        return self.trino_mod.dbapi.connect(host=self.host, port=self.port, user="engineer", http_scheme="http")

    # ------------------------------------------------------------ Trino
    def q(self, sql, conn=None):
        cur = (conn or self.conn).cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return [d[0] for d in cur.description or []], [tuple(r) for r in rows], cur.stats

    def rows(self, sql):
        return self.q(sql)[1]

    def one(self, sql):
        return self.q(sql)[1][0]

    def run(self, *statements):
        for s in statements:
            self.q(s)

    def snapshot(self, table):
        return self.one(f"SELECT snapshot_id FROM {meta(table, 'refs')} WHERE name = 'main'")[0]

    def show(self, sql, limit=20):
        cols, rows, _ = self.q(sql)
        print_table(cols, rows, limit)

    # ------------------------------------------------------------ Spark, Airflow, MinIO
    def spark(self, *args, check=True):
        """Chạy spark/demo/iceberg_demo.py trong airflow-scheduler → danh sách object của các dòng [json]."""
        p = subprocess.run([*SPARK_DEMO, *args], capture_output=True, text=True)
        out = [json.loads(line[7:]) for line in p.stdout.splitlines() if line.startswith("[json] ")]
        if check and p.returncode:
            sys.stderr.write(p.stdout[-3000:] + p.stderr[-3000:])
            raise RuntimeError(f"iceberg_demo.py {args[0]} lỗi (mã {p.returncode})")
        return out

    def spark_sql(self, *statements):
        return self.spark("sql", *statements)

    def objects(self, table):
        return self.spark("objects", table)[0]

    def minio_count(self, prefix):
        """Số object dưới s3://bucket/prefix (đếm bằng mc trong container minio)."""
        path = prefix.removeprefix("s3://")
        cmd = ('mc alias set l http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; '
               f"mc ls --recursive l/{shlex.quote(path)} | wc -l")
        p = subprocess.run([*COMPOSE, "exec", "-T", "minio", "sh", "-c", cmd], capture_output=True, text=True)
        return int(p.stdout.strip() or 0)

    def airflow(self, *args):
        p = subprocess.run([*AIRFLOW, *args], capture_output=True, text=True)
        if p.returncode:
            raise RuntimeError(f"airflow {' '.join(args)}: {p.stderr.strip()[-500:]}")
        return p.stdout

    def busy_dags(self):
        busy = []
        for dag in DAGS:
            for state in ("running", "queued"):
                out = self.airflow("dags", "list-runs", dag, "--state", state, "-o", "json")
                busy += [f"{dag} {r['run_id']}" for r in json.loads(out[out.index("["):] if "[" in out else "[]")]
        return busy

    def trigger_and_wait(self, dag, conf, timeout=3600):
        run_id = f"demo__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
        self.airflow("dags", "unpause", dag)
        self.airflow("dags", "trigger", dag, "--run-id", run_id, "-c", json.dumps(conf))
        t0 = time.time()
        print(f"  {dag} {run_id} conf={json.dumps(conf)}: ", end="", flush=True)
        while time.time() - t0 < timeout:
            time.sleep(15)
            out = self.airflow("dags", "list-runs", dag, "-o", "json")
            state = next((r["state"] for r in json.loads(out[out.index("["):]) if r["run_id"] == run_id), None)
            print(".", end="", flush=True)
            if state in ("success", "failed"):
                print(f" {state}, {time.time() - t0:.0f}s")
                return state, time.time() - t0
        print(" hết thời gian chờ")
        return "timeout", time.time() - t0

    # ------------------------------------------------------------ báo cáo
    def heading(self, n, title, how):
        self.current = n
        print(f"\n{'=' * 100}\nKịch bản {n}. {title}\n  {how}\n{'=' * 100}")

    def step(self, text):
        print(f"\n-- {text}")

    def check(self, label, ok, detail=""):
        self.results.append((self.current, label, bool(ok), detail))
        print(f"  [{'ĐẠT' if ok else 'KHÔNG ĐẠT'}] {label}" + (f": {detail}" if detail else ""))


def fmt_n(v):
    return f"{v:,}" if isinstance(v, int) else str(v)


# ---------------------------------------------------------------- 1. ACID

@scenario(1, "ACID, cô lập snapshot")
def acid(d, args):
    d.heading(1, "ACID, cô lập snapshot",
              "Ghi lại toàn bộ fact (13,3 triệu dòng) trong MỘT commit bằng logic Gold, kill driver Spark "
              "(container spark-connect) khi file mới đã lên MinIO; Trino truy vấn fact liên tục suốt quá trình.")
    busy = d.busy_dags()
    if busy:
        d.check("Airflow không có DAG đang chạy", False, "; ".join(busy))
        return
    total_sql = f"SELECT count(*), sum(amount) FROM {FACT}"
    s0, base = d.snapshot(FACT), d.one(total_sql)
    o0 = d.objects(FACT)
    print(f"  trước: snapshot {s0}, {fmt_n(base[0])} dòng, tổng {base[1]}; MinIO {o0['data_objects']} file dữ liệu, "
          f"{o0['orphans']} file mồ côi")

    polls, stop, t_start = [], threading.Event(), time.time()

    def poll():
        conn = d.connect()
        while not stop.is_set():
            t = time.time() - t_start
            try:
                snap = d.q(f"SELECT snapshot_id FROM {meta(FACT, 'refs')} WHERE name = 'main'", conn)[1][0][0]
                n, amt = d.q(total_sql, conn)[1][0]
                polls.append((round(t, 1), str(snap), n, amt, None))
            except Exception as e:  # noqa: BLE001 – ghi lại, kịch bản đánh giá ở cuối
                polls.append((round(t, 1), None, None, None, str(e)[:80]))
            time.sleep(0.3)

    th = threading.Thread(target=poll, daemon=True)
    th.start()
    d.step("Job Gold ghi lại fact 2010-01 → 2019-10 trong một commit; chờ file mới xuất hiện trên MinIO rồi kill")
    job = subprocess.Popen([*SPARK_DEMO, "gold-rewrite", "--from", "2010-01", "--to", "2019-10"],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in job.stdout:
        if line.startswith("[demo]"):
            print(f"  {line.rstrip()}")
        if "bắt đầu ghi" in line:
            break
    t_write, data_dir, seen = time.time(), f"{o0['location']}/data/", o0["data_objects"]
    while time.time() - t_write < 300 and job.poll() is None:
        seen = d.minio_count(data_dir)
        if seen > o0["data_objects"]:
            break
        time.sleep(1)
    t_kill = time.time() - t_start
    subprocess.run(["docker", "kill", "spark-connect"], capture_output=True)
    print(f"  kill spark-connect ở giây {t_kill:.0f} (sau {time.time() - t_write:.0f}s ghi), MinIO lúc đó có "
          f"{seen} file dữ liệu (trước {o0['data_objects']})")
    time.sleep(6)
    stop.set()
    th.join()
    subprocess.run([*COMPOSE, "exec", "-T", "airflow-scheduler", "python", "-c", KILL_CLIENT], capture_output=True)
    job.communicate(timeout=60)
    code = job.returncode
    print(f"  tiến trình client của job (đang chờ driver đã chết) bị dừng, mã thoát {code}")
    d.step("Khởi động lại spark-connect")
    subprocess.run([*COMPOSE, "up", "-d", "--wait", "spark-connect"], capture_output=True)

    s1, after = d.snapshot(FACT), d.one(total_sql)
    o1 = d.objects(FACT)
    ok_polls = [p for p in polls if p[4] is None]
    span = polls[-1][0] if polls else 0
    print(f"\n  Trino truy vấn {len(polls)} lần trong {span}s ({len(polls) - len(ok_polls)} lỗi):")
    shown = polls[:3] + [("…",) * 5] + polls[-3:] if len(polls) > 6 else polls
    print_table(["giây", "snapshot", "số dòng", "tổng amount", "lỗi"], shown, 10)
    print(f"  sau: snapshot {s1}, {fmt_n(after[0])} dòng; MinIO {o1['data_objects']} file dữ liệu, "
          f"{o1['orphans']} file mồ côi ({o1['orphan_bytes'] / 2**20:.1f} MiB)")
    d.check("Trino luôn đọc được và luôn thấy snapshot cũ, số liệu nguyên vẹn",
            ok_polls and len(ok_polls) == len(polls)
            and all(p[1] == str(s0) and (p[2], p[3]) == base for p in ok_polls),
            f"{len(ok_polls)}/{len(polls)} lần đọc, {len({p[1] for p in ok_polls})} snapshot khác nhau")
    d.check("Job bị dừng không commit: bảng vẫn ở snapshot cũ", code != 0 and s1 == s0 and after == base,
            f"mã thoát {code}, snapshot {'không đổi' if s1 == s0 else 'ĐỔI'}")
    d.check("File đã ghi dở không thuộc snapshot nào (file mồ côi, remove_orphan_files dọn sau ≥ 1 ngày)",
            o1["orphans"] >= o0["orphans"],
            f"{o1['orphans'] - o0['orphans']} file mồ côi mới, {o1['current_files']} file của snapshot hiện tại")


# ---------------------------------------------------------------- 2. time travel, rollback

@scenario(2, "Time travel, rollback")
def time_travel(d, args):
    m = "2019-10"
    d.heading(2, "Time travel, rollback",
              f"Cố ý nạp nhầm tháng {m} vào fact: append thay vì ghi đè partition, mọi giao dịch của tháng bị nhân "
              "đôi. Truy vấn snapshot trước đó bằng FOR VERSION AS OF, rồi rollback_to_snapshot.")
    cond = ts_range(m, m)
    month_sql = f"SELECT count(*), sum(amount) FROM {FACT} WHERE {cond}"
    s0, base, base_all = d.snapshot(FACT), d.one(month_sql), d.one(f"SELECT count(*) FROM {FACT}")
    d.step(f"Nạp nhầm: INSERT INTO fact SELECT giao dịch {m} (Spark)")
    d.spark_sql(f"INSERT INTO {FACT} SELECT * FROM {FACT} WHERE {ts_range(m, m, ntz=True)}")
    s1, bad = d.snapshot(FACT), d.one(month_sql)
    old = d.one(f"SELECT count(*), sum(amount) FROM {FACT} FOR VERSION AS OF {s0} WHERE {cond}")
    d.show("SELECT made_current_at, CAST(snapshot_id AS varchar) AS snapshot_id, CAST(parent_id AS varchar) AS "
           f"parent_id, is_current_ancestor FROM {meta(FACT, 'history')} ORDER BY made_current_at DESC LIMIT 3")
    d.step("rollback_to_snapshot về snapshot trước lúc nạp nhầm (Spark)")
    d.spark_sql(f"CALL gold.system.rollback_to_snapshot('card.fact_transaction', {s0})")
    s2, after, after_all = d.snapshot(FACT), d.one(month_sql), d.one(f"SELECT count(*) FROM {FACT}")
    print_table(["thời điểm", "snapshot", f"số dòng {m}", f"tổng amount {m}"],
                [("trước khi nạp nhầm", str(s0), *base), ("sau khi nạp nhầm", str(s1), *bad),
                 (f"FOR VERSION AS OF {s0}", str(s0), *old), ("sau rollback", str(s2), *after)])
    d.check("Nạp nhầm làm số dòng và tổng tiền của tháng gấp đôi", bad == (2 * base[0], 2 * base[1]))
    d.check("FOR VERSION AS OF snapshot cũ trả đúng số liệu trước khi nạp nhầm", old == base)
    d.check("Sau rollback: snapshot hiện tại là snapshot cũ, số dòng trở về đúng", s2 == s0 and after == base
            and after_all == base_all, f"{fmt_n(after_all[0])} dòng toàn bảng")


# ---------------------------------------------------------------- 3. schema evolution

@scenario(3, "Schema evolution")
def schema_evolution(d, args):
    t = "ops.demo.mcc_codes"
    d.heading(3, "Schema evolution",
              f"Bản sao {t} (mcc_code int, description) do Trino tạo: thêm cột mcc_group, đổi tên description → "
              "mcc_description, mở rộng mcc_code int → bigint; nạp thêm một dòng theo schema mới.")
    d.run("CREATE SCHEMA IF NOT EXISTS ops.demo", f"DROP TABLE IF EXISTS {t}",
          f"CREATE TABLE {t} AS SELECT CAST(mcc_code AS integer) AS mcc_code, description FROM silver.card.mcc_codes")
    s0 = d.snapshot(t)
    files0 = {r[0] for r in d.rows(f"SELECT file_path FROM {meta(t, 'files')}")}
    old_sql = f"SELECT count(*), count(DISTINCT description), sum(mcc_code) FROM {t}"
    old = d.one(old_sql)
    d.step("ALTER TABLE (Trino): ADD COLUMN, RENAME COLUMN, SET DATA TYPE; rồi INSERT một dòng mã > 2^31")
    d.run(f"ALTER TABLE {t} ADD COLUMN mcc_group varchar",
          f"ALTER TABLE {t} RENAME COLUMN description TO mcc_description",
          f"ALTER TABLE {t} ALTER COLUMN mcc_code SET DATA TYPE bigint",
          f"INSERT INTO {t} VALUES (BIGINT '9999999999', 'Demo: mã vượt phạm vi int', 'Khác')")
    d.show(f"DESCRIBE {t}")
    files1 = {r[0] for r in d.rows(f"SELECT file_path FROM {meta(t, 'files')}")}
    cur = d.one(f"SELECT count(*), count(mcc_group), max(mcc_code) FROM {t}")
    tt = d.one(f"SELECT count(*), count(DISTINCT description), sum(mcc_code) FROM {t} FOR VERSION AS OF {s0}")
    same_rows = d.one(f"SELECT count(*), count(DISTINCT mcc_description), sum(mcc_code) FROM {t} "
                      "WHERE mcc_code < 2147483648")
    spark = d.spark_sql(f"SELECT mcc_code, mcc_description, mcc_group FROM {t} WHERE mcc_code > 2147483647",
                        f"SELECT count(*) FROM {t} WHERE mcc_group IS NULL")
    print_table(["truy vấn", "kết quả"], [
        ("trước khi đổi schema: count, distinct description, sum mcc_code", old),
        (f"FOR VERSION AS OF {s0} (schema cũ, cột description)", tt),
        ("schema mới, các dòng cũ (mcc_description)", same_rows),
        ("schema mới: count, count(mcc_group), max(mcc_code)", cur),
        ("Spark đọc bảng do Trino đổi schema: dòng mới", spark[0]["rows"]),
        ("Spark: số dòng mcc_group NULL", spark[1]["rows"][0][0])], 10)
    d.check("Đổi schema không ghi lại file dữ liệu: file cũ giữ nguyên, chỉ thêm file của dòng mới",
            files0 <= files1 and len(files1) == len(files0) + 1, f"{len(files0)} → {len(files1)} file")
    d.check("Truy vấn cũ vẫn chạy trên snapshot cũ; dòng cũ đọc theo tên mới cho cùng kết quả",
            tt == old and same_rows == old)
    d.check("Dòng cũ có mcc_group NULL, dòng mới giữ được giá trị bigint; Spark đọc đúng schema mới",
            cur == (old[0] + 1, 1, 9999999999) and spark[1]["rows"][0][0] == old[0]
            and spark[0]["rows"][0][0] == 9999999999)


# ---------------------------------------------------------------- 4. partition evolution

@scenario(4, "Partition evolution")
def partition_evolution(d, args):
    t = "ops.demo.fact_partition"
    d.heading(4, "Partition evolution",
              f"Bản sao {t} của fact 2018-01 → 2019-06 partition theo tháng; đổi partition sang năm, nạp thêm "
              "2019-07 → 2019-10. So từng tháng với fact thật, xem số dòng Trino đọc khi lọc một tháng.")
    d.run("CREATE SCHEMA IF NOT EXISTS ops.demo", f"DROP TABLE IF EXISTS {t}",
          f"CREATE TABLE {t} WITH (partitioning = ARRAY['month(transaction_ts)']) AS "
          f"SELECT * FROM {FACT} WHERE {ts_range('2018-01', '2019-06')}")
    d.step("ALTER TABLE … SET PROPERTIES partitioning = ARRAY['year(transaction_ts)'] rồi INSERT 4 tháng")
    d.run(f"ALTER TABLE {t} SET PROPERTIES partitioning = ARRAY['year(transaction_ts)']",
          f"INSERT INTO {t} SELECT * FROM {FACT} WHERE {ts_range('2019-07', '2019-10')}")
    d.show(f"SELECT spec_id, count(*) AS files, sum(record_count) AS records, min(partition) AS first_partition, "
           f"max(partition) AS last_partition FROM {meta(t, 'files')} GROUP BY spec_id ORDER BY spec_id")
    by_month = ("SELECT format_datetime(transaction_ts, 'yyyy-MM') AS m, count(*), sum(amount) "
                "FROM {} WHERE {} GROUP BY 1")
    demo = {r[0]: r[1:] for r in d.rows(by_month.format(t, ts_range("2018-01", "2019-10")))}
    real = {r[0]: r[1:] for r in d.rows(by_month.format(FACT, ts_range("2018-01", "2019-10")))}
    total = d.one(f"SELECT count(*) FROM {t}")[0]
    reads = []
    for m in ("2018-03", "2019-08"):
        _, rows, st = d.q(f"SELECT count(*), sum(amount) FROM {t} WHERE {ts_range(m, m)}")
        reads.append((m, "tháng (spec 0)" if m < "2019-07" else "năm (spec 1)", rows[0][0],
                      st["processedRows"], f"{st['physicalInputBytes'] / 2**20:.1f} MiB"))
    print_table(["lọc tháng", "partition của dữ liệu", "kết quả (dòng)", "Trino đọc (dòng)", "đọc (byte)"], reads)
    specs = {r[0] for r in d.rows(f"SELECT DISTINCT spec_id FROM {meta(t, 'files')}")}
    d.check("Dữ liệu cũ giữ layout cũ (partition tháng), dữ liệu mới theo partition năm", specs == {0, 1})
    d.check("Truy vấn trên bảng hai layout cho kết quả đúng (số dòng, tổng tiền từng tháng so với fact thật)",
            len(real) == 22 and demo == real, f"{sum(demo.get(k) == v for k, v in real.items())}/{len(real)} tháng")
    d.check("Lọc một tháng chỉ đọc partition liên quan ở cả hai layout",
            all(r[3] < total / 4 for r in reads), f"bảng {fmt_n(total)} dòng")


# ---------------------------------------------------------------- 5. hidden partitioning

@scenario(5, "Hidden partitioning")
def hidden_partitioning(d, args):
    d.heading(5, "Hidden partitioning",
              "fact partition theo months(transaction_ts) nhưng không có cột partition: truy vấn chỉ lọc "
              "transaction_ts. So với cùng điều kiện viết qua hàm (không suy ra được partition).")
    cols = [r[0] for r in d.rows(f"SHOW COLUMNS FROM {FACT}")]
    part = d.q(f"SELECT * FROM {meta(FACT, 'partitions')} LIMIT 1")[0]
    print(f"  cột của fact: {', '.join(cols)}\n  cột của bảng $partitions: {', '.join(part)}")
    queries = [
        ("lọc trực tiếp transaction_ts", f"WHERE {ts_range('2019-03', '2019-03')}"),
        ("lọc qua hàm date_format(transaction_ts)", "WHERE date_format(transaction_ts, '%Y-%m') = '2019-03'"),
        ("không lọc (quét toàn bảng)", ""),
    ]
    out = []
    for label, where in queries:
        sql = f"SELECT count(*), sum(amount) FROM {FACT} {where}"
        d.q(sql)                                   # làm nóng
        _, rows, st = d.q(sql)
        out.append((label, rows[0][0], rows[0][1], st["processedRows"],
                    f"{st['physicalInputBytes'] / 2**20:.1f} MiB", st["elapsedTimeMillis"]))
    print_table(["truy vấn", "số dòng", "tổng amount", "Trino đọc (dòng)", "đọc (byte)", "ms"], out)
    plan = d.rows(f"EXPLAIN SELECT count(*), sum(amount) FROM {FACT} {queries[0][1]}")[0][0]
    scan = next((ln.strip() for ln in plan.splitlines() if "TableScan" in ln or "ScanFilter" in ln), "")
    print(f"  EXPLAIN: {scan}")
    d.check("Không có cột partition trong schema của fact", not any("month" in c for c in cols))
    d.check("Cùng kết quả, lọc trực tiếp chỉ đọc dòng của partition 2019-03",
            out[0][1:3] == out[1][1:3] and out[0][3] == out[0][1] and out[1][3] == out[2][3],
            f"{fmt_n(out[0][3])} so với {fmt_n(out[1][3])} dòng")
    d.check("EXPLAIN cho thấy điều kiện được đẩy xuống lúc quét (constraint on transaction_ts)",
            "constraint on [transaction_ts]" in scan)


# ---------------------------------------------------------------- 6. bảo trì

@scenario(6, "Bảo trì bảng")
def maintenance(d, args):
    t, days = "ops.demo.fact_small_files", 59
    d.heading(6, "Bảo trì bảng",
              f"Bản sao {t} partition theo tháng, nạp {days} batch nhỏ (mỗi ngày 01/2019 – 02/2019 một INSERT) → "
              "rewrite_data_files, rewrite_manifests, expire_snapshots (Spark). So số file, snapshot, dung lượng.")
    d.run("CREATE SCHEMA IF NOT EXISTS ops.demo", f"DROP TABLE IF EXISTS {t}",
          f"CREATE TABLE {t} WITH (partitioning = ARRAY['month(transaction_ts)']) AS SELECT * FROM {FACT} WITH NO DATA")
    t0, day = time.time(), date(2019, 1, 1)
    for _ in range(days):
        nxt = day + timedelta(days=1)
        d.run(f"INSERT INTO {t} SELECT * FROM {FACT} WHERE transaction_ts >= TIMESTAMP '{day} 00:00:00' "
              f"AND transaction_ts < TIMESTAMP '{nxt} 00:00:00'")
        day = nxt
    print(f"  {days} INSERT, {time.time() - t0:.0f}s")
    check_sql = f"SELECT count(*), sum(amount) FROM {t}"

    def state(label):
        f = d.one(f"SELECT count(*), sum(file_size_in_bytes), avg(file_size_in_bytes) FROM {meta(t, 'files')}")
        mf = d.one(f"SELECT count(*) FROM {meta(t, 'manifests')}")[0]
        o = d.objects(t)
        times = []
        for _ in range(3):
            st = d.q(f"SELECT count(*), sum(amount), count(DISTINCT card_key) FROM {t}")[2]
            times.append(st["elapsedTimeMillis"])
        return (label, f[0], f"{f[2] / 2**10:.0f} KiB", mf, o["snapshots"], o["data_objects"],
                f"{o['bytes'] / 2**20:.1f} MiB", statistics.median(times)), d.one(check_sql)

    s_before, v0 = state(f"sau {days} batch")
    d.step("rewrite_data_files, rewrite_manifests (Spark)")
    r = d.spark_sql("CALL ops.system.rewrite_data_files(table => 'demo.fact_small_files')",
                    "CALL ops.system.rewrite_manifests(table => 'demo.fact_small_files')")
    print(f"  rewrite_data_files: {dict(zip(r[0]['columns'], r[0]['rows'][0]))}")
    s_compact, v1 = state("sau compaction")
    d.step("expire_snapshots: chỉ giữ snapshot hiện tại (Spark)")
    now = f"{datetime.now(timezone.utc) + timedelta(seconds=1):%Y-%m-%d %H:%M:%S}"
    r = d.spark_sql(f"CALL ops.system.expire_snapshots(table => 'demo.fact_small_files', "
                    f"older_than => TIMESTAMP '{now}', retain_last => 1)")
    print(f"  expire_snapshots: {dict(zip(r[0]['columns'], r[0]['rows'][0]))}")
    s_expire, v2 = state("sau expire_snapshots")
    print_table(["thời điểm", "file dữ liệu", "TB mỗi file", "manifest", "snapshot", "object dữ liệu trên MinIO",
                 "dung lượng thư mục bảng", "truy vấn (ms, trung vị 3 lần)"], [s_before, s_compact, s_expire])
    d.check("Compaction gộp file nhỏ: mỗi partition còn một file", s_compact[1] == 2 and s_before[1] >= days,
            f"{s_before[1]} → {s_compact[1]} file")
    d.check("Compaction không xóa file cũ (snapshot cũ còn dùng); expire_snapshots mới xóa",
            s_compact[5] > s_compact[1] and s_expire[5] == s_expire[1] and s_expire[4] == 1,
            f"object dữ liệu {s_before[5]} → {s_compact[5]} → {s_expire[5]}")
    d.check("Số liệu không đổi qua các bước bảo trì", v0 == v1 == v2, f"{fmt_n(v0[0])} dòng")


# ---------------------------------------------------------------- 7. idempotency, tái lập

def fingerprint(d, table, months, month_col="transaction_ts", skip=("_batch_id", "_updated_at", "_ingested_at",
                                                                    "_source")):
    """{tháng: (số dòng, tổng amount, checksum mọi cột nghiệp vụ)} của một bảng (Trino checksum: không phụ
    thuộc thứ tự dòng)."""
    c, s, t = table.split(".")
    cols = [r[0] for r in d.rows(f"SELECT column_name FROM {c}.information_schema.columns "
                                 f"WHERE table_schema = '{s}' AND table_name = '{t}' ORDER BY ordinal_position")
            if r[0] not in skip]
    amount = "sum(total_amount)" if "total_amount" in cols else "sum(amount)"
    rows_n = "sum(txn_count)" if "txn_count" in cols else "count(*)"
    if month_col == "month":
        m, where = "format_datetime(month, 'yyyy-MM')", (f"month >= DATE '{months[0]}-01' AND "
                                                          f"month <= DATE '{months[-1]}-01'")
    else:
        m, where = f"format_datetime({month_col}, 'yyyy-MM')", ts_range(months[0], months[-1], month_col)
    rows = d.rows(f"SELECT {m}, {rows_n}, {amount}, to_hex(checksum(row({', '.join(cols)}))) FROM {table} "
                  f"WHERE {where} GROUP BY 1")
    return {r[0]: r[1:] for r in rows}


def bronze_fingerprint(d, months):
    """Như fingerprint() cho Bronze: các cột đều là chuỗi, không có tổng tiền."""
    cols = ["id", "date", "client_id", "card_id", "amount", "use_chip", "merchant_id", "merchant_city",
            "merchant_state", "zip", "mcc", "errors"]
    ids = ", ".join(f"'{m}'" for m in months)
    rows = d.rows(f"SELECT _batch_id, count(*), NULL, to_hex(checksum(row({', '.join(cols)}))) "
                  f"FROM bronze.card.transactions_raw WHERE _batch_id IN ({ids}) GROUP BY 1")
    return {r[0]: r[1:] for r in rows}


LAYERS = [("bronze.card.transactions_raw", None), ("silver.card.transactions", "transaction_ts"),
          (FACT, "transaction_ts"), ("gold.card.mart_monthly_kpi", "month"), ("gold.card.mart_card_monthly", "month")]


def all_fingerprints(d, months):
    out = {}
    for table, col in LAYERS:
        out[table] = bronze_fingerprint(d, months) if col is None else fingerprint(d, table, months, col)
    return out


def compare_fingerprints(before, after, months):
    rows, same = [], True
    for table in before:
        ok = sum(before[table].get(m) == after[table].get(m) and m in before[table] for m in months)
        rows.append((table, f"{ok}/{len(months)}"))
        same &= ok == len(months)
    return rows, same


@scenario(7, "Idempotency, tái lập")
def idempotency(d, args):
    d.heading(7, "Idempotency, tái lập",
              "Chạy lại dag_monthly cho tháng 2019-06 (force: trích xuất lại từ nguồn); chạy dag_rebuild cho cả "
              "năm 2018 với clean (xóa Silver, fact, mart của năm rồi dựng lại từ Bronze). So số dòng, tổng tiền, "
              "checksum mọi cột của từng tầng trước và sau.")
    busy = d.busy_dags()
    if busy:
        d.check("Airflow không có DAG đang chạy", False, "; ".join(busy))
        return
    for label, dag, conf, months in (
            ("Chạy lại một tháng", "dag_monthly", {"month": "2019-06", "force": True}, ["2019-06"]),
            ("dag_rebuild cả năm 2018", "dag_rebuild",
             {"start_month": "2018-01", "end_month": "2018-12", "clean": True, "extract": False},
             [f"2018-{i:02d}" for i in range(1, 13)])):
        d.step(label)
        snaps0 = {t: d.snapshot(t) for t, _ in LAYERS}
        before = all_fingerprints(d, months)
        state, secs = d.trigger_and_wait(dag, conf)
        after = all_fingerprints(d, months)
        snaps1 = {t: d.snapshot(t) for t, _ in LAYERS}
        rows, same = compare_fingerprints(before, after, months)
        print_table(["bảng", "tháng trùng (số dòng, tổng tiền, checksum)", "snapshot"],
                    [(t, r, "snapshot mới" if snaps0[t] != snaps1[t] else "snapshot không đổi")
                     for (t, r) in rows])
        if len(months) == 1:
            m = months[0]
            print_table(["bảng", "trước", "sau"], [(t, before[t].get(m), after[t].get(m)) for t in before], 10)
        d.check(f"{label}: DAG chạy thành công", state == "success", f"{secs:.0f}s")
        d.check(f"{label}: số dòng, tổng tiền, checksum mọi tầng không đổi", same)
        rewritten = [t for t in snaps0 if snaps0[t] != snaps1[t] and t != "bronze.card.transactions_raw"]
        d.check(f"{label}: dữ liệu thực sự được ghi lại (snapshot mới) mà kết quả trùng khớp",
                len(rewritten) >= 3, f"{len(rewritten)} bảng có snapshot mới")


# ---------------------------------------------------------------- 8. quyền xóa dữ liệu

@scenario(8, "Quyền xóa dữ liệu")
def erasure(d, args):
    cid = args.customer_id
    d.heading(8, "Quyền xóa dữ liệu",
              f"make erase CUSTOMER_ID={cid}: xóa khách hàng và thẻ ở Bronze/Silver/Gold, giao dịch về khóa −2, "
              "dựng lại mart, expire_snapshots; sau đó thử time travel về snapshot trước lúc xóa.")
    if not cid:
        d.check("Có CUSTOMER_ID", False, "kịch bản 8 xóa thật một khách hàng: make demo S=8 CUSTOMER_ID=…")
        return
    busy = d.busy_dags()
    if busy:
        d.check("Airflow không có DAG đang chạy", False, "; ".join(busy))
        return
    who = (f"SELECT (SELECT count(*) FROM bronze.card.customers_raw WHERE id = '{cid}'), "
           f"(SELECT count(*) FROM silver.card.customers WHERE customer_id = {cid}), "
           f"(SELECT count(*) FROM silver.card.cards WHERE customer_id = {cid}), "
           f"(SELECT count(*) FROM silver.card.transactions WHERE customer_id = {cid}), "
           f"(SELECT count(*) FROM gold.card.dim_customer WHERE customer_id = {cid})")
    totals = f"SELECT count(*), sum(amount) FROM {FACT}"
    s0 = d.snapshot("silver.card.customers")
    before, tot0 = d.one(who), d.one(totals)
    tt0 = d.one(f"SELECT count(*) FROM silver.card.customers FOR VERSION AS OF {s0} WHERE customer_id = {cid}")
    d.step(f"make erase CUSTOMER_ID={cid}")
    p = subprocess.run([*COMPOSE, "exec", "-T", "airflow-scheduler", "python",
                        "/opt/airflow/spark/maintenance/erase_customer.py", "--customer-id", str(cid)],
                       capture_output=True, text=True)
    for line in p.stdout.splitlines():
        if line.startswith("[erase]"):
            print(f"  {line}")
    lines = p.stdout.strip().splitlines()
    result = json.loads(lines[-1]) if lines and lines[-1].startswith("{") else {}
    after, tot1 = d.one(who), d.one(totals)
    try:
        d.one(f"SELECT count(*) FROM silver.card.customers FOR VERSION AS OF {s0} WHERE customer_id = {cid}")
        tt1 = "đọc được"
    except d.trino_mod.exceptions.TrinoUserError as e:
        tt1 = e.message.split("\n")[0][:90]
    deleted = d.one("SELECT sum(txn_count), sum(total_amount) FROM gold.card.mart_card_monthly WHERE card_key = -2")
    print_table(["", "bronze customers", "silver customers", "silver cards", "silver giao dịch", "dim_customer"],
                [("trước", *before), ("sau", *after)])
    print_table(["", "fact: số dòng", "tổng amount", f"time travel snapshot {s0}"],
                [("trước", *tot0, f"{tt0[0]} dòng của khách"), ("sau", *tot1, tt1)])
    print(f"  mart_card_monthly, thẻ Deleted (−2): {deleted[0]} giao dịch, {deleted[1]}")
    d.check("erase_customer.py báo DONE", p.returncode == 0 and result.get("status") == "DONE",
            f"{result.get('old_files_gone')}/{result.get('old_files')} file cũ đã xóa khỏi MinIO")
    d.check("Không còn dòng nào của khách hàng ở Bronze, Silver, Gold", before[0] and not any(after))
    d.check("Số liệu tổng hợp không đổi: số dòng, tổng tiền của fact", tot0 == tot1)
    d.check("Time travel về snapshot trước lúc xóa không còn được", tt0[0] == 1 and tt1 != "đọc được", tt1)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*", type=int, help="số kịch bản 1–8 (mặc định 1–7)")
    ap.add_argument("--customer-id", type=int, help="kịch bản 8: khách hàng sẽ bị xóa thật")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8082)
    args = ap.parse_args()
    chosen = args.scenarios or [n for n in SCENARIOS if n != 8]
    if any(n not in SCENARIOS for n in chosen):
        ap.error("kịch bản phải trong 1–8")
    d, t0 = Demo(args.host, args.port), time.time()
    for n in chosen:
        t1 = time.time()
        try:
            SCENARIOS[n][1](d, args)
        except Exception as e:  # noqa: BLE001 – một kịch bản lỗi không dừng các kịch bản còn lại
            d.current = n
            d.check("Kịch bản chạy hết", False, f"{type(e).__name__}: {str(e).splitlines()[0][:150]}")
        print(f"  ({time.time() - t1:.0f}s)")
    print(f"\n{'=' * 100}\nTổng kết ({time.time() - t0:.0f}s)")
    rows = []
    for n in chosen:
        res = [r for r in d.results if r[0] == n]
        rows.append((n, SCENARIOS[n][0], f"{sum(r[2] for r in res)}/{len(res)}",
                     "ĐẠT" if res and all(r[2] for r in res) else "KHÔNG ĐẠT"))
    print_table(["#", "kịch bản", "tiêu chí đạt", "kết quả"], rows)
    return 0 if all(r[2] for r in d.results) else 1


if __name__ == "__main__":
    sys.exit(main())
