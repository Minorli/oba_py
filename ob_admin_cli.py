#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OceanBase sys 租户 简易黑屏管理工具（JSON 外部菜单版）
依赖：
  pip install PyMySQL tabulate
"""

import os
import sys
import csv
import json
import time
import getpass
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

# --------- 依赖检测 ---------
try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:
    print("缺少依赖：请先安装 PyMySQL  =>  pip install PyMySQL")
    raise

try:
    from tabulate import tabulate
    HAVE_TABULATE = True
except Exception:
    HAVE_TABULATE = False


# --------- 控制台与输出工具 ---------
def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


def pause(msg: str = "按回车键继续..."):
    try:
        input(msg)
    except EOFError:
        pass


def prompt_with_default(prompt: str, default: str) -> str:
    s = input(f"{prompt} [{default}]: ").strip()
    return s if s else default


def adaptive_cell_width(headers: List[str], min_w: int = 24, max_w: int = 80) -> int:
    """根据终端宽度自适应单元格截断宽度的一个保守估算"""
    try:
        cols = os.get_terminal_size().columns
    except Exception:
        cols = 120
    # 预估每列宽度（含边框、空格），尽量别过窄也别过宽
    n = max(1, len(headers))
    # 预留索引与边框/间距
    usable = max(40, cols - 16)
    cell = max(min_w, min(max_w, usable // n))
    return cell


# --------- 表格美化输出 ---------
def print_table(rows: List[Dict[str, Any]], max_width: Optional[int] = None):
    """
    美化打印查询结果：
    - 去除换行/制表符与多空格
    - 截断过长字符串
    - tabulate 美观表格；无 tabulate 时纯文本回退
    """
    if not rows:
        print("(无结果)")
        return

    headers = list(rows[0].keys())
    if max_width is None:
        max_width = adaptive_cell_width(headers)

    def clean(val):
        if val is None:
            return ""
        s = str(val)
        # 清洗不可见字符/多空格
        s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        s = " ".join(s.split())
        # 尝试严格编码，捕获不可编码字符并回退
        try:
            s.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            s = s.encode("utf-8", errors="backslashreplace").decode("utf-8", errors="ignore")
        if len(s) > max_width:
            s = s[:max_width - 1] + "…"
        return s

    # 转矩阵（list of lists），配合 headers 列表
    rows_ll = [[clean(r.get(h)) for h in headers] for r in rows]

    if HAVE_TABULATE:
        print(tabulate(rows_ll, headers=headers, tablefmt="fancy_grid", showindex=True))
    else:
        # 纯文本回退
        col_w = [max(len(str(headers[i])), *(len(str(row[i])) for row in rows_ll)) for i in range(len(headers))]

        def line(sep_left="+", sep_mid="+", sep_right="+", fill="-"):
            return sep_left + sep_mid.join(fill * (w + 2) for w in col_w) + sep_right

        print(line())
        print("| " + " | ".join(str(h).ljust(col_w[i]) for i, h in enumerate(headers)) + " |")
        print(line())
        for row in rows_ll:
            print("| " + " | ".join(str(row[i]).ljust(col_w[i]) for i in range(len(headers))) + " |")
        print(line())


def save_csv(rows: List[Dict[str, Any]]):
    if not rows:
        print("无可保存的数据。"); time.sleep(1); return
    default_name = time.strftime("ob_query_%Y%m%d_%H%M%S.csv")
    path = prompt_with_default("保存为 CSV 文件名", default_name)
    headers = list(rows[0].keys())
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig 便于 Excel 识别
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for r in rows:
                writer.writerow({k: "" if v is None else v for k, v in r.items()})
        print(f"已保存：{os.path.abspath(path)}")
    except Exception as e:
        print("保存失败：", e)
    time.sleep(1.0)


def paginate(rows: List[Dict[str, Any]], page_size: int = 20):
    if not rows:
        print("(无结果)")
        return
    total = len(rows)
    page = 0
    while True:
        clear_screen()
        start = page * page_size
        end = min(start + page_size, total)
        print(f"显示结果 {start + 1} - {end} / {total}\n")
        print_table(rows[start:end])
        print("\n[n]下一页  [p]上一页  [s]保存CSV  [q]返回菜单")
        cmd = input("选择操作: ").strip().lower()
        if cmd == "n":
            if end >= total:
                print("已是最后一页。"); time.sleep(0.8)
            else:
                page += 1
        elif cmd == "p":
            if page == 0:
                print("已是第一页。"); time.sleep(0.8)
            else:
                page -= 1
        elif cmd == "s":
            save_csv(rows)
        elif cmd == "q":
            return
        else:
            print("无效指令。"); time.sleep(0.8)


# --------- JSON 外部菜单 ---------
def builtin_queries(dbname: str) -> List[Tuple[str, str]]:
    """回退菜单（当 JSON 不存在或读取失败时使用）"""
    db = dbname or "oceanbase"
    items = [
        ("查看 OceanBase 版本",               "SELECT VERSION() AS version;"),
        ("集群服务器列表（DBA_OB_SERVERS）", f"SELECT * FROM {db}.DBA_OB_SERVERS;"),
        ("租户列表（DBA_OB_TENANTS）",        f"SELECT tenant_id, tenant_name, compatibility_mode, status, locality FROM {db}.DBA_OB_TENANTS;"),
        ("Zone 列表（DBA_OB_ZONES）",        f"SELECT * FROM {db}.DBA_OB_ZONES;"),
        ("Unit 列表（DBA_OB_UNITS）",        f"SELECT * FROM {db}.DBA_OB_UNITS;"),
        ("参数（前200条）",                  f"SELECT * FROM {db}.GV$OB_PARAMETERS LIMIT 200;"),
        ("会话（processlist）",               "SHOW PROCESSLIST;"),
        ("数据库列表",                        "SHOW DATABASES;"),
        ("当前库的表",                        "SHOW TABLES;"),
        ("统计：各租户资源概览",              f"""
            SELECT t.tenant_id, t.tenant_name, u.unit_id, u.resource_pool_id, u.zone, u.max_cpu, u.max_memory
            FROM {db}.DBA_OB_TENANTS t
            LEFT JOIN {db}.DBA_OB_UNITS u USING(tenant_id)
            ORDER BY t.tenant_id, u.unit_id;
        """),
        ("统计：各服务器心跳/状态",            f"""
            SELECT svr_ip, svr_port, zone, status, start_service_time, stop_time
            FROM {db}.DBA_OB_SERVERS
            ORDER BY zone, svr_ip;
        """),
        ("自定义 SQL（手动输入）",            "__CUSTOM__"),
    ]
    return items


def load_menu_from_json(dbname: str, path: str = "queries.json") -> Optional[List[Tuple[str, str]]]:
    """
    从 queries.json 读取菜单：
    - JSON 结构：{"queries": [{"title": "...", "sql": "...", "enabled": true}, ...]}
    - 支持 {db} 占位符
    - 支持 enabled: false 隐藏某条
    - 成功返回列表[(title, sql)]；失败返回 None
    """
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"读取菜单配置失败：{e}，改用内置菜单。")
        return None

    qs = []
    for item in data.get("queries", []):
        if not item.get("enabled", True):
            continue
        title = str(item.get("title", "")).strip()
        sql = str(item.get("sql", "")).strip()
        if not title or not sql:
            continue
        if sql != "__CUSTOM__":
            sql = sql.replace("{db}", dbname or "oceanbase")
        qs.append((title, sql))

    # 如果配置里没放自定义项，就补一条
    if not any(sql == "__CUSTOM__" for _, sql in qs):
        qs.append(("自定义 SQL（手动输入）", "__CUSTOM__"))

    return qs or None


# --------- 数据库连接与执行 ---------
def connect_ob(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    connect_timeout: int = 10,
):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=connect_timeout,
        autocommit=True,
    )


def enforce_utf8(conn):
    """进一步确保连接层与结果集是 utf8mb4，避免输出问号"""
    try:
        with conn.cursor() as cur:
            cur.execute("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci;")
            cur.execute("SET character_set_results = utf8mb4;")
    except Exception as e:
        # 保守：失败不影响主流程
        print(f"[WARN] 设置 UTF-8 失败（可忽略）：{e}")


def run_query(conn, sql: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            rows = cur.fetchall()
        except pymysql.err.InterfaceError:
            rows = [{"affected_rows": cur.rowcount}]
    t1 = time.time()
    print(f"\n-- 执行耗时：{(t1 - t0):.3f}s  |  语句长度：{len(sql)}")
    return rows


# --------- 主流程 ---------
def main():
    # 强制 stdout 尽量用 UTF-8（Windows 旧终端下可缓解问号）
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    clear_screen()
    print("OceanBase sys 租户管理工具（JSON 外部菜单版）\n")

    # 默认连接参数（可按现场调整）
    default_host = "172.16.0.147"
    default_port = 2883
    default_cluster = "observer147"     # 集群名
    default_base_user = "root"          # 基础用户名
    default_password = "PAssw0rd01##"
    default_db = "oceanbase"            # 建议小写

    # 收集输入
    host = prompt_with_default("连接 IP", default_host)
    try:
        port = int(prompt_with_default("端口", str(default_port)))
    except ValueError:
        port = default_port
    cluster = prompt_with_default("集群名（cluster）", default_cluster)
    base_user = prompt_with_default("基础用户名（用于拼接 sys 租户）", default_base_user)

    auto_user = f"{base_user}@sys#{cluster}"
    user = prompt_with_default("用户名（可直接用自动拼接值）", auto_user)

    pwd_default_mask = "*" * min(6, len(default_password))
    pwd_input = getpass.getpass(f"密码 [{pwd_default_mask if default_password else ''}]: ").strip()
    password = pwd_input if pwd_input else default_password

    database = prompt_with_default("数据库名", default_db)

    # 连接
    print("\n正在连接到 OceanBase ...")
    try:
        conn = connect_ob(host, port, user, password, database)
    except Exception as e:
        print(f"连接失败：{e}")
        sys.exit(2)

    print("连接成功！")
    enforce_utf8(conn)
    time.sleep(0.5)

    # 载入菜单（优先 JSON）
    menu_path = prompt_with_default("菜单配置文件路径（默认 queries.json）", "queries.json")
    queries = load_menu_from_json(database, menu_path) or builtin_queries(database)

    # 主菜单循环
    while True:
        clear_screen()
        print("=== OceanBase 管理菜单 ===\n")
        for idx, (title, _) in enumerate(queries, start=1):
            print(f"{idx}. {title}")
        print("r. 重新加载菜单（读取最新 JSON）")
        print("0. 退出")
        choice = input("\n请选择序号: ").strip().lower()

        if choice == "0":
            print("再见！")
            try:
                conn.close()
            except Exception:
                pass
            break

        if choice == "r":
            # 重新加载 JSON
            queries_new = load_menu_from_json(database, menu_path)
            if queries_new:
                queries = queries_new
                print("菜单已重新加载。")
            else:
                print("重新加载失败或配置无变化，继续使用当前菜单。")
            time.sleep(1.0)
            continue

        try:
            idx = int(choice) - 1
            if idx < 0 or idx >= len(queries):
                raise ValueError
        except ValueError:
            print("无效选择。"); time.sleep(1.0); continue

        title, sql = queries[idx]
        if sql == "__CUSTOM__":
            print("\n请输入要执行的 SQL（以分号 ; 结尾，空行直接执行）：")
            lines = []
            while True:
                line = input()
                if not line.strip():
                    break
                lines.append(line)
                if line.strip().endswith(";"):
                    break
            sql = "\n".join(lines).strip()
            if not sql:
                print("未输入任何 SQL。"); time.sleep(1.0); continue

        # 可选：危险语句简单确认
        low = " " + sql.lower() + " "
        DANGER = (" drop ", " truncate ", " delete ", " alter ", " shutdown ", " kill ")
        if any(k in low for k in DANGER):
            ok = input("⚠️ 检测到可能的风险语句，确认执行？(yes/NO): ").strip().lower()
            if ok != "yes":
                print("已取消执行。"); time.sleep(1.0); continue

        # 执行
        try:
            rows = run_query(conn, sql)
            paginate(rows, page_size=20)
        except Exception as e:
            print("\n执行失败：", e)
            pause()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断，已退出。")
