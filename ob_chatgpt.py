import mysql.connector
import getpass
import sys
import json
import os
import shutil  # 用于获取终端宽度
from tabulate import tabulate

# 配置文件名
CONFIG_FILE = 'queries.json'

def load_queries():
    """
    从 JSON 文件加载预设查询。
    """
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ 错误: 找不到配置文件 '{CONFIG_FILE}'")
        print("请在同一目录下创建 queries.json 文件。")
        return {}
        
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"❌ 错误: 配置文件 '{CONFIG_FILE}' 格式无效 (不是合法的 JSON)。")
        return {}
    except Exception as e:
        print(f"❌ 错误: 读取配置文件时发生未知错误: {e}")
        return {}


def get_connection_details():
    """
    通过交互式输入获取连接 OceanBase 的所有必需信息。
    """
    print("--- OceanBase '摸鱼' 管理工具 (配置版) ---")
    print("请输入连接信息 (直接回车使用默认值):\n")

    host = input("  主机 IP [默认: 172.16.0.146]: ") or "172.16.0.146"
    
    port = input("  端 口 [默认: 2883]: ") or 2883
    try:
        port = int(port)
    except ValueError:
        print("无效端口! 使用默认 2883。")
        port = 2883

    cluster_name = input("  集群名 [默认: observer147]: ") or "observer147"
    user_part = input(f"  用户名 [默认: root@sys]: ") or "root@sys"
    user = f"{user_part}#{cluster_name}"

    print("  密 码 [默认: PAssw0rd01##]: ")
    password = getpass.getpass("  > ") or "PAssw0rd01##"
    
    database = input("  数据库 [默认: oceanbase]: ") or "oceanbase"

    print("\n" + "="*30)
    print("连接详情 (用于 obclient):")
    # 出于安全考虑，密码在确认信息中隐藏
    print(f"obclient -h{host} -P{port} -u'{user}' -p'******' -D{database}")
    print("="*30 + "\n")

    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'database': database
    }

def connect_to_oceanbase(conn_details):
    """
    (已更新) 使用提供的详情连接到 OceanBase，并打印准确的版本号。
    """
    try:
        print("正在连接到 OceanBase...")
        conn = mysql.connector.connect(
            **conn_details,
            connect_timeout=5 # 5秒超时
        )
        if conn.is_connected():
            print("✅ 连接成功！")
            
            # --- ★★★ 新增需求：连接成功后自动查询并打印版本信息 ★★★ ---
            cursor = None
            try:
                # 1. 创建一个临时游标
                cursor = conn.cursor()
                
                # 2. 执行你指定的版本查询 SQL
                sql_version = "select ob_version()"
                cursor.execute(sql_version)
                
                # 3. 获取结果 (应该只有一行)
                result = cursor.fetchone()
                
                if result:
                    # 4. 打印格式化的版本信息
                    print(f"  -> OB 版本: {result[0]}")
                
            except mysql.connector.Error as err:
                # 如果这个特定查询失败了(比如权限不够)，只打印警告，不中断程序
                print(f"  -> (无法自动获取 ob_version: {err})")
            finally:
                if cursor:
                    # 5. 关闭游标
                    cursor.close()
            # --- ★★★ 新增需求结束 ★★★ ---
            
            return conn
            
    except mysql.connector.Error as err:
        print(f"❌ 连接失败: {err}")
        return None

# --- ★★★ 核心美化逻辑 ★★★ ---

def print_vertical(results, headers):
    """
    垂直打印 (\\G 风格)，用于混乱的数据。
    """
    print("") # 额外加个换行
    for i, row in enumerate(results):
        print(f"***************************[ Row {i + 1} ]***************************")
        try:
            # 计算最长的标题，用于对齐
            max_header_len = max(len(str(h)) for h in headers)
        except ValueError:
            max_header_len = 0
        
        for j, header in enumerate(headers):
            # 右对齐标题，使其美观
            print(f"{str(header).rjust(max_header_len)} : {row[j]}")
    
    print(f"\n({len(results)} row(s) returned)\n")

def print_horizontal(results, headers):
    """
    (已修复) 美化水平打印，用于简单的数据。
    """
    # 1. 清理数据：将数据中的 \n \r \t 替换为空格，防止破坏表格
    cleaned_results = []
    for row in results:
        # 将 row 转换为 list 以便修改
        cleaned_row = list(row)
        for i in range(len(cleaned_row)):
            item = cleaned_row[i]
            # 仅当
            if isinstance(item, (str, bytes)):
                # 强制转换为 str 并清理
                cleaned_item = str(item).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                cleaned_row[i] = cleaned_item
        cleaned_results.append(tuple(cleaned_row))

    # 2. 动态计算列宽
    try:
        # 获取终端的总宽度
        terminal_width = shutil.get_terminal_size().columns
    except OSError:
        terminal_width = 80 # 如果不在 TTY 中（如管道），给一个默认值

    num_columns = len(headers)
    
    # 启发式计算：(总宽度 - 边框) / 列数
    # 我们给每个'|'和空格 3 个字符的余量
    available_width = terminal_width - (num_columns * 3) - 1
    
    # 每列至少 10 字符，但不超过 40 (防止一列过宽挤压其他列)
    max_col_width = min(40, max(10, available_width // num_columns))
    
    # `maxcolwidths` (复数) 需要一个列表
    max_col_widths_list = [max_col_width] * num_columns

    # 3. 打印
    try:
        print(tabulate(cleaned_results, headers=headers, 
                       
                       # 使用 'grid' 格式。它使用纯 ASCII (-, +, |)
                       # 来绘制完整网格，100% 兼容所有终端。
                       tablefmt="grid",

                       maxcolwidths=max_col_widths_list
                      ))
        print(f"\n({len(results)} row(s) returned)\n")
    except Exception as e:
        print(f"❌ [Tabulate 渲染错误]: {e}")
        print("... 渲染失败，尝试降级到垂直输出 ...")
        print_vertical(results, headers) # 如果 tabulate 失败，则降级

def execute_and_print_query(connection, sql_query):
    """
    执行查询，并智能选择美化方案。
    """
    if not sql_query or not sql_query.strip():
        print("SQL 不能为空。")
        return

    cursor = None
    try:
        cursor = connection.cursor()
        print(f"\n--- [执行]: {sql_query} ---")
        
        cursor.execute(sql_query)
        
        if cursor.description:
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            
            if not results:
                print("\n(查询成功，结果为空)\n")
                return

            # --- ★ 智能决策 ★ ---
            
            # 决策1：列是否太多？
            MAX_HORIZONTAL_COLS = 10 
            is_too_wide = len(headers) > MAX_HORIZONTAL_COLS
            
            # 决策2：数据是否包含换行符？
            has_newlines = False
            if not is_too_wide: 
                for row in results:
                    if any(isinstance(cell, str) and '\n' in cell for cell in row):
                        has_newlines = True
                        break
            
            # --- 智能路由 ---
            if is_too_wide or has_newlines:
                if is_too_wide:
                    print("\n(结果集列数过多，自动切换到垂直格式...)")
                if has_newlines:
                    print("\n(结果集包含换行符，自动切换到垂直格式...)")
                
                print_vertical(results, headers)
            else:
                print_horizontal(results, headers)
            # --- ★ 决策结束 ★ ---

        else:
            # 比如 'UPDATE', 'INSERT', 'SET'
            print(f"\n(命令执行成功，影响行数: {cursor.rowcount})\n")
            
    except mysql.connector.Error as err:
        print(f"\n❌ [SQL 错误]: {err}\n")
    except Exception as e:
        print(f"\n❌ [程序错误]: {e}\n")
    finally:
        if cursor:
            cursor.close()

# --- ★★★ 美化逻辑结束 ★★★ ---


def show_main_menu(connection):
    """
    显示主菜单，循环等待用户输入。
    """
    while True:
        print("\n--- OB '摸鱼' 菜单 (动态加载) ---")
        
        # 1. 每次循环都重新加载配置文件 (实现“刷新”需求)
        queries_menu = load_queries()
        
        if not queries_menu:
            print("菜单为空，请检查 'queries.json' 文件。")
        
        # 2. 动态打印菜单
        # 按 key 排序，确保 '1', '2', '10' 顺序正确
        try:
            sorted_keys = sorted(queries_menu.keys(), key=lambda x: int(x))
        except ValueError:
            # 如果 key 不是数字 (比如 'a', 'b'), 则按字符串排序
            sorted_keys = sorted(queries_menu.keys())
            
        for key in sorted_keys:
            print(f"  {key}. {queries_menu[key].get('title', '无标题')}")
        
        print("\n  C. [自定义 SQL] (输入 'C' 或 'c')")
        print("  R. [刷新配置] (输入 'R' 或 'r' - 已自动刷新)")
        print("  Q. [退出] (输入 'Q' 或 'q')")
        
        choice = input("\n请选择: ").strip().upper()

        if choice == 'Q':
            print("摸鱼结束，再见！")
            break
        
        elif choice == 'R':
            # 只是一个安慰剂，因为循环已经自动刷新了
            print("\n... 配置文件已在显示菜单时自动刷新 ...\n")
            continue

        elif choice == 'C':
            print("\n--- 自定义 SQL ---")
            print("请输入你的 SQL (单行, ; 可选):")
            custom_sql = input("  OceanBase SQL> ")
            execute_and_print_query(connection, custom_sql)
            
        elif choice in queries_menu:
            # 3. 动态处理选择
            try:
                item = queries_menu[choice]
                query_type = item.get('type', 'simple') # 默认为 'simple'

                if query_type == 'simple':
                    sql = item['sql']
                    execute_and_print_query(connection, sql)
                
                elif query_type == 'parameter_query':
                    print(f"\n--- {item['title']} ---")
                    query = input("请输入查询参数 (例如: %timeout%): ")
                    if not query:
                        query = '%' # 默认为 %
                    # 使用模板替换
                    sql = item['sql_template'].format(query=query)
                    execute_and_print_query(connection, sql)
                
                else:
                    print(f"❌ 错误: 'queries.json' 中 '{choice}' 的类型 '{query_type}' 未知。")

            except KeyError:
                print(f"❌ 错误: 'queries.json' 中 '{choice}' 的配置不完整。")
            except Exception as e:
                print(f"❌ 错误: 处理菜单时发生意外: {e}")
            
        else:
            print(f"无效输入 '{choice}'，请重新选择。")

def main():
    conn_details = get_connection_details()
    connection = connect_to_oceanbase(conn_details)

    if connection:
        show_main_menu(connection)
        connection.close()
    else:
        print("无法建立连接，程序退出。")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n检测到 Ctrl+C，强制退出。")
        sys.exit(0)
