import os

import pandas as pd
from sqlalchemy import create_engine, inspect, bindparam, text

from models.table_mapping import get_key_value_map_from_mysql


def delete_records(connection, table_name, primary_column, ids_to_update):
    if not ids_to_update.all():
        return 0

    if isinstance(ids_to_update, (int, str)):
        # 单个id
        query = text(f"DELETE FROM {table_name} WHERE `{primary_column}` = :id")
        connection.execute(query, {"id": ids_to_update})
    else:
        # 多个id
        ids_tuple = tuple(ids_to_update)
        query = text(f"DELETE FROM {table_name} WHERE `{primary_column}` IN :ids")
        query = query.bindparams(bindparam("ids", expanding=True))
        connection.execute(query, {"ids": tuple(ids_tuple)})

    return len(ids_to_update)


def read_file(file_path):
    # 判断文件类型是否为csv
    if file_path.endswith('.csv'):
        # 如果是csv文件，则使用pandas的read_csv函数读取文件
        return pd.read_csv(file_path)
    # 判断文件类型是否为xlsx或xls
    elif file_path.endswith('.xlsx'):
        # 如果是xlsx或xls文件，则使用pandas的ExcelFile函数读取文件
        xls = pd.ExcelFile(file_path)
        # 遍历文件中的所有sheet
        for sheet_name in xls.sheet_names:
            # 使用pandas的read_excel函数读取每个sheet
            return pd.read_excel(file_path, sheet_name, engine='openpyxl')
    elif file_path.endswith('.xls'):
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            return pd.read_excel(file_path, sheet_name, engine='xlrd')
    else:
        # 如果文件类型不是csv、xlsx或xls，则抛出异常
        return ValueError(f"不支持的文件类型: {file_path}")


def xlsx_to_database(folder_path, db_connection_string, file_patterns, primary_column_map, update_status: int):
    """
    读取指定文件夹下的xlsx文件并根据文件名模式将数据写入不同的数据库表
    参数:
        folder_path (str): 包含xlsx文件的文件夹路径
        db_connection_string (str): 数据库连接字符串
        file_patterns (dict): 文件名模式与表名的映射字典
                            格式: {'文件名关键词': '表名'}
                            示例: {'sales': 'sales_data', 'inventory': 'inventory_data'}
    """
    # 创建数据库引擎
    engine = create_engine(db_connection_string)

    # 获取文件夹中所有xlsx文件
    if not os.path.exists(folder_path):
        print(f"文件夹 {folder_path} 不存在")

    files = [f for f in os.listdir(folder_path) if
             (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv'))]

    if not files:
        print(f"在文件夹 {folder_path} 中没有找到任何.xlsx或.xls或.csv文件")
        return

    print(f"找到 {len(files)} 个文件:")
    for file in files:
        print(f"- {file}")

    # 处理每个xlsx文件
    for file in files:
        file_path = os.path.join(folder_path, file)
        print(f"\n正在处理文件: {file}")

        try:
            # 确定文件对应的表名
            table_name = None
            for pattern, tbl_name in file_patterns.items():
                if pattern.lower() in file.lower():
                    table_name = tbl_name
                    break

            if not table_name:
                print(f"  警告: 文件 '{file}' 不匹配任何已知模式，跳过")
                continue
            # 确定表名对应主键
            primary_column = primary_column_map.get(table_name)

            # 获取数据库表列名
            inspector = inspect(engine)
            db_columns = [col['name'] for col in inspector.get_columns(table_name)]
            # 数据库获取映射
            columns_mapping = get_key_value_map_from_mysql(table_name)

            # 读取file文件
            df_list = [read_file(file_path)]

            for df in df_list:
                # 如果表为空则跳过
                if df.empty:
                    print("  警告: 工作表为空，跳过")
                    continue

                # 写入数据库
                # 确保表和数据库列顺序一致
                df.rename(columns=columns_mapping, inplace=True)
                df = df[db_columns]
                # TODO 更新表中列数据(暂不进行任何处理)
                # for col, map_dict in get_text_map().items():
                #     if col in df.columns:
                #         df[col] = df[col].map(map_dict)

                # 如果表已存在，则追加数据（增量更新）
                if update_status == 1:
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',
                        index=False
                        # chunksize=5000, # 分批
                        # method='multi' # 使用多线程更新
                    )
                # 如果表已存在，则替换数据（全量更新）
                elif update_status == 2:
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='replace',
                        index=False
                        # chunksize=5000, # 分批
                        # method='multi' # 使用多线程更新
                    )
                # 如果表已存在，则更新数据（更新已有数据）
                elif update_status == 3:
                    # 先删除载更新
                    with engine.connect() as connection:
                        ids_to_update = df[primary_column].dropna().unique()
                        if len(ids_to_update) == 1:
                            ids_to_update = ids_to_update[0]

                        length = delete_records(connection, table_name, primary_column, ids_to_update)
                        print(f"  已更新{length}行数据！ ")
                        connection.commit()

                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',
                        index=False
                        # chunksize=5000, # 分批
                        # method='multi' # 使用多线程更新
                    )
                print(f"  成功将数据写入数据库表 '{table_name}'")

        except Exception as e:
            print(f"  处理文件 {file} 时出错: {str(e)}")


print("\n所有文件处理完成!")
