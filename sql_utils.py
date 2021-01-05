# -*- encoding: utf-8 -*-

"""
--------------------------------------
@File       : sql_utils.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2021/1/5 20:45
--------------------------------------
"""
from time import time


class SQLUtils:
    """
        生成SQL的一些公用方法
    """

    @staticmethod
    def gen_create_sql(table_name: str, cols_and_type: list or tuple) -> str:
        cols = ',\n'.join([' '.join(x) for x in cols_and_type])
        sql = "create table {}({})".format(table_name, cols)

        return sql

    @staticmethod
    def gen_date_tpl(has_second: bool, sep: str) -> str:
        tpl = 'YYYY{0}MM{0}DD HH24:MI:SS'.format(sep)
        tpl = tpl if has_second else tpl[: -3]

        return tpl

    @staticmethod
    def gen_insert_sql(
            table_name: str,
            cols_and_type: list or tuple,
            date_format: str = None,
            deal_null=True
    ) -> str:
        """
            date_format: 如果不存在，则直接使用原始数据进行插入
            deal_null：是否对数据中的空值进行处理
        """
        cols, values = [], []

        for _, item in enumerate(cols_and_type, 1):
            col_name, col_type = item
            col_type = col_type.upper()
            cols.append(col_name)

            if date_format and ('DATE' in col_type):
                values.append("TO_DATE(:{}, '{}')".format(_, date_format))

            elif 'NUMBER' in col_type:
                values.append("TO_NUMBER(:{})".format(_))

            else:
                if deal_null:
                    values.append('NVL(:{}, null)'.format(_))

                else:
                    values.append(':{}'.format(_))

        cols_str = ','.join(cols)
        values_str = ','.join(values)

        sql = "insert into {}({}) values({})".format(table_name, cols_str, values_str)

        return sql

    def gen_merge_update_insert_sql(
            self,
            src_table,
            dest_table,
            conditions: list,
            col_and_type,
            update_del_columns,
            distinct=False
    ):
        """
            生成 merge into SQL
            update_del_columns: 删除某些在 on_columns中处理过的字段
            distinct: 用于select的数据是否去重
        """
        # on 条件里的内容
        on_content = " AND ".join(conditions)

        # 所有的列名称
        columns = self.col_names(col_and_type)

        # update 内容
        update_columns = set(x.upper() for x in columns) - set(x.upper() for x in update_del_columns)

        update_content = ',\n'.join([f"d.{j}=s.{j}" for j in update_columns])

        # insert 内容
        insert_content_1 = ',\n'.join(columns)
        insert_content_2 = ',\n'.join([f"s.{x}" for x in columns])

        distinct = '' if not distinct else 'distinct'

        sql = f"""merge into {dest_table} d 
                    USING(select {distinct} * from {src_table}) s
                    ON ({on_content})

                    WHEN MATCHED THEN
                    UPDATE SET
                    {update_content}

                    WHEN NOT MATCHED THEN
                    INSERT({insert_content_1})VAlUES ({insert_content_2})
                """

        return sql

    @staticmethod
    def col_names(cols_and_type: list or tuple) -> list:
        names = [x[0] for x in cols_and_type]

        return names

    @staticmethod
    def number_cols(cols_and_type: list or tuple) -> list:
        names = [x[0] for x in cols_and_type if 'NUMBER' in x[-1].upper()]

        return names

    @staticmethod
    def trans_cols(cols_and_type: list or tuple) -> dict:
        """ 生成 dict，用于替换 Dataframe 中的 nan"""
        type_abbrs = {
            'NUMBER': 0.0,
            'VARCHAR': ''
        }

        result = {}
        for item in cols_and_type:
            col_name, col_type = item

            for type_abbr in type_abbrs:
                if type_abbr in col_type:
                    mid_data = {col_name: type_abbrs[type_abbr]}
                    result.update(mid_data)
                    break

        return result

    @staticmethod
    def trans_date_cols(cols_and_type: list or tuple) -> dict:
        null_dict = {x[0]: 0 for x in cols_and_type if 'DATE' in x[-1].upper()}

        return null_dict

    @staticmethod
    def gen_truncate_sql(table_name: str):
        """
            生成清空表语句
        """
        sql = f"truncate table {table_name}"

        return sql

    @staticmethod
    def calc_time(time1: float or int, time2: float or int = None) -> float:
        """ 返回两个时间差的"""
        time2 = time2 or time()
        value = (time2 - time1) / (60 ** 2)

        return round(value, 2)


def demo():
    """
        使用实例
    """
    col_type = [
        ('ID', 'NUMBER'),
        ('NAME', 'NVARCHAR2(64)'),
        ('GENDER', 'NVARCHAR2(4)')
    ]
    table_name = 'STUDENTS'
    table_name_t = 'STUDENTS_T'

    su = SQLUtils()

    conditions = ['d.ID=s.ID']
    update_del_insert_columns = ['ID']

    create_sql = su.gen_create_sql(table_name, col_type)
    insert_sql = su.gen_insert_sql(table_name, col_type)
    merge_sql = su.gen_merge_update_insert_sql(
        table_name_t,
        table_name,
        conditions,
        col_type,
        update_del_insert_columns
    )

    print(create_sql)
    print(insert_sql)
    print(merge_sql)
    """
    out:
    1) create_sql:
        create table STUDENTS(ID NUMBER,
        NAME NVARCHAR2(64),
        GENDER NVARCHAR2(4))
        
    2) insert_sql:
        insert into STUDENTS(ID,NAME,GENDER) values(TO_NUMBER(:1),NVL(:2, null),NVL(:3, null))
    
    3) merge_sql:
        merge into STUDENTS d 
                        USING(select  * from STUDENTS_T) s
                        ON (d.ID=s.ID)
    
                        WHEN MATCHED THEN
                        UPDATE SET
                            d.NAME=s.NAME,
                            d.GENDER=s.GENDER
    
                        WHEN NOT MATCHED THEN
                            INSERT(ID,
                                   NAME,
                                   GENDER)VAlUES (s.ID,
                                   s.NAME,
                                   s.GENDER)
    """


if __name__ == '__main__':
    demo()
