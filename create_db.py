import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    try:
        # 连接到PostgreSQL服务器（连接到默认的postgres数据库）
        conn = psycopg2.connect(
            host="121.4.252.113",
            port=5432,
            user="root",
            password="Qq2272809",
            database="telegram"  # 连接到默认数据库
        )
        
        # 设置自动提交模式
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        # 创建游标
        cursor = conn.cursor()
        
        # 检查数据库是否已存在
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'telegram'")
        exists = cursor.fetchone()
        
        if exists:
            print("数据库 telegram 已存在")
        else:
            # 创建数据库（需要该用户具备 CREATEDB 权限）
            cursor.execute("CREATE DATABASE telegram")
            print("数据库 telegram 创建成功！")
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"创建数据库时出错: {e}")
        return False

if __name__ == "__main__":
    create_database()