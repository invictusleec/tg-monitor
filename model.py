from sqlalchemy import Column, Integer, String, DateTime, JSON, ARRAY, create_engine, Boolean
from sqlalchemy.orm import declarative_base
from datetime import datetime
from config import settings

Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False)
    title = Column(String)
    description = Column(String)
    links = Column(JSON)  # 存储各种网盘链接
    tags = Column(ARRAY(String))  # 标签数组
    source = Column(String)  # 来源
    channel = Column(String)  # 频道
    group_name = Column(String)  # 群组
    bot = Column(String)  # 机器人
    created_at = Column(DateTime, default=datetime.utcnow)

class Credential(Base):
    __tablename__ = "credentials"
    id = Column(Integer, primary_key=True, index=True)
    api_id = Column(String, nullable=False)
    api_hash = Column(String, nullable=False)

class Channel(Base):
    __tablename__ = "channels"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, nullable=False)

class TelegramConfig(Base):
    __tablename__ = "telegram_config"
    id = Column(Integer, primary_key=True, index=True)
    string_session = Column(String, nullable=True)  # StringSession（可选）
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class QuarkLink(Base):
    __tablename__ = "quark_links"
    
    id = Column(Integer, primary_key=True, index=True)
    link = Column(String, nullable=False)
    channel_name = Column(String, nullable=False)
    message_id = Column(Integer)
    message_text = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

# 新增：频道过滤规则（按频道排除“网盘类型 / 关键词 / 标签”）
class ChannelRule(Base):
    __tablename__ = "channel_rules"

    id = Column(Integer, primary_key=True, index=True)
    channel = Column(String, nullable=False)  # 频道用户名，不带@
    exclude_netdisks = Column(JSON, nullable=True)  # 要排除的网盘类型数组
    exclude_keywords = Column(JSON, nullable=True)  # 要排除的关键词数组（标题/描述命中即排除）
    exclude_tags = Column(JSON, nullable=True)  # 要排除的标签数组（与消息tags交集即排除）
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# 数据库连接配置
DATABASE_URL = settings.DATABASE_URL

# 创建数据库引擎
engine = create_engine(DATABASE_URL)

# 创建所有表
def create_tables():
    Base.metadata.create_all(bind=engine)

# 初始化数据库
def init_db():
    """初始化数据库，创建所有表"""
    Base.metadata.create_all(bind=engine)
    return engine