import os
import streamlit as st

REQUIRED = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "DATABASE_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "DEFAULT_CHANNELS",
]

missing = [k for k in REQUIRED if not os.environ.get(k)]

st.set_page_config(page_title="tg-monitor • Setup", page_icon="🛠", layout="centered")
st.title("🛠 部署未完成：缺少环境变量")

if missing:
    st.error("以下环境变量尚未配置：" + ", ".join(missing))
else:
    st.success("看起来环境变量都已设置，重启容器后将自动进入正式应用。")

st.markdown(
    """
    请到部署平台的 Environment/Env Vars 中补充以上变量，然后重启/重新部署容器：
    - TELEGRAM_API_ID / TELEGRAM_API_HASH：来自 Telegram 的 API 凭据
    - DATABASE_URL：PostgreSQL 连接串（例如：postgresql://user:password@host:5432/dbname）
    - POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB：用于初始化和数据库访问
    - DEFAULT_CHANNELS：逗号分隔的频道用户名（不带 @）

    完成后，重新部署即可自动创建数据表并启动：
    - 前台：web.py（端口 8501）
    - 后台：后台.py（端口 8502，可选）
    - 监控：monitor.py（后台进程）
    """
)

st.info("当前页面仅用于占位和指引，避免在参数未配置时容器一直不健康。")