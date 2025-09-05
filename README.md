# TG Monitor

Telegram 频道监控系统，自动抓取网盘链接并提供 Web 界面管理。

## 功能特性

- 🔍 实时监控 Telegram 频道消息
- 💾 自动提取和存储各类网盘链接（夸克、阿里云盘、百度网盘等）
- 🌐 Streamlit Web 界面，支持搜索、过滤、分页
- 🛠️ 后台管理：频道管理、规则配置、API 凭据管理
- 🐋 Docker 部署支持
- 📊 数据统计和标签云

## 快速部署

### 使用 Docker Compose

1. 克隆仓库
```bash
git clone https://github.com/invictusleec/tg-monitor.git
cd tg-monitor
```

2. 配置环境变量
```bash
cp .env.example .env
# 编辑 .env 文件，填入你的配置
```

3. 启动服务
```bash
docker compose up -d
```

4. 访问界面
- 前台界面: http://localhost:8501
- 后台管理: http://localhost:8502

## 环境变量配置

请参考 `.env.example` 文件，主要配置项：

- `TELEGRAM_API_ID` / `TELEGRAM_API_HASH`: Telegram API 凭据
- `DATABASE_URL`: PostgreSQL 数据库连接 URL
- `STRING_SESSION`: Telegram 登录会话（可选）
- `DEFAULT_CHANNELS`: 默认监控频道列表

## 许可证

Private Repository