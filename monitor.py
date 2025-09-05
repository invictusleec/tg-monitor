from telethon import TelegramClient, events
from telethon.sessions import StringSession
from sqlalchemy.orm import Session
from sqlalchemy import or_, cast, String
from model import Message, engine, Channel, Credential, TelegramConfig, ChannelRule, create_tables
import datetime
import json
import re
import sys
import os
from config import settings

def get_api_credentials():
    """获取 API 凭据，优先使用数据库中的凭据"""
    with Session(engine) as session:
        # 尝试从数据库获取凭据
        cred = session.query(Credential).first()
        if cred:
            return int(cred.api_id), cred.api_hash
    # 如果数据库中没有凭据，使用 .env 中的配置
    return settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH

def get_channels():
    """获取频道列表，合并数据库和 .env 中的频道"""
    channels = set()
    
    # 从数据库获取频道
    with Session(engine) as session:
        db_channels = [c.username for c in session.query(Channel).all()]
        channels.update(db_channels)
    
    # 从 .env 获取默认频道
    if hasattr(settings, 'DEFAULT_CHANNELS'):
        env_channels = [c.strip() for c in settings.DEFAULT_CHANNELS.split(',') if c.strip()]
        channels.update(env_channels)
        
        # 将 .env 中的频道添加到数据库
        with Session(engine) as session:
            for username in env_channels:
                if username not in db_channels:
                    channel = Channel(username=username)
                    session.add(channel)
            session.commit()
    
    return list(channels)

def get_string_session():
    """从数据库获取StringSession配置"""
    try:
        with Session(engine) as session:
            config = session.query(TelegramConfig).first()
            if config and config.string_session:
                return config.string_session.strip()
    except Exception as e:
        print(f"⚠️ 读取StringSession配置失败: {e}")
    return None

# Telegram API 凭证
# 使用数据库或.env中的API配置
api_id, api_hash = get_api_credentials()

# 优先使用.env中的StringSession，其次使用数据库中的StringSession，最后才使用session文件
from config import settings
# 优先从数据库读取 StringSession，其次才回退到 .env
db_string = get_string_session()
env_string = (settings.STRING_SESSION.strip() if hasattr(settings, 'STRING_SESSION') and settings.STRING_SESSION else None)
string_session = db_string or env_string

# 创建客户端（如果有StringSession则使用，否则使用session文件）
if string_session:
    client = TelegramClient(StringSession(string_session), api_id, api_hash)
    print(f"🔑 使用{'数据库中的StringSession' if db_string else '.env中的StringSession'}进行身份验证")
else:
    client = TelegramClient('monitor_session', api_id, api_hash)
    print("📁 使用session文件进行身份验证")

# 获取频道列表
channel_usernames = get_channels()

# 规则缓存：{channel: {exclude_netdisks:set, exclude_keywords:[lower], exclude_tags:set}}
RULES_CACHE = {}

# —— 无重启控制：通过控制文件动态暂停/恢复 ——
IS_PAUSED = False
CONTROL_FILE = "monitor_control.json"

def load_control_state():
    """从控制文件读取 paused 状态，变化时打印提示"""
    global IS_PAUSED
    try:
        paused = False
        if os.path.exists(CONTROL_FILE):
            with open(CONTROL_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                paused = bool(data.get("paused", False))
    except Exception as e:
        print(f"⚠️ 读取控制文件失败: {e}")
        paused = False
    if paused != IS_PAUSED:
        IS_PAUSED = paused
        print("⏸ 已暂停监控（无重启）" if IS_PAUSED else "▶️ 已恢复监控（无重启）")

def load_rules_cache():
    global RULES_CACHE
    try:
        with Session(engine) as session:
            rules = session.query(ChannelRule).filter_by(enabled=True).all()
            RULES_CACHE = {
                r.channel: {
                    'exclude_netdisks': set((r.exclude_netdisks or [])),
                    'exclude_keywords': [kw.lower() for kw in (r.exclude_keywords or []) if kw],
                    'exclude_tags': set((r.exclude_tags or [])),
                }
                for r in rules
            }
            print(f"⚙️ 已加载规则 {len(RULES_CACHE)} 条")
    except Exception as e:
        print(f"⚠️ 加载规则失败: {e}")

async def get_channel_username(event) -> str:
    try:
        chat = await event.get_chat()
        uname = getattr(chat, 'username', None)
        if uname:
            return uname
    except Exception:
        pass
    return ''

def should_drop_by_rules(channel: str, parsed: dict) -> bool:
    if not channel:
        return False
    rule = RULES_CACHE.get(channel)
    if not rule:
        return False
    # 1) 网盘类型命中
    links = parsed.get('links') or {}
    if links and rule['exclude_netdisks'] and (set(links.keys()) & rule['exclude_netdisks']):
        return True
    # 2) 关键词命中（标题/描述）
    kws = rule['exclude_keywords']
    if kws:
        title = (parsed.get('title') or '').lower()
        desc = (parsed.get('description') or '').lower()
        for kw in kws:
            if kw and (kw in title or kw in desc):
                return True
    # 3) 标签命中
    tags = set(parsed.get('tags') or [])
    if tags and rule['exclude_tags'] and (tags & rule['exclude_tags']):
        return True
    return False

def parse_message(text):
    """解析消息内容，提取标题、描述、链接等信息（更健壮，支持一行多网盘名链接提取和全局标签提取）"""
    lines = text.split('\n')
    title = ''
    description = ''
    links = {}
    tags = []
    source = ''
    channel = ''
    group = ''
    bot = ''
    current_section = None
    desc_lines = []

    # 网盘关键字与显示名映射
    netdisk_map = [
        (['quark', '夸克'], '夸克网盘'),
        (['aliyundrive', 'aliyun', '阿里', 'alipan'], '阿里云盘'),
        (['baidu', 'pan.baidu'], '百度网盘'),
        (['115.com', '115网盘', '115pan'], '115网盘'),
        (['cloud.189', '天翼', '189.cn'], '天翼云盘'),
        (['123pan', '123.yun'], '123云盘'),
        (['ucdisk', 'uc网盘', 'ucloud', 'drive.uc.cn'], 'UC网盘'),
        (['xunlei', 'thunder', '迅雷'], '迅雷'),
    ]

    # 1. 标题提取：优先"名称："，否则第一行直接当title
    if lines and lines[0].strip():
        if lines[0].startswith('名称：'):
            title = lines[0].replace('名称：', '').strip()
        else:
            title = lines[0].strip()

    # 2. 遍历其余行，提取描述、链接、标签等
    for idx, line in enumerate(lines[1:] if title else lines):
        line = line.strip()
        if not line:
            continue
        # 兼容多种标签前缀
        if line.startswith('🏷 标签：') or line.startswith('标签：'):
            tags.extend([tag.strip('#') for tag in line.replace('🏷 标签：', '').replace('标签：', '').split() if tag.strip('#')])
            continue
        if line.startswith('描述：'):
            current_section = 'description'
            desc_lines.append(line.replace('描述：', '').strip())
        elif line.startswith('链接：'):
            current_section = 'links'
            url = line.replace('链接：', '').strip()
            if not url:
                continue  # 跳过空链接
            # 智能识别网盘名
            found = False
            for keys, name in netdisk_map:
                if any(k in url.lower() for k in keys):
                    links[name] = url
                    found = True
                    break
            if not found:
                links['其他'] = url
        elif line.startswith('🎉 来自：'):
            source = line.replace('🎉 来自：', '').strip()
        elif line.startswith('📢 频道：'):
            channel = line.replace('📢 频道：', '').strip()
        elif line.startswith('👥 群组：'):
            group = line.replace('👥 群组：', '').strip()
        elif line.startswith('🤖 投稿：'):
            bot = line.replace('🤖 投稿：', '').strip()
        elif current_section == 'description':
            desc_lines.append(line)
        else:
            desc_lines.append(line)

    # 3. 全局正则提取所有"网盘名：链接"对，并从描述中移除
    desc_text = '\n'.join(desc_lines)
    # 支持"网盘名：链接"对，允许多个，支持中文冒号和英文冒号
    pattern = re.compile(r'([\u4e00-\u9fa5A-Za-z0-9#]+)[：:](https?://[^\s]+)')
    matches = pattern.findall(desc_text)
    for key, url in matches:
        # 智能识别网盘名
        found = False
        for keys, name in netdisk_map:
            if any(k in url.lower() or k in key for k in keys):
                links[name] = url
                found = True
                break
        if not found:
            links[key.strip()] = url
    # 从描述中移除所有"网盘名：链接"对
    desc_text = pattern.sub('', desc_text)
    # 4. 额外全局提取裸链接（http/https），也归类到links
    url_pattern = re.compile(r'(https?://[^\s]+)')
    for url in url_pattern.findall(desc_text):
        found = False
        for keys, name in netdisk_map:
            if any(k in url.lower() for k in keys):
                links[name] = url
                found = True
                break
        if not found:
            links['其他'] = url
    # 从描述中移除裸链接
    desc_text = url_pattern.sub('', desc_text)
    # 5. 全局正则提取所有#标签，并从描述中移除
    tag_pattern = re.compile(r'#([\u4e00-\u9fa5A-Za-z0-9_]+)')
    found_tags = tag_pattern.findall(desc_text)
    if found_tags:
        tags.extend(found_tags)
        desc_text = tag_pattern.sub('', desc_text)
    # 去重
    tags = list(set(tags))
    # 移除所有网盘名关键词
    netdisk_names = ['夸克', '迅雷', '百度', 'UC', '阿里', '天翼', '115', '123云盘']
    netdisk_name_pattern = re.compile(r'(' + '|'.join(netdisk_names) + r')')
    desc_text = netdisk_name_pattern.sub('', desc_text)
    # 6. 最终description，去除无意义符号行
    desc_lines_final = [line for line in desc_text.strip().split('\n') if line.strip() and not re.fullmatch(r'[.。·、,，-]+', line.strip())]
    description = '\n'.join(desc_lines_final)

    return {
        'title': title,
        'description': description,
        'links': links,
        'tags': tags,
        'source': source,
        'channel': channel,
        'group_name': group,
        'bot': bot
    }

# 动态绑定：替换静态装饰器，函数改名为 on_new_message
# @client.on(events.NewMessage(chats=channel_usernames))
def upsert_message_by_links(session: Session, parsed_data: dict, timestamp: datetime.datetime):
    """基于链接去重的写入逻辑：
    - 若 parsed_data 中包含 links，则以链接为唯一键：
      1) 数据库中存在任意相同链接：覆盖并更新该条消息
      2) 不存在：插入新消息
    - 若不包含 links：沿用原有逻辑（插入新消息）
    返回："updated" 或 "inserted"
    """
    links = parsed_data.get('links') or {}
    urls = set(links.values()) if links else set()

    # 只在存在链接时执行覆盖更新逻辑
    if urls:
        # 先在数据库层用 LIKE 限定候选集，再在 Python 层精确比对，避免误伤
        like_filters = [cast(Message.links, String).like(f"%{u}%") for u in urls]
        candidates = session.query(Message).filter(
            Message.links.isnot(None),
            or_(*like_filters)
        ).order_by(Message.timestamp.desc()).all()

        target = None
        for msg in candidates:
            try:
                msg_links = (msg.links or {}).values()
                # 精确匹配：完全相同的链接才算同一条
                if any(u == v for u in urls for v in msg_links):
                    target = msg
                    break
            except Exception:
                continue

        if target:
            # 覆盖更新该条消息
            target.timestamp = timestamp
            target.title = parsed_data.get('title')
            target.description = parsed_data.get('description')
            target.links = parsed_data.get('links')
            target.tags = parsed_data.get('tags')
            target.source = parsed_data.get('source')
            target.channel = parsed_data.get('channel')
            target.group_name = parsed_data.get('group_name')
            target.bot = parsed_data.get('bot')
            session.commit()
            print(f"♻️ 已覆盖更新现有消息(id={target.id})，按链接去重")
            return "updated"

    # 无链接或未命中：插入新消息
    new_message = Message(timestamp=timestamp, **parsed_data)
    session.add(new_message)
    session.commit()
    print("✅ 新消息已保存（无重复链接）")
    return "inserted"

async def on_new_message(event):
    # 无重启暂停：如被暂停则直接忽略消息
    if IS_PAUSED:
        return
    # 先过滤“回复类”消息（对某条消息的评论/回复），这些往往不是我们要采集的原始推送
    try:
        msg_obj = getattr(event, 'message', None)
        if msg_obj:
            if getattr(msg_obj, 'is_reply', False):
                print("🧹 已忽略回复消息（不入库）")
                return
            # 兼容不同Telethon版本的回复头字段
            if getattr(msg_obj, 'reply_to', None) is not None:
                print("🧹 已忽略回复消息（不入库）")
                return
            if getattr(msg_obj, 'reply_to_msg_id', None) is not None:
                print("🧹 已忽略回复消息（不入库）")
                return
            # 忽略服务类系统消息（置顶、入群等动作）
            if getattr(msg_obj, 'action', None) is not None:
                print("🧹 已忽略服务类系统消息（不入库）")
                return
    except Exception as e:
        print(f"⚠️ 检查是否为回复/服务消息时出错: {e}")
    
    # 忽略空文本/纯媒体消息
    if not (event.raw_text and event.raw_text.strip()):
        print("🧹 已忽略空文本/纯媒体消息（不入库）")
        return

    message = event.raw_text
    # 统一使用 Telegram 消息本身的时间（UTC），如缺失则退化为当前UTC
    try:
        evt_msg = getattr(event, 'message', None)
        evt_dt = getattr(evt_msg, 'date', None)
    except Exception:
        evt_dt = None
    timestamp = _to_utc_naive(evt_dt or datetime.datetime.utcnow())
    
    # 解析消息
    parsed_data = parse_message(message)

    # 若解析后无标题、无描述、无链接、无标签，则忽略
    if not any([parsed_data.get('title'), parsed_data.get('description'), parsed_data.get('links'), parsed_data.get('tags')]):
        print("🧹 已忽略无有效内容的消息（不入库）")
        return

    # 识别频道用户名（优先用事件实体）
    ch_username = await get_channel_username(event)
    if ch_username:
        parsed_data['channel'] = ch_username

    # 规则判断：命中则丢弃不入库
    if should_drop_by_rules(parsed_data.get('channel', ''), parsed_data):
        print(f"🚫 按规则忽略消息 @ {parsed_data.get('channel','')} | 标题: {parsed_data.get('title','')}")
        return
    
    # 基于链接唯一性的写入
    with Session(engine) as session:
        result = upsert_message_by_links(session, parsed_data, timestamp)
    
    print(f"[{timestamp}] 消息已写入数据库（{'覆盖更新' if result=='updated' else '新增'}）")

# 动态事件绑定所需的全局变量与方法
current_event_builder = None
current_channels = []

async def bind_channels():
    """根据数据库与.env动态更新监听频道集合，并重绑事件处理器"""
    global current_event_builder, current_channels
    try:
        new_channels = get_channels()
    except Exception as e:
        print(f"⚠️ 获取频道列表失败: {e}")
        return
    # 若频道无变化则跳过
    if set(new_channels) == set(current_channels):
        return
    # 先移除旧事件绑定
    if current_event_builder is not None:
        try:
            client.remove_event_handler(on_new_message, current_event_builder)
        except Exception as e:
            print(f"⚠️ 移除旧事件处理器失败: {e}")
    # 绑定新事件
    from telethon import events as _events
    ev = _events.NewMessage(chats=new_channels) if new_channels else _events.NewMessage()
    client.add_event_handler(on_new_message, ev)
    current_event_builder = ev
    current_channels[:] = list(new_channels)
    print(f"🎯 更新监听频道为 {len(new_channels)} 个：{new_channels}")

# 周期刷新监听列表
import asyncio as _asyncio
async def channels_watcher(poll_sec: int = 1):
    FLAG_CH = "channels_refresh.flag"
    FLAG_RULES = "rules_refresh.flag"
    while True:
        try:
            # 动态读取控制文件（暂停/恢复）
            load_control_state()
            # 频道刷新
            if os.path.exists(FLAG_CH):
                await bind_channels()
                try:
                    os.remove(FLAG_CH)
                except Exception:
                    pass
                print("🔄 收到后台刷新信号，已立即更新监听频道")
            else:
                await bind_channels()
            # 规则刷新
            if os.path.exists(FLAG_RULES):
                load_rules_cache()
                try:
                    os.remove(FLAG_RULES)
                except Exception:
                    pass
                print("🔄 收到规则刷新信号，已立即更新过滤规则")
        except Exception as e:
            print(f"⚠️ 刷新任务时出错: {e}")
        await _asyncio.sleep(poll_sec)

# 启动阶段打印文案调整
print("📡 正在动态绑定监听频道...")

# 启动监控修改：启动后立即绑定，并后台刷新
# 原：在 start_monitoring 中直接 run_until_disconnected
# 现：先 bind_channels 再启动 watcher
print(f"📡 准备监听 Telegram 频道：{channel_usernames}")

async def start_monitoring():
    """启动监控"""
    try:
        print("🔗 正在连接到Telegram...")
        await client.start()
        print("✅ Telegram连接成功！")
        
        # 获取用户信息
        me = await client.get_me()
        print(f"👤 当前用户: {me.first_name} (@{me.username if me.username else 'N/A'})")
        
        # 动态绑定频道并启动后台刷新任务
        await bind_channels()
        load_rules_cache()
        client.loop.create_task(channels_watcher())
        print("🎯 频道监听已启动（后台自动感知新增频道/规则）")
        
        await client.run_until_disconnected()
        
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print("💡 可能的解决方案:")
        print("   1. 检查网络连接")
        print("   2. 检查StringSession是否有效")
        print("   3. 检查API凭据是否正确")

async def backfill_channel(channel_username: str):
    """回溯抓取指定频道的历史消息，仅存入“包含网盘链接”的消息，并按链接唯一性覆盖更新。"""
    uname = channel_username.lstrip('@') if channel_username else ''
    if not uname:
        print("❌ 请提供有效的频道用户名，例如：--backfill bsbdbfjfjff")
        return

    print(f"⏪ 开始回溯抓取频道: {uname}")
    await client.start()

    inserted, updated, skipped = 0, 0, 0
    try:
        async for msg in client.iter_messages(uname, limit=None):
            text = getattr(msg, 'message', None) or getattr(msg, 'raw_text', None)
            if not text or not text.strip():
                continue
            parsed = parse_message(text)
            # 仅保存“关于网盘”的消息（必须包含 links）
            if not parsed.get('links'):
                skipped += 1
                continue
            parsed['channel'] = uname
            if should_drop_by_rules(uname, parsed):
                continue
            ts = _to_utc_naive(getattr(msg, 'date', None) or datetime.datetime.utcnow())
            with Session(engine) as session:
                r = upsert_message_by_links(session, parsed, ts)
                if r == 'updated':
                    updated += 1
                else:
                    inserted += 1
        print(f"⏪ 回溯完成：新增 {inserted} 条，更新 {updated} 条，跳过非网盘 {skipped} 条")
    except Exception as e:
        print(f"❌ 回溯抓取失败：{e}")

if __name__ == "__main__":
    if "--fix-tags" in sys.argv:
        # 检查并修复tags字段脏数据
        from sqlalchemy import update
        from sqlalchemy.orm import Session
        with Session(engine) as session:
            msgs = session.query(Message).all()
            fixed = 0
            for msg in msgs:
                # 如果tags不是list类型，尝试修正
                if msg.tags is not None and not isinstance(msg.tags, list):
                    try:
                        import ast
                        tags_fixed = ast.literal_eval(msg.tags)
                        if isinstance(msg.tags, list):
                            session.execute(update(Message).where(Message.id==msg.id).values(tags=tags_fixed))
                            fixed += 1
                    except Exception as e:
                        print(f"ID={msg.id} tags修复失败: {e}")
            session.commit()
            print(f"已修复tags字段脏数据条数: {fixed}")
    elif "--dedup-links" in sys.argv:
        # 定期去重：只保留每个网盘链接最新的消息
        from sqlalchemy.orm import Session
        from sqlalchemy import delete
        with Session(engine) as session:
            all_msgs = session.query(Message).order_by(Message.timestamp.desc()).all()
            link_to_id = {}  # {url: 最新消息id}
            id_to_delete = set()
            for msg in all_msgs:
                if not msg.links:
                    continue
                for url in msg.links.values():
                    if url in link_to_id:
                        id_to_delete.add(msg.id)
                    else:
                        link_to_id[url] = msg.id
            if id_to_delete:
                session.execute(delete(Message).where(Message.id.in_(id_to_delete)))
                session.commit()
                print(f"已删除重复网盘链接的旧消息条目: {len(id_to_delete)}")
            else:
                print("没有需要删除的重复网盘链接消息。")
    elif "--backfill" in sys.argv:
        import asyncio
        idx = sys.argv.index("--backfill")
        ch = sys.argv[idx+1] if len(sys.argv) > idx+1 else None
        if not ch:
            print("用法: python monitor.py --backfill <channel_username>")
        else:
            asyncio.run(backfill_channel(ch))
    else:
        import asyncio
        asyncio.run(start_monitoring())


def _to_utc_naive(dt: datetime.datetime) -> datetime.datetime:
    """将任意datetime统一为“UTC无tzinfo”的标准形式，便于数据库一致存储。"""
    if dt is None:
        return datetime.datetime.utcnow()
    if getattr(dt, 'tzinfo', None) is not None:
        return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    # 认为是无tz的UTC时间
    return dt