from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from sqlalchemy.orm import Session
from sqlalchemy import or_, cast, String
from model import Message, engine, ChannelRule, create_tables
from config import settings
import datetime
import re
import sys

# ------------------------ 规则缓存与判断 ------------------------
RULES_CACHE = {}

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

# ------------------------ 文本解析（与 monitor.py 保持一致） ------------------------

def parse_message(text: str):
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

    if lines and lines[0].strip():
        if lines[0].startswith('名称：'):
            title = lines[0].replace('名称：', '').strip()
        else:
            title = lines[0].strip()

    for idx, line in enumerate(lines[1:] if title else lines):
        line = line.strip()
        if not line:
            continue
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
                continue
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

    desc_text = '\n'.join(desc_lines)
    pattern = re.compile(r'([\u4e00-\u9fa5A-Za-z0-9#]+)[：:](https?://[^\s]+)')
    matches = pattern.findall(desc_text)
    for key, url in matches:
        found = False
        for keys, name in netdisk_map:
            if any(k in url.lower() or k in key for k in keys):
                links[name] = url
                found = True
                break
        if not found:
            links[key.strip()] = url
    desc_text = pattern.sub('', desc_text)

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
    desc_text = url_pattern.sub('', desc_text)

    tag_pattern = re.compile(r'#([\u4e00-\u9fa5A-Za-z0-9_]+)')
    found_tags = tag_pattern.findall(desc_text)
    if found_tags:
        tags.extend(found_tags)
        desc_text = tag_pattern.sub('', desc_text)

    tags = list(set(tags))
    netdisk_names = ['夸克', '迅雷', '百度', 'UC', '阿里', '天翼', '115', '123云盘']
    netdisk_name_pattern = re.compile(r'(' + '|'.join(netdisk_names) + r')')
    desc_text = netdisk_name_pattern.sub('', desc_text)

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

# ------------------------ 覆盖写入（以链接为唯一） ------------------------

def upsert_message_by_links(session: Session, parsed_data: dict, timestamp: datetime.datetime):
    links = parsed_data.get('links') or {}
    urls = set(links.values()) if links else set()

    if urls:
        like_filters = [cast(Message.links, String).like(f"%{u}%") for u in urls]
        candidates = session.query(Message).filter(
            Message.links.isnot(None),
            or_(*like_filters)
        ).order_by(Message.timestamp.desc()).all()

        target = None
        for msg in candidates:
            try:
                msg_links = (msg.links or {}).values()
                if any(u == v for u in urls for v in msg_links):
                    target = msg
                    break
            except Exception:
                continue

        if target:
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

    new_message = Message(timestamp=timestamp, **parsed_data)
    session.add(new_message)
    session.commit()
    print("✅ 新消息已保存（无重复链接）")
    return "inserted"

# ------------------------ 主逻辑：回溯导入 ------------------------

def main():
    create_tables()
    load_rules_cache()

    api_id = settings.TELEGRAM_API_ID
    api_hash = settings.TELEGRAM_API_HASH
    string_session = (settings.STRING_SESSION.strip() if getattr(settings, 'STRING_SESSION', None) else None)

    if not string_session:
        raise RuntimeError("未配置 STRING_SESSION，请在 .env 中设置后再运行本脚本")

    target_channel = 'bsbdbfjfjff'
    print(f"⏪ 开始回溯并导入频道 @{target_channel} 的历史消息（按当前规则，仅导入含网盘链接的消息；链接唯一覆盖）")

    inserted, updated, skipped_non_netdisk = 0, 0, 0

    with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
        for message in client.iter_messages(target_channel, reverse=True):
            # 仅处理有文本的消息
            text = getattr(message, 'message', None) or getattr(message, 'raw_text', None) or ''
            if not text.strip():
                continue

            parsed = parse_message(text)
            parsed['channel'] = target_channel

            # 规则过滤
            if should_drop_by_rules(target_channel, parsed):
                continue

            # 仅导入“关于网盘”的消息（必须包含 links）
            if not parsed.get('links'):
                skipped_non_netdisk += 1
                continue

            ts = getattr(message, 'date', None) or datetime.datetime.utcnow()
            with Session(engine) as session:
                r = upsert_message_by_links(session, parsed, ts)
                if r == 'updated':
                    updated += 1
                else:
                    inserted += 1

    print(f"✅ 导入完成：新增 {inserted} 条，覆盖更新 {updated} 条，跳过非网盘 {skipped_non_netdisk} 条")


if __name__ == '__main__':
    main()