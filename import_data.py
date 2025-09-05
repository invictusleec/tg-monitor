from sqlalchemy.orm import Session
from model import Message, engine
import datetime
import re
import os

def parse_quark_link(line):
    """解析夸克网盘链接行"""
    # 提取链接
    quark_pattern = r'https?://pan\.quark\.cn/s/[a-zA-Z0-9]+'
    match = re.search(quark_pattern, line)
    if not match:
        return None
    
    link = match.group(0)
    
    # 提取标题（链接前的部分）
    title = line[:match.start()].strip()
    if not title:
        title = f"夸克资源_{link.split('/')[-1]}"
    
    return {
        'title': title[:100],  # 限制标题长度
        'link': link,
        'description': line.strip()
    }

def import_from_file(file_path, channel_name='imported_data'):
    """从文件导入数据"""
    if not os.path.exists(file_path):
        print(f"❌ 文件不存在: {file_path}")
        return 0
    
    imported_count = 0
    skipped_count = 0
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"📁 开始导入文件: {file_path}")
        print(f"📊 总行数: {len(lines)}")
        
        with Session(engine) as session:
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                
                # 解析夸克链接
                parsed = parse_quark_link(line)
                if not parsed:
                    continue
                
                # 检查是否已存在
                existing = session.query(Message).filter_by(
                    channel=channel_name,
                    title=parsed['title']
                ).first()
                
                if existing:
                    skipped_count += 1
                    continue
                
                # 创建新记录
                message = Message(
                    channel=channel_name,
                    title=parsed['title'],
                    description=parsed['description'],
                    links={'quark': parsed['link']},
                    tags=['导入数据'],
                    source='file_import',
                    timestamp=datetime.datetime.utcnow()
                )
                
                session.add(message)
                imported_count += 1
                
                # 每100条提交一次
                if imported_count % 100 == 0:
                    session.commit()
                    print(f"✅ 已导入 {imported_count} 条记录...")
            
            # 最终提交
            session.commit()
            
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        return 0
    
    print(f"\n📊 导入完成:")
    print(f"   ✅ 成功导入: {imported_count} 条")
    print(f"   ⏭️  跳过重复: {skipped_count} 条")
    
    return imported_count

def create_sample_data():
    """创建一些示例数据"""
    sample_data = [
        {
            'title': '复仇者联盟4：终局之战 4K',
            'description': '复仇者联盟4：终局之战 4K蓝光原盘 https://pan.quark.cn/s/sample001',
            'links': {'quark': 'https://pan.quark.cn/s/sample001'},
            'tags': ['电影', '4K', '科幻'],
            'channel': 'sample_movies'
        },
        {
            'title': '权力的游戏 第八季 1080P',
            'description': '权力的游戏第八季全集1080P https://pan.quark.cn/s/sample002',
            'links': {'quark': 'https://pan.quark.cn/s/sample002'},
            'tags': ['电视剧', '1080P', '奇幻'],
            'channel': 'sample_tv'
        },
        {
            'title': '流浪地球2 IMAX版',
            'description': '流浪地球2 IMAX版本 4K HDR https://pan.quark.cn/s/sample003',
            'links': {'quark': 'https://pan.quark.cn/s/sample003'},
            'tags': ['电影', 'IMAX', '科幻'],
            'channel': 'sample_movies'
        },
        {
            'title': '三体 动画版',
            'description': '三体动画版全集 https://pan.quark.cn/s/sample004',
            'links': {'quark': 'https://pan.quark.cn/s/sample004'},
            'tags': ['动漫', '科幻'],
            'channel': 'sample_anime'
        },
        {
            'title': '肖申克的救赎 4K修复版',
            'description': '肖申克的救赎4K修复版 https://pan.quark.cn/s/sample005',
            'links': {'quark': 'https://pan.quark.cn/s/sample005'},
            'tags': ['电影', '4K', '经典'],
            'channel': 'sample_movies'
        }
    ]
    
    imported_count = 0
    
    try:
        with Session(engine) as session:
            for data in sample_data:
                # 检查是否已存在
                existing = session.query(Message).filter_by(
                    channel=data['channel'],
                    title=data['title']
                ).first()
                
                if existing:
                    continue
                
                # 创建新记录
                message = Message(
                    channel=data['channel'],
                    title=data['title'],
                    description=data['description'],
                    links=data['links'],
                    tags=data['tags'],
                    source='sample_data',
                    timestamp=datetime.datetime.utcnow()
                )
                
                session.add(message)
                imported_count += 1
            
            session.commit()
            
    except Exception as e:
        print(f"❌ 创建示例数据失败: {e}")
        return 0
    
    print(f"✅ 创建了 {imported_count} 条示例数据")
    return imported_count

def main():
    print("=== 数据导入工具 ===")
    
    # 检查当前数据库中的记录数
    try:
        with Session(engine) as session:
            count = session.query(Message).count()
            print(f"📊 当前数据库中有 {count} 条记录")
    except Exception as e:
        print(f"❌ 无法连接数据库: {e}")
        return
    
    # 查找可能的导出文件
    possible_files = [
        'kkbdziyuan副本_quark_links_unique.txt',
        'quark_links.txt',
        'exported_links.txt'
    ]
    
    found_files = []
    for file_name in possible_files:
        if os.path.exists(file_name):
            found_files.append(file_name)
    
    if found_files:
        print(f"\n📁 发现可导入的文件:")
        for file_name in found_files:
            file_size = os.path.getsize(file_name)
            print(f"   - {file_name} ({file_size} 字节)")
        
        # 导入第一个找到的文件
        first_file = found_files[0]
        print(f"\n🔄 开始导入: {first_file}")
        imported = import_from_file(first_file)
        
        if imported > 0:
            print(f"✅ 成功导入 {imported} 条记录")
        else:
            print("❌ 没有导入任何记录")
    else:
        print("\n📁 未找到导出文件，创建示例数据...")
        create_sample_data()
    
    # 显示最终统计
    try:
        with Session(engine) as session:
            total_count = session.query(Message).count()
            print(f"\n📊 数据库中现在有 {total_count} 条记录")
            
            # 按频道统计
            channels = session.query(Message.channel).distinct().all()
            print(f"📺 涉及频道: {len(channels)} 个")
            for (channel,) in channels:
                channel_count = session.query(Message).filter_by(channel=channel).count()
                print(f"   - {channel}: {channel_count} 条")
                
    except Exception as e:
        print(f"❌ 统计失败: {e}")

if __name__ == "__main__":
    main()