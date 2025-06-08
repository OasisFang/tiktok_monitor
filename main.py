import asyncio
import logging
import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Set
import time
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO
import glob

from TikTokLive import TikTokLiveClient
from TikTokLive.client.logger import LogLevel
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent,
    CommentEvent, GiftEvent, LikeEvent, FollowEvent,
    JoinEvent, ShareEvent, SubscribeEvent,
    RoomUserSeqEvent
)
from TikTokLive.client.web.routes.fetch_room_id_live_html import FailedParseRoomIdError

# 配置日志 - 使用轮转日志避免文件过大
from logging.handlers import RotatingFileHandler
import os

def setup_logging():
    """设置日志配置，支持文件轮转"""
    
    # 从环境变量获取日志级别，默认为INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, log_level, logging.INFO)
    
    # 创建日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建轮转文件处理器
    # 最大1MB，保留5个备份文件
    file_handler = RotatingFileHandler(
        'tiktok_monitor.log',
        maxBytes=1*1024*1024,  # 1MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 减少第三方库的日志级别
    logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Flask的日志
    logging.getLogger('socketio').setLevel(logging.WARNING)  # SocketIO的日志
    logging.getLogger('engineio').setLevel(logging.WARNING)  # EngineIO的日志
    logging.getLogger('urllib3').setLevel(logging.WARNING)   # HTTP请求的日志
    
    return logging.getLogger(__name__)

# 设置日志
logger = setup_logging()

# 创建数据存储目录
os.makedirs('stats', exist_ok=True)
os.makedirs('history', exist_ok=True)
os.makedirs('history/daily', exist_ok=True)
os.makedirs('history/sessions', exist_ok=True)
os.makedirs('history/analytics', exist_ok=True)
os.makedirs('config', exist_ok=True)

# 初始化Flask应用
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 存储所有活跃的监控实例
active_monitors = {}
monitor_threads = {}

class ConfigManager:
    """配置管理类"""
    
    def __init__(self):
        self.config_file = 'config/monitor_config.json'
        self.ensure_directories()
    
    def ensure_directories(self):
        """确保配置目录存在"""
        os.makedirs('config', exist_ok=True)
    
    def save_monitor_config(self, monitor_configs: list):
        """保存监控配置"""
        try:
            config_data = {
                'monitors': monitor_configs,
                'saved_at': datetime.now().isoformat(),
                'version': '1.0'
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"监控配置已保存: {len(monitor_configs)} 个账号")
            return True
        except Exception as e:
            logger.error(f"保存监控配置失败: {str(e)}")
            return False
    
    def load_monitor_config(self) -> list:
        """加载监控配置"""
        try:
            if not os.path.exists(self.config_file):
                logger.info("监控配置文件不存在，返回空配置")
                return []
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            monitors = config_data.get('monitors', [])
            logger.info(f"加载监控配置: {len(monitors)} 个账号")
            return monitors
        except Exception as e:
            logger.error(f"加载监控配置失败: {str(e)}")
            return []
    
    def add_monitor_to_config(self, unique_id: str, sessionid: str = None, anti_detection: bool = False):
        """添加监控到配置"""
        try:
            configs = self.load_monitor_config()
            
            # 检查是否已存在
            for config in configs:
                if config['unique_id'] == unique_id:
                    # 更新现有配置
                    config['sessionid'] = sessionid
                    config['anti_detection'] = anti_detection
                    config['updated_at'] = datetime.now().isoformat()
                    self.save_monitor_config(configs)
                    return True
            
            # 添加新配置
            new_config = {
                'unique_id': unique_id,
                'sessionid': sessionid,
                'anti_detection': anti_detection,
                'added_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            configs.append(new_config)
            self.save_monitor_config(configs)
            return True
        except Exception as e:
            logger.error(f"添加监控配置失败: {str(e)}")
            return False
    
    def remove_monitor_from_config(self, unique_id: str):
        """从配置中移除监控"""
        try:
            configs = self.load_monitor_config()
            original_count = len(configs)
            
            configs = [c for c in configs if c['unique_id'] != unique_id]
            
            if len(configs) < original_count:
                self.save_monitor_config(configs)
                logger.info(f"从配置中移除监控: {unique_id}")
                return True
            else:
                logger.warning(f"配置中未找到监控: {unique_id}")
                return False
        except Exception as e:
            logger.error(f"移除监控配置失败: {str(e)}")
            return False

# 创建配置管理器实例
config_manager = ConfigManager()

class HistoryDataManager:
    """历史数据管理类"""
    
    def __init__(self):
        self.base_path = 'history'
        self.ensure_directories()
    
    def ensure_directories(self):
        """确保所有必要的目录存在"""
        directories = [
            'history',
            'history/daily',
            'history/sessions', 
            'history/analytics',
            'history/exports'
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def clear_history_data(self, unique_id: str = None, confirm_token: str = None):
        """清除历史数据"""
        if confirm_token != "CONFIRM_DELETE":
            raise ValueError("需要确认令牌才能删除历史数据")
        
        try:
            deleted_files = 0
            
            if unique_id:
                # 删除指定用户的数据
                logger.info(f"开始删除用户 {unique_id} 的历史数据...")
                
                # 删除每日数据
                daily_pattern = f"{unique_id}_*.json"
                daily_dir = os.path.join(self.base_path, 'daily')
                for file_path in glob.glob(os.path.join(daily_dir, daily_pattern)):
                    os.remove(file_path)
                    deleted_files += 1
                    logger.debug(f"删除每日数据: {file_path}")
                
                # 删除会话数据
                session_pattern = f"{unique_id}_*.json"
                session_dir = os.path.join(self.base_path, 'sessions')
                for file_path in glob.glob(os.path.join(session_dir, session_pattern)):
                    os.remove(file_path)
                    deleted_files += 1
                    logger.debug(f"删除会话数据: {file_path}")
                
                logger.info(f"用户 {unique_id} 的历史数据删除完成，共删除 {deleted_files} 个文件")
            else:
                # 删除所有历史数据
                logger.info("开始删除所有历史数据...")
                
                # 删除所有每日数据
                daily_dir = os.path.join(self.base_path, 'daily')
                if os.path.exists(daily_dir):
                    for file_path in glob.glob(os.path.join(daily_dir, '*.json')):
                        os.remove(file_path)
                        deleted_files += 1
                
                # 删除所有会话数据
                session_dir = os.path.join(self.base_path, 'sessions')
                if os.path.exists(session_dir):
                    for file_path in glob.glob(os.path.join(session_dir, '*.json')):
                        os.remove(file_path)
                        deleted_files += 1
                
                # 删除导出文件
                export_dir = os.path.join(self.base_path, 'exports')
                if os.path.exists(export_dir):
                    for file_path in glob.glob(os.path.join(export_dir, '*.csv')):
                        os.remove(file_path)
                        deleted_files += 1
                
                logger.info(f"所有历史数据删除完成，共删除 {deleted_files} 个文件")
            
            return {
                'success': True,
                'deleted_files': deleted_files,
                'message': f'成功删除 {deleted_files} 个历史数据文件'
            }
        except Exception as e:
            logger.error(f"删除历史数据失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'message': '删除历史数据时发生错误'
            }
    
    def get_data_usage_stats(self):
        """获取数据使用统计"""
        try:
            stats = {
                'daily_files': 0,
                'session_files': 0,
                'export_files': 0,
                'total_size_mb': 0,
                'users': set(),
                'date_range': {'earliest': None, 'latest': None}
            }
            
            # 统计每日数据
            daily_dir = os.path.join(self.base_path, 'daily')
            if os.path.exists(daily_dir):
                for filename in os.listdir(daily_dir):
                    if filename.endswith('.json'):
                        stats['daily_files'] += 1
                        file_path = os.path.join(daily_dir, filename)
                        stats['total_size_mb'] += os.path.getsize(file_path)
                        
                        # 提取用户名和日期
                        parts = filename.replace('.json', '').split('_')
                        if len(parts) >= 4:  # user_YYYY-MM-DD.json
                            user = '_'.join(parts[:-3])
                            date_str = '-'.join(parts[-3:])
                            stats['users'].add(user)
                            
                            if stats['date_range']['earliest'] is None or date_str < stats['date_range']['earliest']:
                                stats['date_range']['earliest'] = date_str
                            if stats['date_range']['latest'] is None or date_str > stats['date_range']['latest']:
                                stats['date_range']['latest'] = date_str
            
            # 统计会话数据
            session_dir = os.path.join(self.base_path, 'sessions')
            if os.path.exists(session_dir):
                for filename in os.listdir(session_dir):
                    if filename.endswith('.json'):
                        stats['session_files'] += 1
                        file_path = os.path.join(session_dir, filename)
                        stats['total_size_mb'] += os.path.getsize(file_path)
            
            # 统计导出文件
            export_dir = os.path.join(self.base_path, 'exports')
            if os.path.exists(export_dir):
                for filename in os.listdir(export_dir):
                    if filename.endswith('.csv'):
                        stats['export_files'] += 1
                        file_path = os.path.join(export_dir, filename)
                        stats['total_size_mb'] += os.path.getsize(file_path)
            
            # 转换为MB
            stats['total_size_mb'] = round(stats['total_size_mb'] / (1024 * 1024), 2)
            stats['users'] = list(stats['users'])
            
            return stats
        except Exception as e:
            logger.error(f"获取数据使用统计失败: {str(e)}")
            return None
    
    def get_daily_file_path(self, unique_id: str, date: str = None) -> str:
        """获取每日数据文件路径"""
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        return os.path.join(self.base_path, 'daily', f"{unique_id}_{date}.json")
    
    def get_session_file_path(self, unique_id: str, session_id: str) -> str:
        """获取单次直播会话文件路径"""
        return os.path.join(self.base_path, 'sessions', f"{unique_id}_{session_id}.json")
    
    def save_session_data(self, unique_id: str, session_data: dict):
        """保存单次直播会话数据"""
        try:
            # 生成会话ID
            session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            session_file = self.get_session_file_path(unique_id, session_id)
            
            # 添加元数据
            session_data['session_id'] = session_id
            session_data['unique_id'] = unique_id
            session_data['saved_at'] = datetime.now().isoformat()
            
            # 保存会话数据
            with open(session_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"会话数据已保存: {session_file}")
            
            # 同时更新每日汇总
            self.update_daily_summary(unique_id, session_data)
            
            return session_file
        except Exception as e:
            logger.error(f"保存会话数据失败: {str(e)}")
            return None
    
    def update_daily_summary(self, unique_id: str, session_data: dict, room_summary: dict = None):
        """更新每日汇总数据"""
        try:
            # 获取今天的日期
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 检查会话数据的日期，只处理今天的数据
            session_start_time = session_data.get('start_time')
            if session_start_time:
                try:
                    session_date = datetime.fromisoformat(session_start_time).strftime('%Y-%m-%d')
                    if session_date != today:
                        logger.info(f"跳过非今日的会话数据: {session_date} (今日: {today})")
                        return  # 不处理非今日的数据
                except Exception as e:
                    logger.warning(f"解析会话开始时间失败: {str(e)}")
            
            date = today
            daily_file = self.get_daily_file_path(unique_id, date)
            
            # 读取现有数据
            daily_data = {}
            if os.path.exists(daily_file):
                with open(daily_file, 'r', encoding='utf-8') as f:
                    daily_data = json.load(f)
            
            # 初始化每日数据结构
            if 'unique_id' not in daily_data:
                daily_data = {
                    'unique_id': unique_id,
                    'date': date,
                    'total_sessions': 0,
                    'total_duration': 0,
                    'peak_viewers': 0,
                    'total_viewers': 0,
                    'total_likes': 0,
                    'total_comments': 0,
                    'total_gifts': 0,
                    'total_gift_value': 0,
                    'total_follows': 0,
                    'total_shares': 0,
                    'sessions': [],
                    'hourly_stats': {str(i): {'sessions': 0, 'avg_viewers': 0, 'likes': 0} for i in range(24)},
                    'room_summary': {},  # 添加房间摘要信息
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
            
            # 更新统计数据
            if session_data.get('start_time'):
                session_start = datetime.fromisoformat(session_data['start_time'])
                hour = session_start.hour
                
                daily_data['total_sessions'] += 1
                daily_data['total_duration'] += session_data.get('duration', 0)
                daily_data['peak_viewers'] = max(daily_data['peak_viewers'], session_data.get('current_viewers', 0))
                daily_data['total_viewers'] += session_data.get('total_viewers', 0)
                daily_data['total_likes'] += session_data.get('like_count', 0)
                daily_data['total_comments'] += session_data.get('comment_count', 0)
                daily_data['total_gifts'] += session_data.get('gift_count', 0)
                daily_data['total_gift_value'] += session_data.get('gift_value', 0)
                daily_data['total_follows'] += session_data.get('follow_count', 0)
                daily_data['total_shares'] += session_data.get('share_count', 0)
                daily_data['updated_at'] = datetime.now().isoformat()
                
                # 更新小时统计
                hourly_key = str(hour)
                daily_data['hourly_stats'][hourly_key]['sessions'] += 1
                daily_data['hourly_stats'][hourly_key]['likes'] += session_data.get('like_count', 0)
                
                # 计算小时平均观众数
                current_avg = daily_data['hourly_stats'][hourly_key]['avg_viewers']
                session_count = daily_data['hourly_stats'][hourly_key]['sessions']
                new_avg = (current_avg * (session_count - 1) + session_data.get('current_viewers', 0)) / session_count
                daily_data['hourly_stats'][hourly_key]['avg_viewers'] = round(new_avg, 1)
                
                # 添加会话摘要
                session_summary = {
                    'session_id': session_data.get('session_id'),
                    'start_time': session_data.get('start_time'),
                    'end_time': session_data.get('end_time'),
                    'duration': session_data.get('duration', 0),
                    'peak_viewers': session_data.get('current_viewers', 0),
                    'total_likes': session_data.get('like_count', 0),
                    'total_gifts': session_data.get('gift_count', 0)
                }
                daily_data['sessions'].append(session_summary)
            
            # 更新 room_summary 信息
            if room_summary:
                daily_data['room_summary'] = room_summary
                daily_data['room_summary']['last_updated'] = datetime.now().isoformat()
            
            # 保存每日数据
            with open(daily_file, 'w', encoding='utf-8') as f:
                json.dump(daily_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"每日汇总已更新: {daily_file}")
            
        except Exception as e:
            logger.error(f"更新每日汇总失败: {str(e)}")
    
    def get_user_history(self, unique_id: str, days: int = 7) -> list:
        """获取用户历史数据"""
        try:
            history = []
            for i in range(days):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                daily_file = self.get_daily_file_path(unique_id, date)
                
                if os.path.exists(daily_file):
                    with open(daily_file, 'r', encoding='utf-8') as f:
                        daily_data = json.load(f)
                        history.append(daily_data)
            
            return sorted(history, key=lambda x: x['date'], reverse=True)
        except Exception as e:
            logger.error(f"获取用户历史数据失败: {str(e)}")
            return []
    
    def get_all_users_summary(self, date: str = None) -> list:
        """获取所有用户的汇总数据"""
        try:
            if date is None:
                date = datetime.now().strftime('%Y-%m-%d')
            
            daily_dir = os.path.join(self.base_path, 'daily')
            pattern = f"*_{date}.json"
            
            summaries = []
            for file_path in glob.glob(os.path.join(daily_dir, pattern)):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        summaries.append(data)
                except Exception as e:
                    logger.error(f"读取文件 {file_path} 失败: {str(e)}")
            
            return sorted(summaries, key=lambda x: x.get('total_viewers', 0), reverse=True)
        except Exception as e:
            logger.error(f"获取所有用户汇总失败: {str(e)}")
            return []
    
    def export_data(self, unique_id: str = None, start_date: str = None, end_date: str = None) -> str:
        """导出数据为CSV格式"""
        try:
            import csv
            
            # 设置日期范围
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if end_date is None:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            # 生成导出文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            if unique_id:
                export_file = os.path.join(self.base_path, 'exports', f"{unique_id}_{start_date}_to_{end_date}_{timestamp}.csv")
            else:
                export_file = os.path.join(self.base_path, 'exports', f"all_users_{start_date}_to_{end_date}_{timestamp}.csv")
            
            # 收集数据
            data_rows = []
            current = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            while current <= end:
                date_str = current.strftime('%Y-%m-%d')
                
                if unique_id:
                    # 单个用户数据
                    daily_file = self.get_daily_file_path(unique_id, date_str)
                    if os.path.exists(daily_file):
                        with open(daily_file, 'r', encoding='utf-8') as f:
                            daily_data = json.load(f)
                            data_rows.append(daily_data)
                else:
                    # 所有用户数据
                    daily_summaries = self.get_all_users_summary(date_str)
                    data_rows.extend(daily_summaries)
                
                current += timedelta(days=1)
            
            # 写入CSV文件
            if data_rows:
                with open(export_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    fieldnames = [
                        'unique_id', 'date', 'total_sessions', 'total_duration', 
                        'peak_viewers', 'total_viewers', 'total_likes', 'total_comments',
                        'total_gifts', 'total_gift_value', 'total_follows', 'total_shares'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for row in data_rows:
                        # 只保留指定字段
                        filtered_row = {field: row.get(field, 0) for field in fieldnames}
                        writer.writerow(filtered_row)
                
                logger.info(f"数据导出完成: {export_file}")
                return export_file
            else:
                logger.warning("没有找到要导出的数据")
                return None
                
        except Exception as e:
            logger.error(f"导出数据失败: {str(e)}")
            return None

# 创建历史数据管理器实例
history_manager = HistoryDataManager()

class TikTokLiveMonitor:
    """TikTok 直播监控类"""
    
    def __init__(self, unique_id: str, sessionid: Optional[str] = None, anti_detection: bool = False):
        """
        初始化监控器
        
        Args:
            unique_id: TikTok 用户名 (不含@符号)
            sessionid: TikTok 会话ID (用于绕过年龄限制)
            anti_detection: 是否启用防检测模式
        """
        self.unique_id = unique_id
        self.sessionid = sessionid
        self.anti_detection = anti_detection
        self.client: Optional[TikTokLiveClient] = TikTokLiveClient(
            unique_id=unique_id
        )
        
        # 如果提供了 sessionid，设置到客户端
        if sessionid:
            self.client.web.set_session_id(sessionid)
            logger.info(f"为 @{unique_id} 设置了 sessionid")
        
        # 如果启用防检测模式，配置伪装参数
        if anti_detection:
            self._setup_anti_detection()
            logger.info(f"为 @{unique_id} 启用了防检测模式")
        
        self.client.logger.setLevel(LogLevel.INFO.value)
        self.is_running = False
        self.is_live = False
        
        # 统计数据 - 优化结构，减少内存占用
        self.stats = {
            'start_time': None,
            'room_id': None,
            'current_viewers': 0,
            'total_viewers': 0,
            'like_count': 0,
            'comment_count': 0,
            'gift_count': 0,
            'gift_value': 0,
            'follow_count': 0,
            'share_count': 0,
            'subscribe_count': 0,
            'gifts_detail': {},  # 简化的礼物记录
            'room_info': {},     # 精简的房间信息
            'comments': [],      # 只保留最近10条评论
            'gifts': [],         # 只保留最近10条礼物记录
            'chart_data': {
                'timestamps': [],
                'viewer_counts': [],
                'like_counts': [],
                'comment_counts': []
            },
            'title': "",         # 直播标题
            'stream_status': 0,  # 直播状态
            'create_time': 0,    # 创建时间戳
            'minute_data': []    # 每分钟记录的数据点
        }
        
        # 每分钟数据记录
        self.last_minute_record = None
        
        # 用于存储用户信息
        self.viewers: Set[str] = set()
        
        # 设置事件监听器
        self._setup_event_listeners()
        
    def _setup_anti_detection(self):
        """设置防检测参数"""
        import random
        
        # 使用更保守的User-Agent列表，模拟真实浏览器
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        # 保守的语言设置
        accept_languages = [
            'en-US,en;q=0.9',
            'zh-CN,zh;q=0.9,en;q=0.8'
        ]
        
        # 更温和的请求头设置 - 只设置关键的几个
        selected_ua = random.choice(user_agents)
        selected_lang = random.choice(accept_languages)
        
        # 仅设置最关键的请求头，避免过度伪装
        minimal_headers = {
            'User-Agent': selected_ua,
            'Accept-Language': selected_lang
        }
        
        # 应用请求头到客户端
        try:
            if hasattr(self.client.web, 'session') and hasattr(self.client.web.session, 'headers'):
                # 只更新关键请求头，保留其他默认设置
                for key, value in minimal_headers.items():
                    self.client.web.session.headers[key] = value
                logger.info(f"已设置防检测请求头: UA={selected_ua[:50]}..., Lang={selected_lang}")
        except Exception as e:
            logger.warning(f"设置请求头失败: {str(e)}")
        
        # 更保守的防检测配置
        self.anti_detection_config = {
            'random_delay_min': 1,    # 降低最小延迟
            'random_delay_max': 5,    # 降低最大延迟
            'request_interval_min': 45,   # 适当的检查间隔
            'request_interval_max': 90    # 更合理的最大间隔
        }
            
    def _setup_event_listeners(self):
        """设置所有事件监听器"""
        
        @self.client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            """连接事件"""
            self.is_live = True
            self.stats['start_time'] = datetime.now()
            self.stats['room_id'] = self.client.room_id
            logger.info(f"已连接到直播间 @{event.unique_id} (房间ID: {self.client.room_id})")
            
            # 获取房间信息
            if hasattr(self.client, 'room_info') and self.client.room_info:
                self.stats['room_info'] = self.client.room_info
                
                # 直播状态
                self.stats['stream_status'] = self.client.room_info.get('status', 0)
                
                # 基本信息
                self.stats['title'] = self.client.room_info.get('title', '')
                self.stats['cover'] = self.client.room_info.get('cover', {})
                self.stats['create_time'] = self.client.room_info.get('create_time', 0)
                
                # 流地址
                self.stats['stream_url'] = self.client.room_info.get('stream_url', {})
                
                socketio.emit('room_info', {
                    'unique_id': self.unique_id,
                    'room_info': self.stats['room_info']
                })
            
        @self.client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            """断开连接事件"""
            duration = (datetime.now() - self.stats['start_time']).total_seconds() if self.stats['start_time'] else 0
            logger.info(f"已断开连接，持续时间: {duration:.0f} 秒")
            socketio.emit('disconnect', {'unique_id': self.unique_id})
            await self.save_stats()
            
        @self.client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            """直播结束事件"""
            self.is_live = False
            logger.info("直播已结束")
            socketio.emit('live_end', {'unique_id': self.unique_id})
            await self.save_stats()
            
        @self.client.on(RoomUserSeqEvent)
        async def on_room_user_seq(event: RoomUserSeqEvent):
            """房间用户数更新事件"""
            try:
                # 尝试所有可能的属性名称获取观众数量
                viewer_count = 0
                tried_attrs = []
                
                # 尝试使用表示"当前"在线观众的属性，而不是累计观众
                for attr_name in ["user_count", "total", "member_count", "m_total", 
                                 "viewers", "audience", "audience_count", "popularity"]:
                    tried_attrs.append(attr_name)
                    if hasattr(event, attr_name):
                        try:
                            viewer_count = getattr(event, attr_name)
                            logger.debug(f"获取到观众数量: {viewer_count}，使用属性: {attr_name}")
                            break
                        except Exception as e:
                            logger.error(f"获取属性 {attr_name} 时出错: {str(e)}")
                
                # 如果通过第一轮属性未获取到观众数量，尝试total_user作为后备
                if viewer_count == 0 and hasattr(event, "total_user"):
                    try:
                        total_user_count = getattr(event, "total_user")
                        # 记录累计观众数，但不用作当前观众数
                        if total_user_count > 0:
                            logger.debug(f"获取到累计观众数量: {total_user_count}，但不作为当前观众数")
                            self.stats['total_viewers'] = max(self.stats['total_viewers'], total_user_count)
                    except Exception as e:
                        logger.error(f"获取total_user属性时出错: {str(e)}")
                
                # 如果所有属性都尝试失败，尝试解析原始字符串
                if viewer_count == 0:
                    logger.warning(f"尝试所有属性都失败: {tried_attrs}，尝试解析原始字符串")
                    try:
                        event_str = str(event)
                        # 尝试从字符串中提取数字
                        import re
                        numbers = re.findall(r'\d+', event_str)
                        if numbers:
                            viewer_count = int(numbers[0])  # 使用第一个数字
                            logger.debug(f"从字符串中提取的观众数量: {viewer_count}")
                    except Exception as e:
                        logger.error(f"解析字符串时出错: {str(e)}")
                
                # 确保我们至少有一个观众
                if viewer_count == 0:
                    viewer_count = 1
                    logger.warning("无法获取有效的观众数量，设置为默认值1")
                
                # 更新统计和图表数据
                self.stats['current_viewers'] = viewer_count
                
                # 每分钟记录一次重要数据
                self._record_minute_data()
                
                # 发送数据更新
                self._send_data_updates()
                
            except Exception as e:
                logger.error(f"处理观众数量更新失败: {str(e)}")
        
        @self.client.on(JoinEvent)
        async def on_join(event: JoinEvent):
            """用户加入事件"""
            try:
                # 获取用户ID - 在新版中可能是 unique_id
                user_id = self._get_user_id(event.user)
                if user_id and user_id not in self.viewers:
                    self.viewers.add(user_id)
                    socketio.emit('user_join', {
                        'unique_id': self.unique_id,
                        'user': {
                            'nickname': event.user.nickname,
                            'user_id': user_id
                        }
                    })
                    
            except Exception as e:
                logger.error(f"处理JoinEvent时出错: {str(e)}")
                
        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            """评论事件"""
            try:
                # 获取用户 - CommentEvent 中用户字段可能是 user_info 而不是 user
                user = event.user if hasattr(event, 'user') else (event.user_info if hasattr(event, 'user_info') else None)
                
                if user is None:
                    logger.warning(f"无法获取评论用户信息，可用属性: {dir(event)}")
                    return
                    
                # 获取用户ID
                user_id = self._get_user_id(user)
                if not user_id:
                    return
                
                # 获取评论内容
                if hasattr(event, 'comment'):
                    comment_text = event.comment
                else:
                    logger.warning(f"无法获取评论内容，可用属性: {dir(event)}")
                    return
                
                # 更新统计
                self.stats['comment_count'] += 1
                
                # 记录评论历史 - 只保留最近10条
                timestamp = datetime.now().isoformat()
                comment_data = {
                    'user': user.nickname,
                    'comment': comment_text,
                    'timestamp': timestamp
                }
                self.stats['comments'].append(comment_data)
                
                # 只保留最新的10条评论
                if len(self.stats['comments']) > 10:
                    self.stats['comments'] = self.stats['comments'][-10:]
                
                # 发送到前端
                socketio.emit('comment', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': user.nickname,
                        'user_id': user_id
                    },
                    'comment': comment_text,
                    'timestamp': timestamp
                })
                
            except Exception as e:
                logger.error(f"处理CommentEvent时出错: {str(e)}")
            
        @self.client.on(LikeEvent)
        async def on_like(event: LikeEvent):
            """点赞事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                
                # 获取点赞数量
                like_count = 1  # 默认值
                if hasattr(event, 'like_count'):
                    like_count = event.like_count
                elif hasattr(event, 'count'):
                    like_count = event.count
                elif hasattr(event, 'likes'):
                    like_count = event.likes
                
                # 记录点赞信息（改为DEBUG级别，减少日志量）
                logger.debug(f"收到点赞: 用户={user_id}, 数量={like_count}")
                
                # 更新统计
                self.stats['like_count'] += like_count
                
                # 发送到前端
                socketio.emit('like', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname if hasattr(event.user, 'nickname') else "用户",
                        'user_id': user_id
                    },
                    'count': like_count,
                    'total_likes': self.stats['like_count'],  # 发送总点赞数
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"处理LikeEvent时出错: {str(e)}")
                logger.exception("详细错误信息:")
            
        @self.client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            """礼物事件"""
            try:
                # 根据最新版本的API，使用正确的属性
                user = event.user if hasattr(event, 'user') else (event.from_user if hasattr(event, 'from_user') else None)
                
                if user is None:
                    logger.warning(f"无法获取礼物发送者信息，可用属性: {dir(event)}")
                    return
                
                # 获取用户ID
                user_id = self._get_user_id(user)
                if not user_id:
                    return
                
                # 根据示例获取正确的礼物属性
                gift = event.gift if hasattr(event, 'gift') else (event.m_gift if hasattr(event, 'm_gift') else None)
                
                if gift is None:
                    logger.warning(f"无法获取礼物信息，可用属性: {dir(event)}")
                    return
                
                # 获取礼物信息
                gift_name = gift.name
                gift_count = event.repeat_count if hasattr(event, 'repeat_count') else 1
                
                # 检查是否处于连击状态
                streaking = False
                if hasattr(event, 'streaking'):
                    streaking = event.streaking
                elif hasattr(gift, 'streakable') and hasattr(event, 'repeat_end'):
                    streaking = gift.streakable and event.repeat_end != 1
                
                # 仅在连击结束或非连击礼物时才记录
                if not streaking or not gift.streakable:
                    # 更新礼物统计
                    self.stats['gift_count'] += gift_count
                    
                    # 记录礼物详情
                    if gift_name not in self.stats['gifts_detail']:
                        self.stats['gifts_detail'][gift_name] = {
                            'count': 0,
                            'users': []
                        }
                    
                    self.stats['gifts_detail'][gift_name]['count'] += gift_count
                    timestamp = datetime.now().isoformat()
                    
                    # 记录发送礼物的用户信息
                    self.stats['gifts_detail'][gift_name]['users'].append({
                        'nickname': user.nickname,
                        'count': gift_count,
                        'time': timestamp
                    })
                    
                    # 记录礼物历史 - 只保留最近10条
                    gift_data = {
                        'user': user.nickname,
                        'gift_name': gift_name,
                        'gift_count': gift_count,
                        'timestamp': timestamp
                    }
                    self.stats['gifts'].append(gift_data)
                    
                    # 只保留最新的10个礼物记录
                    if len(self.stats['gifts']) > 10:
                        self.stats['gifts'] = self.stats['gifts'][-10:]
                    
                    # 如果礼物有价值信息，累加价值
                    if hasattr(gift, 'diamond_count'):
                        self.stats['gift_value'] += gift.diamond_count * gift_count
                    elif hasattr(gift, 'value'):
                        self.stats['gift_value'] += gift.value * gift_count
                
                # 发送礼物事件到前端
                socketio.emit('gift', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': user.nickname,
                        'user_id': user_id
                    },
                    'gift': {
                        'name': gift_name,
                        'count': gift_count,
                        'streaking': streaking
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理GiftEvent时出错: {str(e)}")
                
        @self.client.on(FollowEvent)
        async def on_follow(event: FollowEvent):
            """关注事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['follow_count'] += 1
                socketio.emit('follow', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理FollowEvent时出错: {str(e)}")
            
        @self.client.on(ShareEvent)
        async def on_share(event: ShareEvent):
            """分享事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['share_count'] += 1
                socketio.emit('share', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理ShareEvent时出错: {str(e)}")
            
        @self.client.on(SubscribeEvent)
        async def on_subscribe(event: SubscribeEvent):
            """订阅事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['subscribe_count'] += 1
                socketio.emit('subscribe', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理SubscribeEvent时出错: {str(e)}")
                
    def _get_user_id(self, user):
        """获取用户ID，处理不同版本API的差异"""
        if user is None:
            return "unknown"
        
        try:    
            # 直接调试输出所有可用属性
            logger.debug(f"User对象属性: {dir(user)}")
            
            # 尝试优先获取用户显示名称，如果无法获取ID
            nickname = None
            if hasattr(user, 'nickname'):
                nickname = user.nickname
            
            # 使用正确的属性获取ID
            if hasattr(user, 'unique_id'):
                return user.unique_id
            elif hasattr(user, 'id'):
                return user.id
            elif hasattr(user, 'uid'):
                return user.uid
            elif hasattr(user, 'user_id'):
                return user.user_id
            
            # 如果无法获取ID但有昵称，使用昵称
            if nickname:
                return f"user_{nickname}"
            
            # 记录可用属性帮助调试
            attrs = {}
            for attr in dir(user):
                if not attr.startswith('_') and not callable(getattr(user, attr, None)):
                    try:
                        attrs[attr] = getattr(user, attr)
                    except Exception:
                        attrs[attr] = "无法获取"
            
            logger.warning(f"无法获取用户ID，可用属性: {attrs}")
            
            # 生成一个随机ID
            import uuid
            return f"user_{uuid.uuid4().hex[:8]}"
            
        except Exception as e:
            logger.error(f"获取用户ID时出错: {str(e)}")
            return "unknown"
    
    async def fetch_room_info(self):
        """
        手动拉取房间信息
        """
        try:
            # 防检测：仅在必要时添加轻微延迟
            if self.anti_detection:
                import random
                await asyncio.sleep(random.uniform(0.5, 2))
            
            # 尝试先获取房间ID
            try:
                room_id = await self.client.web.fetch_room_id_from_html(self.client.unique_id)
            except FailedParseRoomIdError:
                logger.info(f"HTML 解析房间ID失败，使用 API 获取")
                # 防检测：在API调用前轻微延迟
                if self.anti_detection:
                    import random
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                room_id = await self.client.web.fetch_room_id_from_api(self.client.unique_id)
                
            self.client.web.params['room_id'] = str(room_id)
            self.stats['room_id'] = room_id
            
            # 获取直播间信息
            room_info = await self.client.web.fetch_room_info()
            # 获取礼物列表信息
            gift_info = await self.client.web.fetch_gift_list()
            
            # 只保存精简的房间信息，不再保存完整的大文件
            simplified_room_info = {
                'id': room_info.get('id', 0),
                'title': room_info.get('title', ''),
                'status': room_info.get('status', 0),
                'user_count': room_info.get('user_count', 0),
                'create_time': room_info.get('create_time', 0),
                'stats': room_info.get('stats', {}),
                'owner': room_info.get('owner', {}).get('nickname', '') if room_info.get('owner') else ''
            }
            
            # 保存精简版本到小文件
            simplified_file = f"{self.unique_id}_room_summary.json"
            summary_data = {
                "room_summary": simplified_room_info, 
                "gift_count": len(gift_info.get('gifts', [])) if gift_info else 0,
                "updated_at": datetime.now().isoformat()
            }
            with open(simplified_file, "w", encoding="utf-8") as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            logger.info(f"已保存精简房间信息到: {simplified_file}")
            
            # 同时将 room_summary 更新到今日的 daily 文件中
            self._update_daily_with_room_summary(summary_data)
            
            # 更新统计数据
            if room_info:
                self.stats['room_info'] = simplified_room_info
                
                # 直播状态
                self.stats['stream_status'] = room_info.get('status', 0)
                self.is_live = self.stats['stream_status'] == 2
                
                # 基本信息
                self.stats['title'] = room_info.get('title', '')
                self.stats['create_time'] = room_info.get('create_time', 0)
                
                # 统计数据
                if 'stats' in room_info:
                    stats = room_info['stats']
                    # 累计观众
                    self.stats['total_viewers'] = stats.get('total_user', 0)
                    self.stats['like_count'] = stats.get('like_count', 0)
                    self.stats['comment_count'] = stats.get('comment_count', 0)
                    
                # 当前在线人数 - 从room_info['user_count']获取
                self.stats['current_viewers'] = room_info.get('user_count', 0)
                
                # 记录分钟数据
                self._record_minute_data()
            
            # 发送数据更新
            self._send_data_updates()
            return True
        except Exception as e:
            logger.error(f"获取房间信息失败: {str(e)}", exc_info=True)
            return False
            
    async def check_and_connect(self):
        """
        检查并连接到直播间
        """
        try:
            # 如果已经连接，只更新房间信息而不重新连接
            if self.client.connected:
                logger.debug(f"@{self.unique_id} 已经连接，更新房间信息")
                await self.fetch_room_info()
                return True

            # 先尝试获取房间信息，判断用户是否在直播
            # 无论获取结果如何，都先初始化一个界面
            result = await self.fetch_room_info()
            
            # 发送初始数据以显示界面
            self._send_default_data()
            
            # 检查是否获取成功并且在直播中
            if result and self.stats['stream_status'] == 2:  # 直播中
                logger.info(f"检查 @{self.unique_id} 是否在直播...")
                
                logger.info(f"开始监控 @{self.unique_id} 的直播...")
                
                # 使用已获取的房间ID直接连接
                room_id = self.stats['room_id']
                
                try:
                    # 防检测模式下，连接前先延迟
                    if self.anti_detection:
                        await self._random_delay()
                    
                    # 确保客户端处于断开状态后再连接
                    if self.client.connected:
                        logger.debug(f"断开现有连接后重新连接 @{self.unique_id}")
                        await self.client.disconnect()
                        await asyncio.sleep(1)  # 等待断开完成
                    
                    # 开始连接
                    await self.client.start(
                        fetch_room_info=True,  # 额外获取房间信息
                        fetch_gift_info=True,   # 额外获取礼物信息
                        process_connect_events=True,
                        room_id=room_id  # 直接使用房间ID，避免再次获取
                    )
                    return True
                except Exception as e:
                    # 如果防检测模式下连接失败，尝试禁用防检测重试一次
                    if self.anti_detection and ("UserNotFoundError" in str(e) or "404" in str(e)):
                        logger.warning(f"防检测模式连接失败，尝试使用标准模式重连: {str(e)}")
                        try:
                            # 临时禁用防检测
                            self.anti_detection = False
                            # 重新设置客户端（恢复默认请求头）
                            self.client = TikTokLiveClient(unique_id=self.unique_id)
                            if self.sessionid:
                                self.client.web.set_session_id(self.sessionid)
                            self._setup_event_listeners()
                            
                            # 重新获取房间信息
                            result = await self.fetch_room_info()
                            if result and self.stats['stream_status'] == 2:
                                await self.client.start(
                                    fetch_room_info=True,
                                    fetch_gift_info=True,
                                    process_connect_events=True,
                                    room_id=self.stats['room_id']
                                )
                                logger.info(f"标准模式重连成功: @{self.unique_id}")
                                return True
                        except Exception as retry_e:
                            logger.error(f"标准模式重连也失败: {str(retry_e)}")
                    
                    # 如果是签名API错误，记录日志但不重试太频繁
                    if "SIGN_NOT_200" in str(e) or "500" in str(e):
                        logger.error(f"签名API错误，等待较长时间后重试: {str(e)}")
                        return False
                    else:
                        # 其他错误记录并继续
                        logger.error(f"连接直播间失败: {str(e)}")
                        return False
            else:
                if result:
                    logger.info(f"@{self.unique_id} 当前不在直播中 (状态: {self.stats['stream_status']})")
                else:
                    logger.info(f"@{self.unique_id} 无法获取房间信息")
                
                # 不在直播或无法获取信息时也发送更新，让前端显示"离线"状态
                self._send_data_updates()
                
            return False
        except Exception as e:
            logger.error(f"连接直播间失败: {str(e)}")
            # 发生错误时也发送更新
            self._send_default_data()
            return False
        
    def _send_default_data(self):
        """发送默认/初始数据"""
        socketio.emit('stream_data', {
            'unique_id': self.unique_id,
            'is_live': self.is_live,
            'room_info': self.stats['room_info'],
            'title': self.stats['title'],
            'current_viewers': self.stats['current_viewers'],
            'total_viewers': self.stats['total_viewers'],
            'total_likes': self.stats['like_count'],
            'total_gifts': self.stats['gift_count'],
            'total_comments': self.stats['comment_count'],
            'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
            'chart_data': self.stats['chart_data'],
            'comments': [],
            'gifts': [],
            'status': 'connecting' if self.is_running else 'idle'
        })
    
    def _update_daily_with_room_summary(self, summary_data):
        """将房间摘要更新到今日的daily文件中"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            daily_file = history_manager.get_daily_file_path(self.unique_id, today)
            
            # 读取现有数据
            daily_data = {}
            if os.path.exists(daily_file):
                with open(daily_file, 'r', encoding='utf-8') as f:
                    daily_data = json.load(f)
            
            # 如果没有daily数据，创建基本结构
            if 'unique_id' not in daily_data:
                daily_data = {
                    'unique_id': self.unique_id,
                    'date': today,
                    'total_sessions': 0,
                    'total_duration': 0,
                    'peak_viewers': 0,
                    'total_viewers': 0,
                    'total_likes': 0,
                    'total_comments': 0,
                    'total_gifts': 0,
                    'total_gift_value': 0,
                    'total_follows': 0,
                    'total_shares': 0,
                    'sessions': [],
                    'hourly_stats': {str(i): {'sessions': 0, 'avg_viewers': 0, 'likes': 0} for i in range(24)},
                    'room_summary': {},
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
            
            # 更新房间摘要
            daily_data['room_summary'] = summary_data
            daily_data['updated_at'] = datetime.now().isoformat()
            
            # 保存数据
            with open(daily_file, 'w', encoding='utf-8') as f:
                json.dump(daily_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"房间摘要已更新到每日数据: {daily_file}")
            
        except Exception as e:
            logger.error(f"更新每日数据中的房间摘要失败: {str(e)}")
    
    def _record_minute_data(self):
        """每分钟记录一次重要数据"""
        current_time = datetime.now()
        current_minute = current_time.strftime('%H:%M')
        
        # 检查是否需要记录新的分钟数据
        if self.last_minute_record is None or self.last_minute_record != current_minute:
            self.last_minute_record = current_minute
            
            # 记录当前分钟的数据点
            minute_record = {
                'timestamp': current_time.isoformat(),
                'minute': current_minute,
                'current_viewers': self.stats['current_viewers'],
                'total_viewers': self.stats['total_viewers'],
                'like_count': self.stats['like_count'],
                'comment_count': self.stats['comment_count'],
                'gift_count': self.stats['gift_count'],
                'gift_value': self.stats['gift_value'],
                'follow_count': self.stats['follow_count'],
                'share_count': self.stats['share_count']
            }
            
            # 添加到分钟数据列表
            self.stats['minute_data'].append(minute_record)
            
            # 同时更新图表数据（保持向后兼容）
            self.stats['chart_data']['timestamps'].append(current_minute)
            self.stats['chart_data']['viewer_counts'].append(self.stats['current_viewers'])
            self.stats['chart_data']['like_counts'].append(self.stats['like_count'])
            self.stats['chart_data']['comment_counts'].append(self.stats['comment_count'])
            
            # 保持数据长度合理（最多保留100个数据点，约1.5小时）
            max_points = 100
            if len(self.stats['minute_data']) > max_points:
                self.stats['minute_data'] = self.stats['minute_data'][-max_points:]
            
            if len(self.stats['chart_data']['timestamps']) > max_points:
                self.stats['chart_data']['timestamps'] = self.stats['chart_data']['timestamps'][-max_points:]
                self.stats['chart_data']['viewer_counts'] = self.stats['chart_data']['viewer_counts'][-max_points:]
                self.stats['chart_data']['like_counts'] = self.stats['chart_data']['like_counts'][-max_points:]
                self.stats['chart_data']['comment_counts'] = self.stats['chart_data']['comment_counts'][-max_points:]
            
            # 只在DEBUG级别记录分钟数据，减少日志量
            logger.debug(f"记录分钟数据: {current_minute}, 观众: {self.stats['current_viewers']}, 点赞: {self.stats['like_count']}")
    
    def _send_data_updates(self):
        """发送数据更新"""
        socketio.emit('stream_data', {
            'unique_id': self.unique_id,
            'is_live': self.is_live,
            'room_info': self.stats['room_info'],
            'title': self.stats['title'],
            'current_viewers': self.stats['current_viewers'],
            'total_viewers': self.stats['total_viewers'],
            'total_likes': self.stats['like_count'],
            'total_gifts': self.stats['gift_count'],
            'total_comments': self.stats['comment_count'],
            'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
            'chart_data': self.stats['chart_data'],
            'comments': self.stats['comments'],  # 现在只有10条
            'gifts': self.stats['gifts'],        # 现在只有10条
            'status': 'live' if self.is_live else 'offline'
        })
    
    async def _random_delay(self):
        """添加随机延迟（仅在启用防检测时）"""
        if self.anti_detection:
            import random
            delay = random.uniform(
                self.anti_detection_config['random_delay_min'],
                self.anti_detection_config['random_delay_max']
            )
            logger.debug(f"防检测随机延迟: {delay:.2f}秒")
            await asyncio.sleep(delay)
    
    async def start_monitoring(self):
        """启动监控"""
        self.is_running = True
        # 用于跟踪连续失败次数
        consecutive_failures = 0
        
        # 如果启用防检测，先进行随机延迟
        if self.anti_detection:
            await self._random_delay()
        
        # 尝试连接，如果失败则每30秒重试一次
        while self.is_running:
            try:
                connection_result = await self.check_and_connect()
                
                if connection_result:
                    # 连接成功，重置失败计数
                    consecutive_failures = 0
                    logger.info(f"@{self.unique_id} 连接成功，开始监控")
                    
                    # 连接成功，定期检查直播状态
                    while self.is_running and self.client.connected:
                        # 使用防检测配置的间隔时间
                        if self.anti_detection:
                            import random
                            interval = random.randint(
                                self.anti_detection_config['request_interval_min'],
                                self.anti_detection_config['request_interval_max']
                            )
                            logger.debug(f"防检测模式，下次检查间隔: {interval}秒")
                        else:
                            interval = 60
                        
                        await asyncio.sleep(interval)
                        
                        # 防检测模式下添加轻微随机延迟
                        if self.anti_detection:
                            import random
                            await asyncio.sleep(random.uniform(0.5, 2))
                        
                        # 只在还连接时才更新房间信息
                        if self.is_running and self.client.connected:
                            await self.fetch_room_info()  # 定期更新房间信息
                else:
                    # 连接失败，增加失败计数并使用指数退避策略
                    consecutive_failures += 1
                    
                    # 基础等待时间是30秒，最多等待5分钟
                    base_wait = 30
                    if self.anti_detection:
                        # 防检测模式下使用稍长的等待时间
                        base_wait = 45
                    
                    wait_time = min(base_wait * (2 ** (consecutive_failures - 1)), 300)
                    logger.info(f"@{self.unique_id} 连接失败，{wait_time}秒后重试 (连续失败: {consecutive_failures}次)")
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                logger.error(f"@{self.unique_id} 监控循环出错: {str(e)}")
                consecutive_failures += 1
                await asyncio.sleep(30)  # 发生异常时等待30秒再重试
    
    async def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        try:
            if self.client and self.client.connected:
                logger.info(f"停止监控 @{self.unique_id}")
                await self.client.disconnect()
                # 等待断开完成
                await asyncio.sleep(0.5)
            # 断开连接后保存统计数据
            await self.save_stats()
        except Exception as e:
            logger.error(f"停止监控时出错 @{self.unique_id}: {str(e)}")
            # 即使出错也要保存统计数据
            await self.save_stats()
    
    async def save_stats(self):
        """保存优化后的统计数据到文件"""
        try:
            # 创建一个优化的数据结构用于保存
            save_data = {
                'unique_id': self.unique_id,
                'room_id': self.stats['room_id'],
                'title': self.stats['title'],
                'stream_status': self.stats['stream_status'],
                'create_time': self.stats['create_time'],
                'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
                'end_time': datetime.now().isoformat(),
                'duration': (datetime.now() - self.stats['start_time']).total_seconds() if self.stats.get('start_time') else 0,
                'peak_viewers': self.stats['current_viewers'],  # 当前观众作为峰值观众
                'total_viewers': self.stats['total_viewers'],
                'like_count': self.stats['like_count'],
                'comment_count': self.stats['comment_count'],
                'gift_count': self.stats['gift_count'],
                'gift_value': self.stats['gift_value'],
                'follow_count': self.stats['follow_count'],
                'share_count': self.stats['share_count'],
                'subscribe_count': self.stats['subscribe_count'],
                # 优化后的数据
                'minute_data': self.stats['minute_data'],  # 分钟级数据记录
                'recent_comments': self.stats['comments'],  # 最近10条评论
                'recent_gifts': self.stats['gifts'],       # 最近10条礼物
                'gifts_summary': {                          # 礼物汇总而非详细记录
                    'total_gifts': self.stats['gift_count'],
                    'total_value': self.stats['gift_value'],
                    'unique_gift_types': len(self.stats['gifts_detail'])
                },
                'anti_detection_used': self.anti_detection,
                'data_optimized': True  # 标记这是优化后的数据格式
            }
            
            # 保存到传统stats目录（向后兼容）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stats/{self.unique_id}_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"优化后的统计数据已保存到 {filename}")
            
            # 同时保存到历史数据管理系统
            history_file = history_manager.save_session_data(self.unique_id, save_data)
            if history_file:
                logger.info(f"历史数据已保存到 {history_file}")
                
        except Exception as e:
            logger.error(f"保存统计数据失败: {str(e)}")


def monitor_thread_func(unique_id):
    """监控线程函数"""
    try:
        logger.info(f"监控线程已启动: {unique_id}")
        monitor = active_monitors[unique_id]
        asyncio.run(monitor.start_monitoring())
    except Exception as e:
        logger.error(f"监控线程异常: {str(e)}", exc_info=True)


# Flask 路由
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_monitor', methods=['POST'])
def start_monitor():
    try:
        data = request.get_json()
        if not data:
            logger.error("请求数据为空或格式错误")
            return jsonify({'error': '请求数据格式错误', 'status': 'error'}), 400
        
        unique_id = data.get('unique_id', '').strip()
        sessionid = data.get('sessionid', '').strip() or None
        anti_detection = data.get('anti_detection', False)
        logger.info(f"收到开始监控请求: {unique_id}, sessionid: {'已提供' if sessionid else '未提供'}, 防检测: {anti_detection}")
    
        if not unique_id:
            logger.warning("没有提供用户名")
            return jsonify({'error': '请提供用户名', 'status': 'error'}), 400
        
        # 移除可能的@符号
        if unique_id.startswith('@'):
            unique_id = unique_id[1:]
            logger.info(f"移除@符号，处理后的ID: {unique_id}")
        
        # 检查是否已经在监控中
        if unique_id in active_monitors:
            logger.warning(f"用户 {unique_id} 已在监控中")
            return jsonify({
                'message': f'用户 @{unique_id} 已在监控中',
                'unique_id': unique_id,
                'status': 'success'  # 返回成功状态，避免前端认为出错
            })
        
        try:
            # 创建新的监控实例
            logger.info(f"创建监控实例: {unique_id}")
            monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection)
            active_monitors[unique_id] = monitor
        
            # 在新线程中启动监控
            logger.info(f"启动监控线程: {unique_id}")
            thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
            thread.start()
            monitor_threads[unique_id] = thread
        
            # 保存监控配置
            config_manager.add_monitor_to_config(unique_id, sessionid, anti_detection)
        
            # 立即返回成功状态，并发送初始数据
            logger.info(f"发送初始数据: {unique_id}")
            monitor._send_default_data()
        
            logger.info(f"成功启动监控: {unique_id}")
            return jsonify({
                'message': f'开始监控 @{unique_id}',
                'unique_id': unique_id,
                'status': 'success'
            })
        except Exception as e:
            logger.error(f"创建或启动监控实例失败: {str(e)}", exc_info=True)
            return jsonify({'error': str(e), 'status': 'error'}), 500
    except Exception as e:
        logger.error(f"处理开始监控请求时发生未预期错误: {str(e)}", exc_info=True)
        return jsonify({'error': '服务器处理请求时出错', 'status': 'error'}), 500

@app.route('/stop_monitor', methods=['POST'])
def stop_monitor():
    data = request.get_json()
    unique_id = data.get('unique_id', '').strip()
    
    if not unique_id:
        return jsonify({'error': '请提供用户名', 'status': 'error'}), 400
        
    # 移除可能的@符号
    if unique_id.startswith('@'):
        unique_id = unique_id[1:]
        
    if unique_id not in active_monitors:
        return jsonify({'error': '该用户未被监控', 'status': 'error'}), 400
        
    try:
        # 获取监控实例
        monitor = active_monitors[unique_id]
        
        # 停止监控
        asyncio.run(monitor.stop_monitoring())
        
        # 从字典中移除
        del active_monitors[unique_id]
        if unique_id in monitor_threads:
            del monitor_threads[unique_id]
        
        # 从配置中移除
        config_manager.remove_monitor_from_config(unique_id)
            
        return jsonify({
            'message': f'已停止监控 @{unique_id}',
            'unique_id': unique_id,
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"停止监控失败: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500

@app.route('/stop_all', methods=['POST'])
def stop_all():
    try:
        stopped_count = 0
        failed_count = 0
        
        # 停止所有监控
        for unique_id, monitor in list(active_monitors.items()):
            try:
                logger.info(f"正在停止监控: @{unique_id}")
                asyncio.run(monitor.stop_monitoring())
                stopped_count += 1
            except Exception as e:
                logger.error(f"停止监控 @{unique_id} 失败: {str(e)}")
                failed_count += 1
        
        # 清空字典
        active_monitors.clear()
        monitor_threads.clear()
        
        # 清空配置
        config_manager.save_monitor_config([])
        
        message = f'已停止 {stopped_count} 个监控'
        if failed_count > 0:
            message += f'，{failed_count} 个停止失败'
            
        return jsonify({
            'message': message,
            'stopped_count': stopped_count,
            'failed_count': failed_count,
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"停止所有监控失败: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500

# 添加一个别名，以兼容前端请求
@app.route('/stop_all_monitors', methods=['POST'])
def stop_all_monitors():
    return stop_all()

@app.route('/list_monitors', methods=['GET'])
def get_active_monitors():
    return jsonify({
        'monitors': list(active_monitors.keys())
    })

@app.route('/get_stream_data', methods=['GET'])
def get_stream_data():
    unique_id = request.args.get('unique_id', '').strip()
    
    # 处理错误的请求参数
    if not unique_id or unique_id == "monitors":
        return jsonify({
            'is_live': False,
            'current_viewers': 0,
            'total_viewers': 0,
            'total_likes': 0,
            'total_gifts': 0,
            'total_comments': 0,
            'room_info': {},
            'chart_data': {'timestamps': [], 'viewer_counts': []},
            'comments': [],
            'gifts': [],
            'status': 'not_found'
        })
    
    # 移除可能的@符号
    if unique_id.startswith('@'):
        unique_id = unique_id[1:]
        
    # 如果直播间不存在，返回默认数据而不是404
    if unique_id not in active_monitors:
        return jsonify({
            'unique_id': unique_id,
            'is_live': False,
            'current_viewers': 0,
            'total_viewers': 0,
            'total_likes': 0,
            'total_gifts': 0,
            'total_comments': 0,
            'room_info': {},
            'chart_data': {'timestamps': [], 'viewer_counts': []},
            'comments': [],
            'gifts': [],
            'status': 'not_found'
        })
        
    monitor = active_monitors[unique_id]
    
    # 构建前端所需的数据结构
    room_info = monitor.stats.get('room_info', {})
    
    return jsonify({
        'unique_id': unique_id,
        'is_live': monitor.is_live,
        'room_info': room_info,
        'current_viewers': monitor.stats['current_viewers'],
        'total_viewers': monitor.stats['total_viewers'],
        'total_likes': monitor.stats['like_count'],
        'total_gifts': monitor.stats['gift_count'],
        'total_comments': monitor.stats['comment_count'],
        'start_time': monitor.stats['start_time'].isoformat() if monitor.stats.get('start_time') else None,
        'chart_data': monitor.stats['chart_data'],
        'comments': monitor.stats['comments'][-20:],  # 仅发送最新的20条
        'gifts': monitor.stats['gifts'][-20:],        # 仅发送最新的20条
        'status': 'success'
    })

@app.route('/update_chart_data', methods=['GET'])
def update_chart_data():
    data = {}
    for unique_id, monitor in active_monitors.items():
        data[unique_id] = {
            'current_viewers': monitor.stats['current_viewers'],
            'like_count': monitor.stats['like_count'],
            'comment_count': monitor.stats['comment_count'],
            'gift_count': monitor.stats['gift_count'],
            'gift_value': monitor.stats['gift_value'],
            'follow_count': monitor.stats['follow_count'],
            'share_count': monitor.stats['share_count'],
            'subscribe_count': monitor.stats['subscribe_count'],
            'is_live': monitor.is_live,
            'chart_data': monitor.stats['chart_data']
        }
    return jsonify(data)

@app.route('/monitor_info/<unique_id>')
def monitor_info(unique_id):
    """
    渲染指定 unique_id 的房间信息和礼物列表（读取 JSON 文件）
    """
    file_path = f"{unique_id}_tiktoklive_info.json"
    if not os.path.exists(file_path):
        return f"信息文件 {file_path} 未找到", 404
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    room_info = json.dumps(data.get('room_info', {}), indent=2, ensure_ascii=False)
    gift_info = json.dumps(data.get('gift_info', {}), indent=2, ensure_ascii=False)
    return render_template('monitor_info.html', unique_id=unique_id, room_info=room_info, gift_info=gift_info)

# 历史数据相关路由
@app.route('/history')
def history_page():
    """历史数据分析页面"""
    return render_template('history.html')

@app.route('/api/history/user/<unique_id>')
def get_user_history(unique_id):
    """获取指定用户的历史数据"""
    days = request.args.get('days', 7, type=int)
    days = min(max(days, 1), 90)  # 限制在1-90天之间
    
    history = history_manager.get_user_history(unique_id, days)
    return jsonify({
        'status': 'success',
        'unique_id': unique_id,
        'days': days,
        'data': history
    })

@app.route('/api/history/summary')
def get_daily_summary():
    """获取每日汇总数据"""
    date = request.args.get('date')  # 格式: YYYY-MM-DD
    
    summaries = history_manager.get_all_users_summary(date)
    return jsonify({
        'status': 'success',
        'date': date or datetime.now().strftime('%Y-%m-%d'),
        'data': summaries
    })

@app.route('/api/history/export')
def export_history():
    """导出历史数据"""
    unique_id = request.args.get('unique_id')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    try:
        export_file = history_manager.export_data(unique_id, start_date, end_date)
        if export_file:
            # 返回文件下载
            directory = os.path.dirname(export_file)
            filename = os.path.basename(export_file)
            return send_from_directory(directory, filename, as_attachment=True)
        else:
            return jsonify({'error': '没有找到要导出的数据'}), 404
    except Exception as e:
        logger.error(f"导出数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/users')
def get_monitored_users():
    """获取所有被监控过的用户列表"""
    try:
        users = []
        daily_dir = os.path.join('history', 'daily')
        
        if os.path.exists(daily_dir):
            # 获取所有日志文件
            for filename in os.listdir(daily_dir):
                if filename.endswith('.json'):
                    # 从文件名提取用户ID
                    parts = filename.split('_')
                    if len(parts) >= 2:
                        user_id = '_'.join(parts[:-1])  # 去掉日期部分
                        if user_id not in users:
                            users.append(user_id)
        
        return jsonify({
            'status': 'success',
            'users': sorted(users)
        })
    except Exception as e:
        logger.error(f"获取用户列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/analytics/<unique_id>')
def get_user_analytics(unique_id):
    """获取用户数据分析"""
    try:
        days = request.args.get('days', 30, type=int)
        history = history_manager.get_user_history(unique_id, days)
        
        if not history:
            return jsonify({
                'status': 'success',
                'unique_id': unique_id,
                'analytics': {
                    'total_sessions': 0,
                    'avg_duration': 0,
                    'peak_viewers': 0,
                    'total_likes': 0,
                    'best_time': None,
                    'trend': 'stable'
                }
            })
        
        # 计算分析数据
        total_sessions = sum(day.get('total_sessions', 0) for day in history)
        total_duration = sum(day.get('total_duration', 0) for day in history)
        avg_duration = total_duration / total_sessions if total_sessions > 0 else 0
        peak_viewers = max(day.get('peak_viewers', 0) for day in history)
        total_likes = sum(day.get('total_likes', 0) for day in history)
        
        # 找到最佳直播时间
        best_hour = 0
        max_avg_viewers = 0
        for day in history:
            hourly_stats = day.get('hourly_stats', {})
            for hour, stats in hourly_stats.items():
                if stats.get('avg_viewers', 0) > max_avg_viewers:
                    max_avg_viewers = stats['avg_viewers']
                    best_hour = int(hour)
        
        # 计算趋势
        if len(history) >= 2:
            recent_avg = sum(day.get('peak_viewers', 0) for day in history[:7]) / min(7, len(history))
            old_avg = sum(day.get('peak_viewers', 0) for day in history[-7:]) / min(7, len(history))
            
            if recent_avg > old_avg * 1.1:
                trend = 'rising'
            elif recent_avg < old_avg * 0.9:
                trend = 'falling'
            else:
                trend = 'stable'
        else:
            trend = 'stable'
        
        analytics = {
            'total_sessions': total_sessions,
            'avg_duration': round(avg_duration / 60, 1),  # 转换为分钟
            'peak_viewers': peak_viewers,
            'total_likes': total_likes,
            'best_time': f"{best_hour:02d}:00",
            'trend': trend,
            'days_analyzed': len(history)
        }
        
        return jsonify({
            'status': 'success',
            'unique_id': unique_id,
            'analytics': analytics
        })
        
    except Exception as e:
        logger.error(f"获取用户分析失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 配置管理相关路由
@app.route('/api/config/load_monitors', methods=['POST'])
def load_saved_monitors():
    """加载保存的监控配置并启动"""
    try:
        configs = config_manager.load_monitor_config()
        
        if not configs:
            return jsonify({
                'status': 'success',
                'message': '没有保存的监控配置',
                'loaded_count': 0
            })
        
        loaded_count = 0
        failed_list = []
        
        for config in configs:
            unique_id = config['unique_id']
            sessionid = config.get('sessionid')
            anti_detection = config.get('anti_detection', False)
            
            # 检查是否已经在监控中
            if unique_id in active_monitors:
                logger.info(f"监控 {unique_id} 已在运行中，跳过")
                continue
            
            try:
                # 创建监控实例
                monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection)
                active_monitors[unique_id] = monitor
                
                # 启动监控线程
                thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
                thread.start()
                monitor_threads[unique_id] = thread
                
                # 发送初始数据
                monitor._send_default_data()
                
                loaded_count += 1
                logger.info(f"成功加载监控: {unique_id}")
                
            except Exception as e:
                logger.error(f"加载监控 {unique_id} 失败: {str(e)}")
                failed_list.append({'unique_id': unique_id, 'error': str(e)})
        
        return jsonify({
            'status': 'success',
            'message': f'成功加载 {loaded_count} 个监控配置',
            'loaded_count': loaded_count,
            'failed_count': len(failed_list),
            'failed_list': failed_list
        })
        
    except Exception as e:
        logger.error(f"加载监控配置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/config/get_saved_monitors', methods=['GET'])
def get_saved_monitors():
    """获取保存的监控配置列表"""
    try:
        configs = config_manager.load_monitor_config()
        return jsonify({
            'status': 'success',
            'configs': configs,
            'count': len(configs)
        })
    except Exception as e:
        logger.error(f"获取监控配置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 历史数据清理相关路由
@app.route('/api/history/clear', methods=['POST'])
def clear_history_data():
    """清除历史数据"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id')  # 可选，不提供则清除所有
        confirm_token = data.get('confirm_token')  # 必须提供确认令牌
        
        result = history_manager.clear_history_data(unique_id, confirm_token)
        
        if result['success']:
            return jsonify({
                'status': 'success',
                'message': result['message'],
                'deleted_files': result['deleted_files']
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result['message'],
                'error': result.get('error')
            }), 400
            
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"清除历史数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/stats', methods=['GET'])
def get_history_stats():
    """获取历史数据使用统计"""
    try:
        stats = history_manager.get_data_usage_stats()
        if stats:
            return jsonify({
                'status': 'success',
                'stats': stats
            })
        else:
            return jsonify({'error': '获取统计数据失败'}), 500
    except Exception as e:
        logger.error(f"获取历史数据统计失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 自动加载配置的启动函数
def auto_load_monitors():
    """应用启动时自动加载监控配置"""
    try:
        logger.info("正在自动加载保存的监控配置...")
        configs = config_manager.load_monitor_config()
        
        if not configs:
            logger.info("没有保存的监控配置")
            return
        
        loaded_count = 0
        for config in configs:
            unique_id = config['unique_id']
            sessionid = config.get('sessionid')
            anti_detection = config.get('anti_detection', False)
            
            try:
                # 创建监控实例（但不立即连接，等待首次检查）
                monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection)
                active_monitors[unique_id] = monitor
                
                # 启动监控线程
                thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
                thread.start()
                monitor_threads[unique_id] = thread
                
                loaded_count += 1
                logger.info(f"自动加载监控: {unique_id}")
                
            except Exception as e:
                logger.error(f"自动加载监控 {unique_id} 失败: {str(e)}")
        
        logger.info(f"自动加载完成，成功加载 {loaded_count} 个监控")
        
    except Exception as e:
        logger.error(f"自动加载监控配置失败: {str(e)}")

if __name__ == '__main__':
    # 打印启动信息和Web UI访问地址
    print("=" * 60)
    print("🚀 TikTok 直播监控系统启动中...")
    print("=" * 60)
    print()
    print("📱 Web UI 访问地址:")
    print("  本地访问: http://localhost:5000")
    print("  本地访问: http://127.0.0.1:5000")
    print("  局域网访问: http://0.0.0.0:5000")
    print()
    print("🔧 功能特性:")
    print("  ✓ 实时直播数据监控")
    print("  ✓ 历史数据分析")
    print("  ✓ 配置管理")
    print("  ✓ 数据导出")
    print("  ✓ 自动日志轮转 (最大1MB)")
    print()
    print("📋 日志级别控制:")
    print("  生产环境: $env:LOG_LEVEL = 'WARNING'")
    print("  调试模式: $env:LOG_LEVEL = 'DEBUG'")
    print()
    print("=" * 60)
    print()
    
    # 启动时自动加载监控配置
    auto_load_monitors()
    
    # 启动Flask应用
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\n\n👋 程序已停止，感谢使用！")
    except Exception as e:
        print(f"\n❌ 程序启动失败: {str(e)}")
        print("请检查端口5000是否被占用或查看错误日志")