import asyncio
import logging
import os
import json
import threading
from datetime import datetime
from typing import Dict, Optional, List, Set
import time
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO

from TikTokLive import TikTokLiveClient
from TikTokLive.client.logger import LogLevel
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent,
    CommentEvent, GiftEvent, LikeEvent, FollowEvent,
    JoinEvent, ShareEvent, SubscribeEvent,
    RoomUserSeqEvent
)
from TikTokLive.client.web.routes.fetch_room_id_live_html import FailedParseRoomIdError

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tiktok_monitor.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建数据存储目录
os.makedirs('stats', exist_ok=True)

# 初始化Flask应用
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 存储所有活跃的监控实例
active_monitors = {}
monitor_threads = {}

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
        
        # 统计数据
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
            'gifts_detail': {},  # 详细的礼物记录
            'room_info': {},     # 房间信息
            'comments': [],      # 评论历史
            'gifts': [],         # 礼物历史
            'chart_data': {
                'timestamps': [],
                'viewer_counts': []
            },
            'stream_url': {},    # 流媒体URL
            'title': "",         # 直播标题
            'cover': {},         # 封面URL
            'stream_status': 0,  # 直播状态
            'create_time': 0     # 创建时间戳
        }
        
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
                            logger.info(f"获取到观众数量: {viewer_count}，使用属性: {attr_name}")
                            break
                        except Exception as e:
                            logger.error(f"获取属性 {attr_name} 时出错: {str(e)}")
                
                # 如果通过第一轮属性未获取到观众数量，尝试total_user作为后备
                if viewer_count == 0 and hasattr(event, "total_user"):
                    try:
                        total_user_count = getattr(event, "total_user")
                        # 记录累计观众数，但不用作当前观众数
                        if total_user_count > 0:
                            logger.info(f"获取到累计观众数量: {total_user_count}，但不作为当前观众数")
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
                            logger.info(f"从字符串中提取的观众数量: {viewer_count}")
                    except Exception as e:
                        logger.error(f"解析字符串时出错: {str(e)}")
                
                # 确保我们至少有一个观众
                if viewer_count == 0:
                    viewer_count = 1
                    logger.warning("无法获取有效的观众数量，设置为默认值1")
                
                # 更新统计和图表数据
                self.stats['current_viewers'] = viewer_count
                
                # 更新图表数据
                # 只有每分钟更新一次图表数据
                current_minute = datetime.now().strftime('%H:%M')
                # 检查是否已经有该分钟的数据点
                if not self.stats['chart_data']['timestamps'] or not self.stats['chart_data']['timestamps'][-1].startswith(current_minute):
                    current_time = datetime.now().strftime('%H:%M:%S')
                    self.stats['chart_data']['timestamps'].append(current_time)
                    self.stats['chart_data']['viewer_counts'].append(self.stats['current_viewers'])
                    
                    # 保持图表数据长度合理
                    max_points = 100
                    if len(self.stats['chart_data']['timestamps']) > max_points:
                        self.stats['chart_data']['timestamps'] = self.stats['chart_data']['timestamps'][-max_points:]
                        self.stats['chart_data']['viewer_counts'] = self.stats['chart_data']['viewer_counts'][-max_points:]
                
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
                
                # 记录评论历史
                timestamp = datetime.now().isoformat()
                comment_data = {
                    'user': user.nickname,
                    'user_id': user_id,
                    'comment': comment_text,
                    'timestamp': timestamp
                }
                self.stats['comments'].append(comment_data)
                
                # 保留最新的50条评论
                if len(self.stats['comments']) > 50:
                    self.stats['comments'] = self.stats['comments'][-50:]
                
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
                
                # 记录点赞信息
                logger.info(f"收到点赞: 用户={user_id}, 数量={like_count}")
                
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
                    
                    # 记录礼物历史
                    gift_data = {
                        'user': user.nickname,
                        'gift_name': gift_name,
                        'gift_count': gift_count,
                        'timestamp': timestamp
                    }
                    self.stats['gifts'].append(gift_data)
                    
                    # 保留最新的50个礼物记录
                    if len(self.stats['gifts']) > 50:
                        self.stats['gifts'] = self.stats['gifts'][-50:]
                    
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
            
            # 保存为JSON文件
            output_file = f"{self.unique_id}_tiktoklive_info.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump({"room_info": room_info, "gift_info": gift_info}, f, ensure_ascii=False, indent=2)
            logger.info(f"已将房间信息写入文件: {output_file}")
            
            # 更新统计数据
            if room_info:
                self.stats['room_info'] = room_info
                
                # 直播状态
                self.stats['stream_status'] = room_info.get('status', 0)
                self.is_live = self.stats['stream_status'] == 2
                
                # 基本信息
                self.stats['title'] = room_info.get('title', '')
                self.stats['cover'] = room_info.get('cover', {})
                self.stats['create_time'] = room_info.get('create_time', 0)
                
                # 流地址
                self.stats['stream_url'] = room_info.get('stream_url', {})
                
                # 统计数据
                if 'stats' in room_info:
                    stats = room_info['stats']
                    # 累计观众
                    self.stats['total_viewers'] = stats.get('total_user', 0)
                    self.stats['like_count'] = stats.get('like_count', 0)
                    self.stats['comment_count'] = stats.get('comment_count', 0)
                    
                # 当前在线人数 - 从room_info['user_count']获取
                self.stats['current_viewers'] = room_info.get('user_count', 0)
                
                # 更新图表数据
                # 只有每分钟更新一次图表数据
                current_minute = datetime.now().strftime('%H:%M')
                # 检查是否已经有该分钟的数据点
                if not self.stats['chart_data']['timestamps'] or not self.stats['chart_data']['timestamps'][-1].startswith(current_minute):
                    current_time = datetime.now().strftime('%H:%M:%S')
                    self.stats['chart_data']['timestamps'].append(current_time)
                    self.stats['chart_data']['viewer_counts'].append(self.stats['current_viewers'])
                    
                    # 保持图表数据长度合理
                    max_points = 100
                    if len(self.stats['chart_data']['timestamps']) > max_points:
                        self.stats['chart_data']['timestamps'] = self.stats['chart_data']['timestamps'][-max_points:]
                        self.stats['chart_data']['viewer_counts'] = self.stats['chart_data']['viewer_counts'][-max_points:]
            
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
            # 如果已经连接，不再重新连接
            if self.client.connected:
                logger.info(f"@{self.unique_id} 已经连接，不再重新连接")
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
            'cover': self.stats['cover'],
            'stream_url': self.stats['stream_url'],
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
    
    def _send_data_updates(self):
        """发送数据更新"""
        socketio.emit('stream_data', {
            'unique_id': self.unique_id,
            'is_live': self.is_live,
            'room_info': self.stats['room_info'],
            'title': self.stats['title'],
            'cover': self.stats['cover'], 
            'stream_url': self.stats['stream_url'],
            'current_viewers': self.stats['current_viewers'],
            'total_viewers': self.stats['total_viewers'],
            'total_likes': self.stats['like_count'],
            'total_gifts': self.stats['gift_count'],
            'total_comments': self.stats['comment_count'],
            'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
            'chart_data': self.stats['chart_data'],
            'comments': self.stats['comments'][-20:],  # 仅发送最新的20条
            'gifts': self.stats['gifts'][-20:],        # 仅发送最新的20条
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
            connection_result = await self.check_and_connect()
            
            if connection_result:
                # 连接成功，重置失败计数
                consecutive_failures = 0
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
                logger.info(f"连接失败，{wait_time}秒后重试 (连续失败: {consecutive_failures}次)")
                await asyncio.sleep(wait_time)
    
    async def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        if self.client and self.client.connected:
            logger.info(f"停止监控 @{self.unique_id}")
            await self.client.disconnect()
            # 断开连接后保存统计数据
            await self.save_stats()
    
    async def save_stats(self):
        """保存统计数据到文件"""
        try:
            # 创建一个干净的字典用于保存
            save_data = {
                'unique_id': self.unique_id,
                'room_id': self.stats['room_id'],
                'title': self.stats['title'],
                'stream_status': self.stats['stream_status'],
                'create_time': self.stats['create_time'],
                'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
                'end_time': datetime.now().isoformat(),
                'duration': (datetime.now() - self.stats['start_time']).total_seconds() if self.stats.get('start_time') else 0,
                'current_viewers': self.stats['current_viewers'],
                'total_viewers': self.stats['total_viewers'],
                'like_count': self.stats['like_count'],
                'comment_count': self.stats['comment_count'],
                'gift_count': self.stats['gift_count'],
                'gift_value': self.stats['gift_value'],
                'follow_count': self.stats['follow_count'],
                'share_count': self.stats['share_count'],
                'subscribe_count': self.stats['subscribe_count'],
                'gifts_detail': self.stats['gifts_detail'],
                'comments': self.stats['comments'],
                'gifts': self.stats['gifts'],
                'chart_data': self.stats['chart_data']
            }
            
            # 保存到文件
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stats/{self.unique_id}_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"统计数据已保存到 {filename}")
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
        # 停止所有监控
        for unique_id, monitor in list(active_monitors.items()):
            try:
                asyncio.run(monitor.stop_monitoring())
            except Exception as e:
                logger.error(f"停止监控 @{unique_id} 失败: {str(e)}")
        
        # 清空字典
        active_monitors.clear()
        monitor_threads.clear()
        
        return jsonify({'message': '已停止所有监控', 'status': 'success'})
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

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)