import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional
import json
import os
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent,
    CommentEvent, GiftEvent, LikeEvent, FollowEvent,
    JoinEvent, ShareEvent, SubscribeEvent,
    RoomUserSeqEvent
)

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

# 初始化Flask应用
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 存储所有活跃的监控实例
active_monitors = {}

class TikTokLiveMonitor:
    """TikTok 直播监控类"""
    
    def __init__(self, unique_id: str):
        """
        初始化监控器
        
        Args:
            unique_id: TikTok 用户名 (不含@符号)
        """
        self.unique_id = unique_id
        self.client: Optional[TikTokLiveClient] = None
        
        # 统计数据
        self.stats = {
            'start_time': None,
            'room_id': None,
            'viewer_count': 0,
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
        }
        
        # 用于存储用户信息
        self.viewers = set()
        
    async def initialize(self):
        """初始化客户端并设置事件监听器"""
        try:
            # 创建客户端
            self.client = TikTokLiveClient(
                unique_id=self.unique_id,
            )
            
            # 设置事件监听器
            self._setup_event_listeners()
            
            logger.info(f"初始化 TikTok 监控器: @{self.unique_id}")
            
        except Exception as e:
            logger.error(f"初始化失败: {e}")
            raise
            
    def _setup_event_listeners(self):
        """设置所有事件监听器"""
        
        @self.client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            """连接事件"""
            self.stats['start_time'] = datetime.now()
            self.stats['room_id'] = self.client.room_id
            logger.info(f"已连接到直播间 @{event.unique_id} (房间ID: {self.client.room_id})")
            
            # 获取房间信息
            if hasattr(self.client, 'room_info') and self.client.room_info:
                self.stats['room_info'] = self.client.room_info
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
            logger.info("直播已结束")
            socketio.emit('live_end', {'unique_id': self.unique_id})
            await self.save_stats()
            
        @self.client.on(RoomUserSeqEvent)
        async def on_room_user_seq(event: RoomUserSeqEvent):
            """房间用户数更新事件"""
            self.stats['viewer_count'] = event.viewer_count
            self.stats['total_viewers'] = max(self.stats['total_viewers'], event.viewer_count)
            socketio.emit('viewer_count', {
                'unique_id': self.unique_id,
                'count': event.viewer_count
            })
            
        @self.client.on(JoinEvent)
        async def on_join(event: JoinEvent):
            """用户加入事件"""
            user_id = event.user.user_id
            if user_id not in self.viewers:
                self.viewers.add(user_id)
                socketio.emit('user_join', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    }
                })
                
        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            """评论事件"""
            self.stats['comment_count'] += 1
            socketio.emit('comment', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                },
                'comment': event.comment,
                'timestamp': datetime.now().isoformat()
            })
            
        @self.client.on(LikeEvent)
        async def on_like(event: LikeEvent):
            """点赞事件"""
            self.stats['like_count'] += event.like_count
            socketio.emit('like', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                },
                'count': event.like_count
            })
            
        @self.client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            """礼物事件"""
            gift_name = event.gift.name
            gift_count = event.gift.repeat_count
            
            # 更新礼物统计
            self.stats['gift_count'] += gift_count
            
            # 记录礼物详情
            if gift_name not in self.stats['gifts_detail']:
                self.stats['gifts_detail'][gift_name] = {
                    'count': 0,
                    'users': []
                }
            
            self.stats['gifts_detail'][gift_name]['count'] += gift_count
            self.stats['gifts_detail'][gift_name]['users'].append({
                'nickname': event.user.nickname,
                'count': gift_count,
                'time': datetime.now().isoformat()
            })
            
            # 如果礼物有价值信息，累加价值
            if hasattr(event.gift, 'diamond_count'):
                self.stats['gift_value'] += event.gift.diamond_count * gift_count
                
            # 发送礼物事件到前端
            socketio.emit('gift', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                },
                'gift': {
                    'name': gift_name,
                    'count': gift_count,
                    'streaking': event.streaking
                }
            })
                
        @self.client.on(FollowEvent)
        async def on_follow(event: FollowEvent):
            """关注事件"""
            self.stats['follow_count'] += 1
            socketio.emit('follow', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                }
            })
            
        @self.client.on(ShareEvent)
        async def on_share(event: ShareEvent):
            """分享事件"""
            self.stats['share_count'] += 1
            socketio.emit('share', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                }
            })
            
        @self.client.on(SubscribeEvent)
        async def on_subscribe(event: SubscribeEvent):
            """订阅事件"""
            self.stats['subscribe_count'] += 1
            socketio.emit('subscribe', {
                'unique_id': self.unique_id,
                'user': {
                    'nickname': event.user.nickname,
                    'user_id': event.user.user_id
                }
            })
            
    async def start_monitoring(self, fetch_room_info: bool = True, fetch_gift_info: bool = True):
        """开始监控直播"""
        try:
            logger.info(f"开始监控 @{self.unique_id} 的直播...")
            
            # 连接到直播间
            await self.client.connect(
                fetch_room_info=fetch_room_info,
                fetch_gift_info=fetch_gift_info
            )
            
        except Exception as e:
            logger.error(f"监控过程中出错: {e}")
            socketio.emit('error', {
                'unique_id': self.unique_id,
                'error': str(e)
            })
            await self.save_stats()
            
    async def stop_monitoring(self):
        """停止监控"""
        if self.client:
            await self.client.disconnect()
            await self.save_stats()
            
    async def save_stats(self):
        """保存统计数据"""
        try:
            # 确保stats目录存在
            os.makedirs('stats', exist_ok=True)
            
            # 保存统计数据
            filename = f"stats/{self.unique_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, ensure_ascii=False, indent=2)
                
            logger.info(f"统计数据已保存到: {filename}")
            
        except Exception as e:
            logger.error(f"保存统计数据时出错: {e}")

# Flask路由
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_monitor', methods=['POST'])
def start_monitor():
    data = request.get_json()
    unique_id = data.get('unique_id')
    
    if not unique_id:
        return jsonify({'error': '请提供用户名'}), 400
        
    if unique_id in active_monitors:
        return jsonify({'error': '该用户已在监控中'}), 400
        
    try:
        monitor = TikTokLiveMonitor(unique_id)
        asyncio.run(monitor.initialize())
        asyncio.run(monitor.start_monitoring())
        active_monitors[unique_id] = monitor
        return jsonify({'message': f'开始监控 @{unique_id}'})
    except Exception as e:
        logger.error(f"启动监控失败: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/stop_monitor', methods=['POST'])
def stop_monitor():
    data = request.get_json()
    unique_id = data.get('unique_id')
    
    if not unique_id:
        return jsonify({'error': '请提供用户名'}), 400
        
    if unique_id not in active_monitors:
        return jsonify({'error': '该用户未被监控'}), 400
        
    try:
        monitor = active_monitors[unique_id]
        asyncio.run(monitor.stop_monitoring())
        del active_monitors[unique_id]
        return jsonify({'message': f'已停止监控 @{unique_id}'})
    except Exception as e:
        logger.error(f"停止监控失败: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/stop_all', methods=['POST'])
def stop_all():
    try:
        for monitor in active_monitors.values():
            asyncio.run(monitor.stop_monitoring())
        active_monitors.clear()
        return jsonify({'message': '已停止所有监控'})
    except Exception as e:
        logger.error(f"停止所有监控失败: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/list_monitors', methods=['GET'])
def get_active_monitors():
    return jsonify({
        'monitors': list(active_monitors.keys())
    })

@app.route('/update_chart_data', methods=['GET'])
def update_chart_data():
    data = {}
    for unique_id, monitor in active_monitors.items():
        data[unique_id] = {
            'viewer_count': monitor.stats['viewer_count'],
            'like_count': monitor.stats['like_count'],
            'comment_count': monitor.stats['comment_count'],
            'gift_count': monitor.stats['gift_count'],
            'gift_value': monitor.stats['gift_value'],
            'follow_count': monitor.stats['follow_count'],
            'share_count': monitor.stats['share_count'],
            'subscribe_count': monitor.stats['subscribe_count']
        }
    return jsonify(data)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)