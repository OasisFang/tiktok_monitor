from flask import Flask, render_template, jsonify, request
import subprocess
import sys
import os
import signal
import atexit
import time
import threading
from datetime import datetime

app = Flask(__name__, static_folder='static')

# 全局变量存储直播数据
all_monitors = {}  # 存储所有活跃的监控进程
all_stream_data = {}  # 存储所有主播的数据

# 默认数据结构
def create_default_data():
    return {
        'is_live': False,
        'start_time': '',
        'current_viewers': 0,
        'total_viewers': 0,
        'total_likes': 0,
        'total_gifts': 0,
        'total_shares': 0,
        'total_follows': 0,
        'gifts': [],
        'comments': [],
        'room_info': {
            'room_id': '',
            'room_title': '',
            'host_name': '',
            'avatar_url': ''
        },
        'chart_data': {
            'timestamps': [],
            'viewer_counts': [],
            'like_counts': [],
            'gift_counts': [],
            'comment_counts': []
        }
    }

# 主页
@app.route('/')
def index():
    return render_template('index.html')

# 获取实时数据摘要
@app.route('/get_summary')
def get_summary():
    # 获取请求中指定的主播ID
    unique_id = request.args.get('unique_id')
    
    # 如果没有指定主播ID，返回所有主播的摘要
    if not unique_id:
        all_summaries = {}
        for uid, data in all_stream_data.items():
            room_info = data.get('room_info', {})
            all_summaries[uid] = {
                'is_live': data.get('is_live', False),
                'room_id': room_info.get('room_id', '-'),
                'room_title': room_info.get('room_title', '-'),
                'host_name': room_info.get('host_name', '-'),
                'status': '在线' if data.get('is_live', False) else '离线',
                'start_time': data.get('start_time', ''),
                'current_viewers': data.get('current_viewers', 0),
                'total_viewers': data.get('total_viewers', 0),
                'total_likes': data.get('total_likes', 0),
                'total_gifts': data.get('total_gifts', 0),
                'total_comments': len(data.get('comments', [])),
                'total_shares': data.get('total_shares', 0),
                'total_follows': data.get('total_follows', 0)
            }
        return jsonify(all_summaries)
    
    # 如果指定了主播ID但不存在数据，返回空数据
    if unique_id not in all_stream_data:
        return jsonify({
            'is_live': False,
            'room_id': '-',
            'room_title': '未找到该主播数据',
            'host_name': '-',
            'status': '离线',
            'start_time': '',
            'current_viewers': 0,
            'total_viewers': 0,
            'total_likes': 0,
            'total_gifts': 0,
            'total_comments': 0,
            'total_shares': 0,
            'total_follows': 0
        })
    
    # 返回指定主播的摘要
    data = all_stream_data[unique_id]
    room_info = data.get('room_info', {})
    return jsonify({
        'is_live': data.get('is_live', False),
        'room_id': room_info.get('room_id', '-'),
        'room_title': room_info.get('room_title', '-'),
        'host_name': room_info.get('host_name', '-'),
        'status': '在线' if data.get('is_live', False) else '离线',
        'start_time': data.get('start_time', ''),
        'current_viewers': data.get('current_viewers', 0),
        'total_viewers': data.get('total_viewers', 0),
        'total_likes': data.get('total_likes', 0),
        'total_gifts': data.get('total_gifts', 0),
        'total_comments': len(data.get('comments', [])),
        'total_shares': data.get('total_shares', 0),
        'total_follows': data.get('total_follows', 0)
    })

# 获取直播数据
@app.route('/get_stream_data')
def get_stream_data():
    unique_id = request.args.get('unique_id')
    
    if not unique_id:
        return jsonify(all_stream_data)
        
    if unique_id not in all_stream_data:
        return jsonify(create_default_data())
        
    return jsonify(all_stream_data[unique_id])

# 每分钟更新一次图表数据
@app.route('/update_chart_data')
def update_chart_data():
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for unique_id, data in all_stream_data.items():
        chart_data = data.get('chart_data', {
            'timestamps': [],
            'viewer_counts': [],
            'like_counts': [],
            'gift_counts': [],
            'comment_counts': []
        })
        
        # 添加新的数据点
        chart_data['timestamps'].append(current_time)
        chart_data['viewer_counts'].append(data.get('current_viewers', 0))
        chart_data['like_counts'].append(data.get('total_likes', 0))
        chart_data['gift_counts'].append(len(data.get('gifts', [])))
        chart_data['comment_counts'].append(len(data.get('comments', [])))
        
        # 保留最近60分钟的数据
        chart_data['timestamps'] = chart_data['timestamps'][-60:]
        chart_data['viewer_counts'] = chart_data['viewer_counts'][-60:]
        chart_data['like_counts'] = chart_data['like_counts'][-60:]
        chart_data['gift_counts'] = chart_data['gift_counts'][-60:]
        chart_data['comment_counts'] = chart_data['comment_counts'][-60:]
        
        # 更新数据
        all_stream_data[unique_id]['chart_data'] = chart_data
        
    return jsonify({'status': 'success'})

# 更新直播数据
@app.route('/update_stream_data', methods=['POST'])
def update_stream_data():
    try:
        data = request.get_json()
        
        # 调试输出
        print(f"接收到更新数据: {data}")
        
        # 获取当前正在更新的主播ID
        unique_id = data.get('unique_id')
        if not unique_id:
            error_msg = '缺少unique_id参数'
            print(f"错误: {error_msg}")
            return jsonify({'status': 'error', 'message': error_msg}), 400
            
        # 移除ID中可能的@符号
        unique_id = unique_id.replace('@', '')
            
        # 如果是该主播的第一次数据更新，创建默认数据结构
        if unique_id not in all_stream_data:
            all_stream_data[unique_id] = create_default_data()
        
        stream_data = all_stream_data[unique_id]
        
        # 处理连接状态和时间
        if 'is_live' in data:
            stream_data['is_live'] = data['is_live']
        if 'start_time' in data:
            stream_data['start_time'] = data['start_time']
        
        # 处理房间信息
        if 'room_info' in data:
            # 将接收到的房间信息与现有房间信息合并
            room_info = data['room_info']
            current_room_info = stream_data.get('room_info', {})
            
            # 只更新提供了的字段
            for key in room_info:
                if room_info[key]:  # 只更新非空值
                    current_room_info[key] = room_info[key]
                    
            stream_data['room_info'] = current_room_info
        
        # 处理观众数量
        if 'current_viewers' in data:
            stream_data['current_viewers'] = data['current_viewers']
        if 'total_viewers' in data:
            stream_data['total_viewers'] = data['total_viewers']
        
        # 处理点赞
        if 'total_likes_increment' in data:
            stream_data['total_likes'] += data['total_likes_increment']
        if 'set_total_likes' in data:
            stream_data['total_likes'] = data['set_total_likes']
        
        # 处理分享和关注
        if 'total_shares_increment' in data:
            stream_data['total_shares'] += data['total_shares_increment']
        if 'total_follows_increment' in data:
            stream_data['total_follows'] += data['total_follows_increment']
        
        # 处理礼物和评论
        if 'add_gift' in data:
            gift_data = data['add_gift']
            stream_data['gifts'].append(gift_data)
            # 获取礼物数量，默认为1
            gift_count = gift_data.get('gift_count', 1)
            stream_data['total_gifts'] += gift_count
            # 限制礼物历史记录数量
            if len(stream_data['gifts']) > 100:
                stream_data['gifts'] = stream_data['gifts'][-100:]
                
        if 'add_comment' in data:
            comment_data = data['add_comment']
            stream_data['comments'].append(comment_data)
            # 限制评论历史记录数量
            if len(stream_data['comments']) > 100:
                stream_data['comments'] = stream_data['comments'][-100:]
        
        # 更新全局数据
        all_stream_data[unique_id] = stream_data
        
        return jsonify({'status': 'success'})
        
    except Exception as e:
        error_msg = f"处理更新数据时出错: {str(e)}"
        print(error_msg)
        return jsonify({'status': 'error', 'message': error_msg}), 500

# 启动监控进程
@app.route('/start_monitor', methods=['POST'])
def start_monitor():
    # 获取TikTok主播ID - 兼容不同的请求格式
    unique_id = None
    
    # 尝试从JSON中获取
    if request.is_json:
        unique_id = request.json.get('unique_id') or request.json.get('room_id')
    
    # 尝试从表单数据中获取
    if not unique_id:
        unique_id = request.form.get('unique_id') or request.form.get('room_id')
    
    # 尝试从URL参数中获取
    if not unique_id:
        unique_id = request.args.get('unique_id') or request.args.get('room_id')
    
    # 尝试从请求体中获取（非典型格式）
    if not unique_id:
        try:
            data = request.get_data(as_text=True)
            if 'room_id=' in data:
                unique_id = data.split('room_id=')[1].split('&')[0]
            elif 'unique_id=' in data:
                unique_id = data.split('unique_id=')[1].split('&')[0]
        except:
            pass
    
    if not unique_id:
        return jsonify({'status': 'error', 'message': '请输入TikTok主播ID'}), 400
    
    # 移除ID中的@符号
    unique_id = unique_id.replace('@', '')
    
    # 如果已经有该主播的监控进程在运行，先停止它
    stop_specific_monitor(unique_id)
    
    # 重置该主播的数据
    all_stream_data[unique_id] = create_default_data()
    
    # 启动新的监控进程
    try:
        monitor_proc = subprocess.Popen(
            [sys.executable, 'monitor.py', f"@{unique_id}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            env={**os.environ, 'PYTHONUNBUFFERED': '1'}  # 确保输出不被缓存
        )
        
        # 保存进程到字典中
        all_monitors[unique_id] = {
            'proc': monitor_proc,
            'start_time': datetime.now().isoformat()
        }
        
        # 后台线程读取进程输出
        def read_output(process, uid):
            while True:
                line = process.stdout.readline()
                if not line and process.poll() is not None:
                    break
                if line:
                    print(f"[{uid}] {line.strip()}")
        
        threading.Thread(target=read_output, args=(monitor_proc, unique_id), daemon=True).start()
        
        return jsonify({
            'status': 'success', 
            'message': f'开始监控 @{unique_id}',
            'unique_id': unique_id
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'启动监控失败: {str(e)}'}), 500

# 停止所有监控进程
@app.route('/stop_all_monitors', methods=['POST'])
def stop_all_monitors():
    success_count = 0
    for uid in list(all_monitors.keys()):
        if stop_specific_monitor(uid):
            success_count += 1
    
    return jsonify({
        'status': 'success', 
        'message': f'已停止 {success_count} 个监控进程'
    })

# 停止指定主播的监控进程
@app.route('/stop_monitor', methods=['POST'])
def stop_monitor():
    unique_id = request.args.get('unique_id')
    if not unique_id:
        if request.is_json:
            unique_id = request.json.get('unique_id')
    
    if not unique_id:
        return jsonify({'status': 'error', 'message': '请指定要停止的TikTok主播ID'}), 400
    
    # 移除ID中的@符号
    unique_id = unique_id.replace('@', '')
    
    if stop_specific_monitor(unique_id):
        return jsonify({'status': 'success', 'message': f'已停止 @{unique_id} 的监控'})
    else:
        return jsonify({'status': 'info', 'message': f'没有找到 @{unique_id} 的监控进程'})

# 列出所有正在监控的主播
@app.route('/list_monitors')
def list_monitors():
    result = {}
    for uid, monitor_info in all_monitors.items():
        result[uid] = {
            'start_time': monitor_info.get('start_time', ''),
            'is_running': monitor_info.get('proc') is not None and monitor_info.get('proc').poll() is None
        }
    return jsonify(result)

# 停止特定主播的监控进程（内部函数）
def stop_specific_monitor(unique_id):
    if unique_id in all_monitors:
        monitor_info = all_monitors[unique_id]
        proc = monitor_info.get('proc')
        if proc and proc.poll() is None:
            try:
                if sys.platform == 'win32':
                    os.kill(proc.pid, signal.CTRL_C_EVENT)
                else:
                    os.kill(proc.pid, signal.SIGTERM)
                
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    proc.wait(timeout=2)
            except:
                pass
                
            # 设置直播状态为离线
            if unique_id in all_stream_data:
                all_stream_data[unique_id]['is_live'] = False
                
            # 从监控列表中删除
            del all_monitors[unique_id]
            return True
    return False

# 退出时清理资源
def cleanup():
    for uid in list(all_monitors.keys()):
        stop_specific_monitor(uid)

atexit.register(cleanup)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 