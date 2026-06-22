# -*- coding:utf-8 -*-
import os
import sys
import threading
import websocket
import datetime
import hashlib
import base64
import hmac
import json
from urllib.parse import urlencode
import ssl
from wsgiref.handlers import format_date_time
from datetime import datetime
from time import mktime
import pyaudio
from threading import Event, Lock
import jieba
from jieba import analyse
import time
import queue
import logging

# ================== 讯飞听写配置区域 ==================
APP_ID = ""
API_KEY = ""
API_SECRET = ""

FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 16000
CHUNK = 1280

STATUS_FIRST_FRAME = 0
STATUS_CONTINUE_FRAME = 1
STATUS_LAST_FRAME = 2

# ================== 日志配置 ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

# ================== 全局变量 ==================
total_result = ""
ws_control_lock = Lock()
audio_queue = queue.Queue(maxsize=30)
stop_event = Event()

# ================== WebSocket管理类 ==================
class WSManager:
    def __init__(self):
        self.ws = None
        self.is_connected = False
        self.create_url()
        self.create_connection()

    def create_url(self):
        now = datetime.now()
        date = format_date_time(mktime(now.timetuple()))
        signature_origin = f"host: ws-api.xfyun.cn\ndate: {date}\nGET /v2/iat HTTP/1.1"
        signature_sha = hmac.new(
            API_SECRET.encode('utf-8'),
            signature_origin.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        signature_sha = base64.b64encode(signature_sha).decode('utf-8')
        authorization_origin = f'api_key="{API_KEY}", algorithm="hmac-sha256", headers="host date request-line", signature="{signature_sha}"'
        authorization = base64.b64encode(authorization_origin.encode('utf-8')).decode('utf-8')
        v = {"authorization": authorization, "date": date, "host": "ws-api.xfyun.cn"}
        self.url = 'wss://ws-api.xfyun.cn/v2/iat?' + urlencode(v)

    def create_connection(self):
        with ws_control_lock:
            if self.ws:
                try:
                    self.ws.close()
                except:
                    pass
                self.ws = None

            self.ws = websocket.WebSocketApp(
                self.url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            self.ws.on_open = self.on_open
            self.is_connected = False

    def connect(self):
        def run():
            try:
                self.ws.run_forever(
                    sslopt={
                        "cert_reqs": ssl.CERT_NONE,
                        "ssl_version": ssl.PROTOCOL_TLSv1_2
                    },
                    ping_interval=15,
                    ping_timeout=5
                )
            except Exception as e:
                logging.error(f"连接异常: {str(e)}")
            finally:
                self.is_connected = False

        with ws_control_lock:
            if not self.is_connected:
                conn_thread = threading.Thread(target=run, daemon=True)
                conn_thread.start()
                time.sleep(2)
                self.is_connected = True if self.ws.sock and self.ws.sock.connected else False

    def safe_send(self, message):
        with ws_control_lock:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                try:
                    self.ws.send(message)
                except Exception as e:
                    logging.error(f"发送失败: {str(e)}")

    def close_connection(self):
        with ws_control_lock:
            if self.ws:
                try:
                    self.ws.close()
                except Exception as e:
                    logging.error(f"关闭连接异常: {str(e)}")
                finally:
                    self.ws = None
                    self.is_connected = False

    # ===== 回调函数 =====
    def on_message(self, ws, message):
        global total_result
        try:
            data = json.loads(message)
            code = data.get("code", -1)

            # 新增错误码10165处理
            if code == 10165:
                logging.warning("检测到静默中断，执行安全退出")
                stop_event.set()
                return
            elif code != 0:
                error_msg = f"错误码：{code}，信息：{data.get('message')}"
                logging.warning(error_msg)
                return

            result = data.get("data", {}).get("result", {})
            text = "".join([w["cw"][0]["w"] for w in result.get("ws", [])])
            pgs = result.get("pgs", "apd")

            if pgs == "rpl":
                total_result = text
            elif pgs == "apd":
                total_result += text
            elif pgs == "end":
                total_result = text
                self.save_result(total_result)

            if text.strip():
                self.process_keywords(text, realtime=True)

            print(f"\r实时结果: {total_result}", end='', flush=True)

        except Exception as e:
            logging.error(f"解析异常: {str(e)}")

    def on_error(self, ws, error):
        logging.error(f"连接错误: {str(error)}")
        stop_event.set()

    def on_close(self, ws, *args):
        logging.info("连接关闭")
        self.save_result(total_result, final=True)
        self.clean_audio_queue()
        logging.info("收到停止指令")
        stop_event.set()

    def on_open(self, ws):
        logging.info("连接建立")
        self.is_connected = True
        global total_result
        total_result = ""

    # ===== 功能方法 =====
    def clean_audio_queue(self):
        while not audio_queue.empty():
            try:
                audio_queue.get_nowait()
            except queue.Empty:
                break

    def process_keywords(self, text, realtime=False):
        try:
            keywords = analyse.extract_tags(
                text,
                topK=5,
                allowPOS=()  # 移除了withWeight参数
            )

            if realtime:
                # 直接使用关键词字符串，不显示权重
                print(f"\n[实时关键词] {' | '.join(keywords)}")

            with open("keywords.txt", "a", encoding="utf-8") as f:
                # 直接写入关键词字符串，不包含权重信息
                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | " +
                        " | ".join(keywords) + "\n")

        except Exception as e:
            logging.error(f"关键词处理失败: {str(e)}")

    def save_result(self, text, final=False):
        if not text.strip():
            return

        try:
            with open("recognized_text.txt", "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {text}\n")

            if final:
                logging.info("最终结果已保存")
                self.process_keywords(text)

        except Exception as e:
            logging.error(f"保存失败: {str(e)}")

# ================== 音频处理模块 ==================
class AudioProcessor:
    def __init__(self):
        self.p = pyaudio.PyAudio()
        self.stream = None
        self._setup()

    def _setup(self):
        try:
            self.stream = self.p.open(
                format=FORMAT,
                channels=CHANNELS,
                rate=RATE,
                input=True,
                frames_per_buffer=CHUNK,
                stream_callback=self.callback,
                start=False
            )
        except Exception as e:
            logging.error(f"音频设备初始化失败: {str(e)}")
            self.p.terminate()
            sys.exit(1)

    def start_recording(self):
        try:
            logging.info("开始录音（按回车键停止）")
            self.stream.start_stream()
        except Exception as e:
            logging.error(f"录音启动失败: {str(e)}")

    def callback(self, in_data, frame_count, time_info, status):
        if not stop_event.is_set():
            try:
                audio_queue.put_nowait(base64.b64encode(in_data).decode())
            except queue.Full:
                logging.warning("音频队列已满，丢弃数据")
        return (None, pyaudio.paContinue)

    def stop_recording(self):
        try:
            if self.stream:
                if self.stream.is_active():
                    self.stream.stop_stream()
                self.stream.close()
            self.p.terminate()
            logging.info("音频资源已释放")
        except Exception as e:
            logging.error(f"资源释放失败: {str(e)}")

# ================== 安全退出函数 ==================
def clean_exit(ws_manager, audio_processor):
    """安全退出程序"""
    logging.info("正在清理资源...")
    try:
        audio_processor.stop_recording()
        ws_manager.close_connection()
        while not audio_queue.empty():
            audio_queue.get_nowait()
        with open('keywords.txt', 'a') as file:
            file.write("over\n")
        logging.info("=== 系统已安全关闭 ===")
    except Exception as e:
        logging.error(f"退出时发生异常: {str(e)}")
    finally:
        os._exit(0)

# ================== 主程序流程 ==================
def keyboard_listener(stop_event):
    try:
        input()  # 阻塞直到输入回车
    except EOFError:
        pass
    logging.info("收到停止指令")
    stop_event.set()

if __name__ == "__main__":
    # ================== 初始化配置 ==================
    try:
        jieba.load_userdict("user_dict.txt")
    except FileNotFoundError:
        logging.warning("未找到user_dict.txt")

    try:
        analyse.set_stop_words("stop_words.txt")
    except FileNotFoundError:
        logging.warning("未找到stop_words.txt")

    # ================== 启动系统 ==================
    ws_manager = WSManager()
    ws_manager.connect()

    audio_processor = AudioProcessor()
    audio_processor.start_recording()

    keyboard_thread = threading.Thread(
        target=keyboard_listener,
        args=(stop_event,),
        daemon=False
    )
    keyboard_thread.start()

    # ================== 主循环 ==================
    try:
        first_frame_sent = False

        while not stop_event.is_set():
            try:
                data = audio_queue.get(timeout=1)

                if not first_frame_sent:
                    msg = json.dumps({
                        "common": {"app_id": APP_ID},
                        "business": {
                            "domain": "iat",
                            "language": "zh_cn",
                            "accent": "mandarin",
                            "vinfo": 1,
                            "vad_eos": 10000,
                            "nunum": 1,
                            "ptt": 0
                        },
                        "data": {
                            "status": STATUS_FIRST_FRAME,
                            "format": "audio/L16;rate=16000",
                            "audio": data,
                            "encoding": "raw"
                        }
                    })
                    first_frame_sent = True
                else:
                    msg = json.dumps({
                        "data": {
                            "status": STATUS_CONTINUE_FRAME,
                            "format": "audio/L16;rate=16000",
                            "audio": data,
                            "encoding": "raw"
                        }
                    })

                ws_manager.safe_send(msg)

            except queue.Empty:
                continue  # 移除超时检测逻辑
            except Exception as e:
                logging.error(f"发生严重错误: {str(e)}")
                clean_exit(ws_manager, audio_processor)

    except KeyboardInterrupt:
        logging.info("用户中断操作")
    finally:
        clean_exit(ws_manager, audio_processor)
        keyboard_thread.join()
