import os
import time
import random
import subprocess
import requests
import json
import signal
import threading
import queue
from pathlib import Path
from fastapi import FastAPI, Response
import uvicorn
from multiprocessing import Process
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Shared configuration 
VIDEO_DIR = "."
DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK')
hls_output = "stream.m3u8"
hls_segment_prefix = "stream"
current_video_queue = queue.Queue()

# FastAPI app for video streaming
app = FastAPI()

def send_discord_notification(video_name):
    if not DISCORD_WEBHOOK:
        print("No se ha configurado DISCORD_WEBHOOK")
        return
        
    try:
        data = {
            "content": f"🎥 Ahora reproduciendo: **{video_name}**"
        }
        response = requests.post(
            DISCORD_WEBHOOK,
            data=json.dumps(data),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print(f"Notificación enviada: {video_name}")
    except Exception as e:
        print(f"Error al enviar notificación a Discord: {e}")

def get_video_files():
    video_files = []
    for ext in ['.mp4', '.mkv', '.avi', '.mov']:
        video_files.extend(list(Path(VIDEO_DIR).glob(f'*{ext}')))
    return [f for f in video_files if f.is_file()]

def create_concat_file(video_files):
    random.shuffle(video_files)
    with open('playlist.txt', 'w', encoding='utf-8') as f:
        for video_file in video_files:
            safe_path = str(video_file).replace("'", "'\\''")
            f.write(f"file '{safe_path}'\n")
    return video_files

def make_ffmpeg_executable():
    try:
        subprocess.run(["chmod", "+x", "./ffmpeg"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error al cambiar permisos de FFmpeg: {e}")

def notification_worker():
    while True:
        try:
            video_name = current_video_queue.get()
            send_discord_notification(video_name)
            current_video_queue.task_done()
        except Exception as e:
            print(f"Error en notification_worker: {e}")
        time.sleep(0.1)

def stream_video():
    notification_thread = threading.Thread(target=notification_worker, daemon=True)
    notification_thread.start()

    while True:
        try:
            video_files = get_video_files()
            if not video_files:
                print("No se encontraron archivos de video en el directorio.")
                time.sleep(10)
                continue

            playlist = create_concat_file(video_files)
            
            for video_file in playlist:
                try:
                    video_name = video_file.stem
                    print(f"Reproduciendo: {video_name}")
                    
                    current_video_queue.put(video_name)
                    
                    ffmpeg_command = [
                        './ffmpeg',
                        '-re',
                        '-i', str(video_file),
                        '-c:v', 'libx264',
                        '-c:a', 'aac',
                        '-b:v', '5000k',
                        '-b:a', '128k',
                        '-s', '1280x720',
                        '-preset', 'veryfast',
                        '-f', 'hls',
                        '-hls_time', '10',
                        '-hls_list_size', '6',
                        '-hls_flags', 'append_list+omit_endlist',
                        '-hls_segment_filename', f'{hls_segment_prefix}%03d.ts',
                        '-strict', 'experimental',
                        hls_output
                    ]
                    
                    process = subprocess.Popen(
                        ffmpeg_command, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE
                    )
                    
                    # Esperar a que termine el video
                    process.wait()
                        
                except Exception as e:
                    print(f"Error procesando {video_name}: {e}")
                    time.sleep(1)
                    continue
                    
        except Exception as e:
            print(f"Error en la transmisión: {e}")
            time.sleep(5)

class StreamManager:
    def __init__(self):
        self.LOCAL_HLS = 'http://0.0.0.0:7860/stream.m3u8'
        self.RTMP_URL = os.getenv('RTMP_URL', 'rtmp://xxx')
        self.last_frame_time = time.time()
        self.frame_count = 0
        self.start_time = None
        self.process = None
        self.stats_thread = None
        self.should_restart = threading.Event()
        self.error_count = 0
        self.last_error_time = time.time()
        self.video_check_interval = 5
        self.last_video_check = time.time()
        self.video_failure_threshold = 15
        self.stream_start_time = time.time()
        self.max_stream_duration = 7200

    def check_video_stream(self):
        try:
            response = requests.get(self.LOCAL_HLS, timeout=5)
            return response.status_code == 200
        except:
            return False

    def monitor_stream(self):
        while not self.should_restart.is_set() and self.process and self.process.poll() is None:
            current_time = time.time()
            if current_time - self.last_video_check >= self.video_check_interval:
                self.last_video_check = current_time
                if not self.check_video_stream():
                    print("\n⚠️ Error: Video stream no disponible")
                    self.should_restart.set()
                    break
            time.sleep(1)

    def check_stream_duration(self):
        while not self.should_restart.is_set() and self.process and self.process.poll() is None:
            current_time = time.time()
            duration = current_time - self.stream_start_time
            
            if duration >= self.max_stream_duration:
                print("\n📢 Stream duration reached 2 hours - Initiating planned restart...")
                self.should_restart.set()
                break
                
            time.sleep(10)

    def log_streaming_stats(self):
        if self.start_time:
            elapsed_time = time.time() - self.start_time
            fps = self.frame_count / elapsed_time if elapsed_time > 0 else 0
            remaining_time = max(0, self.max_stream_duration - (time.time() - self.stream_start_time))
            print(f"Estadísticas de streaming - Frames: {self.frame_count}, "
                  f"Tiempo: {elapsed_time:.2f}s, FPS: {fps:.2f}, "
                  f"Tiempo hasta reinicio: {remaining_time/60:.1f} minutos")

    def stats_worker(self):
        while not self.should_restart.is_set() and self.process and self.process.poll() is None:
            self.log_streaming_stats()
            time.sleep(10)

    def handle_error(self, error_msg):
        current_time = time.time()
        if current_time - self.last_error_time > 300:
            self.error_count = 0
        
        self.error_count += 1
        self.last_error_time = current_time
        
        wait_time = min(self.error_count * 5, 30)
        print(f"\n⚠️ Error detectado: {error_msg}")
        print(f"Esperando {wait_time} segundos antes de reintentar...")
        time.sleep(wait_time)

    def start_streaming(self):
        print(f"\nIniciando configuración de streaming:")
        print(f"Video source: {self.LOCAL_HLS}")
        print(f"RTMP destination: {self.RTMP_URL}")
        print(f"Duración máxima del stream: 2 horas")

        while True:
            try:
                self.should_restart.clear()
                self.frame_count = 0
                self.start_time = time.time()
                self.stream_start_time = time.time()
                self.last_frame_time = time.time()
                self.last_video_check = time.time()

                if not self.check_video_stream():
                    print("⚠️ Video stream no disponible. Esperando...")
                    time.sleep(5)
                    continue

                ffmpeg_cmd = [
                    'ffmpeg',
                    '-hide_banner',
                    '-re',
                    '-i', self.LOCAL_HLS,
                    '-c:v', 'copy',
                    '-c:a', 'copy',
                    '-f', 'flv',
                    '-flvflags', 'no_duration_filesize',
                    '-flags', '+global_header',
                    '-reconnect', '1',
                    '-reconnect_at_eof', '1',
                    '-reconnect_streamed', '1',
                    '-reconnect_delay_max', '30',
                    self.RTMP_URL
                ]

                print("\n" + "="*50)
                print("Iniciando nueva sesión de streaming...")
                print("="*50 + "\n")

                self.process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )

                self.stats_thread = threading.Thread(target=self.stats_worker, daemon=True)
                self.stats_thread.start()

                monitor_thread = threading.Thread(target=self.monitor_stream, daemon=True)
                monitor_thread.start()

                duration_monitor_thread = threading.Thread(target=self.check_stream_duration, daemon=True)
                duration_monitor_thread.start()

                for line in iter(self.process.stderr.readline, ''):
                    print(line, end='', flush=True)
                    
                    if "frame=" in line:
                        self.last_frame_time = time.time()
                        self.frame_count += 1
                    
                    if any(error in line.lower() for error in [
                        "connection refused",
                        "connection reset",
                        "end of file",
                        "invalid data found",
                        "server returned 404",
                        "unable to open resource",
                        "error while decoding"
                    ]):
                        self.handle_error(line)
                        self.should_restart.set()
                        break
                    
                    if self.frame_count > 100:
                        current_time = time.time()
                        if current_time - self.last_frame_time > 15:
                            self.handle_error("Detección de caída de FPS o pérdida de sincronización")
                            self.should_restart.set()
                            break

                print("\nStream interrumpido, reiniciando...")
                self.cleanup()

            except Exception as e:
                self.handle_error(str(e))
                self.cleanup()
                continue

    def cleanup(self):
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except:
                self.process.kill()
            
        try:
            subprocess.run(['pkill', '-9', '-f', 'ffmpeg'])
        except:
            pass
        
        time.sleep(5)

# FastAPI endpoints
@app.on_event("startup")
async def startup_event():
    if not DISCORD_WEBHOOK:
        print("⚠️ ADVERTENCIA: No se ha configurado la variable de entorno DISCORD_WEBHOOK")
    
    make_ffmpeg_executable()
    
    if not get_video_files():
        print("⚠️ ADVERTENCIA: No se encontraron archivos de video en el directorio actual")

@app.get("/")
async def read_root():
    return {
        "message": "Video Streaming Server",
        "status": "running",
        "video_files": len(get_video_files())
    }

@app.get("/stream.m3u8")
def get_m3u8():
    try:
        with open(hls_output, "rb") as f:
            m3u8_content = f.read()
        return Response(content=m3u8_content, media_type="application/vnd.apple.mpegurl")
    except FileNotFoundError:
        return Response(status_code=404, content="Stream not found")

@app.get("/stream{segment}.ts")
def get_segment(segment: int):
    segment_file = f"{hls_segment_prefix}{segment:03d}.ts"
    try:
        with open(segment_file, "rb") as f:
            segment_content = f.read()
        return Response(content=segment_content, media_type="video/mp2t")
    except FileNotFoundError:
        return Response(status_code=404, content="Segment not found")

def cleanup_handler(signum, frame):
    print("\nDeteniendo todos los procesos...")
    try:
        subprocess.run(['pkill', '-9', '-f', 'ffmpeg'])
    except:
        pass
    os._exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)

    # Start video server (FastAPI)
    fastapi_process = Process(target=lambda: uvicorn.run(app, host="0.0.0.0", port=7860))
    fastapi_process.start()
    
    print("Esperando a que el servidor de video esté listo...")
    time.sleep(5)
    
    # Start video streaming process
    video_process = Process(target=stream_video)
    video_process.start()
    
    # Start RTMP streamer
    try:
        rtmp_process = Process(target=lambda: StreamManager().start_streaming())
        rtmp_process.start()
        
        # Keep main process alive
        rtmp_process.join()
    except KeyboardInterrupt:
        cleanup_handler(None, None)
    
    # Wait for all processes to finish
    fastapi_process.join()
    video_process.join()