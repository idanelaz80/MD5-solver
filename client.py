"""
Client - מפענח MD5 בשיטת Client-Server
=========================================
כל Client אחראי על ניצול מלא של משאבי המחשב שעליו הוא רץ: הוא בודק כמה
ליבות (cores) יש למעבד, ומפעיל תהליך (Process) נפרד על כל ליבה - כך שכל
הליבות עובדות ב-100% תפוסה (multiprocessing, לא threading, כדי לעקוף
את ה-GIL של פייתון).

כל תהליך פותח חיבור TCP משלו אל השרת, מבקש ממנו נתח עבודה (Block),
מחשב MD5 עבור כל מספר בנתח ומשווה לערך המבוקש. אם לא נמצא - מבקש נתח
הבא, וכך הלאה עד שנמצא פתרון (על ידי מישהו) או שנגמר מרחב החיפוש.

הרצה:
    python client.py <SERVER_IP> [PORT]

דוגמה:
    python client.py 127.0.0.1 5050
"""

import socket
import json
import hashlib
import multiprocessing
import os
import sys

DIGITS = 10

def get_local_ip():
    """מחזיר את כתובת ה-IP המקומית של המחשב ברשת (לא 127.0.0.1)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))   # לא נשלח בפועל שום מידע, רק "לבדוק" את הממשק
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def worker(core_index: int, server_host: str, server_port: int, local_ip: str):
    client_id = f"{local_ip} - {core_index + 1}"   # לדוגמה: "192.168.4.93 - 10"

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_host, server_port))
        rfile = sock.makefile("r", encoding="utf-8")
        wfile = sock.makefile("w", encoding="utf-8")

        # הזדהות מול השרת
        wfile.write(json.dumps({"type": "HELLO", "client_id": client_id}) + "\n")
        wfile.flush()
        welcome = json.loads(rfile.readline())
        target_md5 = welcome["target_md5"]

        while True:
            wfile.write(json.dumps({"type": "GET_BLOCK"}) + "\n")
            wfile.flush()
            line = rfile.readline()
            if not line:
                break
            msg = json.loads(line)

            if msg["type"] == "STOP":
                # או שנמצא פתרון על ידי תהליך אחר, או שמרחב החיפוש נגמר
                break

            elif msg["type"] == "BLOCK":
                start, end = msg["start"], msg["end"]
                print(f"[{client_id}] קיבלתי נתח עבודה: {start:0{DIGITS}d} - {end:0{DIGITS}d}")

                found_number = None
                for n in range(start, end + 1):
                    candidate = f"{n:0{DIGITS}d}"
                    if hashlib.md5(candidate.encode()).hexdigest() == target_md5:
                        found_number = candidate
                        break

                if found_number is not None:
                    wfile.write(json.dumps(
                        {"type": "FOUND", "number": found_number, "client_id": client_id}
                    ) + "\n")
                    wfile.flush()
                    rfile.readline()  # ACK מהשרת
                    print(f"[{client_id}] *** מצאתי את הפתרון: {found_number} ***")
                    break

        sock.close()

    except Exception as e:
        print(f"[{client_id}] שגיאה בתהליך: {e}")


def main():
    server_host = "127.0.0.1"  # <-- שימו כאן את ה-IP של השרת
    server_port = 5050

    local_ip = get_local_ip()
    num_cores = multiprocessing.cpu_count()
    print(f"[CLIENT] IP מקומי: {local_ip} | מספר ליבות שזוהו: {num_cores}")
    print(f"[CLIENT] מפעיל {num_cores} תהליכים (אחד לכל ליבה), מתחבר ל-{server_host}:{server_port}\n")

    processes = []
    for i in range(num_cores):
        p = multiprocessing.Process(target=worker, args=(i, server_host, server_port, local_ip))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print(f"[CLIENT] ({local_ip}) כל התהליכים סיימו לעבוד.")


if __name__ == "__main__":
    main()