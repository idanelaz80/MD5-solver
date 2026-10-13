"""
Server - מפענח MD5 בשיטת Client-Server
=========================================
השרת אחראי אך ורק על חלוקת עבודה: הוא יודע אילו טווחי מספרים כבר נוסו,
ומחלק לכל תהליך (Process) שמתחבר אליו נתח (Block) חדש של מספרים לבדיקה.
השרת עצמו *לא* מבצע חישובי MD5 - זה תפקידם של תהליכי ה-Client.

גרסה זו כתובה ללא מחלקות (OOP) - כל המצב המשותף (state) מוחזק
במשתנים גלובליים, והגישה אליהם מתואמת באמצעות Lock אחד משותף כדי
למנוע התנגשויות בין Thread-ים שונים (ראו הסבר על Sockets בסוף הקובץ).

הערה חשובה לגבי העיצוב:
כל ליבה (Process) אצל ה-Client פותחת חיבור Socket נפרד משלה אל השרת.
כך השרת מתקשר עם כל תהליך בנפרד (ולא עם "הלקוח" כמקשה אחת), וכך מחשב
בעל 4 ליבות (= 4 תהליכים = 4 חיבורים) יבקש ויקבל אוטומטית כ-4 פי יותר
עבודה ממחשב בעל ליבה אחת, בלי שום לוגיקה מיוחדת.
"""

import socket
import threading
import json
import time

HOST = "0.0.0.0"
DEFAULT_PORT = 5050
DEFAULT_BLOCK_SIZE = 100000     # כמה מספרים נותנים בכל "נתח עבודה"
DIGITS = 10
SPACE_MIN = 1_000_000_000
SPACE_MAX = 9_999_999_999

# ---------------------------------------------------------------------- #
# המצב הגלובלי של השרת (במקום שדות של מחלקה)
# ---------------------------------------------------------------------- #
target_md5 = "EC9C0F7EDCC18A98B1F31853B1813301"
block_size = DEFAULT_BLOCK_SIZE

lock = threading.Lock()
next_start = SPACE_MIN          # המספר הבא שעוד לא חולק לאף client
found = False
found_number = None
found_by = None
start_time = None

known_machines = set()          # מחשבים (IP) שכבר הצטרפו לחישוב

# נתחים שחולקו לתהליך אך הוא התנתק לפני שדיווח שסיים אותם - ממתינים לחלוקה מחדש
requeued_blocks = []


# ---------------------------------------------------------------------- #
def get_next_block():
    """מחזיר (start, end) של נתח העבודה הבא, או None אם אין יותר עבודה.

    נתחים שהוחזרו לתור (requeued_blocks) בעקבות ניתוק של תהליך באמצע
    העבודה עליהם, מחולקים קודם לכל נתח "טרי" חדש - כך לא מפספסים
    אף טווח מספרים שלא נבדק בפועל.
    """
    global next_start
    result = None
    with lock:
        if not found:
            if requeued_blocks:
                result = requeued_blocks.pop(0)
            elif next_start <= SPACE_MAX:
                start = next_start
                end = min(start + block_size - 1, SPACE_MAX)
                next_start = end + 1
                result = (start, end)
    return result


def requeue_block(start: int, end: int, client_id: str):
    """מחזיר נתח עבודה לתור, לאחר שהתהליך שקיבל אותו התנתק לפני שסיים."""
    with lock:
        if not found:
            requeued_blocks.append((start, end))
    print(f"[SERVER] התהליך {client_id} התנתק באמצע הנתח "
          f"{start:0{DIGITS}d}-{end:0{DIGITS}d} - הנתח יוחזר לתור לחלוקה מחדש")


def register_machine(machine_id: str):
    """רושם מחשב חדש (לפי IP) אם זו הפעם הראשונה שהוא מצטרף."""
    global start_time
    with lock:
        if start_time is None:
            start_time = time.time()
        is_new = machine_id not in known_machines
        if is_new:
            known_machines.add(machine_id)
    if is_new:
        print(f"[SERVER] מחשב חדש הצטרף לחישוב: {machine_id}")


def report_found(number: str, client_id: str) -> bool:
    """מדווח על פתרון. מחזיר True אם זהו הדיווח הראשון (הקובע)."""
    global found, found_number, found_by
    is_first = False
    with lock:
        if not found:
            found = True
            found_number = number
            found_by = client_id
            elapsed = time.time() - start_time
            num_machines = len(known_machines)
            is_first = True

    if is_first:
        print("\n================ נמצא פתרון! ================")
        print(f"המחרוזת (המספר) שפוענחה : {number}")
        print(f"מספר המחשבים שהשתתפו    : {num_machines}")
        print(f"זמן עד למציאת הפתרון    : {elapsed:.2f} שניות")
        print(f"המחשב/תהליך שמצא אותו   : {client_id}")
        print("===============================================\n")

    return is_first


# ---------------------------------------------------------------------- #
def handle_connection(conn: socket.socket, addr):
    """רץ ב-Thread נפרד עבור כל תהליך-Client שמתחבר.

    חיבור זה (ה-socket וה-Thread) שייכים אך ורק לתהליך הספציפי שמחובר
    כרגע - תהליכים אחרים (אפילו על אותו מחשב) רצים כל אחד ב-Thread
    וחיבור נפרדים משלו, כך שניתוק כאן לא משפיע עליהם בשום צורה.

    current_block עוקב אחרי הנתח האחרון שנמסר לתהליך הזה ועדיין לא
    קיבלנו ממנו אישור שסיים (אישור = כל הודעה חדשה שמגיעה ממנו, בין אם
    בקשת נתח נוסף ובין אם FOUND). אם החיבור נופל בזמן ש-current_block
    עדיין מוגדר - סימן שהתהליך מת תוך כדי עבודה על אותו נתח, ולכן
    מחזירים אותו לתור חלוקה מחדש.
    """
    machine_ip = addr[0]
    client_id = machine_ip  # יתעדכן לזיהוי המלא לאחר הודעת HELLO
    rfile = conn.makefile("r", encoding="utf-8")
    wfile = conn.makefile("w", encoding="utf-8")
    current_block = None

    try:
        for line in rfile:
            line = line.strip()
            if not line:
                continue
            msg = json.loads(line)
            mtype = msg.get("type")
            current_block = None  # כל הודעה חדשה = הנתח הקודם טופל בהצלחה

            if mtype == "HELLO":
                client_id = msg.get("client_id", machine_ip)
                register_machine(machine_ip)
                response = {"type": "WELCOME", "target_md5": target_md5}
                wfile.write(json.dumps(response) + "\n")
                wfile.flush()

            elif mtype == "GET_BLOCK":
                block = get_next_block()
                if block is None:
                    response = {"type": "STOP"}
                else:
                    current_block = block
                    response = {"type": "BLOCK", "start": block[0], "end": block[1]}
                wfile.write(json.dumps(response) + "\n")
                wfile.flush()

            elif mtype == "FOUND":
                client_id = msg.get("client_id", machine_ip)
                report_found(msg["number"], client_id)
                wfile.write(json.dumps({"type": "ACK"}) + "\n")
                wfile.flush()

    except (ConnectionResetError, BrokenPipeError, json.JSONDecodeError):
        pass
    finally:
        if current_block is not None:
            requeue_block(current_block[0], current_block[1], client_id)
        conn.close()


def main():
    global target_md5, block_size

    target_md5 = "EC9C0F7EDCC18A98B1F31853B1813301".lower()  # <-- שימו כאן את ה-MD5 שלכם
    port = DEFAULT_PORT
    block_size = DEFAULT_BLOCK_SIZE

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((HOST, port))
    sock.listen(100)

    print(f"[SERVER] מאזין על פורט {port}")
    print(f"[SERVER] מחפש מספר בן {DIGITS} ספרות שה-MD5 שלו הוא: {target_md5}")
    print(f"[SERVER] גודל נתח עבודה: {block_size}\n")

    try:
        while True:
            conn, addr = sock.accept()
            t = threading.Thread(target=handle_connection, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[SERVER] נסגר על ידי המשתמש")
    finally:
        sock.close()


if __name__ == "__main__":
    main()