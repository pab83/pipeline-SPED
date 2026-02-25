import os

# Variable privada al módulo
_current_log_file = "default.log"

def set_log_file(path):
    global _current_log_file
    _current_log_file = path

def log(msg, logs_buffer=None):
    """Escribe en el archivo configurado actualmente"""
    with open(_current_log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)
    if logs_buffer is not None:
        logs_buffer.append(msg)