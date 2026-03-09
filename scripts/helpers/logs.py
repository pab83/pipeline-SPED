import os

# Variable privada al módulo
_current_log_file = "default.log"

def set_log_file(path):
    """ Configura el archivo de log actual. Esta función establece la variable global _current_log_file al path proporcionado, lo que hará que todas las llamadas posteriores a log() escriban en ese archivo. Es útil para configurar un archivo de log específico para cada fase o script del pipeline, permitiendo así mantener los logs organizados y separados por fase o script."""
    global _current_log_file
    _current_log_file = path

def log(msg, logs_buffer=None):
    """Escribe en el archivo de logs configurado actualmente"""
    with open(_current_log_file, "a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)
    if logs_buffer is not None:
        logs_buffer.append(msg)