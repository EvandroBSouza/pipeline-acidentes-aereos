# -----------------------------------------------------
# 1. Configuração padrão de Logs
# -----------------------------------------------------

import logging
from datetime import datetime

def configurar_log():
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )

def log(msg):
    logging.info(msg)