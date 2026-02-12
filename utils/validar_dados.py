import pandas as pd

def carregar_dados(caminho):
    extensao = caminho.split('.')[-1].lower()
    if extensao == 'csv':
        return pd.read_csv(caminho, sep=';', encoding='utf-8')
    elif extensao in ['xlsx', 'xls', 'xlsm']:
        return pd.read_excel(caminho)
    else:
        raise ValueError('Formato  não suportado') # interrompe o programa