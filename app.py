from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import pandas as pd
import os


# Função para consultar dados de Produção

def consultar_producao(driver, ano):
    url = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_02'
    driver.get(url)

    try:
        # Colocando esse parte do código para espera do campo - checar se está tudo ocorrendo como espero
        campo_data = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'text_pesq'))
        )
        campo_data.clear()
        campo_data.send_keys(ano)
        campo_data.send_keys(Keys.RETURN)
        time.sleep(3)

        # Coletando os dados da tabela da pagina
        html_pagina = driver.page_source
        soup = BeautifulSoup(html_pagina, 'html.parser')
        tabela = soup.find('table', {'class': 'tb_base tb_dados'})
        if not tabela:
            print(f"Tabela não encontrada para o ano {ano} na aba Produção.")
            return None

        dados_tabela = []
        for linha in tabela.find_all('tr'):
            colunas = linha.find_all('td')
            dados_linha = [coluna.get_text(strip=True) for coluna in colunas]
            if dados_linha:
                dados_linha.append(ano)
                dados_tabela.append(dados_linha)
        return dados_tabela

    except Exception as e:
        print(f"Erro ao consultar os dados de Produção para o ano {ano}: {str(e)}")
        return None


# Função para consultar dados de Processamento, Importação e Exportação com subopções
def consultar_dados_ano_tipo(driver, ano, subopcao, url):
    driver.get(url)
    try:
        botao_subopcao = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//button[@class='btn_sopt' and @name='subopcao' and @value='{subopcao}']"))
        )
        if botao_subopcao.is_displayed():
            botao_subopcao.click()
        else:
            driver.execute_script("arguments[0].scrollIntoView(true);", botao_subopcao)
            driver.execute_script("arguments[0].click();", botao_subopcao)
        time.sleep(3)
        campo_data = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'text_pesq'))
        )
        campo_data.clear()
        campo_data.send_keys(ano)
        campo_data.send_keys(Keys.RETURN)
        time.sleep(3)
        html_pagina = driver.page_source
        soup = BeautifulSoup(html_pagina, 'html.parser')
        tabela = soup.find('table', {'class': 'tb_base tb_dados'})
        if not tabela:
            print(f"Tabela não encontrada para o ano {ano} e subopção {subopcao}.")
            return None
        dados_tabela = []
        for linha in tabela.find_all('tr'):
            colunas = linha.find_all('td')
            dados_linha = [coluna.get_text(strip=True) for coluna in colunas]
            if dados_linha:
                dados_linha.append(ano)
                dados_linha.append(subopcao)
                dados_tabela.append(dados_linha)
        return dados_tabela
    except Exception as e:
        print(f"Erro ao consultar os dados para o ano {ano} e subopção {subopcao}: {str(e)}")
        return None


# Função para consultar dados de Comercialização
def consultar_comercializacao(driver, ano):
    url = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_04'
    driver.get(url)
    campo_data = driver.find_element(By.CLASS_NAME, 'text_pesq')
    campo_data.clear()
    campo_data.send_keys(ano)
    campo_data.send_keys(Keys.RETURN)
    time.sleep(3)
    html_pagina = driver.page_source
    soup = BeautifulSoup(html_pagina, 'html.parser')
    tabela = soup.find('table', {'class': 'tb_base tb_dados'})
    if not tabela:
        print(f"Tabela não encontrada para o ano {ano} na aba Comercialização.")
        return None
    dados_tabela = []
    for linha in tabela.find_all('tr'):
        colunas = linha.find_all('td')
        dados_linha = [coluna.get_text(strip=True) for coluna in colunas]
        if dados_linha:
            dados_linha.append(ano)
            dados_tabela.append(dados_linha)
    return dados_tabela


# Função principal que coleta dados de todas as abas
def coletar_dados():
    options = webdriver.ChromeOptions()
    service = Service('C:/Users/crisg/PycharmProjects/Tech_Challenge_01/chromedriver.exe')
    driver = webdriver.Chrome(service=service, options=options)

    try:
        anos = list(range(1970, 2024))
        diretorio_salvar = 'C:/Users/crisg/Desktop/tabelas/'
        os.makedirs(diretorio_salvar, exist_ok=True)

        # Produção
        dados_producao = []
        for ano in anos:
            print(f"Consultando dados de Produção para o ano {ano}...")
            dados_ano = consultar_producao(driver, ano)
            if dados_ano:
                dados_producao.extend(dados_ano)
        if dados_producao:
            df_producao = pd.DataFrame(dados_producao, columns=['Produto', 'Quantidade', 'Ano'])
            df_producao.to_parquet(os.path.join(diretorio_salvar, 'base_producao.parquet'), engine='pyarrow')

        # Processamento
        url_processamento = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_03'
        subopcoes_processamento = ['subopt_01', 'subopt_02', 'subopt_03', 'subopt_04']
        dados_processamento = []
        for subopcao in subopcoes_processamento:
            for ano in anos:
                print(f"Consultando dados de Processamento para o ano {ano} e subopção {subopcao}...")
                dados_ano_tipo = consultar_dados_ano_tipo(driver, ano, subopcao, url_processamento)
                if dados_ano_tipo:
                    dados_processamento.extend(dados_ano_tipo)
        if dados_processamento:
            df_processamento = pd.DataFrame(dados_processamento, columns=['Cultivar', 'Quantidade', 'Ano', 'Subopcao'])
            df_processamento.to_parquet(os.path.join(diretorio_salvar, 'base_processamento.parquet'), engine='pyarrow')

        # Comercialização
        dados_comercializacao = []
        for ano in anos:
            print(f"Consultando dados de Comercialização para o ano {ano}...")
            dados_ano = consultar_comercializacao(driver, ano)
            if dados_ano:
                dados_comercializacao.extend(dados_ano)
        if dados_comercializacao:
            df_comercializacao = pd.DataFrame(dados_comercializacao, columns=['Produto', 'Quantidade', 'Ano'])
            df_comercializacao.to_parquet(os.path.join(diretorio_salvar, 'base_comercializacao.parquet'),
                                          engine='pyarrow')

        # Importação
        url_importacao = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_05'
        subopcoes_importacao = ['subopt_01', 'subopt_02', 'subopt_03', 'subopt_04', 'subopt_05']
        dados_importacao = []
        for subopcao in subopcoes_importacao:
            for ano in anos:
                print(f"Consultando dados de Importação para o ano {ano} e subopção {subopcao}...")
                dados_ano_tipo = consultar_dados_ano_tipo(driver, ano, subopcao, url_importacao)
                if dados_ano_tipo:
                    dados_importacao.extend(dados_ano_tipo)
        if dados_importacao:
            df_importacao = pd.DataFrame(dados_importacao, columns=['Paises', 'Quantidade', 'Valor', 'Ano', 'Subopcao'])
            df_importacao.to_parquet(os.path.join(diretorio_salvar, 'base_importacao.parquet'), engine='pyarrow')

        # Exportação
        url_exportacao = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_06'
        subopcoes_exportacao = ['subopt_01', 'subopt_02', 'subopt_03', 'subopt_04']
        dados_exportacao = []
        for subopcao in subopcoes_exportacao:
            for ano in anos:
                print(f"Consultando dados de Exportação para o ano {ano} e subopção {subopcao}...")
                dados_ano_tipo = consultar_dados_ano_tipo(driver, ano, subopcao, url_exportacao)
                if dados_ano_tipo:
                    dados_exportacao.extend(dados_ano_tipo)
        if dados_exportacao:
            df_exportacao = pd.DataFrame(dados_exportacao, columns=['Paises', 'Quantidade', 'Valor', 'Ano', 'Subopcao'])
            df_exportacao.to_parquet(os.path.join(diretorio_salvar, 'base_exportacao.parquet'), engine='pyarrow')

    finally:
        driver.quit()


# Executando a coleta de dados
coletar_dados()


