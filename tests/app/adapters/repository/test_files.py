from datetime import datetime

import pandas as pd

from app.adapters.repository.files import factory
from tests.conftest import DECK_TEST_DIR


def test_get_dadvaz(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    dadvaz = repo.get_dadvaz()
    assert dadvaz.data_inicio == datetime(2022, 9, 3)


def test_get_des_log_relato(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    des_log_relato = repo.get_des_log_relato()
    assert des_log_relato.data_estudo == datetime(2022, 9, 3)
    assert des_log_relato.versao == "19.4.5"
    assert isinstance(des_log_relato.variaveis_otimizacao, pd.DataFrame)


def test_get_entdados(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    entdados = repo.get_entdados()
    assert isinstance(entdados.tm(df=True), pd.DataFrame)


def test_get_log_matriz(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    log_matriz = repo.get_log_matriz()
    assert log_matriz.data_estudo == datetime(2022, 9, 3)
    assert log_matriz.versao == "19.4.4"
    assert isinstance(log_matriz.tabela, pd.DataFrame)


def test_get_pdo_eolica(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_eolica()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "20.3"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_hidr(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_hidr()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_inter(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_inter()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.4"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_oper_term(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_oper_term()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_oper_tviag_calha(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_oper_tviag_calha()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "20.3"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_oper_uct(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_oper_uct()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_operacao(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_operacao()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.discretizacao, pd.DataFrame)


def test_get_pdo_sist(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_sist()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_pdo_eco_usih(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    pdo = repo.get_pdo_eco_usih()
    assert pdo.data_estudo == datetime(2022, 9, 3)
    assert pdo.versao == "19.4.5"
    assert isinstance(pdo.tabela, pd.DataFrame)


def test_get_operuh(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    operuh = repo.get_operuh()
    assert isinstance(operuh.rest(df=True), pd.DataFrame)
