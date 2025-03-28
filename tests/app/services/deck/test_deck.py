from app.services.deck.deck import Deck
from app.services.unitofwork import factory
from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)
deck = Deck()


def test_eer_submarket_map(test_settings):
    val = deck.eer_submarket_map(uow)
    assert val.shape == (12, 4)


def test_pdo_sist(test_settings):
    val = deck.pdo_sist(uow)
    assert val.shape == (350, 27)


def test_pdo_sist_sbm(test_settings):
    val = deck.pdo_sist_sbm("cmo", uow)
    assert val.shape == (350, 8)


def test_pdo_sist_sin(test_settings):
    val = deck.pdo_sist_sin("geracao_termica", uow)
    assert val.shape == (70, 7)


def test_pdo_hidr(test_settings):
    val = deck.pdo_hidr(uow)
    assert val.shape == (11410, 44)


def test_pdo_hidr_hydro(test_settings):
    val = deck.pdo_hidr_hydro("valor_agua", uow)
    assert val.shape == (11410, 10)


def test_pdo_hidr_eer(test_settings):
    val = deck.pdo_hidr_eer("valor_agua", uow)
    assert val.shape == (840, 9)


def test_pdo_hidr_sbm(test_settings):
    val = deck.pdo_hidr_sbm("valor_agua", uow)
    assert val.shape == (280, 8)


def test_pdo_hidr_sin(test_settings):
    val = deck.pdo_hidr_sin("valor_agua", uow)
    assert val.shape == (70, 7)


def test_pdo_eolica_sbm(test_settings):
    val = deck.pdo_eolica_sbm("geracao", uow)
    assert val.shape == (280, 8)


def test_pdo_eolica_sin(test_settings):
    val = deck.pdo_eolica_sin("geracao", uow)
    assert val.shape == (70, 7)


def test_pdo_inter_sbp(test_settings):
    val = deck.pdo_inter_sbp("intercambio", uow)
    assert val.shape == (840, 9)


def test_pdo_oper_term_ute(test_settings):
    val = deck.pdo_oper_term_ute("geracao", uow)
    assert val.shape == (6370, 9)


def test_pdo_operacao_costs(test_settings):
    val = deck.pdo_operacao_costs("custo_presente", uow)
    assert val.shape == (70, 7)
