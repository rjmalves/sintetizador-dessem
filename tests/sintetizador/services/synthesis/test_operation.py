from unittest.mock import patch, MagicMock
from sintetizador.services.unitofwork import factory
from sintetizador.services.synthesis.operation import OperationSynthetizer
from datetime import datetime

from tests.conftest import DECK_TEST_DIR

uow = factory("FS", DECK_TEST_DIR)


def test_sintese_cmo_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["CMO_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 35.52


def test_sintese_mer_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["MER_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 36160.35


def test_sintese_mer_sin_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["MER_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 65233.07


def test_sintese_merl_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["MERL_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 31325.35


def test_sintese_merl_sin_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["MERL_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 42182.07


def test_sintese_ghid_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GHID_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 7.54


def test_sintese_ghid_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GHID_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 22206.06


def test_sintese_ghid_sin_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GHID_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 40413.97


def test_sintese_gter_ute_est_pdo_oper_uct(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GTER_UTE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[1, "usina"] == "ANGRA 2"
    assert df.at[1, "estagio"] == 1
    assert df.at[1, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[1, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[1, "valor"] == 1350.00


def test_sintese_gter_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GTER_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 1917.95


def test_sintese_gter_sin_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["GTER_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 3367.95


def test_sintese_earmf_sbm_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["EARMF_SBM_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "submercado"] == "SE"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 114637.76


def test_sintese_earmf_sin_est_pdo_sist(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["EARMF_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 182945.72999999998


def test_sintese_varpf_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["VARPF_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 78.87


def test_sintese_varmf_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["VARMF_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 530.02


def test_sintese_vagua_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["VAGUA_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 35.46


def test_sintese_qtur_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QTUR_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 34.02


def test_sintese_qtur_sin_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QTUR_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 79983.76


def test_sintese_qver_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QVER_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 0.00


def test_sintese_qver_sin_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QVER_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 9239.699999999999


def test_sintese_qinc_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QINC_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "usina"] == "CAMARGOS"
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 41.00


def test_sintese_qafl_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QAFL_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[2, "usina"] == "FUNIL-GRANDE"
    assert df.at[2, "estagio"] == 1
    assert df.at[2, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[2, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[2, "valor"] == 129.0


def test_sintese_qdef_uhe_est_pdo_hidr(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["QDEF_UHE_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[2, "usina"] == "FUNIL-GRANDE"
    assert df.at[2, "estagio"] == 1
    assert df.at[2, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[2, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[2, "valor"] == 124.82


def test_sintese_cop_sin_est_pdo_operacao(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["COP_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 444.809509


def test_sintese_cfu_sin_est_pdo_operacao(test_settings):
    m = MagicMock(lambda df, filename: df)
    with patch(
        "sintetizador.adapters.repository.export.ParquetExportRepository.synthetize_df",
        new=m,
    ):
        synthetizer = OperationSynthetizer()
        synthetizer.synthetize(["CFU_SIN_EST"], uow)
    m.assert_called_once()
    df = m.mock_calls[0].args[0]
    assert df.at[0, "estagio"] == 1
    assert df.at[0, "dataInicio"] == datetime(2022, 9, 3, 0, 0, 0)
    assert df.at[0, "dataFim"] == datetime(2022, 9, 3, 0, 30, 0)
    assert df.at[0, "valor"] == 0
