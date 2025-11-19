"""
Risk Analysis System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli risk analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pyodbc
import logging
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from contextlib import contextmanager

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Log dosyasina yaz (konsol yok)
import os
from pathlib import Path
import sys

# PyInstaller ile freeze edildiginde dosya yollarini duzelt
if getattr(sys, 'frozen', False):
    base_dir = Path(sys.executable).parent
else:
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'risk_analizi.log'

logging.basicConfig(
    level=logging.ERROR,  # Sadece hatalar
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Service Account ve Merkezi Config
# ============================================================================

class RiskAnalysisConfig:
    """
    Service Account ve merkezi config ile yapılandırma

    Artık:
    - Private key kodda YOK
    - Şifreler environment variable'da
    - Ayarlar PRGsheets'ten çekiliyor
    """

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()

            # PRGsheets'ten ayarları yükle
            self.settings = self.config_manager.get_settings()

            logger.info("Config yüklendi")

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            raise

    @property
    def spreadsheet_id(self) -> str:
        """Ana spreadsheet ID (PRGsheets)"""
        return self.config_manager.MASTER_SPREADSHEET_ID

    @property
    def connection_string(self) -> str:
        """
        SQL baglanti string'i
        Tum ayarlar PRGsheet -> Ayar'dan cekilir
        """
        # Gerekli ayarlari kontrol et
        required_settings = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
        missing = [key for key in required_settings if not self.settings.get(key)]

        if missing:
            raise ValueError(
                f"PRGsheet -> Ayar sayfasinda eksik ayarlar: {', '.join(missing)}\n"
                f"Lutfen bu ayarlari Global olarak ekleyin."
            )

        sql_server = self.settings['SQL_SERVER']
        sql_database = self.settings['SQL_DATABASE']
        sql_username = self.settings['SQL_USERNAME']
        sql_password = self.settings['SQL_PASSWORD']

        return (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={sql_server};'
            f'DATABASE={sql_database};'
            f'UID={sql_username};'
            f'PWD={sql_password}'
        )

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Veritabanı işlemleri yöneticisi"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @contextmanager
    def get_connection(self):
        """Context manager ile güvenli bağlantı yönetimi"""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string)
            logger.info("Database connection established")
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")

    def execute_query(
        self,
        connection: pyodbc.Connection,
        query: str,
        params: Optional[tuple] = None
    ) -> List[tuple]:
        """
        Güvenli sorgu çalıştırma (SQL Injection korumalı)

        Args:
            connection: pyodbc bağlantı nesnesi
            query: SQL sorgusu (? placeholder'ları ile)
            params: Parametreler tuple'ı

        Returns:
            Sorgu sonuçları
        """
        try:
            cursor = connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        except pyodbc.Error as e:
            logger.error(f"Query execution error: {e}")
            raise

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yöneticisi

    Artık:
    - Private key YOK
    - Service account YOK
    - Service Account token kullanılıyor
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def get_no_risk_codes(self) -> List[str]:
        """
        PRGsheets → NoRisk sayfasından kodları çek

        """
        try:
            # PRGsheet'ten NoRisk verilerini çek
            data = self.config_manager.get_worksheet_data('PRGsheet', 'NoRisk')

            # İlk kolondan kodları al (header'ı atla)
            codes = [row[0] for row in data[1:] if row and row[0]]

            logger.info(f"{len(codes)} NoRisk kodu yüklendi")
            return codes

        except Exception as e:
            logger.warning(f"NoRisk worksheet hatası: {e}")
            return []

    def update_risk_worksheet(self, data: pd.DataFrame) -> None:
        """
        Risk sayfasına veri yaz

        Args:
            data: Risk verileri (DataFrame)
        """
        try:
            # PRGsheet'i dogrudan ac (Config entry'si gerekmez)
            risk_spreadsheet = self.config_manager.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            # Risk worksheet'i bul veya oluştur
            try:
                risk_worksheet = risk_spreadsheet.worksheet('Risk')
                risk_worksheet.clear()
            except:
                risk_worksheet = risk_spreadsheet.add_worksheet(
                    title='Risk',
                    rows=1000,
                    cols=10
                )

            if not data.empty:
                values = [data.columns.values.tolist()] + data.values.tolist()
                risk_worksheet.update(values, value_input_option='USER_ENTERED')
                logger.info(f"{len(data)} satır Risk sayfasına yazıldı")
            else:
                logger.warning("Risk verisi bulunamadı")

        except Exception as e:
            logger.error(f"Risk worksheet güncelleme hatası: {e}")
            raise

# ============================================================================
# RISK ANALYZER
# ============================================================================

class RiskAnalyzer:
    """Risk analizi ana sınıfı"""

    def __init__(self, config: RiskAnalysisConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def get_high_risk_customers(self, no_risk_codes: List[str]) -> pd.DataFrame:
        """
        Yüksek riskli müşterileri getir

        SQL Injection DÜZELTME:
        - Artık parametreli sorgu kullanılıyor
        - String concatenation YOK

        Args:
            no_risk_codes: Risk dışı kodlar listesi

        Returns:
            High risk müşteri DataFrame'i
        """
        base_query = """
        SELECT
            [msg_S_0088],
            [cariAdi],
            [cariAciklama],
            [cariTelefon],
            [cariKod],
            [cariBakiye]
        FROM
            [dbo].[CARI_HESAPLAR_CHOOSE_3A]
        WHERE
            ([cariBakiye] > 7 OR [cariBakiye] < -7)
        """

        # SQL Injection korumalı NOT IN clause
        if no_risk_codes:
            # Placeholder'lar oluştur: ?, ?, ?, ...
            placeholders = ','.join(['?'] * len(no_risk_codes))
            query = f"{base_query} AND [cariKod] NOT IN ({placeholders})"
        else:
            query = base_query

        high_risk_data = []

        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()

                # Parametreli sorgu çalıştır
                if no_risk_codes:
                    cursor.execute(query, tuple(no_risk_codes))
                else:
                    cursor.execute(query)

                initial_rows = cursor.fetchall()

                for row in initial_rows:
                    cari_kod = row[4]

                    sp_query = """
                    EXEC dbo.sp_SiparisOperasyonlari 0,'20230101','20770711',0,0,2,0,0,0,N'',1,?,0,0,0,1
                    """

                    sp_cursor = connection.cursor()
                    sp_cursor.execute(sp_query, (cari_kod,))

                    sp_has_results = False

                    # Tüm sonuç setlerini tüket
                    while True:
                        try:
                            rows = sp_cursor.fetchall()
                            if rows:
                                sp_has_results = True
                        except pyodbc.ProgrammingError:
                            pass

                        if not sp_cursor.nextset():
                            break

                    sp_cursor.close()

                    if not sp_has_results:
                        siparis_query = """
                        SELECT TOP 1 [#msg_S_1130], [msg_S_0241]
                        FROM dbo.fn_CariSiparisFoyu(?, '20230101', '20771231')
                        ORDER BY [msg_S_0088] DESC
                        """

                        siparis_cursor = connection.cursor()
                        siparis_cursor.execute(siparis_query, (cari_kod,))
                        siparis_result = siparis_cursor.fetchone()
                        siparis_cursor.close()

                        last_delivery_info = siparis_result[0] if siparis_result else None
                        order_date_raw = siparis_result[1] if siparis_result else None

                        order_date = (
                            order_date_raw.strftime('%Y-%m-%d')
                            if isinstance(order_date_raw, datetime)
                            else ''
                        )

                        high_risk_data.append({
                            'msg_S_0088': row[0],
                            'cariAdi': row[1],
                            'cariAciklama': row[2],
                            'cariTelefon': row[3],
                            'cariKod': row[4],
                            'cariBakiye': round(row[5]),
                            'Personel': last_delivery_info,
                            'Tarih': order_date
                        })

            except pyodbc.Error as e:
                logger.error(f"High risk customers hatası: {e}")
                raise

        return pd.DataFrame(high_risk_data)

    def get_pending_risk_customers(self) -> pd.DataFrame:
        """Bekleyen riskli müşterileri getir"""
        with self.db_manager.get_connection() as connection:
            try:
                cursor = connection.cursor()

                sql_query = """
                EXEC dbo.sp_SiparisOperasyonlari 0,'20230101','20770101',0,0,2,0,0,0,N'',1,N'',0,0,0,1
                """

                cursor.execute(sql_query)

                # Sonuç seti olan ilk seti bul
                columns = None
                rows = []

                while True:
                    if cursor.description:
                        columns = [column[0] for column in cursor.description]
                        rows = cursor.fetchall()
                        break

                    if not cursor.nextset():
                        break

                if not columns:
                    cursor.close()
                    return pd.DataFrame()

                df = pd.DataFrame.from_records(rows, columns=columns)
                df.sort_values(by='msg_S_0088', inplace=True)
                df.drop_duplicates(subset=['msg_S_0200'], keep='first', inplace=True)

                final_df = df[['msg_S_0200', 'msg_S_0201', 'msg_S_0241', '#msg_S_1130']].copy()
                final_df['msg_S_0241'] = pd.to_datetime(final_df['msg_S_0241'], errors='coerce').dt.strftime('%Y-%m-%d')
                final_df['msg_S_0241'] = final_df['msg_S_0241'].fillna('')

                all_results = []

                for cari_kod in final_df['msg_S_0200']:
                    detail_query = """
                    SELECT
                        [msg_S_0088],
                        [cariAdi],
                        [cariAciklama],
                        [cariTelefon],
                        [cariKod],
                        [cariBakiye]
                    FROM
                        [dbo].[CARI_HESAPLAR_CHOOSE_3A]
                    WHERE
                        [cariKod] = ?
                    """

                    detail_cursor = connection.cursor()
                    detail_cursor.execute(detail_query, (cari_kod,))
                    results = detail_cursor.fetchall()

                    if results:
                        results_columns = [column[0] for column in detail_cursor.description]
                        results_list_of_dicts = [dict(zip(results_columns, row)) for row in results]

                        for res_dict in results_list_of_dicts:
                            current_cari_kod = res_dict['cariKod']
                            risk_query = """
                            SELECT * FROM dbo.fn_CariRiskFoyu(0,?,'20000101','20000101','20770101',0,N'',0)
                            """

                            risk_cursor = None
                            try:
                                risk_cursor = connection.cursor()
                                risk_cursor.execute(risk_query, (current_cari_kod,))
                                risk_results = risk_cursor.fetchall()

                                s0111_values = []
                                if risk_results and risk_cursor.description:
                                    risk_columns = [column[0] for column in risk_cursor.description]
                                    s1720_index = risk_columns.index('#msg_S_1720') if '#msg_S_1720' in risk_columns else -1
                                    s0111_index = risk_columns.index('msg_S_0111') if 'msg_S_0111' in risk_columns else -1

                                    if s1720_index != -1 and s0111_index != -1:
                                        for risk_row in risk_results:
                                            if str(risk_row[s1720_index]) == '9':
                                                s0111_values.append(risk_row[s0111_index])

                                res_dict['Risk_Verileri'] = s0111_values

                            except pyodbc.Error as e:
                                logger.warning(f"Risk data hatası ({current_cari_kod}): {e}")
                                res_dict['Risk_Verileri'] = []
                            finally:
                                if risk_cursor:
                                    risk_cursor.close()

                            total_risk = sum(res_dict['Risk_Verileri'])
                            res_dict['sonuc'] = round(res_dict['cariBakiye'] + total_risk)
                            all_results.append(res_dict)

                    detail_cursor.close()

                if not all_results:
                    cursor.close()
                    return pd.DataFrame()

                sonuc_df = pd.DataFrame(all_results)
                final_df_for_merge = final_df[['msg_S_0200', 'msg_S_0241', '#msg_S_1130']]
                merged_sonuc_df = pd.merge(
                    sonuc_df,
                    final_df_for_merge,
                    left_on='cariKod',
                    right_on='msg_S_0200',
                    how='left'
                )

                merged_sonuc_df.rename(columns={'msg_S_0241': 'Tarih', '#msg_S_1130': 'Personel'}, inplace=True)
                merged_sonuc_df.drop(columns=['msg_S_0200'], inplace=True)

                filtered_df = merged_sonuc_df[
                    (merged_sonuc_df['sonuc'] < -7) | (merged_sonuc_df['sonuc'] > 7)
                ].copy()

                filtered_df.drop(columns=['Risk_Verileri', 'cariBakiye'], inplace=True)
                filtered_df.rename(columns={'sonuc': 'cariBakiye'}, inplace=True)

                ordered_columns = [
                    'msg_S_0088', 'cariAdi', 'cariAciklama', 'cariTelefon',
                    'cariKod', 'cariBakiye', 'Personel', 'Tarih'
                ]
                existing_columns = [col for col in ordered_columns if col in filtered_df.columns]
                filtered_df = filtered_df[existing_columns]

                cursor.close()
                return filtered_df

            except pyodbc.Error as e:
                logger.error(f"Pending risk customers hatası: {e}")
                raise

    def process_combined_data(
        self,
        high_risk_df: pd.DataFrame,
        pending_risk_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Verileri birleştir ve formatla"""
        combined_df = pd.concat([high_risk_df, pending_risk_df], ignore_index=True)

        if combined_df.empty:
            return combined_df

        if 'msg_S_0088' in combined_df.columns:
            combined_df.drop(columns=['msg_S_0088'], inplace=True)

        if 'cariAciklama' in combined_df.columns:
            combined_df['cariAciklama'] = combined_df['cariAciklama'].fillna('')

        if 'cariKod' in combined_df.columns:
            combined_df['cariKod'] = combined_df['cariKod'].astype(str).apply(
                lambda x: f"'{x}" if pd.notna(x) and x != '' else x
            )

        if 'cariBakiye' in combined_df.columns:
            combined_df['cariBakiye'] = combined_df['cariBakiye'].apply(
                lambda x: int(x) if pd.notna(x) and isinstance(x, (int, float)) and x == int(x) else x
            )

        if 'Tarih' in combined_df.columns:
            combined_df['Tarih'] = pd.to_datetime(combined_df['Tarih'], errors='coerce')
            combined_df['Tarih'] = combined_df['Tarih'].dt.strftime('%Y-%m-%d')
            combined_df['Tarih'] = combined_df['Tarih'].fillna('')

        desired_order = ['cariAdi', 'cariAciklama', 'cariTelefon', 'cariKod', 'cariBakiye', 'Personel', 'Tarih']
        existing_columns = [col for col in desired_order if col in combined_df.columns]
        combined_df = combined_df[existing_columns]

        if 'Personel' in combined_df.columns:
            combined_df.sort_values(by='Personel', ascending=True, inplace=True)

        final_df = combined_df.rename(columns={
            "cariAdi": "Cari hesap adı",
            "cariAciklama": "Cari hesap adı 2",
            "cariTelefon": "cariTelefon",
            "cariKod": "Cari hesap kodu",
            "cariBakiye": "Risk",
            "Personel": "Satıcı Adi",
            "Tarih": "Tarih"
        })

        return final_df

    def run_analysis(self) -> None:
        """Ana analiz workflow'u çalıştır"""
        try:
            logger.info("Risk analizi başlatılıyor...")

            # 1. NoRisk kodlarını çek (PRGsheets'ten - Service Account ile)
            no_risk_codes = self.sheets_manager.get_no_risk_codes()
            logger.info(f"{len(no_risk_codes)} NoRisk kodu bulundu")

            # 2. High risk müşterileri bul
            high_risk_df = self.get_high_risk_customers(no_risk_codes)
            logger.info(f"{len(high_risk_df)} high risk müşteri")

            # 3. Pending risk müşterileri bul
            pending_risk_df = self.get_pending_risk_customers()
            logger.info(f"{len(pending_risk_df)} pending risk müşteri")

            # 4. Verileri birleştir ve işle
            final_df = self.process_combined_data(high_risk_df, pending_risk_df)

            # 5. Risk sayfasına yaz (Service Account ile)
            if not final_df.empty:
                self.sheets_manager.update_risk_worksheet(final_df)
                logger.info("[OK] Risk analizi tamamlandi!")
            else:
                logger.warning("Risk verisi bulunamadi")

        except Exception as e:
            logger.error(f"[HATA] Risk analizi hatasi: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_risk_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasina yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = RiskAnalysisConfig()

        # Analyzer oluştur
        analyzer = RiskAnalyzer(config)

        # Analiz çalıştır
        analyzer.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatasi: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_risk_analysis()
