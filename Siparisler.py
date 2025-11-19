"""
Siparisler Analysis System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli sipariş analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
- Batch processing ile performans optimizasyonu
"""

import pyodbc
import logging
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from contextlib import contextmanager
import time

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
log_file = log_dir / 'siparisler_analizi.log'

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

class SiparislerConfig:
    """
    Siparisler analizi icin yaplandirma sinifi
    Service Account ve merkezi config kullanir
    """

    def __init__(self):
        try:
            # Merkezi config manager olustur (Service Account otomatik baslar)
            self.config_manager = CentralConfigManager()

            # PRGsheet'ten ayarlari yukle
            self.settings = self.config_manager.get_settings()

        except Exception as e:
            logger.error(f"Config yukleme hatasi: {e}")
            raise

    @property
    def spreadsheet_id(self) -> str:
        """PRGsheet spreadsheet ID"""
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
            f'PWD={sql_password};'
            f'TrustServerCertificate=yes;'
            f'Connection Timeout=30;'
        )

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Veritabani baglanti yoneticisi"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.max_retries = 3
        self.retry_delay = 2

    @contextmanager
    def get_connection(self):
        """Guvenli veritabani baglantisi context manager"""
        connection = None
        retry_count = 0

        try:
            while retry_count < self.max_retries:
                try:
                    connection = pyodbc.connect(self.connection_string)
                    yield connection
                    break

                except pyodbc.Error as e:
                    retry_count += 1
                    if retry_count >= self.max_retries:
                        logger.error(f"Veritabani baglanti hatasi: {e}")
                        raise
                    time.sleep(self.retry_delay)

        finally:
            if connection:
                try:
                    connection.close()
                except Exception as e:
                    logger.error(f"Baglanti kapatma hatasi: {e}")

    def execute_query(
        self,
        connection: pyodbc.Connection,
        query: str,
        params: Optional[tuple] = None
    ) -> List[tuple]:
        """Guvenli sorgu calistirici (SQL Injection korumal)"""
        try:
            cursor = connection.cursor()
            cursor.execute(query, params or ())
            return cursor.fetchall()

        except pyodbc.Error as e:
            logger.error(f"Sorgu calistirma hatasi: {e}")
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yoneticisi
    Service account yerine Service Account token kullanir
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmis client
        self.max_retries = 3
        self.retry_delay = 2

    def get_last_two_years_start_date(self) -> str:
        """Son 2 yilin baslangic tarihini YYYYMMDD formatinda dondurur"""
        current_year = datetime.now().year
        two_years_ago = current_year - 2
        return f"{two_years_ago}0101"

    def update_siparisler_worksheet(self, data: pd.DataFrame) -> None:
        """Siparisler calisma sayfasini gunceller"""
        retry_count = 0

        while retry_count < self.max_retries:
            try:
                # PRGsheet'i dogrudan ac (Config entry'si gerekmez)
                spreadsheet = self.config_manager.gc.open_by_key(
                    self.config_manager.MASTER_SPREADSHEET_ID
                )

                # Siparisler sayfasini bul veya olustur
                try:
                    siparisler_worksheet = spreadsheet.worksheet('Siparisler')
                    siparisler_worksheet.clear()
                except:
                    siparisler_worksheet = spreadsheet.add_worksheet(
                        title='Siparisler',
                        rows=2000,
                        cols=25
                    )

                if not data.empty:
                    # NaN degerleri bos string ile degistir
                    data_cleaned = data.fillna('')

                    # "Cari Kod" sütununu string olarak formatla (bilimsel notasyon engelle)
                    if 'Cari Kod' in data_cleaned.columns:
                        data_cleaned['Cari Kod'] = data_cleaned['Cari Kod'].astype(str)

                    # Sutun basliklarini ve verileri hazirla
                    values = [data_cleaned.columns.values.tolist()] + data_cleaned.values.tolist()

                    # Verileri toplu olarak guncelle (RAW: binlik ayraç eklenmez)
                    siparisler_worksheet.update(values, value_input_option='RAW')

                break  # Basarili olursa donguden cik

            except Exception as e:
                retry_count += 1
                if retry_count >= self.max_retries:
                    logger.error(f"Google Sheets guncelleme basarisiz: {e}")
                    raise
                time.sleep(self.retry_delay)

# ============================================================================
# SIPARISLER ANALYZER
# ============================================================================

class SiparislerAnalyzer:
    """Siparisler analiz sinifi"""

    def __init__(self, config: SiparislerConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def _get_cari_unvan_data(
        self,
        connection: pyodbc.Connection,
        cari_codes
    ) -> pd.DataFrame:
        """Cari hesap unvan bilgilerini getirir (batch processing)"""
        try:
            # Numpy array ise listeye cevir
            cari_codes_list = cari_codes.tolist() if hasattr(cari_codes, 'tolist') else list(cari_codes)

            # Bos veya None degerleri filtrele
            clean_codes = [str(code).strip() for code in cari_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

            # SQL Server IN clause limitine karsi batch islem yap (1000'er parca)
            batch_size = 1000
            all_results = []

            for i in range(0, len(clean_codes), batch_size):
                batch_codes = clean_codes[i:i + batch_size]
                placeholders = ', '.join(['?' for _ in batch_codes])

                query = f"""
                SELECT [cari_kod] AS [Cari Kod],
                       ISNULL([cari_unvan1], '') AS [Cari Adi]
                FROM [dbo].[CARI_HESAPLAR]
                WHERE [cari_kod] IN ({placeholders})
                """

                cursor = connection.cursor()
                cursor.execute(query, batch_codes)
                rows = cursor.fetchall()

                if rows:
                    columns = [column[0] for column in cursor.description]
                    batch_df = pd.DataFrame.from_records(rows, columns=columns)
                    all_results.append(batch_df)

                cursor.close()

            # Tum batch sonuclarini birlestir
            if all_results:
                return pd.concat(all_results, ignore_index=True)
            else:
                return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

        except Exception as e:
            logger.error(f"Cari unvan veri hatasi: {e}")
            return pd.DataFrame(columns=['Cari Kod', 'Cari Adi'])

    def _get_personel_data(
        self,
        connection: pyodbc.Connection,
        satici_codes
    ) -> pd.DataFrame:
        """Personel bilgilerini getirir"""
        try:
            satici_codes_list = satici_codes.tolist() if hasattr(satici_codes, 'tolist') else list(satici_codes)
            clean_codes = [str(code).strip() for code in satici_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['cari_per_kod', 'Personel'])

            placeholders = ', '.join(['?' for _ in clean_codes])
            query = f"""
            SELECT cari_per_kod,
                   ISNULL(cari_per_adi, '') + ' ' + ISNULL(cari_per_soyadi, '') AS [Personel]
            FROM [dbo].[CARI_PERSONEL_TANIMLARI]
            WHERE cari_per_kod IN ({placeholders})
            """

            cursor = connection.cursor()
            cursor.execute(query, clean_codes)
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()

            return pd.DataFrame.from_records(rows, columns=columns)

        except Exception as e:
            logger.error(f"Personel veri hatasi: {e}")
            return pd.DataFrame(columns=['cari_per_kod', 'Personel'])

    def _get_stok_data(
        self,
        connection: pyodbc.Connection,
        stok_codes
    ) -> pd.DataFrame:
        """Stok bilgilerini getirir (batch processing)"""
        try:
            stok_codes_list = stok_codes.tolist() if hasattr(stok_codes, 'tolist') else list(stok_codes)
            clean_codes = [str(code).strip() for code in stok_codes_list if code is not None and str(code).strip()]

            if not clean_codes:
                return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

            # Batch islem (1000'er parca)
            batch_size = 1000
            all_results = []

            for i in range(0, len(clean_codes), batch_size):
                batch_codes = clean_codes[i:i + batch_size]
                placeholders = ', '.join(['?' for _ in batch_codes])

                query = f"""
                SELECT [sto_kod],
                       ISNULL([sto_isim], '') AS [Malzeme Adı]
                FROM [dbo].[STOKLAR]
                WHERE [sto_kod] IN ({placeholders})
                """

                cursor = connection.cursor()
                cursor.execute(query, batch_codes)
                rows = cursor.fetchall()

                if rows:
                    columns = [column[0] for column in cursor.description]
                    batch_df = pd.DataFrame.from_records(rows, columns=columns)
                    all_results.append(batch_df)

                cursor.close()

            if all_results:
                return pd.concat(all_results, ignore_index=True)
            else:
                return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

        except Exception as e:
            logger.error(f"Stok veri hatasi: {e}")
            return pd.DataFrame(columns=['sto_kod', 'Malzeme Adı'])

    def get_siparis_data(self, start_date: str) -> pd.DataFrame:
        """Belirtilen tarihten sonraki siparis verilerini getirir"""
        query = """
        SELECT [sip_tarih] AS [Tarih],
               [sip_evrakno_sira] AS [Sipariş No],
               [sip_satirno] AS [Satir],
               [sip_belgeno] AS [Sozlesme],
               [sip_satici_kod] AS [Satici],
               [sip_musteri_kod] AS [Cari Kod],
               [sip_stok_kod] AS [Malzeme Kodu],
               [sip_b_fiyat] AS [Birim Fiyat],
               [sip_miktar] AS [Miktar],
               [sip_teslim_miktar] AS [Teslimat],
               [sip_iskonto_1] AS [Iskonto],
               [sip_vergi_pntr] AS [VergiKod],
               [sip_vergi] AS [Vergi],
               [sip_aciklama] AS [Aciklama],
               [sip_aciklama2] AS [Header],
               [sip_depono] AS [Depo],
               [sip_cari_sormerk] AS [Mağaza]
        FROM [dbo].[SIPARISLER]
        WHERE [sip_tarih] >= ? AND [sip_evrakno_seri] != 'S'
        ORDER BY [sip_evrakno_sira] DESC
        """

        with self.db_manager.get_connection() as connection:
            try:
                # Ana siparis verilerini getir
                cursor = connection.cursor()
                cursor.execute(query, (start_date,))
                columns = [column[0] for column in cursor.description]
                rows = cursor.fetchall()

                df = pd.DataFrame.from_records(rows, columns=columns)

                if df.empty:
                    return df

                # Tarih formatini duzenle
                if 'Tarih' in df.columns:
                    df['Tarih'] = pd.to_datetime(df['Tarih'], errors='coerce').dt.strftime('%Y-%m-%d')
                    df['Tarih'] = df['Tarih'].fillna('')

                # Cari hesap bilgilerini ekle
                if 'Cari Kod' in df.columns:
                    unique_cari_codes = df['Cari Kod'].dropna().unique()
                    if len(unique_cari_codes) > 0:
                        cari_df = self._get_cari_unvan_data(connection, unique_cari_codes)
                        df = pd.merge(df, cari_df, on='Cari Kod', how='left')

                # Personel bilgilerini ekle
                if 'Satici' in df.columns:
                    unique_satici_codes = df['Satici'].dropna().unique()
                    if len(unique_satici_codes) > 0:
                        personel_df = self._get_personel_data(connection, unique_satici_codes)
                        df = pd.merge(df, personel_df, left_on='Satici', right_on='cari_per_kod', how='left')

                        # Gereksiz sutunlari kaldir
                        columns_to_drop = ['cari_per_kod', 'Satici']
                        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

                # Stok bilgilerini ekle
                if 'Malzeme Kodu' in df.columns:
                    unique_stok_codes = df['Malzeme Kodu'].dropna().unique()
                    if len(unique_stok_codes) > 0:
                        stok_df = self._get_stok_data(connection, unique_stok_codes)
                        df = pd.merge(df, stok_df, left_on='Malzeme Kodu', right_on='sto_kod', how='left')

                        if 'sto_kod' in df.columns:
                            df = df.drop(columns=['sto_kod'])

                # Sutun siralamasini yeniden duzenle
                desired_columns = [
                    'Satir', 'Sipariş No', 'Tarih', 'Sozlesme', 'Cari Kod', 'Cari Adi',
                    'Malzeme Kodu', 'Malzeme Adı', 'Miktar', 'Teslimat', 'Depo',
                    'Personel', 'Mağaza', 'Birim Fiyat', 'Vergi', 'Iskonto', 'Header', 'Aciklama'
                ]

                available_columns = [col for col in desired_columns if col in df.columns]
                remaining_columns = [col for col in df.columns if col not in available_columns]
                final_columns = available_columns + remaining_columns

                df = df[final_columns]

                # Siparis No buyukten kucuge, Satir kucukten buyuge sirala
                if 'Sipariş No' in df.columns and 'Satir' in df.columns:
                    df = df.sort_values(by=['Sipariş No', 'Satir'], ascending=[False, True], na_position='last')
                    df = df.reset_index(drop=True)

                return df

            except Exception as e:
                logger.error(f"Siparis veri hatasi: {e}")
                raise

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # Son 2 yilin baslangic tarihini al
            start_date = self.sheets_manager.get_last_two_years_start_date()

            # Son 2 yilin siparis verilerini getir
            siparis_df = self.get_siparis_data(start_date)

            if not siparis_df.empty:
                # Google Sheets'e yukle
                self.sheets_manager.update_siparisler_worksheet(siparis_df)
            else:
                logger.error("Son 2 yilda siparis verisi bulunamadi")

        except Exception as e:
            logger.error(f"Siparisler analizi basarisiz: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_siparisler_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasina yazar)"""
    try:
        # Config olustur (Service Account otomatik baslar)
        config = SiparislerConfig()

        # Analyzer olustur
        analyzer = SiparislerAnalyzer(config)

        # Analiz calistir
        analyzer.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatasi: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_siparisler_analysis()
