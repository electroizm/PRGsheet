"""
SanalPos System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli SanalPos analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- Excel ve SQL veri kaynaklarını birleştirir
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pyodbc
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd
from contextlib import contextmanager
from dateutil.relativedelta import relativedelta

# Merkezi config manager'ı import et
from central_config import CentralConfigManager

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# PyInstaller ile freeze edildiginde dosya yollarini duzelt
if getattr(sys, 'frozen', False):
    base_dir = Path(sys.executable).parent
else:
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'sanalpos_analizi.log'

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

class SanalPosConfig:
    """
    SanalPos analizi için yapılandırma sınıfı
    Service Account ve merkezi config kullanır
    """

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()

            # PRGsheet'ten ayarları yükle
            self.settings = self.config_manager.get_settings()

        except Exception as e:
            logger.error(f"Config yükleme hatası: {e}")
            raise

    @property
    def spreadsheet_id(self) -> str:
        """PRGsheet spreadsheet ID"""
        return self.config_manager.MASTER_SPREADSHEET_ID

    @property
    def sanalpos_excel_path(self) -> str:
        """
        SanalPos Excel dosya yolu
        PRGsheet -> Ayar'dan SANALPOS_EXCEL_PATH alınır
        """
        excel_path = self.settings.get('SANALPOS_EXCEL_PATH', '')

        if not excel_path:
            # Varsayılan yol
            excel_path = r"D:/GoogleDrive/PRG/SanalPos İrsaliye.xlsx"

        return excel_path

    @property
    def connection_string(self) -> str:
        """SQL bağlantı string'i"""
        required_settings = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
        missing = [key for key in required_settings if not self.settings.get(key)]

        if missing:
            raise ValueError(
                f"PRGsheet -> Ayar sayfasında eksik ayarlar: {', '.join(missing)}\n"
                f"Lütfen bu ayarları Global olarak ekleyin."
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
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        """SQL sorgusu çalıştırma"""
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                return pd.DataFrame.from_records(rows, columns=columns)

            except pyodbc.Error as e:
                logger.error(f"Query execution error: {e}")
                raise

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """
    Service Account kullanan Google Sheets yöneticisi
    Service account yerine Service Account token kullanır
    """

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """DataFrame'i Google Sheets için temizle"""
        df_copy = df.copy()

        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].astype(str)
            elif df_copy[col].dtype == 'object':
                df_copy[col] = df_copy[col].apply(
                    lambda x: str(x) if hasattr(x, 'strftime') else x
                )

        return df_copy.fillna('').replace([pd.NA, float('inf'), float('-inf')], '')

    def save_to_worksheet(self, df: pd.DataFrame, worksheet_name: str) -> None:
        """SanalPos sayfasına veri yaz"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            # Worksheet'i bul veya oluştur
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name,
                    rows=1000,
                    cols=10
                )

            if not df.empty:
                # Clean data
                df_clean = self._clean_dataframe(df)
                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

                # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
                worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            logger.error(f"SanalPos worksheet güncelleme hatası: {e}")
            raise

# ============================================================================
# DATA PROCESSOR
# ============================================================================

class DataProcessor:
    """Veri işleme sınıfı"""

    SANALPOS_COLUMNS = [
        "Belge tarihi", "Ödeme Koşulu Tanımı", "UPB Tutarı",
        "Ödeme Biçimi Tanımı", "Referans"
    ]

    KASA_COLUMN_MAPPING = {
        "#msg_S_0200": "KASA KODU",
        "#msg_S_0201": "KASA ADI",
        "msg_S_0089": "Tarih",
        "#msg_S_0085": "ACIKLAMA",
        "msg_S_0100": "Alacak / Borç",
        "#msg_S_0103\\T": "TUTAR",
        "msg_S_0115": "CARI ADI",
        "msg_S_0094": "Evrak Tipi",
        "msg_S_0003": "Nakit / Dekont"
    }

    KASA_COLUMNS = [
        "Tarih", "KASA KODU", "KASA ADI", "CARI ADI",
        "TUTAR", "ACIKLAMA", "Alacak / Borç", "Nakit / Dekont"
    ]

    def __init__(self, config: SanalPosConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def process_sanalpos_excel(self, file_path: str, days_filter: int = 77) -> pd.DataFrame:
        """Excel dosyasından SanalPos verilerini işle"""
        df = pd.read_excel(file_path)
        df["Belge tarihi"] = pd.to_datetime(df["Belge tarihi"])

        date_threshold = datetime.now() - timedelta(days=days_filter)
        df = df[
            (df["Belge tarihi"] > date_threshold) &
            (df["Kullanıcının adı"] == "CRM_RFCUSER")
        ]

        return df[self.SANALPOS_COLUMNS]

    def process_kasa_data(self, days_filter: int = 377) -> pd.DataFrame:
        """SQL'den kasa verilerini işle"""
        query = """
        SELECT * FROM dbo.fn_CariFoy (
            N'0',4,N'',NULL,'20241231','20240101','20771231',0,N''
        ) ORDER BY [msg_S_0089], [msg_S_0094], [msg_S_0090], [msg_S_0091]
        """

        df = self.db_manager.execute_query(query)
        df = df.rename(columns=self.KASA_COLUMN_MAPPING)
        df = df[self.KASA_COLUMNS].copy()

        df["Tarih"] = pd.to_datetime(df["Tarih"], format="%Y-%m-%d")
        date_threshold = datetime.now() - timedelta(days=days_filter)
        df = df[df["Tarih"] > date_threshold]

        df = df.sort_values(by="Tarih", ascending=False)
        df["Tarih"] = df["Tarih"].dt.strftime("%Y-%m-%d")

        return df

    def prepare_monthly_data(self, df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        """Aylık veri aralığını hazırla"""
        now = datetime.now()
        current_month_start = datetime(now.year, now.month, 1).date()
        previous_month_start = (current_month_start - relativedelta(months=1))

        df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.date

        return df[
            (df[date_column] >= previous_month_start) &
            (df[date_column] < current_month_start + relativedelta(months=1))
        ]

    def merge_sanalpos_kasa_data(self, sanalpos_df: pd.DataFrame, kasa_df: pd.DataFrame) -> pd.DataFrame:
        """SanalPos ve Kasa verilerini birleştir"""
        # SanalPos verilerini hazırla
        sanalpos_df["UPB Tutarı"] = sanalpos_df["UPB Tutarı"].fillna(0).astype(int)
        sanalpos_df["Referans"] = sanalpos_df["Referans"].astype(str).str.zfill(6)

        prosap_monthly = self.prepare_monthly_data(sanalpos_df, "Belge tarihi")

        # Kasa verilerini hazırla
        kasa_filtered = kasa_df[
            (kasa_df["Nakit / Dekont"] != "Dekont") &
            (kasa_df["KASA KODU"] == "100-SNL")
        ].copy()

        kasa_filtered["ACIKLAMA"] = kasa_filtered["ACIKLAMA"].astype(str).str.zfill(6)
        kasa_filtered["TUTAR"] = kasa_filtered["TUTAR"].fillna(0).astype(int)

        mikro_monthly = self.prepare_monthly_data(kasa_filtered, "Tarih")

        # Birleştirme
        merged_df = pd.merge(
            prosap_monthly, mikro_monthly,
            left_on="Referans", right_on="ACIKLAMA",
            how="outer"
        )

        merged_df["UPB Tutarı"] = merged_df["UPB Tutarı"].fillna(0).astype(int)
        merged_df["TUTAR"] = merged_df["TUTAR"].fillna(0).astype(int)

        # Filtreleme
        merged_df = merged_df[
            ((merged_df["UPB Tutarı"] + merged_df["TUTAR"]) != 0) |
            (((merged_df["UPB Tutarı"] + merged_df["TUTAR"]) == 0) &
             (merged_df["Ödeme Biçimi Tanımı"] != "Ön Ödeme"))
        ]

        return merged_df.sort_values(by="Belge tarihi", ascending=False)

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # SanalPos Excel verilerini yükle
            sanalpos_df = self.process_sanalpos_excel(self.config.sanalpos_excel_path)

            # Kasa verilerini yükle
            kasa_df = self.process_kasa_data()

            # Birleştir
            merged_df = self.merge_sanalpos_kasa_data(sanalpos_df, kasa_df)

            # Google Sheets'e kaydet
            if not merged_df.empty:
                self.sheets_manager.save_to_worksheet(merged_df, "SanalPos")

        except Exception as e:
            logger.error(f"SanalPos analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_sanalpos_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = SanalPosConfig()

        # Processor oluştur
        processor = DataProcessor(config)

        # Analiz çalıştır
        processor.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_sanalpos_analysis()
