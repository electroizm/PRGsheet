"""
BagKodu System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli bağ kodu analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL Injection koruması
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pandas as pd
import pyodbc
import logging
from contextlib import contextmanager
from pathlib import Path
import sys

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
log_file = log_dir / 'bagkodu_analizi.log'

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

class BagKoduConfig:
    """Configuration management for BagKodu processing"""

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
    def spreadsheet_name(self) -> str:
        return 'PRGsheet'

    @property
    def connection_string(self) -> str:
        """Get database connection string from PRGsheet"""
        required_settings = ['SQL_SERVER', 'SQL_DATABASE', 'SQL_USERNAME', 'SQL_PASSWORD']
        missing = [key for key in required_settings if not self.settings.get(key)]

        if missing:
            raise ValueError(
                f"PRGsheet -> Ayar sayfasında eksik ayarlar: {', '.join(missing)}\n"
                f"Lütfen bu ayarları Global olarak ekleyin."
            )

        return (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.settings["SQL_SERVER"]};'
            f'DATABASE={self.settings["SQL_DATABASE"]};'
            f'UID={self.settings["SQL_USERNAME"]};'
            f'PWD={self.settings["SQL_PASSWORD"]}'
        )

# ============================================================================
# DATABASE MANAGER
# ============================================================================

class DatabaseManager:
    """Database connection and query management"""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string)
            yield connection

        except pyodbc.Error as e:
            raise Exception(f"Database connection error: {e}")
        finally:
            if connection:
                connection.close()

    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        with self.get_connection() as connection:
            try:
                cursor = connection.cursor()
                cursor.execute(query)
                rows = cursor.fetchall()
                columns = [column[0] for column in cursor.description]
                return pd.DataFrame.from_records(rows, columns=columns)

            except pyodbc.Error as e:
                raise Exception(f"Query execution error: {e}")

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """Google Sheets API management with Service Account"""

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.gc = config_manager.gc  # Service Account ile yetkilendirilmiş client

    def save_to_worksheet(
        self,
        df: pd.DataFrame,
        spreadsheet_name: str,
        worksheet_name: str
    ) -> None:
        """Save DataFrame to Google Sheets worksheet"""
        try:
            # PRGsheet'i doğrudan aç
            spreadsheet = self.gc.open(spreadsheet_name)

            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()
            except:
                worksheet = spreadsheet.add_worksheet(
                    title=worksheet_name,
                    rows=1000,
                    cols=20
                )

            if not df.empty:
                # Clean data
                df_clean = self._clean_dataframe(df)
                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

                # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
                worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            raise Exception(f"Worksheet save error for '{worksheet_name}': {e}")

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for Google Sheets"""
        df_clean = df.copy()
        df_clean = df_clean.fillna('')

        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                df_clean[col] = df_clean[col].astype(str).replace('NaT', '')
            elif df_clean[col].dtype in ['float64', 'float32']:
                df_clean[col] = df_clean[col].replace([float('inf'), float('-inf')], '')
                df_clean[col] = df_clean[col].astype(str).replace('nan', '').replace('NaN', '')
            elif df_clean[col].dtype in ['int64', 'int32']:
                df_clean[col] = df_clean[col].astype(str)
            else:
                df_clean[col] = df_clean[col].astype(str).replace('nan', '').replace('NaN', '').replace('None', '')

        # Final cleanup
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].apply(
                lambda x: '' if str(x) in ['nan', 'NaN', 'None', 'NaT'] else str(x)
            )

        return df_clean

# ============================================================================
# BAGKODU PROCESSOR
# ============================================================================

class BagKoduProcessor:
    """BagKodu veri işleyici"""

    def __init__(self, config: BagKoduConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def get_bagkodu_data(self) -> pd.DataFrame:
        """Fetch barkod data from database"""
        query = """
        SELECT TOP 100 PERCENT
            bar_RECno AS [barkodKayit],
            bar_serino_veya_bagkodu AS [bagKodum],
            bar_stokkodu AS [malzemeKodu],
            sto_isim AS [malzemeAdi]
        FROM dbo.BARKOD_TANIMLARI WITH (NOLOCK)
        LEFT OUTER JOIN dbo.STOKLAR ON sto_kod = bar_stokkodu
        WHERE sto_pasif_fl IS NULL OR sto_pasif_fl=0
        ORDER BY bar_RECno DESC
        """

        return self.db_manager.execute_query(query)

    def run_analysis(self) -> None:
        """Ana analiz fonksiyonu"""
        try:
            # Bağ kodu verilerini çek
            bagkodu_df = self.get_bagkodu_data()

            # Google Sheets'e kaydet
            if not bagkodu_df.empty:
                self.sheets_manager.save_to_worksheet(
                    bagkodu_df,
                    self.config.spreadsheet_name,
                    'BagKodu'
                )

        except Exception as e:
            logger.error(f"BagKodu analizi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_bagkodu_analysis() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = BagKoduConfig()

        # Processor oluştur
        processor = BagKoduProcessor(config)

        # Analiz çalıştır
        processor.run_analysis()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_bagkodu_analysis()
