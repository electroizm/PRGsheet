"""
Irsaliye System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli irsaliye analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- SQL + Excel veri kaynakları birleştirme
- Fatura - İrsaliye eşleştirmesi
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import pandas as pd
import pyodbc
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys
from contextlib import contextmanager
from typing import Optional

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
log_file = log_dir / 'irsaliye_analizi.log'

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

class IrsaliyeConfig:
    """Configuration management for Irsaliye processing - Service Account kullanır"""

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
    def days_lookback(self) -> int:
        """Kaç gün geriye bakılacak (PRGsheet'ten alınabilir)"""
        days = self.settings.get('IRSALIYE_DAYS_LOOKBACK', '')
        if days:
            return int(days)
        return 127  # Varsayılan

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
                df_clean = self._clean_dataframe(df)
                values = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

                # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
                worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            raise Exception(f"Error updating {worksheet_name} worksheet: {e}")

    def delete_worksheet(self, spreadsheet_name: str, worksheet_name: str) -> None:
        """Delete a worksheet from Google Sheets"""
        try:
            spreadsheet = self.gc.open(spreadsheet_name)

            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                spreadsheet.del_worksheet(worksheet)
            except:
                pass  # Worksheet zaten yok

        except Exception as e:
            raise Exception(f"Error deleting {worksheet_name} worksheet: {e}")

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
# IRSALIYE PROCESSOR
# ============================================================================

class IrsaliyeProcessor:
    """Main Irsaliye processing logic"""

    def __init__(self, config: IrsaliyeConfig):
        self.config = config
        self.db_manager = DatabaseManager(config.connection_string)
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def get_irsaliye_data(self) -> pd.DataFrame:
        """Get Irsaliye data from database"""
        query = """
            SELECT * FROM dbo.fn_StokHareketEvrakYonetimi('20240101','20771231',N'',N',13,')
            ORDER BY [msg_S_0089], [msg_S_0077], [msg_S_0555], [msg_S_0090], [msg_S_0157],
                     [msg_S_0003], [msg_S_0097], [msg_S_0199], [msg_S_0200]
        """
        return self.db_manager.execute_query(query)

    def load_sanalpos_data(self) -> pd.DataFrame:
        """Load SanalPos Irsaliye Excel file"""
        file_path = "D:/GoogleDrive/PRG/SanalPos İrsaliye.xlsx"
        try:
            df = pd.read_excel(file_path)
            return df
        except Exception as e:
            logger.error(f"Error loading SanalPos data: {e}")
            raise

    def load_fatura_data(self) -> pd.DataFrame:
        """Load Fatura Excel file"""
        file_path = "D:/GoogleDrive/PRG/Fatura.xlsx"
        try:
            df = pd.read_excel(file_path)
            return df
        except Exception as e:
            logger.error(f"Error loading Fatura data: {e}")
            raise

    def process_prosap_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process PROSAP fatura data"""
        if "Tayin" not in df.columns:
            raise KeyError('Excel dosyasında "Tayin" sütunu bulunamadı.')

        if "Belge tarihi" not in df.columns:
            raise KeyError('Excel dosyasında "Belge tarihi" sütunu bulunamadı.')

        # Filter non-empty Tayin values
        prosap_fatura = df[df["Tayin"].notna()].copy()

        # Filter last N days
        cutoff_date = datetime.today() - timedelta(days=self.config.days_lookback)
        prosap_fatura["Belge tarihi"] = pd.to_datetime(prosap_fatura["Belge tarihi"])
        prosap_fatura["Belge tarihi"] = prosap_fatura["Belge tarihi"].dt.date
        prosap_fatura = prosap_fatura[prosap_fatura["Belge tarihi"] >= cutoff_date.date()]

        # Clean empty rows
        prosap_fatura = prosap_fatura.dropna(how="all")

        # Data type conversions
        prosap_fatura["Tayin"] = prosap_fatura["Tayin"].astype(int).astype(str)
        prosap_fatura["UPB Tutarı"] = prosap_fatura["UPB Tutarı"].fillna(0).astype(int)
        prosap_fatura["Esle"] = prosap_fatura["Belge tarihi"].astype(str) + " - " + prosap_fatura["Tayin"].astype(str)

        # Select required columns
        selected_columns = ["Esle", "Tayin", "Ödeme Koşulu Tanımı", "UPB Tutarı", "Kullanıcının adı", "Referans"]
        return prosap_fatura[selected_columns]

    def process_mikro_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process Mikro irsaliye data"""
        # Rename columns
        df = df.rename(columns={
            "msg_S_0157": "SIRA NO",
            "msg_S_0089": "TARİH"
        })

        # Select required columns
        irsaliye_sutun = ["SIRA NO", "TARİH", "Vergili TUTAR"]
        mikro_irsaliye = df[irsaliye_sutun].copy()

        # Filter last N days
        cutoff_date = datetime.today() - timedelta(days=self.config.days_lookback)

        if "TARİH" in mikro_irsaliye.columns:
            mikro_irsaliye["TARİH"] = pd.to_datetime(mikro_irsaliye["TARİH"])
            mikro_irsaliye["TARİH"] = mikro_irsaliye["TARİH"].dt.date
            mikro_irsaliye = mikro_irsaliye[mikro_irsaliye["TARİH"] >= cutoff_date.date()]
        else:
            raise KeyError('DataFrame\'inde "TARİH" sütunu bulunamadı.')

        # Process SIRA NO
        if "SIRA NO" in mikro_irsaliye.columns:
            mikro_irsaliye["SIRA NO"] = "900" + mikro_irsaliye["SIRA NO"].astype(str)
            mikro_irsaliye["SIRA NO"] = mikro_irsaliye["SIRA NO"].astype(int).astype(str)
        else:
            raise KeyError('DataFrame\'inde "SIRA NO" sütunu bulunamadı.')

        # Process amounts
        mikro_irsaliye["Vergili TUTAR"] = mikro_irsaliye["Vergili TUTAR"].fillna(0).astype(int)
        mikro_irsaliye["Esle"] = mikro_irsaliye["TARİH"].astype(str) + " - " + mikro_irsaliye["SIRA NO"].astype(str)

        return mikro_irsaliye

    def merge_and_analyze_differences(self, prosap_df: pd.DataFrame, mikro_df: pd.DataFrame) -> pd.DataFrame:
        """Merge data and analyze differences"""
        # Merge data
        merged_df = pd.merge(prosap_df, mikro_df, left_on="Esle", right_on="Esle", how="outer")

        # Calculate differences
        merged_df["Vergili TUTAR"] = merged_df["Vergili TUTAR"].fillna(0).astype(int)
        merged_df["Fark"] = abs(merged_df["UPB Tutarı"] - merged_df["Vergili TUTAR"])

        # Filter significant differences
        significant_diff = merged_df[merged_df["Fark"] >= 7].copy()

        if "Referans" in significant_diff.columns:
            significant_diff["Referans"] = significant_diff["Referans"].astype(str)

        # Rename columns
        significant_diff = significant_diff.rename(columns={
            "Tayin": "Fatura No",
            "Ödeme Koşulu Tanımı": "Ödeme Kartı",
            "UPB Tutarı": "Tutar",
            "SIRA NO": "İrsaliye No",
            "TARİH": "Tarih"
        })

        # Clean data
        if "İrsaliye No" in significant_diff.columns:
            significant_diff["İrsaliye No"] = significant_diff["İrsaliye No"].astype(str).str.replace(r"\.0$", "", regex=True)

        if "Tarih" in significant_diff.columns:
            significant_diff["Tarih"] = pd.to_datetime(significant_diff["Tarih"]).dt.date

        return significant_diff

    def process_fatura_details(self, prosap_diff_df: pd.DataFrame, fatura_df: pd.DataFrame) -> pd.DataFrame:
        """Process detailed fatura information"""
        prosap_records = prosap_diff_df.copy()

        if prosap_records.empty:
            return pd.DataFrame()

        # Convert data types for merge
        prosap_records["Fatura No"] = prosap_records["Fatura No"].astype(int)
        fatura_df["Fatura No"] = fatura_df["Fatura No"].astype(int)

        # Merge with fatura data
        if "Fatura No" not in fatura_df.columns:
            raise KeyError('Fatura.xlsx dosyasında "Fatura No" sütunu bulunamadı.')

        merged_with_fatura = pd.merge(
            prosap_records,
            fatura_df,
            left_on="Fatura No",
            right_on="Fatura No",
            how="left"
        )

        # Filter positive amounts
        if "Net Tutar" not in merged_with_fatura.columns:
            raise KeyError('Birleştirilmiş DataFrame\'de "Net Tutar" sütunu bulunamadı.')

        merged_with_fatura_filtered = merged_with_fatura[merged_with_fatura["Net Tutar"] > 0]

        # Select and rename columns
        selected_columns = [
            "Fiili Mal Hareket Tarihi", "Fatura No", "Ödeme Kartı", "Malzeme Kısa Tanımı",
            "Net Tutar", "Faturalanan Gerçek Miktar", "Vergi Sınıfı Tanımı",
            "Ad-Soyad", "Prosap Sas Kalem no", "Malzeme"
        ]

        missing_columns = [col for col in selected_columns if col not in merged_with_fatura_filtered.columns]
        if missing_columns:
            raise KeyError(f"Aşağıdaki sütunlar DataFrame'de bulunamadı: {missing_columns}")

        result_df = merged_with_fatura_filtered[selected_columns].copy()
        result_df.rename(columns={
            "Fiili Mal Hareket Tarihi": 'Fatura Tarihi',
            'Faturalanan Gerçek Miktar': 'Miktar',
            "Vergi Sınıfı Tanımı": 'Vergi'
        }, inplace=True)

        # Clean date column
        if "Fatura Tarihi" in result_df.columns:
            result_df["Fatura Tarihi"] = pd.to_datetime(result_df["Fatura Tarihi"]).dt.date

        return result_df

    def process_all(self) -> None:
        """Main processing pipeline"""
        try:
            # Load data
            irsaliye_sql_data = self.get_irsaliye_data()
            sanalpos_data = self.load_sanalpos_data()
            fatura_data = self.load_fatura_data()

            # Process data
            prosap_processed = self.process_prosap_data(sanalpos_data)
            mikro_processed = self.process_mikro_data(irsaliye_sql_data)

            # Analyze differences
            differences_df = self.merge_and_analyze_differences(prosap_processed, mikro_processed)

            # Save to Google Sheets - Fatura sheet
            if not differences_df.empty:
                self.sheets_manager.save_to_worksheet(
                    differences_df,
                    self.config.spreadsheet_name,
                    "Fatura"
                )
            else:
                self.sheets_manager.delete_worksheet(
                    self.config.spreadsheet_name,
                    "Fatura"
                )

            # Process detailed fatura information
            fatura_details_df = self.process_fatura_details(differences_df, fatura_data)

            # Save to Google Sheets - Irsaliye sheet
            if not fatura_details_df.empty:
                self.sheets_manager.save_to_worksheet(
                    fatura_details_df,
                    self.config.spreadsheet_name,
                    "Irsaliye"
                )
            else:
                self.sheets_manager.delete_worksheet(
                    self.config.spreadsheet_name,
                    "Irsaliye"
                )

        except Exception as e:
            logger.error(f"Error in processing pipeline: {e}")
            raise Exception(f"Error in processing pipeline: {e}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main entry point"""
    try:
        config = IrsaliyeConfig()
        processor = IrsaliyeProcessor(config)
        processor.process_all()
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
