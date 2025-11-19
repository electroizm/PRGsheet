"""
OKC (Ödeme Kaydedicisi Cihazı) System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli OKC fatura analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- GUI ile Excel dosya seçimi
- Otomatik duplike kayıt önleme
- Tarih bazlı veri filtreleme
"""

import pandas as pd
from tkinter import Tk, filedialog
from tkinter.messagebox import showinfo, showerror
import warnings
from datetime import datetime
import logging
from typing import Optional
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
log_file = log_dir / 'okc_analizi.log'

logging.basicConfig(
    level=logging.ERROR,  # Sadece hatalar
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# GOOGLE SHEETS MANAGER - Service Account
# ============================================================================

class GoogleSheetsManager:
    """Google Sheets API management with Service Account"""

    def __init__(self):
        try:
            # Merkezi config manager oluştur (Service Account otomatik başlar)
            self.config_manager = CentralConfigManager()
            self.gc = self.config_manager.gc  # Service Account ile yetkilendirilmiş client

        except Exception as e:
            logger.error(f"Google Sheets init hatası: {e}")
            raise

    def clean_data_for_sheets(self, df: pd.DataFrame):
        """Clean DataFrame for Google Sheets - convert everything to basic types"""
        df_clean = df.copy()

        for col in df_clean.columns:
            # Convert datetime columns to strings
            if str(df_clean[col].dtype).startswith('datetime'):
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d')
            # Fill NaN values
            elif df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].fillna('')
            # Convert numeric NaN to empty string
            else:
                df_clean[col] = df_clean[col].fillna('')

        # Convert to basic Python types
        clean_data = []
        headers = [str(col) for col in df_clean.columns]

        for _, row in df_clean.iterrows():
            clean_row = []
            for val in row:
                if pd.isna(val) or val is None:
                    clean_row.append('')
                else:
                    clean_row.append(str(val))
            clean_data.append(clean_row)

        return headers, clean_data

    def get_okc_data(self):
        """OKC sayfasındaki mevcut verileri al"""
        try:
            spreadsheet = self.gc.open("PRGsheet")
            worksheet = spreadsheet.worksheet('OKC')
            existing_records = worksheet.get_all_records()
            return pd.DataFrame(existing_records), worksheet

        except Exception as e:
            logger.error(f"OKC worksheet okuma hatası: {e}")
            raise

    def update_okc_data(self, worksheet, df: pd.DataFrame):
        """OKC sayfasını güncelle"""
        try:
            # Clean data for Google Sheets
            headers, clean_rows = self.clean_data_for_sheets(df)

            # Update worksheet
            worksheet.clear()
            values = [headers] + clean_rows

            # RAW: binlik ayraç eklenmez, veri olduğu gibi yazılır
            worksheet.update(values, value_input_option='RAW')

        except Exception as e:
            logger.error(f"OKC worksheet güncelleme hatası: {e}")
            raise

# ============================================================================
# OKC PROCESSOR
# ============================================================================

def excel_oku():
    """Excel dosyasından OKC verilerini oku ve Google Sheets'e ekle"""
    root = Tk()
    root.withdraw()

    warnings.filterwarnings('ignore', category=UserWarning, message="Workbook contains no default style")

    try:
        # File selection
        dosya_yolu = filedialog.askopenfilename(
            title="Excel Dosyası Seçin",
            filetypes=[("Excel Dosyaları", "*.xlsx *.xls"), ("Tüm Dosyalar", "*.*")]
        )

        if not dosya_yolu:
            showinfo("Bilgi", "Dosya seçilmedi!")
            return

        # Required columns
        istenen_sutunlar = [
            'Fatura Numarası', 'Alıcı VKN/TCKN', 'Ödenecek Tutar',
            'Alıcı Unvanı /Adı Soyadı', 'Fatura Düzenlenme Tarihi'
        ]

        # Read new data
        yeni_veri = pd.read_excel(dosya_yolu, usecols=istenen_sutunlar)

        # Convert 'Ödenecek Tutar' to integer
        yeni_veri['Ödenecek Tutar'] = pd.to_numeric(yeni_veri['Ödenecek Tutar'], errors='coerce').fillna(0).astype(int)

        # Process dates in new data
        yeni_veri['Fatura Düzenlenme Tarihi'] = pd.to_datetime(
            yeni_veri['Fatura Düzenlenme Tarihi'],
            dayfirst=True,
            errors='coerce'
        )

        # Check for invalid dates
        invalid_dates = yeni_veri['Fatura Düzenlenme Tarihi'].isna().sum()
        if invalid_dates > 0:
            showinfo("Uyarı", f"{invalid_dates} adet hatalı tarih formatı bulundu ve NaN olarak işaretlendi")

        # Initialize Google Sheets Manager (Service Account)
        sheets_manager = GoogleSheetsManager()

        try:
            # Get existing data
            mevcut_veri, worksheet = sheets_manager.get_okc_data()

            if not mevcut_veri.empty:
                # Process existing dates
                mevcut_veri['Fatura Düzenlenme Tarihi'] = pd.to_datetime(
                    mevcut_veri['Fatura Düzenlenme Tarihi'],
                    format='%Y-%m-%d',
                    errors='coerce'
                )

                # Convert 'Ödenecek Tutar' to integer in existing data
                if 'Ödenecek Tutar' in mevcut_veri.columns:
                    mevcut_veri['Ödenecek Tutar'] = pd.to_numeric(mevcut_veri['Ödenecek Tutar'], errors='coerce').fillna(0).astype(int)

                # Exclude existing invoice numbers (sadece fatura numarasına bak)
                existing_invoices = set(mevcut_veri['Fatura Numarası'].astype(str))
                truly_new_records = yeni_veri[~yeni_veri['Fatura Numarası'].astype(str).isin(existing_invoices)]

                if truly_new_records.empty:
                    showinfo("Bilgi", "Eklenebilecek yeni veri bulunamadı!\n\nMevcut veri sayısı: " + str(len(mevcut_veri)))
                    return

                # Add YazarKasa column if it exists in existing data
                if 'YazarKasa' in mevcut_veri.columns:
                    truly_new_records = truly_new_records.copy()
                    truly_new_records['YazarKasa'] = ''

                # Combine data
                final_data = pd.concat([truly_new_records, mevcut_veri], ignore_index=True)
                new_count = len(truly_new_records)
            else:
                final_data = yeni_veri
                new_count = len(yeni_veri)

            # Sort by date (newest first)
            final_data = final_data.sort_values(by='Fatura Düzenlenme Tarihi', ascending=False)

            # Update Google Sheets
            sheets_manager.update_okc_data(worksheet, final_data)

            # Success message
            showinfo("Başarılı", f"{new_count} yeni kayıt eklendi!\nToplam kayıt: {len(final_data)}")

        except Exception as e:
            if "WorksheetNotFound" in str(e):
                showerror("Hata", "OKC sayfası bulunamadı! Lütfen önce OKC sayfasını oluşturun.")
            else:
                raise

    except Exception as e:
        error_msg = f"Hata oluştu: {str(e)}"
        logger.error(error_msg)
        showerror("Hata", error_msg)
    finally:
        root.destroy()

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    excel_oku()
