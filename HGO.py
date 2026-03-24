"""
Dogtas Bayi Ticari Prim Hesaplama Sistemi (V2)
===============================================

Kullanici tarafindan girilen aylik hedef verileri ile Dogtas API'sinden
cekilen gerceklesen siparis ve fatura verilerini karsilastirarak,
Dogtas 2026 Q1 Ticari Politikasi'na uygun prim hakedislerini hesaplar.

Siniflar:
    PrimApiClient  - Dogtas API veri cekme islemleri
    PrimCalculator - Prim hesaplama mantigi
    StorageManager - Google Sheets hedef yonetimi

Kullanim:
    python DogtasPrimHesaplama.py
"""

import sys
import requests
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from central_config import CentralConfigManager

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QComboBox, QLineEdit, QPushButton, QTableWidget,
    QTableWidgetItem, QProgressBar, QGroupBox, QHeaderView,
    QMessageBox, QFrame, QSpacerItem, QSizePolicy, QTextEdit,
    QTabWidget, QSpinBox, QDoubleSpinBox
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QColor, QIcon

# ============================================================================
# LOGGING
# ============================================================================

if getattr(sys, 'frozen', False):
    base_dir = Path(sys.executable).parent
else:
    base_dir = Path(__file__).parent

log_dir = base_dir / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'prim_hesaplama.log'

logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# SABITLER
# ============================================================================

TURKISH_MONTHS = {
    1: 'Ocak', 2: 'Şubat', 3: 'Mart', 4: 'Nisan',
    5: 'Mayıs', 6: 'Haziran', 7: 'Temmuz', 8: 'Ağustos',
    9: 'Eylül', 10: 'Ekim', 11: 'Kasım', 12: 'Aralık'
}


# ============================================================================
# PRIM API CLIENT
# ============================================================================

class PrimApiClient:
    """Dogtas API uzerinden siparis ve fatura verisi ceken istemci."""

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager
        self.token = None
        self._load_config()

    def _load_config(self):
        """Ayar sayfasindan API konfigurasyonlarini yukler."""
        try:
            sheet = self.config_manager.gc.open("PRGsheet").worksheet('Ayar')
            all_values = sheet.get_all_values()

            if not all_values:
                raise ValueError("Ayar sayfasi bos")

            headers = all_values[0]
            key_index = headers.index('Key')
            value_index = headers.index('Value')

            config = {}
            for row in all_values[1:]:
                if len(row) > max(key_index, value_index):
                    key = row[key_index].strip() if row[key_index] else ''
                    value = row[value_index].strip() if row[value_index] else ''
                    if key:
                        config[key] = value

            self.base_url = config.get('base_url', '')
            self.endpoint = config.get('bekleyenler', '')
            self.customer_no = config.get('CustomerNo', '')

            self.auth_data = {
                "userName": config.get('userName', ''),
                "password": config.get('password', ''),
                "clientId": config.get('clientId', ''),
                "clientSecret": config.get('clientSecret', ''),
                "applicationCode": config.get('applicationCode', '')
            }

        except Exception as e:
            logger.error(f"Config yukleme hatasi: {e}")
            self.base_url = ''
            self.endpoint = ''
            self.customer_no = ''
            self.auth_data = {}

    def _get_token(self) -> bool:
        """API access token alir."""
        try:
            response = requests.post(
                f"{self.base_url}/Authorization/GetAccessToken",
                json=self.auth_data,
                timeout=10
            )

            if response.status_code == 200:
                data = response.json()
                if data.get('isSuccess') and 'data' in data:
                    self.token = data['data']['accessToken']
                    return True
            return False
        except Exception as e:
            logger.error(f"Token alma hatasi: {e}")
            return False

    def fetch_data(self, start_date: str, end_date: str) -> list:
        """
        Belirtilen tarih araligindaki TUM siparisleri ceker.
        Iptal edilmis siparisler haric tutulur.
        purchaseInvoiceDate filtresi UYGULANMAZ (bekleyen + faturalanmis).

        Args:
            start_date: Baslangic tarihi DD.MM.YYYY
            end_date: Bitis tarihi DD.MM.YYYY

        Returns:
            Filtrelenmis siparis kayitlari listesi
        """
        if not self.token and not self._get_token():
            logger.error("Token alinamadi")
            return []

        try:
            payload = {
                "orderId": "",
                "CustomerNo": self.customer_no,
                "RegistrationDateStart": start_date,
                "RegistrationDateEnd": end_date,
                "referenceDocumentNo": "",
                "SalesDocumentType": ""
            }

            response = requests.post(
                f"{self.base_url}{self.endpoint}",
                json=payload,
                headers={
                    'Authorization': f'Bearer {self.token}',
                    'Content-Type': 'application/json'
                },
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"API yanit kodu: {response.status_code}")
                return []

            result = response.json()
            if not result.get('isSuccess') or not isinstance(result.get('data'), list):
                logger.error("API basarisiz yanit")
                return []

            data = result['data']

            # Iptal edilmis siparisleri ve MHZ parcalarini (Z347) cikar
            filtered = [
                record for record in data
                if 'iptal' not in str(record.get('orderStatus', '')).lower()
                and str(record.get('odemeKosulu', '')).strip() != 'Z347'
            ]

            # Duplicate eliminasyon: orderId + orderLineId
            seen = set()
            unique = []
            for record in filtered:
                key = f"{record.get('orderId', '')}-{record.get('orderLineId', '')}"
                if key not in seen:
                    seen.add(key)
                    unique.append(record)

            return unique

        except Exception as e:
            logger.error(f"API cagrisi hatasi: {e}")
            return []


# ============================================================================
# PRIM CALCULATOR
# ============================================================================

class PrimCalculator:
    """Dogtas ticari politikasina gore prim hesaplama motoru."""

    @staticmethod
    def calculate_monthly_premium(
        realized_order: Decimal,
        target: Decimal,
        realized_invoice: Decimal
    ) -> dict:
        """
        Aylik prim hesaplamasi.

        HGO = (Gerceklesen Siparis / Hedef) * 100
        Prim = Prim Orani * Net Fatura Tutari

        Returns:
            {'hgo': Decimal, 'rate': Decimal, 'premium_amount': Decimal}
        """
        if target <= 0:
            return {
                'hgo': Decimal('0'),
                'rate': Decimal('0'),
                'premium_amount': Decimal('0')
            }

        hgo = (realized_order / target) * 100

        if hgo >= 120:
            rate = Decimal('3.0')
        elif hgo >= 110:
            rate = Decimal('2.5')
        elif hgo >= 100:
            rate = Decimal('2.0')
        elif hgo >= 90:
            rate = Decimal('1')
        else:
            rate = Decimal('0')

        premium_amount = (rate / 100) * realized_invoice

        return {
            'hgo': hgo,
            'rate': rate,
            'premium_amount': premium_amount
        }

    @staticmethod
    def calculate_quarterly_extra_premium(
        total_order: Decimal,
        total_target: Decimal,
        total_invoice: Decimal,
        ek_prim_tiers: list = None
    ) -> dict:
        """
        Ceyrek bazli ek hacim primi hesaplamasi.

        Kosul: 3 Aylik Toplam HGO >= %100
        Ek prim orani toplam siparis tutarina gore belirlenir.
        Ek prim = Ek oran * Toplam Net Fatura Tutari

        Returns:
            {'eligible': bool, 'hgo': Decimal, 'rate': Decimal,
             'premium_amount': Decimal, 'reason': str}
        """
        if total_target <= 0:
            return {
                'eligible': False,
                'hgo': Decimal('0'),
                'rate': Decimal('0'),
                'premium_amount': Decimal('0'),
                'reason': 'Hedef tanımlanmamış'
            }

        hgo = (total_order / total_target) * 100

        if hgo < 100:
            return {
                'eligible': False,
                'hgo': hgo,
                'rate': Decimal('0'),
                'premium_amount': Decimal('0'),
                'reason': f'Çeyrek HGO %100 altı (%{hgo:.2f})'
            }

        # Ciro baraji kontrolu (dinamik dilimler)
        if ek_prim_tiers is None:
            ek_prim_tiers = [
                {'alt_sinir': Decimal('50000000'), 'oran': Decimal('5')},
                {'alt_sinir': Decimal('35000000'), 'oran': Decimal('4')},
                {'alt_sinir': Decimal('20000000'), 'oran': Decimal('3')},
                {'alt_sinir': Decimal('10000000'), 'oran': Decimal('2')},
                {'alt_sinir': Decimal('7000000'), 'oran': Decimal('1')},
            ]

        # Alt sinira gore buyukten kucuge sirala
        sorted_tiers = sorted(ek_prim_tiers, key=lambda t: t['alt_sinir'], reverse=True)

        rate = Decimal('0')
        for tier in sorted_tiers:
            if total_order >= tier['alt_sinir']:
                rate = tier['oran']
                break

        if rate == 0:
            return {
                'eligible': False,
                'hgo': hgo,
                'rate': Decimal('0'),
                'premium_amount': Decimal('0'),
                'reason': f'Ciro barajı karşılanmadı ({format_currency(total_order)})'
            }

        premium_amount = (rate / 100) * total_invoice

        return {
            'eligible': True,
            'hgo': hgo,
            'rate': rate,
            'premium_amount': premium_amount,
            'reason': ''
        }

    @staticmethod
    def generate_forecast(monthly_data: dict, months: list, ek_prim_tiers: list = None) -> list:
        """
        Mevcut verilere bakarak ceyrek sonu tahmini ve strateji onerileri uretir.

        Args:
            monthly_data: Aylik siparis/fatura/hedef verileri
            months: Ceyregin ay numaralari [1,2,3]
            ek_prim_tiers: Ek prim dilimleri (opsiyonel)

        Returns:
            Oneri satirlari listesi (str list)
        """
        lines = []
        current_month = datetime.now().month

        # Verisi olan aylari tespit et
        active_months = [m for m in months if monthly_data[m]['realized_order'] > 0]
        remaining_months = [m for m in months if monthly_data[m]['realized_order'] == 0]

        if not active_months:
            lines.append("Henüz veri yok. Hesapla butonuna basın.")
            return lines

        # Toplam gerceklesen ve hedef
        total_order = sum(monthly_data[m]['realized_order'] for m in months)
        total_invoice = sum(monthly_data[m]['realized_invoice'] for m in months)
        total_target = sum(monthly_data[m]['target'] for m in months)

        if total_target <= 0:
            return ["Hedef tanımlanmamış."]

        current_hgo = (total_order / total_target) * 100

        # --- Projeksiyon ---
        if remaining_months:
            avg_monthly_order = total_order / len(active_months)
            avg_monthly_invoice = total_invoice / len(active_months)
            projected_order = total_order + avg_monthly_order * len(remaining_months)
            projected_invoice = total_invoice + avg_monthly_invoice * len(remaining_months)
            projected_hgo = (projected_order / total_target) * 100

            lines.append(f"-- ÇEYREK SONU TAHMİNİ ({len(active_months)} ay verisi ile) --")
            lines.append(f"Aylık ortalama sipariş: {format_currency(avg_monthly_order)}")
            lines.append(f"Tahmini çeyrek sonu sipariş: {format_currency(projected_order)}")
            lines.append(f"Tahmini çeyrek sonu HGO: %{projected_hgo:.1f}")
            lines.append("")
        else:
            projected_order = total_order
            projected_invoice = total_invoice
            projected_hgo = current_hgo
            lines.append("-- ÇEYREK TAMAMLANDI --")
            lines.append(f"Toplam sipariş: {format_currency(total_order)}")
            lines.append(f"Çeyrek HGO: %{current_hgo:.1f}")
            lines.append("")

        # --- Icinde bulunulan ay icin bireysel oneri ---
        hgo_tiers = [
            (Decimal('120'), Decimal('3'), 'Altın'),
            (Decimal('110'), Decimal('2.5'), 'Gümüş'),
            (Decimal('100'), Decimal('2'), 'Bronz'),
            (Decimal('90'), Decimal('1'), 'Başlangıç'),
        ]

        if current_month in months and current_month in monthly_data:
            m_data = monthly_data[current_month]
            m_target = m_data['target']
            m_order = m_data['realized_order']
            m_name = get_turkish_month_name(current_month)

            if m_target > 0:
                m_hgo = (m_order / m_target) * 100
                lines.append(f"-- {m_name.upper()} AYI BIREYSEL HEDEF DURUMU --")
                lines.append(f"Hedef: {format_currency(m_target)} | Sipariş: {format_currency(m_order)} | HGO: %{m_hgo:.1f}")

                for threshold, rate, name in reversed(hgo_tiers):
                    needed = (threshold / 100) * m_target
                    if m_order >= needed:
                        lines.append(f"  %{threshold} ({name} - %{rate} prim): ULAŞILDI")
                    else:
                        gap = needed - m_order
                        lines.append(f"  %{threshold} ({name} - %{rate} prim): {format_currency(gap)} daha sipariş gerekli")

                lines.append("")

        # --- Ceyrein 3. ayinda EkPrim onerisi ---
        third_month = months[-1]  # Ceyrein son ayi
        if current_month == third_month and ek_prim_tiers:
            sorted_tiers = sorted(ek_prim_tiers, key=lambda t: t['alt_sinir'])
            lines.append("-- EK PRİM DURUMU --")

            reached = None
            next_tier = None
            for tier in sorted(ek_prim_tiers, key=lambda t: t['alt_sinir'], reverse=True):
                if projected_order >= tier['alt_sinir']:
                    if reached is None:
                        reached = tier
                else:
                    next_tier = tier

            if reached:
                lines.append(
                    f"  Mevcut dilim: {format_currency(reached['alt_sinir'])} (%{reached['oran']} ek prim) - ULAŞILDI"
                )
            else:
                lines.append("  Henuz hiçbir dilime ulaşılamadı.")

            if next_tier:
                gap = next_tier['alt_sinir'] - projected_order
                lines.append(
                    f"  Sonraki dilim: {format_currency(next_tier['alt_sinir'])} (%{next_tier['oran']} ek prim)"
                    f" - {format_currency(gap)} daha sipariş gerekli"
                )

            lines.append("")

        # --- Strateji onerisi (hedef her zaman %100) ---
        lines.append("-- STRATEJI ÖNERİSİ --")

        needed_for_100 = total_target  # %100 = total_target
        if projected_order >= needed_for_100:
            lines.append(f"  Çeyrek hedefi (%100): ULAŞILDI")
        else:
            gap = needed_for_100 - total_order
            if remaining_months:
                monthly_needed = gap / len(remaining_months)
                lines.append(
                    f"  Çeyrek hedefi (%100 - Bronz): {format_currency(gap)} daha sipariş gerekli"
                )
                lines.append(
                    f"  Kalan {len(remaining_months)} ayda aylik {format_currency(monthly_needed)} sipariş ile ulaşılabilir."
                )
            else:
                lines.append(
                    f"  Çeyrek hedefi (%100 - Bronz): {format_currency(gap)} eksik"
                )

        # Tahmini prim kazanci
        if projected_hgo >= 90:
            proj_result = PrimCalculator.calculate_monthly_premium(
                projected_order, total_target, projected_invoice
            )
            lines.append(f"  Tahmini toplam aylık prim: {format_currency(proj_result['premium_amount'])}")

        return lines


# ============================================================================
# STORAGE MANAGER
# ============================================================================

class StorageManager:
    """Google Sheets uzerinde Hedef verilerini yoneten sinif."""

    WORKSHEET_NAME = 'Hedef'
    HEADERS = ['Yıl', 'Çeyrek', 'Ay', 'Hedef Tutar']

    def __init__(self, config_manager: CentralConfigManager):
        self.config_manager = config_manager

    def _get_or_create_worksheet(self):
        """Hedef worksheet'ini ac veya olustur."""
        spreadsheet = self.config_manager.gc.open_by_key(
            self.config_manager.MASTER_SPREADSHEET_ID
        )
        try:
            worksheet = spreadsheet.worksheet(self.WORKSHEET_NAME)
        except Exception:
            worksheet = spreadsheet.add_worksheet(
                title=self.WORKSHEET_NAME,
                rows=100,
                cols=len(self.HEADERS)
            )
            worksheet.update(values=[self.HEADERS], range_name='A1')
        return worksheet

    def load_targets(self, year: int, quarter: int) -> list:
        """
        Belirtilen yil ve ceyrek icin hedefleri yukler.

        Returns:
            [{'yil': 2026, 'ceyrek': 'Q1', 'ay': 1, 'hedef_tutar': Decimal(...)}, ...]
        """
        try:
            worksheet = self._get_or_create_worksheet()
            all_values = worksheet.get_all_values()

            if len(all_values) <= 1:
                return []

            headers = all_values[0]
            try:
                yil_idx = headers.index('Yıl')
                ceyrek_idx = headers.index('Çeyrek')
                ay_idx = headers.index('Ay')
                tutar_idx = headers.index('Hedef Tutar')
            except ValueError:
                return []

            quarter_str = f"Q{quarter}"
            targets = []

            for row in all_values[1:]:
                if len(row) > max(yil_idx, ceyrek_idx, ay_idx, tutar_idx):
                    if row[yil_idx].strip() == str(year) and row[ceyrek_idx].strip() == quarter_str:
                        try:
                            targets.append({
                                'yil': year,
                                'ceyrek': quarter_str,
                                'ay': int(row[ay_idx].strip()),
                                'hedef_tutar': Decimal(row[tutar_idx].strip())
                            })
                        except (ValueError, InvalidOperation):
                            continue

            return targets

        except Exception as e:
            logger.error(f"Hedef yukleme hatasi: {e}")
            return []

    def save_targets(self, year: int, quarter: int, targets: list):
        """
        Hedefleri Google Sheets'e kaydeder.
        Ayni donem icin mevcut kayitlari temizleyip yenilerini yazar.
        """
        try:
            worksheet = self._get_or_create_worksheet()
            all_values = worksheet.get_all_values()

            quarter_str = f"Q{quarter}"

            # Mevcut veriyi oku, bu doneme ait olmayanlari tut
            kept_rows = []
            if len(all_values) > 1:
                headers = all_values[0]
                try:
                    yil_idx = headers.index('Yıl')
                    ceyrek_idx = headers.index('Çeyrek')
                except ValueError:
                    yil_idx, ceyrek_idx = 0, 1

                for row in all_values[1:]:
                    if len(row) > max(yil_idx, ceyrek_idx):
                        if not (row[yil_idx].strip() == str(year) and row[ceyrek_idx].strip() == quarter_str):
                            kept_rows.append(row)

            # Yeni hedef satirlarini ekle
            for t in targets:
                kept_rows.append([
                    str(year),
                    quarter_str,
                    str(t['ay']),
                    str(t['hedef_tutar'])
                ])

            # Worksheet'i temizle ve yeniden yaz
            worksheet.clear()
            all_data = [self.HEADERS] + kept_rows
            worksheet.update(values=all_data, range_name='A1')

        except Exception as e:
            logger.error(f"Hedef kaydetme hatasi: {e}")

    # --- EkPrim Worksheet ---

    EK_PRIM_WORKSHEET = 'EkPrim'
    EK_PRIM_HEADERS = ['Yıl', 'Çeyrek', 'AltSınır', 'PrimOranı']

    EK_PRIM_DEFAULTS = [
        {'alt_sinir': Decimal('50000000'), 'oran': Decimal('5')},
        {'alt_sinir': Decimal('35000000'), 'oran': Decimal('4')},
        {'alt_sinir': Decimal('20000000'), 'oran': Decimal('3')},
        {'alt_sinir': Decimal('10000000'), 'oran': Decimal('2')},
        {'alt_sinir': Decimal('7000000'), 'oran': Decimal('1')},
    ]

    def _get_or_create_ek_prim_worksheet(self):
        """EkPrim worksheet'ini ac veya olustur."""
        spreadsheet = self.config_manager.gc.open_by_key(
            self.config_manager.MASTER_SPREADSHEET_ID
        )
        try:
            worksheet = spreadsheet.worksheet(self.EK_PRIM_WORKSHEET)
        except Exception:
            worksheet = spreadsheet.add_worksheet(
                title=self.EK_PRIM_WORKSHEET,
                rows=100,
                cols=len(self.EK_PRIM_HEADERS)
            )
            worksheet.update(values=[self.EK_PRIM_HEADERS], range_name='A1')
        return worksheet

    def load_ek_prim_tiers(self, year: int, quarter: int) -> list:
        """
        Belirtilen yil ve ceyrek icin ek prim dilimlerini yukler.

        Returns:
            [{'alt_sinir': Decimal, 'oran': Decimal}, ...] buyukten kucuge sirali
        """
        try:
            worksheet = self._get_or_create_ek_prim_worksheet()
            all_values = worksheet.get_all_values()

            if len(all_values) <= 1:
                return []

            headers = all_values[0]
            try:
                yil_idx = headers.index('Yıl')
                ceyrek_idx = headers.index('Çeyrek')
                sinir_idx = headers.index('AltSınır')
                oran_idx = headers.index('PrimOranı')
            except ValueError:
                return []

            quarter_str = f"Q{quarter}"
            tiers = []

            for row in all_values[1:]:
                if len(row) > max(yil_idx, ceyrek_idx, sinir_idx, oran_idx):
                    if row[yil_idx].strip() == str(year) and row[ceyrek_idx].strip() == quarter_str:
                        try:
                            tiers.append({
                                'alt_sinir': Decimal(row[sinir_idx].strip()),
                                'oran': Decimal(row[oran_idx].strip())
                            })
                        except (ValueError, InvalidOperation):
                            continue

            return sorted(tiers, key=lambda t: t['alt_sinir'], reverse=True)

        except Exception as e:
            logger.error(f"EkPrim yükleme hatası: {e}")
            return []

    def save_ek_prim_tiers(self, year: int, quarter: int, tiers: list):
        """
        Ek prim dilimlerini Google Sheets'e kaydeder.
        Ayni donem icin mevcut kayitlari temizleyip yenilerini yazar.
        """
        try:
            worksheet = self._get_or_create_ek_prim_worksheet()
            all_values = worksheet.get_all_values()

            quarter_str = f"Q{quarter}"

            kept_rows = []
            if len(all_values) > 1:
                headers = all_values[0]
                try:
                    yil_idx = headers.index('Yıl')
                    ceyrek_idx = headers.index('Çeyrek')
                except ValueError:
                    yil_idx, ceyrek_idx = 0, 1

                for row in all_values[1:]:
                    if len(row) > max(yil_idx, ceyrek_idx):
                        if not (row[yil_idx].strip() == str(year) and row[ceyrek_idx].strip() == quarter_str):
                            kept_rows.append(row)

            for t in tiers:
                kept_rows.append([
                    str(year),
                    quarter_str,
                    str(t['alt_sinir']),
                    str(t['oran'])
                ])

            worksheet.clear()
            all_data = [self.EK_PRIM_HEADERS] + kept_rows
            worksheet.update(values=all_data, range_name='A1')

        except Exception as e:
            logger.error(f"EkPrim kaydetme hatası: {e}")


# ============================================================================
# YARDIMCI FONKSIYONLAR
# ============================================================================

def get_quarter_dates(year: int, quarter: int):
    """Ceyregin baslangic ve bitis tarihlerini dondurur."""
    mapping = {
        1: (date(year, 1, 1), date(year, 3, 31)),
        2: (date(year, 4, 1), date(year, 6, 30)),
        3: (date(year, 7, 1), date(year, 9, 30)),
        4: (date(year, 10, 1), date(year, 12, 31)),
    }
    return mapping.get(quarter, (None, None))


def get_quarter_months(quarter: int) -> list:
    """Ceyregin ay numaralarini dondurur."""
    mapping = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9], 4: [10, 11, 12]}
    return mapping.get(quarter, [])


def get_turkish_month_name(month_num: int) -> str:
    return TURKISH_MONTHS.get(month_num, str(month_num))


def format_currency(amount: Decimal) -> str:
    """Decimal degerini para birimi formatinda gosterir (ondalik yok)."""
    return f"{amount:,.0f} TL"


def _parse_date(date_str: str):
    """API'den gelen tarih stringini datetime'a cevirir."""
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            return datetime.strptime(date_str.split('T')[0], "%Y-%m-%d")
        elif '-' in date_str:
            return datetime.strptime(date_str, "%Y-%m-%d")
        elif '.' in date_str:
            return datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        pass
    return None


def _parse_invoice_date(date_str: str):
    """
    purchaseInvoiceDate alanini parse eder.
    Desteklenen formatlar: YYYYMMDD, DD.MM.YYYY, YYYY-MM-DD
    """
    if not date_str or date_str == '00000000':
        return None
    try:
        if len(date_str) == 8 and date_str.isdigit():
            return datetime.strptime(date_str, "%Y%m%d")
        elif '.' in date_str:
            return datetime.strptime(date_str, "%d.%m.%Y")
        elif '-' in date_str:
            return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        pass
    return None


def _safe_decimal(value) -> Decimal:
    """
    Herhangi bir degeri guvenli sekilde Decimal'e cevirir.
    Turkce sayi formatini destekler: '5409,39' -> 5409.39, '1,000' -> 1.0
    """
    try:
        if value is None or value == '':
            return Decimal('0')
        s = str(value).strip()
        # Turkce format: nokta binlik ayirici, virgul ondalik ayirici
        if ',' in s:
            s = s.replace('.', '').replace(',', '.')
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal('0')


def _process_raw_data(raw_data: list, start_date: date, end_date: date, months: list, target_map: dict) -> dict:
    """Ham API verisini isleyerek aylik sonuclari dondurur."""
    monthly_data = {}
    for m in months:
        monthly_data[m] = {
            'target': target_map.get(m, Decimal('0')),
            'realized_order': Decimal('0'),
            'realized_invoice': Decimal('0'),
        }

    for item in raw_data:
        qty = _safe_decimal(item.get('orderLineQuantity', '0'))
        net_price = _safe_decimal(item.get('netPrice', '0'))
        original_price = _safe_decimal(item.get('originalPrice', '0'))

        price = net_price if net_price != 0 else original_price
        line_total = qty * price

        # SIPARIS: sadece orderDate1 ceyrekte olan kayitlar
        o_date = _parse_date(item.get('orderDate1', ''))
        if o_date and o_date.month in monthly_data:
            if start_date <= o_date.date() <= end_date:
                monthly_data[o_date.month]['realized_order'] += line_total

        # FATURA: purchaseInvoiceDate ceyrekte olan TUM kayitlar
        inv_date_str = item.get('purchaseInvoiceDate', '')
        if inv_date_str and inv_date_str != '00000000':
            inv_date = _parse_invoice_date(inv_date_str)
            if inv_date and inv_date.month in monthly_data:
                if start_date <= inv_date.date() <= end_date:
                    monthly_data[inv_date.month]['realized_invoice'] += line_total

    return monthly_data


# ============================================================================
# DATA FETCH WORKER (QThread)
# ============================================================================

class DataFetchWorker(QThread):
    """API veri cekme islemini arka planda calistirir."""
    progress = pyqtSignal(str)
    finished_ok = pyqtSignal(list)
    finished_err = pyqtSignal(str)

    def __init__(self, api_client: PrimApiClient, start_date: str, end_date: str):
        super().__init__()
        self.api_client = api_client
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        try:
            self.progress.emit("API'ye bağlanılıyor...")
            data = self.api_client.fetch_data(self.start_date, self.end_date)
            if data:
                self.progress.emit(f"{len(data)} kayıt çekildi. Hesaplanıyor...")
                self.finished_ok.emit(data)
            else:
                self.finished_err.emit("API'den veri çekilemedi veya kayıt bulunamadı.")
        except Exception as e:
            self.finished_err.emit(f"Hata: {e}")


# ============================================================================
# STYLESHEET
# ============================================================================

STYLESHEET = """
QWidget {
    font-family: 'Segoe UI', Arial;
    font-size: 12px;
    background-color: #f8f9fa;
}
QGroupBox {
    font-weight: bold;
    font-size: 13px;
    border: 1px solid #dee2e6;
    border-radius: 6px;
    margin-top: 10px;
    padding-top: 14px;
    background-color: #ffffff;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 12px;
    padding: 0 6px;
    color: #495057;
}
QLineEdit {
    padding: 6px 10px;
    border: 1px solid #ced4da;
    border-radius: 4px;
    background-color: #ffffff;
    font-size: 13px;
}
QLineEdit:focus {
    border-color: #007acc;
}
QComboBox {
    padding: 6px 10px;
    border: 1px solid #ced4da;
    border-radius: 4px;
    background-color: #ffffff;
    font-size: 13px;
    min-width: 80px;
}
QPushButton {
    padding: 8px 20px;
    border: none;
    border-radius: 4px;
    font-size: 13px;
    font-weight: bold;
    color: #ffffff;
    background-color: #007acc;
}
QPushButton:hover {
    background-color: #005fa3;
}
QPushButton:disabled {
    background-color: #adb5bd;
}
QPushButton#btn_save {
    background-color: #28a745;
}
QPushButton#btn_save:hover {
    background-color: #218838;
}
QTableWidget {
    border: 1px solid #dee2e6;
    border-radius: 4px;
    background-color: #ffffff;
    gridline-color: #e9ecef;
    font-size: 12px;
}
QTableWidget::item {
    padding: 6px;
}
QHeaderView::section {
    background-color: #343a40;
    color: #ffffff;
    padding: 8px;
    border: none;
    font-weight: bold;
    font-size: 12px;
}
QProgressBar {
    border: 1px solid #dee2e6;
    border-radius: 4px;
    text-align: center;
    background-color: #e9ecef;
    height: 22px;
}
QProgressBar::chunk {
    background-color: #007acc;
    border-radius: 3px;
}
"""


# ============================================================================
# GUI UYGULAMASI
# ============================================================================

class PrimHesaplamaApp(QWidget):
    """Dogtas Prim Hesaplama PyQt5 Arayuzu"""

    def __init__(self):
        super().__init__()
        self.setWindowTitle("DOĞTAŞ HGO")
        self.resize(950, 950)
        self.setStyleSheet(STYLESHEET)

        # Backend
        self.config_manager = CentralConfigManager()
        self.storage = StorageManager(self.config_manager)
        self.api_client = PrimApiClient(self.config_manager)
        self.worker = None

        # State
        self.target_inputs = {}
        self.ek_prim_tiers = None  # Yuklenen ek prim dilimleri

        self._setup_ui()
        self._load_targets()
        self._load_ek_prim_tiers()

    # ---------------------------------------------------------------
    # UI SETUP
    # ---------------------------------------------------------------

    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(16, 16, 16, 16)

        # -- Baslik --
        title = QLabel("DOĞTAŞ HGO")
        title.setAlignment(Qt.AlignCenter)
        title.setFont(QFont("Segoe UI", 16, QFont.Bold))
        title.setStyleSheet("color: #212529; padding: 8px; background: transparent;")
        main_layout.addWidget(title)

        # -- Tab Widget --
        self.tab_widget = QTabWidget()
        self.tab_widget.setStyleSheet(
            "QTabWidget::pane { border: 1px solid #dee2e6; border-radius: 4px; background: #ffffff; }"
            "QTabBar::tab { padding: 10px 40px; font-size: 14px; font-weight: bold; min-width: 180px; }"
            "QTabBar::tab:selected { background: #007acc; color: #ffffff; border-radius: 4px 4px 0 0; }"
            "QTabBar::tab:!selected { background: #e9ecef; color: #495057; }"
        )
        main_layout.addWidget(self.tab_widget)

        # === TAB 1: PRIM HESAPLAMA ===
        tab1 = QWidget()
        tab1_layout = QVBoxLayout(tab1)
        tab1_layout.setSpacing(10)

        # -- Donem + Hesapla --
        period_group = QGroupBox("Dönem Seçimi")
        period_layout = QHBoxLayout(period_group)

        period_layout.addWidget(QLabel("Yıl:"))
        self.year_combo = QComboBox()
        current_year = datetime.now().year
        for y in range(current_year - 2, current_year + 3):
            self.year_combo.addItem(str(y), y)
        self.year_combo.setCurrentText(str(current_year))
        self.year_combo.currentIndexChanged.connect(self._on_period_changed)
        period_layout.addWidget(self.year_combo)

        period_layout.addSpacing(20)
        period_layout.addWidget(QLabel("Çeyrek:"))
        self.quarter_combo = QComboBox()
        for q in range(1, 5):
            self.quarter_combo.addItem(f"Q{q}", q)
        current_quarter = (datetime.now().month - 1) // 3 + 1
        self.quarter_combo.setCurrentIndex(current_quarter - 1)
        self.quarter_combo.currentIndexChanged.connect(self._on_period_changed)
        period_layout.addWidget(self.quarter_combo)

        period_layout.addSpacing(30)
        self.btn_calculate = QPushButton("HESAPLA")
        self.btn_calculate.setMinimumWidth(140)
        self.btn_calculate.setMinimumHeight(36)
        self.btn_calculate.clicked.connect(self._on_calculate)
        period_layout.addWidget(self.btn_calculate)

        period_layout.addStretch()
        tab1_layout.addWidget(period_group)

        # -- Hedefler --
        self.target_group = QGroupBox("Aylık Hedefler")
        self.target_layout = QGridLayout(self.target_group)
        self.target_layout.setSpacing(8)

        self.btn_save_targets = QPushButton("Hedefleri Kaydet")
        self.btn_save_targets.setObjectName("btn_save")
        self.btn_save_targets.clicked.connect(self._save_targets)

        self._rebuild_target_inputs()
        tab1_layout.addWidget(self.target_group)

        # -- Sonuc Tablosu --
        table_group = QGroupBox("Aylık Performans Raporu")
        table_layout = QVBoxLayout(table_group)

        self.result_table = QTableWidget(3, 7)
        self.result_table.setHorizontalHeaderLabels(
            ["Ay", "Hedef", "Sipariş", "HGO %", "Fatura", "Prim Oranı", "Prim Tutarı"]
        )
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.result_table.verticalHeader().setVisible(False)
        self.result_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.result_table.setSelectionMode(QTableWidget.SingleSelection)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setStyleSheet(
            "QTableWidget { alternate-background-color: #f5f5f5; }"
        )
        self.result_table.setMinimumHeight(140)
        table_layout.addWidget(self.result_table)

        tab1_layout.addWidget(table_group)

        # -- Ceyrek Ozeti --
        summary_group = QGroupBox("Çeyrek Sonu Özeti")
        summary_layout = QGridLayout(summary_group)
        summary_layout.setSpacing(8)

        self.lbl_total_target = QLabel("-")
        self.lbl_total_order = QLabel("-")
        self.lbl_total_invoice = QLabel("-")
        self.lbl_quarter_hgo = QLabel("-")
        self.lbl_extra_premium = QLabel("-")

        labels_data = [
            ("Toplam Hedef:", self.lbl_total_target, 0, 0),
            ("Toplam Sipariş:", self.lbl_total_order, 0, 2),
            ("Toplam Fatura:", self.lbl_total_invoice, 0, 4),
            ("Çeyrek HGO:", self.lbl_quarter_hgo, 1, 0),
            ("Ek Hacim Primi:", self.lbl_extra_premium, 1, 2),
        ]
        for text, widget, row, col in labels_data:
            lbl = QLabel(text)
            lbl.setFont(QFont("Segoe UI", 11, QFont.Bold))
            lbl.setStyleSheet("background: transparent;")
            summary_layout.addWidget(lbl, row, col)
            widget.setFont(QFont("Segoe UI", 11))
            widget.setStyleSheet("background: transparent;")
            summary_layout.addWidget(widget, row, col + 1)

        tab1_layout.addWidget(summary_group)

        # -- Toplam Prim --
        self.lbl_total_premium = QLabel("TOPLAM HAK EDILEN PRIM: -")
        self.lbl_total_premium.setAlignment(Qt.AlignCenter)
        self.lbl_total_premium.setFont(QFont("Segoe UI", 18, QFont.Bold))
        self.lbl_total_premium.setStyleSheet(
            "color: #28a745; padding: 14px; background-color: #ffffff; "
            "border: 2px solid #28a745; border-radius: 8px;"
        )
        tab1_layout.addWidget(self.lbl_total_premium)

        # -- Tahmin ve Oneri (2 sutunlu) --
        forecast_group = QGroupBox("Tahmin ve Öneri")
        forecast_layout = QHBoxLayout(forecast_group)
        forecast_layout.setSpacing(10)

        forecast_text_style = (
            "QTextEdit { background-color: #f8f9fa; border: 1px solid #dee2e6; "
            "border-radius: 4px; font-family: 'Consolas', 'Courier New', monospace; "
            "font-size: 12px; padding: 8px; color: #212529; }"
        )

        # Sol: Ceyrek tahmini + strateji
        self.forecast_left = QTextEdit()
        self.forecast_left.setReadOnly(True)
        self.forecast_left.setMinimumHeight(220)
        self.forecast_left.setStyleSheet(forecast_text_style)
        self.forecast_left.setPlaceholderText("Çeyrek sonu tahmini...")
        forecast_layout.addWidget(self.forecast_left)

        # Sag: Bireysel oneri + ek prim
        self.forecast_right = QTextEdit()
        self.forecast_right.setReadOnly(True)
        self.forecast_right.setMinimumHeight(220)
        self.forecast_right.setStyleSheet(forecast_text_style)
        self.forecast_right.setPlaceholderText("Aylık bireysel hedef durumu...")
        forecast_layout.addWidget(self.forecast_right)

        tab1_layout.addWidget(forecast_group)

        self.tab_widget.addTab(tab1, "Prim Hesaplama")

        # === TAB 2: EK PRIM AYARLARI ===
        tab2 = QWidget()
        tab2_layout = QVBoxLayout(tab2)
        tab2_layout.setSpacing(10)

        # -- Donem secimi (EkPrim icin) --
        ep_period_group = QGroupBox("Dönem Seçimi")
        ep_period_layout = QHBoxLayout(ep_period_group)

        ep_period_layout.addWidget(QLabel("Yıl:"))
        self.ep_year_combo = QComboBox()
        for y in range(current_year - 2, current_year + 3):
            self.ep_year_combo.addItem(str(y), y)
        self.ep_year_combo.setCurrentText(str(current_year))
        ep_period_layout.addWidget(self.ep_year_combo)

        ep_period_layout.addSpacing(20)
        ep_period_layout.addWidget(QLabel("Çeyrek:"))
        self.ep_quarter_combo = QComboBox()
        for q in range(1, 5):
            self.ep_quarter_combo.addItem(f"Q{q}", q)
        self.ep_quarter_combo.setCurrentIndex(current_quarter - 1)
        ep_period_layout.addWidget(self.ep_quarter_combo)

        ep_period_layout.addSpacing(20)
        btn_ep_load = QPushButton("Yükle")
        btn_ep_load.clicked.connect(self._load_ek_prim_tiers)
        ep_period_layout.addWidget(btn_ep_load)

        ep_period_layout.addStretch()
        tab2_layout.addWidget(ep_period_group)

        # -- Dilim tablosu --
        tiers_group = QGroupBox("Toplam Sipariş Hacmi Prim Dilimleri")
        tiers_layout = QVBoxLayout(tiers_group)

        self.ep_table = QTableWidget(0, 2)
        self.ep_table.setHorizontalHeaderLabels(["Alt Sınır (TL)", "Prim Oranı (%)"])
        self.ep_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.ep_table.verticalHeader().setVisible(False)
        self.ep_table.setMinimumHeight(200)
        self.ep_table.setStyleSheet(
            "QTableWidget { alternate-background-color: #f5f5f5; }"
        )
        self.ep_table.setAlternatingRowColors(True)
        tiers_layout.addWidget(self.ep_table)

        # Butonlar
        ep_btn_layout = QHBoxLayout()
        btn_add_row = QPushButton("Satır Ekle")
        btn_add_row.setMinimumWidth(100)
        btn_add_row.clicked.connect(self._ep_add_row)
        ep_btn_layout.addWidget(btn_add_row)

        btn_del_row = QPushButton("Satır Sil")
        btn_del_row.setMinimumWidth(100)
        btn_del_row.setStyleSheet(
            "QPushButton { background-color: #dc3545; }"
            "QPushButton:hover { background-color: #c82333; }"
        )
        btn_del_row.clicked.connect(self._ep_del_row)
        ep_btn_layout.addWidget(btn_del_row)

        btn_defaults = QPushButton("Varsayılanlari Yükle")
        btn_defaults.setMinimumWidth(140)
        btn_defaults.setStyleSheet(
            "QPushButton { background-color: #6c757d; }"
            "QPushButton:hover { background-color: #5a6268; }"
        )
        btn_defaults.clicked.connect(self._ep_load_defaults)
        ep_btn_layout.addWidget(btn_defaults)

        ep_btn_layout.addStretch()

        btn_ep_save = QPushButton("Kaydet")
        btn_ep_save.setObjectName("btn_save")
        btn_ep_save.setMinimumWidth(120)
        btn_ep_save.clicked.connect(self._save_ek_prim_tiers)
        ep_btn_layout.addWidget(btn_ep_save)

        tiers_layout.addLayout(ep_btn_layout)
        tab2_layout.addWidget(tiers_group)

        # Aciklama
        ep_info = QLabel(
            "Not: Ek hacim primi, ceyrekteki 3 aylik toplam siparis hedefini %100 ve uzeri "
            "gerceklestiren bayiler icin gecerlidir. Prim orani toplam siparis tutarina gore "
            "belirlenir, prim tutari ise toplam faturalanan net tutar uzerinden hesaplanir."
        )
        ep_info.setWordWrap(True)
        ep_info.setStyleSheet(
            "color: #6c757d; padding: 10px; background: #f8f9fa; "
            "border: 1px solid #dee2e6; border-radius: 4px;"
        )
        tab2_layout.addWidget(ep_info)

        tab2_layout.addStretch()
        self.tab_widget.addTab(tab2, "EkPrim Ayarlari")

        # -- Progress + Status (tab disinda, altta) --
        status_layout = QHBoxLayout()
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # indeterminate
        self.progress_bar.setVisible(False)
        status_layout.addWidget(self.progress_bar)

        self.status_label = QLabel("Hazır")
        self.status_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.status_label.setStyleSheet("color: #6c757d; background: transparent;")
        status_layout.addWidget(self.status_label)
        main_layout.addLayout(status_layout)

    # ---------------------------------------------------------------
    # DONEM DEGISIMI
    # ---------------------------------------------------------------

    def _rebuild_target_inputs(self):
        """Hedef input alanlarini secili ceyrege gore yeniden olusturur."""
        # Kaydet butonunu layout'tan cikar (silinmesin)
        self.target_layout.removeWidget(self.btn_save_targets)

        # Diger widget'lari temizle
        while self.target_layout.count():
            child = self.target_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()

        self.target_inputs = {}
        self.target_labels = {}

        quarter = self.quarter_combo.currentData()
        if quarter is None:
            return

        months = get_quarter_months(quarter)
        for i, m in enumerate(months):
            lbl = QLabel(f"{get_turkish_month_name(m)}:")
            lbl.setMinimumWidth(60)
            self.target_layout.addWidget(lbl, 0, i * 3)
            self.target_labels[m] = lbl

            inp = QLineEdit()
            inp.setPlaceholderText("0")
            inp.setAlignment(Qt.AlignRight)
            inp.setMinimumWidth(150)
            self.target_layout.addWidget(inp, 0, i * 3 + 1)
            self.target_inputs[m] = inp

            tl_lbl = QLabel("TL")
            self.target_layout.addWidget(tl_lbl, 0, i * 3 + 2)

        self.target_layout.addWidget(self.btn_save_targets, 1, 0, 1, 9, Qt.AlignRight)

    def _on_period_changed(self):
        """Yil veya ceyrek degistiginde hedef alanlarini yeniden olusturur."""
        self._rebuild_target_inputs()
        self._load_targets()

    # ---------------------------------------------------------------
    # EK PRIM AYARLARI (Tab 2)
    # ---------------------------------------------------------------

    def _load_ek_prim_tiers(self):
        """Google Sheets'ten ek prim dilimlerini yukler ve tabloya yazar."""
        year = self.ep_year_combo.currentData()
        quarter = self.ep_quarter_combo.currentData()
        if year is None or quarter is None:
            return

        tiers = self.storage.load_ek_prim_tiers(year, quarter)

        if not tiers:
            self._ep_load_defaults()
            self.status_label.setText(f"EkPrim: {year} Q{quarter} icin kayıt yok, varsayılanlar yüklendi")
            return

        self.ek_prim_tiers = tiers
        self._ep_populate_table(tiers)
        self.status_label.setText(f"EkPrim: {year} Q{quarter} dilimleri yüklendi")

    def _ep_populate_table(self, tiers: list):
        """EkPrim tablosunu dilim verileriyle doldurur."""
        self.ep_table.setRowCount(len(tiers))
        for row, tier in enumerate(tiers):
            sinir_item = QTableWidgetItem(f"{tier['alt_sinir']:,.0f}")
            sinir_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.ep_table.setItem(row, 0, sinir_item)

            oran_item = QTableWidgetItem(f"{tier['oran']}")
            oran_item.setTextAlignment(Qt.AlignCenter)
            self.ep_table.setItem(row, 1, oran_item)

    def _ep_add_row(self):
        """EkPrim tablosuna bos satir ekler."""
        row = self.ep_table.rowCount()
        self.ep_table.insertRow(row)
        sinir_item = QTableWidgetItem("0")
        sinir_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
        self.ep_table.setItem(row, 0, sinir_item)
        oran_item = QTableWidgetItem("0")
        oran_item.setTextAlignment(Qt.AlignCenter)
        self.ep_table.setItem(row, 1, oran_item)

    def _ep_del_row(self):
        """EkPrim tablosundan secili satiri siler."""
        row = self.ep_table.currentRow()
        if row >= 0:
            self.ep_table.removeRow(row)

    def _ep_load_defaults(self):
        """Varsayilan ek prim dilimlerini tabloya yukler."""
        defaults = StorageManager.EK_PRIM_DEFAULTS
        self._ep_populate_table(defaults)

    def _save_ek_prim_tiers(self):
        """EkPrim tablosundaki verileri Google Sheets'e kaydeder."""
        year = self.ep_year_combo.currentData()
        quarter = self.ep_quarter_combo.currentData()

        tiers = []
        for row in range(self.ep_table.rowCount()):
            sinir_text = self.ep_table.item(row, 0)
            oran_text = self.ep_table.item(row, 1)
            if not sinir_text or not oran_text:
                continue
            try:
                sinir_val = sinir_text.text().strip().replace(',', '').replace(' ', '')
                oran_val = oran_text.text().strip().replace(',', '.')
                alt_sinir = Decimal(sinir_val)
                oran = Decimal(oran_val)
                if alt_sinir < 0 or oran < 0:
                    QMessageBox.warning(self, "Uyarı", "Negatif değer girilemez.")
                    return
                tiers.append({'alt_sinir': alt_sinir, 'oran': oran})
            except (InvalidOperation, ValueError):
                QMessageBox.warning(self, "Uyarı", f"Satır {row + 1}: Geçerli sayı giriniz.")
                return

        if not tiers:
            QMessageBox.warning(self, "Uyarı", "En az bir dilim tanımlanmalıdır.")
            return

        try:
            self.storage.save_ek_prim_tiers(year, quarter, tiers)
            self.ek_prim_tiers = sorted(tiers, key=lambda t: t['alt_sinir'], reverse=True)
            self.status_label.setText(f"EkPrim: {year} Q{quarter} dilimleri kaydedildi")
        except Exception as e:
            QMessageBox.critical(self, "Hata", f"Kaydetme hatası: {e}")

    # ---------------------------------------------------------------
    # HEDEF YUKLEME / KAYDETME
    # ---------------------------------------------------------------

    def _load_targets(self):
        """Google Sheets'ten mevcut hedefleri yukleyip input alanlarina yazar."""
        year = self.year_combo.currentData()
        quarter = self.quarter_combo.currentData()
        if year is None or quarter is None:
            return

        try:
            targets = self.storage.load_targets(year, quarter)
            months = get_quarter_months(quarter)
            target_map = {t['ay']: t['hedef_tutar'] for t in targets}

            for m in months:
                if m in self.target_inputs:
                    val = target_map.get(m)
                    if val is not None:
                        self.target_inputs[m].setText(f"{val:,.0f}".replace(",", "."))
                    else:
                        self.target_inputs[m].clear()

            if targets:
                self.status_label.setText("Hedefler yüklendi")
        except Exception as e:
            self.status_label.setText(f"Hedef yükleme hatası: {e}")

    def _save_targets(self):
        """Input alanlarindan hedefleri okuyup Google Sheets'e kaydeder."""
        year = self.year_combo.currentData()
        quarter = self.quarter_combo.currentData()
        months = get_quarter_months(quarter)

        targets = []
        for m in months:
            inp = self.target_inputs.get(m)
            if not inp:
                continue
            val = inp.text().strip()
            if not val:
                QMessageBox.warning(self, "Uyari",
                                    f"{get_turkish_month_name(m)} için hedef giriniz.")
                return
            try:
                val = val.replace('.', '').replace(',', '.')
                amount = Decimal(val)
                if amount < 0:
                    QMessageBox.warning(self, "Uyarı", "Negatif değer girilemez.")
                    return
                targets.append({'ay': m, 'hedef_tutar': amount})
            except (InvalidOperation, ValueError):
                QMessageBox.warning(self, "Uyari",
                                    f"{get_turkish_month_name(m)}: Geçerli bir sayı giriniz.")
                return

        try:
            self.storage.save_targets(year, quarter, targets)
            self.status_label.setText("Hedefler kaydedildi")
        except Exception as e:
            QMessageBox.critical(self, "Hata", f"Kaydetme hatası: {e}")

    # ---------------------------------------------------------------
    # HESAPLAMA
    # ---------------------------------------------------------------

    def _on_calculate(self):
        """Hesapla butonuna tiklandiginda API'den veri cekip hesaplama yapar."""
        year = self.year_combo.currentData()
        quarter = self.quarter_combo.currentData()

        # Hedefleri kontrol et
        months = get_quarter_months(quarter)
        for m in months:
            inp = self.target_inputs.get(m)
            if not inp or not inp.text().strip():
                QMessageBox.warning(self, "Uyarı",
                                    f"{get_turkish_month_name(m)} için hedef giriniz.")
                return

        start_date, end_date = get_quarter_dates(year, quarter)
        extended_start = start_date - relativedelta(years=1)
        api_start = extended_start.strftime("%d.%m.%Y")
        api_end = end_date.strftime("%d.%m.%Y")

        # UI state
        self.btn_calculate.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.status_label.setText("API'den veriler çekiliyor...")

        # Worker thread
        self.worker = DataFetchWorker(self.api_client, api_start, api_end)
        self.worker.progress.connect(self._on_progress)
        self.worker.finished_ok.connect(self._on_data_received)
        self.worker.finished_err.connect(self._on_error)
        self.worker.finished.connect(self._on_worker_done)
        self.worker.start()

    def _on_progress(self, msg: str):
        self.status_label.setText(msg)

    def _on_error(self, msg: str):
        self.status_label.setText(msg)
        QMessageBox.warning(self, "Uyarı", msg)

    def _on_worker_done(self):
        self.btn_calculate.setEnabled(True)
        self.progress_bar.setVisible(False)

    def _on_data_received(self, raw_data: list):
        """API verisi geldiginde hesaplamalari yapar ve sonuclari gosterir."""
        year = self.year_combo.currentData()
        quarter = self.quarter_combo.currentData()
        months = get_quarter_months(quarter)
        start_date, end_date = get_quarter_dates(year, quarter)

        # Hedefleri input'lardan oku
        target_map = {}
        for m in months:
            inp = self.target_inputs.get(m)
            if inp and inp.text().strip():
                val = inp.text().strip().replace('.', '').replace(',', '.')
                target_map[m] = Decimal(val)
            else:
                target_map[m] = Decimal('0')

        # Verileri isle
        monthly_data = _process_raw_data(raw_data, start_date, end_date, months, target_map)

        # Tabloyu doldur
        self._populate_results_table(monthly_data)

        self.status_label.setText(f"{len(raw_data)} kayıt işlendi")

    # ---------------------------------------------------------------
    # TABLO VE OZET
    # ---------------------------------------------------------------

    def _populate_results_table(self, monthly_data: dict):
        """Sonuc tablosunu ve ceyrek ozetini doldurur."""
        sorted_months = sorted(monthly_data.keys())
        self.result_table.setRowCount(len(sorted_months))
        self.result_table.setUpdatesEnabled(False)

        total_premium = Decimal('0')
        quarter_total_target = Decimal('0')
        quarter_total_order = Decimal('0')
        quarter_total_invoice = Decimal('0')

        for row, m in enumerate(sorted_months):
            data = monthly_data[m]
            result = PrimCalculator.calculate_monthly_premium(
                data['realized_order'], data['target'], data['realized_invoice']
            )

            quarter_total_target += data['target']
            quarter_total_order += data['realized_order']
            quarter_total_invoice += data['realized_invoice']
            total_premium += result['premium_amount']

            hgo_val = float(result['hgo'])

            # HGO renk kodlamasi
            if hgo_val >= 100:
                hgo_color = QColor("#28a745")  # yesil
            elif hgo_val >= 90:
                hgo_color = QColor("#fd7e14")  # turuncu
            else:
                hgo_color = QColor("#dc3545")  # kirmizi

            row_data = [
                get_turkish_month_name(m),
                format_currency(data['target']),
                format_currency(data['realized_order']),
                f"%{hgo_val:.1f}",
                format_currency(data['realized_invoice']),
                f"%{result['rate']}",
                format_currency(result['premium_amount']),
            ]

            for col, text in enumerate(row_data):
                item = QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignCenter)
                if col == 0:
                    item.setFont(QFont("Segoe UI", 11, QFont.Bold))
                elif col == 3:
                    item.setForeground(hgo_color)
                    item.setFont(QFont("Segoe UI", 11, QFont.Bold))
                elif col == 6 and result['premium_amount'] > 0:
                    item.setForeground(QColor("#28a745"))
                    item.setFont(QFont("Segoe UI", 11, QFont.Bold))
                self.result_table.setItem(row, col, item)

            self.result_table.setRowHeight(row, 40)

        self.result_table.setUpdatesEnabled(True)

        # Ceyrek ozeti
        extra = PrimCalculator.calculate_quarterly_extra_premium(
            quarter_total_order, quarter_total_target, quarter_total_invoice,
            ek_prim_tiers=self.ek_prim_tiers
        )

        self.lbl_total_target.setText(format_currency(quarter_total_target))
        self.lbl_total_order.setText(format_currency(quarter_total_order))
        self.lbl_total_invoice.setText(format_currency(quarter_total_invoice))
        self.lbl_quarter_hgo.setText(f"%{float(extra['hgo']):.2f}")

        if extra['eligible']:
            self.lbl_extra_premium.setText(
                f"KAZANILDI (%{extra['rate']}) = {format_currency(extra['premium_amount'])}"
            )
            self.lbl_extra_premium.setStyleSheet("color: #28a745; font-weight: bold; background: transparent;")
            total_premium += extra['premium_amount']
        else:
            self.lbl_extra_premium.setText(f"KAZANILMADI ({extra.get('reason', '')})")
            self.lbl_extra_premium.setStyleSheet("color: #dc3545; font-weight: bold; background: transparent;")

        # Toplam prim
        self.lbl_total_premium.setText(f"TOPLAM HAK EDILEN PRIM: {format_currency(total_premium)}")

        # Tahmin ve oneri (sol: ceyrek tahmini + strateji, sag: bireysel + ek prim)
        forecast_lines = PrimCalculator.generate_forecast(
            monthly_data, sorted_months, ek_prim_tiers=self.ek_prim_tiers
        )
        self.forecast_left.clear()
        self.forecast_right.clear()

        # Satirlari bolumlerine gore ayir
        right_sections = ['BIREYSEL HEDEF DURUMU', 'EK HACIM PRIMI DURUMU']
        current_target = self.forecast_left
        for line in forecast_lines:
            # Baslik satiriysa hangi panele ait kontrol et
            if line.startswith("--") and line.endswith("--"):
                if any(s in line for s in right_sections):
                    current_target = self.forecast_right
                elif 'STRATEJI' in line:
                    current_target = self.forecast_left

            self._append_forecast_line(current_target, line)

    def _append_forecast_line(self, widget: QTextEdit, line: str):
        """Tahmin satirini renklendirip widget'a ekler."""
        if line.startswith("--") and line.endswith("--"):
            widget.append(f'<b style="color: #007acc;">{line}</b>')
        elif "ULAŞILDI" in line:
            widget.append(f'<span style="color: #28a745;">{line}</span>')
        elif "eksik" in line or "gerekli" in line:
            widget.append(f'<span style="color: #dc3545;">{line}</span>')
        elif line == "":
            widget.append("")
        else:
            widget.append(line)


# ============================================================================
# ANA UYGULAMA
# ============================================================================

def main():
    app = QApplication(sys.argv)
    window = PrimHesaplamaApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
