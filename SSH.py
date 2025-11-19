"""
SSH (Servis Sipariş Hareketleri) System - Service Account Versiyonu
Merkezi config ve Service Account ile güvenli SSH analizi

Özellikler:
- Service Account ile güvenli Google Sheets erişimi
- Merkezi config yönetimi (PRGsheet)
- Excel'den veri okuma ve Sheets'e yazma
- Tüm hassas bilgiler PRGsheet'te saklanır
"""

import logging
import pandas as pd
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
log_file = log_dir / 'ssh_analizi.log'

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

class SshConfig:
    """
    SSH analizi için yapılandırma sınıfı
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
    def excel_dosya_yolu(self) -> str:
        """
        Excel dosya yolu
        PRGsheet -> Ayar'dan SSH_EXCEL_PATH alınır
        """
        excel_path = self.settings.get('SSH_EXCEL_PATH', '')

        if not excel_path:
            # Varsayılan yol
            excel_path = r"D:/GoogleDrive/PRG/Montaj Raporu.xlsx"

        return excel_path

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

    def mevcut_veriyi_al(self) -> pd.DataFrame:
        """Ssh sayfasındaki mevcut veriyi al"""
        try:
            # PRGsheet'i doğrudan aç
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            try:
                ssh_sayfasi = spreadsheet.worksheet('Ssh')
                mevcut_veri = ssh_sayfasi.get_all_values()

                if len(mevcut_veri) > 1:  # Başlık + veri var mı
                    basliklar = mevcut_veri[0]
                    veri_satirlari = mevcut_veri[1:]
                    df = pd.DataFrame(veri_satirlari, columns=basliklar)

                    # Sayısal sütunları temizle
                    numeric_columns = ['Servis Bakım ID', 'Sözleşme Numarası', 'Yedek Parça Sipariş No',
                                     'Ürün ID', 'Yedek Parça Ürün ID', 'Yedek Parça Ürün Miktarı']
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = df[col].apply(lambda x: str(x).replace('.0', '') if str(x).endswith('.0') else str(x))

                    # "Parça Durumu" sütununu ekle (yoksa)
                    if 'Parça Durumu' not in df.columns:
                        df['Parça Durumu'] = ''

                    return df
                else:
                    return pd.DataFrame()

            except Exception:
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Mevcut veri alınırken hata: {e}")
            return pd.DataFrame()

    def ssh_sayfasini_guncelle(self, veri: pd.DataFrame) -> None:
        """Ssh sayfasını güncelle"""
        try:
            # PRGsheet'i doğrudan aç (Config entry'si gerekmez)
            spreadsheet = self.gc.open_by_key(
                self.config_manager.MASTER_SPREADSHEET_ID
            )

            try:
                ssh_sayfasi = spreadsheet.worksheet('Ssh')
                ssh_sayfasi.clear()
            except:
                ssh_sayfasi = spreadsheet.add_worksheet(title='Ssh', rows=5000, cols=50)

            if not veri.empty:
                # Sayısal sütunları temizle
                numeric_columns = ['Servis Bakım ID', 'Sözleşme Numarası', 'Yedek Parça Sipariş No',
                                 'Ürün ID', 'Yedek Parça Ürün ID', 'Yedek Parça Ürün Miktarı']
                for col in numeric_columns:
                    if col in veri.columns:
                        veri[col] = veri[col].apply(
                            lambda x: str(x).replace('.0', '') if isinstance(x, str) and str(x).endswith('.0') else str(x) if x != '' else ''
                        )

                # Başlık ve veriyi Google Sheets'e yükle
                degerler = [veri.columns.values.tolist()] + veri.values.tolist()
                ssh_sayfasi.update(degerler, value_input_option='USER_ENTERED')

        except Exception as e:
            logger.error(f"Ssh sayfası güncellenirken hata: {e}")
            raise

# ============================================================================
# SSH MANAGER
# ============================================================================

class SshManager:
    """SSH veri yöneticisi"""

    def __init__(self, config: SshConfig):
        self.config = config
        self.sheets_manager = GoogleSheetsManager(config.config_manager)

    def excel_verisi_oku(self) -> pd.DataFrame:
        """Excel dosyasından veri oku"""
        try:
            excel_yolu = self.config.excel_dosya_yolu
            df = pd.read_excel(excel_yolu)

            # Hariç tutulacak sütunları belirle
            haric_sutunlar = [
                'Satış Bürosu', 'Marka', 'Müşteri No', 'Malı Teslim Alan No','Şehir', 'İlçe', 'Belge Kapanış Tarihi',
                'Kalem H. Nedeni', 'Kalem H. Alt Nedeni', 'Montaj Türü','Sözleşme Tarihi', 'Kurulum Durumu',
                'Logismart ID', 'Belge Durumu', 'Kalem Durumu', 'Kalem Durum Nedeni', 'Notlar','Garanti Durumu',
                'SMS Onay Tarihi', 'Sorunlu T.Tarihi','Yedek Parça Mamul', 'Yedek Parça Mamul Tanımı',
                'Yedek Parça Tarih','ERP Durumu', 'Spec Adı', 'Miktar','Kalem Kurulum Durumu',
                'İş Emri', 'İade İş Emri','SMS Onay Durumu', 'Telefon1', 'Telefon2'
            ]

            # Hariç tutulacak sütunları kaldır
            tutulacak_sutunlar = [sutun for sutun in df.columns if sutun not in haric_sutunlar]
            df = df[tutulacak_sutunlar]

            # Tarih sütunlarını işle
            tarih_sutunlari = ['Montaj Belgesi Tarihi']
            for sutun in tarih_sutunlari:
                if sutun in df.columns:
                    df[sutun] = pd.to_datetime(df[sutun], errors='coerce')
                    df[sutun] = df[sutun].dt.strftime('%Y-%m-%d')
                    df[sutun] = df[sutun].fillna('')

            # NaN değerlerini boş string ile doldur
            df = df.fillna('')

            # Sayısal sütunları işle
            numeric_columns = ['Servis Bakım ID', 'Sözleşme Numarası', 'Yedek Parça Sipariş No',
                             'Ürün ID', 'Yedek Parça Ürün ID', 'Yedek Parça Ürün Miktarı']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].apply(
                        lambda x: str(int(float(x))) if x != '' and pd.notna(x) and str(x).strip() != '' else ''
                    )

            # "Yedek Parça Sipariş No" boş olan satırları sil
            if 'Yedek Parça Sipariş No' in df.columns:
                df = df[df['Yedek Parça Sipariş No'].astype(str).str.strip() != '']

            # "Parça Durumu" sütununu ekle
            if 'Parça Durumu' not in df.columns:
                df['Parça Durumu'] = ''

            # Sütun sıralamasını belirle
            istenen_sutun_sirasi = [
                'Parça Durumu', 'Belge Durum Nedeni', 'Montaj Belgesi Tarihi', 'Müşteri Adı',
                'Ürün Adı', 'Yedek Parça Ürün Tanımı', 'Yedek Parça Ürün Miktarı',
                'Sözleşme Numarası', 'Servis Bakım ID', 'Yedek Parça Sipariş No',
                'Ürün ID', 'Yedek Parça Ürün ID'
            ]

            mevcut_sutunlar = [sutun for sutun in istenen_sutun_sirasi if sutun in df.columns]
            kalan_sutunlar = [sutun for sutun in df.columns if sutun not in mevcut_sutunlar]
            df = df[mevcut_sutunlar + kalan_sutunlar]

            return df

        except Exception as e:
            logger.error(f"Excel dosyası okunurken hata: {e}")
            raise

    def verileri_birlestir(self, yeni_df: pd.DataFrame, mevcut_df: pd.DataFrame) -> pd.DataFrame:
        """Yeni veriyi mevcut veri ile birleştir - "Parça Durumu" korunur"""

        if mevcut_df.empty:
            return yeni_df.sort_values(by='Servis Bakım ID', ascending=False)

        # Anahtar sütunlar
        anahtar_sutunlar = ['Servis Bakım ID', 'Yedek Parça Sipariş No', 'Ürün ID', 'Yedek Parça Ürün ID']

        # Kontrol
        for sutun in anahtar_sutunlar:
            if sutun not in yeni_df.columns or sutun not in mevcut_df.columns:
                return yeni_df.sort_values(by='Servis Bakım ID', ascending=False)

        try:
            # Anahtar sütunları string tipine çevir
            for sutun in anahtar_sutunlar:
                yeni_df[sutun] = yeni_df[sutun].astype(str).str.strip()
                mevcut_df[sutun] = mevcut_df[sutun].astype(str).str.strip()

            # Birleşik anahtar oluştur
            yeni_df['_birlestirme_anahtari'] = yeni_df[anahtar_sutunlar].apply(lambda x: ''.join(x), axis=1)
            mevcut_df['_birlestirme_anahtari'] = mevcut_df[anahtar_sutunlar].apply(lambda x: ''.join(x), axis=1)

            # Anahtarları bul
            mevcut_anahtarlar = set(mevcut_df['_birlestirme_anahtari'])
            yeni_anahtarlar = set(yeni_df['_birlestirme_anahtari'])

            guncelleme_anahtarlari = mevcut_anahtarlar.intersection(yeni_anahtarlar)
            ekleme_anahtarlari = yeni_anahtarlar - mevcut_anahtarlar
            saklama_anahtarlari = mevcut_anahtarlar - yeni_anahtarlar

            son_df_listesi = []

            # Güncellenecek kayıtlar: "Parça Durumu" mevcut veriden al
            if guncelleme_anahtarlari:
                for anahtar in guncelleme_anahtarlari:
                    yeni_kayit = yeni_df[yeni_df['_birlestirme_anahtari'] == anahtar].copy()
                    mevcut_kayit = mevcut_df[mevcut_df['_birlestirme_anahtari'] == anahtar]

                    if 'Parça Durumu' in mevcut_kayit.columns and 'Parça Durumu' in yeni_kayit.columns:
                        yeni_kayit['Parça Durumu'] = mevcut_kayit['Parça Durumu'].values[0]

                    son_df_listesi.append(yeni_kayit)

            # Yeni kayıtlar
            if ekleme_anahtarlari:
                yeni_kayitlar = yeni_df[yeni_df['_birlestirme_anahtari'].isin(ekleme_anahtarlari)].copy()
                son_df_listesi.append(yeni_kayitlar)

            # Eski kayıtları koru
            if saklama_anahtarlari:
                saklanacak_eski_kayitlar = mevcut_df[mevcut_df['_birlestirme_anahtari'].isin(saklama_anahtarlari)].copy()
                son_df_listesi.append(saklanacak_eski_kayitlar)

            # Birleştir
            if son_df_listesi:
                son_df = pd.concat(son_df_listesi, ignore_index=True)
            else:
                son_df = yeni_df.copy()

            # Anahtar sütununu kaldır
            son_df = son_df.drop(columns=['_birlestirme_anahtari'])

            # Sırala
            son_df = son_df.sort_values(by='Servis Bakım ID', ascending=False)

            return son_df

        except Exception as e:
            logger.error(f"Veri birleştirme hatası: {e}")
            return yeni_df.sort_values(by='Servis Bakım ID', ascending=False)

    def run_update(self) -> None:
        """Ana güncelleme işlemi"""
        try:
            # Excel'den yeni veriyi oku
            yeni_veri = self.excel_verisi_oku()

            # Google Sheets'den mevcut veriyi al
            mevcut_veri = self.sheets_manager.mevcut_veriyi_al()

            # Verileri birleştir
            son_veri = self.verileri_birlestir(yeni_veri, mevcut_veri)

            # Google Sheets'i güncelle
            if not son_veri.empty:
                self.sheets_manager.ssh_sayfasini_guncelle(son_veri)

        except Exception as e:
            logger.error(f"SSH güncellemesi başarısız: {e}")
            raise

# ============================================================================
# MAIN
# ============================================================================

def run_ssh_update() -> None:
    """Ana fonksiyon - Sessiz mod (log dosyasına yazar)"""
    try:
        # Config oluştur (Service Account otomatik başlar)
        config = SshConfig()

        # Manager oluştur
        manager = SshManager(config)

        # Güncelleme çalıştır
        manager.run_update()

    except Exception as e:
        logger.error(f"Uygulama hatası: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_ssh_update()
