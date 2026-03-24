import sys
from datetime import datetime, date
from decimal import Decimal
import calendar

# Kendi modüllerimiz
import storage
import calculator
from api_client import PrimApiClient

def get_quarter_dates(year, quarter):
    """
    Yıl ve çeyrek bilgisine göre başlangıç ve bitiş tarihlerini döndürür.
    """
    if quarter == 1:
        return date(year, 1, 1), date(year, 3, 31)
    elif quarter == 2:
        return date(year, 4, 1), date(year, 6, 30)
    elif quarter == 3:
        return date(year, 7, 1), date(year, 9, 30)
    elif quarter == 4:
        return date(year, 10, 1), date(year, 12, 31)
    return None, None

def get_month_name(month_num):
    return calendar.month_name[month_num]

def main():
    print("=== Doğtaş Bayi Ticari Prim Hesaplama Sistemi (V2) ===")
    
    # 1. Dönem Seçimi
    try:
        year = int(input("Yıl giriniz (Örn: 2026): "))
        quarter = int(input("Çeyrek Dönem giriniz (1-4): "))
    except ValueError:
        print("Hatalı giriş! Lütfen sayı giriniz.")
        return

    start_date, end_date = get_quarter_dates(year, quarter)
    if not start_date:
        print("Geçersiz çeyrek dönem!")
        return

    print(f"\nSeçilen Dönem: {year} Q{quarter} ({start_date} - {end_date})")

    # 2. Hedef Girişi (Önce kayıtlıları dene, yoksa sor)
    targets = storage.load_targets()
    
    # Bu dönem için hedef var mı kontrol et
    current_targets = [t for t in targets if t['yil'] == year and t['ceyre_donem'] == f"Q{quarter}"]
    
    if not current_targets:
        print("\n--- Hedef Girişi ---")
        current_targets = []
        months = []
        if quarter == 1: months = [1, 2, 3]
        elif quarter == 2: months = [4, 5, 6]
        elif quarter == 3: months = [7, 8, 9]
        elif quarter == 4: months = [10, 11, 12]
        
        for m in months:
            while True:
                try:
                    val = input(f"{m}. Ay Hedef Tutarı (TL): ")
                    amount = Decimal(val)
                    if amount < 0:
                        print("Negatif değer girilemez.")
                        continue
                    current_targets.append({
                        'yil': year,
                        'ceyre_donem': f"Q{quarter}",
                        'ay_no': m,
                        'hedef_tutar': amount
                    })
                    break
                except ValueError:
                    print("Lütfen geçerli bir sayı giriniz.")
        
        # Yeni hedefleri kaydet (mevcutların üzerine ekle veya güncelle)
        # Basitlik için tüm listeyi yeniden yazıyoruz, pratikte ID kontrolü gerekebilir
        # Mevcut listede bu döneme ait eski kayıt varsa temizle
        targets = [t for t in targets if not (t['yil'] == year and t['ceyre_donem'] == f"Q{quarter}")]
        targets.extend(current_targets)
        storage.save_targets(targets)
    else:
        print("\nKayıtlı hedefler yüklendi.")

    # 3. Veri Çekme
    print("\nAPI'den veriler çekiliyor...")
    client = PrimApiClient()
    # Tarih formatı: DD.MM.YYYY
    api_start = start_date.strftime("%d.%m.%Y")
    api_end = end_date.strftime("%d.%m.%Y")
    
    raw_data = client.fetch_data(api_start, api_end)
    print(f"Toplam {len(raw_data)} kayıt çekildi.")

    # 4. Verileri Aylara Göre Grupla ve Hesapla
    # Gerekli alanlar: 
    # - Sipariş Tutarı (order listesinden toplanacak)
    # - Fatura Tutarı (invoice listesinden toplanacak - burada aynı listede varsayıyoruz)
    # Varsayım: 'netPrice' veya benzeri bir alan sipariş tutarını, 
    # 'purchaseInvoiceAmount' fatura tutarını taşıyor olabilir. 
    # Ancak BekleyenAPI.py'de 'originalPrice', 'orderLineQuantity' var.
    # Sipariş Tutarı = originalPrice * orderLineQuantity
    # Fatura Tutarı = Eğer faturalanmışsa, bu tutarı al. 'purchaseInvoiceDate' dolu ise.
    
    monthly_results = {}
    
    quarter_total_target = Decimal('0')
    quarter_total_order = Decimal('0')
    quarter_total_invoice = Decimal('0')
    
    # Hedefleri sözlüğe çevir kolay erişim için
    target_map = {t['ay_no']: t['hedef_tutar'] for t in current_targets}
    
    # Ayları başlat
    for m in target_map.keys():
        monthly_results[m] = {
            'target': target_map[m],
            'realized_order': Decimal('0'),
            'realized_invoice': Decimal('0')
        }
        quarter_total_target += target_map[m]

    for item in raw_data:
        try:
            # Tarihi parse et (orderDate1: YYYY-MM-DD veya benzeri)
            # BekleyenAPI: orderDate1
            o_date_str = item.get('orderDate1', '')
            if not o_date_str: continue
            
            # Format değişebilir, API dönüşüne bakmak lazım. BekleyenAPI.py'de pd.to_datetime kullanılıyor.
            # Biz basitçe string parsing deneyelim veya dateutil kullanalım.
            # Genelde ISO format gelir: 2025-01-30T...
            try:
                if 'T' in o_date_str:
                    o_date = datetime.strptime(o_date_str.split('T')[0], "%Y-%m-%d")
                else:
                    o_date = datetime.strptime(o_date_str, "%Y-%m-%d")
            except:
                # Fallback: maybe DD.MM.YYYY
                try:
                    o_date = datetime.strptime(o_date_str, "%d.%m.%Y")
                except:
                    continue # Tarih parse edilemedi

            if o_date.month not in monthly_results:
                continue

            # Tutar Hesaplama
            qty = Decimal(str(item.get('orderLineQuantity', '0')))
            price = Decimal(str(item.get('netPrice', '0'))) # netPrice yoksa originalPrice?
            if price == 0:
                price = Decimal(str(item.get('originalPrice', '0'))) 
            
            line_total = qty * price
            
            monthly_results[o_date.month]['realized_order'] += line_total
            
            # Fatura Tutarı (Sadece faturalanmışsa)
            # Fatura tarihi veya no varsa faturalanmış sayalım
            inv_date = item.get('purchaseInvoiceDate', '')
            if inv_date and inv_date != '00000000':
                monthly_results[o_date.month]['realized_invoice'] += line_total
                
        except Exception as e:
            # print(f"Satır hatası: {e}")
            pass

    # 5. Sonuçları Hesapla ve Yazdır
    print("\n--- Aylık Performans Raporu ---")
    print(f"{'Ay':<5} | {'Hedef':<15} | {'Sipariş':<15} | {'HGO(%)':<8} | {'Fatura':<15} | {'Prim Oranı':<10} | {'Prim Tutarı':<15}")
    print("-" * 100)

    total_premium = Decimal('0')

    for m in sorted(monthly_results.keys()):
        res = monthly_results[m]
        target = res['target']
        realized_order = res['realized_order']
        realized_invoice = res['realized_invoice']
        
        calc = calculator.calculate_monthly_premium(realized_order, target, realized_invoice)
        
        quarter_total_order += realized_order
        quarter_total_invoice += realized_invoice
        total_premium += calc['premium_amount']
        
        print(f"{m:<5} | {target:,.2f} TL | {realized_order:,.2f} TL | {calc['hgo']:6.2f} | {realized_invoice:,.2f} TL | %{calc['rate']:<9} | {calc['premium_amount']:,.2f} TL")

    # 6. Çeyrek Sonu Ek Prim
    print("\n--- Çeyrek Sonu Özeti ---")
    extra_calc = calculator.calculate_quarterly_extra_premium(
        quarter_total_order, 
        quarter_total_target, 
        quarter_total_invoice
    )
    
    print(f"Toplam Hedef: {quarter_total_target:,.2f} TL")
    print(f"Toplam Sipariş: {quarter_total_order:,.2f} TL")
    print(f"Toplam Fatura: {quarter_total_invoice:,.2f} TL")
    
    if extra_calc.get('eligible'):
        print(f"Ek Hacim Primi: KAZANILDI (%{extra_calc['rate']})")
        print(f"Ek Prim Tutarı: {extra_calc['premium_amount']:,.2f} TL")
        total_premium += extra_calc['premium_amount']
    else:
        print(f"Ek Hacim Primi: KAZANILMADI ({extra_calc.get('reason')})")

    print(f"\nTOPLAM HAK EDİLEN PRİM: {total_premium:,.2f} TL")

if __name__ == "__main__":
    main()
