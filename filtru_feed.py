#!/usr/bin/env python3
"""
Filtrare feed 2Performant (piese auto) -> feed Google Merchant Center.

Citeste feedul CSV direct din link, in flux (nu il incarca in memorie),
aplica filtrele si scrie fisiere .tsv.gz de max. CHUNK produse.

Moduri:
  python filtru_feed.py descarca   -> salveaza feedul in feed_local.csv (cu reluare la intrerupere)
  python filtru_feed.py antet     -> afiseaza coloanele feedului si primele 3 randuri
  python filtru_feed.py analiza    -> statistici pe brand / categorie / pret (raport_*.csv)
  python filtru_feed.py export_mc  -> feed_marketcompare.csv: produsele filtrate in formatul 2Performant (import marketcompare)
  python filtru_feed.py export     -> scrie feed_google_partN.tsv.gz cu produsele filtrate

Cerinte: Python 3.9+ si `pip install requests`
"""
import csv, gzip, io, json, re, sys
from collections import Counter

import requests

import os
FEED_URL = os.environ.get("FEED_URL") or "https://api.2performant.com/feed/13c3dacc8.csv"

# --------------------------------------------------------------------------
# 1) NUMELE COLOANELOR - ruleaza intai `antet` si corecteaza daca difera
# --------------------------------------------------------------------------
COL = {
    "id":       ["product_id", "id", "Product ID"],
    "title":    ["title", "Title", "product_name", "name"],
    "desc":     ["description", "Description"],
    "price":    ["price", "sale_price", "Sale price"],
    "brand":    ["brand", "Brand"],
    "category": ["category", "Category", "subcategory"],
    "url":      ["url", "product_url", "Product URL"],
    "image":    ["image_urls", "image_url", "image", "Images"],
    "gtin":     ["gtin", "GTIN", "ean"],
    "avail":    ["product_active", "availability", "in_stock", "Availability"],
    "old_price": ["old_price"],
    "other":    ["other_data"],
}

# --------------------------------------------------------------------------
# 2) FILTRE - le ajustezi dupa ce vezi raportul din `analiza`
# --------------------------------------------------------------------------
PRET_MINIM = 200.0          # RON; lotul 1: piese de valoare, comisionul acopera CPC-ul
PRET_MAXIM = 20000.0
# Lotul 1: branduri premium aftermarket / OE (None = toate)
BRANDURI = {"BOSCH","VALEO","SACHS","LUK","TRW","ATE","BREMBO","TEXTAR","KYB","MONROE","SKF","GATES","CONTITECH","INA",
            "MAHLE","HELLA","DENSO","NISSENS","NRF","LEMFÖRDER","FEBI BILSTEIN","MEYLE","ELRING","VICTOR REINZ","MANN-FILTER",
            "DELPHI","MAGNETI MARELLI","GARRETT","BORGWARNER","PIERBURG","OE VOLKSWAGEN","CORTECO","DAYCO","SNR","FAG","ZF",
            "BILSTEIN","ZIMMERMANN","TOPRAN","NGK","HEPU","FERODO","PAGID","SWAG","BEHR","CONTINENTAL","VDO","BERU","SCHAEFFLER"}
# Lotul 1: categorii cu valoare mare si cautari dupa cod (None = toate)
CATEGORII_INCLUSE = ["ambreiaj","volanta","disc frana","etrier","amortizor","planetara","alternator","generator","starter",
                     "radiator","compresor","pompă de apă","pompa de apa","distributie","turbo","intercooler","egr","injector",
                     "far","pompa hidraulica","sonda lambda","debit aer","placute frana","arc spiral","brat, suspensie","rulment roata"]
CERE_GTIN = True            # lotul 1: doar produse cu EAN valid (aprobare mai sigura in Merchant Center)
CATEGORII_EXCLUSE = ["odorizant", "cosmetic", "lavete"]  # cuvinte din categorie de exclus
CHUNK = 500_000             # produse per fisier

GOOGLE_CAT = "Vehicles & Parts > Vehicle Parts & Accessories > Motor Vehicle Parts"


def link_produs(p):
    """CSS direct: linkul duce pe pagina produsului din magazinul promovat.
    Trackingul de afiliere se pune in Google Ads (tracking template / final URL suffix)."""
    return p["url"]


# --------------------------------------------------------------------------
FISIER_LOCAL = "feed_local.csv"


def descarca(url, dest=FISIER_LOCAL, incercari=20):
    """Descarca feedul pe disc; daca se rupe conexiunea, reia de unde a ramas (HTTP Range)."""
    import os, time
    tmp = dest + ".part"
    if os.path.exists(tmp):
        os.remove(tmp)
    for i in range(1, incercari + 1):
        have = os.path.getsize(tmp) if os.path.exists(tmp) else 0
        headers = {"Range": f"bytes={have}-"} if have else {}
        try:
            with requests.get(url, stream=True, timeout=120, headers=headers) as r:
                if have and r.status_code != 206:      # serverul nu suporta reluarea -> o luam de la capat
                    have = 0
                    mode = "wb"
                else:
                    mode = "ab" if have else "wb"
                r.raise_for_status()
                with open(tmp, mode) as f:
                    done = have
                    for chunk in r.iter_content(1024 * 1024):
                        f.write(chunk)
                        done += len(chunk)
                        if done // (50 * 1024 * 1024) != (done - len(chunk)) // (50 * 1024 * 1024):
                            print(f"  ...{done / 1024 / 1024:,.0f} MB descarcati", flush=True)
            os.replace(tmp, dest)
            print(f"Gata: {os.path.getsize(dest) / 1024 / 1024:,.0f} MB salvati in {dest}")
            return
        except (requests.RequestException, OSError) as e:
            print(f"  Conexiune intrerupta ({type(e).__name__}), reiau in 10 sec. (incercarea {i}/{incercari})")
            time.sleep(10)
    sys.exit("Descarcarea a esuat de prea multe ori. Ruleaza din nou `descarca`.")


def stream_rows(url):
    import os
    if os.path.exists(FISIER_LOCAL):
        print(f"Citesc fisierul local {FISIER_LOCAL} (sterge-l sau ruleaza `descarca` pentru date noi)")
        text = open(FISIER_LOCAL, encoding="utf-8-sig", errors="replace", newline="")
    else:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        r.raw.decode_content = True
        text = io.TextIOWrapper(r.raw, encoding="utf-8-sig", errors="replace", newline="")
    csv.field_size_limit(10 * 1024 * 1024)
    sample = text.readline()
    delim = max([",", ";", "\t", "|"], key=sample.count)
    header = next(csv.reader([sample], delimiter=delim))
    reader = csv.DictReader(text, fieldnames=header, delimiter=delim)
    return header, reader


def resolve_columns(header):
    m = {}
    for key, cands in COL.items():
        m[key] = next((c for c in cands if c in header), None)
    lipsa = [k for k in ("id", "title", "price", "url") if not m[k]]
    if lipsa:
        sys.exit(f"Nu gasesc coloanele {lipsa}. Ruleaza `antet` si completeaza dictionarul COL.\nColoane: {header}")
    return m


def norm(row, m):
    g = lambda k: (row.get(m[k]) or "").strip() if m[k] else ""
    def num(s):
        try:
            return float(s.replace(" ", "").replace(",", "."))
        except ValueError:
            return 0.0
    pret, pret_vechi = num(g("price")), num(g("old_price"))
    img = re.split(r"[,|\s]+", g("image"))[0] if g("image") else ""
    mpn = ""
    if g("other"):
        try:
            mpn = str(json.loads(g("other")).get("mpn", "")).strip()
        except (ValueError, AttributeError):
            mm = re.search(r'"mpn"\s*:\s*"([^"]+)"', g("other"))
            mpn = mm.group(1).strip() if mm else ""
    gtin = re.sub(r"\D", "", g("gtin"))
    return {"id": g("id"), "title": g("title"), "desc": g("desc"), "price": pret,
            "old_price": pret_vechi, "brand": g("brand").upper(), "category": g("category"),
            "url": g("url"), "image": img, "gtin": gtin if len(gtin) in (8, 12, 13, 14) else "",
            "avail": g("avail").lower(), "mpn": mpn}


def e_disponibil(p):
    return p["avail"] in ("", "1", "true", "yes", "in stock", "in_stock", "da")


def extrage_mpn(title, brand):
    """'senzor ... TEXTAR 98058301' -> '98058301' (tot ce urmeaza dupa brand in titlu)."""
    if brand:
        i = title.upper().rfind(brand)
        if i >= 0:
            rest = title[i + len(brand):].strip(" -,")
            if rest and re.search(r"\d", rest):
                return rest
    m = re.search(r"([A-Z0-9][A-Z0-9.\-/ ]{3,})$", title.strip())
    return m.group(1).strip() if m and re.search(r"\d", m.group(1)) else ""


def get_mpn(p):
    return p["mpn"] or extrage_mpn(p["title"], p["brand"])


def curata_desc(d):
    d = re.sub(r"<[^>]+>", " ", d)
    d = re.sub(r"([.!?;:])([A-ZĂÂÎȘȚ])", r"\1 \2", d)          # "VAG.Retine" -> "VAG. Retine"
    d = re.sub(r"([a-zăâîșț]{2})([A-ZĂÂÎȘȚ][a-zăâîșț])", r"\1. \2", d)  # "prafSchimb" -> "praf. Schimb"
    return re.sub(r"\s+", " ", d).strip()


def comprima(cod):
    """'CUK 2939' -> 'CUK2939', '81.30301-0001' -> '81303010001'"""
    return re.sub(r"[\s.\-/]", "", cod).upper()


def variante_cod(cod):
    v = [cod.strip(), comprima(cod)]
    return list(dict.fromkeys(x for x in v if x))


OE_RE = re.compile(
    r"(?:numere\s+OE(?:\s+de\s+referin\w+)?|cod(?:uri)?\s+OE|nr\.?\s*OE|OE\s+number\w*|OEM|OE)\s*[:\-]\s*"
    r"(.+?)(?=(?:\.\s+[A-ZĂÂÎȘȚ])|\n|$)", re.I)


def extrage_oe(desc):
    """Extrage coduri OE din descriere: 'Numere OE de referinta: DAF 1307569, MAN 81303010001'
    -> ['DAF 1307569', 'MAN 81303010001']. Pastreaza doar elemente care contin cifre."""
    if not desc:
        return []
    text = re.sub(r"<[^>]+>", "\n", desc)
    out = []
    for m in OE_RE.finditer(text):
        for item in re.split(r"[,;|]|\s+și\s+|\s+si\s+", m.group(1)):
            item = re.sub(r"\s+", " ", item).strip(" .")
            item = re.sub(r"\s+la alegere.*$", "", item, flags=re.I)
            item = re.sub(r"^(printre care|dintre care|ex\.?|de exemplu)\s+", "", item, flags=re.I).strip(" .")
            if re.search(r"\d", item) and 3 <= len(item) <= 40:
                out.append(item)
    return list(dict.fromkeys(out))[:30]


def construieste_titlu(p, mpn, oes):
    t = curata_titlu(p["title"])
    if mpn and comprima(mpn) not in comprima(t):             # codul producatorului garantat in titlu
        t = f"{t} {p['brand']} {mpn}"
    for i, oe in enumerate(oes[:2]):                          # max 2 coduri OE in titlu
        cand = f"{t}{' - compatibil OE ' if i == 0 else ', '}{oe}"
        if len(cand) > 150:
            break
        t = cand
    return t[:150]


def construieste_desc(p, titlu, mpn, oes):
    baza = curata_desc(p["desc"]) or f"{titlu}. Verifica compatibilitatea cu vehiculul tau inainte de comanda."
    if baza and baza[-1] not in ".!?":
        baza += "."
    extra = []
    if mpn:
        extra.append(f"Cod producator {p['brand']}: {', '.join(variante_cod(mpn))}.")
    if oes:
        toate = []
        for oe in oes:
            cod = oe.split(" ", 1)[-1] if re.match(r"^[A-Za-z\-]+ ", oe) else oe
            toate += [oe] + [x for x in variante_cod(cod) if x != oe]
        extra.append("Coduri OE echivalente: " + ", ".join(dict.fromkeys(toate)) + ".")
    return (baza + " " + " ".join(extra)).strip()[:5000]


def construieste_detalii(p, mpn, oes):
    d = []
    if mpn:
        d.append(f"Identificare:Cod producator:{mpn}")
    for oe in oes[:10]:
        d.append(f"Compatibilitate:Cod OE:{oe}")
    return ",".join(x.replace(",", " ") for x in d)


def curata_titlu(t):
    t = re.sub(r"\s*,\s*", ", ", t)          # "avertizare,uzura" -> "avertizare, uzura"
    t = re.sub(r"\s+", " ", t).strip()
    return t[:1].upper() + t[1:150]


def trece(p):
    if not (p["id"] and p["title"] and p["url"] and p["image"]):
        return False
    if not e_disponibil(p) or not (PRET_MINIM <= p["price"] <= PRET_MAXIM):
        return False
    if BRANDURI and p["brand"] not in BRANDURI:
        return False
    cat = p["category"].lower()
    if CATEGORII_INCLUSE and not any(c in cat for c in CATEGORII_INCLUSE):
        return False
    if any(c in cat for c in CATEGORII_EXCLUSE):
        return False
    if CERE_GTIN and not p["gtin"]:
        return False
    return bool(p["brand"] and (p["gtin"] or get_mpn(p)))


def bucket(pret):
    for lim in (50, 100, 150, 200, 300, 500, 1000, 2000):
        if pret < lim:
            return f"<{lim}"
    return ">=2000"


# --------------------------------------------------------------------------
def cmd_antet():
    header, reader = stream_rows(FEED_URL)
    print("COLOANE:", header)
    for i, row in enumerate(reader):
        print(f"\n--- rand {i+1} ---")
        for k, v in row.items():
            print(f"  {k}: {(v or '')[:120]}")
        if i == 2:
            break


def cmd_analiza():
    header, reader = stream_rows(FEED_URL)
    m = resolve_columns(header)
    total = trec = cu_oe = cu_oe_trec = cu_mpn = cu_gtin = cu_desc = 0
    exemple_oe = []
    brand_all, brand_ok, cat_all, cat_ok, pret_b = Counter(), Counter(), Counter(), Counter(), Counter()
    for row in reader:
        p = norm(row, m)
        total += 1
        cu_mpn += bool(get_mpn(p))
        cu_gtin += bool(p["gtin"])
        cu_desc += bool(p["desc"])
        oes = extrage_oe(p["desc"])
        if oes:
            cu_oe += 1
            cu_oe_trec += trece(p)
            if len(exemple_oe) < 15:
                exemple_oe.append((p["title"], oes))
        brand_all[p["brand"]] += 1
        cat_all[p["category"]] += 1
        pret_b[bucket(p["price"])] += 1
        if trece(p):
            trec += 1
            brand_ok[p["brand"]] += 1
            cat_ok[p["category"]] += 1
        if total % 100_000 == 0:
            print(f"  ...{total:,} randuri citite, {trec:,} trec filtrele", flush=True)

    def scrie(nume, toate, ok):
        with open(nume, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["valoare", "produse_total", "trec_filtrele"])
            for k, n in toate.most_common():
                w.writerow([k, n, ok.get(k, 0)])

    scrie("raport_branduri.csv", brand_all, brand_ok)
    scrie("raport_categorii.csv", cat_all, cat_ok)
    with open("raport_preturi.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["interval_pret", "produse"])
        for k, n in sorted(pret_b.items(), key=lambda x: float(re.sub(r"[^\d]", "", x[0]))):
            w.writerow([k, n])
    with open("raport_exemple_oe.txt", "w", encoding="utf-8") as f:
        for t, o in exemple_oe:
            f.write(f"{t}\n   OE: {o}\n\n")
    pc = lambda x: f"{x:,} ({x / max(total, 1):.0%})"
    print(f"\nTOTAL: {total:,} produse | trec filtrele actuale: {trec:,}")
    print(f"  cu cod producator (MPN): {pc(cu_mpn)}")
    print(f"  cu GTIN valid:           {pc(cu_gtin)}")
    print(f"  cu descriere:            {pc(cu_desc)}")
    print(f"  cu coduri OE in descr.:  {pc(cu_oe)}  | dintre ele trec filtrele: {cu_oe_trec:,}")
    print("Exemple de coduri OE gasite: raport_exemple_oe.txt")
    print("Rapoarte: raport_branduri.csv, raport_categorii.csv, raport_preturi.csv (se deschid in Excel)")


def cmd_export():
    header, reader = stream_rows(FEED_URL)
    m = resolve_columns(header)
    cols = ["id", "title", "description", "link", "image_link", "price", "sale_price", "availability",
            "condition", "brand", "mpn", "gtin", "google_product_category", "product_type",
            "product_detail", "custom_label_0", "custom_label_1"]
    total = n = part = 0
    out = w = None
    for row in reader:
        total += 1
        p = norm(row, m)
        if not trece(p):
            continue
        if n % CHUNK == 0:
            if out:
                out.close()
            part += 1
            out = gzip.open(f"feed_google_part{part}.tsv.gz", "wt", encoding="utf-8", newline="")
            w = csv.writer(out, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            w.writerow(cols)
        mpn = get_mpn(p)
        oes = extrage_oe(p["desc"])
        titlu = construieste_titlu(p, mpn, oes)
        desc = construieste_desc(p, titlu, mpn, oes)
        if p["old_price"] > p["price"]:
            pret, pret_redus = f"{p['old_price']:.2f} RON", f"{p['price']:.2f} RON"
        else:
            pret, pret_redus = f"{p['price']:.2f} RON", ""
        w.writerow([p["id"], titlu, desc[:5000], link_produs(p), p["image"],
                    pret, pret_redus, "in_stock", "new", p["brand"], mpn, p["gtin"],
                    GOOGLE_CAT, re.sub(r"\s*>\s*", " > ", p["category"])[:750],
                    construieste_detalii(p, mpn, oes), bucket(p["price"]),
                    "are_oe" if oes else "fara_oe"])
        n += 1
        if total % 100_000 == 0:
            print(f"  ...{total:,} citite, {n:,} exportate", flush=True)
    if out:
        out.close()
    print(f"\nGata: {n:,} produse din {total:,}, in {part} fisier(e) feed_google_partN.tsv.gz")


def cmd_export_mc():
    """Feed filtrat in formatul ORIGINAL 2Performant (aceleasi coloane), pentru importul in marketcompare.
    Titlul si descrierea sunt inlocuite cu variantele imbogatite (coduri producator + OE)."""
    header, reader = stream_rows(FEED_URL)
    m = resolve_columns(header)
    total = n = 0
    with open("feed_marketcompare.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        w.writeheader()
        for row in reader:
            total += 1
            p = norm(row, m)
            if not trece(p):
                continue
            mpn = get_mpn(p)
            oes = extrage_oe(p["desc"])
            titlu = construieste_titlu(p, mpn, oes)
            row[m["title"]] = titlu
            if m["desc"]:
                row[m["desc"]] = construieste_desc(p, titlu, mpn, oes)
            w.writerow(row)
            n += 1
            if total % 100_000 == 0:
                print(f"  ...{total:,} citite, {n:,} exportate", flush=True)
    print(f"\nGata: {n:,} produse in feed_marketcompare.csv (format 2Performant)")


if __name__ == "__main__":
    cmds = {"descarca": lambda: descarca(FEED_URL), "antet": cmd_antet,
            "analiza": cmd_analiza, "export": cmd_export, "export_mc": cmd_export_mc}
    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        sys.exit(__doc__)
    cmds[sys.argv[1]]()
