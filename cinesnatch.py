"""CineSnatch v4 — Movies & Series Downloader"""
import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import threading, subprocess, os, sys, json, re, shutil, io, urllib.request, time
from pathlib import Path
import requests
from PIL import Image

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

# ── Colour palette — deep space purple/indigo/pink gradient ──────────────────
BG       = "#0A0A18"   # deepest background — very dark indigo
BG2      = "#0F0F24"   # sidebar bg
CARD     = "#141430"   # card surface
PANEL    = "#1C1C42"   # selected / hover
INPUT    = "#181835"   # input fields
BORDER   = "#252550"   # subtle border
BORDER2  = "#32326A"   # visible border

PURPLE   = "#8B5CF6"   # violet — primary CTA
PURPLE2  = "#7C3AED"   # darker violet
PINK     = "#EC4899"   # hot pink — secondary accent
PINK2    = "#DB2777"   # darker pink
CYAN     = "#22D3EE"   # sky cyan — tertiary
CYAN2    = "#06B6D4"
GREEN    = "#34D399"   # emerald success
YELLOW   = "#FBBF24"   # amber warning
ORANGE   = "#FB923C"   # orange info
RED      = "#F87171"   # soft red error
RED2     = "#DC2626"   # hard stop

WHITE    = "#EEF0FF"   # near-white with blue tint
GRAY     = "#8B8DB8"   # muted text
DIM      = "#45456A"   # very muted

ACCENT_BG = "#0D0B2A"  # logo area

F_H1    = ("Segoe UI", 18, "bold")
F_H2    = ("Segoe UI", 14, "bold")
F_BODY  = ("Segoe UI", 12)
F_SMALL = ("Segoe UI", 10)
F_TINY  = ("Segoe UI", 9)
F_MONO  = ("Consolas", 10)

# Alias so old references don't break
RED_DIM = PURPLE2
ACCENT  = PURPLE

RESOLUTIONS=["1080p","720p","480p","2160p (4K)","360p","Best Available"]
RES_FMT={
    "Best Available": "bestvideo+bestaudio/best",
    "2160p (4K)":     "bestvideo[height=2160]+bestaudio/bestvideo[height=1080]+bestaudio/best",
    "1080p":          "bestvideo[height=1080]+bestaudio/bestvideo[height=720]+bestaudio/best",
    "720p":           "bestvideo[height=720]+bestaudio/bestvideo[height=480]+bestaudio/best",
    "480p":           "bestvideo[height=480]+bestaudio/bestvideo[height=360]+bestaudio/best",
    "360p":           "bestvideo[height=360]+bestaudio/best",
}
SETTINGS_FILE=Path.home()/".cinesnatch4.json"

# ── API Keys — fill these in or use the Settings tab ─────────────────────────
OMDB_KEY_DEF=""        # https://www.omdbapi.com/apikey.aspx
TMDB_API_KEY=""        # https://www.themoviedb.org/settings/api
OPENSUBS_API_KEY=""    # https://www.opensubtitles.com/en/consumers

def load_cfg():
    try: return json.loads(SETTINGS_FILE.read_text())
    except: return {}
def save_cfg(d):
    try: SETTINGS_FILE.write_text(json.dumps(d,indent=2))
    except: pass

# ── OMDB ──────────────────────────────────────────────────────────────────────
def omdb_search(q,key):
    try:
        r=requests.get("https://www.omdbapi.com/",params={"s":q,"apikey":key},timeout=8)
        d=r.json(); return d.get("Search",[]) if d.get("Response")=="True" else []
    except: return []

def omdb_detail(imdb_id,key):
    try:
        r=requests.get("https://www.omdbapi.com/",params={"i":imdb_id,"apikey":key,"plot":"full"},timeout=8)
        return r.json()
    except: return {}

def omdb_season(imdb_id,season,key):
    try:
        r=requests.get("https://www.omdbapi.com/",params={"i":imdb_id,"Season":season,"apikey":key},timeout=8)
        d=r.json(); return d.get("Episodes",[]) if d.get("Response")=="True" else []
    except: return []

# Poster cache: url → PIL Image (raw, resized)
_poster_cache: dict = {}
_poster_disk_cache = Path.home() / ".cinesnatch4_posters"
_poster_disk_cache.mkdir(exist_ok=True)

import concurrent.futures as _cf
_poster_pool = _cf.ThreadPoolExecutor(max_workers=4)

def _download_pil(url, size):
    """Download and return a resized PIL Image. Pure background-safe."""
    try:
        # Check disk cache first
        import hashlib
        key = hashlib.md5(f"{url}{size}".encode()).hexdigest()
        disk_path = _poster_disk_cache / f"{key}.jpg"
        if disk_path.exists():
            img = Image.open(disk_path).convert("RGB")
            return img.resize(size, Image.LANCZOS)
        data = requests.get(url, timeout=7, headers=HEADERS).content
        if len(data) < 500: return None
        img = Image.open(io.BytesIO(data)).convert("RGB")
        img = img.resize(size, Image.LANCZOS)
        # Save to disk cache
        try: img.save(disk_path, "JPEG", quality=85)
        except: pass
        return img
    except: return None

def _download_image(url, size):
    """Return CTkImage — must be called from main thread or after PIL is ready."""
    pil = _download_pil(url, size)
    if pil: return ctk.CTkImage(pil, size=size)
    return None

def _download_tk_image(url, size):
    """Return raw PIL Image for deferred PhotoImage creation on main thread."""
    return _download_pil(url, size)

def _tmdb_poster(title, year, size):
    """Try TMDB for poster (free, huge coverage, no key needed via scraping)."""
    try:
        # Use TMDB search API with a free-tier key embedded
        q = requests.utils.quote(title)
        yr = str(year)[:4] if year else ""
        url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={q}&year={yr}"
        r = requests.get(url, headers=HEADERS, timeout=6)
        if r.status_code != 200: return None
        results = r.json().get("results", [])
        if not results: return None
        # Pick best result with poster
        for item in results[:5]:
            pp = item.get("poster_path")
            if pp:
                img_url = f"https://image.tmdb.org/t/p/w185{pp}"
                img = _download_image(img_url, size)
                if img: return img
    except: pass
    return None

def _google_poster(title, year, size):
    """Last resort: Google Images scrape for movie poster."""
    try:
        q = requests.utils.quote(f"{title} {year} movie poster")
        r = requests.get(f"https://www.google.com/search?tbm=isch&q={q}",
                         headers={**HEADERS, "User-Agent":
                             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/120.0.0.0 Safari/537.36"},
                         timeout=8)
        # Pull first direct image URL from results
        urls = re.findall(r'"(https://[^"]+\.(?:jpg|jpeg|png))"[^>]*(?:gstatic|tmdb|imdb|media)', r.text)
        if not urls:
            urls = re.findall(r'https://[^\s"]+\.(?:jpg|jpeg|png)', r.text)
        for u in urls[:4]:
            if any(x in u for x in ["gstatic","tmdb","imdb","media","poster"]):
                img = _download_image(u, size)
                if img: return img
    except: pass
    return None

def fetch_poster(url, size=(90,133), title="", year="", tk=False):
    """Fetch poster. tk=True returns PIL Image (caller creates PhotoImage on main thread)."""
    cache_key = f"{url}|{title}|{size[0]}|{'tk' if tk else 'ctk'}"
    if cache_key in _poster_cache:
        return _poster_cache[cache_key]

    pil = None
    if url and url != "N/A":
        pil = _download_pil(url, size)
    if not pil and title:
        try:
            q = requests.utils.quote(title)
            yr = str(year)[:4] if year else ""
            r2 = requests.get(
                f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={q}&year={yr}",
                headers=HEADERS, timeout=6)
            for item in r2.json().get("results", [])[:5]:
                pp = item.get("poster_path")
                if pp:
                    pil = _download_pil(f"https://image.tmdb.org/t/p/w185{pp}", size)
                    if pil: break
        except: pass

    if pil is None: return None

    result = pil if tk else ctk.CTkImage(pil, size=size)
    _poster_cache[cache_key] = result
    return result

HEADERS={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120","Accept":"application/json,*/*"}

def _fmt_size(b):
    if not b: return "?"
    for u in ["B","KB","MB","GB"]:
        if b<1024: return f"{b:.1f} {u}"
        b/=1024
    return f"{b:.1f} TB"

# ── Source search ──────────────────────────────────────────────────────────────
def torrentio_search(imdb_id,season=None,episode=None):
    results=[]
    try:
        if season is not None and episode is not None:
            url=f"https://torrentio.strem.fun/sort=seeders/stream/series/{imdb_id}:{season}:{episode}.json"
        else:
            url=f"https://torrentio.strem.fun/sort=seeders/stream/movie/{imdb_id}.json"
        r=requests.get(url,headers=HEADERS,timeout=14)
        if r.status_code!=200: return results
        data=r.json()
        ep_tag=f"S{str(season).zfill(2)}E{str(episode).zfill(2)}" if season and episode else ""
        sea_tag=f"S{str(season).zfill(2)}" if season else ""
        for stream in data.get("streams",[]):
            title_raw=stream.get("title","") or ""
            full=( title_raw+" "+( stream.get("name","") or "") ).lower()
            if re.search(r"s\d{2}[\s\-]+s\d{2}",full): continue
            if re.search(r"complete.{0,10}series|all.{0,5}seasons",full): continue
            has_ep=ep_tag and ep_tag.lower() in full
            has_sea=sea_tag and sea_tag.lower() in full
            if not has_ep and not has_sea and (ep_tag or sea_tag): continue
            is_pack=has_sea and not has_ep
            quality="?"
            for q in ["2160p","4k","1080p","720p","480p","360p"]:
                if q in full: quality="2160p" if q=="4k" else q; break
            seeds_m=re.search(r"👤\s*(\d+)",title_raw)
            size_m=re.search(r"💾\s*([\d.]+\s*(?:GB|MB))",title_raw)
            seeds=int(seeds_m.group(1)) if seeds_m else 0
            size=size_m.group(1).strip() if size_m else "?"
            fname=(title_raw.split(chr(10))[0]).strip()
            ih=stream.get("infoHash","")
            if not ih: continue
            magnet=(f"magnet:?xt=urn:btih:{ih}&dn={requests.utils.quote(fname)}"
                    f"&tr=udp://tracker.opentrackr.org:1337&tr=udp://open.stealth.si:80/announce"
                    f"&tr=udp://tracker.openbittorrent.com:6969&tr=udp://exodus.desync.com:6969")
            results.append({"title":f"[PACK] {fname}" if is_pack else fname,
                            "size":size,"seeds":seeds,"quality":quality,
                            "source":"Torrentio","url":magnet,"info_hash":ih,
                            "is_pack":is_pack,"type":"magnet"})
    except: pass
    qr={"2160p":5,"1080p":4,"720p":3,"480p":2,"360p":1,"?":0}
    results.sort(key=lambda x:(not x.get("is_pack",False),qr.get(x["quality"],0),x["seeds"]),reverse=True)
    return results[:12]

def yts_search(query):
    results=[]
    clean_q=re.sub(r"\b(19|20)\d{2}\b","",query).strip()
    for q_try in list({query,clean_q}):
        try:
            r=requests.get("https://yts.mx/api/v2/list_movies.json",
                           params={"query_term":q_try,"limit":8,"sort_by":"seeds"},
                           headers=HEADERS,timeout=10)
            for m in r.json().get("data",{}).get("movies",[]):
                for t in m.get("torrents",[]):
                    q=t.get("quality","?"); ih=t.get("hash","").lower()
                    if not ih: continue
                    magnet=(f"magnet:?xt=urn:btih:{ih}&dn={requests.utils.quote(m.get('title',''))}"
                            f"&tr=udp://tracker.opentrackr.org:1337&tr=udp://open.stealth.si:80/announce")
                    results.append({"title":f"{m.get('title','')} ({m.get('year','')}) [{q} {t.get('type','')}]",
                                    "size":t.get("size","?"),"seeds":t.get("seeds",0),"quality":q,
                                    "source":"YTS","url":magnet,"info_hash":ih,"is_pack":False,"type":"magnet"})
        except: pass
    seen,out=set(),[]
    for r in sorted(results,key=lambda x:x["seeds"],reverse=True):
        if r["info_hash"] not in seen: seen.add(r["info_hash"]); out.append(r)
    return out[:6]

def tpb_search(query,imdb_id=""):
    results=[]
    clean_q=re.sub(r"\b(19|20)\d{2}\b","",query).strip()
    for q_try in list({query,clean_q})[:2]:
        try:
            r=requests.get(f"https://apibay.org/q.php?q={requests.utils.quote(q_try)}",
                           headers=HEADERS,timeout=10)
            qw=[w.lower() for w in re.split(r"\W+",q_try) if len(w)>2]
            for t in r.json():
                if t.get("id","0")=="0": continue
                name=t.get("name","?"); nlow=name.lower()
                if qw and sum(1 for w in qw if w in nlow)<max(1,int(len(qw)*0.35)): continue
                seeds=int(t.get("seeders",0)); size=_fmt_size(int(t.get("size",0)))
                ih=t.get("info_hash","").lower()
                if not ih: continue
                q="?"
                for qn in ["2160p","1080p","720p","480p"]:
                    if qn in nlow: q=qn; break
                magnet=(f"magnet:?xt=urn:btih:{ih}&dn={requests.utils.quote(name)}"
                        f"&tr=udp://tracker.opentrackr.org:1337&tr=udp://open.stealth.si:80/announce")
                results.append({"title":name,"size":size,"seeds":seeds,"quality":q,
                                "source":"TPB","url":magnet,"info_hash":ih,"is_pack":False,"type":"magnet"})
        except: pass
    seen,out=set(),[]
    for r in sorted(results,key=lambda x:x["seeds"],reverse=True):
        if r["info_hash"] not in seen: seen.add(r["info_hash"]); out.append(r)
    return out[:6]

def eztv_search(query, title="", imdb_id=""):
    """EZTV - strict title matching, no cross-contamination."""
    results=[]
    try:
        # Use IMDB ID if available for precision
        params = {"imdb_id": imdb_id.replace("tt","")} if imdb_id else {"keywords": query, "limit": 30}
        r=requests.get("https://eztvx.to/api/get-torrents",
                       params=params, headers=HEADERS, timeout=10)
        data = r.json().get("torrents", [])
        # Build strict word filter from show title only (not episode info)
        base_title = (title or query.split("S0")[0].split("E0")[0]).strip()
        filter_words = [w.lower() for w in re.split(r"\W+", base_title) if len(w) > 2]
        for t in data:
            tname=t.get("title",""); tlow=tname.lower()
            # Must match ALL main title words
            if filter_words and not all(w in tlow for w in filter_words[:min(3,len(filter_words))]): continue
            q="?"
            for qn in ["2160p","1080p","720p","480p"]:
                if qn in tlow: q=qn; break
            ih_m=re.search(r"btih:([a-fA-F0-9]+)",t.get("magnet_url",""),re.I)
            results.append({"title":tname,"size":_fmt_size(int(t.get("size_bytes",0))),
                            "seeds":int(t.get("seeds",0)),"quality":q,"source":"EZTV",
                            "url":t.get("magnet_url",""),
                            "info_hash":ih_m.group(1).lower() if ih_m else "",
                            "is_pack":False,"type":"magnet"})
        results.sort(key=lambda x:x["seeds"],reverse=True)
    except: pass
    return results[:5]

def search_all_sources(query,imdb_id="",season=None,episode=None,is_series=False,title="",year=""):
    all_r=[]; lock=threading.Lock()
    def run(fn,*args):
        try:
            res=fn(*args)
            with lock: all_r.extend(res)
        except: pass
    tasks=[]
    if imdb_id: tasks.append((torrentio_search,imdb_id,season,episode))
    if not is_series: tasks.append((yts_search,query))
    tasks.append((tpb_search,query,imdb_id))
    if is_series: tasks.append((eztv_search,query,title,imdb_id))
    threads=[threading.Thread(target=run,args=(fn,*a),daemon=True) for fn,*a in tasks]
    for t in threads: t.start()
    for t in threads: t.join(timeout=14)
    seen,unique=set(),[]
    for r in all_r:
        k=r.get("info_hash") or re.sub(r"\W","",r["title"].lower())[:35]
        if k not in seen: seen.add(k); unique.append(r)
    qr={"2160p":5,"1080p":4,"720p":3,"480p":2,"360p":1,"?":0}
    unique.sort(key=lambda x:(qr.get(x.get("quality","?"),0),x.get("seeds",0)),reverse=True)
    return unique

# ── AllDebrid ─────────────────────────────────────────────────────────────────
AD_BASE="https://api.alldebrid.com"

def _adh(k): return {"Authorization":f"Bearer {k}","User-Agent":"CineSnatch/4","Content-Type":"application/x-www-form-urlencoded"}

def alldebrid_test_key(apikey):
    try:
        r=requests.get(f"{AD_BASE}/v4/user",headers=_adh(apikey),timeout=10)
        d=r.json()
        if d.get("status")=="success":
            u=d.get("data",{}).get("user",{})
            if not u.get("isPremium",False): return False,f"Account '{u.get('username','?')}' is not premium"
            return True,f"✓ Logged in as {u.get('username','?')} (Premium)"
        return False,d.get("error",{}).get("message","Unknown error")
    except Exception as e: return False,str(e)

def alldebrid_upload_magnet(magnet,apikey):
    try:
        r=requests.post(f"{AD_BASE}/v4/magnet/upload",headers=_adh(apikey),
                        data={"magnets[]":magnet},timeout=15)
        d=r.json()
        if d.get("status")!="success": return None,d.get("error",{}).get("message","Upload failed")
        mags=d.get("data",{}).get("magnets",[])
        return (str(mags[0].get("id","")),""  ) if mags else (None,"No magnet returned")
    except Exception as e: return None,str(e)

def alldebrid_get_status(mag_id,apikey):
    try:
        r=requests.post(f"{AD_BASE}/v4.1/magnet/status",headers=_adh(apikey),
                        data={"id":mag_id},timeout=10)
        d=r.json()
        if d.get("status")=="success":
            mags=d.get("data",{}).get("magnets",{})
            if isinstance(mags,list) and mags: return mags[0]
            if isinstance(mags,dict): return mags
    except: pass
    return {}

def alldebrid_get_files(mag_id,apikey):
    try:
        r=requests.post(f"{AD_BASE}/v4/magnet/files",headers=_adh(apikey),
                        data={"id[]":mag_id},timeout=10)
        d=r.json()
        if d.get("status")=="success":
            mags=d.get("data",{}).get("magnets",[])
            if mags: return mags[0].get("files",[])
    except: pass
    return []

def _find_video_file(files,ep_hint=""):
    vexts={".mkv",".mp4",".avi",".m4v",".mov",".ts"}
    candidates=[]
    def recurse(items,depth=0):
        if depth>6 or not isinstance(items,list): return
        for item in items:
            if not isinstance(item,dict): continue
            fname=(item.get("n","") or item.get("name","") or "")
            fname_low=fname.lower()
            link=item.get("l","") or item.get("link","") or ""
            if not link and isinstance(item.get("e"),str): link=item["e"]
            if link and fname:
                ext=("."+fname.rsplit(".",1)[-1]).lower() if "." in fname else ""
                if ext in vexts:
                    size=item.get("s",0) or 0
                    ep_match=ep_hint.lower() in fname_low if ep_hint else False
                    candidates.append((ep_match,size,link,fname))
            for key in ("e","files","f","children"):
                child=item.get(key)
                if isinstance(child,list): recurse(child,depth+1)
                elif isinstance(child,dict): recurse([child],depth+1)
    recurse(files)
    if not candidates: return None
    if ep_hint:
        ep_m=[c for c in candidates if c[0]]
        if ep_m: return max(ep_m,key=lambda x:x[1])[2]
    return max(candidates,key=lambda x:x[1])[2]

def alldebrid_unlock(link,apikey):
    try:
        r=requests.post(f"{AD_BASE}/v4/link/unlock",headers=_adh(apikey),
                        data={"link":link},timeout=15)
        d=r.json()
        if d.get("status")=="success": return d.get("data",{}).get("link")
    except: pass
    return None

def alldebrid_get_direct_link(magnet,apikey,ep_hint="",on_log=None):
    def log(m):
        if on_log: on_log(m)
    ok,msg=alldebrid_test_key(apikey)
    if not ok: log(f"✗ AllDebrid: {msg}"); return None
    log(f"✓ {msg}")
    log("☁ Uploading to AllDebrid…")
    mag_id,err=alldebrid_upload_magnet(magnet,apikey)
    if not mag_id: log(f"✗ Upload failed: {err}"); return None
    log(f"✓ Uploaded (id={mag_id})")
    log("⏳ Waiting for AllDebrid to process…")
    for attempt in range(60):
        time.sleep(3)
        status=alldebrid_get_status(mag_id,apikey)
        st=status.get("status",""); pct=status.get("downloaded",0)
        log(f"☁ AllDebrid: {st} {pct}% ({attempt*3}s)")
        if st in ("Ready","Seeding"): break
        if st in ("Error","Banned","Fail","Dead"): log(f"✗ Error: {st}"); return None
    else: log("✗ Timed out"); return None
    log("📂 Getting files…")
    files=alldebrid_get_files(mag_id,apikey)
    link=_find_video_file(files,ep_hint)
    if not link: log("✗ No video file found"); return None
    log(f"✓ Found file")
    log("🔓 Unlocking…")
    direct=alldebrid_unlock(link,apikey)
    if direct: log("✓ Direct link ready!"); return direct
    log("✗ Unlock failed"); return None

def open_magnet(url):
    try:
        if sys.platform=="win32": os.startfile(url)
        else: subprocess.Popen(["xdg-open",url])
    except: pass

# ── exe finder ────────────────────────────────────────────────────────────────
def find_exe(name):
    exe_dir = Path(sys.executable).parent
    meipass  = getattr(sys, "_MEIPASS", None)
    checks   = []
    if meipass: checks += [Path(meipass)/(name+".exe"), Path(meipass)/name]
    checks  += [exe_dir/(name+".exe"), exe_dir/name, Path(name+".exe"), Path(name)]
    for p in checks:
        if p.exists(): return str(p)
    return shutil.which(name)

def parse_pct(line):
    m = re.search(r"(\d+\.\d+)%", line)
    return float(m.group(1))/100 if m else None

# ── Subtitle engine ──────────────────────────────────────────────────────────
import zipfile as _zipfile, gzip as _gzip, io as _io2, queue as _queue

def _timeout_call(fn, *args, timeout=12):
    res = [None]
    def _w():
        try: res[0] = fn(*args)
        except: pass
    t = threading.Thread(target=_w, daemon=True)
    t.start(); t.join(timeout)
    return res[0]

# ── Source 1: yt-dlp built-in subtitle downloader ────────────────────────────
# Most reliable — yt-dlp handles OpenSubtitles auth, rate limits etc internally
def _src_ytdlp(query, lang, tmp, ytdlp_exe):
    """Use yt-dlp --write-subs to grab from OpenSubtitles via its internal API."""
    if not ytdlp_exe: return None
    try:
        import subprocess, sys
        cflags = subprocess.CREATE_NO_WINDOW if sys.platform=="win32" else 0
        out_tmpl = str(Path(tmp) / "sub_%(title)s.%(ext)s")
        cmd = [ytdlp_exe,
               "--skip-download",
               "--write-subs", "--write-auto-subs",
               "--sub-lang", lang,
               "--sub-format", "srt/best",
               "--convert-subs", "srt",
               "--output", out_tmpl,
               "--no-playlist",
               "--no-check-certificates",
               f"ytsearch1:{query}"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                           creationflags=cflags)
        # Find any .srt written
        srts = list(Path(tmp).glob("sub_*.srt"))
        if srts:
            return str(srts[0])
    except: pass
    return None

# ── Source 2: OpenSubtitles.com REST ─────────────────────────────────────────
def _src_opensubtitles_com(query, lang, tmp):
    try:
        r = requests.get("https://api.opensubtitles.com/api/v1/subtitles",
            params={"query": query, "languages": lang, "per_page": 5},
            headers={"Api-Key": OPENSUBS_API_KEY,
                     "Content-Type": "application/json",
                     "User-Agent": "CineSnatch v4"},
            timeout=10)
        if r.status_code not in (200, 206): return None
        items = r.json().get("data", [])
        if not items: return None
        for item in items[:5]:
            try:
                fid = item["attributes"]["files"][0]["file_id"]
                r2 = requests.post("https://api.opensubtitles.com/api/v1/download",
                    json={"file_id": fid},
                    headers={"Api-Key": OPENSUBS_API_KEY,
                             "Content-Type": "application/json",
                             "User-Agent": "CineSnatch v4"},
                    timeout=10)
                if r2.status_code not in (200, 206): continue
                link = r2.json().get("link")
                if not link: continue
                raw = requests.get(link, timeout=12).content
                if len(raw) > 200:
                    p = Path(tmp) / f"_osc_{lang}.srt"
                    p.write_bytes(raw); return str(p)
            except: continue
    except: pass
    return None

# ── Source 3: OpenSubtitles.org legacy ───────────────────────────────────────
def _src_opensubtitles_org(query, lang, tmp):
    lmap = {"en":"eng","ar":"ara","fr":"fre","es":"spa","de":"ger",
            "it":"ita","pt":"por","ja":"jpn","ko":"kor","zh":"chi"}
    l = lmap.get(lang, "eng")
    try:
        from urllib.parse import quote
        r = requests.get(
            f"https://rest.opensubtitles.org/search/query-{quote(query)}/sublanguageid-{l}",
            headers={**HEADERS, "X-User-Agent": "TemporaryUserAgent"}, timeout=10)
        if r.status_code != 200: return None
        items = r.json()
        if not items: return None
        best = sorted(items, key=lambda x: int(x.get("SubDownloadsCnt",0)), reverse=True)
        for item in best[:5]:
            try:
                dl = item.get("SubDownloadLink","")
                if not dl: continue
                raw = requests.get(dl, timeout=12).content
                try: raw = _gzip.decompress(raw)
                except: pass
                if len(raw) > 200:
                    p = Path(tmp) / f"_oso_{lang}.srt"
                    p.write_bytes(raw); return str(p)
            except: continue
    except: pass
    return None

# ── Source 4: Podnapisi XML API ───────────────────────────────────────────────
def _src_podnapisi(query, lang, tmp):
    lmap = {"en":"en","ar":"ar","fr":"fr","es":"es","de":"de",
            "it":"it","pt":"pt","ja":"ja","ko":"ko","zh":"zh"}
    lc = lmap.get(lang, "en")
    try:
        from urllib.parse import quote
        r = requests.get(
            f"https://www.podnapisi.net/subtitles/search/old?sXML=1&sL={lc}&sK={quote(query)}",
            headers=HEADERS, timeout=10)
        if r.status_code != 200: return None
        pids = re.findall(r"<pid>(\d+)</pid>", r.text)
        if not pids: return None
        for pid in pids[:4]:
            try:
                resp = requests.get(f"https://www.podnapisi.net/subtitles/{pid}/download",
                                    headers=HEADERS, timeout=12, allow_redirects=True)
                if resp.status_code != 200: continue
                raw = resp.content
                if raw[:2] == b"PK":
                    zf = _zipfile.ZipFile(_io2.BytesIO(raw))
                    srts = [f for f in zf.namelist() if f.lower().endswith(".srt")]
                    if srts: raw = zf.read(sorted(srts)[0])
                if len(raw) > 200:
                    p = Path(tmp) / f"_pdn_{lang}.srt"
                    p.write_bytes(raw); return str(p)
            except: continue
    except: pass
    return None

# ── Source 5: Subdl.com ───────────────────────────────────────────────────────
def _src_subdl(query, lang, tmp):
    lmap = {"en":"EN","ar":"AR","fr":"FR","es":"ES","de":"DE",
            "it":"IT","pt":"PT","ja":"JA","ko":"KO","zh":"ZH"}
    try:
        r = requests.get("https://api.subdl.com/api/v1/subtitles",
            params={"api_key":"free","query":query,
                    "languages":lmap.get(lang,lang.upper()),"subs_per_page":5},
            headers=HEADERS, timeout=10)
        if r.status_code != 200: return None
        subs = r.json().get("subtitles",[])
        if not subs: return None
        for sub in subs[:5]:
            try:
                u = sub.get("url","")
                if not u: continue
                dl = u if u.startswith("http") else "https://dl.subdl.com" + u
                resp = requests.get(dl, headers=HEADERS, timeout=12)
                if resp.status_code != 200: continue
                raw = resp.content
                if raw[:2] == b"PK":
                    zf = _zipfile.ZipFile(_io2.BytesIO(raw))
                    srts = [f for f in zf.namelist() if f.lower().endswith(".srt")]
                    if not srts: continue
                    raw = zf.read(sorted(srts)[0])
                if len(raw) > 200:
                    p = Path(tmp) / f"_sdl_{lang}.srt"
                    p.write_bytes(raw); return str(p)
            except: continue
    except: pass
    return None

# ── Source 6: YifySubtitles (movies) ─────────────────────────────────────────
def _src_yts(query, lang, tmp):
    lmap = {"en":"English","ar":"Arabic","fr":"French","es":"Spanish","de":"German",
            "it":"Italian","pt":"Portuguese","ja":"Japanese","ko":"Korean","zh":"Chinese"}
    lang_name = lmap.get(lang, "English")
    for domain in ["yifysubtitles.ch", "yifysubtitles.me"]:
        try:
            r = requests.get(f"https://{domain}/search?q={requests.utils.quote(query)}",
                             headers=HEADERS, timeout=8)
            links = re.findall(r'href="(/movie-imdb/tt\d+)"', r.text)
            if not links: continue
            r2 = requests.get(f"https://{domain}{links[0]}", headers=HEADERS, timeout=8)
            rows = re.findall(r'href="(/subtitles/[^"]+)"', r2.text)
            if not rows: continue
            for row in rows[:8]:
                try:
                    r3 = requests.get(f"https://{domain}{row}", headers=HEADERS, timeout=8)
                    if lang_name.lower() not in r3.text.lower(): continue
                    dl = re.search(r'href="(https?://[^"]+\.srt)"', r3.text)
                    if not dl: continue
                    raw = requests.get(dl.group(1), headers=HEADERS, timeout=12).content
                    if len(raw) > 200:
                        p = Path(tmp) / f"_yts_{lang}.srt"
                        p.write_bytes(raw); return str(p)
                except: continue
        except: continue
    return None

# ── Source 7: TVsubtitles.net ─────────────────────────────────────────────────
def _src_tvsubtitles(query, lang, tmp):
    try:
        r = requests.get("https://www.tvsubtitles.net/search.php",
                         params={"q": query}, headers=HEADERS, timeout=8)
        shows = re.findall(r'href="(/tvshow-\d+\.html)"', r.text)
        if not shows: return None
        r2 = requests.get(f"https://www.tvsubtitles.net{shows[0]}", headers=HEADERS, timeout=8)
        ep_links = re.findall(r'href="(/episode-\d+\.html)"', r2.text)
        if not ep_links: return None
        for ep_link in ep_links[:5]:
            try:
                r3 = requests.get(f"https://www.tvsubtitles.net{ep_link}", headers=HEADERS, timeout=8)
                dl = re.findall(r'href="(/download-\d+\.html)"', r3.text)
                if not dl: continue
                r4 = requests.get(f"https://www.tvsubtitles.net{dl[0]}", headers=HEADERS, timeout=10)
                raw = r4.content
                if raw[:2] == b"PK":
                    zf = _zipfile.ZipFile(_io2.BytesIO(raw))
                    srts = [f for f in zf.namelist() if f.lower().endswith(".srt")]
                    if srts: raw = zf.read(sorted(srts)[0])
                if len(raw) > 200:
                    p = Path(tmp) / f"_tvs_{lang}.srt"
                    p.write_bytes(raw); return str(p)
            except: continue
    except: pass
    return None


def fetch_subtitle(title, ep_hint, lang_code, out_dir, final_path, ytdlp_exe=None):
    """Race all subtitle sources in parallel. First hit wins. Max 25s total."""
    if not lang_code: return None
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    show = re.sub(r"S\d+E\d+.*", "", title, flags=re.I).strip() or title
    queries = []
    if ep_hint: queries.append(f"{show} {ep_hint}")
    queries.append(title)
    if show != title: queries.append(show)
    queries = list(dict.fromkeys(queries))

    result_q = _queue.Queue()

    def _worker(src, q, **kw):
        try:
            raw = src(q, lang_code, out_dir, **kw)
            if raw and Path(raw).exists():
                result_q.put(raw)
        except: pass

    threads = []

    # yt-dlp first — most reliable — gets its own thread per query
    if ytdlp_exe:
        for q in queries:
            t = threading.Thread(target=_worker,
                                 args=(_src_ytdlp, q),
                                 kwargs={"ytdlp_exe": ytdlp_exe},
                                 daemon=True)
            t.start(); threads.append(t)

    # All HTTP sources race in parallel
    http_sources = [_src_opensubtitles_com, _src_opensubtitles_org,
                    _src_podnapisi, _src_subdl, _src_yts, _src_tvsubtitles]
    for src in http_sources:
        for q in queries:
            t = threading.Thread(target=_worker, args=(src, q), daemon=True)
            t.start(); threads.append(t)

    # Wait up to 25 seconds for first winner
    deadline = time.time() + 25
    while time.time() < deadline:
        try:
            raw = result_q.get(timeout=0.25)
            if raw and Path(raw).exists():
                try:
                    shutil.copy2(raw, final_path)
                    try: Path(raw).unlink(missing_ok=True)
                    except: pass
                    return final_path
                except: pass
        except _queue.Empty: pass
        if all(not t.is_alive() for t in threads): break

    # Drain any late arrivals
    while True:
        try:
            raw = result_q.get_nowait()
            if raw and Path(raw).exists():
                try:
                    shutil.copy2(raw, final_path)
                    return final_path
                except: pass
        except _queue.Empty: break
    return None


import zipfile as _zipfile, io as _io2

SUB_LANGS = {"English":"en","Arabic":"ar","French":"fr","Spanish":"es","German":"de",
             "Italian":"it","Portuguese":"pt","Japanese":"ja","Korean":"ko","Chinese":"zh","None":""}
SUBDL_LANG = {"en":"EN","ar":"AR","fr":"FR","es":"ES","de":"DE",
              "it":"IT","pt":"PT","ja":"JA","ko":"KO","zh":"ZH"}

def fetch_subtitle(title, ep_hint, lang_code, out_dir, final_path,
                   api_key="", imdb_id="", season=None, episode=None):
    """Download subtitle from subdl.com. Uses IMDB ID + season/ep for accuracy."""
    if not lang_code or not api_key: return None
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    lang_code_up = SUBDL_LANG.get(lang_code, lang_code.upper())

    # Clean show name — strip torrent junk like "S01.1080p.BluRay.x265-RARBG"
    show = re.sub(r"[Ss]\d+.*", "", title).strip()           # remove from S01 onwards
    show = re.sub(r"(1080p|720p|480p|2160p|BluRay|WEB|HDTV|x264|x265|RARBG|"
                  r"XviD|DTS|AAC|HEVC|HDR|BrRip|WEBRip|DVDRip|YIFY|YTS).*",
                  "", show, flags=re.I).strip()
    show = show.replace(".", " ").replace("_", " ").strip()
    if not show: show = "The Mentalist"  # absolute fallback

    # Parse season/episode from ep_hint if not passed directly (e.g. "S01E19")
    if ep_hint and (season is None or episode is None):
        m = re.search(r"[Ss](\d+)[Ee](\d+)", ep_hint)
        if m:
            season  = season  or int(m.group(1))
            episode = episode or int(m.group(2))

    ep_code = f"S{str(season).zfill(2)}E{str(episode).zfill(2)}" if season and episode else ep_hint

    def _pick_srt_from_zip(raw_zip, ep_code):
        """From a zip, pick the SRT that matches the episode code best."""
        try:
            zf = _zipfile.ZipFile(_io2.BytesIO(raw_zip))
            srts = [f for f in zf.namelist() if f.lower().endswith(".srt")]
            if not srts: return None
            if ep_code:
                # Prefer the file whose name contains S01E19 / 1x19 etc
                code_lower = ep_code.lower()
                ep_n = str(episode).zfill(2) if episode else ""
                for srt in srts:
                    sname = srt.lower()
                    if code_lower in sname: return zf.read(srt)
                    if ep_n and (f"e{ep_n}" in sname or f"ep{ep_n}" in sname):
                        return zf.read(srt)
            # Fallback: first one
            return zf.read(sorted(srts)[0])
        except: return None

    def _try_download(params):
        try:
            r = requests.get("https://api.subdl.com/api/v1/subtitles",
                params={**params, "api_key": api_key,
                        "languages": lang_code_up, "subs_per_page": 10},
                headers=HEADERS, timeout=10)
            if r.status_code != 200: return None
            subs = r.json().get("subtitles", [])
            for sub in subs:
                try:
                    url_path = sub.get("url", "")
                    if not url_path: continue
                    dl = url_path if url_path.startswith("http") else f"https://dl.subdl.com{url_path}"
                    resp = requests.get(dl, headers=HEADERS, timeout=12)
                    if resp.status_code != 200: continue
                    raw = resp.content
                    # ZIP archive
                    if raw[:2] == b"PK":
                        raw = _pick_srt_from_zip(raw, ep_code)
                        if not raw: continue
                    # RAR archive — can't extract without extra deps, skip to next result
                    elif raw[:4] == b"Rar!": continue
                    if len(raw) > 200:
                        Path(final_path).write_bytes(raw)
                        return final_path
                except: continue
        except: pass
        return None

    # ── Try 1: IMDB ID + season + episode — most accurate ─────────────────────
    if imdb_id and season and episode:
        result = _try_download({
            "imdb_id": imdb_id.replace("tt", ""),
            "season_number": season,
            "episode_number": episode,
            "type": "tv"
        })
        if result: return result

    # ── Try 2: Clean show name + season + episode ─────────────────────────────
    if season and episode:
        result = _try_download({
            "film_name": show,
            "season_number": season,
            "episode_number": episode,
            "type": "tv"
        })
        if result: return result

    # ── Try 3: IMDB ID alone (movie) ──────────────────────────────────────────
    if imdb_id and not season:
        result = _try_download({
            "imdb_id": imdb_id.replace("tt", ""),
            "type": "movie"
        })
        if result: return result

    # ── Try 4: Clean show name only ───────────────────────────────────────────
    result = _try_download({"film_name": show})
    if result: return result

    return None


# ── Subtitle Purifier engine ──────────────────────────────────────────────────
try:
    import pysrt as _pysrt
    HAS_PYSRT = True
except ImportError:
    HAS_PYSRT = False

HAS_BP = True  # built-in custom filter — no external library needed

# ── Hardcoded profanity list — ONLY genuine strong/sexual words ───────────────
# Does NOT censor: god, kill, damn, hell, crap, or anything mild.
_PROFANITY_WORDS = [
    # F-word and variants
    "fuck","fucker","fucked","fucking","fucks","fuckin","f*ck","fck","fuk","fukin",
    "motherfucker","motherfucking","mf",
    # S-word
    "shit","shitting","shits","shitted","shitty","bullshit","sh1t",
    # B-word
    "bitch","bitches","bitching","bitchy","b1tch",
    # C-word
    "cunt","cunts","c*nt",
    # Sexual terms
    "cock","cocks","dick","dicks","pussy","pussies","asshole","assholes",
    "blowjob","blow job","handjob","hand job","cumshot","cum","cumming",
    "jizz","boner","erection","rape","raping","rapist","molest",
    "whore","whores","slut","slutty","skank",
    "wanker","twat","prick",
    # N-word
    "nigger","niggers","nigga","niggas",
]

def _build_profanity_pattern():
    words = sorted(_PROFANITY_WORDS, key=len, reverse=True)
    parts = [r"(?<![a-zA-Z])" + re.escape(w) + r"(?![a-zA-Z])" for w in words]
    return re.compile("|".join(parts), re.IGNORECASE)

_PROFANITY_RE = _build_profanity_pattern()

def purify_srt(srt_path, replacement="beep", extra_words=None):
    """Clean profanity from an SRT file in-place. Returns (cleaned_path, count)."""
    if not HAS_PYSRT:
        return srt_path, 0
    try:
        global _PROFANITY_RE
        pat = _PROFANITY_RE
        if extra_words:
            extra_parts = [r"(?<![a-zA-Z])" + re.escape(w) + r"(?![a-zA-Z])" for w in extra_words]
            pat = re.compile(_PROFANITY_RE.pattern + "|" + "|".join(extra_parts), re.IGNORECASE)
        # Try UTF-8 first, fall back to latin-1 so encoding errors don't silently return 0
        subs = None
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                subs = _pysrt.open(srt_path, encoding=enc)
                break
            except Exception:
                continue
        if subs is None:
            return srt_path, 0
        count = 0
        for sub in subs:
            orig = sub.text
            # Count every individual word match, not just lines
            matches = pat.findall(orig)
            if matches:
                sub.text = pat.sub(replacement, orig)
                count += len(matches)
        subs.save(srt_path, encoding="utf-8")
        return srt_path, count
    except Exception:
        return srt_path, 0


def run_download(job, on_log, on_progress, on_progress2, on_done, on_error, on_sub_done=None):
    ytdlp   = find_exe("yt-dlp")
    ffmpeg  = find_exe("ffmpeg")
    url      = job["url"]
    url_type = job.get("type", "magnet")
    out_dir  = Path(job["out_dir"])
    fmt      = RES_FMT.get(job["resolution"], "bestvideo+bestaudio/best")
    sub_lang = job.get("sub_lang", "")
    ep_hint  = job.get("ep_hint", "")
    ad_key   = job.get("ad_key", "").strip()
    cflags   = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_dir / ".cine_tmp"
    tmp_dir.mkdir(exist_ok=True)

    # ── Torrent/magnet via AllDebrid ──────────────────────────────────────────
    if url_type in ("magnet", "torrent_file") and ad_key:
        if not ytdlp: on_error("yt-dlp.exe not found next to CineSnatch.exe"); return
        on_log("☁ Sending to AllDebrid…"); on_progress(0.02)
        direct = alldebrid_get_direct_link(url, ad_key, ep_hint=ep_hint, on_log=on_log)
        if not direct: on_error("AllDebrid failed. Check your API key in Settings."); return
        on_progress(0.05)
        _do_download(direct, job, ytdlp, ffmpeg, tmp_dir, out_dir, fmt,
                     sub_lang, ep_hint, cflags,
                     on_log, on_progress, on_progress2, on_done, on_error, on_sub_done)
        return

    # ── Torrent without AllDebrid → open in qBittorrent ──────────────────────
    if url_type in ("magnet", "torrent_file"):
        on_log("🧲 Opening in qBittorrent…")
        try:
            if sys.platform == "win32": os.startfile(url)
            else: subprocess.Popen(["xdg-open", url])
            on_progress(1.0); on_done("Opened in qBittorrent")
        except Exception as e:
            on_error(f"Could not open torrent client: {e}")
        return

    # ── Direct HTTP download ──────────────────────────────────────────────────
    if not ytdlp: on_error("yt-dlp.exe not found next to CineSnatch.exe"); return
    _do_download(url, job, ytdlp, ffmpeg, tmp_dir, out_dir, fmt,
                 sub_lang, ep_hint, cflags,
                 on_log, on_progress, on_progress2, on_done, on_error, on_sub_done)


def _do_download(url, job, ytdlp, ffmpeg, tmp_dir, out_dir, fmt,
                 sub_lang, ep_hint, cflags,
                 on_log, on_progress, on_progress2, on_done, on_error, on_sub_done):

    is_direct = url.startswith("http") and not any(x in url for x in
                ["youtube","youtu.be","vidsrc","embed","twitch","dailymotion"])

    video_file = None

    # ── Step 1a: Direct HTTP (AllDebrid CDN) — stream via requests ────────────
    if is_direct:
        on_log("⬇ Connecting…")
        try:
            resp = requests.get(url, stream=True, timeout=60,
                                headers={**HEADERS, "User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "")
            ext_map = {"video/mp4": ".mp4", "video/x-matroska": ".mkv",
                       "video/webm": ".webm", "video/x-msvideo": ".avi",
                       "video/quicktime": ".mov", "video/MP2T": ".ts"}
            ext = ext_map.get(ct.split(";")[0].strip(), "")
            if not ext:
                url_path = url.split("?")[0]
                for e in [".mp4", ".mkv", ".avi", ".mov", ".ts", ".webm"]:
                    if url_path.lower().endswith(e): ext = e; break
            if not ext: ext = ".mp4"
            total = int(resp.headers.get("Content-Length", 0))
            tmp_file = tmp_dir / f"download{ext}"
            downloaded = 0
            pause_event = job.get("pause_event")
            with open(tmp_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 512):
                    # Honour pause — block here until resumed
                    if pause_event:
                        while pause_event.is_set():
                            time.sleep(0.2)
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            p = downloaded / total
                            on_progress(p)
                            mb = downloaded / 1024 / 1024
                            total_mb = total / 1024 / 1024
                            on_log(f"⬇ {p*100:.1f}%  {mb:.0f}/{total_mb:.0f} MB")
            if downloaded == 0:
                on_error("Download returned 0 bytes — link may have expired. Try again."); return
            video_file = str(tmp_file)
            on_log(f"⬇ Complete: {downloaded/1024/1024:.1f} MB")
        except Exception as e:
            on_error(f"Download failed: {e}"); return

    # ── Step 1b: Non-direct — use yt-dlp ─────────────────────────────────────
    else:
        template = str(tmp_dir / "%(title)s.%(ext)s")
        on_log("⬇ Downloading…")
        cmd = [ytdlp, "--output", template, "--no-playlist", "--newline", "--progress",
               "--no-check-certificates", "--concurrent-fragments", "16",
               "--buffer-size", "16K", "--http-chunk-size", "10M",
               "--retries", "10", "--fragment-retries", "10",
               "--format", fmt, "--merge-output-format", "mkv", url]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, bufsize=1, creationflags=cflags)
        job["proc"] = proc
        last_err = ""

        for line in proc.stdout:
            line = line.strip()
            p = parse_pct(line)
            if p is not None:
                on_progress(p)
                m = re.search(r"at\s+([\d.]+\w+/s)", line)
                on_log(f"⬇ {p*100:.1f}%  {m.group(1) if m else ''}")
            if "Destination:" in line:
                m = re.search(r"Destination:\s*(.+)", line)
                if m: video_file = m.group(1).strip()
            if "[download]" in line and "Destination:" not in line and "%" not in line:
                on_log(f"ℹ {line[:100]}")
            if "ERROR" in line:
                last_err = line
                on_log(f"⚠ {line[:100]}")

        proc.wait()
        if proc.returncode != 0:
            on_error(last_err[:140] if last_err else "Download failed."); return

        if not video_file or not Path(video_file).exists():
            vexts = {".mp4", ".mkv", ".avi", ".m4v", ".mov", ".ts", ".webm"}
            cands = sorted(tmp_dir.glob("*.*"), key=lambda f: f.stat().st_mtime, reverse=True)
            for v in cands:
                if v.suffix.lower() in vexts: video_file = str(v); break

    if not video_file or not Path(video_file).exists():
        on_error("Downloaded file not found.")
        return

    on_progress(1.0)

    # ── Step 2: Save file ────────────────────────────────────────────────────
    on_log(f"💾 Found: {Path(video_file).name}")
    # Build clean name: "The Mentalist S01E02" or "Inception (2010)"
    # show_name is always the clean OMDB title, year is stored separately
    show_name = job.get("show_name","").strip()
    ep_hint2  = job.get("ep_hint","")
    year      = job.get("year","").strip()
    # Strip any accidental torrent junk that crept into show_name
    clean = re.sub(r"\b(1080p|720p|480p|2160p|BluRay|WEB|HDTV|x264|x265|"
                   r"RARBG|XviD|DTS|AAC|HEVC|HDR|WEBRip|DVDRip|YIFY|YTS)\b.*",
                   "", show_name, flags=re.I).strip()
    clean = re.sub(r"\(\d{4}\).*", "", clean).strip()
    clean = re.sub(r"[Ss]\d{2}[Ee]\d{2}.*", "", clean).strip()
    clean = clean.replace(".", " ").replace("_", " ").strip()
    clean = re.sub(r"\s+", " ", clean).strip()
    if not clean: clean = "Download"
    if ep_hint2:
        safe = f"{clean} {ep_hint2}"
    elif year:
        safe = f"{clean} ({year})"
    else:
        safe = clean
    # Final sanitize for Windows
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', safe).strip()
    safe = re.sub(r'\s+', ' ', safe)[:80].strip()
    if not safe: safe = "download"
    orig_ext  = Path(video_file).suffix.lower() or ".mp4"
    final     = str(out_dir / (safe + orig_ext))
    on_log(f"💾 Saving to: {Path(final).name}")
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        if Path(final).exists():
            Path(final).unlink()
        shutil.move(str(video_file), final)
        if not Path(final).exists():
            # title had bad chars Windows rejected — fall back to raw stem name
            fallback = str(out_dir / ("video_" + Path(video_file).stem[-40:] + orig_ext))
            shutil.move(str(video_file), fallback)
            if Path(fallback).exists():
                final = fallback
            else:
                on_error(f"Save failed — file vanished after move. Check: {out_dir}"); return
    except Exception as e:
        on_error(f"Save failed: {e}"); return

    try: shutil.rmtree(tmp_dir, ignore_errors=True)
    except: pass

    on_progress(1.0)
    on_log(f"✓ Saved: {Path(final).name}")

    # ── Step 4: Fire completion, then fetch subs in background ───────────────
    on_done(final)

    subdl_key = job.get("subdl_key", "").strip()
    if sub_lang and subdl_key and on_sub_done:
        title_hint  = job.get("title", "")
        show_hint   = job.get("show_name", title_hint)   # clean show name if set
        ep_code_sfx = f".{job['ep_hint']}" if job.get("ep_hint") else ""
        safe_name   = re.sub(r'[<>:"/\\|?*]', '', show_hint)[:60]
        sub_save_dir = Path(job.get("sub_dir", str(out_dir)))
        sub_save_dir.mkdir(parents=True, exist_ok=True)
        sub_final   = str(sub_save_dir / (safe_name + ep_code_sfx + f".{sub_lang}.srt"))

        def _fetch_subs_bg():
            on_log(f"💬 Fetching subtitles for {show_hint}{ep_code_sfx}…")
            on_progress2(0.3)
            found = fetch_subtitle(show_hint, ep_hint, sub_lang,
                                   out_dir, sub_final, api_key=subdl_key,
                                   imdb_id=job.get("imdb_id",""),
                                   season=job.get("season"),
                                   episode=job.get("episode"))
            if found:
                # Read purify setting LIVE from config file (not from job snapshot)
                live_cfg   = load_cfg()
                purify     = live_cfg.get("purify_subs", False)
                replacement = live_cfg.get("purify_word", "beep").strip() or "beep"
                if purify and HAS_PYSRT and HAS_BP:
                    on_log("🧹 Purifying subtitles…")
                    _, count = purify_srt(found, replacement=replacement)
                    msg = f"✓ Subtitle saved & purified — {count} word{'s' if count!=1 else ''} replaced with \"{replacement}\": {Path(found).name}"
                    on_log(msg)
                else:
                    on_log(f"✓ Subtitle saved: {Path(found).name}")
                on_progress2(1.0)
                on_sub_done(found)
            else:
                on_progress2(0)
                on_log("⚠ Subtitle not found on subdl.com")

        threading.Thread(target=_fetch_subs_bg, daemon=True).start()



# ══════════════════════════════════════════════════════════════════════════
# APP
# ══════════════════════════════════════════════════════════════════════════
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("CineSnatch")
        self.geometry("1300x840")
        self.minsize(1000, 700)
        self.configure(fg_color=BG)
        try: self.tk.call("tk", "scaling", 1.0)
        except: pass
        self._set_icon()
        self._cfg          = load_cfg()
        self._jobs         = []
        self._cur_det      = {}
        self._render_token = 0
        self._res_var  = ctk.StringVar(value=self._cfg.get("def_res","1080p"))
        self._lang_var = ctk.StringVar(value=self._cfg.get("sub_lang_name","English"))
        self._build()

    def _set_icon(self):
        for name in ["icon.ico","icon.png"]:
            p = Path(sys.executable).parent/"assets"/name
            if not p.exists():
                meipass = getattr(sys,"_MEIPASS",None)
                if meipass: p = Path(meipass)/"assets"/name
            if not p.exists(): continue
            try:
                if name.endswith(".ico"): self.iconbitmap(str(p))
                else:
                    img = tk.PhotoImage(file=str(p))
                    self.iconphoto(True, img)
                break
            except: pass

    def _build(self):
        self.configure(fg_color=BG2)
        self._mk_sidebar()
        self._mk_content()
        self._nav_to("search")

    def _mk_sidebar(self):
        sb = ctk.CTkFrame(self, width=210, fg_color=BG2, corner_radius=0)
        sb.pack(side="left", fill="y")
        sb.pack_propagate(False)

        # ── Logo ──────────────────────────────────────────────────────────────
        logo = ctk.CTkFrame(sb, fg_color=ACCENT_BG, corner_radius=0, height=72)
        logo.pack(fill="x"); logo.pack_propagate(False)
        # Film icon + name
        inner = ctk.CTkFrame(logo, fg_color="transparent")
        inner.place(relx=.5, rely=.5, anchor="center")
        ctk.CTkLabel(inner, text="🎬", font=("Segoe UI",22)).pack(side="left")
        ctk.CTkLabel(inner, text=" CineSnatch",
                     font=("Segoe UI",17,"bold"), text_color=WHITE).pack(side="left")
        # Gradient accent line under logo
        ctk.CTkFrame(sb, height=2, fg_color=PURPLE, corner_radius=0).pack(fill="x")

        # ── Nav buttons ───────────────────────────────────────────────────────
        ctk.CTkFrame(sb, height=16, fg_color="transparent").pack()  # spacer
        self._nav_btns = {}
        def _draw_search_icon(canvas, col):
            canvas.delete("all")
            canvas.create_oval(4,4,15,15, outline=col, width=2)
            canvas.create_line(13,13,20,20, fill=col, width=2)
        def _draw_download_icon(canvas, col):
            canvas.delete("all")
            canvas.create_line(12,3,12,16, fill=col, width=2)
            canvas.create_line(7,11,12,17,17,11, fill=col, width=2)
            canvas.create_line(4,19,20,19, fill=col, width=2)
        def _draw_settings_icon(canvas, col):
            canvas.delete("all")
            canvas.create_oval(8,8,16,16, outline=col, width=2)
            import math
            for angle in range(0,360,45):
                r1,r2 = 9,12
                a = math.radians(angle)
                x1=12+r1*math.cos(a); y1=12+r1*math.sin(a)
                x2=12+r2*math.cos(a); y2=12+r2*math.sin(a)
                canvas.create_line(x1,y1,x2,y2,fill=col,width=2)
        def _draw_subs_icon(canvas, col):
            canvas.delete("all")
            canvas.create_rectangle(3,6,21,18, outline=col, width=2)
            canvas.create_line(6,10,11,10, fill=col, width=2)
            canvas.create_line(6,14,18,14, fill=col, width=2)
            canvas.create_line(13,10,18,10, fill=col, width=2)
        icon_fns = {
            "search":    _draw_search_icon,
            "download":  _draw_download_icon,
            "subtitles": _draw_subs_icon,
            "settings":  _draw_settings_icon,
        }
        nav_items = [
            ("search",    "Search"),
            ("download",  "Downloads"),
            ("subtitles", "Subtitles"),
            ("settings",  "Settings"),
        ]
        for key, lbl in nav_items:
            row_f = tk.Frame(sb, bg=BG2, cursor="hand2")
            row_f.pack(fill="x", padx=12, pady=2)
            icon_canvas = tk.Canvas(row_f, width=24, height=24,
                                    bg=BG2, highlightthickness=0)
            icon_canvas.pack(side="left", padx=(10,0), pady=12)
            icon_fns[key](icon_canvas, "#6B7FA3")
            lbl_w = tk.Label(row_f, text=lbl, font=("Segoe UI",13,"bold"),
                             fg=GRAY, bg=BG2, anchor="w")
            lbl_w.pack(side="left", padx=(10,0))
            def _on_enter(e, rf=row_f, ic=icon_canvas, fn=icon_fns[key], lw=lbl_w):
                rf.configure(bg=PANEL)
                ic.configure(bg=PANEL); fn(ic, WHITE)
                lw.configure(bg=PANEL, fg=WHITE)
            def _on_leave(e, rf=row_f, ic=icon_canvas, fn=icon_fns[key], lw=lbl_w):
                rf.configure(bg=BG2)
                ic.configure(bg=BG2); fn(ic, "#6B7FA3")
                lw.configure(bg=BG2, fg=GRAY)
            def _on_click(e, k=key): self._nav_to(k)
            for w in [row_f, icon_canvas, lbl_w]:
                w.bind("<Enter>", _on_enter)
                w.bind("<Leave>", _on_leave)
                w.bind("<Button-1>", _on_click)
            self._nav_btns[key] = (row_f, lbl_w, icon_canvas, icon_fns[key])

        # ── Bottom version ────────────────────────────────────────────────────
        bot = ctk.CTkFrame(sb, fg_color="transparent")
        bot.pack(side="bottom", fill="x", padx=14, pady=14)
        ctk.CTkFrame(bot, height=1, fg_color=BORDER).pack(fill="x", pady=(0,8))
        ctk.CTkLabel(bot, text="v4.0  ·  CineSnatch",
                     font=F_TINY, text_color=DIM).pack(anchor="w")

    def _mk_content(self):
        self._area = ctk.CTkFrame(self, fg_color=BG, corner_radius=0)
        self._area.pack(side="left", fill="both", expand=True)
        self._pages = {}
        self._mk_search_page()
        self._mk_dl_page()
        self._mk_subs_page()
        self._mk_settings_page()

    def _nav_to(self, name):
        for k, p in self._pages.items():
            p.pack_forget()
            val = self._nav_btns[k]
            if isinstance(val, tuple):
                rf, lw, ic, fn = val
                rf.configure(bg=BG2); lw.configure(bg=BG2, fg=GRAY)
                ic.configure(bg=BG2); fn(ic, "#6B7FA3")
            else:
                val.configure(fg_color="transparent", text_color=GRAY)
        self._pages[name].pack(fill="both", expand=True)
        val = self._nav_btns[name]
        if isinstance(val, tuple):
            rf, lw, ic, fn = val
            rf.configure(bg=PANEL); lw.configure(bg=PANEL, fg=WHITE)
            ic.configure(bg=PANEL); fn(ic, WHITE)
        else:
            val.configure(fg_color=PANEL, text_color=WHITE)

    # ══════════════════════════════════════════════════════════════════════════
    # SEARCH PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _mk_search_page(self):
        page = ctk.CTkFrame(self._area, fg_color=BG2, corner_radius=0)
        self._pages["search"] = page
        # ── Search header ─────────────────────────────────────────────────────
        top_tk = tk.Frame(page, bg=BG2, height=72)
        top_tk.pack(fill="x"); top_tk.pack_propagate(False)
        tk.Frame(top_tk, width=5, bg=PURPLE).pack(side="left", fill="y")
        tk.Label(top_tk, text="What do you want to watch?",
                 font=("Segoe UI",14,"bold"), fg=WHITE, bg=BG2
                 ).pack(side="left", padx=(14,20))
        tk.Button(top_tk, text="Search",
                  font=("Segoe UI",12,"bold"),
                  bg=BG2, fg=WHITE, bd=0, relief="flat",
                  activebackground=BG2, activeforeground=GRAY,
                  cursor="hand2", command=self._do_search
                  ).pack(side="right", padx=16)
        self._q = ctk.StringVar()
        ent = ctk.CTkEntry(top_tk, textvariable=self._q,
                           placeholder_text="Search movies & series…",
                           font=("Segoe UI",13), fg_color=INPUT,
                           border_width=2, border_color=PURPLE,
                           text_color=WHITE, placeholder_text_color=DIM,
                           corner_radius=22, height=44)
        ent.pack(side="left", fill="x", expand=True, padx=(0,10), pady=14)
        ent.bind("<Return>", lambda e: self._do_search())
        tk.Frame(page, bg=PURPLE, height=2).pack(fill="x")

        # ── Search history dropdown — parented to root so it floats on top ────
        self._hist_popup = tk.Frame(self, bg=CARD,
                                    highlightthickness=1,
                                    highlightbackground=BORDER2)
        self._search_history = self._cfg.get("search_history", [])[:3]

        def _show_history(e=None):
            if not self._search_history: return
            _rebuild_history()
            ex = ent.winfo_rootx() - self.winfo_rootx()
            ey = ent.winfo_rooty() - self.winfo_rooty() + ent.winfo_height() + 2
            ew = ent.winfo_width()
            self._hist_popup.place(x=ex, y=ey, width=ew)
            self._hist_popup.lift()

        def _hide_history(e=None):
            self._hist_popup.place_forget()

        def _rebuild_history():
            for w in self._hist_popup.winfo_children(): w.destroy()
            for i, term in enumerate(self._search_history[:3]):
                if i > 0:
                    tk.Frame(self._hist_popup, bg=BORDER, height=1).pack(fill="x")
                inner = tk.Frame(self._hist_popup, bg=CARD)
                inner.pack(fill="x")
                def _pick(t=term):
                    self._q.set(t)
                    _hide_history()
                    self._do_search()
                lbl = tk.Label(inner, text="🕐  " + term,
                         font=("Segoe UI",11), fg=GRAY, bg=CARD,
                         anchor="w", cursor="hand2")
                lbl.pack(side="left", fill="x", expand=True, padx=(12,0), pady=9)
                def _del(t=term):
                    if t in self._search_history: self._search_history.remove(t)
                    self._cfg["search_history"] = self._search_history
                    threading.Thread(target=save_cfg, args=(dict(self._cfg),), daemon=True).start()
                    if self._search_history: _rebuild_history()
                    else: _hide_history()
                x_btn = tk.Label(inner, text="✕", font=("Segoe UI",10),
                                 fg=DIM, bg=CARD, cursor="hand2", padx=12)
                x_btn.pack(side="right", pady=9)
                x_btn.bind("<Button-1>", lambda e, t=term: _del(t))
                lbl.bind("<Button-1>", lambda e, t=term: _pick(t))
                inner.bind("<Button-1>", lambda e, t=term: _pick(t))
                def _ent(e, r=inner):  r.configure(bg=PANEL); [c.configure(bg=PANEL) for c in r.winfo_children()]
                def _lve(e, r=inner):  r.configure(bg=CARD);  [c.configure(bg=CARD)  for c in r.winfo_children()]
                for w in [inner] + list(inner.winfo_children()):
                    w.bind("<Enter>", _ent); w.bind("<Leave>", _lve)

        ent.bind("<FocusIn>",  _show_history)
        ent.bind("<Button-1>", _show_history)
        ent.bind("<FocusOut>", lambda e: self.after(200, _hide_history))
        self.bind("<Button-1>", lambda e: _hide_history() if e.widget not in [ent, getattr(ent, "_entry", None)] else None)
        # Also bind the inner tk.Entry that CTkEntry wraps
        try: ent._entry.bind("<FocusIn>",  _show_history)
        except: pass
        try: ent._entry.bind("<Button-1>", _show_history)
        except: pass
        self._hide_history = _hide_history

        # ── Body grid (columns configured for split view) ─────────────────────
        self._body = ctk.CTkFrame(page, fg_color="transparent")
        self._body.pack(fill="both", expand=True)
        self._body.columnconfigure(0, weight=50)
        self._body.columnconfigure(1, weight=50)
        self._body.rowconfigure(0, weight=1)

        # ── LEFT panel (results list — only shown in split view) ──────────────
        self._left_panel = ctk.CTkFrame(self._body, fg_color=BG2, corner_radius=0)
        ctk.CTkFrame(self._left_panel, width=1, fg_color=BORDER).pack(side="right", fill="y")

        # Back button row
        back_row = ctk.CTkFrame(self._left_panel, fg_color="transparent")
        back_row.pack(fill="x", padx=10, pady=(8,0))
        ctk.CTkButton(back_row, text="← Back",
                      font=("Segoe UI",10,"bold"),
                      fg_color="transparent", hover_color=PANEL,
                      text_color=GRAY, height=26, width=70, corner_radius=8, anchor="w",
                      command=self._show_browse_area).pack(side="left")

        # Filter buttons — fill width evenly
        frow = ctk.CTkFrame(self._left_panel, fg_color=BORDER, corner_radius=20, height=32)
        frow.pack(fill="x", padx=12, pady=(10,4)); frow.pack_propagate(False)
        self._type = ctk.StringVar(value="All")
        self._filter_btns = {}
        self._all_results = []  # cache of full unfiltered results
        def _set_filter(val):
            self._type.set(val)
            for k2, b2 in self._filter_btns.items():
                b2.configure(fg_color=PURPLE if k2==val else "transparent",
                              text_color=WHITE if k2==val else GRAY)
            # Re-render from cache instantly
            if self._all_results:
                filtered = self._all_results if val == "All" else [
                    r for r in self._all_results if r.get("Type","").lower() == val.lower()]
                self._show_results(filtered)
        for t in ["All","Movie","Series"]:
            b = ctk.CTkButton(frow, text=t, height=26,
                              font=F_TINY, corner_radius=18,
                              fg_color=PURPLE if t=="All" else "transparent",
                              hover_color=PANEL, text_color=WHITE if t=="All" else GRAY,
                              command=lambda v=t: _set_filter(v))
            b.pack(side="left", padx=2, pady=2, fill="x", expand=True)
            self._filter_btns[t] = b

        ctk.CTkLabel(self._left_panel, text="RESULTS",
                     font=("Segoe UI",8,"bold"), text_color=DIM
                     ).pack(anchor="w", padx=14, pady=(2,2))
        self._results_box = ctk.CTkScrollableFrame(self._left_panel, fg_color="transparent",
                                                    scrollbar_button_color=BORDER2)
        self._results_box.pack(fill="both", expand=True, padx=4, pady=(0,4))

        # ── BROWSE AREA (full-width, shown before any title is picked) ────────
        self._browse_area = ctk.CTkFrame(self._body, fg_color=BG, corner_radius=0)
        self._browse_area.grid(row=0, column=0, columnspan=2, sticky="nsew")

        # ── RIGHT panel (detail view — shown in split view) ───────────────────
        self._right = ctk.CTkFrame(self._body, fg_color=BG, corner_radius=0)
        self._det_area = ctk.CTkFrame(self._right, fg_color="transparent")
        self._det_area.pack(fill="both", expand=True)
        self._detail_built = False
        self._split_active = False

        # Build detail panel now (hidden) and load recommended titles after window is fully idle
        self._build_detail_panel()
        self._show_browse_area()
        self.after(1200, self._load_recommended)

    def _show_browse_area(self):
        """Switch to full-width browse mode."""
        try: self._left_panel.grid_forget()
        except: pass
        try: self._right.grid_forget()
        except: pass
        self._browse_area.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self._split_active = False

    def _show_results_only(self):
        """Show results list full-width, no right panel, until a title is picked."""
        try: self._browse_area.grid_forget()
        except: pass
        try: self._right.grid_forget()
        except: pass
        self._left_panel.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self._split_active = False   # not a full split yet

    def _activate_split(self):
        """Switch to full split view: left results list + right detail panel."""
        if getattr(self, "_split_active", False):
            return
        try: self._browse_area.grid_forget()
        except: pass
        self._left_panel.grid(row=0, column=0, sticky="nsew")
        self._right.grid(row=0, column=1, sticky="nsew")
        self._split_active = True

    # ── Browse / recommended grid ─────────────────────────────────────────────
    _RECOMMENDED = [
        "Inception", "Interstellar", "The Dark Knight", "Parasite",
        "Breaking Bad", "Game of Thrones", "Stranger Things", "The Boys",
        "Avengers: Endgame", "Oppenheimer", "Dune", "The Witcher",
        "Peaky Blinders", "Severance", "The Last of Us", "House of the Dragon",
        "Fight Club", "The Shawshank Redemption", "Pulp Fiction", "Gladiator",
        "Better Call Saul", "Succession", "Chernobyl", "Black Mirror", "Arcane",
    ]

    def _load_recommended(self):
        """Load recommended titles — canvas scroll, batch 12+13, append-only."""
        for w in self._browse_area.winfo_children(): w.destroy()

        hdr = tk.Frame(self._browse_area, bg=BG2)
        hdr.pack(fill="x", padx=24, pady=(18,8))
        tk.Label(hdr, text="✨  Recommended for You",
                 font=("Segoe UI",15,"bold"), fg=WHITE, bg=BG2).pack(side="left")
        self._rec_status = tk.Label(hdr, text="Loading…", font=("Segoe UI",9),
                                    fg=GRAY, bg=BG2)
        self._rec_status.pack(side="left", padx=14)

        # Canvas + scrollbar — native tk, no CTk wrapper bugs
        wrap = tk.Frame(self._browse_area, bg=BG2)
        wrap.pack(fill="both", expand=True, padx=12, pady=(0,12))
        vsb = tk.Scrollbar(wrap, orient="vertical", bg=BG2,
                           troughcolor=BG2, activebackground=BORDER2,
                           highlightthickness=0, bd=0)
        vsb.pack(side="right", fill="y")
        canvas = tk.Canvas(wrap, bg=BG2, highlightthickness=0,
                           yscrollcommand=vsb.set)
        canvas.pack(side="left", fill="both", expand=True)
        vsb.config(command=canvas.yview)

        # Inner frame lives inside the canvas
        inner = tk.Frame(canvas, bg=BG2)
        win_id = canvas.create_window((0,0), window=inner, anchor="nw")

        _rjob=[None]; _cjob=[None]
        def _on_inner_resize(e):
            if _rjob[0]: self.after_cancel(_rjob[0])
            _rjob[0]=self.after(60,lambda:canvas.configure(scrollregion=canvas.bbox("all")))
        def _on_canvas_resize(e):
            canvas.itemconfig(win_id, width=e.width)
            if _cjob[0]: self.after_cancel(_cjob[0])
            _cjob[0]=self.after(60,_reflow)
        inner.bind("<Configure>", _on_inner_resize)
        canvas.bind("<Configure>", _on_canvas_resize)

        def _scroll(e):
            canvas.yview_scroll(int(-1*(e.delta/120)), "units")
        canvas.bind("<MouseWheel>", _scroll)
        inner.bind("<MouseWheel>", _scroll)

        self._rec_canvas = canvas
        self._rec_inner  = inner
        self._rec_items  = []
        self._rec_cards  = []
        self._rec_cols   = 0

        def _col_count():
            w = canvas.winfo_width()
            if w < 10: w = 960
            return max(1, w // (CARD_W_PX + 16))

        def _reflow():
            if not self._rec_items: return
            cols = _col_count()
            if cols == self._rec_cols and all(c is not None for c in self._rec_cards):
                return
            self._rec_cols = cols
            for c in self._rec_cards:
                if c: c.grid_forget()
            for idx, card in enumerate(self._rec_cards):
                if card is None: continue
                r, c2 = divmod(idx, cols)
                card.grid(row=r, column=c2, padx=7, pady=6, sticky="nw")

        def _append_cards(items):
            cols = _col_count()
            self._rec_cols = cols
            inner.unbind("<Configure>")
            # Add ONE card then yield — guarantees UI stays responsive between each
            def _add_one(items_left):
                if not items_left: 
                    inner.bind("<Configure>", _on_inner_resize)
                    self.after(80, lambda: canvas.configure(scrollregion=canvas.bbox("all")))
                    return
                item = items_left[0]
                idx = len(self._rec_cards)
                self._rec_items.append(item)
                card = _make_card(inner, item)
                self._rec_cards.append(card)
                r, c2 = divmod(idx, cols)
                card.grid(row=r, column=c2, padx=7, pady=6, sticky="nw")
                # Schedule next card — gives tkinter a full event loop cycle to handle clicks
                self.after(0, lambda: _add_one(items_left[1:]))
            _add_one(list(items))

        POSTER_W, POSTER_H, CARD_W_PX, CARD_H_PX = 158, 218, 176, 290

        def _make_card(parent, item):
            typ = item.get("Type","").lower()
            badge_col = {"movie": PINK, "series": CYAN}.get(typ, PURPLE)

            card = tk.Frame(parent, bg=CARD, cursor="hand2",
                            width=CARD_W_PX, height=CARD_H_PX,
                            highlightthickness=1, highlightbackground=BORDER)
            card.pack_propagate(False)

            # Poster container — fixed size, clips image inside
            poster_frame = tk.Frame(card, bg=INPUT,
                                    width=POSTER_W, height=POSTER_H)
            poster_frame.pack(padx=8, pady=(8,0))
            poster_frame.pack_propagate(False)

            pl = tk.Label(poster_frame, text="🎬", font=("Segoe UI",30),
                          fg=DIM, bg=INPUT, cursor="hand2",
                          width=POSTER_W, height=POSTER_H)
            pl.place(x=0, y=0, width=POSTER_W, height=POSTER_H)

            # Title
            tk.Label(card, text=item.get("Title","?")[:26],
                     font=("Segoe UI",10,"bold"), fg=WHITE, bg=CARD,
                     wraplength=CARD_W_PX-14, justify="left", anchor="w"
                     ).pack(anchor="w", padx=8, pady=(6,0))

            # Year + badge
            row = tk.Frame(card, bg=CARD)
            row.pack(anchor="w", padx=8, pady=(2,0))
            tk.Label(row, text=item.get("Year",""), font=("Segoe UI",8),
                     fg=DIM, bg=CARD).pack(side="left")
            badge = tk.Frame(row, bg=badge_col)
            badge.pack(side="left", padx=(6,0))
            tk.Label(badge, text=typ.upper(), font=("Segoe UI",7,"bold"),
                     fg=BG, bg=badge_col).pack(padx=5, pady=2)

            _ck = f"rec|{item.get('Poster','')}|{item.get('Title','')}|{POSTER_W}x{POSTER_H}"
            def _load_poster(lbl=pl, url=item.get("Poster",""),
                             t=item.get("Title",""), y=item.get("Year",""), ck=_ck):
                if ck in _poster_cache:
                    photo = _poster_cache[ck]
                    def _fast(l=lbl, p=photo):
                        try: l._img=p; l.configure(image=p, text="", bg=CARD)
                        except: pass
                    self.after(0, _fast); return
                from PIL import ImageTk
                pil = fetch_poster(url, size=(POSTER_W, POSTER_H), title=t, year=y, tk=True)
                if not pil: return
                try:
                    photo = ImageTk.PhotoImage(pil)
                    _poster_cache[ck] = photo
                    def _apply(l=lbl, p=photo):
                        try: l._img=p; l.configure(image=p, text="", bg=CARD)
                        except: pass
                    self.after(0, _apply)
                except: pass
            threading.Thread(target=_load_poster, daemon=True).start()

            def _click(e, it=item):
                self._activate_split()
                self._show_results(list(self._rec_items))
                self._det_loading()
                key2 = self._cfg.get("omdb_key", OMDB_KEY_DEF)
                def w():
                    det = omdb_detail(it.get("imdbID",""), key2) or it
                    self.after(0, lambda: self._det_render(det))
                threading.Thread(target=w, daemon=True).start()
            def _ent(e, c=card): c.configure(highlightbackground=PURPLE)
            def _lve(e, c=card): c.configure(highlightbackground=BORDER)
            all_widgets = [card, pl, poster_frame] + list(card.winfo_children()) + list(poster_frame.winfo_children())
            for w2 in all_widgets:
                try:
                    w2.bind("<Button-1>", _click)
                    w2.bind("<Enter>", _ent)
                    w2.bind("<Leave>", _lve)
                except: pass
            return card

        key = self._cfg.get("omdb_key", OMDB_KEY_DEF)
        titles = self._RECOMMENDED

        def fetch_batch(batch, on_done):
            lock2 = threading.Lock()
            remaining = [len(batch)]
            batch_items = [None] * len(batch)
            def fetch_one(i, title):
                try:
                    r = requests.get("https://www.omdbapi.com/",
                                     params={"t": title, "apikey": key}, timeout=6)
                    d = r.json()
                    if d.get("Response") == "True":
                        batch_items[i] = d
                except: pass
                with lock2:
                    remaining[0] -= 1
                    if remaining[0] == 0:
                        self.after(0, lambda: on_done([x for x in batch_items if x]))
            for i, t in enumerate(batch):
                threading.Thread(target=fetch_one, args=(i, t), daemon=True).start()

        def on_first_batch(items):
            _append_cards(items)
            try: self._rec_status.configure(text="")
            except: pass
            self.after(500, lambda: fetch_batch(titles[12:], on_second_batch))

        def on_second_batch(items):
            _append_cards(items)
            try: self._rec_status.configure(text="")
            except: pass

        fetch_batch(titles[:12], on_first_batch)

    def _do_search(self):
        q = self._q.get().strip().title()
        if not q: return
        self._q.set(q)
        # Save to history — case-insensitive dedup
        self._search_history = [h for h in self._search_history if h.lower() != q.lower()]
        self._search_history.insert(0, q)
        self._search_history = self._search_history[:3]
        self._cfg["search_history"] = self._search_history
        threading.Thread(target=save_cfg, args=(dict(self._cfg),), daemon=True).start()
        try: self._hide_history()
        except: pass
        self._show_results_only()
        for w in self._results_box.winfo_children(): w.destroy()
        self._det_empty()
        self._all_results = []
        ctk.CTkLabel(self._results_box, text="Searching…",
                     font=F_SMALL, text_color=GRAY).pack(pady=16)
        key = self._cfg.get("omdb_key", OMDB_KEY_DEF)
        def worker():
            res = omdb_search(q, key)
            self.after(0, lambda: self._on_search_done(res))
        threading.Thread(target=worker, daemon=True).start()

    def _on_search_done(self, results):
        self._all_results = results
        t = self._type.get()
        filtered = results if t == "All" else [
            r for r in results if r.get("Type","").lower() == t.lower()]
        self._show_results(filtered)

    def _show_results(self, results):
        for w in self._results_box.winfo_children(): w.destroy()
        if not results:
            ctk.CTkLabel(self._results_box, text="No results.",
                         font=F_SMALL, text_color=GRAY).pack(pady=16)
            return
        def _batch(i):
            for item in results[i:i+3]: self._result_row(item)
            if i+3 < len(results): self.after(8, lambda: _batch(i+3))
        _batch(0)

    def _result_row(self, item):
        typ        = item.get("Type","").lower()
        badge_col  = {"movie": PINK, "series": CYAN}.get(typ, DIM)
        poster_url = item.get("Poster","")
        CARD_H = 170; POSTER_W = 114

        card = ctk.CTkFrame(self._results_box, fg_color=CARD,
                            corner_radius=10, cursor="hand2",
                            border_width=1, border_color=BORDER, height=CARD_H)
        card.pack(fill="x", pady=5, padx=6)
        card.pack_propagate(False)

        poster_canvas = tk.Canvas(card, width=POSTER_W, height=CARD_H,
                                  bg=INPUT, highlightthickness=0, cursor="hand2")
        poster_canvas.pack(side="left", fill="y", padx=0, pady=0)

        right = ctk.CTkFrame(card, fg_color="transparent")
        right.pack(side="left", fill="both", expand=True)
        ctk.CTkFrame(right, width=3, fg_color=badge_col, corner_radius=2
                     ).pack(side="left", fill="y", padx=(8,0), pady=14)

        inn = ctk.CTkFrame(right, fg_color="transparent")
        inn.pack(side="left", fill="x", expand=True, padx=12, pady=0)
        r1 = ctk.CTkFrame(inn, fg_color="transparent")
        r1.pack(fill="x", pady=(30,4))
        ctk.CTkLabel(r1, text=item.get("Title","?"),
                     font=("Segoe UI",14,"bold"), text_color=WHITE,
                     anchor="w", wraplength=160).pack(side="left")
        type_f = ctk.CTkFrame(r1, fg_color=badge_col, corner_radius=8)
        type_f.pack(side="left", padx=(8,0))
        ctk.CTkLabel(type_f, text=typ.upper(),
                     font=("Segoe UI",8,"bold"), text_color=BG).pack(padx=7, pady=3)
        ctk.CTkLabel(inn, text=item.get("Year",""),
                     font=("Segoe UI",11), text_color=DIM, anchor="w").pack(fill="x")

        _ptitle = item.get("Title","")
        _ck = f"rr|{poster_url}|{_ptitle}|{POSTER_W}x{CARD_H}"

        def load_poster(url=poster_url, cv=poster_canvas,
                        t=_ptitle, y=item.get("Year",""), pw=POSTER_W, ph=CARD_H, ck=_ck):
            if ck in _poster_cache:
                photo = _poster_cache[ck]
                def _fast(c=cv, p=photo):
                    try:
                        if c.winfo_exists():
                            c.delete("all"); c.create_image(0,0,anchor="nw",image=p); c._img=p
                    except: pass
                self.after(0, _fast); return
            pil = fetch_poster(url, size=(pw,ph), title=t, year=y, tk=True)
            if not pil: return
            try:
                from PIL import ImageTk, ImageDraw
                S=2; big=pil.resize((pw*S,ph*S),Image.LANCZOS)
                mask=Image.new("L",big.size,0)
                ImageDraw.Draw(mask).rounded_rectangle(
                    [0,0,big.width-1,big.height-1], radius=12*S, fill=255)
                out=Image.new("RGBA",big.size,(0,0,0,0))
                out.paste(big.convert("RGBA"),mask=mask)
                out=out.resize((pw,ph),Image.LANCZOS)
                photo=ImageTk.PhotoImage(out); _poster_cache[ck]=photo
                def _apply(c=cv, p=photo):
                    try:
                        if c.winfo_exists():
                            c.delete("all"); c.create_image(0,0,anchor="nw",image=p); c._img=p
                    except: pass
                self.after(0, _apply)
            except: pass
        threading.Thread(target=load_poster, daemon=True).start()

        def on_click(e, i=item, c=card):
            self._activate_split()
            for w2 in self._results_box.winfo_children():
                try: w2.configure(fg_color=CARD, border_color=BORDER)
                except: pass
            c.configure(fg_color=PANEL, border_color=PURPLE)
            self._det_loading()
            key2 = self._cfg.get("omdb_key", OMDB_KEY_DEF)
            def w(captured=i):
                det = omdb_detail(captured.get("imdbID",""), key2) or captured
                self.after(0, lambda d=det: self._det_render(d))
            threading.Thread(target=w, daemon=True).start()

        for w in [card, inn, r1, poster_canvas, right] + list(inn.winfo_children()) + list(r1.winfo_children()):
            try: w.bind("<Button-1>", on_click)
            except: pass


    # ══════════════════════════════════════════════════════════════════════════
    # RIGHT PANEL — built ONCE, updated in-place (zero destroy/rebuild = smooth)
    # ══════════════════════════════════════════════════════════════════════════
    def _build_detail_panel(self):
        """Called once at startup. Creates all right-panel frames and caches refs."""
        da = self._det_area

        # ── Hero (poster + meta) ──────────────────────────────────────────────
        self._hero_frame = ctk.CTkFrame(da, fg_color=CARD, corner_radius=0, height=240)
        self._hero_frame.pack(fill="x"); self._hero_frame.pack_propagate(False)
        ctk.CTkFrame(self._hero_frame, width=5, fg_color=PURPLE,
                     corner_radius=0).pack(side="left", fill="y")
        pf = ctk.CTkFrame(self._hero_frame, fg_color="transparent", width=150)
        pf.pack(side="left", fill="y", padx=(16,0), pady=16); pf.pack_propagate(False)
        self._poster_lbl = ctk.CTkLabel(pf, text="", width=130, height=193,
                                         font=("Segoe UI",38), text_color=DIM)
        self._poster_lbl.pack(expand=True)
        hinn = ctk.CTkFrame(self._hero_frame, fg_color="transparent")
        hinn.pack(fill="both", expand=True, padx=16, pady=16)
        # Back button + title on same row
        title_row = ctk.CTkFrame(hinn, fg_color="transparent")
        title_row.pack(fill="x", anchor="w")
        self._back_btn = ctk.CTkButton(title_row, text="←",
                      font=("Segoe UI",14,"bold"),
                      fg_color=PANEL, hover_color=BORDER2,
                      text_color=GRAY, height=28, width=34, corner_radius=8,
                      command=self._show_results_only)
        self._back_btn.pack(side="left", padx=(0,10))
        self._title_lbl = ctk.CTkLabel(title_row, text="", font=("Segoe UI",20,"bold"),
                                          text_color=WHITE, anchor="w",
                                          wraplength=480, justify="left")
        self._title_lbl.pack(side="left", fill="x", expand=True)
        self._tags_row    = ctk.CTkFrame(hinn, fg_color="transparent")
        self._tags_row.pack(anchor="w", pady=(6,0))
        self._genre_lbl   = ctk.CTkLabel(hinn, text="", font=("Segoe UI",10),
                                          text_color=ORANGE)
        self._genre_lbl.pack(anchor="w", pady=(6,2))
        self._plot_lbl    = ctk.CTkLabel(hinn, text="", font=("Segoe UI",11),
                                          text_color=GRAY, wraplength=560, justify="left")
        self._plot_lbl.pack(anchor="w")

        # ── Options bar ───────────────────────────────────────────────────────
        self._obar = ctk.CTkFrame(da, fg_color=CARD, height=54,
                                   corner_radius=0, border_width=0)
        self._obar.pack(fill="x"); self._obar.pack_propagate(False)
        ctk.CTkFrame(self._obar, height=1, fg_color=BORDER).pack(fill="x", side="bottom")
        ctk.CTkLabel(self._obar, text="Resolution", font=F_TINY,
                     text_color=GRAY).pack(side="left", padx=(18,4))
        ctk.CTkOptionMenu(self._obar, values=RESOLUTIONS, variable=self._res_var,
                          width=120, font=F_SMALL, fg_color=INPUT, button_color=BORDER2,
                          dropdown_fg_color=PANEL, text_color=WHITE
                          ).pack(side="left", padx=(0,18), pady=10)
        ctk.CTkLabel(self._obar, text="Subtitles", font=F_TINY,
                     text_color=GRAY).pack(side="left", padx=(0,4))
        ctk.CTkOptionMenu(self._obar, values=list(SUB_LANGS.keys()),
                          variable=self._lang_var, width=120, font=F_SMALL,
                          fg_color=INPUT, button_color=BORDER2,
                          dropdown_fg_color=PANEL, text_color=WHITE
                          ).pack(side="left", pady=10)
        # Movie download button (hidden for series) — packed side=left after dropdowns
        self._movie_dl_btn = ctk.CTkButton(
            self._obar, text="⬇  Download",
            font=("Segoe UI",11,"bold"), fg_color=PURPLE, hover_color=PURPLE2,
            height=36, corner_radius=8, width=160)

        # ── Lower area: series browser OR movie hint ──────────────────────────
        self._lower = ctk.CTkFrame(da, fg_color="transparent")
        self._lower.pack(fill="both", expand=True)

        # Series: side-by-side season list + episode panel (built once, reused)
        self._series_area = ctk.CTkFrame(self._lower, fg_color="transparent")
        # (packed when series selected)

        # Season sidebar — fixed width, no resize
        self._season_sidebar = ctk.CTkFrame(self._series_area, fg_color=BG2,
                                             corner_radius=0, width=190)
        self._season_sidebar.pack(side="left", fill="y"); self._season_sidebar.pack_propagate(False)
        ctk.CTkFrame(self._season_sidebar, width=1, fg_color=BORDER).pack(side="right", fill="y")
        ctk.CTkLabel(self._season_sidebar, text="SEASONS",
                     font=("Segoe UI",8,"bold"), text_color=DIM
                     ).pack(anchor="w", padx=14, pady=(12,4))
        self._season_scroll = ctk.CTkScrollableFrame(self._season_sidebar,
                                                      fg_color="transparent",
                                                      scrollbar_button_color=BORDER2)
        self._season_scroll.pack(fill="both", expand=True, padx=6, pady=(0,6))

        # Episode panel — fills rest
        self._ep_panel = ctk.CTkFrame(self._series_area, fg_color=BG, corner_radius=0)
        self._ep_panel.pack(side="left", fill="both", expand=True)

        # Keep old ref so pack_forget calls don't break
        self._movie_hint = ctk.CTkFrame(self._lower, fg_color="transparent")

        # Empty state
        self._empty_hint = ctk.CTkFrame(da, fg_color="transparent")
        self._empty_hint.pack(fill="both", expand=True)
        ef = ctk.CTkFrame(self._empty_hint, fg_color="transparent")
        ef.place(relx=.5, rely=.45, anchor="center")
        ctk.CTkLabel(ef, text="▶", font=("Segoe UI",52,"bold"), text_color=PURPLE).pack()
        ctk.CTkLabel(ef, text="Select a title", font=F_H2, text_color=DIM).pack(pady=(10,0))

        self._detail_built = True
        self._detail_mode  = "empty"   # "empty" | "movie" | "series"

    def _det_empty(self):
        if not getattr(self,"_detail_built",False): self._build_detail_panel()
        if self._detail_mode == "empty": return
        self._hero_frame.pack_forget()
        self._obar.pack_forget()
        self._lower.pack_forget()
        self._series_area.pack_forget()
        self._movie_hint.pack_forget()
        self._empty_hint.pack(fill="both", expand=True)
        self._detail_mode = "empty"

    def _det_loading(self):
        if not getattr(self,"_detail_built",False): self._build_detail_panel()
        # Show hero if already built, just update title
        try:
            self._title_lbl.configure(text="Loading…", text_color=GRAY)
        except: pass

    def _det_render(self, d):
        self._cur_det = d
        if not getattr(self,"_detail_built",False): self._build_detail_panel()

        is_series = d.get("Type","").lower() == "series"
        try: n_seasons = int(d.get("totalSeasons","0"))
        except: n_seasons = 0

        # ── Update hero content IN-PLACE ──────────────────────────────────────
        self._title_lbl.configure(text=d.get("Title","?"), text_color=WHITE)
        self._genre_lbl.configure(text=d.get("Genre","") if d.get("Genre","") not in ("","N/A") else "")
        self._plot_lbl.configure(text=d.get("Plot",""))

        # Rebuild tags row (cheap — just labels)
        for w in self._tags_row.winfo_children(): w.destroy()
        def tag(txt, tc=GRAY, bc=INPUT):
            f = ctk.CTkFrame(self._tags_row, fg_color=bc, corner_radius=4)
            f.pack(side="left", padx=(0,6))
            ctk.CTkLabel(f, text=txt, font=("Segoe UI",9,"bold"), text_color=tc).pack(padx=7,pady=3)
        tag(d.get("Year","?"))
        if d.get("Rated","") not in ("","N/A"): tag(d["Rated"])
        if d.get("Runtime","") not in ("","N/A"): tag(d["Runtime"])
        r = d.get("imdbRating","")
        if r and r!="N/A": tag(f"★ {r}", YELLOW, "#2A1E00")
        if is_series and n_seasons: tag(f"{n_seasons} Seasons", ORANGE, "#2A1400")

        # Reset poster to placeholder, then fetch async
        self._poster_lbl.configure(image=None, text="")
        def lbp(url=d.get("Poster",""), t=d.get("Title",""), y=d.get("Year","")):
            img = fetch_poster(url, size=(130,193), title=t, year=y)
            if img:
                try: self.after(0, lambda: self._poster_lbl.configure(image=img, text=""))
                except: pass
        threading.Thread(target=lbp, daemon=True).start()

        # ── Show/hide correct layout without destroying ───────────────────────
        self._empty_hint.pack_forget()

        # Show hero + obar (always same)
        self._hero_frame.pack(fill="x")
        self._obar.pack(fill="x")
        self._lower.pack(fill="both", expand=True)

        if is_series:
            self._movie_dl_btn.pack_forget()
            self._series_area.pack(fill="both", expand=True)
            self._movie_hint.pack_forget()
            self._detail_mode = "series"
            self._init_season_sidebar(d, n_seasons)
        else:
            self._series_area.pack_forget()
            self._movie_dl_btn.configure(
                command=lambda: self._find_sources(d, None, None, d.get("Title","")))
            self._movie_dl_btn.pack(side="left", padx=(18,0), pady=9)
            self._movie_hint.pack(fill="both", expand=True)
            self._detail_mode = "movie"

    def _init_season_sidebar(self, d, n_seasons):
        """Rebuild season buttons only if show changed."""
        # Clear old buttons
        for w in self._season_scroll.winfo_children(): w.destroy()
        self._sbtns = {}
        for s in range(1, (n_seasons or 15) + 1):
            b = ctk.CTkButton(self._season_scroll, text=f"  Season {s}",
                              font=("Segoe UI",11,"bold"),
                              fg_color="transparent", hover_color=PANEL,
                              text_color=GRAY, anchor="w", height=38, corner_radius=8,
                              command=lambda sn=s: self._load_season(d, sn))
            b.pack(fill="x", pady=2)
            self._sbtns[s] = b
        # Clear episode panel
        for w in self._ep_panel.winfo_children(): w.destroy()
        ctk.CTkLabel(self._ep_panel, text="← Select a season",
                     font=F_BODY, text_color=DIM
                     ).place(relx=.5, rely=.5, anchor="center")
        self._load_season(d, 1)

    def _load_season(self, d, sn):
        for s, b in self._sbtns.items():
            b.configure(fg_color=PANEL if s==sn else "transparent",
                        text_color=WHITE if s==sn else GRAY)
        for w in self._ep_panel.winfo_children(): w.destroy()
        ctk.CTkLabel(self._ep_panel, text="Loading…",
                     font=F_BODY, text_color=GRAY
                     ).place(relx=.5, rely=.5, anchor="center")
        key = self._cfg.get("omdb_key", OMDB_KEY_DEF)
        def worker():
            eps = omdb_season(d.get("imdbID",""), sn, key)
            self.after(0, lambda: self._show_episodes(d, sn, eps))
        threading.Thread(target=worker, daemon=True).start()

    def _show_episodes(self, d, sn, episodes):
        for w in self._ep_panel.winfo_children(): w.destroy()
        # Header
        hdr = ctk.CTkFrame(self._ep_panel, fg_color=CARD, height=50, corner_radius=0)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        ctk.CTkFrame(hdr, width=4, fg_color=PURPLE, corner_radius=0).pack(side="left", fill="y")
        ctk.CTkLabel(hdr, text=f"  Season {sn}  ·  {len(episodes)} episodes",
                     font=("Segoe UI",13,"bold"), text_color=WHITE
                     ).place(rely=.5, x=20, anchor="w")
        ctk.CTkButton(hdr, text="⬇  All",
                      font=("Segoe UI",10,"bold"), fg_color=PURPLE2, hover_color=PURPLE,
                      height=30, corner_radius=7, width=80,
                      command=lambda: [self._ep_dl(d,sn,ep.get("Episode","?"),ep.get("Title","?"))
                                       for ep in episodes]
                      ).place(relx=.99, rely=.5, anchor="e", x=-12)
        if not episodes:
            ctk.CTkLabel(self._ep_panel, text="No episode data.", font=F_BODY,
                         text_color=DIM).pack(pady=20)
            return
        scroll = ctk.CTkScrollableFrame(self._ep_panel, fg_color="transparent",
                                         scrollbar_button_color=BORDER2)
        scroll.pack(fill="both", expand=True)
        # Async batch render — 5 at a time, 1 frame apart
        ep_list = list(episodes)
        rid = id(scroll)
        def _batch(i, rid=rid):
            try:
                if not scroll.winfo_exists(): return
            except: return
            for ep in ep_list[i:i+5]: self._ep_row(scroll, d, sn, ep)
            if i+5 < len(ep_list): self.after(16, lambda: _batch(i+5, rid))
        _batch(0)

    def _ep_row(self, parent, d, sn, ep):
        ep_n  = ep.get("Episode","?")
        title = ep.get("Title","?")
        rate  = ep.get("imdbRating","")
        date  = ep.get("Released","")
        code  = f"S{str(sn).zfill(2)}E{str(ep_n).zfill(2)}"
        meta  = code
        if rate and rate != "N/A": meta += f"  ★ {rate}"
        if date and date != "N/A": meta += f"  · {date[:7]}"
        card = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=10,
                             border_width=1, border_color=BORDER)
        card.pack(fill="x", pady=3, padx=2)
        ctk.CTkLabel(card, text=code, font=("Segoe UI",9,"bold"),
                     fg_color=PURPLE2, corner_radius=6,
                     text_color=WHITE, width=72, height=24
                     ).pack(side="left", padx=(12,10), pady=12)
        ctk.CTkLabel(card, text=title, font=("Segoe UI",12,"bold"),
                     text_color=WHITE, anchor="w"
                     ).pack(side="left", fill="x", expand=True)
        ctk.CTkLabel(card, text=meta, font=F_TINY, text_color=DIM,
                     anchor="w"
                     ).pack(side="left", padx=(0,10))
        ctk.CTkButton(card, text="⬇",
                       font=("Segoe UI",14), fg_color=PURPLE, hover_color=PURPLE2,
                       width=44, height=32, corner_radius=8,
                       command=lambda t=title, s=sn, e=ep_n: self._ep_dl(d, s, e, t)
                       ).pack(side="right", padx=12, pady=10)

    def _ep_dl(self, d, sn, ep_n, ep_title):
        show    = d.get("Title","")
        ep_code = f"S{str(sn).zfill(2)}E{str(ep_n).zfill(2)}"
        label   = f"{show} {ep_code} {ep_title}"
        self._find_sources(d, sn, ep_n, label,
                           query=f"{show} {ep_code}",
                           is_episode=True, ep_hint=ep_code)


    def _find_sources(self, d, sn, ep_n, label, query=None, is_episode=False, ep_hint=""):
        if query is None: query = f"{d.get('Title','')} {d.get('Year','')}"
        is_series = d.get("Type","").lower()=="series"
        self._src_picker(query, label, is_series or is_episode,
                         imdb_id=d.get("imdbID",""), season=sn, episode=ep_n,
                         title_str=d.get("Title",""), year=d.get("Year",""),
                         ep_hint=ep_hint)

    def _src_picker(self, query, title, is_series=False, imdb_id="",
                    season=None, episode=None, title_str="", year="", ep_hint=""):
        pop = ctk.CTkToplevel(self)
        pop.title("Pick a source")
        pop.geometry("820x600")
        pop.configure(fg_color=BG)
        pop.grab_set(); pop.focus_force()
        hdr = ctk.CTkFrame(pop, fg_color=BG2, height=66, corner_radius=0)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        ctk.CTkFrame(hdr, width=5, fg_color=PURPLE, corner_radius=0).place(x=0,y=0,relheight=1)
        ctk.CTkLabel(hdr, text=f'🔍  {title[:65]}',
                     font=("Segoe UI",13,"bold"), text_color=WHITE
                     ).place(x=20, rely=.5, anchor="w")
        ctk.CTkFrame(pop, height=2, fg_color=PURPLE, corner_radius=0).pack(fill="x")
        ctk.CTkLabel(pop, text="  Searching Torrentio · YTS · EZTV · TPB for best quality sources…",
                     font=F_SMALL, text_color=GRAY).pack(anchor="w", padx=20, pady=(10,4))
        scroll = ctk.CTkScrollableFrame(pop, fg_color="transparent",
                                         scrollbar_button_color=BORDER2)
        scroll.pack(fill="both", expand=True, padx=12, pady=(0,8))
        loading = ctk.CTkLabel(scroll, text="⏳  Searching all sources…",
                               font=F_BODY, text_color=GRAY)
        loading.pack(pady=30)
        bot = ctk.CTkFrame(pop, fg_color=BG2, height=60, corner_radius=0)
        bot.pack(fill="x", side="bottom"); bot.pack_propagate(False)
        ctk.CTkFrame(bot, height=1, fg_color=BORDER).pack(fill="x", side="top")

        sel = {"url": None, "t": title, "type": "", "ep_hint": ep_hint}

        def pick(url, t, src_type, is_pack, card_ref):
            sel["url"]  = url; sel["t"] = t
            sel["type"] = src_type; sel["is_pack"] = is_pack
            for c in scroll.winfo_children():
                try: c.configure(fg_color=CARD)
                except: pass
            try: card_ref.configure(fg_color=PANEL)
            except: pass

        def do_dl():
            if not sel["url"]:
                messagebox.showwarning("Nothing selected","Click a source first.", parent=pop)
                return
            url      = sel["url"]
            url_type = sel["type"]
            ad_key   = self._cfg.get("ad_key","").strip()
            # If magnet and no AllDebrid key → open in qBittorrent
            if url_type == "magnet" and not ad_key:
                try: os.startfile(url)
                except: pass
                pop.destroy(); return
            pop.destroy()
            self._queue_dl(sel["t"], url, url_type=url_type, ep_hint=sel["ep_hint"],
                           imdb_id=imdb_id, season=season, episode=episode,
                           title_str=title_str)

        ctk.CTkButton(bot, text="Cancel", width=90, font=F_BODY,
                      fg_color=PANEL, hover_color=INPUT, text_color=GRAY,
                      height=36, corner_radius=8, command=pop.destroy
                      ).pack(side="left", padx=16, pady=12)

        ad_key_set = bool(self._cfg.get("ad_key","").strip())
        dl_label = "⬇  Download (AllDebrid)" if ad_key_set else "🧲  Open in qBittorrent"
        ctk.CTkButton(bot, text=dl_label, width=220,
                      font=("Segoe UI",12,"bold"),
                      fg_color=PURPLE if ad_key_set else ORANGE,
                      hover_color=PURPLE2, height=36, corner_radius=8,
                      command=do_dl).pack(side="right", padx=16, pady=12)

        def worker():
            results = search_all_sources(query, imdb_id=imdb_id, season=season,
                                          episode=episode, is_series=is_series,
                                          title=title_str, year=year)
            self.after(0, lambda: show_res(results))

        def show_res(results):
            loading.pack_forget()
            if not results:
                ctk.CTkLabel(scroll, text="No sources found. Try a different search.",
                             font=F_BODY, text_color=DIM).pack(pady=20)
                return
            # ── Resolution filtering: prefer chosen res, fallback to best ────
            res_pref = self._res_var.get()
            RES_ORDER = ["2160p","1080p","720p","480p","360p"]
            pref_clean = res_pref.replace(" (4K)","").replace("2160p","2160p")
            if res_pref != "Best Available" and pref_clean in RES_ORDER:
                start_idx = RES_ORDER.index(pref_clean)
                filtered = []
                shown_tier = None
                for tier in RES_ORDER[start_idx:]:
                    tier_results = [r for r in results
                                    if r.get("quality","").lower() == tier.lower()]
                    if tier_results:
                        filtered = tier_results
                        shown_tier = tier
                        break
                if filtered:
                    results = filtered
                    if shown_tier != pref_clean:
                        ctk.CTkLabel(scroll,
                            text=f"⚠  No {res_pref} found — showing best available: {shown_tier}",
                            font=F_TINY, text_color=YELLOW).pack(anchor="w", pady=(0,4))
            ad_note = "✓  AllDebrid key set — clicking Download will auto-download" if ad_key_set else                       "⚠  No AllDebrid key — sources will open in qBittorrent. Add key in Settings for auto-download."
            ctk.CTkLabel(scroll, text=ad_note, font=F_TINY,
                         text_color=GREEN if ad_key_set else YELLOW).pack(anchor="w", pady=(0,8))
            first = None
            for v in results:
                src_color = {"Torrentio":PURPLE,"TPB":ORANGE,"YTS":GREEN,
                             "EZTV":CYAN}.get(v["source"],GRAY)
                seeds = v.get("seeds",0)
                seed_color = GREEN if seeds>50 else (YELLOW if seeds>10 else GRAY)
                is_pack = v.get("is_pack", False)
                card = ctk.CTkFrame(scroll, fg_color=CARD, corner_radius=10, cursor="hand2")
                card.pack(fill="x", pady=4)
                ctk.CTkFrame(card, height=2, fg_color=src_color, corner_radius=0
                             ).pack(fill="x", side="top")
                inn = ctk.CTkFrame(card, fg_color="transparent")
                inn.pack(fill="x", padx=14, pady=10)
                row = ctk.CTkFrame(inn, fg_color="transparent"); row.pack(fill="x")
                bf = ctk.CTkFrame(row, fg_color=src_color, corner_radius=4)
                bf.pack(side="left", padx=(0,8))
                ctk.CTkLabel(bf, text=v["source"], font=("Segoe UI",9,"bold"),
                             text_color=BG).pack(padx=7, pady=3)
                if is_pack:
                    pf = ctk.CTkFrame(row, fg_color="#2A1A00", corner_radius=4)
                    pf.pack(side="left", padx=(0,8))
                    ctk.CTkLabel(pf, text="PACK", font=("Segoe UI",9,"bold"),
                                 text_color=YELLOW).pack(padx=7, pady=3)
                ctk.CTkLabel(row, text=v["title"],
                             font=("Segoe UI",11,"bold"), text_color=WHITE,
                             anchor="w", wraplength=460).pack(side="left")
                meta = ctk.CTkFrame(inn, fg_color="transparent")
                meta.pack(fill="x", pady=(4,0))
                ctk.CTkLabel(meta, text=f"📦 {v['size']}", font=F_TINY, text_color=GRAY
                             ).pack(side="left", padx=(0,16))
                ctk.CTkLabel(meta, text=f"🌱 {seeds} seeds", font=F_TINY,
                             text_color=seed_color).pack(side="left", padx=(0,16))
                q_lbl = v.get("quality","?")
                q_color = {"1080p":GREEN,"2160p":YELLOW,"720p":ORANGE}.get(q_lbl, GRAY)
                ctk.CTkLabel(meta, text=f"📺 {q_lbl}", font=F_TINY,
                             text_color=q_color).pack(side="left", padx=(0,16))
                # Codec warning
                vtitle = v["title"].lower()
                if any(x in vtitle for x in ["x265","hevc","h265","av1"]):
                    ctk.CTkLabel(meta, text="⚠ HEVC→H.264", font=F_TINY,
                                 text_color=YELLOW).pack(side="left", padx=(0,8))
                elif any(x in vtitle for x in ["x264","h264","avc"]):
                    ctk.CTkLabel(meta, text="✓ H.264", font=F_TINY,
                                 text_color=GREEN).pack(side="left", padx=(0,8))
                cb = lambda e, u=v["url"], t=v["title"], st=v["type"], ip=is_pack, c=card: pick(u,t,st,ip,c)
                for w in [card,inn,row,meta]+list(inn.winfo_children())+list(row.winfo_children())+list(meta.winfo_children()):
                    try: w.bind("<Button-1>", cb)
                    except: pass
                if first is None: first = (v, card)
            if first:
                pick(first[0]["url"], first[0]["title"], first[0]["type"], first[0].get("is_pack",False), first[1])
                first[1].configure(fg_color=PANEL)

        threading.Thread(target=worker, daemon=True).start()

    def _queue_dl(self, title, url, url_type="magnet", ep_hint="", imdb_id="", season=None, episode=None, title_str=""):
        out    = self._cfg.get("out_dir", str(Path.home()/"Videos"/"CineSnatch"))
        sub_out = self._cfg.get("sub_dir", str(Path.home()/"Videos"/"CineSnatch"/"Subtitles"))
        sub    = SUB_LANGS.get(self._lang_var.get(), "")
        res    = self._res_var.get()
        ad_key    = self._cfg.get("ad_key","").strip()
        subdl_key = self._cfg.get("subdl_key","").strip()
        job = {"title":title,"url":url,"type":url_type,"resolution":res,
               "sub_lang":sub,"subdl_key":subdl_key,
               "imdb_id":imdb_id,"season":season,"episode":episode,
               "show_name":title_str if title_str else title,
               "year":getattr(self,"_cur_det",{}).get("Year","")[:4],
               "out_dir":out,"sub_dir":sub_out,"ad_key":ad_key,"ep_hint":ep_hint,
               "proc":None,"pause_event":threading.Event()}
        self._jobs.append(job)
        card = self._add_dl_card(job)
        self._nav_to("download")
        def worker():
            run_download(job,
                on_log       = lambda t:   self.after(0, lambda _t=t,   c=card: self._dl_log(c,_t)),
                on_progress  = lambda p:   self.after(0, lambda _p=p,   c=card: self._dl_prog(c,_p)),
                on_progress2 = lambda p:   self.after(0, lambda _p=p,   c=card: self._dl_prog2(c,_p)),
                on_done      = lambda pa:  self.after(0, lambda _pa=pa, c=card: self._dl_done(c,_pa)),
                on_error     = lambda e:   self.after(0, lambda _e=e,   c=card: self._dl_err(c,_e)),
                on_sub_done  = lambda sf:  self.after(0, lambda _sf=sf, c=card: self._dl_sub_done(c,_sf)))
        threading.Thread(target=worker, daemon=True).start()

    # ══════════════════════════════════════════════════════════════════════════
    # DOWNLOADS PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _mk_dl_page(self):
        page = ctk.CTkFrame(self._area, fg_color="transparent")
        self._pages["download"] = page
        top = ctk.CTkFrame(page, fg_color=BG2, height=72, corner_radius=0)
        top.pack(fill="x"); top.pack_propagate(False)
        # Pink accent bar
        ctk.CTkFrame(top, width=5, fg_color=PINK, corner_radius=0).place(x=0, y=0, relheight=1)
        ctk.CTkLabel(top, text="Downloads", font=("Segoe UI",17,"bold"),
                     text_color=WHITE).place(x=22, rely=.5, anchor="w")
        ctk.CTkButton(top, text="📂  Open Folder", width=130, font=F_SMALL,
                      fg_color=PANEL, hover_color=BORDER2, text_color=GRAY,
                      height=34, corner_radius=10, command=self._open_dl_folder
                      ).place(relx=.99, rely=.5, anchor="e", x=-18)
        ctk.CTkFrame(page, height=2, fg_color=PINK, corner_radius=0).pack(fill="x")
        self._dl_scroll = ctk.CTkScrollableFrame(page, fg_color="transparent",
                                                   scrollbar_button_color=BORDER2)
        self._dl_scroll.pack(fill="both", expand=True, padx=28, pady=18)
        self._dl_empty = ctk.CTkLabel(self._dl_scroll,
                                       text="🎬  No downloads yet.\nSearch something and hit Download.",
                                       font=F_BODY, text_color=DIM, justify="center")
        self._dl_empty.pack(pady=80)

    def _add_dl_card(self, job):
        try: self._dl_empty.pack_forget()
        except: pass

        frame = ctk.CTkFrame(self._dl_scroll, fg_color=CARD, corner_radius=14,
                              border_width=1, border_color=BORDER)
        frame.pack(fill="x", pady=8)

        # Top gradient stripe — match card corner radius at top
        stripe = ctk.CTkFrame(frame, fg_color=PURPLE, height=4,
                              corner_radius=14)
        stripe.pack(fill="x", padx=0)

        inn = ctk.CTkFrame(frame, fg_color="transparent")
        inn.pack(fill="x", padx=20, pady=14)

        # ── Row 1: title + buttons ────────────────────────────────────────────
        r1 = ctk.CTkFrame(inn, fg_color="transparent"); r1.pack(fill="x")
        status_lbl = ctk.CTkLabel(r1, text="● Queued", font=("Segoe UI",10,"bold"),
                                   text_color=DIM)
        status_lbl.pack(side="right", padx=(8,0))
        state = {"done": False, "cancelled": False, "paused": False}

        def do_stop():
            if state["done"]: return
            state["cancelled"] = True
            try:
                p = job.get("proc")
                if p: p.kill()
            except: pass
            status_lbl.configure(text="✗  Stopped", text_color=ORANGE)
            stripe.configure(fg_color=ORANGE)
            stop_btn.configure(state="disabled", fg_color=BORDER)
            pause_btn.configure(state="disabled", fg_color=BORDER)

        def do_pause():
            if state["done"] or state["cancelled"]: return
            pe = job.get("pause_event")
            if not state["paused"]:
                state["paused"] = True
                if pe: pe.set()   # set = paused
                pause_btn.configure(text="▶  Resume",
                                    fg_color="#1C2A12", text_color=GREEN)
                status_lbl.configure(text="⏸  Paused", text_color=YELLOW)
            else:
                state["paused"] = False
                if pe: pe.clear()  # clear = running
                pause_btn.configure(text="⏸  Pause", fg_color=PANEL, text_color=WHITE)
                status_lbl.configure(text="⬇  Downloading", text_color=CYAN)

        pause_btn = ctk.CTkButton(r1, text="⏸  Pause", width=88,
                                   font=("Segoe UI",10,"bold"),
                                   fg_color=PANEL, hover_color=BORDER2,
                                   text_color=WHITE, height=28, corner_radius=8,
                                   command=do_pause)
        pause_btn.pack(side="right", padx=(0,6))

        stop_btn = ctk.CTkButton(r1, text="■  Stop", width=76,
                                  font=("Segoe UI",10,"bold"),
                                  fg_color="#2A0A0A", hover_color=RED2,
                                  text_color=RED, height=28, corner_radius=8,
                                  command=do_stop)
        stop_btn.pack(side="right", padx=(0,4))

        ctk.CTkLabel(r1, text=job["title"],
                     font=("Segoe UI",12,"bold"), text_color=WHITE,
                     wraplength=680, anchor="w").pack(side="left")

        # ── Row 2: meta info ──────────────────────────────────────────────────
        meta_row = ctk.CTkFrame(inn, fg_color="transparent")
        meta_row.pack(fill="x", pady=(4,8))
        res_f = ctk.CTkFrame(meta_row, fg_color=INPUT, corner_radius=6)
        res_f.pack(side="left", padx=(0,8))
        ctk.CTkLabel(res_f, text=job["resolution"], font=F_TINY,
                     text_color=CYAN).pack(padx=7, pady=2)
        ctk.CTkLabel(meta_row, text=job["out_dir"],
                     font=F_TINY, text_color=DIM, anchor="w").pack(side="left")

        # ── Progress bar 1 — video ────────────────────────────────────────────
        pb1r = ctk.CTkFrame(inn, fg_color="transparent"); pb1r.pack(fill="x", pady=(2,0))
        ctk.CTkLabel(pb1r, text="Video", font=F_TINY, text_color=GRAY,
                     width=42).pack(side="left")
        prog1 = ctk.CTkProgressBar(pb1r, fg_color=INPUT, progress_color=PURPLE,
                                    height=8, corner_radius=4)
        prog1.set(0); prog1.pack(side="left", fill="x", expand=True, padx=(6,0))
        pct1_lbl = ctk.CTkLabel(pb1r, text="0%", font=F_TINY, text_color=DIM, width=38)
        pct1_lbl.pack(side="left", padx=(6,0))

        # ── Progress bar 2 — subtitles ────────────────────────────────────────
        # ── Progress bar 2 — subtitles ────────────────────────────────────────
        pb2r = ctk.CTkFrame(inn, fg_color="transparent"); pb2r.pack(fill="x", pady=(4,0))
        ctk.CTkLabel(pb2r, text="Subs", font=F_TINY, text_color=DIM,
                     width=42).pack(side="left")
        prog2 = ctk.CTkProgressBar(pb2r, fg_color=INPUT, progress_color=CYAN,
                                    height=5, corner_radius=3)
        prog2.set(0); prog2.pack(side="left", fill="x", expand=True, padx=(6,0))
        pct2_lbl = ctk.CTkLabel(pb2r, text="—", font=F_TINY, text_color=DIM, width=38)
        pct2_lbl.pack(side="left", padx=(6,0))

        # ── Log line ──────────────────────────────────────────────────────────
        log_lbl = ctk.CTkLabel(inn, text="", font=F_MONO, text_color=DIM, anchor="w")
        log_lbl.pack(fill="x", pady=(7,0))

        return {"frame": frame, "stripe": stripe, "status_lbl": status_lbl,
                "prog": prog1, "prog2": prog2, "pct1": pct1_lbl, "pct2": pct2_lbl,
                "log_lbl": log_lbl, "done": False,
                "stop_btn": stop_btn, "pause_btn": pause_btn, "state": state}

    def _dl_log(self, c, t):
        try: c["log_lbl"].configure(text=t[:140])
        except: pass
    def _dl_prog(self, c, p):
        try:
            c["prog"].set(min(p,1.0)); c["pct1"].configure(text=f"{int(p*100)}%")
            if p > 0.01: c["status_lbl"].configure(text="⬇  Downloading", text_color=CYAN)
        except: pass
    def _dl_prog2(self, c, p):
        try: c["prog2"].set(min(p,1.0)); c["pct2"].configure(text=f"{int(p*100)}%")
        except: pass
    def _dl_done(self, c, path):
        try:
            c["prog"].set(1.0); c["pct1"].configure(text="100%")
            c["prog"].configure(progress_color=GREEN)
            c["stripe"].configure(fg_color=GREEN)
            c["status_lbl"].configure(text="✓  Complete", text_color=GREEN)
            c["log_lbl"].configure(text=f"✓  Saved → {Path(path).name}", text_color=GREEN)
            c["done"] = True; c["state"]["done"] = True
            c["stop_btn"].configure(state="disabled", fg_color=BORDER, text_color=DIM)
            c["pause_btn"].configure(state="disabled", fg_color=BORDER, text_color=DIM)
            c["frame"].configure(border_color=GREEN)
        except: pass
    def _dl_sub_done(self, c, sub_path):
        try:
            c["prog2"].set(1.0); c["prog2"].configure(progress_color=CYAN)
            c["pct2"].configure(text="✓", text_color=CYAN)
            purified = self._cfg.get("purify_subs", False) and HAS_PYSRT and HAS_BP
            label = f"✨  Subtitle purified → {Path(sub_path).name}" if purified else f"💬  Subtitle → {Path(sub_path).name}"
            c["log_lbl"].configure(text=label, text_color=CYAN)
        except: pass
    def _dl_err(self, c, e):
        try:
            c["stripe"].configure(fg_color=RED)
            c["frame"].configure(border_color=RED)
            c["status_lbl"].configure(text="✗  Error", text_color=RED)
            c["log_lbl"].configure(text=f"✗  {e}", text_color=RED)
            c["state"]["done"] = True
            c["stop_btn"].configure(state="disabled", fg_color=BORDER, text_color=DIM)
            c["pause_btn"].configure(state="disabled", fg_color=BORDER, text_color=DIM)
        except: pass
    def _open_dl_folder(self):
        d = self._cfg.get("out_dir", str(Path.home()/"Videos"/"CineSnatch"))
        Path(d).mkdir(parents=True, exist_ok=True)
        if sys.platform=="win32": os.startfile(d)
        else: subprocess.Popen(["xdg-open",d])

    # ══════════════════════════════════════════════════════════════════════════
    # SETTINGS PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _mk_settings_page(self):
        page = ctk.CTkFrame(self._area, fg_color="transparent")
        self._pages["settings"] = page
        top = ctk.CTkFrame(page, fg_color=BG2, height=72, corner_radius=0)
        top.pack(fill="x"); top.pack_propagate(False)
        ctk.CTkFrame(top, width=5, fg_color=CYAN, corner_radius=0).place(x=0, y=0, relheight=1)
        ctk.CTkLabel(top, text="Settings", font=("Segoe UI",17,"bold"),
                     text_color=WHITE).place(x=22, rely=.5, anchor="w")
        ctk.CTkFrame(page, height=2, fg_color=CYAN, corner_radius=0).pack(fill="x")
        sc = ctk.CTkScrollableFrame(page, fg_color="transparent")
        sc.pack(fill="both", expand=True, padx=32, pady=20)

        def sec(title):
            ctk.CTkLabel(sc, text=title, font=("Segoe UI",9,"bold"), text_color=GRAY
                         ).pack(anchor="w", pady=(18,6))
            f = ctk.CTkFrame(sc, fg_color=CARD, corner_radius=12); f.pack(fill="x")
            return f

        def row(parent, label, key, default, ph="", browse=False, password=False):
            f = ctk.CTkFrame(parent, fg_color="transparent")
            f.pack(fill="x", padx=20, pady=12)
            ctk.CTkFrame(parent, height=1, fg_color=BORDER).pack(fill="x", padx=20)
            ctk.CTkLabel(f, text=label, font=("Segoe UI",12,"bold"),
                         text_color=WHITE, width=220, anchor="w").pack(side="left")
            var = ctk.StringVar(value=self._cfg.get(key, default))
            ent = ctk.CTkEntry(f, textvariable=var, placeholder_text=ph,
                               font=F_BODY, fg_color=INPUT, border_color=BORDER2,
                               text_color=WHITE, height=36,
                               show="*" if password else "")
            ent.pack(side="left", fill="x", expand=True)
            if browse:
                def pick_dir(v=var):
                    d = filedialog.askdirectory()
                    if d: v.set(d)
                ctk.CTkButton(f, text="Browse", width=80, font=F_SMALL,
                              fg_color=PANEL, hover_color=INPUT,
                              text_color=GRAY, height=36, corner_radius=6,
                              command=pick_dir).pack(side="left", padx=(8,0))
            var.trace_add("write", lambda *a, k=key, v=var: (
                self._cfg.update({k:v.get()}), save_cfg(self._cfg)))

        s1 = sec("DOWNLOADS")
        row(s1, "Save folder", "out_dir", str(Path.home()/"Videos"/"CineSnatch"), browse=True)
        row(s1, "Subtitles folder", "sub_dir", str(Path.home()/"Videos"/"CineSnatch"/"Subtitles"), browse=True)
        row(s1, "Default resolution", "def_res", "1080p")

        s2 = sec("ALLDEBRID  (for instant downloads)")
        ctk.CTkLabel(s2,
                     text="AllDebrid converts torrents to direct HTTP links for 1-click downloads at full speed. Free trial at alldebrid.com/apikeys",
                     font=F_TINY, text_color=GRAY, justify="left"
                     ).pack(anchor="w", padx=20, pady=(10,4))
        row(s2, "AllDebrid API key", "ad_key", "", "Paste key from alldebrid.com/apikeys")

        def test_ad():
            key = self._cfg.get("ad_key","").strip()
            if not key:
                messagebox.showwarning("No key", "Enter your AllDebrid API key first.")
                return
            ok, msg = alldebrid_test_key(key)
            if ok: messagebox.showinfo("AllDebrid ✓", msg)
            else:  messagebox.showerror("AllDebrid ✗", msg)

        ctk.CTkButton(s2, text="🔑  Test AllDebrid Key",
                      font=("Segoe UI",11,"bold"),
                      fg_color="#1A1040", hover_color="#2A1A60",
                      text_color=PURPLE, height=36, corner_radius=8, width=200,
                      command=test_ad).pack(anchor="w", padx=20, pady=(4,14))

        s2b = sec("SUBDL  (automatic subtitles)")
        ctk.CTkLabel(s2b,
                     text="Get a free API key at subdl.com/profile/api — subtitles auto-download after each video.",
                     font=F_TINY, text_color=GRAY, justify="left", wraplength=700
                     ).pack(anchor="w", padx=20, pady=(10,4))
        row(s2b, "Subdl API key", "subdl_key", "", "Paste key from subdl.com/profile/api")
        row(s2b, "Default subtitle language", "sub_lang_name", "None")

        def test_subdl():
            key = self._cfg.get("subdl_key","").strip()
            if not key:
                messagebox.showwarning("No key", "Enter your Subdl API key first.")
                return
            try:
                r = requests.get("https://api.subdl.com/api/v1/subtitles",
                    params={"api_key": key, "film_name": "The Dark Knight",
                            "languages": "EN", "subs_per_page": 1},
                    headers=HEADERS, timeout=8)
                d = r.json()
                if r.status_code == 200 and "subtitles" in d:
                    messagebox.showinfo("Subdl ✓", "API key is valid! Subtitles will auto-download.")
                else:
                    messagebox.showerror("Subdl ✗", f"Error {r.status_code}: {d.get('message','Invalid key')}")
            except Exception as e:
                messagebox.showerror("Subdl ✗", f"Connection failed:\n{e}")
        ctk.CTkButton(s2b, text="🔑  Test Subdl Key",
                      font=("Segoe UI",11,"bold"),
                      fg_color="#0A1A10", hover_color="#1A3020",
                      text_color=GREEN, height=36, corner_radius=8, width=200,
                      command=test_subdl).pack(anchor="w", padx=20, pady=(4,14))

        s2c = sec("SUBTITLE PURIFIER  (auto-clean profanity)")

        # Purify toggle row
        pf = ctk.CTkFrame(s2c, fg_color="transparent")
        pf.pack(fill="x", padx=20, pady=12)
        ctk.CTkFrame(s2c, height=1, fg_color=BORDER).pack(fill="x", padx=20)
        ctk.CTkLabel(pf, text="Auto-purify subtitles", font=("Segoe UI",12,"bold"),
                     text_color=WHITE, width=220, anchor="w").pack(side="left")
        purify_var = ctk.BooleanVar(value=self._cfg.get("purify_subs", False))
        ctk.CTkSwitch(pf, text="Remove profanity from every downloaded .srt",
                      variable=purify_var, font=F_SMALL, text_color=GRAY,
                      progress_color=GREEN, button_color=WHITE,
                      command=lambda: (
                          self._cfg.update({"purify_subs": purify_var.get()}),
                          save_cfg(self._cfg)
                      )).pack(side="left")

        row(s2c, "Replacement word", "purify_word", "beep",
            "Word to replace profanity with (default: beep)")

        # Warn if libs missing
        if not HAS_PYSRT:
            ctk.CTkLabel(s2c,
                         text="⚠  Run: pip install pysrt   to enable purifier",
                         font=F_TINY, text_color=YELLOW, justify="left"
                         ).pack(anchor="w", padx=20, pady=(0,10))
        else:
            ctk.CTkLabel(s2c,
                         text="✓  Subtitle purifier ready — filters f/s/b/c-words and sexual terms only",
                         font=F_TINY, text_color=GREEN, justify="left"
                         ).pack(anchor="w", padx=20, pady=(0,10))

        s3 = sec("API")
        row(s3, "OMDB API key", "omdb_key", OMDB_KEY_DEF, "Free key at omdbapi.com")

        s4 = sec("HOW IT WORKS")
        for n in ["⚡  16-thread parallel downloads (maximum speed)",
                  "🧲  Sources: Torrentio · YTS · TPB · EZTV",
                  "☁  AllDebrid: torrent → direct HTTP at full speed",
                  "💬  Subtitles via subdl.com — add your free API key in Settings",
                  "📁  Video saved in original format — no re-encoding"]:
            ctk.CTkLabel(s4, text=n, font=("Consolas",11),
                         text_color=GRAY, anchor="w").pack(anchor="w", padx=20, pady=4)

    # ══════════════════════════════════════════════════════════════════════════
    # SUBTITLES PAGE
    # ══════════════════════════════════════════════════════════════════════════
    def _mk_subs_page(self):
        page = ctk.CTkFrame(self._area, fg_color="transparent")
        self._pages["subtitles"] = page

        # ── Header ────────────────────────────────────────────────────────────
        top = ctk.CTkFrame(page, fg_color=BG2, height=72, corner_radius=0)
        top.pack(fill="x"); top.pack_propagate(False)
        ctk.CTkFrame(top, width=5, fg_color=GREEN, corner_radius=0).place(x=0, y=0, relheight=1)
        ctk.CTkLabel(top, text="Subtitles", font=("Segoe UI",17,"bold"),
                     text_color=WHITE).place(x=22, rely=.5, anchor="w")
        ctk.CTkFrame(page, height=2, fg_color=GREEN, corner_radius=0).pack(fill="x")

        sc = ctk.CTkScrollableFrame(page, fg_color="transparent")
        sc.pack(fill="both", expand=True, padx=32, pady=20)

        # ── Search row ────────────────────────────────────────────────────────
        ctk.CTkLabel(sc, text="SEARCH TITLE", font=("Segoe UI",9,"bold"),
                     text_color=GRAY).pack(anchor="w", pady=(0,6))
        search_card = ctk.CTkFrame(sc, fg_color=CARD, corner_radius=12)
        search_card.pack(fill="x")
        search_row = ctk.CTkFrame(search_card, fg_color="transparent")
        search_row.pack(fill="x", padx=20, pady=14)

        self._sub_q = ctk.StringVar()
        sub_ent = ctk.CTkEntry(search_row, textvariable=self._sub_q,
                               placeholder_text="Movie or series name…",
                               font=F_BODY, fg_color=INPUT, border_color=BORDER2,
                               text_color=WHITE, height=40, corner_radius=10)
        sub_ent.pack(side="left", fill="x", expand=True, padx=(0,10))

        self._sub_search_btn = ctk.CTkButton(search_row, text="Search",
                               font=("Segoe UI",12,"bold"),
                               fg_color=PURPLE, hover_color=PURPLE2,
                               text_color=WHITE, height=40, width=100, corner_radius=10,
                               command=self._subs_do_search)
        self._sub_search_btn.pack(side="left")
        sub_ent.bind("<Return>", lambda e: self._subs_do_search())

        # ── Results list ──────────────────────────────────────────────────────
        ctk.CTkLabel(sc, text="RESULTS", font=("Segoe UI",9,"bold"),
                     text_color=GRAY).pack(anchor="w", pady=(18,6))
        self._sub_results_frame = ctk.CTkFrame(sc, fg_color=CARD, corner_radius=12)
        self._sub_results_frame.pack(fill="x")
        self._sub_no_results = ctk.CTkLabel(self._sub_results_frame,
                                            text="Search for a title above to get started.",
                                            font=F_BODY, text_color=DIM)
        self._sub_no_results.pack(pady=20)
        self._sub_selected = {}   # holds selected omdb result dict

        # ── Episode row (series only) — hidden until a series is selected ──────
        self._sub_ep_outer = ctk.CTkFrame(sc, fg_color="transparent")
        # NOT packed here — only shown in _subs_build_ep_picker

        # ── Options row ───────────────────────────────────────────────────────
        ctk.CTkLabel(sc, text="OPTIONS", font=("Segoe UI",9,"bold"),
                     text_color=GRAY).pack(anchor="w", pady=(18,6))
        opts_card = ctk.CTkFrame(sc, fg_color=CARD, corner_radius=12)
        opts_card.pack(fill="x")
        opts_row = ctk.CTkFrame(opts_card, fg_color="transparent")
        opts_row.pack(fill="x", padx=20, pady=14)

        ctk.CTkLabel(opts_row, text="Language:", font=F_BODY,
                     text_color=WHITE).pack(side="left", padx=(0,10))
        self._sub_lang_var = ctk.StringVar(value=self._cfg.get("sub_lang_name","English") or "English")
        lang_menu = ctk.CTkOptionMenu(opts_row,
                                      values=list(SUB_LANGS.keys()),
                                      variable=self._sub_lang_var,
                                      font=F_BODY, fg_color=INPUT,
                                      button_color=BORDER2, button_hover_color=PANEL,
                                      text_color=WHITE, width=160)
        lang_menu.pack(side="left", padx=(0,24))

        ctk.CTkLabel(opts_row, text="Save to:", font=F_BODY,
                     text_color=WHITE).pack(side="left", padx=(0,10))
        self._sub_savedir_var = ctk.StringVar(
            value=self._cfg.get("sub_dir", str(Path.home()/"Videos"/"CineSnatch"/"Subtitles")))
        ctk.CTkEntry(opts_row, textvariable=self._sub_savedir_var,
                     font=F_SMALL, fg_color=INPUT, border_color=BORDER2,
                     text_color=WHITE, height=36, width=240).pack(side="left")
        ctk.CTkButton(opts_row, text="Browse", width=76, font=F_SMALL,
                      fg_color=PANEL, hover_color=INPUT, text_color=GRAY,
                      height=36, corner_radius=6,
                      command=lambda: self._sub_savedir_var.set(
                          filedialog.askdirectory() or self._sub_savedir_var.get()
                      )).pack(side="left", padx=(8,0))

        # Purify toggle + custom word
        purify_row = ctk.CTkFrame(opts_card, fg_color="transparent")
        purify_row.pack(fill="x", padx=20, pady=(0,14))
        self._sub_purify_var = ctk.BooleanVar(value=self._cfg.get("purify_subs", False))
        ctk.CTkSwitch(purify_row, text="Auto-purify after download",
                      variable=self._sub_purify_var,
                      font=F_SMALL, text_color=GRAY,
                      progress_color=GREEN, button_color=WHITE).pack(side="left")
        ctk.CTkLabel(purify_row, text="Replace with:",
                     font=F_SMALL, text_color=GRAY).pack(side="left", padx=(24,6))
        self._sub_replace_var = ctk.StringVar(value=self._cfg.get("purify_word", "beep"))
        ctk.CTkEntry(purify_row, textvariable=self._sub_replace_var,
                     width=100, font=F_BODY, fg_color=INPUT,
                     border_color=BORDER2, text_color=WHITE, height=32,
                     corner_radius=8).pack(side="left")
        self._sub_replace_var.trace_add("write", lambda *a: (
            self._cfg.update({"purify_word": self._sub_replace_var.get()}),
            save_cfg(self._cfg)))

        # ── Download button ───────────────────────────────────────────────────
        self._sub_dl_btn = ctk.CTkButton(sc, text="💬  Download Subtitle",
                           font=("Segoe UI",13,"bold"),
                           fg_color=GREEN, hover_color="#22A878",
                           text_color=BG, height=48, corner_radius=12,
                           command=self._subs_download)
        self._sub_dl_btn.pack(fill="x", pady=(20,4))

        # ── Status log ────────────────────────────────────────────────────────
        self._sub_status = ctk.CTkLabel(sc, text="", font=F_BODY,
                                        text_color=GRAY, wraplength=700, justify="left")
        self._sub_status.pack(anchor="w", pady=(8,0))

    def _subs_do_search(self):
        q = self._sub_q.get().strip()
        if not q: return
        self._sub_search_btn.configure(state="disabled", text="Searching…")
        self._sub_status.configure(text="")
        # clear old results
        for w in self._sub_results_frame.winfo_children(): w.destroy()
        for w in self._sub_ep_outer.winfo_children(): w.destroy()
        self._sub_ep_outer.pack_forget()
        self._sub_selected = {}
        key = self._cfg.get("omdb_key", OMDB_KEY_DEF)
        def worker():
            results = omdb_search(q, key)
            self.after(0, lambda: self._subs_render_results(results))
        threading.Thread(target=worker, daemon=True).start()

    def _subs_render_results(self, results):
        self._sub_search_btn.configure(state="normal", text="Search")
        for w in self._sub_results_frame.winfo_children(): w.destroy()
        if not results:
            ctk.CTkLabel(self._sub_results_frame, text="No results found.",
                         font=F_BODY, text_color=DIM).pack(pady=20)
            return
        # Show up to 8 results as selectable rows
        self._sub_result_btns = []
        for item in results[:8]:
            title = item.get("Title","?")
            year  = item.get("Year","")
            typ   = item.get("Type","").capitalize()
            imdb  = item.get("imdbID","")
            label = f"{title}  ({year})  — {typ}"
            btn = ctk.CTkButton(self._sub_results_frame, text=label,
                                font=F_BODY, anchor="w",
                                fg_color="transparent", hover_color=PANEL,
                                text_color=GRAY, height=38, corner_radius=8,
                                command=lambda i=item: self._subs_select(i))
            btn.pack(fill="x", padx=8, pady=2)
            self._sub_result_btns.append((imdb, btn))

    def _subs_select(self, item):
        # Highlight selected
        imdb = item.get("imdbID","")
        for iid, btn in self._sub_result_btns:
            btn.configure(fg_color=PANEL if iid==imdb else "transparent",
                          text_color=WHITE if iid==imdb else GRAY)
        self._sub_selected = item
        # Clear episode picker and hide it
        for w in self._sub_ep_outer.winfo_children(): w.destroy()
        self._sub_ep_outer.pack_forget()
        if item.get("Type","").lower() == "series":
            self._subs_build_ep_picker(item)

    def _subs_build_ep_picker(self, item):
        outer = self._sub_ep_outer
        outer.pack(fill="x")  # show it now
        ctk.CTkLabel(outer, text="EPISODE", font=("Segoe UI",9,"bold"),
                     text_color=GRAY).pack(anchor="w", pady=(18,6))
        ep_card = ctk.CTkFrame(outer, fg_color=CARD, corner_radius=12)
        ep_card.pack(fill="x")
        ep_row = ctk.CTkFrame(ep_card, fg_color="transparent")
        ep_row.pack(fill="x", padx=20, pady=14)

        ctk.CTkLabel(ep_row, text="Season:", font=F_BODY,
                     text_color=WHITE).pack(side="left", padx=(0,8))
        self._sub_season_var = ctk.StringVar(value="1")
        seas_spin = ctk.CTkEntry(ep_row, textvariable=self._sub_season_var,
                                 width=60, font=F_BODY, fg_color=INPUT,
                                 border_color=BORDER2, text_color=WHITE, height=36)
        seas_spin.pack(side="left", padx=(0,20))

        ctk.CTkLabel(ep_row, text="Episode:", font=F_BODY,
                     text_color=WHITE).pack(side="left", padx=(0,8))
        self._sub_episode_var = ctk.StringVar(value="1")
        ep_spin = ctk.CTkEntry(ep_row, textvariable=self._sub_episode_var,
                               width=60, font=F_BODY, fg_color=INPUT,
                               border_color=BORDER2, text_color=WHITE, height=36)
        ep_spin.pack(side="left")

    def _subs_download(self):
        if not self._sub_selected:
            self._sub_status.configure(text="⚠  Pick a title from the results first.", text_color=YELLOW)
            return
        lang_name = self._sub_lang_var.get()
        lang_code = SUB_LANGS.get(lang_name, "en")
        if not lang_code:
            self._sub_status.configure(text="⚠  Please choose a language.", text_color=YELLOW)
            return
        api_key = self._cfg.get("subdl_key","").strip()
        if not api_key:
            self._sub_status.configure(text="⚠  No Subdl API key set — add it in Settings.", text_color=YELLOW)
            return

        item     = self._sub_selected
        title    = item.get("Title","Unknown")
        imdb_id  = item.get("imdbID","")
        is_series = item.get("Type","").lower() == "series"
        season = episode = None
        ep_hint = ""
        if is_series:
            try: season  = int(self._sub_season_var.get())
            except: season = 1
            try: episode = int(self._sub_episode_var.get())
            except: episode = 1
            ep_hint = f"S{str(season).zfill(2)}E{str(episode).zfill(2)}"

        save_dir = Path(self._sub_savedir_var.get())
        save_dir.mkdir(parents=True, exist_ok=True)
        safe_title = re.sub(r'[\\/*?:"<>|]', "", title).replace(" ", ".")
        fname = f"{safe_title}.{ep_hint}.srt" if ep_hint else f"{safe_title}.srt"
        final_path = str(save_dir / fname)

        self._sub_dl_btn.configure(state="disabled", text="Downloading…")
        self._sub_status.configure(text="⏳  Connecting to subdl.com…", text_color=GRAY)

        do_purify    = self._sub_purify_var.get()
        purify_word  = self._sub_replace_var.get().strip() or "beep"

        def worker():
            result = fetch_subtitle(
                title=title, ep_hint=ep_hint, lang_code=lang_code,
                out_dir=str(save_dir), final_path=final_path,
                api_key=api_key, imdb_id=imdb_id,
                season=season, episode=episode)
            if not result:
                self.after(0, lambda: (
                    self._sub_dl_btn.configure(state="normal", text="💬  Download Subtitle"),
                    self._sub_status.configure(
                        text="✗  Could not find subtitles. Try a different language or check your Subdl key.",
                        text_color=RED)))
                return
            purified_count = 0
            if do_purify and HAS_PYSRT:
                try:
                    _, purified_count = purify_srt(result, replacement=purify_word)
                except: pass
            def _done():
                self._sub_dl_btn.configure(state="normal", text="💬  Download Subtitle")
                msg = f"✓  Saved to: {result}"
                if do_purify:
                    msg += f"   ({purified_count} word(s) purified)" if HAS_PYSRT else "   (install pysrt to enable purifier)"
                self._sub_status.configure(text=msg, text_color=GREEN)
            self.after(0, _done)
        threading.Thread(target=worker, daemon=True).start()

    def on_close(self):
        for j in self._jobs:
            try:
                p = j.get("proc")
                if p: p.terminate()
            except: pass
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.protocol("WM_DELETE_WINDOW", app.on_close)
    app.mainloop()
