import argparse
import json
import logging
import os
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

import pdfplumber
import requests
import schedule
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

ABEAM_URL = "https://abeam.org.br/estudo-da-frota/"
DATA_DIR = Path("data")
PDF_DIR = DATA_DIR / "pdfs"
JSON_DIR = DATA_DIR / "json"
STATE_FILE = DATA_DIR / "last_processed.json"

WP_URL = os.getenv("WP_URL", "")
WP_USER = os.getenv("WP_USER", "")
WP_PASSWORD = os.getenv("WP_PASSWORD", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
STATIC_DIR = Path(os.getenv("STATIC_DIR", "public/data"))

for d in [PDF_DIR, JSON_DIR, STATIC_DIR]:
    d.mkdir(parents=True, exist_ok=True)

TYPE_ORDER = [
    "PSV / OSRV", "AHTS", "LH / SV", "RSV", "CSV/MPSV", "PLSV",
    "CREW / FSV", "FLOTEL/CSOV", "SDSV", "RV", "WSV", "HLV",
    "DLV", "OTSV", "DSV",
]

FOREIGN_TYPE_ORDER = [
    "PSV / OSRV", "PLSV", "CSV/MPSV", "FLOTEL/CSOV", "AHTS", "RV",
    "HLV", "DLV", "RSV", "WSV", "CREW / FSV",
]

# Dados históricos semente (antes do pipeline automatizado)
HISTORICO_SEED = [
    {"periodo": "1997",     "label": "1997",    "total": 137, "brasileira": 32,  "estrangeira": 105},
    {"periodo": "2000",     "label": "2000",    "total": 155, "brasileira": 65,  "estrangeira": 90},
    {"periodo": "2004",     "label": "2004",    "total": 205, "brasileira": 90,  "estrangeira": 115},
    {"periodo": "2007",     "label": "2007",    "total": 280, "brasileira": 112, "estrangeira": 168},
    {"periodo": "2010",     "label": "2010",    "total": 400, "brasileira": 200, "estrangeira": 200},
    {"periodo": "2012",     "label": "2012",    "total": 460, "brasileira": 243, "estrangeira": 217},
    {"periodo": "2014",     "label": "2014",    "total": 500, "brasileira": 247, "estrangeira": 253},
    {"periodo": "2016",     "label": "2016",    "total": 386, "brasileira": 311, "estrangeira": 75},
    {"periodo": "2018",     "label": "2018",    "total": 360, "brasileira": 300, "estrangeira": 60},
    {"periodo": "2020",     "label": "2020",    "total": 374, "brasileira": 336, "estrangeira": 38},
    {"periodo": "2021",     "label": "2021",    "total": 393, "brasileira": 361, "estrangeira": 32},
    {"periodo": "2022",     "label": "2022",    "total": 415, "brasileira": 376, "estrangeira": 39},
    {"periodo": "2024",     "label": "2024",    "total": 453, "brasileira": 382, "estrangeira": 71},
]

MESES_PT = {
    "janeiro": "Jan", "fevereiro": "Fev", "março": "Mar", "marco": "Mar",
    "abril": "Abr", "maio": "Mai", "junho": "Jun",
    "julho": "Jul", "agosto": "Ago", "setembro": "Set",
    "outubro": "Out", "novembro": "Nov", "dezembro": "Dez",
}


def periodo_label(periodo: str) -> str:
    """Converte 'Fevereiro 2026' → 'Fev/26'."""
    if not periodo:
        return periodo
    m = re.match(r"(\w+)\s+(\d{4})", periodo)
    if not m:
        return periodo
    mes = MESES_PT.get(m.group(1).lower(), m.group(1)[:3])
    return f"{mes}/{m.group(2)[2:]}"


def get_latest_pdf_link() -> dict | None:
    log.info("Verificando página ABEAM...")
    try:
        r = requests.get(ABEAM_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
    except requests.RequestException as e:
        log.error(f"Erro ao acessar ABEAM: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    links = [a for a in soup.find_all("a", href=True) if "wpdmdl" in a["href"]]
    if not links:
        log.warning("Nenhum link de download encontrado.")
        return None

    # Ordena por ano/mês encontrado no texto do link para pegar o mais recente
    def link_sort_key(a):
        text = a.get_text()
        year = re.search(r"20(\d{2})", text)
        month_map = {m: i for i, m in enumerate(
            ["janeiro","fevereiro","março","marco","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"], 1)}
        month = next((month_map[k] for k in month_map if k in text.lower()), 0)
        return (int(year.group(1)) if year else 0, month)

    links.sort(key=link_sort_key)
    latest = links[-1]
    return {"label": latest.get_text(strip=True), "url": latest["href"]}


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def is_new(link: dict, state: dict) -> bool:
    return link["url"] != state.get("last_url")


def slugify(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def download_pdf(url: str, label: str) -> Path | None:
    pdf_path = PDF_DIR / f"abeam-{slugify(label)}.pdf"
    if pdf_path.exists():
        log.info(f"PDF já existe: {pdf_path.name}")
        return pdf_path
    try:
        r = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        pdf_path.write_bytes(r.content)
        log.info(f"PDF salvo: {pdf_path.name}")
        return pdf_path
    except requests.RequestException as e:
        log.error(f"Erro no download do PDF: {e}")
        return None


def read_pages(pdf_path: Path) -> list[str]:
    with pdfplumber.open(pdf_path) as pdf:
        return [(page.extract_text() or "") for page in pdf.pages]


def extract_period_and_totals(pages: list[str]) -> tuple[str, dict]:
    text = "\n".join(pages)
    period_match = re.search(
        r"(Janeiro|Fevereiro|Março|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)\s*/?\s*(\d{4})",
        text,
        re.IGNORECASE,
    )
    period = f"{period_match.group(1).capitalize()} {period_match.group(2)}" if period_match else None

    # \d+ aceita qualquer quantidade de dígitos (não mais limitado a 2-3)
    totals_match = re.search(
        r"(\d+)\s+embarca[çc][õo]es,\s*(\d+)\s*\((\d+)%\)\s*de bandeira brasileira\s*e\s*(\d+)\s*\((\d+)%\)\s*de bandeira estrangeira",
        text,
        re.IGNORECASE,
    )
    if not totals_match:
        raise ValueError("Não foi possível extrair os totais gerais do PDF.")

    totals = {
        "total": int(totals_match.group(1)),
        "brasileira": int(totals_match.group(2)),
        "pct_brasileira": int(totals_match.group(3)),
        "estrangeira": int(totals_match.group(4)),
    }
    return period, totals


def ints_from_line(line: str) -> list[int]:
    return [int(x) for x in re.findall(r"\d+", line)]


def find_page_with_total(pages: list[str], expected_last: int, min_numbers: int) -> str:
    """Busca dinamicamente a página que contém a linha Total correta."""
    for page in pages:
        for line in page.splitlines():
            if re.match(r"^Total\b", line.strip()):
                nums = ints_from_line(line)
                if nums and nums[-1] == expected_last and len(nums) >= min_numbers:
                    return page
    raise ValueError(f"Página com linha Total={expected_last} e ≥{min_numbers} colunas não encontrada.")


def find_companies_page(pages: list[str]) -> str:
    """Busca dinamicamente a página com tabela de empresas."""
    best_page = None
    best_count = 0
    for page in pages:
        count = len([l for l in page.splitlines()
                     if re.search(r'\b(ABEAM|Não Associado|Nao Associado)\b', l, re.IGNORECASE)])
        if count > best_count:
            best_count = count
            best_page = page
    if not best_page or best_count < 3:
        raise ValueError("Página de empresas não encontrada (menos de 3 linhas com status).")
    return best_page


def parse_total_line(page_text: str, expected_last: int, min_numbers: int) -> list[int]:
    for line in page_text.splitlines():
        if re.match(r"^Total\b", line.strip()):
            nums = ints_from_line(line)
            if nums and nums[-1] == expected_last and len(nums) >= min_numbers:
                return nums
    raise ValueError(f"Linha Total não encontrada para valor final {expected_last}.")


def parse_types(pages: list[str], totals: dict) -> list[dict]:
    total_page = find_page_with_total(pages, totals["total"], 16)
    total_counts = parse_total_line(total_page, totals["total"], 16)[:-1]
    if len(total_counts) != len(TYPE_ORDER):
        raise ValueError(f"Contagem total por tipo: esperado {len(TYPE_ORDER)}, obtido {len(total_counts)}.")

    foreign_page = find_page_with_total(pages, totals["estrangeira"], 12)
    foreign_counts = parse_total_line(foreign_page, totals["estrangeira"], 12)[:-1]
    if len(foreign_counts) != len(FOREIGN_TYPE_ORDER):
        raise ValueError(f"Contagem estrangeira: esperado {len(FOREIGN_TYPE_ORDER)}, obtido {len(foreign_counts)}.")

    total_map = dict(zip(TYPE_ORDER, total_counts))
    foreign_map = {k: 0 for k in TYPE_ORDER}
    foreign_map.update(dict(zip(FOREIGN_TYPE_ORDER, foreign_counts)))

    result = []
    for t in TYPE_ORDER:
        total = total_map[t]
        foreign = foreign_map[t]
        brazilian = total - foreign
        if brazilian < 0:
            raise ValueError(f"Tipo {t} ficou com bandeira brasileira negativa.")
        result.append({
            "tipo": t,
            "total": total,
            "brasileira": brazilian,
            "estrangeira": foreign,
            "pct": round(total / totals["total"] * 100, 1),
        })
    return result


def parse_company_line(line: str) -> dict | None:
    line = line.strip()
    if not line:
        return None
    if re.match(r"^(Total|Bandeira|Empresa)", line):
        return None
    if "Empresa Status Total" in line:
        return None
    # Status flexível: aceita ABEAM, Não Associado e variações
    m = re.match(
        r"^(.*?)\s+(ABEAM|N[ãa]o\s+Associado)\s+((?:\d+\s+){1,2}\d+)\s*$",
        line,
        re.IGNORECASE,
    )
    if not m:
        return None
    empresa = m.group(1).strip()
    status = m.group(2).strip()
    nums = [int(x) for x in m.group(3).split()]
    if len(nums) == 2:
        brasileira, total = nums
        estrangeira = 0
    elif len(nums) == 3:
        brasileira, estrangeira, total = nums
    else:
        return None
    return {
        "empresa": empresa.title()
            .replace("Dof / Norskan", "DOF / Norskan")
            .replace("Wsut", "WSUT")
            .replace("Cbo", "CBO"),
        "status": status,
        "brasileira": brasileira,
        "estrangeira": estrangeira,
        "total": total,
    }


def parse_companies(pages: list[str], totals: dict) -> tuple[list[dict], int]:
    companies_page = find_companies_page(pages)
    companies = []
    for line in companies_page.splitlines():
        row = parse_company_line(line)
        if row:
            companies.append(row)
    if not companies:
        raise ValueError("Não foi possível extrair a tabela de empresas.")
    total_empresas = len(companies)
    if sum(c["total"] for c in companies) != totals["total"]:
        raise ValueError("Soma das empresas não bate com o total da frota.")
    top = sorted(companies, key=lambda x: (-x["total"], x["empresa"]))[:10]
    return top, total_empresas


def validate_data(data: dict):
    totals = data["totais"]
    if totals["brasileira"] + totals["estrangeira"] != totals["total"]:
        raise ValueError("Totais gerais inconsistentes.")
    if sum(item["total"] for item in data["por_tipo"]) != totals["total"]:
        raise ValueError("Soma dos tipos não bate com o total da frota.")
    if sum(item["brasileira"] for item in data["por_tipo"]) != totals["brasileira"]:
        raise ValueError("Soma brasileira por tipo não bate com o total brasileiro.")
    if sum(item["estrangeira"] for item in data["por_tipo"]) != totals["estrangeira"]:
        raise ValueError("Soma estrangeira por tipo não bate com o total estrangeiro.")
    for item in data["por_tipo"]:
        if item["brasileira"] + item["estrangeira"] != item["total"]:
            raise ValueError(f"Tipo inconsistente: {item['tipo']}")
    if not data["top_empresas"]:
        raise ValueError("Top empresas vazio.")


def load_historico() -> list[dict]:
    """Carrega histórico existente do JSON mais recente, ou retorna semente."""
    latest = STATIC_DIR / "abeam-latest.json"
    if latest.exists():
        try:
            data = json.loads(latest.read_text(encoding="utf-8"))
            if data.get("historico"):
                return data["historico"]
        except Exception:
            pass
    return list(HISTORICO_SEED)


def update_historico(historico: list[dict], periodo: str, totals: dict) -> list[dict]:
    """Adiciona o período atual ao histórico se ainda não existir."""
    label = periodo_label(periodo)
    # Verifica se este período já está no histórico (por label ou periodo)
    for entry in historico:
        if entry.get("periodo") == periodo or entry.get("label") == label:
            log.info(f"Período {periodo} já está no histórico.")
            return historico
    historico.append({
        "periodo": periodo,
        "label": label,
        "total": totals["total"],
        "brasileira": totals["brasileira"],
        "estrangeira": totals["estrangeira"],
    })
    log.info(f"Período {periodo} adicionado ao histórico ({len(historico)} entradas).")
    return historico


def extract_data(pdf_path: Path) -> dict:
    log.info(f"Extraindo dados: {pdf_path.name}")
    pages = read_pages(pdf_path)
    period, totals = extract_period_and_totals(pages)
    por_tipo = parse_types(pages, totals)
    top_empresas, empresas_total = parse_companies(pages, totals)

    historico = load_historico()
    historico = update_historico(historico, period, totals)

    data = {
        "source": "ABEAM / Syndarma",
        "extracted": datetime.now().isoformat(),
        "periodo": period,
        "totais": totals,
        "por_tipo": por_tipo,
        "top_empresas": top_empresas,
        "empresas_total": empresas_total,
        "historico": historico,
    }
    validate_data(data)
    log.info(
        "Extração concluída: %s · %s embarcações · %s tipos · %s pontos históricos",
        data["periodo"],
        data["totais"]["total"],
        len(data["por_tipo"]),
        len(data["historico"]),
    )
    return data


def save_json(data: dict, label: str) -> Path:
    slug = slugify(label)
    target = JSON_DIR / f"abeam-{slug}.json"
    latest = JSON_DIR / "abeam-latest.json"
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    target.write_text(payload, encoding="utf-8")
    latest.write_text(payload, encoding="utf-8")
    return latest


def copy_json_to_static(json_path: Path):
    dest = STATIC_DIR / "abeam-latest.json"
    shutil.copyfile(json_path, dest)
    log.info(f"JSON copiado para {dest}")


def update_wordpress(data: dict):
    if not all([WP_URL, WP_USER, WP_PASSWORD]):
        return
    page_id = os.getenv("WP_PAGE_ID", "")
    if not page_id:
        log.warning("WP_PAGE_ID não definido. Pulando atualização do WordPress.")
        return
    endpoint = f"{WP_URL}/wp-json/wp/v2/pages/{page_id}"
    payload = {
        "meta": {
            "abeam_data": json.dumps(data, ensure_ascii=False),
            "abeam_updated": data["extracted"],
        }
    }
    r = requests.post(endpoint, json=payload, auth=(WP_USER, WP_PASSWORD), timeout=30)
    r.raise_for_status()
    log.info("WordPress atualizado com sucesso.")


def notify(data: dict):
    if not WEBHOOK_URL:
        return
    msg = {
        "text": (
            f"VAPOZEIRO — relatório ABEAM atualizado\n"
            f"Período: {data['periodo']}\n"
            f"Total: {data['totais']['total']}\n"
            f"Brasileira: {data['totais']['brasileira']}\n"
            f"Estrangeira: {data['totais']['estrangeira']}"
        )
    }
    try:
        requests.post(WEBHOOK_URL, json=msg, timeout=10)
    except Exception as exc:
        log.warning(f"Falha ao enviar notificação: {exc}")


def process_pdf(pdf_path: Path, label: str):
    data = extract_data(pdf_path)
    latest = save_json(data, label)
    copy_json_to_static(latest)
    update_wordpress(data)
    notify(data)
    return data


def run_pipeline(input_pdf: str | None = None):
    if input_pdf:
        pdf_path = Path(input_pdf)
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF não encontrado: {pdf_path}")
        label = pdf_path.stem
        process_pdf(pdf_path, label)
        return

    link = get_latest_pdf_link()
    if not link:
        return
    state = load_state()
    if not is_new(link, state):
        log.info("Sem novo relatório. Nada para atualizar.")
        return
    pdf_path = download_pdf(link["url"], link["label"])
    if not pdf_path:
        raise RuntimeError("Falha no download do PDF.")
    process_pdf(pdf_path, link["label"])
    save_state({"last_url": link["url"], "last_label": link["label"], "last_run": datetime.now().isoformat()})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Executa uma vez e termina")
    parser.add_argument("--input-pdf", help="Usa um PDF local em vez de buscar na ABEAM")
    args = parser.parse_args()

    try:
        if args.once or args.input_pdf:
            run_pipeline(args.input_pdf)
            return
        run_pipeline()
        schedule.every().day.at("08:00").do(run_pipeline)
        while True:
            schedule.run_pending()
            time.sleep(60)
    except Exception as exc:
        log.exception("Falha no pipeline: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
